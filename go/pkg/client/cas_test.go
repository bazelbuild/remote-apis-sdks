package client_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"path/filepath"

	"strings"
	"testing"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/client"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/fakes"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/filemetadata"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/portpicker"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/uploadinfo"
	"github.com/google/go-cmp/cmp"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	// Redundant imports are required for the google3 mirror. Aliases should not be changed.
	regrpc "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	bsgrpc "google.golang.org/genproto/googleapis/bytestream"
)

const (
	instance              = "instance"
	defaultCASConcurrency = 50
	reqMaxSleepDuration   = 5 * time.Millisecond
)

func TestSplitEndpoints(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	l1, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	port := portpicker.PickUnusedPortTB(t)
	l2, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	defer l1.Close()
	defer l2.Close()
	execServer := grpc.NewServer()
	casServer := grpc.NewServer()
	blob := []byte("foobar")
	fake := &fakes.Reader{
		Blob:             blob,
		Chunks:           []int{6},
		ExpectCompressed: false,
	}
	bsgrpc.RegisterByteStreamServer(casServer, fake)
	go execServer.Serve(l1)
	go casServer.Serve(l2)
	defer casServer.Stop()
	defer execServer.Stop()
	c, err := client.NewClient(ctx, instance, client.DialParams{
		Service:    l1.Addr().String(),
		CASService: l2.Addr().String(),
		NoSecurity: true,
	}, client.StartupCapabilities(false))
	if err != nil {
		t.Fatalf("Error connecting to server: %v", err)
	}
	defer c.Close()

	got, _, err := c.ReadBlob(ctx, digest.NewFromBlob(blob))
	if err != nil {
		t.Errorf("c.ReadBlob(ctx, digest) gave error %s, want nil", err)
	}
	if !bytes.Equal(blob, got) {
		t.Errorf("c.ReadBlob(ctx, digest) gave diff: want %v, got %v", blob, got)
	}
}

func TestReadEmptyBlobDoesNotCallServer(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()
	fake := e.Server.CAS
	c := e.Client.GrpcClient

	got, _, err := c.ReadBlob(ctx, digest.Empty)
	if err != nil {
		t.Errorf("c.ReadBlob(ctx, Empty) gave error %s, want nil", err)
	}
	if len(got) != 0 {
		t.Errorf("c.ReadBlob(ctx, Empty) gave diff: want nil, got %v", got)
	}
	reads := fake.BlobReads(digest.Empty)
	if reads != 0 {
		t.Errorf("expected no blob reads to the fake, got %v", reads)
	}
}

func TestRead(t *testing.T) {
	t.Parallel()
	type testCase struct {
		name     string
		fake     fakes.Reader
		offset   int64
		limit    int64
		compress bool
		want     []byte // If nil, fake.blob is expected by default.
	}
	tests := []testCase{
		{
			name: "empty blob, 10 chunks",
			fake: fakes.Reader{
				Blob:   []byte{},
				Chunks: []int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			},
		},
		{
			name: "blob 'foobar', 1 chunk",
			fake: fakes.Reader{
				Blob:   []byte("foobar"),
				Chunks: []int{6},
			},
		},
		{
			name: "blob 'foobar', 3 evenly sized chunks",
			fake: fakes.Reader{
				Blob:   []byte("foobar"),
				Chunks: []int{2, 2, 2},
			},
		},
		{
			name: "blob 'foobar', 3 unequal chunks",
			fake: fakes.Reader{
				Blob:   []byte("foobar"),
				Chunks: []int{1, 3, 2},
			},
		},
		{
			name: "blob 'foobar', 2 chunks with 0-sized chunk between",
			fake: fakes.Reader{
				Blob:   []byte("foobar"),
				Chunks: []int{3, 0, 3},
			},
		},
		{
			name: "blob 'foobarbaz', partial read spanning multiple chunks",
			fake: fakes.Reader{
				Blob:   []byte("foobarbaz"),
				Chunks: []int{3, 0, 3, 3},
			},
			offset: 2,
			limit:  5,
			want:   []byte("obarb"),
		},
		{
			name: "blob 'foobar', partial read within chunk",
			fake: fakes.Reader{
				Blob:   []byte("foobar"),
				Chunks: []int{6},
			},
			offset: 2,
			limit:  3,
			want:   []byte("oba"),
		},
		{
			name: "blob 'foobar', partial read from start",
			fake: fakes.Reader{
				Blob:   []byte("foobar"),
				Chunks: []int{3, 3},
			},
			offset: 0,
			limit:  5,
			want:   []byte("fooba"),
		},
		{
			name: "blob 'foobar', partial read with no limit",
			fake: fakes.Reader{
				Blob:   []byte("foobar"),
				Chunks: []int{3, 3},
			},
			offset: 2,
			limit:  0,
			want:   []byte("obar"),
		},
		{
			name: "blob 'foobar', partial read with limit extending beyond end of blob",
			fake: fakes.Reader{
				Blob:   []byte("foobar"),
				Chunks: []int{3, 3},
			},
			offset: 2,
			limit:  8,
			want:   []byte("obar"),
		},
	}
	var compressionTests []testCase
	for _, tc := range tests {
		if tc.limit == 0 {
			// Limit tests don't work well with compression, as the limit refers to the compressed bytes
			// while offset, per spec, refers to uncompressed bytes.
			tc.compress = true
			tc.name = tc.name + "_compressed"
			compressionTests = append(compressionTests, tc)
		}
	}
	tests = append(tests, compressionTests...)

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			listener, err := net.Listen("tcp", ":0")
			if err != nil {
				t.Fatalf("Cannot listen: %v", err)
			}
			defer listener.Close()
			server := grpc.NewServer()
			bsgrpc.RegisterByteStreamServer(server, &tc.fake)
			go server.Serve(listener)
			defer server.Stop()
			c, err := client.NewClient(ctx, instance, client.DialParams{
				Service:    listener.Addr().String(),
				NoSecurity: true,
			}, client.StartupCapabilities(false))
			if err != nil {
				t.Fatalf("Error connecting to server: %v", err)
			}
			defer c.Close()

			tc.fake.Validate(t)

			c.CompressedBytestreamThreshold = -1
			if tc.compress {
				c.CompressedBytestreamThreshold = 0
			}
			tc.fake.ExpectCompressed = tc.compress

			want := tc.want
			if want == nil {
				want = tc.fake.Blob
			}

			if tc.offset == 0 && tc.limit > int64(len(tc.fake.Blob)) {
				got, stats, err := c.ReadBlob(ctx, digest.NewFromBlob(want))
				if err != nil {
					t.Errorf("c.ReadBlob(ctx, digest) gave error %s, want nil", err)
				}
				if !bytes.Equal(want, got) {
					t.Errorf("c.ReadBlob(ctx, digest) gave diff: want %v, got %v", want, got)
				}
				if int64(len(got)) != stats.LogicalMoved {
					t.Errorf("c.ReadBlob(ctx, digest) = _, %v - logical bytes moved different than len of blob received", stats.LogicalMoved)
				}
				if tc.compress && len(tc.fake.Blob) > 0 && stats.LogicalMoved == stats.RealMoved {
					t.Errorf("c.ReadBlob(ctx, digest) = %v - compression on but different real and logical bytes", stats)
				}
			}

			got, stats, err := c.ReadBlobRange(ctx, digest.NewFromBlob(tc.fake.Blob), tc.offset, tc.limit)
			if err != nil {
				t.Errorf("c.ReadBlobRange(ctx, digest, %d, %d) gave error %s, want nil", tc.offset, tc.limit, err)
			}
			if !bytes.Equal(want, got) {
				t.Errorf("c.ReadBlobRange(ctx, digest, %d, %d) gave diff: want %v, got %v", tc.offset, tc.limit, want, got)
			}
			if int64(len(got)) != stats.LogicalMoved {
				t.Errorf("c.ReadBlob(ctx, digest) = _, %v - logical bytes moved different than len of blob received", stats.LogicalMoved)
			}
			if tc.compress && len(tc.fake.Blob) > 0 && stats.LogicalMoved == stats.RealMoved {
				t.Errorf("c.ReadBlob(ctx, digest) = %v - compression on but same real and logical bytes", stats)
			}
		})
	}
}

func TestWrite(t *testing.T) {
	t.Parallel()
	type testcase struct {
		name string
		blob []byte
		cmp  client.CompressedBytestreamThreshold
	}
	tests := []testcase{
		{
			name: "empty blob",
			blob: []byte{},
		},
		{
			name: "small blob",
			blob: []byte("this is a pretty small blob comparatively"),
		},
		{
			name: "5MB zero blob",
			blob: make([]byte, 5*1024*1024),
		},
	}
	var allTests []testcase
	for _, tc := range tests {
		for _, th := range []int{0, -1} {
			t := tc
			t.name += fmt.Sprintf("CompressionThreshold=%d", th)
			t.cmp = client.CompressedBytestreamThreshold(th)
			allTests = append(allTests, t)
		}
	}

	for _, tc := range allTests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			listener, err := net.Listen("tcp", ":0")
			if err != nil {
				t.Fatalf("Cannot listen: %v", err)
			}
			defer listener.Close()
			server := grpc.NewServer()
			fake := &fakes.Writer{}
			bsgrpc.RegisterByteStreamServer(server, fake)
			go server.Serve(listener)
			defer server.Stop()
			c, err := client.NewClient(ctx, instance, client.DialParams{
				Service:    listener.Addr().String(),
				NoSecurity: true,
			}, client.StartupCapabilities(false), client.ChunkMaxSize(20)) // Use small write chunk size for tests.
			if err != nil {
				t.Fatalf("Error connecting to server: %v", err)
			}
			defer c.Close()

			fake.ExpectCompressed = int(tc.cmp) == 0
			tc.cmp.Apply(c)

			gotDg, err := c.WriteBlob(ctx, tc.blob)
			if err != nil {
				t.Errorf("c.WriteBlob(ctx, blob) gave error %s, wanted nil", err)
			}
			if fake.Err != nil {
				t.Errorf("c.WriteBlob(ctx, blob) caused the server to return error %s (possibly unseen by c)", fake.Err)
			}
			if !bytes.Equal(tc.blob, fake.Buf) {
				t.Errorf("c.WriteBlob(ctx, blob) had diff on blobs, want %v, got %v:", tc.blob, fake.Buf)
			}
			dg := digest.NewFromBlob(tc.blob)
			if dg != gotDg {
				t.Errorf("c.WriteBlob(ctx, blob) had diff on digest returned (want %s, got %s)", dg, gotDg)
			}
		})
	}
}

func TestMissingBlobs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		// present is the blobs present in the CAS.
		present []string
		// input is the digests given to MissingBlobs.
		input []digest.Digest
		// want is the returned list of digests.
		want []digest.Digest
	}{
		{
			name:    "none present",
			present: nil,
			input: []digest.Digest{
				digest.NewFromBlob([]byte("foo")),
				digest.NewFromBlob([]byte("bar")),
				digest.NewFromBlob([]byte("baz")),
			},
			want: []digest.Digest{
				digest.NewFromBlob([]byte("foo")),
				digest.NewFromBlob([]byte("bar")),
				digest.NewFromBlob([]byte("baz")),
			},
		},
		{
			name:    "all present",
			present: []string{"foo", "bar", "baz"},
			input: []digest.Digest{
				digest.NewFromBlob([]byte("foo")),
				digest.NewFromBlob([]byte("bar")),
				digest.NewFromBlob([]byte("baz")),
			},
			want: nil,
		},
		{
			name:    "some present",
			present: []string{"foo", "bar"},
			input: []digest.Digest{
				digest.NewFromBlob([]byte("foo")),
				digest.NewFromBlob([]byte("bar")),
				digest.NewFromBlob([]byte("baz")),
			},
			want: []digest.Digest{
				digest.NewFromBlob([]byte("baz")),
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			e, cleanup := fakes.NewTestEnv(t)
			defer cleanup()
			fake := e.Server.CAS
			c := e.Client.GrpcClient
			for _, s := range tc.present {
				fake.Put([]byte(s))
			}
			t.Logf("CAS contains digests of %s", tc.present)
			got, err := c.MissingBlobs(ctx, tc.input)
			if err != nil {
				t.Errorf("c.MissingBlobs(ctx, %v) gave error %s, expected nil", tc.input, err)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("c.MissingBlobs(ctx, %v) gave diff (want -> got):\n%s", tc.input, diff)
			}
		})
	}
}

func TestUploadConcurrent(t *testing.T) {
	t.Parallel()
	blobs := make([][]byte, 50)
	for i := range blobs {
		blobs[i] = []byte(fmt.Sprint(i))
	}
	type testCase struct {
		name string
		// Whether to use batching.
		batching client.UseBatchOps
		// Whether to use background CAS ops.
		unified client.UnifiedUploads
		// The batch size.
		maxBatchDigests client.MaxBatchDigests
		// The CAS concurrency for uploading the blobs.
		concurrency client.CASConcurrency
	}
	var tests []testCase
	for _, ub := range []client.UseBatchOps{false, true} {
		for _, cb := range []client.UnifiedUploads{false, true} {
			for _, conc := range []client.CASConcurrency{3, 100} {
				tc := testCase{
					name:            fmt.Sprintf("batch:%t,unified:%t,conc:%d", ub, cb, conc),
					batching:        ub,
					unified:         cb,
					maxBatchDigests: client.MaxBatchDigests(9),
					concurrency:     conc,
				}
				tests = append(tests, tc)
			}
		}
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			e, cleanup := fakes.NewTestEnv(t)
			defer cleanup()
			fake := e.Server.CAS
			fake.ReqSleepDuration = reqMaxSleepDuration
			fake.ReqSleepRandomize = true
			c := e.Client.GrpcClient
			for _, opt := range []client.Opt{tc.batching, tc.maxBatchDigests, tc.concurrency, tc.unified} {
				opt.Apply(c)
			}
			c.RunBackgroundTasks(ctx)

			eg, eCtx := errgroup.WithContext(ctx)
			for i := 0; i < 100; i++ {
				eg.Go(func() error {
					var input []*uploadinfo.Entry
					for _, blob := range append(blobs, blobs...) {
						input = append(input, uploadinfo.EntryFromBlob(blob))
					}
					if _, _, err := c.UploadIfMissing(eCtx, input...); err != nil {
						return fmt.Errorf("c.UploadIfMissing(ctx, input) gave error %v, expected nil", err)
					}
					return nil
				})
			}
			if err := eg.Wait(); err != nil {
				t.Error(err)
			}
			// Verify everything was written exactly once.
			for i, blob := range blobs {
				dg := digest.NewFromBlob(blob)
				if tc.unified {
					if fake.BlobWrites(dg) != 1 {
						t.Errorf("wanted 1 write for blob %v: %v, got %v", i, dg, fake.BlobWrites(dg))
					}
					if fake.BlobMissingReqs(dg) != 100 {
						// 100 requests per blob.
						t.Errorf("wanted 100 missing request for blob %v: %v, got %v", i, dg, fake.BlobMissingReqs(dg))
					}
				}
			}
		})
	}
}

func TestUploadConcurrentBatch(t *testing.T) {
	t.Parallel()
	blobs := make([][]byte, 100)
	for i := range blobs {
		blobs[i] = []byte(fmt.Sprint(i))
	}
	ctx := context.Background()
	for _, uo := range []client.UnifiedUploads{false, true} {
		uo := uo
		t.Run(fmt.Sprintf("unified:%t", uo), func(t *testing.T) {
			t.Parallel()
			e, cleanup := fakes.NewTestEnv(t)
			defer cleanup()
			fake := e.Server.CAS
			fake.ReqSleepDuration = reqMaxSleepDuration
			fake.ReqSleepRandomize = true
			c := e.Client.GrpcClient
			c.MaxBatchDigests = 50
			client.UnifiedUploadTickDuration(500 * time.Millisecond).Apply(c)
			uo.Apply(c)
			c.RunBackgroundTasks(ctx)

			eg, eCtx := errgroup.WithContext(ctx)
			for i := 0; i < 10; i++ {
				i := i
				eg.Go(func() error {
					var input []*uploadinfo.Entry
					// Upload 15 digests in a sliding window.
					for j := i * 10; j < i*10+15 && j < len(blobs); j++ {
						input = append(input, uploadinfo.EntryFromBlob(blobs[j]))
						// Twice to have the same upload in same call, in addition to between calls.
						input = append(input, uploadinfo.EntryFromBlob(blobs[j]))
					}
					if _, _, err := c.UploadIfMissing(eCtx, input...); err != nil {
						return fmt.Errorf("c.UploadIfMissing(ctx, input) gave error %v, expected nil", err)
					}
					return nil
				})
			}
			if err := eg.Wait(); err != nil {
				t.Error(err)
			}
			// Verify everything was written exactly once.
			for i, blob := range blobs {
				dg := digest.NewFromBlob(blob)
				if c.UnifiedUploads {
					if fake.BlobWrites(dg) != 1 {
						t.Errorf("wanted 1 write for blob %v: %v, got %v", i, dg, fake.BlobWrites(dg))
					}
				}
			}
			expectedReqs := 10
			if c.UnifiedUploads {
				// All the 100 digests will be batched into two batches, together.
				expectedReqs = 2
			}
			if fake.BatchReqs() != expectedReqs {
				t.Errorf("%d requests were made to BatchUpdateBlobs, wanted %v", fake.BatchReqs(), expectedReqs)
			}
		})
	}
}

func TestUploadCancel(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	blob := []byte{1, 2, 3}
	dg := digest.NewFromBlob(blob)
	for _, uo := range []client.UnifiedUploads{false, true} {
		uo := uo
		t.Run(fmt.Sprintf("unified:%t", uo), func(t *testing.T) {
			t.Parallel()
			e, cleanup := fakes.NewTestEnv(t)
			defer cleanup()
			fake := e.Server.CAS
			wait := make(chan bool)
			fake.PerDigestBlockFn[dg] = func() {
				<-wait
			}
			c := e.Client.GrpcClient
			uo.Apply(c)
			client.UseBatchOps(false).Apply(c)
			c.RunBackgroundTasks(ctx)

			cCtx, cancel := context.WithCancel(ctx)
			eg, _ := errgroup.WithContext(cCtx)
			ue := uploadinfo.EntryFromBlob(blob)
			eg.Go(func() error {
				if _, _, err := c.UploadIfMissing(cCtx, ue); !errors.Is(err, context.Canceled) {
					return fmt.Errorf("c.UploadIfMissing(ctx, input) gave error %v, expected to wrap context.Canceled", err)
				}
				return nil
			})
			eg.Go(func() error {
				time.Sleep(60 * time.Millisecond) // Enough time to trigger upload cycle.
				cancel()
				time.Sleep(10 * time.Millisecond)
				return nil
			})
			if err := eg.Wait(); err != nil {
				t.Error(err)
			}
			// Verify that nothing was written.
			if fake.BlobWrites(ue.Digest) != 0 {
				t.Errorf("Blob was written, expected cancellation.")
			}
			close(wait)
		})
	}
}

func TestUploadConcurrentCancel(t *testing.T) {
	t.Parallel()
	blobs := make([][]byte, 50)
	for i := range blobs {
		blobs[i] = []byte(fmt.Sprint(i))
	}
	var input []*uploadinfo.Entry
	for _, blob := range blobs {
		input = append(input, uploadinfo.EntryFromBlob(blob))
	}
	input = append(input, input...)
	type testCase struct {
		name string
		// Whether to use batching.
		batching client.UseBatchOps
		// Whether to use background CAS ops.
		unified client.UnifiedUploads
		// The batch size.
		maxBatchDigests client.MaxBatchDigests
		// The CAS concurrency for uploading the blobs.
		concurrency client.CASConcurrency
	}
	var tests []testCase
	for _, ub := range []client.UseBatchOps{false, true} {
		for _, uo := range []client.UnifiedUploads{false, true} {
			for _, conc := range []client.CASConcurrency{3, 20} {
				tc := testCase{
					name:            fmt.Sprintf("batch:%t,unified:%t,conc:%d", ub, uo, conc),
					batching:        ub,
					unified:         uo,
					maxBatchDigests: client.MaxBatchDigests(9),
					concurrency:     conc,
				}
				tests = append(tests, tc)
			}
		}
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			e, cleanup := fakes.NewTestEnv(t)
			defer cleanup()
			fake := e.Server.CAS
			fake.ReqSleepDuration = reqMaxSleepDuration
			fake.ReqSleepRandomize = true
			c := e.Client.GrpcClient
			for _, opt := range []client.Opt{tc.batching, tc.maxBatchDigests, tc.concurrency, tc.unified} {
				opt.Apply(c)
			}
			c.RunBackgroundTasks(ctx)

			eg, eCtx := errgroup.WithContext(ctx)
			eg.Go(func() error {
				if _, _, err := c.UploadIfMissing(eCtx, input...); err != nil {
					return fmt.Errorf("c.UploadIfMissing(ctx, input) gave error %v, expected nil", err)
				}
				return nil
			})
			cCtx, cancel := context.WithCancel(eCtx)
			for i := 0; i < 50; i++ {
				eg.Go(func() error {
					// Verify that we got a context cancellation error. Sometimes, the request can succeed, if the original thread takes a while to run.
					if _, _, err := c.UploadIfMissing(cCtx, input...); err != nil && !errors.Is(err, context.Canceled) {
						return fmt.Errorf("c.UploadIfMissing(ctx, input) gave error %+v!, expected context canceled", err)
					}
					return nil
				})
			}
			eg.Go(func() error {
				time.Sleep(time.Duration(20*rand.Float32()) * time.Microsecond)
				cancel()
				return nil
			})
			if err := eg.Wait(); err != nil {
				t.Error(err)
			}
			if tc.unified {
				// Verify everything was written exactly once, despite the context being canceled.
				for i, blob := range blobs {
					dg := digest.NewFromBlob(blob)
					if fake.BlobWrites(dg) != 1 {
						t.Errorf("wanted 1 write for blob %v: %v, got %v", i, dg, fake.BlobWrites(dg))
					}
				}
			}
		})
	}
}

func TestUpload(t *testing.T) {
	t.Parallel()
	var twoThousandBlobs [][]byte
	var thousandBlobs [][]byte
	for i := 0; i < 2000; i++ {
		var buf = new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, i)
		// Write a few extra bytes so that we have > chunkSize sized blobs.
		for j := 0; j < 10; j++ {
			binary.Write(buf, binary.LittleEndian, 0)
		}
		twoThousandBlobs = append(twoThousandBlobs, buf.Bytes())
		if i%2 == 0 {
			thousandBlobs = append(thousandBlobs, buf.Bytes())
		}
	}

	type testcase struct {
		name string
		// input is the blobs to try to store; they're converted to a file map by the test
		input [][]byte
		// present is the blobs already present in the CAS; they're pre-loaded into the fakes.CAS object
		// and the test verifies no attempt was made to upload them.
		present [][]byte
		opts    []client.Opt
	}
	tests := []testcase{
		{
			name:    "No blobs",
			input:   nil,
			present: nil,
		},
		{
			name:    "None present",
			input:   [][]byte{[]byte("foo"), []byte("bar"), []byte("baz")},
			present: nil,
		},
		{
			name:    "All present",
			input:   [][]byte{[]byte("foo"), []byte("bar"), []byte("baz")},
			present: [][]byte{[]byte("foo"), []byte("bar"), []byte("baz")},
		},
		{
			name:    "Some present",
			input:   [][]byte{[]byte("foo"), []byte("bar"), []byte("baz")},
			present: [][]byte{[]byte("bar")},
		},
		{
			name:    "2000 blobs heavy concurrency",
			input:   twoThousandBlobs,
			present: thousandBlobs,
			opts:    []client.Opt{client.CASConcurrency(500)},
		},
	}

	var allTests []testcase
	for _, tc := range tests {
		for _, ub := range []client.UseBatchOps{false, true} {
			for _, uo := range []client.UnifiedUploads{false, true} {
				for _, cmp := range []client.CompressedBytestreamThreshold{-1, 0} {
					t := tc
					t.name = fmt.Sprintf("%s_UsingBatch:%t,UnifiedUploads:%t,CompressionThresh:%d", tc.name, ub, uo, cmp)
					t.opts = append(t.opts, []client.Opt{ub, uo, cmp}...)
					allTests = append(allTests, t)
				}
			}
		}
	}
	for _, tc := range allTests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			e, cleanup := fakes.NewTestEnv(t)
			defer cleanup()
			fake := e.Server.CAS
			c := e.Client.GrpcClient
			for _, o := range tc.opts {
				o.Apply(c)
			}
			c.RunBackgroundTasks(ctx)

			present := make(map[digest.Digest]bool)
			for _, blob := range tc.present {
				fake.Put(blob)
				present[digest.NewFromBlob(blob)] = true
			}
			var input []*uploadinfo.Entry
			for _, blob := range tc.input {
				input = append(input, uploadinfo.EntryFromBlob(blob))
			}

			missing, bMoved, err := c.UploadIfMissing(ctx, input...)
			if err != nil {
				t.Errorf("c.UploadIfMissing(ctx, input) gave error %v, expected nil", err)
			}

			missingSet := make(map[digest.Digest]struct{})
			totalBytes := int64(0)
			for _, dg := range missing {
				missingSet[dg] = struct{}{}
				totalBytes += dg.Size
			}

			// It's much harder to check the case where compression is on as we also have to ignore batch ops,
			// so we just don't.
			if int(c.CompressedBytestreamThreshold) < 0 && bMoved != totalBytes {
				t.Errorf("c.UploadIfMissing(ctx, input) = %v, expected %v (reported different bytes moved and digest size despite no compression)", bMoved, totalBytes)
			}

			for i, ue := range input {
				dg := ue.Digest
				blob := tc.input[i]
				if present[dg] {
					if fake.BlobWrites(dg) > 0 {
						t.Errorf("blob %v with digest %s was uploaded even though it was already present in the CAS", blob, dg)
					}
					if _, ok := missingSet[dg]; ok {
						t.Errorf("Stats said that blob %v with digest %s was missing in the CAS", blob, dg)
					}
					continue
				}
				if gotBlob, ok := fake.Get(dg); !ok {
					t.Errorf("blob %v with digest %s was not uploaded, expected it to be present in the CAS", blob, dg)
				} else if !bytes.Equal(blob, gotBlob) {
					t.Errorf("blob digest %s had diff on uploaded blob: want %v, got %v", dg, blob, gotBlob)
				}
				if _, ok := missingSet[dg]; !ok {
					t.Errorf("Stats said that blob %v with digest %s was present in the CAS", blob, dg)
				}
			}
			if fake.MaxConcurrency() > defaultCASConcurrency {
				t.Errorf("CAS concurrency %v was higher than max %v", fake.MaxConcurrency(), defaultCASConcurrency)
			}
		})
	}
}

func TestWriteBlobsBatching(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tests := []struct {
		name      string
		sizes     []int
		batchReqs int
		writeReqs int
	}{
		{
			name:      "single small blob",
			sizes:     []int{1},
			batchReqs: 0,
			writeReqs: 1,
		},
		{
			name:      "large and small blobs hitting max exactly",
			sizes:     []int{338, 338, 338, 1, 1, 1},
			batchReqs: 3,
			writeReqs: 0,
		},
		{
			name:      "small batches of big blobs",
			sizes:     []int{88, 88, 88, 88, 88, 88, 88},
			batchReqs: 2,
			writeReqs: 1,
		},
		{
			name:      "batch with blob that's too big",
			sizes:     []int{400, 88, 88, 88},
			batchReqs: 1,
			writeReqs: 1,
		},
		{
			name:      "many small blobs hitting max digests",
			sizes:     []int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
			batchReqs: 4,
			writeReqs: 0,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e, cleanup := fakes.NewTestEnv(t)
			defer cleanup()
			fake := e.Server.CAS
			c := e.Client.GrpcClient
			c.MaxBatchSize = 500
			c.MaxBatchDigests = 4
			// Each batch request frame overhead is 13 bytes.
			// A per-blob overhead is 74 bytes.

			blobs := make(map[digest.Digest][]byte)
			for i, sz := range tc.sizes {
				blob := make([]byte, int(sz))
				blob[0] = byte(i) // Ensure blobs are distinct
				blobs[digest.NewFromBlob(blob)] = blob
			}

			err := c.WriteBlobs(ctx, blobs)
			if err != nil {
				t.Fatalf("c.WriteBlobs(ctx, inputs) gave error %s, expected nil", err)
			}

			for d, blob := range blobs {
				if gotBlob, ok := fake.Get(d); !ok {
					t.Errorf("blob with digest %s was not uploaded, expected it to be present in the CAS", d)
				} else if !bytes.Equal(blob, gotBlob) {
					t.Errorf("blob with digest %s had diff on uploaded blob: wanted %v, got %v", d, blob, gotBlob)
				}
			}
			if fake.BatchReqs() != tc.batchReqs {
				t.Errorf("%d requests were made to BatchUpdateBlobs, wanted %d", fake.BatchReqs(), tc.batchReqs)
			}
			if fake.WriteReqs() != tc.writeReqs {
				t.Errorf("%d requests were made to Write, wanted %d", fake.WriteReqs(), tc.writeReqs)
			}
		})
	}
}

func TestFlattenActionOutputs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()
	fake := e.Server.CAS
	c := e.Client.GrpcClient

	fooDigest := digest.TestNew("1001", 1)
	barDigest := digest.TestNew("1002", 2)
	dirB := &repb.Directory{
		Files: []*repb.FileNode{
			{Name: "foo", Digest: fooDigest.ToProto(), IsExecutable: true},
		},
	}
	bDigest := digest.TestNewFromMessage(dirB)
	dirA := &repb.Directory{
		Directories: []*repb.DirectoryNode{
			{Name: "b", Digest: bDigest.ToProto()},
		},
		Files: []*repb.FileNode{
			{Name: "bar", Digest: barDigest.ToProto()},
		},
	}
	aDigest := digest.TestNewFromMessage(dirA)
	root := &repb.Directory{
		Directories: []*repb.DirectoryNode{
			{Name: "a", Digest: aDigest.ToProto()},
			{Name: "b", Digest: bDigest.ToProto()},
		},
	}
	tr := &repb.Tree{
		Root:     root,
		Children: []*repb.Directory{dirA, dirB},
	}
	treeBlob, err := proto.Marshal(tr)
	if err != nil {
		t.Errorf("failed marshalling Tree: %s", err)
	}
	treeA := &repb.Tree{
		Root:     dirA,
		Children: []*repb.Directory{dirB},
	}
	treeABlob, err := proto.Marshal(treeA)
	if err != nil {
		t.Errorf("failed marshalling Tree: %s", err)
	}
	treeDigest := fake.Put(treeBlob)
	treeADigest := fake.Put(treeABlob)
	ar := &repb.ActionResult{
		OutputFiles: []*repb.OutputFile{
			&repb.OutputFile{Path: "foo", Digest: fooDigest.ToProto()}},
		OutputFileSymlinks: []*repb.OutputSymlink{
			&repb.OutputSymlink{Path: "x/bar", Target: "../dir/a/bar"}},
		OutputDirectorySymlinks: []*repb.OutputSymlink{
			&repb.OutputSymlink{Path: "x/a", Target: "../dir/a"}},
		OutputDirectories: []*repb.OutputDirectory{
			&repb.OutputDirectory{Path: "dir", TreeDigest: treeDigest.ToProto()},
			&repb.OutputDirectory{Path: "dir2", TreeDigest: treeADigest.ToProto()},
		},
	}
	outputs, err := c.FlattenActionOutputs(ctx, ar)
	if err != nil {
		t.Errorf("error in FlattenActionOutputs: %s", err)
	}
	wantOutputs := map[string]*client.TreeOutput{
		"dir/a/b/foo": &client.TreeOutput{Digest: fooDigest, IsExecutable: true},
		"dir/a/bar":   &client.TreeOutput{Digest: barDigest},
		"dir/b/foo":   &client.TreeOutput{Digest: fooDigest, IsExecutable: true},
		"dir2/b/foo":  &client.TreeOutput{Digest: fooDigest, IsExecutable: true},
		"dir2/bar":    &client.TreeOutput{Digest: barDigest},
		"foo":         &client.TreeOutput{Digest: fooDigest},
		"x/a":         &client.TreeOutput{SymlinkTarget: "../dir/a"},
		"x/bar":       &client.TreeOutput{SymlinkTarget: "../dir/a/bar"},
	}
	if len(outputs) != len(wantOutputs) {
		t.Errorf("FlattenActionOutputs gave wrong number of outputs: want %d, got %d", len(wantOutputs), len(outputs))
	}
	for path, wantOut := range wantOutputs {
		got, ok := outputs[path]
		if !ok {
			t.Errorf("expected output %s is missing", path)
		}
		if got.Path != path {
			t.Errorf("FlattenActionOutputs keyed %s output with %s path", got.Path, path)
		}
		if wantOut.Digest != got.Digest {
			t.Errorf("FlattenActionOutputs gave digest diff on %s: want %v, got: %v", path, wantOut.Digest, got.Digest)
		}
		if wantOut.IsExecutable != got.IsExecutable {
			t.Errorf("FlattenActionOutputs gave IsExecutable diff on %s: want %v, got: %v", path, wantOut.IsExecutable, got.IsExecutable)
		}
		if wantOut.SymlinkTarget != got.SymlinkTarget {
			t.Errorf("FlattenActionOutputs gave symlink target diff on %s: want %s, got: %s", path, wantOut.SymlinkTarget, got.SymlinkTarget)
		}
	}
}

func TestDownloadActionOutputs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()
	fake := e.Server.CAS
	c := e.Client.GrpcClient
	cache := filemetadata.NewSingleFlightCache()

	fooDigest := fake.Put([]byte("foo"))
	barDigest := fake.Put([]byte("bar"))
	dirB := &repb.Directory{
		Files: []*repb.FileNode{
			{Name: "foo", Digest: fooDigest.ToProto(), IsExecutable: true},
		},
	}
	bDigest := digest.TestNewFromMessage(dirB)
	dirA := &repb.Directory{
		Directories: []*repb.DirectoryNode{
			{Name: "b", Digest: bDigest.ToProto()},
			{Name: "e2", Digest: digest.Empty.ToProto()},
		},
		Files: []*repb.FileNode{
			{Name: "bar", Digest: barDigest.ToProto()},
		},
	}
	aDigest := digest.TestNewFromMessage(dirA)
	root := &repb.Directory{
		Directories: []*repb.DirectoryNode{
			{Name: "a", Digest: aDigest.ToProto()},
			{Name: "b", Digest: bDigest.ToProto()},
			{Name: "e1", Digest: digest.Empty.ToProto()},
		},
	}
	tree := &repb.Tree{
		Root:     root,
		Children: []*repb.Directory{dirA, dirB, &repb.Directory{}},
	}
	treeBlob, err := proto.Marshal(tree)
	if err != nil {
		t.Fatalf("failed marshalling Tree: %s", err)
	}
	treeA := &repb.Tree{
		Root:     dirA,
		Children: []*repb.Directory{dirB, &repb.Directory{}},
	}
	treeABlob, err := proto.Marshal(treeA)
	if err != nil {
		t.Fatalf("failed marshalling Tree: %s", err)
	}
	treeDigest := fake.Put(treeBlob)
	treeADigest := fake.Put(treeABlob)
	ar := &repb.ActionResult{
		OutputFiles: []*repb.OutputFile{
			&repb.OutputFile{Path: "../foo", Digest: fooDigest.ToProto()}},
		OutputFileSymlinks: []*repb.OutputSymlink{
			&repb.OutputSymlink{Path: "x/bar", Target: "../dir/a/bar"}},
		OutputDirectorySymlinks: []*repb.OutputSymlink{
			&repb.OutputSymlink{Path: "x/a", Target: "../dir/a"}},
		OutputDirectories: []*repb.OutputDirectory{
			&repb.OutputDirectory{Path: "dir", TreeDigest: treeDigest.ToProto()},
			&repb.OutputDirectory{Path: "dir2", TreeDigest: treeADigest.ToProto()},
		},
	}
	execRoot := t.TempDir()
	wd := "wd"
	if err := os.Mkdir(filepath.Join(execRoot, wd), os.ModePerm); err != nil {
		t.Fatalf("failed to create working directory %v: %v", wd, err)
	}
	_, err = c.DownloadActionOutputs(ctx, ar, filepath.Join(execRoot, wd), cache)
	if err != nil {
		t.Errorf("error in DownloadActionOutputs: %s", err)
	}
	wantOutputs := []struct {
		path             string
		isExecutable     bool
		contents         []byte
		symlinkTarget    string
		isEmptyDirectory bool
		fileDigest       *digest.Digest
	}{
		{
			path:             "wd/dir/e1",
			isEmptyDirectory: true,
		},
		{
			path:             "wd/dir/a/e2",
			isEmptyDirectory: true,
		},
		{
			path:         "wd/dir/a/b/foo",
			isExecutable: true,
			contents:     []byte("foo"),
			fileDigest:   &fooDigest,
		},
		{
			path:     "wd/dir/a/bar",
			contents: []byte("bar"),
		},
		{
			path:         "wd/dir/b/foo",
			isExecutable: true,
			contents:     []byte("foo"),
			fileDigest:   &fooDigest,
		},
		{
			path:             "wd/dir2/e2",
			isEmptyDirectory: true,
		},
		{
			path:         "wd/dir2/b/foo",
			isExecutable: true,
			contents:     []byte("foo"),
			fileDigest:   &fooDigest,
		},
		{
			path:     "wd/dir2/bar",
			contents: []byte("bar"),
		},
		{
			path:       "foo",
			contents:   []byte("foo"),
			fileDigest: &fooDigest,
		},
		{
			path:          "wd/x/a",
			symlinkTarget: "../dir/a",
		},
		{
			path:          "wd/x/bar",
			symlinkTarget: "../dir/a/bar",
		},
	}
	for _, out := range wantOutputs {
		path := filepath.Join(execRoot, out.path)
		fi, err := os.Lstat(path)
		if err != nil {
			t.Errorf("expected output %s is missing", path)
		}
		if out.fileDigest != nil {
			fmd := cache.Get(path)
			if fmd == nil {
				t.Errorf("cache does not contain metadata for path: %v", path)
			} else {
				if diff := cmp.Diff(*out.fileDigest, fmd.Digest); diff != "" {
					t.Errorf("invalid digeset in cache for path %v, (-want +got): %v", path, diff)
				}
			}
		}
		if out.symlinkTarget != "" {
			if fi.Mode()&os.ModeSymlink == 0 {
				t.Errorf("expected %s to be a symlink, got %v", path, fi.Mode())
			}
			target, e := os.Readlink(path)
			if e != nil {
				t.Errorf("expected %s to be a symlink, got error reading symlink: %v", path, err)
			}
			if target != out.symlinkTarget {
				t.Errorf("expected %s to be a symlink to %s, got %s", path, out.symlinkTarget, target)
			}
		} else if out.isEmptyDirectory {
			if !fi.Mode().IsDir() {
				t.Errorf("expected %s to be a directory, got %s", path, fi.Mode())
			}
			files, err := os.ReadDir(path)
			if err != nil {
				t.Errorf("expected %s to be a directory, got error reading directory: %v", path, err)
			}
			if len(files) != 0 {
				t.Errorf("expected %s to be an empty directory, got contents: %v", path, files)
			}
		} else {
			contents, err := os.ReadFile(path)
			if err != nil {
				t.Errorf("error reading from %s: %v", path, err)
			}
			if !bytes.Equal(contents, out.contents) {
				t.Errorf("expected %s to contain %v, got %v", path, out.contents, contents)
			}
			// TODO(olaola): verify the file is executable, if required.
			// Doing this naively failed go test in CI.
		}
	}
}

func TestDownloadActionOutputs_TestFileModifiedTimestamp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()
	fake := e.Server.CAS
	c := e.Client.GrpcClient
	cache := filemetadata.NewSingleFlightCache()

	fooDigest := fake.Put([]byte("foo"))
	barDigest := fake.Put([]byte("bar"))
	emptyDigest := fake.Put([]byte(""))

	// Expected tree structure:
	// root
	//     --> a/
	//         --> b/
	//             --> foo
	//             --> empty
	//         --> bar
	//         --> empty
	//     --> b/
	//         --> foo
	//         --> empty

	ar := &repb.ActionResult{
		OutputFiles: []*repb.OutputFile{
			&repb.OutputFile{Path: "dir/a/b/foo", Digest: fooDigest.ToProto()},
			&repb.OutputFile{Path: "dir/a/b/empty", Digest: emptyDigest.ToProto()},
			&repb.OutputFile{Path: "dir/a/bar", Digest: barDigest.ToProto()},
			&repb.OutputFile{Path: "dir/a/empty", Digest: emptyDigest.ToProto()},
			&repb.OutputFile{Path: "dir/b/foo", Digest: fooDigest.ToProto()},
			&repb.OutputFile{Path: "dir/b/empty", Digest: emptyDigest.ToProto()},
		},
	}
	execRoot := t.TempDir()
	wd := "wd"
	if err := os.Mkdir(filepath.Join(execRoot, wd), os.ModePerm); err != nil {
		t.Fatalf("failed to create working directory %v: %v", wd, err)
	}

	// Pre-create some output files to make sure file modified timestamps get updated post
	// file download.
	outputFilesToPrecreate := []string{
		filepath.Join(execRoot, "wd/dir/a/b/empty"),
		filepath.Join(execRoot, "wd/dir/a/empty"),
	}
	for _, p := range outputFilesToPrecreate {
		dir := filepath.Dir(p)
		if err := os.MkdirAll(dir, 0750); err != nil {
			t.Fatalf("Unable to precreate dirs for file %v: %v", dir, err)
		}
		f, err := os.Create(p)
		f.Close()
		if err != nil {
			t.Fatalf("Unable to precreate file %v: %v", p, err)
		}
	}
	wantMinModTime := time.Now().Local()
	time.Sleep(2 * time.Second) // system mtimes aren't super accurate - https://apenwarr.ca/log/20181113

	_, err := c.DownloadActionOutputs(ctx, ar, filepath.Join(execRoot, wd), cache)
	if err != nil {
		t.Errorf("error in DownloadActionOutputs: %s", err)
	}
	wantOutputs := []struct {
		path         string
		isExecutable bool
		contents     []byte
		fileDigest   *digest.Digest
	}{
		{
			path:         "wd/dir/a/b/foo",
			isExecutable: true,
			contents:     []byte("foo"),
			fileDigest:   &fooDigest,
		},
		{
			path:     "wd/dir/a/bar",
			contents: []byte("bar"),
		},
		{
			path:         "wd/dir/b/foo",
			isExecutable: true,
			contents:     []byte("foo"),
			fileDigest:   &fooDigest,
		},
		{
			path:     "wd/dir/a/b/empty",
			contents: []byte(""),
		},
		{
			path:     "wd/dir/a/empty",
			contents: []byte(""),
		},
		{
			path:     "wd/dir/b/empty",
			contents: []byte(""),
		},
	}
	for _, out := range wantOutputs {
		path := filepath.Join(execRoot, out.path)
		fi, err := os.Lstat(path)
		if err != nil {
			t.Errorf("expected output %s is missing", path)
		}
		if fi.ModTime().Before(wantMinModTime) {
			t.Errorf("File %v has old timestamp, want >= %v, got %v", path, wantMinModTime, fi.ModTime())
		}
		if out.fileDigest != nil {
			fmd := cache.Get(path)
			if fmd == nil {
				t.Errorf("cache does not contain metadata for path: %v", path)
			} else {
				if diff := cmp.Diff(*out.fileDigest, fmd.Digest); diff != "" {
					t.Errorf("invalid digeset in cache for path %v, (-want +got): %v", path, diff)
				}
			}
		} else {
			contents, err := os.ReadFile(path)
			if err != nil {
				t.Errorf("error reading from %s: %v", path, err)
			}
			if !bytes.Equal(contents, out.contents) {
				t.Errorf("expected %s to contain %v, got %v", path, out.contents, contents)
			}
		}
	}
}

func TestDownloadDirectory(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()
	fake := e.Server.CAS
	c := e.Client.GrpcClient
	cache := filemetadata.NewSingleFlightCache()

	fooDigest := fake.Put([]byte("foo"))
	dir := &repb.Directory{
		Files: []*repb.FileNode{
			{Name: "foo", Digest: fooDigest.ToProto(), IsExecutable: true},
		},
		Directories: []*repb.DirectoryNode{
			{Name: "empty", Digest: digest.Empty.ToProto()},
		},
	}
	dirBlob, err := proto.Marshal(dir)
	if err != nil {
		t.Fatalf("failed marshalling Tree: %s", err)
	}
	fake.Put(dirBlob)

	d := digest.TestNewFromMessage(dir)
	execRoot := t.TempDir()

	outputs, _, err := c.DownloadDirectory(ctx, d, execRoot, cache)
	if err != nil {
		t.Errorf("error in DownloadActionOutputs: %s", err)
	}

	if diff := cmp.Diff(outputs, map[string]*client.TreeOutput{
		"empty": {
			Digest:           digest.Empty,
			Path:             "empty",
			IsEmptyDirectory: true,
		},
		"foo": {
			Digest:       fooDigest,
			Path:         "foo",
			IsExecutable: true,
		}}); diff != "" {
		t.Fatalf("DownloadDirectory() mismatch (-want +got):\n%s", diff)
	}

	b, err := os.ReadFile(filepath.Join(execRoot, "foo"))
	if err != nil {
		t.Fatalf("failed to read foo: %s", err)
	}
	if want, got := []byte("foo"), b; !bytes.Equal(want, got) {
		t.Errorf("want %s, got %s", want, got)
	}
}

func TestDownloadActionOutputsErrors(t *testing.T) {
	ar := &repb.ActionResult{}
	ar.OutputFiles = append(ar.OutputFiles, &repb.OutputFile{Path: "foo", Digest: digest.NewFromBlob([]byte("foo")).ToProto()})
	ar.OutputFiles = append(ar.OutputFiles, &repb.OutputFile{Path: "bar", Digest: digest.NewFromBlob([]byte("bar")).ToProto()})
	execRoot := t.TempDir()

	for _, ub := range []client.UseBatchOps{false, true} {
		ub := ub
		t.Run(fmt.Sprintf("%sUsingBatch:%t", t.Name(), ub), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			e, cleanup := fakes.NewTestEnv(t)
			defer cleanup()
			c := e.Client.GrpcClient
			ub.Apply(c)

			_, err := c.DownloadActionOutputs(ctx, ar, execRoot, filemetadata.NewSingleFlightCache())
			if status.Code(err) != codes.NotFound && !strings.Contains(err.Error(), "not found") {
				t.Errorf("expected 'not found' error in DownloadActionOutputs, got: %v", err)
			}
		})
	}
}

func TestDownloadActionOutputsBatching(t *testing.T) {
	tests := []struct {
		name      string
		sizes     []int
		locality  bool
		batchReqs int
	}{
		{
			name:      "single small blob",
			sizes:     []int{1},
			batchReqs: 0,
		},
		{
			name:      "large and small blobs hitting max exactly",
			sizes:     []int{338, 338, 338, 1, 1, 1},
			batchReqs: 3,
		},
		{
			name:      "small batches of big blobs",
			sizes:     []int{88, 88, 88, 88, 88, 88, 88},
			batchReqs: 2,
		},
		{
			name:      "batch with blob that's too big",
			sizes:     []int{400, 88, 88, 88},
			batchReqs: 1,
		},
		{
			name:      "many small blobs hitting max digests",
			sizes:     []int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
			batchReqs: 4,
		},
		{
			name:      "single small blob locality",
			sizes:     []int{1},
			locality:  true,
			batchReqs: 0,
		},
		{
			name:      "large and small blobs hitting max exactly locality",
			sizes:     []int{338, 338, 338, 1, 1, 1},
			locality:  true,
			batchReqs: 2,
		},
		{
			name:      "small batches of big blobs locality",
			sizes:     []int{88, 88, 88, 88, 88, 88, 88},
			locality:  true,
			batchReqs: 2,
		},
		{
			name:      "batch with blob that's too big locality",
			sizes:     []int{400, 88, 88, 88},
			locality:  true,
			batchReqs: 1,
		},
		{
			name:      "many small blobs hitting max digests locality",
			sizes:     []int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
			locality:  true,
			batchReqs: 4,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			e, cleanup := fakes.NewTestEnv(t)
			defer cleanup()
			fake := e.Server.CAS
			c := e.Client.GrpcClient
			c.MaxBatchSize = 500
			c.MaxBatchDigests = 4
			// Each batch request frame overhead is 13 bytes.
			// A per-blob overhead is 74 bytes.

			c.UtilizeLocality = client.UtilizeLocality(tc.locality)
			var dgs []digest.Digest
			blobs := make(map[digest.Digest][]byte)
			ar := &repb.ActionResult{}
			for i, sz := range tc.sizes {
				blob := make([]byte, int(sz))
				if sz > 0 {
					blob[0] = byte(i) // Ensure blobs are distinct
				}
				dg := digest.NewFromBlob(blob)
				blobs[dg] = blob
				dgs = append(dgs, dg)
				if sz > 0 {
					// Don't seed fake with empty blob, because it should not be called.
					fake.Put(blob)
				}
				name := fmt.Sprintf("foo_%s", dg)
				ar.OutputFiles = append(ar.OutputFiles, &repb.OutputFile{Path: name, Digest: dg.ToProto()})
			}
			execRoot := t.TempDir()
			_, err := c.DownloadActionOutputs(ctx, ar, execRoot, filemetadata.NewSingleFlightCache())
			if err != nil {
				t.Errorf("error in DownloadActionOutputs: %s", err)
			}
			for dg, data := range blobs {
				path := filepath.Join(execRoot, fmt.Sprintf("foo_%s", dg))
				contents, err := os.ReadFile(path)
				if err != nil {
					t.Errorf("error reading from %s: %v", path, err)
				}
				if !bytes.Equal(contents, data) {
					t.Errorf("expected %s to contain %v, got %v", path, contents, data)
				}
			}
			if fake.BatchReqs() != tc.batchReqs {
				t.Errorf("%d requests were made to BatchReadBlobs, wanted %d", fake.BatchReqs(), tc.batchReqs)
			}
		})
	}
}

func TestDownloadActionOutputsConcurrency(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	type testBlob struct {
		digest digest.Digest
		blob   []byte
	}
	blobs := make([]*testBlob, 1000)
	for i := 0; i < 1000; i++ {
		blob := []byte(fmt.Sprint(i))
		blobs[i] = &testBlob{
			digest: digest.NewFromBlob(blob),
			blob:   blob,
		}
	}

	for _, ub := range []client.UseBatchOps{false, true} {
		for _, uo := range []client.UnifiedDownloads{false, true} {
			ub, uo := ub, uo
			t.Run(fmt.Sprintf("%sUsingBatch:%t,UnifiedDownloads:%t", t.Name(), ub, uo), func(t *testing.T) {
				t.Parallel()
				e, cleanup := fakes.NewTestEnv(t)
				defer cleanup()
				fake := e.Server.CAS
				fake.ReqSleepDuration = reqMaxSleepDuration
				fake.ReqSleepRandomize = true
				c := e.Client.GrpcClient
				client.CASConcurrency(defaultCASConcurrency).Apply(c)
				client.MaxBatchDigests(300).Apply(c)
				ub.Apply(c)
				uo.Apply(c)
				for _, b := range blobs {
					fake.Put(b.blob)
				}
				c.RunBackgroundTasks(ctx)

				eg, eCtx := errgroup.WithContext(ctx)
				for i := 0; i < 100; i++ {
					i := i
					eg.Go(func() error {
						var input []*testBlob
						ar := &repb.ActionResult{}
						// Download 15 digests in a sliding window.
						for j := i * 10; j < i*10+15 && j < len(blobs); j++ {
							input = append(input, blobs[j])
						}
						for _, i := range input {
							name := fmt.Sprintf("foo_%s", i.digest)
							dgPb := i.digest.ToProto()
							ar.OutputFiles = append(ar.OutputFiles, &repb.OutputFile{Path: name, Digest: dgPb})
							ar.OutputFiles = append(ar.OutputFiles, &repb.OutputFile{Path: name + "_copy", Digest: dgPb})
						}

						execRoot := t.TempDir()
						if _, err := c.DownloadActionOutputs(eCtx, ar, execRoot, filemetadata.NewSingleFlightCache()); err != nil {
							return fmt.Errorf("error in DownloadActionOutputs: %s", err)
						}
						for _, i := range input {
							name := filepath.Join(execRoot, fmt.Sprintf("foo_%s", i.digest))
							for _, path := range []string{name, name + "_copy"} {
								contents, err := os.ReadFile(path)
								if err != nil {
									return fmt.Errorf("error reading from %s: %v", path, err)
								}
								if !bytes.Equal(contents, i.blob) {
									return fmt.Errorf("expected %s to contain %v, got %v", path, contents, i.blob)
								}
							}
						}
						return nil
					})
				}
				if err := eg.Wait(); err != nil {
					t.Error(err)
				}
				if fake.MaxConcurrency() > defaultCASConcurrency {
					t.Errorf("CAS concurrency %v was higher than max %v", fake.MaxConcurrency(), defaultCASConcurrency)
				}
				if ub {
					if uo {
						// Check that we batch requests from different Actions.
						if fake.BatchReqs() > 50 {
							t.Errorf("%d requests were made to BatchReadBlobs, wanted <= 50", fake.BatchReqs())
						}
					} else {
						if fake.BatchReqs() != 100 {
							t.Errorf("%d requests were made to BatchReadBlobs, wanted 100", fake.BatchReqs())
						}
					}
				}
			})
		}
	}
}

func TestDownloadActionOutputsOneSlowRead(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	type testBlob struct {
		digest digest.Digest
		blob   []byte
	}
	blobs := make([]*testBlob, 20)
	for i := 0; i < len(blobs); i++ {
		blob := []byte(fmt.Sprint(i))
		blobs[i] = &testBlob{
			digest: digest.NewFromBlob(blob),
			blob:   blob,
		}
	}
	problemBlob := make([]byte, 2000) // Will not be batched.
	problemDg := digest.NewFromBlob(problemBlob)

	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()
	fake := e.Server.CAS
	fake.ReqSleepDuration = reqMaxSleepDuration
	fake.ReqSleepRandomize = true
	wait := make(chan bool)
	fake.PerDigestBlockFn[problemDg] = func() {
		<-wait
	}
	c := e.Client.GrpcClient
	client.MaxBatchSize(1000).Apply(c)
	for _, b := range blobs {
		fake.Put(b.blob)
	}
	fake.Put(problemBlob)

	// Start downloading the problem digest.
	pg, pCtx := errgroup.WithContext(ctx)
	pg.Go(func() error {
		name := fmt.Sprintf("problem_%s", problemDg)
		dgPb := problemDg.ToProto()
		ar := &repb.ActionResult{}
		ar.OutputFiles = append(ar.OutputFiles, &repb.OutputFile{Path: name, Digest: dgPb})
		ar.OutputFiles = append(ar.OutputFiles, &repb.OutputFile{Path: name + "_copy", Digest: dgPb})

		execRoot := t.TempDir()
		if _, err := c.DownloadActionOutputs(pCtx, ar, execRoot, filemetadata.NewSingleFlightCache()); err != nil {
			return fmt.Errorf("error in DownloadActionOutputs: %s", err)
		}
		for _, path := range []string{name, name + "_copy"} {
			contents, err := os.ReadFile(filepath.Join(execRoot, path))
			if err != nil {
				return fmt.Errorf("error reading from %s: %v", path, err)
			}
			if !bytes.Equal(contents, problemBlob) {
				return fmt.Errorf("expected %s to contain %v, got %v", path, problemBlob, contents)
			}
		}
		return nil
	})
	// Download a bunch of fast-downloading blobs.
	eg, eCtx := errgroup.WithContext(ctx)
	for i := 0; i < 100; i++ {
		i := i
		eg.Go(func() error {
			var input []*testBlob
			ar := &repb.ActionResult{}
			// Download 15 digests in a sliding window.
			for j := i * 10; j < i*10+15 && j < len(blobs); j++ {
				input = append(input, blobs[j])
			}
			totalBytes := int64(0)
			for _, i := range input {
				name := fmt.Sprintf("foo_%s", i.digest)
				dgPb := i.digest.ToProto()
				ar.OutputFiles = append(ar.OutputFiles, &repb.OutputFile{Path: name, Digest: dgPb})
				ar.OutputFiles = append(ar.OutputFiles, &repb.OutputFile{Path: name + "_copy", Digest: dgPb})
				// Count only once due to dedup
				totalBytes += i.digest.Size
			}

			execRoot := t.TempDir()
			stats, err := c.DownloadActionOutputs(eCtx, ar, execRoot, filemetadata.NewSingleFlightCache())
			if err != nil {
				return fmt.Errorf("error in DownloadActionOutputs: %s", err)
			}
			if stats.LogicalMoved != stats.RealMoved {
				t.Errorf("c.DownloadActionOutputs: logical (%v) and real (%v) bytes moved different despite compression off", stats.LogicalMoved, stats.RealMoved)
			}
			if stats.LogicalMoved != totalBytes {
				t.Errorf("c.DownloadActionOutputs: logical (%v) bytes moved different from sum of digests (%v) despite downloaded", stats.LogicalMoved, stats.RealMoved)
			}
			for _, i := range input {
				name := filepath.Join(execRoot, fmt.Sprintf("foo_%s", i.digest))
				for _, path := range []string{name, name + "_copy"} {
					contents, err := os.ReadFile(path)
					if err != nil {
						return fmt.Errorf("error reading from %s: %v", path, err)
					}
					if !bytes.Equal(contents, i.blob) {
						return fmt.Errorf("expected %s to contain %v, got %v", path, i.blob, contents)
					}
				}
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		t.Error(err)
	}
	// Now let the problem digest download finish.
	close(wait)
	if err := pg.Wait(); err != nil {
		t.Error(err)
	}
}

func TestWriteAndReadProto(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()
	fake := e.Server.CAS
	c := e.Client.GrpcClient

	fooDigest := fake.Put([]byte("foo"))
	dirA := &repb.Directory{
		Files: []*repb.FileNode{
			{Name: "foo", Digest: fooDigest.ToProto(), IsExecutable: true},
		},
	}
	d, err := c.WriteProto(ctx, dirA)
	if err != nil {
		t.Errorf("Failed writing proto: %s", err)
	}

	dirB := &repb.Directory{}
	if _, err := c.ReadProto(ctx, d, dirB); err != nil {
		t.Errorf("Failed reading proto: %s", err)
	}
	if !proto.Equal(dirA, dirB) {
		t.Errorf("Protos not equal: %s / %s", dirA, dirB)
	}
}

func TestDownloadFiles(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()
	fake := e.Server.CAS
	c := e.Client.GrpcClient

	fooDigest := fake.Put([]byte("foo"))
	barDigest := fake.Put([]byte("bar"))

	execRoot := t.TempDir()
	stats, err := c.DownloadFiles(ctx, execRoot, map[digest.Digest]*client.TreeOutput{
		fooDigest: {Digest: fooDigest, Path: "foo", IsExecutable: true},
		barDigest: {Digest: barDigest, Path: "bar"},
	})
	if err != nil {
		t.Errorf("Failed to run DownloadFiles: %v", err)
	}
	if stats.LogicalMoved != stats.RealMoved {
		t.Errorf("c.DownloadFiles: logical (%v) and real (%v) bytes moved different despite compression off", stats.LogicalMoved, stats.RealMoved)
	}
	if stats.LogicalMoved != fooDg.Size+barDigest.Size {
		t.Errorf("c.DownloadFiles: logical (%v) bytes moved different from sum of digests (%v) despite no duplication", stats.LogicalMoved, fooDg.Size+barDigest.Size)
	}

	if b, err := os.ReadFile(filepath.Join(execRoot, "foo")); err != nil {
		t.Errorf("failed to read file: %v", err)
	} else if diff := cmp.Diff(b, []byte("foo")); diff != "" {
		t.Errorf("foo mismatch (-want +got):\n%s", diff)
	}

	if b, err := os.ReadFile(filepath.Join(execRoot, "bar")); err != nil {
		t.Errorf("failed to read file: %v", err)
	} else if diff := cmp.Diff(b, []byte("bar")); diff != "" {
		t.Errorf("foo mismatch (-want +got):\n%s", diff)
	}
}

func TestDownloadFilesCancel(t *testing.T) {
	t.Parallel()
	for _, uo := range []client.UnifiedDownloads{false, true} {
		uo := uo
		t.Run(fmt.Sprintf("UnifiedDownloads:%t", uo), func(t *testing.T) {
			t.Parallel()
			execRoot := t.TempDir()
			ctx := context.Background()
			e, cleanup := fakes.NewTestEnv(t)
			defer cleanup()
			fake := e.Server.CAS
			fooDigest := fake.Put([]byte{1, 2, 3})
			wait := make(chan bool)
			fake.PerDigestBlockFn[fooDigest] = func() {
				<-wait
			}
			c := e.Client.GrpcClient
			uo.Apply(c)
			eg, eCtx := errgroup.WithContext(ctx)
			cCtx, cancel := context.WithCancel(eCtx)
			eg.Go(func() error {
				if _, err := c.DownloadFiles(cCtx, execRoot, map[digest.Digest]*client.TreeOutput{
					fooDigest: {Digest: fooDigest, Path: "foo", IsExecutable: true},
				}); err != context.Canceled {
					return fmt.Errorf("Failed to run DownloadFiles: expected context.Canceled, got %v", err)
				}
				return nil
			})
			eg.Go(func() error {
				cancel()
				return nil
			})
			if err := eg.Wait(); err != nil {
				t.Error(err)
			}
			if fake.BlobReads(fooDigest) != 0 {
				t.Errorf("Expected no reads for foo since request is cancelled, got %v.", fake.BlobReads(fooDigest))
			}
			close(wait)
		})
	}
}

func TestBatchDownloadBlobsCompressed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	fakeCAS := fakes.NewCAS()
	defer listener.Close()
	server := grpc.NewServer()
	regrpc.RegisterContentAddressableStorageServer(server, fakeCAS)
	go server.Serve(listener)
	defer server.Stop()
	c, err := client.NewClient(ctx, instance, client.DialParams{
		Service:    listener.Addr().String(),
		NoSecurity: true,
	}, client.StartupCapabilities(false))
	if err != nil {
		t.Fatalf("Error connecting to server: %v", err)
	}
	defer c.Close()

	fooDigest := fakeCAS.Put([]byte("foo"))
	barDigest := fakeCAS.Put([]byte("bar"))
	digests := []digest.Digest{fooDigest, barDigest}
	client.UseBatchCompression(true).Apply(c)

	wantBlobs := map[digest.Digest]client.CompressedBlobInfo{
		fooDigest: client.CompressedBlobInfo{
			CompressedSize: 16,
			Data:           []byte("foo"),
		},
		barDigest: client.CompressedBlobInfo{
			CompressedSize: 16,
			Data:           []byte("bar"),
		},
	}
	gotBlobs, err := c.BatchDownloadBlobsWithStats(ctx, digests)
	if err != nil {
		t.Errorf("client.BatchDownloadBlobs(ctx, digests) failed: %v", err)
	}
	if diff := cmp.Diff(wantBlobs, gotBlobs); diff != "" {
		t.Errorf("client.BatchDownloadBlobs(ctx, digests) had diff (want -> got):\n%s", diff)
	}
}
