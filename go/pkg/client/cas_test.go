package client_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/chunker"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/client"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/fakes"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/filemetadata"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/portpicker"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/tree"
	"github.com/golang/protobuf/proto"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	bsgrpc "google.golang.org/genproto/googleapis/bytestream"
)

const (
	instance              = "instance"
	defaultCASConcurrency = 50
)

func TestSplitEndpoints(t *testing.T) {
	ctx := context.Background()
	l1, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	port, err := portpicker.PickUnusedPort()
	if err != nil {
		t.Fatalf("Failed picking unused port: %v", err)
	}
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
		Blob:   blob,
		Chunks: []int{6},
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
	})
	if err != nil {
		t.Fatalf("Error connecting to server: %v", err)
	}
	defer c.Close()

	got, err := c.ReadBlob(ctx, digest.NewFromBlob(blob))
	if err != nil {
		t.Errorf("c.ReadBlob(ctx, digest) gave error %s, want nil", err)
	}
	if !bytes.Equal(blob, got) {
		t.Errorf("c.ReadBlob(ctx, digest) gave diff: want %v, got %v", blob, got)
	}
}

func TestReadEmptyBlobDoesNotCallServer(t *testing.T) {
	ctx := context.Background()
	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()
	fake := e.Server.CAS
	c := e.Client.GrpcClient

	got, err := c.ReadBlob(ctx, digest.Empty)
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
	ctx := context.Background()
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	defer listener.Close()
	server := grpc.NewServer()
	fake := &fakes.Reader{}
	bsgrpc.RegisterByteStreamServer(server, fake)
	go server.Serve(listener)
	defer server.Stop()
	c, err := client.NewClient(ctx, instance, client.DialParams{
		Service:    listener.Addr().String(),
		NoSecurity: true,
	})
	if err != nil {
		t.Fatalf("Error connecting to server: %v", err)
	}
	defer c.Close()

	tests := []struct {
		name   string
		fake   fakes.Reader
		offset int64
		limit  int64
		want   []byte // If nil, fake.blob is expected by default.
	}{
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

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			*fake = tc.fake
			fake.Validate(t)

			want := tc.want
			if want == nil {
				want = fake.Blob
			}

			if tc.offset == 0 && tc.limit == 0 {
				got, err := c.ReadBlob(ctx, digest.NewFromBlob(want))
				if err != nil {
					t.Errorf("c.ReadBlob(ctx, digest) gave error %s, want nil", err)
				}
				if !bytes.Equal(want, got) {
					t.Errorf("c.ReadBlob(ctx, digest) gave diff: want %v, got %v", want, got)
				}
			}

			got, err := c.ReadBlobRange(ctx, digest.NewFromBlob(fake.Blob), tc.offset, tc.limit)
			if err != nil {
				t.Errorf("c.ReadBlobRange(ctx, digest, %d, %d) gave error %s, want nil", tc.offset, tc.limit, err)
			}
			if !bytes.Equal(want, got) {
				t.Errorf("c.ReadBlobRange(ctx, digest, %d, %d) gave diff: want %v, got %v", tc.offset, tc.limit, want, got)
			}
		})
	}
}

func TestWrite(t *testing.T) {
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
	}, client.ChunkMaxSize(20)) // Use small write chunk size for tests.
	if err != nil {
		t.Fatalf("Error connecting to server: %v", err)
	}
	defer c.Close()

	tests := []struct {
		name string
		blob []byte
	}{
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

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
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
	ctx := context.Background()
	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()
	fake := e.Server.CAS
	c := e.Client.GrpcClient

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
		t.Run(tc.name, func(t *testing.T) {
			fake.Clear()
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

func TestUpload(t *testing.T) {
	ctx := context.Background()
	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()
	fake := e.Server.CAS
	c := e.Client.GrpcClient
	client.CASConcurrency(defaultCASConcurrency).Apply(c)

	chunkSize := 5
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

	tests := []struct {
		name string
		// input is the blobs to try to store; they're converted to a file map by the test
		input [][]byte
		// present is the blobs already present in the CAS; they're pre-loaded into the fakes.CAS object
		// and the test verifies no attempt was made to upload them.
		present [][]byte
		// concurrency is the CAS concurrency with which we should be uploading the blobs. If not
		// specified, it uses defaultCASConcurrency.
		concurrency client.CASConcurrency
	}{
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
			name:        "2000 blobs heavy concurrency",
			input:       twoThousandBlobs,
			present:     thousandBlobs,
			concurrency: client.CASConcurrency(500),
		},
	}

	for _, ub := range []client.UseBatchOps{false, true} {
		t.Run(fmt.Sprintf("UsingBatch:%t", ub), func(t *testing.T) {
			ub.Apply(c)
			for _, tc := range tests {
				t.Run(tc.name, func(t *testing.T) {
					fake.Clear()
					if tc.concurrency > 0 {
						tc.concurrency.Apply(c)
					}

					present := make(map[digest.Digest]bool)
					for _, blob := range tc.present {
						fake.Put(blob)
						present[digest.NewFromBlob(blob)] = true
					}
					var input []*chunker.Chunker
					for _, blob := range tc.input {
						input = append(input, chunker.NewFromBlob(blob, chunkSize))
					}

					missing, err := c.UploadIfMissing(ctx, input...)
					if err != nil {
						t.Errorf("c.UploadIfMissing(ctx, input) gave error %v, expected nil", err)
					}

					missingSet := make(map[digest.Digest]struct{})
					for _, dg := range missing {
						missingSet[dg] = struct{}{}
					}
					for _, ch := range input {
						dg := ch.Digest()
						blob, err := ch.FullData()
						if err != nil {
							t.Errorf("ch.FullData() returned an error: %v", err)
						}
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
		})
	}
}

func TestWriteBlobsBatching(t *testing.T) {
	ctx := context.Background()
	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()
	fake := e.Server.CAS
	c := e.Client.GrpcClient
	c.MaxBatchSize = 500
	c.MaxBatchDigests = 4
	// Each batch request frame overhead is 13 bytes.
	// A per-blob overhead is 74 bytes.

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
		t.Run(tc.name, func(t *testing.T) {
			fake.Clear()
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
	wantOutputs := map[string]*tree.Output{
		"dir/a/b/foo": &tree.Output{Digest: fooDigest, IsExecutable: true},
		"dir/a/bar":   &tree.Output{Digest: barDigest},
		"dir/b/foo":   &tree.Output{Digest: fooDigest, IsExecutable: true},
		"dir2/b/foo":  &tree.Output{Digest: fooDigest, IsExecutable: true},
		"dir2/bar":    &tree.Output{Digest: barDigest},
		"foo":         &tree.Output{Digest: fooDigest},
		"x/a":         &tree.Output{SymlinkTarget: "../dir/a"},
		"x/bar":       &tree.Output{SymlinkTarget: "../dir/a/bar"},
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
	execRoot, err := ioutil.TempDir("", "DownloadOuts")
	if err != nil {
		t.Fatalf("failed to make temp dir: %v", err)
	}
	defer os.RemoveAll(execRoot)
	err = c.DownloadActionOutputs(ctx, ar, execRoot, cache)
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
			path:             "dir/e1",
			isEmptyDirectory: true,
		},
		{
			path:             "dir/a/e2",
			isEmptyDirectory: true,
		},
		{
			path:         "dir/a/b/foo",
			isExecutable: true,
			contents:     []byte("foo"),
			fileDigest:   &fooDigest,
		},
		{
			path:     "dir/a/bar",
			contents: []byte("bar"),
		},
		{
			path:         "dir/b/foo",
			isExecutable: true,
			contents:     []byte("foo"),
			fileDigest:   &fooDigest,
		},
		{
			path:             "dir2/e2",
			isEmptyDirectory: true,
		},
		{
			path:         "dir2/b/foo",
			isExecutable: true,
			contents:     []byte("foo"),
			fileDigest:   &fooDigest,
		},
		{
			path:     "dir2/bar",
			contents: []byte("bar"),
		},
		{
			path:       "foo",
			contents:   []byte("foo"),
			fileDigest: &fooDigest,
		},
		{
			path:          "x/a",
			symlinkTarget: "../dir/a",
		},
		{
			path:          "x/bar",
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
			files, err := ioutil.ReadDir(path)
			if err != nil {
				t.Errorf("expected %s to be a directory, got error reading directory: %v", path, err)
			}
			if len(files) != 0 {
				t.Errorf("expected %s to be an empty directory, got contents: %v", path, files)
			}
		} else {
			contents, err := ioutil.ReadFile(path)
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

func TestDownloadDirectory(t *testing.T) {
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
	}
	dirBlob, err := proto.Marshal(dir)
	if err != nil {
		t.Fatalf("failed marshalling Tree: %s", err)
	}
	fake.Put(dirBlob)

	d := digest.TestNewFromMessage(dir)
	execRoot := t.TempDir()

	outputs, err := c.DownloadDirectory(ctx, d, execRoot, cache)
	if err != nil {
		t.Errorf("error in DownloadActionOutputs: %s", err)
	}

	if diff := cmp.Diff(outputs, map[string]*tree.Output{"foo": {
		Digest:       fooDigest,
		Path:         "foo",
		IsExecutable: true,
	}}); diff != "" {
		t.Fatalf("DownloadDirectory() mismatch (-want +got):\n%s", diff)
	}

	b, err := ioutil.ReadFile(filepath.Join(execRoot, "foo"))
	if err != nil {
		t.Fatalf("failed to read foo: %s", err)
	}
	if want, got := []byte("foo"), b; !bytes.Equal(want, got) {
		t.Errorf("want %s, got %s", want, got)
	}
}

func TestDownloadActionOutputsBatching(t *testing.T) {
	ctx := context.Background()
	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()
	fake := e.Server.CAS
	c := e.Client.GrpcClient
	c.MaxBatchSize = 500
	c.MaxBatchDigests = 4
	// Each batch request frame overhead is 13 bytes.
	// A per-blob overhead is 74 bytes.

	tests := []struct {
		name      string
		sizes     []int
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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake.Clear()
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
			execRoot, err := ioutil.TempDir("", "DownloadOuts")
			if err != nil {
				t.Fatalf("failed to make temp dir: %v", err)
			}
			defer os.RemoveAll(execRoot)
			err = c.DownloadActionOutputs(ctx, ar, execRoot, filemetadata.NewSingleFlightCache())
			if err != nil {
				t.Errorf("error in DownloadActionOutputs: %s", err)
			}
			for dg, data := range blobs {
				path := filepath.Join(execRoot, fmt.Sprintf("foo_%s", dg))
				contents, err := ioutil.ReadFile(path)
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
	ctx := context.Background()
	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()
	fake := e.Server.CAS
	c := e.Client.GrpcClient
	client.CASConcurrency(defaultCASConcurrency).Apply(c)

	blobs := make(map[digest.Digest][]byte)
	for i := 0; i < 1000; i++ {
		var buf = new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, i)
		blob := buf.Bytes()
		blobs[digest.NewFromBlob(blob)] = blob
	}

	for _, ub := range []client.UseBatchOps{false, true} {
		t.Run(fmt.Sprintf("%sUsingBatch:%t", t.Name(), ub), func(t *testing.T) {
			ub.Apply(c)
			fake.Clear()
			ar := &repb.ActionResult{}
			for dg, blob := range blobs {
				fake.Put(blob)
				name := fmt.Sprintf("foo_%s", dg)
				ar.OutputFiles = append(ar.OutputFiles, &repb.OutputFile{Path: name, Digest: dg.ToProto()})
			}

			execRoot, err := ioutil.TempDir("", "DownloadOuts")
			if err != nil {
				t.Fatalf("failed to make temp dir: %v", err)
			}
			defer os.RemoveAll(execRoot)
			err = c.DownloadActionOutputs(ctx, ar, execRoot, filemetadata.NewSingleFlightCache())
			if err != nil {
				t.Errorf("error in DownloadActionOutputs: %s", err)
			}
			for dg, data := range blobs {
				path := filepath.Join(execRoot, fmt.Sprintf("foo_%s", dg))
				contents, err := ioutil.ReadFile(path)
				if err != nil {
					t.Errorf("error reading from %s: %v", path, err)
				}
				if !bytes.Equal(contents, data) {
					t.Errorf("expected %s to contain %v, got %v", path, contents, data)
				}
			}
			if fake.MaxConcurrency() > defaultCASConcurrency {
				t.Errorf("CAS concurrency %v was higher than max %v", fake.MaxConcurrency(), defaultCASConcurrency)
			}
		})
	}
}

func TestWriteAndReadProto(t *testing.T) {
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
	if err := c.ReadProto(ctx, d, dirB); err != nil {
		t.Errorf("Failed reading proto: %s", err)
	}
	if !proto.Equal(dirA, dirB) {
		t.Errorf("Protos not equal: %s / %s", dirA, dirB)
	}
}

func TestDownloadFiles(t *testing.T) {
	ctx := context.Background()
	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()
	fake := e.Server.CAS
	c := e.Client.GrpcClient

	fooDigest := fake.Put([]byte("foo"))
	barDigest := fake.Put([]byte("bar"))

	execRoot, err := ioutil.TempDir("", "DownloadOuts")
	if err != nil {
		t.Fatalf("failed to make temp dir: %v", err)
	}
	defer os.RemoveAll(execRoot)

	if err := c.DownloadFiles(ctx, execRoot, map[digest.Digest]*tree.Output{
		fooDigest: {Digest: fooDigest, Path: "foo", IsExecutable: true},
		barDigest: {Digest: barDigest, Path: "bar"},
	}); err != nil {
		t.Errorf("Failed to run DownloadFiles: %v", err)
	}

	if b, err := ioutil.ReadFile(filepath.Join(execRoot, "foo")); err != nil {
		t.Errorf("failed to read file: %v", err)
	} else if diff := cmp.Diff(b, []byte("foo")); diff != "" {
		t.Errorf("foo mismatch (-want +got):\n%s", diff)
	}

	if b, err := ioutil.ReadFile(filepath.Join(execRoot, "bar")); err != nil {
		t.Errorf("failed to read file: %v", err)
	} else if diff := cmp.Diff(b, []byte("bar")); diff != "" {
		t.Errorf("foo mismatch (-want +got):\n%s", diff)
	}
}
