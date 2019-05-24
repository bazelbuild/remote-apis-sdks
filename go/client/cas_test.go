package client_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/client"
	"github.com/bazelbuild/remote-apis-sdks/go/digest"
	"github.com/golang/protobuf/proto"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/kylelemons/godebug/pretty"
	"google.golang.org/grpc"

	regrpc "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	bsgrpc "google.golang.org/genproto/googleapis/bytestream"
)

const instance = "instance"

func TestRead(t *testing.T) {
	ctx := context.Background()
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	defer listener.Close()
	server := grpc.NewServer()
	fake := &fakeReader{}
	bsgrpc.RegisterByteStreamServer(server, fake)
	go server.Serve(listener)
	defer server.Stop()
	c, err := client.Dial(ctx, instance, client.DialParams{
		Service:    listener.Addr().String(),
		NoSecurity: true,
	})
	if err != nil {
		t.Fatalf("Error connecting to server: %v", err)
	}
	defer c.Close()

	tests := []struct {
		name   string
		fake   fakeReader
		offset int64
		limit  int64
		want   []byte // If nil, fake.blob is expected by default.
	}{
		{
			name: "empty blob, 10 chunks",
			fake: fakeReader{
				blob:   []byte{},
				chunks: []int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			},
		},
		{
			name: "blob 'foobar', 1 chunk",
			fake: fakeReader{
				blob:   []byte("foobar"),
				chunks: []int{6},
			},
		},
		{
			name: "blob 'foobar', 3 evenly sized chunks",
			fake: fakeReader{
				blob:   []byte("foobar"),
				chunks: []int{2, 2, 2},
			},
		},
		{
			name: "blob 'foobar', 3 unequal chunks",
			fake: fakeReader{
				blob:   []byte("foobar"),
				chunks: []int{1, 3, 2},
			},
		},
		{
			name: "blob 'foobar', 2 chunks with 0-sized chunk between",
			fake: fakeReader{
				blob:   []byte("foobar"),
				chunks: []int{3, 0, 3},
			},
		},
		{
			name: "blob 'foobarbaz', partial read spanning multiple chunks",
			fake: fakeReader{
				blob:   []byte("foobarbaz"),
				chunks: []int{3, 0, 3, 3},
			},
			offset: 2,
			limit:  5,
			want:   []byte("obarb"),
		},
		{
			name: "blob 'foobar', partial read within chunk",
			fake: fakeReader{
				blob:   []byte("foobar"),
				chunks: []int{6},
			},
			offset: 2,
			limit:  3,
			want:   []byte("oba"),
		},
		{
			name: "blob 'foobar', partial read from start",
			fake: fakeReader{
				blob:   []byte("foobar"),
				chunks: []int{3, 3},
			},
			offset: 0,
			limit:  5,
			want:   []byte("fooba"),
		},
		{
			name: "blob 'foobar', partial read with no limit",
			fake: fakeReader{
				blob:   []byte("foobar"),
				chunks: []int{3, 3},
			},
			offset: 2,
			limit:  0,
			want:   []byte("obar"),
		},
		{
			name: "blob 'foobar', partial read with limit extending beyond end of blob",
			fake: fakeReader{
				blob:   []byte("foobar"),
				chunks: []int{3, 3},
			},
			offset: 2,
			limit:  8,
			want:   []byte("obar"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			*fake = tc.fake
			fake.validate(t)

			want := tc.want
			if want == nil {
				want = fake.blob
			}

			if tc.offset == 0 && tc.limit == 0 {
				got, err := c.ReadBlob(ctx, digest.FromBlob(want))
				if err != nil {
					t.Errorf("c.ReadBlob(ctx, digest) gave error %s, want nil", err)
				}
				if diff := cmp.Diff(want, got, cmpopts.EquateEmpty()); diff != "" {
					t.Errorf("c.ReadBlob(ctx, digest) gave diff (-want, +got):\n%s", diff)
				}
			}

			got, err := c.ReadBlobRange(ctx, digest.FromBlob(fake.blob), tc.offset, tc.limit)
			if err != nil {
				t.Errorf("c.ReadBlobRange(ctx, digest, %d, %d) gave error %s, want nil", tc.offset, tc.limit, err)
			}
			if diff := cmp.Diff(want, got, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("c.ReadBlobRange(ctx, digest, %d, %d) gave diff (-want, +got):\n%s", tc.offset, tc.limit, diff)
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
	fake := &fakeWriter{}
	bsgrpc.RegisterByteStreamServer(server, fake)
	go server.Serve(listener)
	defer server.Stop()
	c, err := client.Dial(ctx, instance, client.DialParams{
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
			if fake.err != nil {
				t.Errorf("c.WriteBlob(ctx, blob) caused the server to return error %s (possibly unseen by c)", fake.err)
			}
			if diff := cmp.Diff(tc.blob, fake.buf, cmp.Comparer(bytes.Equal)); diff != "" {
				t.Errorf("c.WriteBlob(ctx, blob) had diff on blobs (-sent, +received):\n%s", diff)
			}
			if diff := cmp.Diff(digest.FromBlob(tc.blob), gotDg); diff != "" {
				t.Errorf("c.WriteBlob(ctx, blob) had diff on digest returned (want -> got):\n%s", diff)
			}
		})
	}
}

func TestMissingBlobs(t *testing.T) {
	ctx := context.Background()
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	defer listener.Close()
	server := grpc.NewServer()
	fake := &fakeCAS{}
	regrpc.RegisterContentAddressableStorageServer(server, fake)
	go server.Serve(listener)
	defer server.Stop()
	c, err := client.Dial(ctx, instance, client.DialParams{
		Service:    listener.Addr().String(),
		NoSecurity: true,
	})
	if err != nil {
		t.Fatalf("Error connecting to server: %v", err)
	}
	defer c.Close()

	tests := []struct {
		name string
		// present is the digests present in the CAS.
		present []*repb.Digest
		// input is the digests given to MissingBlobs.
		input []*repb.Digest
		// want is the returned list of digests.
		want []*repb.Digest
	}{
		{
			name:    "none present",
			present: nil,
			input: []*repb.Digest{
				digest.FromBlob([]byte("foo")),
				digest.FromBlob([]byte("bar")),
				digest.FromBlob([]byte("baz")),
			},
			want: []*repb.Digest{
				digest.FromBlob([]byte("foo")),
				digest.FromBlob([]byte("bar")),
				digest.FromBlob([]byte("baz")),
			},
		},
		{
			name: "all present",
			present: []*repb.Digest{
				digest.FromBlob([]byte("baz")),
				digest.FromBlob([]byte("foo")),
				digest.FromBlob([]byte("bar")),
			},
			input: []*repb.Digest{
				digest.FromBlob([]byte("foo")),
				digest.FromBlob([]byte("bar")),
				digest.FromBlob([]byte("baz")),
			},
			want: nil,
		},
		{
			name: "some present",
			present: []*repb.Digest{
				digest.FromBlob([]byte("foo")),
				digest.FromBlob([]byte("bar")),
			},
			input: []*repb.Digest{
				digest.FromBlob([]byte("foo")),
				digest.FromBlob([]byte("bar")),
				digest.FromBlob([]byte("baz")),
			},
			want: []*repb.Digest{
				digest.FromBlob([]byte("baz")),
			},
		},
	}

	printCfg := &pretty.Config{Compact: true}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake.blobs = make(map[digest.Key][]byte)
			for _, dg := range tc.present {
				// We don't care about blob content in this test so just include nil values.
				fake.blobs[digest.ToKey(dg)] = nil
			}
			t.Logf("CAS contains digests %s", printCfg.Sprint(tc.present))
			got, err := c.MissingBlobs(ctx, tc.input)
			if err != nil {
				t.Errorf("c.MissingBlobs(ctx, %s) gave error %s, expected nil", printCfg.Sprint(tc.input), err)
			}
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("c.MissingBlobs(ctx, %s) gave diff (want -> got):\n%s", printCfg.Sprint(tc.input), diff)
			}
		})
	}
}

func TestWriteBlobs(t *testing.T) {
	ctx := context.Background()
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	defer listener.Close()
	server := grpc.NewServer()
	fake := &fakeCAS{}
	bsgrpc.RegisterByteStreamServer(server, fake)
	regrpc.RegisterContentAddressableStorageServer(server, fake)
	go server.Serve(listener)
	defer server.Stop()
	c, err := client.Dial(ctx, instance, client.DialParams{
		Service:    listener.Addr().String(),
		NoSecurity: true,
	}, client.ChunkMaxSize(20)) // Use small write chunk size for tests.
	if err != nil {
		t.Fatalf("Error connecting to server: %v", err)
	}
	defer c.Close()

	var thousandBlobs [][]byte
	var halfThousandBlobs [][]byte
	for i := 0; i < 1000; i++ {
		var buf = new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, i)
		thousandBlobs = append(thousandBlobs, buf.Bytes())
		if i%2 == 0 {
			halfThousandBlobs = append(halfThousandBlobs, buf.Bytes())
		}
	}

	tests := []struct {
		name string
		// input is the blobs to try to store; they're converted to a file map by the test
		input [][]byte
		// present is the blobs already present in the CAS; they're pre-loaded into the fakeCAS object
		// and the test verifies no attempt was made to upload them.
		present [][]byte
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
			name:    "1000 blobs heavy concurrency",
			input:   thousandBlobs,
			present: halfThousandBlobs,
		},
	}

	for _, ub := range []client.UseBatchOps{false, true} {
		t.Run(fmt.Sprintf("UsingBatch:%t", ub), func(t *testing.T) {
			ub.Apply(c)
			for _, tc := range tests {
				t.Run(tc.name, func(t *testing.T) {
					fake.blobs = make(map[digest.Key][]byte)
					fake.batchReqs = 0
					fake.writeReqs = 0
					for _, blob := range tc.present {
						// We don't care about blob content in this test so just include nil values.
						fake.blobs[digest.ToKey(digest.FromBlob(blob))] = nil
					}
					input := make(map[digest.Key][]byte)
					for _, blob := range tc.input {
						input[digest.ToKey(digest.FromBlob(blob))] = blob
					}

					err := c.WriteBlobs(ctx, input)
					if err != nil {
						t.Fatalf("c.WriteBlobs(ctx, inputs) gave error %s, expected nil", err)
					}

					for _, blob := range tc.present {
						dg := digest.FromBlob(blob)
						if gotBlob := fake.blobs[digest.ToKey(dg)]; gotBlob != nil {
							t.Errorf("blob %v with digest %s was uploaded even though it was already present in the CAS", blob, digest.ToString(dg))
						}
						// Remove this from the input map so we don't iterate it below.
						delete(input, digest.ToKey(dg))
					}
					for dg, blob := range input {
						if gotBlob, ok := fake.blobs[dg]; !ok {
							t.Errorf("blob %v with digest %s was not uploaded, expected it to be present in the CAS", blob, digest.ToString(digest.FromKey(dg)))
						} else if diff := cmp.Diff(blob, gotBlob); diff != "" {
							t.Errorf("blob %v with digest %s had diff on uploaded blob:\n%s", blob, digest.ToString(digest.FromKey(dg)), diff)
						}
					}
				})
			}
		})
	}
}

func TestWriteBlobsBatching(t *testing.T) {
	ctx := context.Background()
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	defer listener.Close()
	server := grpc.NewServer()
	fake := &fakeCAS{}
	bsgrpc.RegisterByteStreamServer(server, fake)
	regrpc.RegisterContentAddressableStorageServer(server, fake)
	go server.Serve(listener)
	defer server.Stop()
	c, err := client.Dial(ctx, instance, client.DialParams{
		Service:    listener.Addr().String(),
		NoSecurity: true,
	}, client.UseBatchOps(true))
	if err != nil {
		t.Fatalf("Error connecting to server: %v", err)
	}
	defer c.Close()

	const mb = 1024 * 1024
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
			sizes:     []int{client.MaxBatchSz - 1, client.MaxBatchSz - 1, client.MaxBatchSz - 1, 1, 1, 1},
			batchReqs: 3,
			writeReqs: 0,
		},
		{
			name:      "small batches of big blobs",
			sizes:     []int{1*mb + 1, 1*mb + 1, 1*mb + 1, 1*mb + 1, 1*mb + 1, 1*mb + 1, 1*mb + 1},
			batchReqs: 2,
			writeReqs: 1,
		},
		{
			name:      "batch with blob that's too big",
			sizes:     []int{5 * mb, 1 * mb, 1 * mb, 1 * mb},
			batchReqs: 1,
			writeReqs: 1,
		},
		{
			name:      "many small blobs",
			sizes:     []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
			batchReqs: 1,
			writeReqs: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake.blobs = make(map[digest.Key][]byte)
			fake.writeReqs = 0
			fake.batchReqs = 0
			blobs := make(map[digest.Key][]byte)
			for i, sz := range tc.sizes {
				blob := make([]byte, int(sz))
				blob[0] = byte(i) // Ensure blobs are distinct
				blobs[digest.ToKey(digest.FromBlob(blob))] = blob
			}

			err := c.WriteBlobs(ctx, blobs)
			if err != nil {
				t.Fatalf("c.WriteBlobs(ctx, inputs) gave error %s, expected nil", err)
			}

			for k, blob := range blobs {
				dg := digest.FromKey(k)
				if gotBlob, ok := fake.blobs[k]; !ok {
					t.Errorf("blob of size %d with digest %s was not uploaded, expected it to be present in the CAS", dg.SizeBytes, digest.ToString(dg))
				} else if diff := cmp.Diff(blob, gotBlob, cmp.Comparer(bytes.Equal)); diff != "" {
					t.Errorf("blob of size %d with digest %s had diff on uploaded blob:\n%s", dg.SizeBytes, digest.ToString(dg), diff)
				}
			}
			if fake.batchReqs != tc.batchReqs {
				t.Errorf("%d requests were made to BatchUpdateBlobs, wanted %d", fake.batchReqs, tc.batchReqs)
			}
			if fake.writeReqs != tc.writeReqs {
				t.Errorf("%d requests were made to Write, wanted %d", fake.writeReqs, tc.writeReqs)
			}
		})
	}
}

func TestFlattenActionOutputs(t *testing.T) {
	ctx := context.Background()
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	defer listener.Close()
	server := grpc.NewServer()
	fake := &fakeCAS{}
	bsgrpc.RegisterByteStreamServer(server, fake)
	regrpc.RegisterContentAddressableStorageServer(server, fake)
	go server.Serve(listener)
	defer server.Stop()
	c, err := client.Dial(ctx, instance, client.DialParams{
		Service:    listener.Addr().String(),
		NoSecurity: true,
	}, client.UseBatchOps(true))
	if err != nil {
		t.Fatalf("Error connecting to server: %v", err)
	}
	defer c.Close()

	fooDigest := digest.TestNew("1001", 1)
	barDigest := digest.TestNew("1002", 2)
	dirB := &repb.Directory{
		Files: []*repb.FileNode{
			{Name: "foo", Digest: fooDigest, IsExecutable: true},
		},
	}
	bDigest := digest.TestFromProto(dirB)
	dirA := &repb.Directory{
		Directories: []*repb.DirectoryNode{
			{Name: "b", Digest: bDigest},
		},
		Files: []*repb.FileNode{
			{Name: "bar", Digest: barDigest},
		},
	}
	aDigest := digest.TestFromProto(dirA)
	root := &repb.Directory{
		Directories: []*repb.DirectoryNode{
			{Name: "a", Digest: aDigest},
			{Name: "b", Digest: bDigest},
		},
	}
	tree := &repb.Tree{
		Root:     root,
		Children: []*repb.Directory{dirA, dirB},
	}
	treeBlob, err := proto.Marshal(tree)
	if err != nil {
		t.Errorf("failed marshalling Tree: %s", err)
	}
	treeDigest := digest.FromBlob(treeBlob)
	treeA := &repb.Tree{
		Root:     dirA,
		Children: []*repb.Directory{dirB},
	}
	treeABlob, err := proto.Marshal(treeA)
	if err != nil {
		t.Errorf("failed marshalling Tree: %s", err)
	}
	treeADigest := digest.FromBlob(treeABlob)
	fake.blobs = make(map[digest.Key][]byte)
	fake.blobs[digest.ToKey(treeDigest)] = treeBlob
	fake.blobs[digest.ToKey(treeADigest)] = treeABlob
	ar := &repb.ActionResult{
		OutputFiles: []*repb.OutputFile{
			&repb.OutputFile{Path: "foo", Digest: fooDigest}},
		OutputFileSymlinks: []*repb.OutputSymlink{
			&repb.OutputSymlink{Path: "x/bar", Target: "../dir/a/bar"}},
		OutputDirectorySymlinks: []*repb.OutputSymlink{
			&repb.OutputSymlink{Path: "x/a", Target: "../dir/a"}},
		OutputDirectories: []*repb.OutputDirectory{
			&repb.OutputDirectory{Path: "dir", TreeDigest: treeDigest},
			&repb.OutputDirectory{Path: "dir2", TreeDigest: treeADigest},
		},
	}
	outputs, err := c.FlattenActionOutputs(ctx, ar)
	if err != nil {
		t.Errorf("error in FlattenActionOutputs: %s", err)
	}
	wantOutputs := map[string]*client.Output{
		"dir/a/b/foo": &client.Output{Digest: digest.ToKey(fooDigest), IsExecutable: true},
		"dir/a/bar":   &client.Output{Digest: digest.ToKey(barDigest)},
		"dir/b/foo":   &client.Output{Digest: digest.ToKey(fooDigest), IsExecutable: true},
		"dir2/b/foo":  &client.Output{Digest: digest.ToKey(fooDigest), IsExecutable: true},
		"dir2/bar":    &client.Output{Digest: digest.ToKey(barDigest)},
		"foo":         &client.Output{Digest: digest.ToKey(fooDigest)},
		"x/a":         &client.Output{SymlinkTarget: "../dir/a"},
		"x/bar":       &client.Output{SymlinkTarget: "../dir/a/bar"},
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
