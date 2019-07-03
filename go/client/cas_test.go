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
	"github.com/bazelbuild/remote-apis-sdks/go/internal/test/fakes"
	"github.com/golang/protobuf/proto"
	"github.com/google/go-cmp/cmp"
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
	fake := &fakes.Reader{}
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
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	defer listener.Close()
	server := grpc.NewServer()
	fake := fakes.NewCAS()
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

func TestWriteBlobs(t *testing.T) {
	ctx := context.Background()
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	defer listener.Close()
	server := grpc.NewServer()
	fake := fakes.NewCAS()
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
		// present is the blobs already present in the CAS; they're pre-loaded into the fakes.CAS object
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
					fake.Clear()
					for _, blob := range tc.present {
						fake.Put(blob)
					}
					input := make(map[digest.Digest][]byte)
					for _, blob := range tc.input {
						input[digest.NewFromBlob(blob)] = blob
					}

					err := c.WriteBlobs(ctx, input)
					if err != nil {
						t.Fatalf("c.WriteBlobs(ctx, inputs) gave error %s, expected nil", err)
					}

					for _, blob := range tc.present {
						dg := digest.NewFromBlob(blob)
						if fake.GetBlobWrites(dg) > 0 {
							t.Errorf("blob %v with digest %s was uploaded even though it was already present in the CAS", blob, dg)
						}
						// Remove this from the input map so we don't iterate it below.
						delete(input, dg)
					}
					for dg, blob := range input {
						if gotBlob, ok := fake.Get(dg); !ok {
							t.Errorf("blob %v with digest %s was not uploaded, expected it to be present in the CAS", blob, dg)
						} else if !bytes.Equal(blob, gotBlob) {
							t.Errorf("blob digest %s had diff on uploaded blob: want %v, got %v", dg, blob, gotBlob)
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
	fake := fakes.NewCAS()
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
			if fake.GetBatchReqs() != tc.batchReqs {
				t.Errorf("%d requests were made to BatchUpdateBlobs, wanted %d", fake.GetBatchReqs(), tc.batchReqs)
			}
			if fake.GetWriteReqs() != tc.writeReqs {
				t.Errorf("%d requests were made to Write, wanted %d", fake.GetWriteReqs(), tc.writeReqs)
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
	fake := fakes.NewCAS()
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
	tree := &repb.Tree{
		Root:     root,
		Children: []*repb.Directory{dirA, dirB},
	}
	treeBlob, err := proto.Marshal(tree)
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
	wantOutputs := map[string]*client.Output{
		"dir/a/b/foo": &client.Output{Digest: fooDigest, IsExecutable: true},
		"dir/a/bar":   &client.Output{Digest: barDigest},
		"dir/b/foo":   &client.Output{Digest: fooDigest, IsExecutable: true},
		"dir2/b/foo":  &client.Output{Digest: fooDigest, IsExecutable: true},
		"dir2/bar":    &client.Output{Digest: barDigest},
		"foo":         &client.Output{Digest: fooDigest},
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
