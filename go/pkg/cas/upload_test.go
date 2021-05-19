package cas

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/fakes"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

func TestFS(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tmpDir := t.TempDir()
	putFile(t, filepath.Join(tmpDir, "root", "a"), "a")
	aItem := uploadItemFromBlob(filepath.Join(tmpDir, "root", "a"), []byte("a"))

	putFile(t, filepath.Join(tmpDir, "root", "b"), "b")
	bItem := uploadItemFromBlob(filepath.Join(tmpDir, "root", "b"), []byte("b"))

	putFile(t, filepath.Join(tmpDir, "root", "subdir", "c"), "c")
	cItem := uploadItemFromBlob(filepath.Join(tmpDir, "root", "subdir", "c"), []byte("c"))

	subdirItem := uploadItemFromDirMsg(filepath.Join(tmpDir, "root", "subdir"), &repb.Directory{
		Files: []*repb.FileNode{{
			Name:   "c",
			Digest: cItem.Digest,
		}},
	})
	rootItem := uploadItemFromDirMsg(filepath.Join(tmpDir, "root"), &repb.Directory{
		Files: []*repb.FileNode{
			{Name: "a", Digest: aItem.Digest},
			{Name: "b", Digest: bItem.Digest},
		},
		Directories: []*repb.DirectoryNode{
			{Name: "subdir", Digest: subdirItem.Digest},
		},
	})

	putFile(t, filepath.Join(tmpDir, "medium-dir", "medium"), "medium")
	mediumItem := uploadItemFromBlob(filepath.Join(tmpDir, "medium-dir", "medium"), []byte("medium"))
	mediumDirItem := uploadItemFromDirMsg(filepath.Join(tmpDir, "medium-dir"), &repb.Directory{
		Files: []*repb.FileNode{{
			Name:   "medium",
			Digest: mediumItem.Digest,
		}},
	})

	putSymlink(t, filepath.Join(tmpDir, "with-symlinks", "file"), filepath.Join("..", "root", "a"))
	putSymlink(t, filepath.Join(tmpDir, "with-symlinks", "dir"), filepath.Join("..", "root", "subdir"))
	withSymlinksItemPreserved := uploadItemFromDirMsg(filepath.Join(tmpDir, "with-symlinks"), &repb.Directory{
		Symlinks: []*repb.SymlinkNode{
			{
				Name:   "file",
				Target: "../root/a",
			},
			{
				Name:   "dir",
				Target: "../root/subdir",
			},
		},
	})

	withSymlinksItemNotPreserved := uploadItemFromDirMsg(filepath.Join(tmpDir, "with-symlinks"), &repb.Directory{
		Files: []*repb.FileNode{
			{Name: "a", Digest: aItem.Digest},
		},
		Directories: []*repb.DirectoryNode{
			{Name: "subdir", Digest: subdirItem.Digest},
		},
	})

	putSymlink(t, filepath.Join(tmpDir, "with-dangling-symlink", "dangling"), "non-existent")
	withDanglingSymlinksItem := uploadItemFromDirMsg(filepath.Join(tmpDir, "with-dangling-symlink"), &repb.Directory{
		Symlinks: []*repb.SymlinkNode{
			{Name: "dangling", Target: "non-existent"},
		},
	})

	tests := []struct {
		desc                string
		inputs              []*UploadInput
		wantScheduledChecks []*uploadItem
		wantErr             error
		opt                 UploadOptions
	}{
		{
			desc:                "root",
			inputs:              []*UploadInput{{Path: filepath.Join(tmpDir, "root")}},
			wantScheduledChecks: []*uploadItem{rootItem, aItem, bItem, subdirItem, cItem},
		},
		{
			desc:   "root-without-a-using-callback",
			inputs: []*UploadInput{{Path: filepath.Join(tmpDir, "root")}},
			opt: UploadOptions{
				Prelude: func(absPath string, mode os.FileMode) error {
					if filepath.Base(absPath) == "a" {
						return ErrSkip
					}
					return nil
				},
			},
			wantScheduledChecks: []*uploadItem{
				uploadItemFromDirMsg(filepath.Join(tmpDir, "root"), &repb.Directory{
					Files: []*repb.FileNode{
						{Name: "b", Digest: bItem.Digest},
					},
					Directories: []*repb.DirectoryNode{
						{Name: "subdir", Digest: subdirItem.Digest},
					},
				}),
				bItem,
				subdirItem,
				cItem,
			},
		},
		{
			desc: "root-without-b-using-exclude",
			inputs: []*UploadInput{{
				Path:        filepath.Join(tmpDir, "root"),
				PathExclude: regexp.MustCompile(`[/\\]b$`),
			}},
			wantScheduledChecks: []*uploadItem{
				uploadItemFromDirMsg(filepath.Join(tmpDir, "root"), &repb.Directory{
					Files: []*repb.FileNode{
						{Name: "a", Digest: aItem.Digest},
					},
					Directories: []*repb.DirectoryNode{
						{Name: "subdir", Digest: subdirItem.Digest},
					},
				}),
				aItem,
				subdirItem,
				cItem,
			},
		},
		{
			desc: "same-regular-file-is-read-only-once",
			// The two regexps below do not exclude anything.
			// This test ensures that same files aren't checked twice.
			inputs: []*UploadInput{
				{
					Path:        filepath.Join(tmpDir, "root"),
					PathExclude: regexp.MustCompile(`1$`),
				},
				{
					Path:        filepath.Join(tmpDir, "root"),
					PathExclude: regexp.MustCompile(`2$`),
				},
			},
			// Directories are checked twice, but files are checked only once.
			wantScheduledChecks: []*uploadItem{rootItem, rootItem, aItem, bItem, subdirItem, subdirItem, cItem},
		},
		{
			desc:   "root-without-subdir",
			inputs: []*UploadInput{{Path: filepath.Join(tmpDir, "root")}},
			opt: UploadOptions{
				Prelude: func(absPath string, mode os.FileMode) error {
					if strings.Contains(absPath, "subdir") {
						return ErrSkip
					}
					return nil
				},
			},
			wantScheduledChecks: []*uploadItem{
				uploadItemFromDirMsg(filepath.Join(tmpDir, "root"), &repb.Directory{
					Files: []*repb.FileNode{
						{Name: "a", Digest: aItem.Digest},
						{Name: "b", Digest: bItem.Digest},
					},
				}),
				aItem,
				bItem,
			},
		},
		{
			desc:                "blob",
			inputs:              []*UploadInput{{Content: []byte("foo")}},
			wantScheduledChecks: []*uploadItem{uploadItemFromBlob("digest 2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae/3", []byte("foo"))},
		},
		{
			desc:                "medium",
			inputs:              []*UploadInput{{Path: filepath.Join(tmpDir, "medium-dir")}},
			wantScheduledChecks: []*uploadItem{mediumDirItem, mediumItem},
		},
		{
			desc:                "symlinks-preserved",
			opt:                 UploadOptions{PreserveSymlinks: true},
			inputs:              []*UploadInput{{Path: filepath.Join(tmpDir, "with-symlinks")}},
			wantScheduledChecks: []*uploadItem{aItem, subdirItem, cItem, withSymlinksItemPreserved},
		},
		{
			desc:                "symlinks-not-preserved",
			inputs:              []*UploadInput{{Path: filepath.Join(tmpDir, "with-symlinks")}},
			wantScheduledChecks: []*uploadItem{aItem, subdirItem, cItem, withSymlinksItemNotPreserved},
		},
		{
			desc:    "dangling-symlinks-disallow",
			inputs:  []*UploadInput{{Path: filepath.Join(tmpDir, "with-dangling-symlinks")}},
			wantErr: os.ErrNotExist,
		},
		{
			desc:                "dangling-symlinks-allow",
			opt:                 UploadOptions{PreserveSymlinks: true, AllowDanglingSymlinks: true},
			inputs:              []*UploadInput{{Path: filepath.Join(tmpDir, "with-dangling-symlink")}},
			wantScheduledChecks: []*uploadItem{withDanglingSymlinksItem},
		},
		{
			desc: "dangling-symlink-via-filtering",
			opt:  UploadOptions{PreserveSymlinks: true},
			inputs: []*UploadInput{{
				Path:        filepath.Join(tmpDir, "with-symlinks"),
				PathExclude: regexp.MustCompile("root"),
			}},
			wantErr: ErrFilteredSymlinkTarget,
		},
		{
			desc: "dangling-symlink-via-filtering-allow",
			opt:  UploadOptions{PreserveSymlinks: true, AllowDanglingSymlinks: true},
			inputs: []*UploadInput{{
				Path:        filepath.Join(tmpDir, "with-symlinks"),
				PathExclude: regexp.MustCompile("root"),
			}},
			wantScheduledChecks: []*uploadItem{withSymlinksItemPreserved},
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			var mu sync.Mutex
			var gotScheduledChecks []*uploadItem

			client := &Client{
				Config: DefaultClientConfig(),
				testScheduleCheck: func(ctx context.Context, item *uploadItem) error {
					mu.Lock()
					defer mu.Unlock()
					gotScheduledChecks = append(gotScheduledChecks, item)
					return nil
				},
			}
			client.Config.SmallFileThreshold = 5
			client.Config.LargeFileThreshold = 10
			client.init()

			_, err := client.Upload(ctx, tc.opt, inputChanFrom(tc.inputs...))
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("error mismatch: want %q, got %q", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}

			sort.Slice(gotScheduledChecks, func(i, j int) bool {
				return gotScheduledChecks[i].Title < gotScheduledChecks[j].Title
			})
			if diff := cmp.Diff(tc.wantScheduledChecks, gotScheduledChecks, cmp.Comparer(compareUploadItems)); diff != "" {
				t.Errorf("unexpected scheduled checks (-want +got):\n%s", diff)
			}
		})
	}
}

func TestSmallBlobs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	var mu sync.Mutex
	var gotDigestChecks []*repb.Digest
	var gotDigestCheckRequestSizes []int
	var gotUploadBlobReqs []*repb.BatchUpdateBlobsRequest_Request
	failCBlob := true // blob "c" is uploaded below.
	cas := &fakeCAS{
		findMissingBlobs: func(ctx context.Context, in *repb.FindMissingBlobsRequest, opts ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
			mu.Lock()
			defer mu.Unlock()
			gotDigestChecks = append(gotDigestChecks, in.BlobDigests...)
			gotDigestCheckRequestSizes = append(gotDigestCheckRequestSizes, len(in.BlobDigests))
			return &repb.FindMissingBlobsResponse{MissingBlobDigests: in.BlobDigests[:1]}, nil
		},
		batchUpdateBlobs: func(ctx context.Context, in *repb.BatchUpdateBlobsRequest, opts ...grpc.CallOption) (*repb.BatchUpdateBlobsResponse, error) {
			mu.Lock()
			defer mu.Unlock()

			gotUploadBlobReqs = append(gotUploadBlobReqs, in.Requests...)

			res := &repb.BatchUpdateBlobsResponse{
				Responses: make([]*repb.BatchUpdateBlobsResponse_Response, len(in.Requests)),
			}
			for i, r := range in.Requests {
				res.Responses[i] = &repb.BatchUpdateBlobsResponse_Response{Digest: r.Digest}
				if string(r.Data) == "c" && failCBlob {
					res.Responses[i].Status = status.New(codes.Internal, "internal retrible error").Proto()
					failCBlob = false
				}
			}
			return res, nil
		},
	}
	client := &Client{
		InstanceName: "projects/p/instances/i",
		Config:       DefaultClientConfig(),
		cas:          cas,
	}
	client.Config.FindMissingBlobs.MaxItems = 2
	client.init()

	inputC := inputChanFrom(
		&UploadInput{Content: []byte("a")},
		&UploadInput{Content: []byte("b")},
		&UploadInput{Content: []byte("c")},
		&UploadInput{Content: []byte("d")},
	)
	if _, err := client.Upload(ctx, UploadOptions{}, inputC); err != nil {
		t.Fatalf("failed to upload: %s", err)
	}

	wantDigestChecks := []*repb.Digest{
		{Hash: "18ac3e7343f016890c510e93f935261169d9e3f565436429830faf0934f4f8e4", SizeBytes: 1},
		{Hash: "2e7d2c03a9507ae265ecf5b5356885a53393a2029d241394997265a1a25aefc6", SizeBytes: 1},
		{Hash: "3e23e8160039594a33894f6564e1b1348bbd7a0088d42c4acb73eeaed59c009d", SizeBytes: 1},
		{Hash: "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb", SizeBytes: 1},
	}
	sort.Slice(gotDigestChecks, func(i, j int) bool {
		return gotDigestChecks[i].Hash < gotDigestChecks[j].Hash
	})
	if diff := cmp.Diff(wantDigestChecks, gotDigestChecks, cmp.Comparer(proto.Equal)); diff != "" {
		t.Error(diff)
	}
	if diff := cmp.Diff([]int{2, 2}, gotDigestCheckRequestSizes); diff != "" {
		t.Error(diff)
	}

	wantUploadBlobsReqs := []*repb.BatchUpdateBlobsRequest_Request{
		{
			Digest: &repb.Digest{Hash: "2e7d2c03a9507ae265ecf5b5356885a53393a2029d241394997265a1a25aefc6", SizeBytes: 1},
			Data:   []byte("c"),
		},
		// We expect two requets for c because the first one failed transiently.
		{
			Digest: &repb.Digest{Hash: "2e7d2c03a9507ae265ecf5b5356885a53393a2029d241394997265a1a25aefc6", SizeBytes: 1},
			Data:   []byte("c"),
		},
		{
			Digest: &repb.Digest{Hash: "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb", SizeBytes: 1},
			Data:   []byte("a"),
		},
	}
	sort.Slice(gotUploadBlobReqs, func(i, j int) bool {
		return gotUploadBlobReqs[i].Digest.Hash < gotUploadBlobReqs[j].Digest.Hash
	})
	if diff := cmp.Diff(wantUploadBlobsReqs, gotUploadBlobReqs, cmp.Comparer(proto.Equal)); diff != "" {
		t.Error(diff)
	}
}

func TestStreaming(t *testing.T) {
	// TODO(nodir): add tests for retries.
	t.Parallel()
	ctx := context.Background()

	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()
	conn, err := e.Server.NewClientConn(ctx)
	if err != nil {
		t.Fatal(err)
	}

	cfg := DefaultClientConfig()
	cfg.BatchUpdateBlobs.MaxSizeBytes = 1
	cfg.ByteStreamWrite.MaxSizeBytes = 2 // force multiple requests in a stream
	cfg.SmallFileThreshold = 2
	cfg.LargeFileThreshold = 3
	cfg.CompressedBytestreamThreshold = 7 // between medium and large
	client, err := NewClientWithConfig(ctx, conn, "instance", cfg)
	if err != nil {
		t.Fatal(err)
	}

	largeFilePath := filepath.Join(t.TempDir(), "testdata", "large")
	putFile(t, largeFilePath, "laaaaaaaaaaarge")

	inputC := inputChanFrom(
		&UploadInput{Content: []byte("medium")}, // large blob
		&UploadInput{Path: largeFilePath},       // large file
	)
	gotStats, err := client.Upload(ctx, UploadOptions{}, inputC)
	if err != nil {
		t.Fatalf("failed to upload: %s", err)
	}

	cas := e.Server.CAS
	if cas.WriteReqs() != 2 {
		t.Errorf("want 2 write requests, got %d", cas.WriteReqs())
	}

	blobDigest := digest.Digest{Hash: "c082456a7766e23a18db084cd34b6ff510baef506548b897cc80e9b7d3e121c8", Size: 6}
	if got := cas.BlobWrites(blobDigest); got != 1 {
		t.Errorf("want 1 write of %s, got %d", blobDigest, got)
	}

	fileDigest := digest.Digest{Hash: "71944dd83e7e86354c3a9284e299e0d76c0b1108be62c8e7cefa72adf22128bf", Size: 15}
	if got := cas.BlobWrites(fileDigest); got != 1 {
		t.Errorf("want 1 write of %s, got %d", fileDigest, got)
	}

	wantStats := &TransferStats{
		CacheMisses: DigestStat{Digests: 2, Bytes: 21},
		Streamed:    DigestStat{Digests: 2, Bytes: 21},
	}
	if diff := cmp.Diff(wantStats, gotStats); diff != "" {
		t.Errorf("unexpected stats (-want +got):\n%s", diff)
	}

	// Upload the large file again.
	if _, err := client.Upload(ctx, UploadOptions{}, inputChanFrom(&UploadInput{Path: largeFilePath})); err != nil {
		t.Fatalf("failed to upload: %s", err)
	}
}

func compareUploadItems(x, y *uploadItem) bool {
	return x.Title == y.Title &&
		proto.Equal(x.Digest, y.Digest) &&
		((x.Open == nil && y.Open == nil) || cmp.Equal(mustReadAll(x), mustReadAll(y)))
}

func mustReadAll(item *uploadItem) []byte {
	data, err := item.ReadAll()
	if err != nil {
		panic(err)
	}
	return data
}

func inputChanFrom(inputs ...*UploadInput) chan *UploadInput {
	inputC := make(chan *UploadInput, len(inputs))
	for _, in := range inputs {
		inputC <- in
	}
	close(inputC)
	return inputC
}

type fakeCAS struct {
	repb.ContentAddressableStorageClient
	findMissingBlobs func(ctx context.Context, in *repb.FindMissingBlobsRequest, opts ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error)
	batchUpdateBlobs func(ctx context.Context, in *repb.BatchUpdateBlobsRequest, opts ...grpc.CallOption) (*repb.BatchUpdateBlobsResponse, error)
}

func (c *fakeCAS) FindMissingBlobs(ctx context.Context, in *repb.FindMissingBlobsRequest, opts ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
	return c.findMissingBlobs(ctx, in, opts...)
}

func (c *fakeCAS) BatchUpdateBlobs(ctx context.Context, in *repb.BatchUpdateBlobsRequest, opts ...grpc.CallOption) (*repb.BatchUpdateBlobsResponse, error) {
	return c.batchUpdateBlobs(ctx, in, opts...)
}

func putFile(t *testing.T, path, contents string) {
	if err := os.MkdirAll(filepath.Dir(path), 0777); err != nil {
		t.Fatal(err)
	}
	if err := ioutil.WriteFile(path, []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
}

func putSymlink(t *testing.T, path, target string) {
	if err := os.MkdirAll(filepath.Dir(path), 0777); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
}
