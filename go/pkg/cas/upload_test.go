package cas

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/fakes"
	// Redundant imports are required for the google3 mirror. Aliases should not be changed.
	regrpc "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
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

	putFile(t, filepath.Join(tmpDir, "root", "subdir", "d"), "d")
	dItem := uploadItemFromBlob(filepath.Join(tmpDir, "root", "subdir", "d"), []byte("d"))

	subdirItem := uploadItemFromDirMsg(filepath.Join(tmpDir, "root", "subdir"), &repb.Directory{
		Files: []*repb.FileNode{
			{
				Name:   "c",
				Digest: cItem.Digest,
			},
			{
				Name:   "d",
				Digest: dItem.Digest,
			},
		},
	})
	subdirWithoutDItem := uploadItemFromDirMsg(filepath.Join(tmpDir, "root", "subdir"), &repb.Directory{
		Files: []*repb.FileNode{
			{
				Name:   "c",
				Digest: cItem.Digest,
			},
		},
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
	rootWithoutAItem := uploadItemFromDirMsg(filepath.Join(tmpDir, "root"), &repb.Directory{
		Files: []*repb.FileNode{
			{Name: "b", Digest: bItem.Digest},
		},
		Directories: []*repb.DirectoryNode{
			{Name: "subdir", Digest: subdirItem.Digest},
		},
	})
	rootWithoutSubdirItem := uploadItemFromDirMsg(filepath.Join(tmpDir, "root"), &repb.Directory{
		Files: []*repb.FileNode{
			{Name: "a", Digest: aItem.Digest},
			{Name: "b", Digest: bItem.Digest},
		},
	})
	rootWithoutDItem := uploadItemFromDirMsg(filepath.Join(tmpDir, "root"), &repb.Directory{
		Files: []*repb.FileNode{
			{Name: "a", Digest: aItem.Digest},
			{Name: "b", Digest: bItem.Digest},
		},
		Directories: []*repb.DirectoryNode{
			{Name: "subdir", Digest: subdirWithoutDItem.Digest},
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

	digSlice := func(items ...*uploadItem) []digest.Digest {
		ret := make([]digest.Digest, len(items))
		for i, item := range items {
			ret[i] = digest.NewFromProtoUnvalidated(item.Digest)
		}
		return ret
	}

	tests := []struct {
		desc                string
		inputs              []*UploadInput
		wantDigests         []digest.Digest
		wantScheduledChecks []*uploadItem
		wantErr             error
		opt                 UploadOptions
	}{
		{
			desc:                "root",
			inputs:              []*UploadInput{{Path: filepath.Join(tmpDir, "root")}},
			wantDigests:         digSlice(rootItem),
			wantScheduledChecks: []*uploadItem{rootItem, aItem, bItem, subdirItem, cItem, dItem},
		},
		{
			desc:        "root-without-a-using-callback",
			inputs:      []*UploadInput{{Path: filepath.Join(tmpDir, "root")}},
			wantDigests: digSlice(rootWithoutAItem),
			opt: UploadOptions{
				Prelude: func(absPath string, mode os.FileMode) error {
					if filepath.Base(absPath) == "a" {
						return ErrSkip
					}
					return nil
				},
			},
			wantScheduledChecks: []*uploadItem{rootWithoutAItem, bItem, subdirItem, cItem, dItem},
		},
		{
			desc:                "root-without-a-using-allowlist",
			inputs:              []*UploadInput{{Path: filepath.Join(tmpDir, "root"), Allowlist: []string{"b", "subdir"}}},
			wantDigests:         digSlice(rootWithoutAItem),
			wantScheduledChecks: []*uploadItem{rootWithoutAItem, bItem, subdirItem, cItem, dItem},
		},
		{
			desc:                "root-without-subdir-using-allowlist",
			inputs:              []*UploadInput{{Path: filepath.Join(tmpDir, "root"), Allowlist: []string{"a", "b"}}},
			wantDigests:         digSlice(rootWithoutSubdirItem),
			wantScheduledChecks: []*uploadItem{rootWithoutSubdirItem, aItem, bItem},
		},
		{
			desc:                "root-without-d-using-allowlist",
			inputs:              []*UploadInput{{Path: filepath.Join(tmpDir, "root"), Allowlist: []string{"a", "b", filepath.Join("subdir", "c")}}},
			wantDigests:         digSlice(rootWithoutDItem),
			wantScheduledChecks: []*uploadItem{rootWithoutDItem, aItem, bItem, subdirWithoutDItem, cItem},
		},
		{
			desc: "root-without-b-using-exclude",
			inputs: []*UploadInput{{
				Path:    filepath.Join(tmpDir, "root"),
				Exclude: regexp.MustCompile(`[/\\]a$`),
			}},
			wantDigests:         digSlice(rootWithoutAItem),
			wantScheduledChecks: []*uploadItem{rootWithoutAItem, bItem, subdirItem, cItem, dItem},
		},
		{
			desc: "same-regular-file-is-read-only-once",
			// The two regexps below do not exclude anything.
			// This test ensures that same files aren't checked twice.
			inputs: []*UploadInput{
				{
					Path:    filepath.Join(tmpDir, "root"),
					Exclude: regexp.MustCompile(`1$`),
				},
				{
					Path:    filepath.Join(tmpDir, "root"),
					Exclude: regexp.MustCompile(`2$`),
				},
			},
			// OnDigest is called for each UploadItem separately.
			wantDigests: digSlice(rootItem, rootItem),
			// Directories are checked twice, but files are checked only once.
			wantScheduledChecks: []*uploadItem{rootItem, rootItem, aItem, bItem, subdirItem, subdirItem, cItem, dItem},
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
			wantDigests:         digSlice(rootWithoutSubdirItem),
			wantScheduledChecks: []*uploadItem{rootWithoutSubdirItem, aItem, bItem},
		},
		{
			desc:                "medium",
			inputs:              []*UploadInput{{Path: filepath.Join(tmpDir, "medium-dir")}},
			wantDigests:         digSlice(mediumDirItem),
			wantScheduledChecks: []*uploadItem{mediumDirItem, mediumItem},
		},
		{
			desc:                "symlinks-preserved",
			opt:                 UploadOptions{PreserveSymlinks: true},
			inputs:              []*UploadInput{{Path: filepath.Join(tmpDir, "with-symlinks")}},
			wantDigests:         digSlice(withSymlinksItemPreserved),
			wantScheduledChecks: []*uploadItem{withSymlinksItemPreserved},
		},
		{
			desc:                "symlinks-not-preserved",
			inputs:              []*UploadInput{{Path: filepath.Join(tmpDir, "with-symlinks")}},
			wantDigests:         digSlice(withSymlinksItemNotPreserved),
			wantScheduledChecks: []*uploadItem{aItem, subdirItem, cItem, dItem, withSymlinksItemNotPreserved},
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
			wantDigests:         digSlice(withDanglingSymlinksItem),
			wantScheduledChecks: []*uploadItem{withDanglingSymlinksItem},
		},
		{
			desc: "dangling-symlink-via-filtering",
			opt:  UploadOptions{PreserveSymlinks: true},
			inputs: []*UploadInput{{
				Path:    filepath.Join(tmpDir, "with-symlinks"),
				Exclude: regexp.MustCompile("root"),
			}},
			wantDigests:         digSlice(withSymlinksItemPreserved),
			wantScheduledChecks: []*uploadItem{withSymlinksItemPreserved},
		},
		{
			desc: "dangling-symlink-via-filtering-allow",
			opt:  UploadOptions{PreserveSymlinks: true, AllowDanglingSymlinks: true},
			inputs: []*UploadInput{{
				Path:    filepath.Join(tmpDir, "with-symlinks"),
				Exclude: regexp.MustCompile("root"),
			}},
			wantDigests:         digSlice(withSymlinksItemPreserved),
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

			_, err := client.Upload(ctx, tc.opt, uploadInputChanFrom(tc.inputs...))
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

			gotDigests := make([]digest.Digest, 0, len(tc.inputs))
			for _, in := range tc.inputs {
				dig, err := in.Digest(".")
				if err != nil {
					t.Errorf("UploadResult.Digest(%#v) failed: %s", in.Path, err)
				} else {
					gotDigests = append(gotDigests, dig)
				}
			}
			if diff := cmp.Diff(tc.wantDigests, gotDigests); diff != "" {
				t.Errorf("unexpected digests (-want +got):\n%s", diff)
			}
		})
	}
}

func TestDigest(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tmpDir := t.TempDir()
	putFile(t, filepath.Join(tmpDir, "root", "a"), "a")
	putFile(t, filepath.Join(tmpDir, "root", "b"), "b")
	putFile(t, filepath.Join(tmpDir, "root", "subdir", "c"), "c")
	putFile(t, filepath.Join(tmpDir, "root", "subdir", "d"), "d")

	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()
	conn, err := e.Server.NewClientConn(ctx)
	if err != nil {
		t.Fatal(err)
	}

	client, err := NewClientWithConfig(ctx, conn, "instance", DefaultClientConfig())
	if err != nil {
		t.Fatal(err)
	}

	inputs := []struct {
		input       *UploadInput
		wantDigests map[string]digest.Digest
	}{
		{
			input: &UploadInput{
				Path:      filepath.Join(tmpDir, "root"),
				Allowlist: []string{"a", "b", filepath.Join("subdir", "c")},
			},
			wantDigests: map[string]digest.Digest{
				".":      {Hash: "9a0af914385de712675cd780ae2dcb5e17b8943dc62cf9fc6fbf8ccd6f8c940d", Size: 230},
				"a":      {Hash: "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb", Size: 1},
				"subdir": {Hash: "2d5c8ba78600fcadae65bab790bdf1f6f88278ec4abe1dc3aa7c26e60137dfc8", Size: 75},
			},
		},
		{
			input: &UploadInput{
				Path:      filepath.Join(tmpDir, "root"),
				Allowlist: []string{"a", "b", filepath.Join("subdir", "d")},
			},
			wantDigests: map[string]digest.Digest{
				".":      {Hash: "2ab9cc3c9d504c883a66da62b57eb44fc9ca57abe05e75633b435e017920d8df", Size: 230},
				"a":      {Hash: "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb", Size: 1},
				"subdir": {Hash: "ce33c7475f9ff2f2ee501eafcb2f21825b24a63de6fbabf7fbb886d606a448b9", Size: 75},
			},
		},
	}

	uploadInputs := make([]*UploadInput, len(inputs))
	for i, in := range inputs {
		uploadInputs[i] = in.input
		if in.input.DigestsComputed() == nil {
			t.Fatalf("DigestCopmuted() returned nil")
		}
	}

	if _, err := client.Upload(ctx, UploadOptions{}, uploadInputChanFrom(uploadInputs...)); err != nil {
		t.Fatal(err)
	}

	for i, in := range inputs {
		t.Logf("input %d", i)
		select {
		case <-in.input.DigestsComputed():
			// Good
		case <-time.After(time.Second):
			t.Errorf("Upload succeeded, but DigestsComputed() is not closed")
		}

		for relPath, wantDig := range in.wantDigests {
			gotDig, err := in.input.Digest(relPath)
			if err != nil {
				t.Error(err)
				continue
			}
			if diff := cmp.Diff(gotDig, wantDig); diff != "" {
				t.Errorf("unexpected digest for %s (-want +got):\n%s", relPath, diff)
			}
		}
	}
}
func TestSmallFiles(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	var mu sync.Mutex
	var gotDigestChecks []*repb.Digest
	var gotDigestCheckRequestSizes []int
	var gotUploadBlobReqs []*repb.BatchUpdateBlobsRequest_Request
	var missing []*repb.Digest
	failFirstMissing := true
	cas := &fakeCAS{
		findMissingBlobs: func(ctx context.Context, in *repb.FindMissingBlobsRequest, opts ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
			mu.Lock()
			defer mu.Unlock()
			gotDigestChecks = append(gotDigestChecks, in.BlobDigests...)
			gotDigestCheckRequestSizes = append(gotDigestCheckRequestSizes, len(in.BlobDigests))
			missing = append(missing, in.BlobDigests[0])
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
				if proto.Equal(r.Digest, missing[0]) && failFirstMissing {
					res.Responses[i].Status = status.New(codes.Internal, "internal retrible error").Proto()
					failFirstMissing = false
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

	tmpDir := t.TempDir()
	putFile(t, filepath.Join(tmpDir, "a"), "a")
	putFile(t, filepath.Join(tmpDir, "b"), "b")
	putFile(t, filepath.Join(tmpDir, "c"), "c")
	putFile(t, filepath.Join(tmpDir, "d"), "d")
	inputC := uploadInputChanFrom(
		&UploadInput{Path: filepath.Join(tmpDir, "a")},
		&UploadInput{Path: filepath.Join(tmpDir, "b")},
		&UploadInput{Path: filepath.Join(tmpDir, "c")},
		&UploadInput{Path: filepath.Join(tmpDir, "d")},
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

	if len(missing) != 2 {
		t.Fatalf("want 2 missing, got %d", len(missing))
	}
	var wantUploadBlobsReqs []*repb.BatchUpdateBlobsRequest_Request
	for _, blob := range []string{"a", "b", "c", "d"} {
		blobBytes := []byte(blob)
		req := &repb.BatchUpdateBlobsRequest_Request{Data: blobBytes, Digest: digest.NewFromBlob(blobBytes).ToProto()}
		switch {
		case proto.Equal(req.Digest, missing[0]):
			wantUploadBlobsReqs = append(wantUploadBlobsReqs, req, req)
		case proto.Equal(req.Digest, missing[1]):
			wantUploadBlobsReqs = append(wantUploadBlobsReqs, req)
		}

	}
	sort.Slice(wantUploadBlobsReqs, func(i, j int) bool {
		return wantUploadBlobsReqs[i].Digest.Hash < wantUploadBlobsReqs[j].Digest.Hash
	})
	sort.Slice(gotUploadBlobReqs, func(i, j int) bool {
		return gotUploadBlobReqs[i].Digest.Hash < gotUploadBlobReqs[j].Digest.Hash
	})
	if diff := cmp.Diff(wantUploadBlobsReqs, gotUploadBlobReqs, cmp.Comparer(proto.Equal)); diff != "" {
		t.Error(diff)
	}
}

func TestStreaming(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// TODO(nodir): add tests for retries.

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

	tmpDir := t.TempDir()
	largeFilePath := filepath.Join(tmpDir, "testdata", "large")
	putFile(t, largeFilePath, "laaaaaaaaaaarge")

	res, err := client.Upload(ctx, UploadOptions{}, uploadInputChanFrom(
		&UploadInput{Path: largeFilePath}, // large file
	))
	if err != nil {
		t.Fatalf("failed to upload: %s", err)
	}

	cas := e.Server.CAS
	if cas.WriteReqs() != 1 {
		t.Errorf("want 1 write requests, got %d", cas.WriteReqs())
	}

	fileDigest := digest.Digest{Hash: "71944dd83e7e86354c3a9284e299e0d76c0b1108be62c8e7cefa72adf22128bf", Size: 15}
	if got := cas.BlobWrites(fileDigest); got != 1 {
		t.Errorf("want 1 write of %s, got %d", fileDigest, got)
	}

	wantStats := &TransferStats{
		CacheMisses: DigestStat{Digests: 1, Bytes: 15},
		Streamed:    DigestStat{Digests: 1, Bytes: 15},
	}
	if diff := cmp.Diff(wantStats, &res.Stats); diff != "" {
		t.Errorf("unexpected stats (-want +got):\n%s", diff)
	}

	// Upload the large file again.
	if _, err := client.Upload(ctx, UploadOptions{}, uploadInputChanFrom(&UploadInput{Path: largeFilePath})); err != nil {
		t.Fatalf("failed to upload: %s", err)
	}
}

func TestPartialMerkleTree(t *testing.T) {
	t.Parallel()

	mustDigest := func(m proto.Message) *repb.Digest {
		d, err := digest.NewFromMessage(m)
		if err != nil {
			t.Fatal(err)
		}
		return d.ToProto()
	}

	type testCase struct {
		tree      map[string]*digested
		wantItems []*uploadItem
	}

	test := func(t *testing.T, tc testCase) {
		in := &UploadInput{
			tree:      tc.tree,
			cleanPath: "/",
		}
		gotItems := in.partialMerkleTree()
		sort.Slice(gotItems, func(i, j int) bool {
			return gotItems[i].Title < gotItems[j].Title
		})

		if diff := cmp.Diff(tc.wantItems, gotItems, cmp.Comparer(compareUploadItems)); diff != "" {
			t.Errorf("unexpected digests (-want +got):\n%s", diff)
		}
	}

	t.Run("works", func(t *testing.T) {
		barDigest := digest.NewFromBlob([]byte("bar")).ToProto()
		bazDigest := mustDigest(&repb.Directory{})

		foo := &repb.Directory{
			Files: []*repb.FileNode{{
				Name:   "bar",
				Digest: barDigest,
			}},
			Directories: []*repb.DirectoryNode{{
				Name:   "baz",
				Digest: bazDigest,
			}},
		}

		root := &repb.Directory{
			Directories: []*repb.DirectoryNode{{
				Name:   "foo",
				Digest: mustDigest(foo),
			}},
		}

		test(t, testCase{
			tree: map[string]*digested{
				"foo/bar": {
					dirEntry: &repb.FileNode{
						Name:   "bar",
						Digest: barDigest,
					},
					digest: barDigest,
				},
				"foo/baz": {
					dirEntry: &repb.DirectoryNode{
						Name:   "baz",
						Digest: bazDigest,
					},
					digest: bazDigest,
				},
			},
			wantItems: []*uploadItem{
				uploadItemFromDirMsg("/", root),
				uploadItemFromDirMsg("/foo", foo),
			},
		})
	})

	t.Run("redundant info in the tree", func(t *testing.T) {
		barDigest := mustDigest(&repb.Directory{})
		barNode := &repb.DirectoryNode{
			Name:   "bar",
			Digest: barDigest,
		}
		foo := &repb.Directory{
			Directories: []*repb.DirectoryNode{barNode},
		}
		root := &repb.Directory{
			Directories: []*repb.DirectoryNode{{
				Name:   "foo",
				Digest: mustDigest(foo),
			}},
		}

		test(t, testCase{
			tree: map[string]*digested{
				"foo/bar": {dirEntry: barNode, digest: barDigest},
				// Redundant
				"foo/bar/baz": {}, // content doesn't matter
			},
			wantItems: []*uploadItem{
				uploadItemFromDirMsg("/", root),
				uploadItemFromDirMsg("/foo", foo),
			},
		})
	})

	t.Run("nodes at different levels", func(t *testing.T) {
		barDigest := digest.NewFromBlob([]byte("bar")).ToProto()
		barNode := &repb.FileNode{
			Name:   "bar",
			Digest: barDigest,
		}

		bazDigest := digest.NewFromBlob([]byte("bar")).ToProto()
		bazNode := &repb.FileNode{
			Name:   "baz",
			Digest: bazDigest,
		}

		foo := &repb.Directory{
			Files: []*repb.FileNode{barNode},
		}
		root := &repb.Directory{
			Directories: []*repb.DirectoryNode{{
				Name:   "foo",
				Digest: mustDigest(foo),
			}},
			Files: []*repb.FileNode{bazNode},
		}

		test(t, testCase{
			tree: map[string]*digested{
				"foo/bar": {dirEntry: barNode, digest: barDigest},
				"baz":     {dirEntry: bazNode, digest: bazDigest}, // content doesn't matter
			},
			wantItems: []*uploadItem{
				uploadItemFromDirMsg("/", root),
				uploadItemFromDirMsg("/foo", foo),
			},
		})
	})
}

func TestUploadInputInit(t *testing.T) {
	t.Parallel()
	absPath := filepath.Join(t.TempDir(), "foo")
	testCases := []struct {
		desc               string
		in                 *UploadInput
		dir                bool
		wantCleanAllowlist []string
		wantErrContain     string
	}{
		{
			desc: "valid",
			in:   &UploadInput{Path: absPath},
		},
		{
			desc:           "relative path",
			in:             &UploadInput{Path: "foo"},
			wantErrContain: "not absolute",
		},
		{
			desc:           "relative path",
			in:             &UploadInput{Path: "foo"},
			wantErrContain: "not absolute",
		},
		{
			desc:           "regular file with allowlist",
			in:             &UploadInput{Path: absPath, Allowlist: []string{"x"}},
			wantErrContain: "the Allowlist is not supported for regular files",
		},
		{
			desc:               "not clean allowlisted path",
			in:                 &UploadInput{Path: absPath, Allowlist: []string{"bar/"}},
			dir:                true,
			wantCleanAllowlist: []string{"bar"},
		},
		{
			desc:           "absolute allowlisted path",
			in:             &UploadInput{Path: absPath, Allowlist: []string{"/bar"}},
			dir:            true,
			wantErrContain: "not relative",
		},
		{
			desc:           "parent dir in allowlisted path",
			in:             &UploadInput{Path: absPath, Allowlist: []string{"bar/../.."}},
			dir:            true,
			wantErrContain: "..",
		},
		{
			desc:               "no allowlist",
			in:                 &UploadInput{Path: absPath},
			dir:                true,
			wantCleanAllowlist: []string{"."},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			tmpFilePath := absPath
			if tc.dir {
				tmpFilePath = filepath.Join(absPath, "bar")
			}
			putFile(t, tmpFilePath, "")
			defer os.RemoveAll(absPath)

			err := tc.in.init(&uploader{})
			if tc.wantErrContain == "" {
				if err != nil {
					t.Error(err)
				}
			} else {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrContain) {
					t.Errorf("expected err to contain %q; got %v", tc.wantErrContain, err)
				}
			}

			if len(tc.wantCleanAllowlist) != 0 {
				if diff := cmp.Diff(tc.wantCleanAllowlist, tc.in.cleanAllowlist); diff != "" {
					t.Errorf("unexpected cleanAllowlist (-want +got):\n%s", diff)
				}
			}
		})
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

func uploadInputChanFrom(inputs ...*UploadInput) chan *UploadInput {
	ch := make(chan *UploadInput, len(inputs))
	for _, in := range inputs {
		ch <- in
	}
	close(ch)
	return ch
}

type fakeCAS struct {
	regrpc.ContentAddressableStorageClient
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
	if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
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
