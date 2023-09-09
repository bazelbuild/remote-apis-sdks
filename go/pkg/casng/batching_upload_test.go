package casng_test

import (
	"context"
	"io"
	"sort"
	"testing"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/casng"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/impath"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/walker"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/symlinkopts"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	log "github.com/golang/glog"
	"github.com/google/go-cmp/cmp"
	// Redundant imports are required for the google3 mirror. Aliases should not be changed.
	bsgrpc "google.golang.org/genproto/googleapis/bytestream"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	rpcstpb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
)

func TestUpload_Batching(t *testing.T) {
	tests := []struct {
		name         string
		fs           map[string][]byte
		root         string
		ioCfg        casng.IOConfig
		batchRPCCfg  *casng.GRPCConfig
		bsc          *fakeByteStreamClient
		cc           *fakeCAS
		wantStats    casng.Stats
		wantUploaded []digest.Digest
	}{
		{
			name: "cache_hit",
			fs: map[string][]byte{
				"foo.c": []byte("int c;"),
			},
			root:  "foo.c",
			ioCfg: casng.IOConfig{BufferSize: 1},
			bsc: &fakeByteStreamClient{
				write: func(_ context.Context, _ ...grpc.CallOption) (bsgrpc.ByteStream_WriteClient, error) {
					return &fakeByteStreamWriteClient{
						send: func(wr *bspb.WriteRequest) error {
							return io.EOF
						},
						closeAndRecv: func() (*bspb.WriteResponse, error) {
							return &bspb.WriteResponse{}, nil
						},
					}, nil
				},
			},
			cc: &fakeCAS{
				findMissingBlobs: func(ctx context.Context, in *repb.FindMissingBlobsRequest, opts ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
					return &repb.FindMissingBlobsResponse{}, nil
				},
				batchUpdateBlobs: func(ctx context.Context, in *repb.BatchUpdateBlobsRequest, opts ...grpc.CallOption) (*repb.BatchUpdateBlobsResponse, error) {
					return &repb.BatchUpdateBlobsResponse{
						Responses: []*repb.BatchUpdateBlobsResponse_Response{{Digest: in.Requests[0].Digest, Status: &rpcstpb.Status{}}},
					}, nil
				},
			},
			wantStats: casng.Stats{
				BytesRequested:     6,
				LogicalBytesCached: 6,
				InputFileCount:     1,
				CacheHitCount:      1,
				DigestCount:        1,
			},
			wantUploaded: nil,
		},
		{
			name: "batch_single_blob",
			fs: map[string][]byte{
				"foo.c": []byte("int c;"), // 6 bytes
			},
			root:  "foo.c",
			ioCfg: casng.IOConfig{CompressionSizeThreshold: 1024},
			batchRPCCfg: &casng.GRPCConfig{
				ConcurrentCallsLimit: 1,
				ItemsLimit:           1,
				BytesLimit:           1024,
				Timeout:              time.Second,
				BundleTimeout:        time.Millisecond,
				RetryPolicy:          retryNever,
				RetryPredicate:       retry.TransientOnly,
			},
			bsc: &fakeByteStreamClient{
				write: func(_ context.Context, _ ...grpc.CallOption) (bsgrpc.ByteStream_WriteClient, error) {
					var size int64
					return &fakeByteStreamWriteClient{
						send: func(wr *bspb.WriteRequest) error {
							size += int64(len(wr.Data))
							return nil
						},
						closeAndRecv: func() (*bspb.WriteResponse, error) {
							return &bspb.WriteResponse{CommittedSize: size}, nil
						},
					}, nil
				},
			},
			cc: &fakeCAS{
				findMissingBlobs: func(ctx context.Context, in *repb.FindMissingBlobsRequest, opts ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
					return &repb.FindMissingBlobsResponse{MissingBlobDigests: in.BlobDigests}, nil
				},
				batchUpdateBlobs: func(ctx context.Context, in *repb.BatchUpdateBlobsRequest, opts ...grpc.CallOption) (*repb.BatchUpdateBlobsResponse, error) {
					return &repb.BatchUpdateBlobsResponse{
						Responses: []*repb.BatchUpdateBlobsResponse_Response{{Digest: in.Requests[0].Digest, Status: &rpcstpb.Status{}}},
					}, nil
				},
			},
			wantStats: casng.Stats{
				BytesRequested:      6,
				LogicalBytesMoved:   6,
				TotalBytesMoved:     6,
				EffectiveBytesMoved: 6,
				LogicalBytesBatched: 6,
				CacheMissCount:      1,
				BatchedCount:        1,
				InputFileCount:      1,
				DigestCount:         1,
			},
			wantUploaded: []digest.Digest{{Hash: "62f74d0e355efb6101ee13172d05e89592d4aef21ba0e4041584d8653e60c4c3", Size: 6}},
		},
		{
			name: "stream_single_blob",
			fs: map[string][]byte{
				"foo.c": []byte("int c;"), // 6 bytes
			},
			root:  "foo.c",
			ioCfg: casng.IOConfig{CompressionSizeThreshold: 1024},
			batchRPCCfg: &casng.GRPCConfig{
				ConcurrentCallsLimit: 1,
				ItemsLimit:           1,
				BytesLimit:           1,
				Timeout:              time.Second,
				BundleTimeout:        time.Millisecond,
				RetryPolicy:          retryNever,
				RetryPredicate:       retry.TransientOnly,
			},
			bsc: &fakeByteStreamClient{
				write: func(_ context.Context, _ ...grpc.CallOption) (bsgrpc.ByteStream_WriteClient, error) {
					var size int64
					return &fakeByteStreamWriteClient{
						send: func(wr *bspb.WriteRequest) error {
							size += int64(len(wr.Data))
							return nil
						},
						closeAndRecv: func() (*bspb.WriteResponse, error) {
							return &bspb.WriteResponse{CommittedSize: size}, nil
						},
					}, nil
				},
			},
			cc: &fakeCAS{
				findMissingBlobs: func(ctx context.Context, in *repb.FindMissingBlobsRequest, opts ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
					return &repb.FindMissingBlobsResponse{MissingBlobDigests: in.BlobDigests}, nil
				},
				batchUpdateBlobs: func(ctx context.Context, in *repb.BatchUpdateBlobsRequest, opts ...grpc.CallOption) (*repb.BatchUpdateBlobsResponse, error) {
					return &repb.BatchUpdateBlobsResponse{
						Responses: []*repb.BatchUpdateBlobsResponse_Response{{Digest: in.Requests[0].Digest, Status: &rpcstpb.Status{}}},
					}, nil
				},
			},
			wantStats: casng.Stats{
				BytesRequested:       6,
				LogicalBytesMoved:    6,
				TotalBytesMoved:      6,
				EffectiveBytesMoved:  6,
				LogicalBytesStreamed: 6,
				CacheMissCount:       1,
				StreamedCount:        1,
				InputFileCount:       1,
				DigestCount:          1,
			},
			wantUploaded: []digest.Digest{{Hash: "62f74d0e355efb6101ee13172d05e89592d4aef21ba0e4041584d8653e60c4c3", Size: 6}},
		},
		{
			name: "batch_directory",
			fs: map[string][]byte{
				"foo/bar.c":   []byte("int bar;"),
				"foo/baz.c":   []byte("int baz;"),
				"foo/a/b/c.c": []byte("int c;"),
			},
			root:  "foo",
			ioCfg: casng.IOConfig{CompressionSizeThreshold: 1024},
			batchRPCCfg: &casng.GRPCConfig{
				ConcurrentCallsLimit: 1,
				ItemsLimit:           1,
				BytesLimit:           1024,
				Timeout:              time.Second,
				BundleTimeout:        time.Millisecond,
				RetryPolicy:          retryNever,
				RetryPredicate:       retry.TransientOnly,
			},
			bsc: &fakeByteStreamClient{
				write: func(_ context.Context, _ ...grpc.CallOption) (bsgrpc.ByteStream_WriteClient, error) {
					var size int64
					return &fakeByteStreamWriteClient{
						send: func(wr *bspb.WriteRequest) error {
							size += int64(len(wr.Data))
							return nil
						},
						closeAndRecv: func() (*bspb.WriteResponse, error) {
							return &bspb.WriteResponse{CommittedSize: size}, nil
						},
					}, nil
				},
			},
			cc: &fakeCAS{
				findMissingBlobs: func(ctx context.Context, in *repb.FindMissingBlobsRequest, opts ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
					return &repb.FindMissingBlobsResponse{MissingBlobDigests: in.BlobDigests}, nil
				},
				batchUpdateBlobs: func(_ context.Context, in *repb.BatchUpdateBlobsRequest, _ ...grpc.CallOption) (*repb.BatchUpdateBlobsResponse, error) {
					resp := make([]*repb.BatchUpdateBlobsResponse_Response, len(in.Requests))
					for i, r := range in.Requests {
						resp[i] = &repb.BatchUpdateBlobsResponse_Response{Digest: r.Digest, Status: &rpcstpb.Status{}}
					}
					return &repb.BatchUpdateBlobsResponse{
						Responses: resp,
					}, nil
				},
			},
			wantStats: casng.Stats{
				BytesRequested:      407,
				LogicalBytesMoved:   407,
				TotalBytesMoved:     407,
				EffectiveBytesMoved: 407,
				LogicalBytesBatched: 407,
				CacheMissCount:      6,
				BatchedCount:        6,
				InputFileCount:      3,
				InputDirCount:       3,
				DigestCount:         6,
			},
			wantUploaded: []digest.Digest{
				{Hash: "62f74d0e355efb6101ee13172d05e89592d4aef21ba0e4041584d8653e60c4c3", Size: 6},   // foo/a/b/c.c
				{Hash: "9877358cfe402635019ce7bf591e9fd86d27953b0077e1f173b7875f0043d87a", Size: 8},   // foo/bar.c
				{Hash: "6aaaeea4a97ffca961316ffc535dc101d077c89aed6885da0e8893fa497bf8c2", Size: 8},   // foo/baz.c
				{Hash: "9093edf5f915dd0a5b2181ea08180d8a129e9e231bd0341bdf188c20d0c270d5", Size: 77},  // foo/a/b
				{Hash: "8ecd5bd172610cf88adf66f171251299ac0541be80bc40c7ac081de911284624", Size: 75},  // foo/a
				{Hash: "e62b0cd1aedd5cd2249d3afb418454483777f5deecfcd2df7fc113f546340b2e", Size: 233}, // foo
			},
		},
		{
			name: "batch_stream_directory",
			fs: map[string][]byte{
				"foo/bar.c":   []byte("int bar;"),
				"foo/baz.c":   []byte("int baz;"),
				"foo/a/b/c.c": []byte("int c;"),
			},
			root:  "foo",
			ioCfg: casng.IOConfig{CompressionSizeThreshold: 1024},
			batchRPCCfg: &casng.GRPCConfig{
				ConcurrentCallsLimit: 1,
				ItemsLimit:           1,
				BytesLimit:           1,
				Timeout:              time.Second,
				BundleTimeout:        time.Millisecond,
				RetryPolicy:          retryNever,
				RetryPredicate:       retry.TransientOnly,
			},
			bsc: &fakeByteStreamClient{
				write: func(_ context.Context, _ ...grpc.CallOption) (bsgrpc.ByteStream_WriteClient, error) {
					var size int64
					return &fakeByteStreamWriteClient{
						send: func(wr *bspb.WriteRequest) error {
							size += int64(len(wr.Data))
							return nil
						},
						closeAndRecv: func() (*bspb.WriteResponse, error) {
							return &bspb.WriteResponse{CommittedSize: size}, nil
						},
					}, nil
				},
			},
			cc: &fakeCAS{
				findMissingBlobs: func(ctx context.Context, in *repb.FindMissingBlobsRequest, opts ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
					return &repb.FindMissingBlobsResponse{MissingBlobDigests: in.BlobDigests}, nil
				},
				batchUpdateBlobs: func(_ context.Context, in *repb.BatchUpdateBlobsRequest, _ ...grpc.CallOption) (*repb.BatchUpdateBlobsResponse, error) {
					resp := make([]*repb.BatchUpdateBlobsResponse_Response, len(in.Requests))
					for i, r := range in.Requests {
						resp[i] = &repb.BatchUpdateBlobsResponse_Response{Digest: r.Digest, Status: &rpcstpb.Status{}}
					}
					return &repb.BatchUpdateBlobsResponse{
						Responses: resp,
					}, nil
				},
			},
			wantStats: casng.Stats{
				BytesRequested:       407,
				LogicalBytesMoved:    407,
				TotalBytesMoved:      407,
				EffectiveBytesMoved:  407,
				LogicalBytesStreamed: 407,
				CacheMissCount:       6,
				StreamedCount:        6,
				InputFileCount:       3,
				InputDirCount:        3,
				DigestCount:          6,
			},
			wantUploaded: []digest.Digest{
				{Hash: "62f74d0e355efb6101ee13172d05e89592d4aef21ba0e4041584d8653e60c4c3", Size: 6},   // foo/a/b/c.c
				{Hash: "9877358cfe402635019ce7bf591e9fd86d27953b0077e1f173b7875f0043d87a", Size: 8},   // foo/bar.c
				{Hash: "6aaaeea4a97ffca961316ffc535dc101d077c89aed6885da0e8893fa497bf8c2", Size: 8},   // foo/baz.c
				{Hash: "9093edf5f915dd0a5b2181ea08180d8a129e9e231bd0341bdf188c20d0c270d5", Size: 77},  // foo/a/b
				{Hash: "8ecd5bd172610cf88adf66f171251299ac0541be80bc40c7ac081de911284624", Size: 75},  // foo/a
				{Hash: "e62b0cd1aedd5cd2249d3afb418454483777f5deecfcd2df7fc113f546340b2e", Size: 233}, // foo
			},
		},
		{
			name: "stream_unified",
			fs: map[string][]byte{
				"foo/bar1.c": []byte("int bar;"),
				"foo/bar2.c": []byte("int bar;"),
			},
			root: "foo",
			ioCfg: casng.IOConfig{
				CompressionSizeThreshold: 1024,
				OpenFilesLimit:           10,
				OpenLargeFilesLimit:      10,
				BufferSize:               1024,
			},
			batchRPCCfg: &casng.GRPCConfig{
				ConcurrentCallsLimit: 1,
				ItemsLimit:           1,
				BytesLimit:           1,
				Timeout:              time.Second,
				BundleTimeout:        time.Millisecond,
				RetryPolicy:          retryNever,
				RetryPredicate:       retry.TransientOnly,
			},
			bsc: &fakeByteStreamClient{
				write: func(_ context.Context, _ ...grpc.CallOption) (bsgrpc.ByteStream_WriteClient, error) {
					var size int64
					return &fakeByteStreamWriteClient{
						send: func(wr *bspb.WriteRequest) error {
							<-time.After(10 * time.Millisecond) // Fake high latency.
							size += int64(len(wr.Data))
							return nil
						},
						closeAndRecv: func() (*bspb.WriteResponse, error) {
							return &bspb.WriteResponse{CommittedSize: size}, nil
						},
					}, nil
				},
			},
			cc: &fakeCAS{
				findMissingBlobs: func(ctx context.Context, in *repb.FindMissingBlobsRequest, opts ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
					return &repb.FindMissingBlobsResponse{MissingBlobDigests: in.BlobDigests}, nil
				},
				batchUpdateBlobs: func(_ context.Context, in *repb.BatchUpdateBlobsRequest, _ ...grpc.CallOption) (*repb.BatchUpdateBlobsResponse, error) {
					resp := make([]*repb.BatchUpdateBlobsResponse_Response, len(in.Requests))
					for i, r := range in.Requests {
						resp[i] = &repb.BatchUpdateBlobsResponse_Response{Digest: r.Digest, Status: &rpcstpb.Status{}}
					}
					return &repb.BatchUpdateBlobsResponse{
						Responses: resp,
					}, nil
				},
			},
			wantStats: casng.Stats{
				BytesRequested:       176,
				LogicalBytesMoved:    168,
				TotalBytesMoved:      168,
				EffectiveBytesMoved:  168,
				LogicalBytesStreamed: 168,
				LogicalBytesCached:   8, // the other copy
				CacheMissCount:       2,
				CacheHitCount:        1,
				StreamedCount:        2,
				InputFileCount:       2,
				InputDirCount:        1,
				DigestCount:          3,
			},
			wantUploaded: []digest.Digest{
				{Hash: "9877358cfe402635019ce7bf591e9fd86d27953b0077e1f173b7875f0043d87a", Size: 8},   // the file
				{Hash: "ffd7226e23f331727703bfd6ce4ebc0f503127f73eaa1ad62469749c82374ec5", Size: 160}, // the directory
			},
		},
		{
			name: "batch_unified",
			fs: map[string][]byte{
				"foo/bar1.c": []byte("int bar;"),
				"foo/bar2.c": []byte("int bar;"),
			},
			root:  "foo",
			ioCfg: casng.IOConfig{CompressionSizeThreshold: 1024},
			batchRPCCfg: &casng.GRPCConfig{
				ConcurrentCallsLimit: 1,
				BytesLimit:           1000,
				ItemsLimit:           3,                      // Matches the number of blobs in this test.
				BundleTimeout:        100 * time.Millisecond, // Large to ensure the bundle gets dispatched by ItemsLimit.
				Timeout:              time.Second,
			},
			bsc: &fakeByteStreamClient{
				write: func(_ context.Context, _ ...grpc.CallOption) (bsgrpc.ByteStream_WriteClient, error) {
					return &fakeByteStreamWriteClient{
						send: func(wr *bspb.WriteRequest) error {
							return nil
						},
						closeAndRecv: func() (*bspb.WriteResponse, error) {
							return &bspb.WriteResponse{}, nil
						},
					}, nil
				},
			},
			cc: &fakeCAS{
				findMissingBlobs: func(ctx context.Context, in *repb.FindMissingBlobsRequest, opts ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
					return &repb.FindMissingBlobsResponse{MissingBlobDigests: in.BlobDigests}, nil
				},
				batchUpdateBlobs: func(_ context.Context, in *repb.BatchUpdateBlobsRequest, _ ...grpc.CallOption) (*repb.BatchUpdateBlobsResponse, error) {
					resp := make([]*repb.BatchUpdateBlobsResponse_Response, len(in.Requests))
					for i, r := range in.Requests {
						resp[i] = &repb.BatchUpdateBlobsResponse_Response{Digest: r.Digest, Status: &rpcstpb.Status{}}
					}
					return &repb.BatchUpdateBlobsResponse{
						Responses: resp,
					}, nil
				},
			},
			wantStats: casng.Stats{
				BytesRequested:      176,
				LogicalBytesMoved:   168,
				TotalBytesMoved:     168,
				EffectiveBytesMoved: 168,
				LogicalBytesCached:  8,
				LogicalBytesBatched: 168,
				CacheMissCount:      2,
				CacheHitCount:       1,
				BatchedCount:        2,
				InputFileCount:      2,
				InputDirCount:       1,
				DigestCount:         3,
			},
			wantUploaded: []digest.Digest{
				{Hash: "9877358cfe402635019ce7bf591e9fd86d27953b0077e1f173b7875f0043d87a", Size: 8},   // the file
				{Hash: "ffd7226e23f331727703bfd6ce4ebc0f503127f73eaa1ad62469749c82374ec5", Size: 160}, // the directory
			},
		},
	}

	rpcCfg := casng.GRPCConfig{
		ConcurrentCallsLimit: 5,
		ItemsLimit:           2,
		BytesLimit:           1024,
		Timeout:              time.Second,
		BundleTimeout:        time.Millisecond,
		RetryPolicy:          retryNever,
		RetryPredicate:       func(error) bool { return true },
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			log.Infof("test: %s", test.name)
			tmp := makeFs(t, test.fs)
			if test.batchRPCCfg == nil {
				test.batchRPCCfg = &rpcCfg
			}
			if test.ioCfg.ConcurrentWalksLimit <= 0 {
				test.ioCfg.ConcurrentWalksLimit = 1
			}
			if test.ioCfg.BufferSize <= 0 {
				test.ioCfg.BufferSize = 1
			}
			if test.ioCfg.OpenFilesLimit <= 0 {
				test.ioCfg.OpenFilesLimit = 1
			}
			if test.ioCfg.OpenLargeFilesLimit <= 0 {
				test.ioCfg.OpenLargeFilesLimit = 1
			}
			ctx, ctxCancel := context.WithCancel(context.Background())
			defer ctxCancel()
			u, err := casng.NewBatchingUploader(ctx, test.cc, test.bsc, "", rpcCfg, *test.batchRPCCfg, rpcCfg, test.ioCfg)
			if err != nil {
				t.Fatalf("error creating batching uploader: %v", err)
			}
			root := impath.MustAbs(tmp, test.root)
			uploaded, stats, err := u.Upload(ctx, casng.UploadRequest{Path: root, SymlinkOptions: symlinkopts.PreserveAllowDangling()})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if diff := cmp.Diff(test.wantStats, stats); diff != "" {
				t.Errorf("stats mismatch, (-want +got): %s", diff)
			}
			sort.Slice(uploaded, func(i, j int) bool { return uploaded[i].Hash > uploaded[j].Hash })
			sort.Slice(test.wantUploaded, func(i, j int) bool { return test.wantUploaded[i].Hash > test.wantUploaded[j].Hash })
			if diff := cmp.Diff(test.wantUploaded, uploaded); diff != "" {
				t.Errorf("uploaded mismatch, (-want +got): %s", diff)
			}
		})
	}
}

func TestUpload_BatchingAbort(t *testing.T) {
	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()
	_, err := casng.NewBatchingUploader(ctx, &fakeCAS{}, &fakeByteStreamClient{}, "", defaultRPCCfg, defaultRPCCfg, defaultRPCCfg, defaultIOCfg)
	if err != nil {
		t.Fatalf("error creating batching uploader: %v", err)
	}
}

func TestUpload_BatchingTree(t *testing.T) {
	bsc := &fakeByteStreamClient{}
	cc := &fakeCAS{
		findMissingBlobs: func(ctx context.Context, in *repb.FindMissingBlobsRequest, opts ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
			return &repb.FindMissingBlobsResponse{}, nil
		},
	}
	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()
	u, err := casng.NewBatchingUploader(ctx, cc, bsc, "", defaultRPCCfg, defaultRPCCfg, defaultRPCCfg, defaultIOCfg)
	if err != nil {
		t.Fatalf("error creating batching uploader: %v", err)
	}
	tmp := makeFs(t, map[string][]byte{"wd/a/b/c/foo.go": []byte("foo"), "wd/a/b/bar.go": []byte("bar"), "wd/e/f/baz.go": []byte("baz")})
	ngTree := new(string)
	ctx = context.WithValue(ctx, "ng_tree", ngTree)
	rootDigest, _, _, err := u.UploadTree(ctx, impath.MustAbs(tmp), impath.MustRel("wd"), impath.MustRel("rwd"),
		casng.UploadRequest{Path: impath.MustAbs(tmp, "wd/a/b/c/foo.go")},
		casng.UploadRequest{Path: impath.MustAbs(tmp, "wd/a/b/bar.go")},
		casng.UploadRequest{Path: impath.MustAbs(tmp, "wd/e/f/baz.go")},
		casng.UploadRequest{Path: impath.MustAbs(tmp, "wd/e/f")}, // This should cause baz.go to be skipped as a redundant request since it's part of f's tree.
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if diff := cmp.Diff("4b29476de8abdfcce452b64003ed82517aa003d9e447ff943723e556e723d75c/78", rootDigest.String()); diff != "" {
		t.Errorf("root digest mismatch, (-want +got): %s\nng_tree:\n%s", diff, *ngTree)
	}
}

func TestUpload_BatchingDigestTree(t *testing.T) {
	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()

	u, err := casng.NewBatchingUploader(ctx, &fakeCAS{}, &fakeByteStreamClient{}, "", defaultRPCCfg, defaultRPCCfg, defaultRPCCfg, defaultIOCfg)
	if err != nil {
		t.Fatalf("error creating batching uploader: %v", err)
	}
	tmp := makeFs(t, map[string][]byte{"rwd/a/b/c/foo.go": []byte("foo"), "rwd/a/b/bar.go": []byte("bar"), "rwd/e/f/baz.go": []byte("baz")})
	rootDigest, stats, err := u.DigestTree(ctx, impath.MustAbs(tmp), symlinkopts.ResolveAlways(), walker.Filter{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if diff := cmp.Diff("4b29476de8abdfcce452b64003ed82517aa003d9e447ff943723e556e723d75c/78", rootDigest.String()); diff != "" {
		t.Errorf("root digest mismatch, (-want +got): %s", diff)
	}
	wantStats := casng.Stats{
		InputFileCount: 3,
		InputDirCount:  7,
		DigestCount:    10,
	}
	if diff := cmp.Diff(wantStats, stats); diff != "" {
		t.Errorf("stats mismatch, (-want +got): %s", diff)
	}
}

func TestUpload_ReplaceWorkingDir(t *testing.T) {
	tests := []struct {
		name     string
		path     impath.Absolute
		root     impath.Absolute
		wd       impath.Relative
		rwd      impath.Relative
		wantPath impath.Absolute
		wantErr  bool
	}{
		{
			name:     "both_set",
			path:     impath.MustAbs("/root/wd/foo"),
			root:     impath.MustAbs("/root"),
			wd:       impath.MustRel("wd"),
			rwd:      impath.MustRel("rwd"),
			wantPath: impath.MustAbs("/root/rwd/foo"),
		},
		{
			name:     "both_empty",
			path:     impath.MustAbs("/root/foo"),
			root:     impath.MustAbs("/root"),
			wd:       impath.MustRel(""),
			rwd:      impath.MustRel(""),
			wantPath: impath.MustAbs("/root/foo"),
		},
		{
			name:     "wd_empty",
			path:     impath.MustAbs("/root/foo"),
			root:     impath.MustAbs("/root"),
			wd:       impath.MustRel(""),
			rwd:      impath.MustRel("rwd"),
			wantPath: impath.MustAbs("/root/rwd/foo"),
		},
		{
			name:     "rwd_empty",
			path:     impath.MustAbs("/root/wd/foo"),
			root:     impath.MustAbs("/root"),
			wd:       impath.MustRel("wd"),
			rwd:      impath.MustRel(""),
			wantPath: impath.MustAbs("/root/foo"),
		},
		{
			name:     "root_not_prefix",
			path:     impath.MustAbs("/root/wd/foo"),
			root:     impath.MustAbs("/root2"),
			wd:       impath.MustRel("wd"),
			rwd:      impath.MustRel("rwd"),
			wantPath: impath.Absolute{},
			wantErr:  true,
		},
		{
			name:     "outside_wd",
			path:     impath.MustAbs("/root/out/foo"),
			root:     impath.MustAbs("/root"),
			wd:       impath.MustRel("out/src"),
			rwd:      impath.MustRel("rwd/a/b"),
			wantPath: impath.MustAbs("/root/rwd/a/foo"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			p, err := casng.ReplaceWorkingDir(test.path, test.root, test.wd, test.rwd)
			if err != nil && !test.wantErr {
				t.Errorf("unexpected error: %v", err)
			}
			if err == nil && test.wantErr {
				t.Errorf("expected an error, but didn't get one")
			}
			if test.wantPath.String() != p.String() {
				t.Errorf("Path mismatch: want: %q, got %q", test.wantPath, p)
			}
		})
	}
}
