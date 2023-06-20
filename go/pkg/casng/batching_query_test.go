// Using a different package name to strictly exclude types defined here from the original package.
package casng_test

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/casng"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/errors"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	log "github.com/golang/glog"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc"
)

func TestQuery_Batching(t *testing.T) {
	errSend := fmt.Errorf("send error")
	tests := []struct {
		name        string
		digests     []digest.Digest
		cas         *fakeCAS
		wantErr     error
		wantDigests []digest.Digest
	}{
		{"empty_request", nil, &fakeCAS{}, nil, nil},
		{
			"no_missing",
			[]digest.Digest{{Hash: "a"}, {Hash: "b"}},
			&fakeCAS{findMissingBlobs: func(_ context.Context, _ *repb.FindMissingBlobsRequest, _ ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
				return &repb.FindMissingBlobsResponse{}, nil
			}},
			nil,
			[]digest.Digest{},
		},
		{
			"all_missing",
			[]digest.Digest{{Hash: "a"}, {Hash: "b"}},
			&fakeCAS{findMissingBlobs: func(_ context.Context, req *repb.FindMissingBlobsRequest, _ ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
				return &repb.FindMissingBlobsResponse{MissingBlobDigests: req.BlobDigests}, nil
			}},
			nil,
			[]digest.Digest{{Hash: "a"}, {Hash: "b"}},
		},
		{
			"some_missing",
			[]digest.Digest{{Hash: "a"}, {Hash: "b"}},
			&fakeCAS{findMissingBlobs: func(_ context.Context, req *repb.FindMissingBlobsRequest, _ ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
				return &repb.FindMissingBlobsResponse{MissingBlobDigests: []*repb.Digest{{Hash: "a"}}}, nil
			}},
			nil,
			[]digest.Digest{{Hash: "a"}},
		},
		{
			"error_call",
			[]digest.Digest{{Hash: "a"}, {Hash: "b"}},
			&fakeCAS{findMissingBlobs: func(_ context.Context, req *repb.FindMissingBlobsRequest, _ ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
				return &repb.FindMissingBlobsResponse{}, errSend
			}},
			errSend,
			[]digest.Digest{{Hash: "a"}, {Hash: "b"}},
		},
		{
			"two_batches",
			[]digest.Digest{{Hash: "a"}, {Hash: "b"}, {Hash: "c"}},
			&fakeCAS{findMissingBlobs: func(_ context.Context, req *repb.FindMissingBlobsRequest, _ ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
				return &repb.FindMissingBlobsResponse{MissingBlobDigests: req.BlobDigests}, nil
			}},
			nil,
			[]digest.Digest{{Hash: "a"}, {Hash: "b"}, {Hash: "c"}},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			log.Infof("test: %s", test.name)
			ctx, ctxCancel := context.WithCancel(context.Background())
			defer ctxCancel()
			u, err := casng.NewBatchingUploader(ctx, test.cas, &fakeByteStreamClient{}, "", defaultRPCCfg, defaultRPCCfg, defaultRPCCfg, defaultIOCfg)
			if err != nil {
				t.Fatalf("error creating batching uploader: %v", err)
			}
			missing, err := u.MissingBlobs(ctx, test.digests)
			if test.wantErr == nil && err != nil {
				t.Errorf("MissingBlobs failed: %v", err)
			}
			if test.wantErr != nil && !errors.Is(err, test.wantErr) {
				t.Errorf("error mismatch: want %v, got %v", test.wantErr, err)
			}
			sort.Sort(byHash(test.wantDigests))
			sort.Sort(byHash(missing))
			if diff := cmp.Diff(test.wantDigests, missing); diff != "" {
				t.Errorf("missing mismatch, (-want +got): %s", diff)
			}
		})
	}
}
