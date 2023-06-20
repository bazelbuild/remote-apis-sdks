// Using a different package name to strictly exclude types defined here from the original package.
package casng_test

import (
	"context"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/casng"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"
	// Redundant imports are required for the google3 mirror. Aliases should not be changed.
	regrpc "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	bsgrpc "google.golang.org/genproto/googleapis/bytestream"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
)

var (
	retryNever     = retry.Immediately(retry.Attempts(1))
	retryTwice     = retry.ExponentialBackoff(time.Microsecond, time.Microsecond, retry.Attempts(2))
	retryAllErrors = func(error) bool { return true }
	defaultRPCCfg  = casng.GRPCConfig{
		ConcurrentCallsLimit: 5,
		ItemsLimit:           2,
		BytesLimit:           1024,
		Timeout:              time.Second,
		BundleTimeout:        time.Millisecond,
		RetryPolicy:          retryNever,
		RetryPredicate:       retry.TransientOnly,
	}
	defaultIOCfg = casng.IOConfig{
		ConcurrentWalksLimit:     1,
		OpenFilesLimit:           1,
		OpenLargeFilesLimit:      1,
		SmallFileSizeThreshold:   1,
		LargeFileSizeThreshold:   1,
		CompressionSizeThreshold: 10,
		BufferSize:               2,
	}
)

type fakeByteStreamClient struct {
	bsgrpc.ByteStreamClient
	write func(ctx context.Context, opts ...grpc.CallOption) (bsgrpc.ByteStream_WriteClient, error)
}

type fakeByteStreamWriteClient struct {
	bsgrpc.ByteStream_WriteClient
	send         func(*bspb.WriteRequest) error
	closeAndRecv func() (*bspb.WriteResponse, error)
}

func (s *fakeByteStreamClient) Write(ctx context.Context, opts ...grpc.CallOption) (bsgrpc.ByteStream_WriteClient, error) {
	if s.write != nil {
		return s.write(ctx, opts...)
	}
	return &fakeByteStreamWriteClient{}, nil
}

func (s *fakeByteStreamWriteClient) Send(wr *bspb.WriteRequest) error {
	if s.send != nil {
		return s.send(wr)
	}
	return nil
}

func (s *fakeByteStreamWriteClient) CloseAndRecv() (*bspb.WriteResponse, error) {
	if s.closeAndRecv != nil {
		return s.closeAndRecv()
	}
	return &bspb.WriteResponse{}, nil
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

type byHash []digest.Digest

func (a byHash) Len() int {
	return len(a)
}

func (a byHash) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a byHash) Less(i, j int) bool {
	return a[i].Hash < a[j].Hash
}
