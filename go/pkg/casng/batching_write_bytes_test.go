package casng_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/casng"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/errors"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"
	"github.com/google/go-cmp/cmp"
	// Redundant imports are required for the google3 mirror. Aliases should not be changed.
	bsgrpc "google.golang.org/genproto/googleapis/bytestream"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestUpload_WriteBytes(t *testing.T) {
	errWrite := fmt.Errorf("write error")
	errClose := fmt.Errorf("close error")

	tests := []struct {
		name        string
		bs          *fakeByteStreamClient
		b           []byte
		offset      int64
		finish      bool
		retryPolicy *retry.BackoffPolicy
		wantStats   casng.Stats
		wantErr     error
	}{
		{
			name: "no_compression",
			bs: &fakeByteStreamClient{
				write: func(_ context.Context, _ ...grpc.CallOption) (bsgrpc.ByteStream_WriteClient, error) {
					bytesSent := int64(0)
					return &fakeByteStreamWriteClient{
						send: func(wr *bspb.WriteRequest) error {
							bytesSent += int64(len(wr.Data))
							return nil
						},
						closeAndRecv: func() (*bspb.WriteResponse, error) {
							return &bspb.WriteResponse{CommittedSize: bytesSent}, nil
						},
					}, nil
				},
			},
			b:       []byte("abs"),
			wantErr: nil,
			wantStats: casng.Stats{
				BytesRequested:       3,
				EffectiveBytesMoved:  3,
				TotalBytesMoved:      3,
				LogicalBytesMoved:    3,
				LogicalBytesStreamed: 3,
				CacheMissCount:       1,
				StreamedCount:        1,
			},
		},
		{
			name: "compression",
			bs: &fakeByteStreamClient{
				write: func(_ context.Context, _ ...grpc.CallOption) (bsgrpc.ByteStream_WriteClient, error) {
					return &fakeByteStreamWriteClient{
						send: func(wr *bspb.WriteRequest) error {
							return nil
						},
						closeAndRecv: func() (*bspb.WriteResponse, error) {
							return &bspb.WriteResponse{CommittedSize: 3500}, nil
						},
					}, nil
				},
			},
			b:       []byte(strings.Repeat("abcdefg", 500)),
			wantErr: nil,
			wantStats: casng.Stats{
				BytesRequested:       3500,
				EffectiveBytesMoved:  29,
				TotalBytesMoved:      29,
				LogicalBytesMoved:    3500,
				LogicalBytesStreamed: 3500,
				CacheMissCount:       1,
				StreamedCount:        1,
			},
		},
		{
			name: "write_call_error",
			bs: &fakeByteStreamClient{
				write: func(ctx context.Context, opts ...grpc.CallOption) (bsgrpc.ByteStream_WriteClient, error) {
					return nil, errWrite
				},
			},
			b:         []byte("abc"),
			wantErr:   errWrite,
			wantStats: casng.Stats{},
		},
		{
			name: "cache_hit",
			bs: &fakeByteStreamClient{
				write: func(ctx context.Context, opts ...grpc.CallOption) (bsgrpc.ByteStream_WriteClient, error) {
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
			b:       []byte("abc"),
			wantErr: nil,
			wantStats: casng.Stats{
				BytesRequested:       3,
				EffectiveBytesMoved:  2, // matches buffer size
				TotalBytesMoved:      2,
				LogicalBytesMoved:    2,
				LogicalBytesStreamed: 2,
				CacheHitCount:        1,
				LogicalBytesCached:   3,
				StreamedCount:        1,
			},
		},
		{
			name: "send_error",
			bs: &fakeByteStreamClient{
				write: func(ctx context.Context, opts ...grpc.CallOption) (bsgrpc.ByteStream_WriteClient, error) {
					return &fakeByteStreamWriteClient{
						send: func(wr *bspb.WriteRequest) error {
							return errWrite
						},
						closeAndRecv: func() (*bspb.WriteResponse, error) {
							return &bspb.WriteResponse{}, nil
						},
					}, nil
				},
			},
			b:       []byte("abc"),
			wantErr: casng.ErrGRPC,
			wantStats: casng.Stats{
				BytesRequested:       3,
				EffectiveBytesMoved:  2, // matches buffer size
				TotalBytesMoved:      2,
				LogicalBytesMoved:    2,
				LogicalBytesStreamed: 2,
				CacheMissCount:       1,
				StreamedCount:        0,
			},
		},
		{
			name: "send_retry_timeout",
			bs: &fakeByteStreamClient{
				write: func(ctx context.Context, opts ...grpc.CallOption) (bsgrpc.ByteStream_WriteClient, error) {
					return &fakeByteStreamWriteClient{
						send: func(wr *bspb.WriteRequest) error {
							return status.Error(codes.DeadlineExceeded, "error")
						},
						closeAndRecv: func() (*bspb.WriteResponse, error) {
							return &bspb.WriteResponse{}, nil
						},
					}, nil
				},
			},
			b:       []byte("abc"),
			wantErr: casng.ErrGRPC,
			wantStats: casng.Stats{
				BytesRequested:       3,
				EffectiveBytesMoved:  2, // matches one buffer size
				TotalBytesMoved:      4, // matches two buffer sizes
				LogicalBytesMoved:    2,
				LogicalBytesStreamed: 2,
				CacheMissCount:       1,
				StreamedCount:        0,
			},
			retryPolicy: &retryTwice,
		},
		{
			name: "stream_close_error",
			bs: &fakeByteStreamClient{
				write: func(ctx context.Context, opts ...grpc.CallOption) (bsgrpc.ByteStream_WriteClient, error) {
					return &fakeByteStreamWriteClient{
						send: func(wr *bspb.WriteRequest) error {
							return nil
						},
						closeAndRecv: func() (*bspb.WriteResponse, error) {
							return nil, errClose
						},
					}, nil
				},
			},
			b:       []byte("abc"),
			wantErr: casng.ErrGRPC,
			wantStats: casng.Stats{
				BytesRequested:       3,
				EffectiveBytesMoved:  3,
				TotalBytesMoved:      3,
				LogicalBytesMoved:    3,
				LogicalBytesStreamed: 3,
				CacheMissCount:       1,
				StreamedCount:        1,
			},
		},
		{
			name: "arbitrary_offset",
			bs: &fakeByteStreamClient{
				write: func(ctx context.Context, opts ...grpc.CallOption) (bsgrpc.ByteStream_WriteClient, error) {
					return &fakeByteStreamWriteClient{
						send: func(wr *bspb.WriteRequest) error {
							if wr.WriteOffset < 5 {
								return fmt.Errorf("mismatched offset: want 5, got %d", wr.WriteOffset)
							}
							return nil
						},
						closeAndRecv: func() (*bspb.WriteResponse, error) {
							return &bspb.WriteResponse{CommittedSize: 3}, nil
						},
					}, nil
				},
			},
			b:      []byte("abc"),
			offset: 5,
			wantStats: casng.Stats{
				BytesRequested:       3,
				EffectiveBytesMoved:  3,
				TotalBytesMoved:      3,
				LogicalBytesMoved:    3,
				LogicalBytesStreamed: 3,
				CacheMissCount:       1,
				StreamedCount:        1,
			},
		},
		{
			name: "finish_write",
			bs: &fakeByteStreamClient{
				write: func(ctx context.Context, opts ...grpc.CallOption) (bsgrpc.ByteStream_WriteClient, error) {
					return &fakeByteStreamWriteClient{
						send: func(wr *bspb.WriteRequest) error {
							if len(wr.Data) == 0 && !wr.FinishWrite {
								return fmt.Errorf("finish write was not set")
							}
							return nil
						},
						closeAndRecv: func() (*bspb.WriteResponse, error) {
							return &bspb.WriteResponse{CommittedSize: 3}, nil
						},
					}, nil
				},
			},
			b:      []byte("abc"),
			finish: true,
			wantStats: casng.Stats{
				BytesRequested:       3,
				EffectiveBytesMoved:  3,
				TotalBytesMoved:      3,
				LogicalBytesMoved:    3,
				LogicalBytesStreamed: 3,
				CacheMissCount:       1,
				StreamedCount:        1,
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			testRPCCfg := defaultRPCCfg
			if test.retryPolicy != nil {
				testRPCCfg.RetryPolicy = *test.retryPolicy
			}
			u, err := casng.NewBatchingUploader(context.Background(), &fakeCAS{}, test.bs, "", testRPCCfg, testRPCCfg, testRPCCfg, defaultIOCfg)
			if err != nil {
				t.Fatalf("error creating batching uploader: %v", err)
			}
			var name string
			if len(test.b) >= int(defaultIOCfg.CompressionSizeThreshold) {
				name = casng.MakeCompressedWriteResourceName("instance", "hash", 0)
			} else {
				name = casng.MakeWriteResourceName("instance", "hash", 0)
			}
			var stats casng.Stats
			if test.finish {
				stats, err = u.WriteBytes(context.Background(), name, bytes.NewReader(test.b), int64(len(test.b)), test.offset)
			} else {
				stats, err = u.WriteBytesPartial(context.Background(), name, bytes.NewReader(test.b), int64(len(test.b)), test.offset)
			}
			if test.wantErr == nil && err != nil {
				t.Errorf("WriteBytes failed: %v", err)
			}
			if test.wantErr != nil && !errors.Is(err, test.wantErr) {
				t.Errorf("error mismatch: want %v, got %v", test.wantErr, err)
			}
			if diff := cmp.Diff(test.wantStats, stats); diff != "" {
				t.Errorf("stats mismatch, (-want +got): %s", diff)
			}
		})
	}
}
