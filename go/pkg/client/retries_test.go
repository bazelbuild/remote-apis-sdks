package client_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/client"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/klauspost/compress/zstd"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	// Redundant imports are required for the google3 mirror. Aliases should not be changed.
	regrpc "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	bsgrpc "google.golang.org/genproto/googleapis/bytestream"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	opgrpc "google.golang.org/genproto/googleapis/longrunning"
	oppb "google.golang.org/genproto/googleapis/longrunning"
	anypb "google.golang.org/protobuf/types/known/anypb"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

var zstdEncoder, _ = zstd.NewWriter(nil, zstd.WithZeroFrames(true))

type flakyServer struct {
	// TODO(jsharpe): This is a hack to work around WaitOperation not existing in some versions of
	// the long running operations API that we need to support.
	opgrpc.OperationsServer
	mu               sync.RWMutex     // Protects numCalls.
	numCalls         map[string]int   // A counter of calls the server encountered thus far, by method.
	muOffset         sync.RWMutex     // Protects initialOffsets.
	initialOffsets   map[string]int64 // Stores an initial offset to verify if retries are started over from a correct offset.
	retriableForever bool             // Set to true to make the flaky server return a retriable error forever, rather than eventually a non-retriable error.
	sleepDelay       time.Duration    // How long to sleep on each RPC.
	useBSCompression bool             // Whether to use/expect compression on ByteStream calls.
}

func (f *flakyServer) incNumCalls(method string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.numCalls[method]++
	return f.numCalls[method]
}

func (f *flakyServer) setInitialOffset(name string, offset int64) {
	f.muOffset.Lock()
	defer f.muOffset.Unlock()
	f.initialOffsets[name] = offset
}

func (f *flakyServer) fetchInitialOffset(name string) int64 {
	f.muOffset.Lock()
	defer f.muOffset.Unlock()
	offset, ok := f.initialOffsets[name]
	if !ok {
		return 0
	}
	return offset
}

func (f *flakyServer) Write(stream bsgrpc.ByteStream_WriteServer) error {
	numCalls := f.incNumCalls("Write")
	if numCalls < 3 {
		time.Sleep(f.sleepDelay)
		return status.Error(codes.Canceled, "transient error!")
	}

	req, err := stream.Recv()
	if err != nil {
		return err
	}
	// Verify that the client sends the first chunk, because they should retry from scratch.
	initialOffset := f.fetchInitialOffset(req.ResourceName)
	if req.WriteOffset != initialOffset || req.FinishWrite {
		return status.Error(codes.FailedPrecondition, fmt.Sprintf("expected first chunk, got %v", req))
	}
	if numCalls < 5 {
		return status.Error(codes.Internal, "another transient error!")
	}
	return stream.SendAndClose(&bspb.WriteResponse{CommittedSize: 4})
}

func (f *flakyServer) Read(req *bspb.ReadRequest, stream bsgrpc.ByteStream_ReadServer) error {
	numCalls := f.incNumCalls("Read")
	if numCalls < 3 {
		time.Sleep(f.sleepDelay)
		return status.Error(codes.Canceled, "transient error!")
	}
	if numCalls < 4 {
		b := []byte("bl")
		if f.useBSCompression {
			b = zstdEncoder.EncodeAll(b, nil)
		}
		// We send the 4 byte test blob in two chunks.
		if err := stream.Send(&bspb.ReadResponse{Data: b}); err != nil {
			return err
		}
		return status.Error(codes.Internal, "another transient error!")
	}
	// Client now will only ask for the remaining two bytes.
	if numCalls < 5 {
		time.Sleep(f.sleepDelay)
		return status.Error(codes.Aborted, "yet another transient error!")
	}
	b := []byte("ob")
	if f.useBSCompression {
		b = zstdEncoder.EncodeAll(b, nil)
	}
	return stream.Send(&bspb.ReadResponse{Data: b})
}

func (f *flakyServer) flakeAndFail(method string) error {
	numCalls := f.incNumCalls(method)
	if numCalls == 1 {
		if f.sleepDelay != 0 {
			time.Sleep(f.sleepDelay)
			// The error we return here should not matter; the deadline should have passed by now and the
			// retrier should retry DeadlineExceeded.
			return status.Error(codes.InvalidArgument, "non retriable error")
		}
		return status.Error(codes.DeadlineExceeded, "transient error!")
	}
	if f.retriableForever || numCalls < 4 {
		return status.Error(codes.Canceled, "transient error!")
	}
	return status.Error(codes.Unimplemented, "a non retriable error")
}

func (f *flakyServer) QueryWriteStatus(context.Context, *bspb.QueryWriteStatusRequest) (*bspb.QueryWriteStatusResponse, error) {
	return nil, f.flakeAndFail("QueryWriteStatus")
}

func (f *flakyServer) GetActionResult(ctx context.Context, req *repb.GetActionResultRequest) (*repb.ActionResult, error) {
	return nil, f.flakeAndFail("GetActionResult")
}

func (f *flakyServer) UpdateActionResult(ctx context.Context, req *repb.UpdateActionResultRequest) (*repb.ActionResult, error) {
	return nil, f.flakeAndFail("UpdateActionResult")
}

func (f *flakyServer) FindMissingBlobs(ctx context.Context, req *repb.FindMissingBlobsRequest) (*repb.FindMissingBlobsResponse, error) {
	return nil, f.flakeAndFail("FindMissingBlobs")
}

func (f *flakyServer) BatchUpdateBlobs(ctx context.Context, req *repb.BatchUpdateBlobsRequest) (*repb.BatchUpdateBlobsResponse, error) {
	return nil, f.flakeAndFail("BatchUpdateBlobs")
}

func (f *flakyServer) BatchReadBlobs(ctx context.Context, req *repb.BatchReadBlobsRequest) (*repb.BatchReadBlobsResponse, error) {
	return nil, f.flakeAndFail("BatchReadBlobs")
}

func (f *flakyServer) GetTree(req *repb.GetTreeRequest, stream regrpc.ContentAddressableStorage_GetTreeServer) error {
	numCalls := f.incNumCalls("GetTree")
	if numCalls < 3 {
		return status.Error(codes.Canceled, "transient error!")
	}
	if numCalls < 4 {
		// Send one directory then cut the stream.
		resp := &repb.GetTreeResponse{
			Directories:   []*repb.Directory{{Files: []*repb.FileNode{{Name: "I'm a file!"}}}},
			NextPageToken: "I should be a base64-encoded token, but I'm not",
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
		return status.Error(codes.Internal, "another transient error!")
	}
	// Client now will only ask for the remaining directories.
	if numCalls < 5 {
		return status.Error(codes.Aborted, "yet another transient error!")
	}
	resp := &repb.GetTreeResponse{
		Directories: []*repb.Directory{{Files: []*repb.FileNode{{Name: "I, too, am a file."}}}},
	}
	return stream.Send(resp)
}

func (f *flakyServer) Execute(req *repb.ExecuteRequest, stream regrpc.Execution_ExecuteServer) error {
	numCalls := f.incNumCalls("Execute")
	if numCalls < 2 {
		return status.Error(codes.Canceled, "transient error!")
	}
	stream.Send(&oppb.Operation{Done: false, Name: "dummy"})
	// After this error, retries should to go the WaitExecution method.
	return status.Error(codes.Internal, "another transient error!")
}

func (f *flakyServer) WaitExecution(req *repb.WaitExecutionRequest, stream regrpc.Execution_WaitExecutionServer) error {
	numCalls := f.incNumCalls("WaitExecution")
	if numCalls < 2 {
		return status.Error(codes.Canceled, "transient error!")
	}
	if numCalls < 4 {
		stream.Send(&oppb.Operation{Done: false, Name: "dummy"})
		return status.Error(codes.Internal, "another transient error!")
	}
	// Execute (above) will fail twice (and be retried twice) before ExecuteAndWait() switches to
	// WaitExecution. WaitExecution will fail 4 more times more before succeeding, for a total of 6 retries.
	execResp := &repb.ExecuteResponse{Status: status.New(codes.Aborted, "transient operation failure!").Proto()}
	any, e := anypb.New(execResp)
	if e != nil {
		return e
	}
	return stream.Send(&oppb.Operation{Name: "op", Done: true, Result: &oppb.Operation_Response{Response: any}})
}

func (f *flakyServer) GetOperation(ctx context.Context, req *oppb.GetOperationRequest) (*oppb.Operation, error) {
	return nil, f.flakeAndFail("GetOperation")
}

func (f *flakyServer) ListOperations(ctx context.Context, req *oppb.ListOperationsRequest) (*oppb.ListOperationsResponse, error) {
	return nil, f.flakeAndFail("ListOperations")
}

func (f *flakyServer) CancelOperation(ctx context.Context, req *oppb.CancelOperationRequest) (*emptypb.Empty, error) {
	return nil, f.flakeAndFail("CancelOperation")
}

func (f *flakyServer) DeleteOperation(ctx context.Context, req *oppb.DeleteOperationRequest) (*emptypb.Empty, error) {
	return nil, f.flakeAndFail("DeleteOperation")
}

type flakyFixture struct {
	client   *client.Client
	listener net.Listener
	server   *grpc.Server
	fake     *flakyServer
	ctx      context.Context
}

func setup(t *testing.T) *flakyFixture {
	f := &flakyFixture{ctx: context.Background()}
	var err error
	f.listener, err = net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	f.server = grpc.NewServer()
	f.fake = &flakyServer{numCalls: make(map[string]int), initialOffsets: make(map[string]int64)}
	bsgrpc.RegisterByteStreamServer(f.server, f.fake)
	regrpc.RegisterActionCacheServer(f.server, f.fake)
	regrpc.RegisterContentAddressableStorageServer(f.server, f.fake)
	regrpc.RegisterExecutionServer(f.server, f.fake)
	opgrpc.RegisterOperationsServer(f.server, f.fake)
	go f.server.Serve(f.listener)
	f.client, err = client.NewClient(f.ctx, instance, client.DialParams{
		Service:    f.listener.Addr().String(),
		NoSecurity: true,
	}, client.StartupCapabilities(false), client.ChunkMaxSize(2))
	if err != nil {
		t.Fatalf("Error connecting to server: %v", err)
	}
	return f
}

func (f *flakyFixture) shutDown() {
	f.client.Close()
	f.listener.Close()
	f.server.Stop()
}

func compressionBoolToValue(use bool) client.CompressedBytestreamThreshold {
	if use {
		return client.CompressedBytestreamThreshold(0)
	}
	return client.CompressedBytestreamThreshold(-1)
}

func TestWriteRetries(t *testing.T) {
	t.Parallel()
	for _, sleep := range []bool{false, true} {
		sleep := sleep
		t.Run(fmt.Sprintf("sleep=%t", sleep), func(t *testing.T) {
			t.Parallel()
			f := setup(t)
			defer f.shutDown()
			if sleep {
				f.fake.sleepDelay = time.Second
				client.RPCTimeouts(map[string]time.Duration{"default": 500 * time.Millisecond}).Apply(f.client)
			}

			blob := []byte("blob")
			gotDg, err := f.client.WriteBlob(f.ctx, blob)
			if err != nil {
				t.Errorf("client.WriteBlob(ctx, blob) gave error %s, wanted nil", err)
			}
			if diff := cmp.Diff(digest.NewFromBlob(blob), gotDg); diff != "" {
				t.Errorf("client.WriteBlob(ctx, blob) had diff on digest returned (want -> got):\n%s", diff)
			}
		})
	}
}

func TestRetryWriteBytesAtRemoteOffset(t *testing.T) {
	tests := []struct {
		description   string
		initialOffset int64
	}{
		{
			description:   "offset 0",
			initialOffset: 0,
		},
		{
			description:   "offset 3",
			initialOffset: 3,
		},
	}

	for _, doNotFinalize := range []bool{true, false} {
		for _, test := range tests {
			t.Run(fmt.Sprintf("%s and doNotFinalize %t", test.description, doNotFinalize), func(t *testing.T) {
				f := setup(t)
				defer f.shutDown()
				name := test.description
				data := []byte("Hello World!")
				if test.initialOffset > 0 {
					f.fake.setInitialOffset(name, test.initialOffset)
				}

				writtenBytes, err := f.client.WriteBytesAtRemoteOffset(f.ctx, name, data[test.initialOffset:], doNotFinalize, test.initialOffset)
				if err != nil {
					t.Errorf("client.WriteBytesAtRemoteOffset(ctx, name, %s, %t, %d) gave error %s, want nil", string(data), doNotFinalize, test.initialOffset, err)
				}
				if int64(len(data))-test.initialOffset != writtenBytes {
					t.Errorf("client.WriteBytesAtRemoteOffset(ctx, name, %s,  %t, %d) gave %d byte(s), want %d", string(data), doNotFinalize, test.initialOffset, writtenBytes, int64(len(data))-test.initialOffset)
				}
			})
		}
	}
}

func TestReadRetries(t *testing.T) {
	t.Parallel()
	for _, sleep := range []bool{false, true} {
		for _, comp := range []bool{false, true} {
			sleep := sleep
			comp := comp
			t.Run(fmt.Sprintf("sleep=%t,comp=%t", sleep, comp), func(t *testing.T) {
				t.Parallel()
				f := setup(t)
				defer f.shutDown()
				f.fake.useBSCompression = comp
				compOpt := compressionBoolToValue(comp)
				compOpt.Apply(f.client)
				if sleep {
					f.fake.sleepDelay = time.Second
					client.RPCTimeouts(map[string]time.Duration{"default": 500 * time.Millisecond}).Apply(f.client)
				}

				blob := []byte("blob")
				got, _, err := f.client.ReadBlob(f.ctx, digest.NewFromBlob(blob))
				if err != nil {
					t.Errorf("client.ReadBlob(ctx, digest) gave error %s, want nil", err)
				}
				if diff := cmp.Diff(blob, got, cmpopts.EquateEmpty()); diff != "" {
					t.Errorf("client.ReadBlob(ctx, digest) gave diff (-want, +got):\n%s", diff)
				}
			})
		}
	}
}

func TestReadToFileRetries(t *testing.T) {
	t.Parallel()
	for _, sleep := range []bool{false, true} {
		for _, comp := range []bool{false, true} {
			sleep := sleep
			comp := comp
			t.Run(fmt.Sprintf("sleep=%t", sleep), func(t *testing.T) {
				t.Parallel()
				f := setup(t)
				defer f.shutDown()
				f.fake.useBSCompression = comp
				compOpt := compressionBoolToValue(comp)
				compOpt.Apply(f.client)

				if sleep {
					f.fake.sleepDelay = time.Second
					client.RPCTimeouts(map[string]time.Duration{"default": 500 * time.Millisecond}).Apply(f.client)
				}

				blob := []byte("blob")
				path := filepath.Join(t.TempDir(), strings.Replace(t.Name(), "/", "_", -1))
				stats, err := f.client.ReadBlobToFile(f.ctx, digest.NewFromBlob(blob), path)
				if err != nil {
					t.Errorf("client.ReadBlobToFile(ctx, digest) gave error %s, want nil", err)
				}
				if stats.LogicalMoved != int64(len(blob)) {
					t.Errorf("client.ReadBlobToFile(ctx, digest) returned %d read bytes, wanted %d", stats.LogicalMoved, len(blob))
				}
				if comp && stats.LogicalMoved == stats.RealMoved {
					t.Errorf("client.ReadBlobToFile(ctx, digest) = %v - compression on but same real and logical bytes", stats)
				}

				contents, err := os.ReadFile(path)
				if err != nil {
					t.Errorf("error reading from %s: %v", path, err)
				}
				if !bytes.Equal(contents, blob) {
					t.Errorf("expected %s to contain %v, got %v", path, blob, contents)
				}
			})
		}
	}
}

// Verify for one arbitrary method that when retries are exhausted, we get the retriable error code
// back.
func TestBatchWriteBlobsRpcRetriesExhausted(t *testing.T) {
	t.Parallel()
	f := setup(t)
	f.fake.retriableForever = true
	defer f.shutDown()

	blobs := map[digest.Digest][]byte{
		digest.TestNew("a", 1): []byte{1},
		digest.TestNew("b", 1): []byte{2},
	}
	err := f.client.BatchWriteBlobs(f.ctx, blobs)
	if err == nil {
		t.Error("BatchWriteBlobs(ctx, {}) = nil; expected Canceled error got nil")
	} else if s, ok := status.FromError(err); ok && s.Code() != codes.Canceled {
		t.Errorf("BatchWriteBlobs(ctx, {}) = %v; expected Canceled error, got %v", err, s.Code())
	} else if !ok {
		t.Errorf("BatchWriteBlobs(ctx, {}) = %v; expected Canceled error (status.FromError failed)", err)
	}
}

func TestGetTreeRetries(t *testing.T) {
	t.Parallel()
	f := setup(t)
	defer f.shutDown()

	blob := []byte("blob")
	got, err := f.client.GetDirectoryTree(f.ctx, digest.NewFromBlob(blob).ToProto())
	if err != nil {
		t.Errorf("client.GetDirectoryTree(ctx, digest) gave err %s, want nil", err)
	}
	if len(got) != 2 {
		t.Errorf("client.GetDirectoryTree(ctx, digest) gave %d directories, want 2", len(got))
	}
}

func TestExecuteAndWaitRetries(t *testing.T) {
	t.Parallel()
	f := setup(t)
	defer f.shutDown()

	op, err := f.client.ExecuteAndWait(f.ctx, &repb.ExecuteRequest{})
	if err != nil {
		t.Fatalf("client.WaitExecution(ctx, {}) = %v", err)
	}
	st := client.OperationStatus(op)
	if st == nil {
		t.Errorf("client.WaitExecution(ctx, {}) returned no status, expected Aborted")
	}
	if st != nil && st.Code() != codes.Aborted {
		t.Errorf("client.WaitExecution(ctx, {}) returned unexpected status code %s", st.Code())
	}
	// 2 separate transient Execute errors.
	if f.fake.numCalls["Execute"] != 2 {
		t.Errorf("Expected 2 Execute calls, got %v", f.fake.numCalls["Execute"])
	}
	// 3 separate transient WaitExecution errors + the final successful call.
	if f.fake.numCalls["WaitExecution"] != 4 {
		t.Errorf("Expected 4 WaitExecution calls, got %v", f.fake.numCalls["WaitExecution"])
	}
}

func TestNonStreamingRpcRetries(t *testing.T) {
	t.Parallel()
	testcases := []struct {
		name string
		rpc  func(*flakyFixture) (interface{}, error)
	}{
		{
			name: "QueryWriteStatus",
			rpc: func(f *flakyFixture) (interface{}, error) {
				return f.client.QueryWriteStatus(f.ctx, &bspb.QueryWriteStatusRequest{})
			},
		},
		{
			name: "GetActionResult",
			rpc: func(f *flakyFixture) (interface{}, error) {
				return f.client.GetActionResult(f.ctx, &repb.GetActionResultRequest{})
			},
		},
		{
			name: "UpdateActionResult",
			rpc: func(f *flakyFixture) (interface{}, error) {
				return f.client.UpdateActionResult(f.ctx, &repb.UpdateActionResultRequest{})
			},
		},
		{
			name: "FindMissingBlobs",
			rpc: func(f *flakyFixture) (interface{}, error) {
				return f.client.FindMissingBlobs(f.ctx, &repb.FindMissingBlobsRequest{})
			},
		},
		{
			name: "BatchUpdateBlobs",
			rpc: func(f *flakyFixture) (interface{}, error) {
				return f.client.BatchUpdateBlobs(f.ctx, &repb.BatchUpdateBlobsRequest{})
			},
		},
		{
			name: "GetOperation",
			rpc: func(f *flakyFixture) (interface{}, error) {
				return f.client.GetOperation(f.ctx, &oppb.GetOperationRequest{})
			},
		},
		{
			name: "ListOperations",
			rpc: func(f *flakyFixture) (interface{}, error) {
				return f.client.ListOperations(f.ctx, &oppb.ListOperationsRequest{})
			},
		},
		{
			name: "CancelOperation",
			rpc: func(f *flakyFixture) (interface{}, error) {
				return f.client.CancelOperation(f.ctx, &oppb.CancelOperationRequest{})
			},
		},
		{
			name: "DeleteOperation",
			rpc: func(f *flakyFixture) (interface{}, error) {
				return f.client.DeleteOperation(f.ctx, &oppb.DeleteOperationRequest{})
			},
		},
	}
	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := setup(t)
			defer f.shutDown()

			got, err := tc.rpc(f)
			if !reflect.ValueOf(got).IsNil() {
				t.Errorf("%s(ctx, {}) gave result %s, want nil", tc.name, got)
			}
			if err == nil {
				t.Errorf("%s(ctx, {}) = nil; expected Unimplemented error got nil", tc.name)
			} else if s, ok := status.FromError(err); ok && s.Code() != codes.Unimplemented {
				t.Errorf("%s(ctx, {}) = %v; expected Unimplemented error, got %v", tc.name, err, s.Code())
			} else if !ok {
				t.Errorf("%s(ctx, {}) = %v; expected Unimplemented error (status.FromError failed)", tc.name, err)
			}
		})
	}
}

func TestNonStreamingRpcRetriesSleep(t *testing.T) {
	t.Parallel()
	f := setup(t)
	defer f.shutDown()
	f.fake.sleepDelay = time.Second
	client.RPCTimeouts(map[string]time.Duration{"QueryWriteStatus": 500 * time.Millisecond}).Apply(f.client)

	got, err := f.client.QueryWriteStatus(f.ctx, &bspb.QueryWriteStatusRequest{})
	if got != nil {
		t.Errorf("client.QueryWriteStatus(ctx, digest) gave result %s, want nil", got)
	}
	if err == nil {
		t.Error("QueryWriteStatus(ctx, {}) = nil; expected Unimplemented error got nil")
	} else if s, ok := status.FromError(err); ok && s.Code() != codes.Unimplemented {
		t.Errorf("QueryWriteStatus(ctx, {}) = %v; expected Unimplemented error, got %v", err, s.Code())
	} else if !ok {
		t.Errorf("QueryWriteStatus(ctx, {}) = %v; expected Unimplemented error (status.FromError failed)", err)
	}
}
