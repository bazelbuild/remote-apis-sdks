package client_test

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/client"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/golang/protobuf/ptypes"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	regrpc "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	emptypb "github.com/golang/protobuf/ptypes/empty"
	bsgrpc "google.golang.org/genproto/googleapis/bytestream"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	opgrpc "google.golang.org/genproto/googleapis/longrunning"
	oppb "google.golang.org/genproto/googleapis/longrunning"
)

type flakyServer struct {
	// TODO(jsharpe): This is a hack to work around WaitOperation not existing in some versions of
	// the long running operations API that we need to support.
	opgrpc.OperationsServer
	mu               sync.RWMutex   // protects numCalls.
	numCalls         map[string]int // A counter of calls the server encountered thus far, by method.
	retriableForever bool           // Set to true to make the flaky server return a retriable error forever, rather than eventually a non-retriable error.
}

func (f *flakyServer) incNumCalls(method string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.numCalls[method]++
	return f.numCalls[method]
}

func (f *flakyServer) Write(stream bsgrpc.ByteStream_WriteServer) error {
	numCalls := f.incNumCalls("Write")
	if numCalls < 3 {
		return status.Error(codes.Canceled, "transient error!")
	}

	req, err := stream.Recv()
	if err != nil {
		return err
	}
	// Verify that the client sends the first chunk, because they should retry from scratch.
	if req.WriteOffset != 0 || req.FinishWrite {
		return status.Error(codes.FailedPrecondition, fmt.Sprintf("expected first chunk, got %v", req))
	}
	if numCalls < 5 {
		return status.Error(codes.ResourceExhausted, "another transient error!")
	}
	return stream.SendAndClose(&bspb.WriteResponse{CommittedSize: 4})
}

func (f *flakyServer) Read(req *bspb.ReadRequest, stream bsgrpc.ByteStream_ReadServer) error {
	numCalls := f.incNumCalls("Read")
	if numCalls < 3 {
		return status.Error(codes.Canceled, "transient error!")
	}
	if numCalls < 4 {
		// We send the 4 byte test blob in two chunks.
		if err := stream.Send(&bspb.ReadResponse{Data: []byte("bl")}); err != nil {
			return err
		}
		return status.Error(codes.ResourceExhausted, "another transient error!")
	}
	// Client now will only ask for the remaining two bytes.
	if numCalls < 5 {
		return status.Error(codes.Aborted, "yet another transient error!")
	}
	return stream.Send(&bspb.ReadResponse{Data: []byte("ob")})
}

func (f *flakyServer) flakeAndFail(method string) error {
	numCalls := f.incNumCalls(method)
	if numCalls == 1 {
		time.Sleep(2 * time.Second)
		// The error we return here should not matter; the deadline should have passed by now and the
		// retrier should retry DeadlineExceeded.
		return status.Error(codes.InvalidArgument, "non retriable error")
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
		return status.Error(codes.ResourceExhausted, "another transient error!")
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
	return status.Error(codes.ResourceExhausted, "another transient error!")
}

func (f *flakyServer) WaitExecution(req *repb.WaitExecutionRequest, stream regrpc.Execution_WaitExecutionServer) error {
	numCalls := f.incNumCalls("WaitExecution")
	if numCalls < 2 {
		return status.Error(codes.Canceled, "transient error!")
	}
	if numCalls < 4 {
		stream.Send(&oppb.Operation{Done: false, Name: "dummy"})
		return status.Error(codes.ResourceExhausted, "another transient error!")
	}
	// Execute (above) will fail twice (and be retried twice) before ExecuteAndWait() switches to
	// WaitExecution. WaitExecution will fail 4 more times more before succeeding, for a total of 6 retries.
	execResp := &repb.ExecuteResponse{Status: status.New(codes.Aborted, "transient operation failure!").Proto()}
	any, e := ptypes.MarshalAny(execResp)
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
	f.fake = &flakyServer{numCalls: make(map[string]int)}
	bsgrpc.RegisterByteStreamServer(f.server, f.fake)
	regrpc.RegisterActionCacheServer(f.server, f.fake)
	regrpc.RegisterContentAddressableStorageServer(f.server, f.fake)
	regrpc.RegisterExecutionServer(f.server, f.fake)
	opgrpc.RegisterOperationsServer(f.server, f.fake)
	go f.server.Serve(f.listener)
	f.client, err = client.NewClient(f.ctx, instance, client.DialParams{
		Service:    f.listener.Addr().String(),
		NoSecurity: true,
	}, client.ChunkMaxSize(2), client.RPCTimeout(time.Second))
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

func TestWriteRetries(t *testing.T) {
	f := setup(t)
	defer f.shutDown()

	blob := []byte("blob")
	gotDg, err := f.client.WriteBlob(f.ctx, blob)
	if err != nil {
		t.Errorf("client.WriteBlob(ctx, blob) gave error %s, wanted nil", err)
	}
	if diff := cmp.Diff(digest.NewFromBlob(blob), gotDg); diff != "" {
		t.Errorf("client.WriteBlob(ctx, blob) had diff on digest returned (want -> got):\n%s", diff)
	}
}

func TestReadRetries(t *testing.T) {
	f := setup(t)
	defer f.shutDown()

	blob := []byte("blob")
	got, err := f.client.ReadBlob(f.ctx, digest.NewFromBlob(blob))
	if err != nil {
		t.Errorf("client.ReadBlob(ctx, digest) gave error %s, want nil", err)
	}
	if diff := cmp.Diff(blob, got, cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("client.ReadBlob(ctx, digest) gave diff (-want, +got):\n%s", diff)
	}
}

func assertCanceledErr(t *testing.T, err error, method string) {
	t.Helper()
	if err == nil {
		t.Errorf("%s(ctx, {}) = nil; expected Canceled error got nil", method)
	} else if s, ok := status.FromError(err); ok && s.Code() != codes.Canceled {
		t.Errorf("%s(ctx, {}) = %v; expected Canceled error, got %v", method, err, s.Code())
	} else if !ok {
		t.Errorf("%s(ctx, {}) = %v; expected Canceled error (status.FromError failed)", method, err)
	}
}

func assertUnimplementedErr(t *testing.T, err error, method string) {
	t.Helper()
	if err == nil {
		t.Errorf("%s(ctx, {}) = nil; expected Unimplemented error got nil", method)
	} else if s, ok := status.FromError(err); ok && s.Code() != codes.Unimplemented {
		t.Errorf("%s(ctx, {}) = %v; expected Unimplemented error, got %v", method, err, s.Code())
	} else if !ok {
		t.Errorf("%s(ctx, {}) = %v; expected Unimplemented error (status.FromError failed)", method, err)
	}
}

func TestQueryWriteStatusRetries(t *testing.T) {
	f := setup(t)
	defer f.shutDown()

	got, err := f.client.QueryWriteStatus(f.ctx, &bspb.QueryWriteStatusRequest{})
	if got != nil {
		t.Errorf("client.QueryWriteStatus(ctx, digest) gave result %s, want nil", got)
	}
	assertUnimplementedErr(t, err, "client.QueryWriteStatus")
}

func TestGetActionResultRetries(t *testing.T) {
	f := setup(t)
	defer f.shutDown()

	got, err := f.client.GetActionResult(f.ctx, &repb.GetActionResultRequest{})
	if got != nil {
		t.Errorf("client.GetActionResult(ctx, digest) gave result %s, want nil", got)
	}
	assertUnimplementedErr(t, err, "client.GetActionResult")
}

func TestUpdateActionResultRetries(t *testing.T) {
	f := setup(t)
	defer f.shutDown()

	got, err := f.client.UpdateActionResult(f.ctx, &repb.UpdateActionResultRequest{})
	if got != nil {
		t.Errorf("client.UpdateActionResult(ctx, digest) gave result %s, want nil", got)
	}
	assertUnimplementedErr(t, err, "client.UpdateActionResult")
}

func TestFindMissingBlobsRetries(t *testing.T) {
	f := setup(t)
	defer f.shutDown()

	got, err := f.client.FindMissingBlobs(f.ctx, &repb.FindMissingBlobsRequest{})
	if got != nil {
		t.Errorf("client.FindMissingBlobs(ctx, digest) gave result %s, want nil", got)
	}
	assertUnimplementedErr(t, err, "client.FindMissingBlobs")
}

func TestBatchUpdateBlobsRpcRetries(t *testing.T) {
	f := setup(t)
	defer f.shutDown()

	got, err := f.client.BatchUpdateBlobs(f.ctx, &repb.BatchUpdateBlobsRequest{})
	if got != nil {
		t.Errorf("client.BatchUpdateBlobs(ctx, digest) gave result %s, want nil", got)
	}
	assertUnimplementedErr(t, err, "client.BatchUpdateBlobs")
}

func TestBatchWriteBlobsRpcRetries(t *testing.T) {
	f := setup(t)
	defer f.shutDown()

	blobs := map[digest.Digest][]byte{
		digest.TestNew("a", 1): []byte{1},
		digest.TestNew("b", 1): []byte{2},
	}
	err := f.client.BatchWriteBlobs(f.ctx, blobs)
	assertUnimplementedErr(t, err, "client.BatchWriteBlobs")
}

// Verify for one arbitrary method that when retries are exhausted, we get the retriable error code
// back.
func TestBatchWriteBlobsRpcRetriesExhausted(t *testing.T) {
	f := setup(t)
	f.fake.retriableForever = true
	defer f.shutDown()

	blobs := map[digest.Digest][]byte{
		digest.TestNew("a", 1): []byte{1},
		digest.TestNew("b", 1): []byte{2},
	}
	err := f.client.BatchWriteBlobs(f.ctx, blobs)
	assertCanceledErr(t, err, "client.BatchWriteBlobs")
}

func TestGetTreeRetries(t *testing.T) {
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

func TestGetOperationRetries(t *testing.T) {
	f := setup(t)
	defer f.shutDown()

	got, err := f.client.GetOperation(f.ctx, &oppb.GetOperationRequest{})
	if got != nil {
		t.Errorf("client.GetOperation(ctx, digest) gave result %s, want nil", got)
	}
	if err == nil {
		t.Errorf("client.GetOperation(ctx, {}) = nil; expected Unimplemented error got nil")
	} else if s, ok := status.FromError(err); ok && s.Code() != codes.Unimplemented {
		t.Errorf("client.GetOperation(ctx, {}) = %v; expected Unimplemented error, got %v", err, s.Code())
	}
}

func TestListOperationsRetries(t *testing.T) {
	f := setup(t)
	defer f.shutDown()

	got, err := f.client.ListOperations(f.ctx, &oppb.ListOperationsRequest{})
	if got != nil {
		t.Errorf("client.ListOperations(ctx, digest) gave result %s, want nil", got)
	}
	if err == nil {
		t.Errorf("client.ListOperations(ctx, {}) = nil; expected Unimplemented error got nil")
	} else if s, ok := status.FromError(err); ok && s.Code() != codes.Unimplemented {
		t.Errorf("client.ListOperations(ctx, {}) = %v; expected Unimplemented error, got %v", err, s.Code())
	}
}

func TestCancelOperationRetries(t *testing.T) {
	f := setup(t)
	defer f.shutDown()

	got, err := f.client.CancelOperation(f.ctx, &oppb.CancelOperationRequest{})
	if got != nil {
		t.Errorf("client.CancelOperation(ctx, digest) gave result %s, want nil", got)
	}
	if err == nil {
		t.Errorf("client.CancelOperation(ctx, {}) = nil; expected Unimplemented error got nil")
	} else if s, ok := status.FromError(err); ok && s.Code() != codes.Unimplemented {
		t.Errorf("client.CancelOperation(ctx, {}) = %v; expected Unimplemented error, got %v", err, s.Code())
	}
}

func TestDeleteOperationRetries(t *testing.T) {
	f := setup(t)
	defer f.shutDown()

	got, err := f.client.DeleteOperation(f.ctx, &oppb.DeleteOperationRequest{})
	if got != nil {
		t.Errorf("client.DeleteOperation(ctx, digest) gave result %s, want nil", got)
	}
	if err == nil {
		t.Errorf("client.DeleteOperation(ctx, {}) = nil; expected Unimplemented error got nil")
	} else if s, ok := status.FromError(err); ok && s.Code() != codes.Unimplemented {
		t.Errorf("client.DeleteOperation(ctx, {}) = %v; expected Unimplemented error, got %v", err, s.Code())
	}
}

func TestExecuteAndWaitRetries(t *testing.T) {
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
