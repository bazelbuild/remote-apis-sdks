package client_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/client"
	"github.com/bazelbuild/remote-apis-sdks/go/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/retry"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	regrpc "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	spb "google.golang.org/genproto/googleapis/rpc/status"
)

type flakyBatchServer struct {
	numErrors      int // A counter of errors the server has returned thus far.
	updateRequests []*repb.BatchUpdateBlobsRequest
	readRequests   []*repb.BatchReadBlobsRequest
}

func (f *flakyBatchServer) FindMissingBlobs(ctx context.Context, req *repb.FindMissingBlobsRequest) (*repb.FindMissingBlobsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (f *flakyBatchServer) BatchReadBlobs(ctx context.Context, req *repb.BatchReadBlobsRequest) (*repb.BatchReadBlobsResponse, error) {
	f.readRequests = append(f.readRequests, req)
	if f.numErrors < 1 {
		f.numErrors++
		resp := &repb.BatchReadBlobsResponse{
			Responses: []*repb.BatchReadBlobsResponse_Response{
				{Digest: digest.TestNew("a", 1).ToProto(), Status: &spb.Status{Code: int32(codes.OK)}, Data: []byte{1}},
				// all retriable errors.
				{Digest: digest.TestNew("b", 1).ToProto(), Status: &spb.Status{Code: int32(codes.Internal)}},
				{Digest: digest.TestNew("c", 1).ToProto(), Status: &spb.Status{Code: int32(codes.Canceled)}},
				{Digest: digest.TestNew("d", 1).ToProto(), Status: &spb.Status{Code: int32(codes.Aborted)}},
			},
		}
		return resp, nil
	}
	if f.numErrors < 2 {
		f.numErrors++
		resp := &repb.BatchReadBlobsResponse{
			Responses: []*repb.BatchReadBlobsResponse_Response{
				{Digest: digest.TestNew("b", 1).ToProto(), Status: &spb.Status{Code: int32(codes.OK)}, Data: []byte{2}},
				// all retriable errors.
				{Digest: digest.TestNew("c", 1).ToProto(), Status: &spb.Status{Code: int32(codes.Internal)}},
				{Digest: digest.TestNew("d", 1).ToProto(), Status: &spb.Status{Code: int32(codes.Canceled)}},
			},
		}
		return resp, nil
	}
	if f.numErrors < 3 {
		f.numErrors++
		resp := &repb.BatchReadBlobsResponse{
			Responses: []*repb.BatchReadBlobsResponse_Response{
				// One non-retriable error.
				{Digest: digest.TestNew("c", 1).ToProto(), Status: &spb.Status{Code: int32(codes.Internal)}},
				{Digest: digest.TestNew("d", 1).ToProto(), Status: &spb.Status{Code: int32(codes.PermissionDenied)}},
			},
		}
		return resp, nil
	}
	// Will not be reached.
	return nil, status.Error(codes.Unimplemented, "")
}

func (f *flakyBatchServer) GetTree(req *repb.GetTreeRequest, stream regrpc.ContentAddressableStorage_GetTreeServer) error {
	return status.Error(codes.Unimplemented, "")
}

func (f *flakyBatchServer) BatchUpdateBlobs(ctx context.Context, req *repb.BatchUpdateBlobsRequest) (*repb.BatchUpdateBlobsResponse, error) {
	f.updateRequests = append(f.updateRequests, req)
	if f.numErrors < 1 {
		f.numErrors++
		resp := &repb.BatchUpdateBlobsResponse{
			Responses: []*repb.BatchUpdateBlobsResponse_Response{
				{Digest: digest.TestNew("a", 1).ToProto(), Status: &spb.Status{Code: int32(codes.OK)}},
				// all retriable errors.
				{Digest: digest.TestNew("b", 1).ToProto(), Status: &spb.Status{Code: int32(codes.Internal)}},
				{Digest: digest.TestNew("c", 1).ToProto(), Status: &spb.Status{Code: int32(codes.Canceled)}},
				{Digest: digest.TestNew("d", 1).ToProto(), Status: &spb.Status{Code: int32(codes.Aborted)}},
			},
		}
		return resp, nil
	}
	if f.numErrors < 2 {
		f.numErrors++
		resp := &repb.BatchUpdateBlobsResponse{
			Responses: []*repb.BatchUpdateBlobsResponse_Response{
				{Digest: digest.TestNew("b", 1).ToProto(), Status: &spb.Status{Code: int32(codes.OK)}},
				// all retriable errors.
				{Digest: digest.TestNew("c", 1).ToProto(), Status: &spb.Status{Code: int32(codes.Internal)}},
				{Digest: digest.TestNew("d", 1).ToProto(), Status: &spb.Status{Code: int32(codes.Canceled)}},
			},
		}
		return resp, nil
	}
	if f.numErrors < 3 {
		f.numErrors++
		resp := &repb.BatchUpdateBlobsResponse{
			Responses: []*repb.BatchUpdateBlobsResponse_Response{
				// One non-retriable error.
				{Digest: digest.TestNew("c", 1).ToProto(), Status: &spb.Status{Code: int32(codes.Internal)}},
				{Digest: digest.TestNew("d", 1).ToProto(), Status: &spb.Status{Code: int32(codes.PermissionDenied)}},
			},
		}
		return resp, nil
	}
	// Will not be reached.
	return nil, status.Error(codes.Unimplemented, "")
}

func TestBatchUpdateBlobsIndividualRequestRetries(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	server := grpc.NewServer()
	fake := &flakyBatchServer{}
	regrpc.RegisterContentAddressableStorageServer(server, fake)
	go server.Serve(listener)
	ctx := context.Background()
	client, err := client.NewClient(ctx, instance, client.DialParams{
		Service:    listener.Addr().String(),
		NoSecurity: true,
	}, client.RetryTransient())
	if err != nil {
		t.Fatalf("Error connecting to server: %v", err)
	}
	defer server.Stop()
	defer listener.Close()
	defer client.Close()

	blobs := map[digest.Digest][]byte{
		digest.TestNew("a", 1): []byte{1},
		digest.TestNew("b", 1): []byte{2},
		digest.TestNew("c", 1): []byte{3},
		digest.TestNew("d", 1): []byte{4},
	}
	err = client.BatchWriteBlobs(ctx, blobs)
	if err == nil {
		t.Errorf("client.BatchWriteBlobs(ctx, blobs) = nil; expected PermissionDenied error got nil")
	} else if s, ok := status.FromError(err); ok && s.Code() != codes.PermissionDenied {
		t.Errorf("client.BatchWriteBlobs(ctx, blobs) = %v; expected PermissionDenied error, got %v", err, s.Code())
	}
	wantRequests := []*repb.BatchUpdateBlobsRequest{
		{
			Requests: []*repb.BatchUpdateBlobsRequest_Request{
				{Digest: digest.TestNew("a", 1).ToProto(), Data: []byte{1}},
				{Digest: digest.TestNew("b", 1).ToProto(), Data: []byte{2}},
				{Digest: digest.TestNew("c", 1).ToProto(), Data: []byte{3}},
				{Digest: digest.TestNew("d", 1).ToProto(), Data: []byte{4}},
			},
			InstanceName: "instance",
		},
		{
			Requests: []*repb.BatchUpdateBlobsRequest_Request{
				{Digest: digest.TestNew("b", 1).ToProto(), Data: []byte{2}},
				{Digest: digest.TestNew("c", 1).ToProto(), Data: []byte{3}},
				{Digest: digest.TestNew("d", 1).ToProto(), Data: []byte{4}},
			},
			InstanceName: "instance",
		},
		{
			Requests: []*repb.BatchUpdateBlobsRequest_Request{
				{Digest: digest.TestNew("c", 1).ToProto(), Data: []byte{3}},
				{Digest: digest.TestNew("d", 1).ToProto(), Data: []byte{4}},
			},
			InstanceName: "instance",
		},
	}
	if len(fake.updateRequests) != len(wantRequests) {
		t.Errorf("client.BatchWriteBlobs(ctx, blobs) wrong number of requests; expected %d, got %d", len(wantRequests), len(fake.updateRequests))
	}
	for i, req := range wantRequests {
		if diff := cmp.Diff(req, fake.updateRequests[i], cmpopts.SortSlices(func(a, b interface{}) bool {
			return fmt.Sprint(a) < fmt.Sprint(b)
		})); diff != "" {
			t.Errorf("client.BatchWriteBlobs(ctx, blobs) diff on request at index %d (want -> got):\n%s", i, diff)
		}
	}
}

func TestBatchReadBlobsIndividualRequestRetries(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	server := grpc.NewServer()
	fake := &flakyBatchServer{}
	regrpc.RegisterContentAddressableStorageServer(server, fake)
	go server.Serve(listener)
	ctx := context.Background()
	client, err := client.NewClient(ctx, instance, client.DialParams{
		Service:    listener.Addr().String(),
		NoSecurity: true,
	}, client.RetryTransient())
	if err != nil {
		t.Fatalf("Error connecting to server: %v", err)
	}
	defer server.Stop()
	defer listener.Close()
	defer client.Close()

	digests := []digest.Digest{
		digest.TestNew("a", 1),
		digest.TestNew("b", 1),
		digest.TestNew("c", 1),
		digest.TestNew("d", 1),
	}
	wantBlobs := map[digest.Digest][]byte{
		digest.TestNew("a", 1): []byte{1},
		digest.TestNew("b", 1): []byte{2},
	}
	gotBlobs, err := client.BatchDownloadBlobs(ctx, digests)
	if err == nil {
		t.Errorf("client.BatchDownloadBlobs(ctx, digests) = nil; expected PermissionDenied error got nil")
	} else if s, ok := status.FromError(err); ok && s.Code() != codes.PermissionDenied {
		t.Errorf("client.BatchDownloadBlobs(ctx, digests) = %v; expected PermissionDenied error, got %v", err, s.Code())
	}
	if diff := cmp.Diff(wantBlobs, gotBlobs); diff != "" {
		t.Errorf("client.BatchDownloadBlobs(ctx, digests) had diff (want -> got):\n%s", diff)
	}
	wantRequests := []*repb.BatchReadBlobsRequest{
		{
			Digests: []*repb.Digest{
				digest.TestNew("a", 1).ToProto(),
				digest.TestNew("b", 1).ToProto(),
				digest.TestNew("c", 1).ToProto(),
				digest.TestNew("d", 1).ToProto(),
			},
			InstanceName: "instance",
		},
		{
			Digests: []*repb.Digest{
				digest.TestNew("b", 1).ToProto(),
				digest.TestNew("c", 1).ToProto(),
				digest.TestNew("d", 1).ToProto(),
			},
			InstanceName: "instance",
		},
		{
			Digests: []*repb.Digest{
				digest.TestNew("c", 1).ToProto(),
				digest.TestNew("d", 1).ToProto(),
			},
			InstanceName: "instance",
		},
	}
	if len(fake.readRequests) != len(wantRequests) {
		t.Errorf("client.BatchWriteBlobs(ctx, blobs) wrong number of requests; expected %d, got %d", len(wantRequests), len(fake.readRequests))
	}
	for i, req := range wantRequests {
		if diff := cmp.Diff(req, fake.readRequests[i], cmpopts.SortSlices(func(a, b interface{}) bool {
			return fmt.Sprint(a) < fmt.Sprint(b)
		})); diff != "" {
			t.Errorf("client.BatchWriteBlobs(ctx, blobs) diff on request at index %d (want -> got):\n%s", i, diff)
		}
	}
}

type sleepyBatchServer struct {
	timeout        time.Duration
	numErrors      int // A counter of DEADLINE_EXCEEDED errors the server has returned thus far.
	updateRequests int
	readRequests   int
}

func (f *sleepyBatchServer) FindMissingBlobs(ctx context.Context, req *repb.FindMissingBlobsRequest) (*repb.FindMissingBlobsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "")
}

func (f *sleepyBatchServer) GetTree(req *repb.GetTreeRequest, stream regrpc.ContentAddressableStorage_GetTreeServer) error {
	return status.Error(codes.Unimplemented, "")
}

func (f *sleepyBatchServer) BatchReadBlobs(ctx context.Context, req *repb.BatchReadBlobsRequest) (*repb.BatchReadBlobsResponse, error) {
	f.readRequests++
	if f.numErrors < 3 {
		f.numErrors++
		time.Sleep(f.timeout)
		return &repb.BatchReadBlobsResponse{}, nil
	}
	// Will not be reached.
	return nil, status.Error(codes.Unimplemented, "")
}

func (f *sleepyBatchServer) BatchUpdateBlobs(ctx context.Context, req *repb.BatchUpdateBlobsRequest) (*repb.BatchUpdateBlobsResponse, error) {
	f.updateRequests++
	if f.numErrors < 3 {
		f.numErrors++
		time.Sleep(f.timeout)
		return &repb.BatchUpdateBlobsResponse{}, nil
	}
	// Will not be reached.
	return nil, status.Error(codes.Unimplemented, "")
}

func TestBatchReadBlobsDeadlineExceededRetries(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	server := grpc.NewServer()
	fake := &sleepyBatchServer{timeout: 200 * time.Millisecond}
	regrpc.RegisterContentAddressableStorageServer(server, fake)
	go server.Serve(listener)
	ctx := context.Background()
	retrier := client.RetryTransient()
	retrier.Backoff = retry.Immediately(retry.Attempts(3))
	client, err := client.NewClient(ctx, instance, client.DialParams{
		Service:    listener.Addr().String(),
		NoSecurity: true,
	}, retrier, client.RPCTimeout(100*time.Millisecond))
	if err != nil {
		t.Fatalf("Error connecting to server: %v", err)
	}
	defer server.Stop()
	defer listener.Close()
	defer client.Close()

	digests := []digest.Digest{digest.TestNew("a", 1)}
	_, err = client.BatchDownloadBlobs(ctx, digests)
	if err == nil {
		t.Errorf("client.BatchDownloadBlobs(ctx, digests) = nil; expected DeadlineExceeded error got nil")
	} else if s, ok := status.FromError(err); ok && s.Code() != codes.DeadlineExceeded {
		t.Errorf("client.BatchDownloadBlobs(ctx, digests) = %v; expected DeadlineExceeded error, got %v", err, s.Code())
	}
	wantRequests := 3
	if fake.readRequests != wantRequests {
		t.Errorf("client.BatchDownloadBlobs(ctx, digests) resulted in %v requests, expected %v", fake.readRequests, wantRequests)
	}
}

func TestBatchUpdateBlobsDeadlineExceededRetries(t *testing.T) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	server := grpc.NewServer()
	fake := &sleepyBatchServer{timeout: 200 * time.Millisecond}
	regrpc.RegisterContentAddressableStorageServer(server, fake)
	go server.Serve(listener)
	ctx := context.Background()
	retrier := client.RetryTransient()
	retrier.Backoff = retry.Immediately(retry.Attempts(3))
	client, err := client.NewClient(ctx, instance, client.DialParams{
		Service:    listener.Addr().String(),
		NoSecurity: true,
	}, retrier, client.RPCTimeout(100*time.Millisecond))
	if err != nil {
		t.Fatalf("Error connecting to server: %v", err)
	}
	defer server.Stop()
	defer listener.Close()
	defer client.Close()

	blobs := map[digest.Digest][]byte{digest.TestNew("a", 1): []byte{1}}
	err = client.BatchWriteBlobs(ctx, blobs)
	if err == nil {
		t.Errorf("client.BatchWriteBlobs(ctx, blobs) = nil; expected DeadlineExceeded error got nil")
	} else if s, ok := status.FromError(err); ok && s.Code() != codes.DeadlineExceeded {
		t.Errorf("client.BatchWriteBlobs(ctx, blobs) = %v; expected DeadlineExceeded error, got %v", err, s.Code())
	}
	wantRequests := 3
	if fake.updateRequests != wantRequests {
		t.Errorf("client.BatchWriteBlobs(ctx, blobs) resulted in %v requests, expected %v", fake.updateRequests, wantRequests)
	}
}
