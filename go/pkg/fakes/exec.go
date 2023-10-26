package fakes

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/client"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	// Redundant imports are required for the google3 mirror. Aliases should not be changed.
	regrpc "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	oppb "google.golang.org/genproto/googleapis/longrunning"
	anypb "google.golang.org/protobuf/types/known/anypb"
)

// Exec implements the complete RE execution interface for a single execution, returning a fixed
// result or an error.
type Exec struct {
	// Execution will check the action cache first, and update the action cache upon completion.
	ac *ActionCache
	// The action will be fetched from the CAS at start of execution, and outputs will be put in the
	// CAS upon execution completion.
	cas *CAS
	// Fake result of an execution.
	// The returned completed result, if any.
	ActionResult *repb.ActionResult
	// Returned completed execution status, if not Ok.
	Status *status.Status
	// Whether action was fake-fetched from the action cache upon execution (simulates a race between
	// two executions).
	Cached bool
	// Any blobs that will be put in the CAS after the fake execution completes.
	OutputBlobs [][]byte
	// Name of the logstream to write stdout to.
	StdOutStreamName string
	// Name of the logstream to write stderr to.
	StdErrStreamName string
	// Number of Execute calls.
	numExecCalls int32
	// Used for errors.
	t testing.TB
	// The digest of the fake action.
	adg digest.Digest
}

// NewExec returns a new empty Exec.
func NewExec(t testing.TB, ac *ActionCache, cas *CAS) *Exec {
	c := &Exec{t: t, ac: ac, cas: cas}
	c.Clear()
	return c
}

// Clear removes all preset results from the fake.
func (s *Exec) Clear() {
	s.ActionResult = nil
	s.Status = nil
	s.Cached = false
	s.OutputBlobs = nil
	atomic.StoreInt32(&s.numExecCalls, 0)
}

// ExecuteCalls returns the total number of Execute calls.
func (s *Exec) ExecuteCalls() int {
	return int(atomic.LoadInt32(&s.numExecCalls))
}

func fakeOPName(adg digest.Digest) string {
	return "fake-action-" + adg.String()
}

func (s *Exec) fakeExecution(dg digest.Digest, skipCacheLookup bool) (*oppb.Operation, error) {
	ar := s.ActionResult
	st := s.Status
	cached := s.Cached
	// Check action cache first, unless instructed not to.
	if !skipCacheLookup {
		cr := s.ac.Get(dg)
		if cr != nil {
			ar = cr
			st = nil
			cached = true
		}
	}
	// Fetch action from CAS.
	blob, ok := s.cas.Get(dg)
	if !ok {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("action blob with digest %v not in the cas", dg))
	}
	apb := &repb.Action{}
	if err := proto.Unmarshal(blob, apb); err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("error unmarshalling %v as Action", blob))
	}
	if !apb.DoNotCache {
		s.ac.Put(dg, ar)
	}
	for _, out := range s.OutputBlobs {
		s.cas.Put(out)
	}
	execResp := &repb.ExecuteResponse{
		Result:       ar,
		Status:       st.Proto(),
		CachedResult: cached,
	}
	any, err := anypb.New(execResp)
	if err != nil {
		return nil, err
	}
	return &oppb.Operation{
		Name:   fakeOPName(dg),
		Done:   true,
		Result: &oppb.Operation_Response{Response: any},
	}, nil
}

// GetCapabilities returns the fake capabilities.
func (s *Exec) GetCapabilities(ctx context.Context, req *repb.GetCapabilitiesRequest) (res *repb.ServerCapabilities, err error) {
	dgFn := digest.GetDigestFunction()
	res = &repb.ServerCapabilities{
		ExecutionCapabilities: &repb.ExecutionCapabilities{
			DigestFunction: dgFn,
			ExecEnabled:    true,
		},
		CacheCapabilities: &repb.CacheCapabilities{
			DigestFunctions: []repb.DigestFunction_Value{dgFn},
			ActionCacheUpdateCapabilities: &repb.ActionCacheUpdateCapabilities{
				UpdateEnabled: true,
			},
			MaxBatchTotalSizeBytes:      client.DefaultMaxBatchSize,
			SymlinkAbsolutePathStrategy: repb.SymlinkAbsolutePathStrategy_DISALLOWED,
		},
	}
	return res, nil
}

// Execute returns the saved result ActionResult, or a Status. It also puts it in the action cache
// unless the execute request specified
func (s *Exec) Execute(req *repb.ExecuteRequest, stream regrpc.Execution_ExecuteServer) (err error) {
	dg, err := digest.NewFromProto(req.ActionDigest)
	if err != nil {
		return status.Error(codes.InvalidArgument, fmt.Sprintf("invalid digest received: %v", req.ActionDigest))
	}
	if dg != s.adg {
		s.t.Errorf("unexpected action digest received by fake: expected %v, got %v", s.adg, dg)
		return status.Error(codes.InvalidArgument, fmt.Sprintf("unexpected digest received: %v", req.ActionDigest))
	}
	if s.StdOutStreamName != "" || s.StdErrStreamName != "" {
		md, err := anypb.New(&repb.ExecuteOperationMetadata{
			StdoutStreamName: s.StdOutStreamName,
			StderrStreamName: s.StdErrStreamName,
		})
		if err != nil {
			return err
		}
		if err := stream.Send(&oppb.Operation{Name: fakeOPName(dg), Metadata: md}); err != nil {
			return err
		}
	}
	if op, err := s.fakeExecution(dg, req.SkipCacheLookup); err != nil {
		return err
	} else if err = stream.Send(op); err != nil {
		return err
	}
	atomic.AddInt32(&s.numExecCalls, 1)
	return nil
}

func (s *Exec) WaitExecution(req *repb.WaitExecutionRequest, stream regrpc.Execution_WaitExecutionServer) (err error) {
	if req.Name != fakeOPName(s.adg) {
		return status.Errorf(codes.NotFound, "requested operation %v not found", req.Name)
	}
	if op, err := s.fakeExecution(s.adg, true); err != nil {
		return err
	} else {
		return stream.Send(op)
	}
}
