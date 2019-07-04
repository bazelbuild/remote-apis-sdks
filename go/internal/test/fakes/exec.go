package fakes

import (
	"fmt"

	"github.com/bazelbuild/remote-apis-sdks/go/digest"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	regrpc "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	oppb "google.golang.org/genproto/googleapis/longrunning"
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
	// Number of Execute calls.
	numExecCalls int
}

// NewExec returns a new empty Exec.
func NewExec(ac *ActionCache, cas *CAS) *Exec {
	c := &Exec{ac: ac, cas: cas}
	c.Clear()
	return c
}

// Clear removes all preset results from the fake.
func (s *Exec) Clear() {
	s.ActionResult = nil
	s.Status = nil
	s.Cached = false
	s.OutputBlobs = nil
	s.numExecCalls = 0
}

// ExecuteCalls returns the total number of Execute calls.
func (s *Exec) ExecuteCalls() int {
	return s.numExecCalls
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
	any, err := ptypes.MarshalAny(execResp)
	if err != nil {
		return nil, err
	}
	return &oppb.Operation{
		Name:   "fake",
		Done:   true,
		Result: &oppb.Operation_Response{Response: any},
	}, nil
}

// Execute returns the saved result ActionResult, or a Status. It also puts it in the action cache
// unless the execute request specified
func (s *Exec) Execute(req *repb.ExecuteRequest, stream regrpc.Execution_ExecuteServer) (err error) {
	dg, err := digest.NewFromProto(req.ActionDigest)
	if err != nil {
		return status.Error(codes.InvalidArgument, fmt.Sprintf("invalid digest received: %v", req.ActionDigest))
	}
	if op, err := s.fakeExecution(dg, req.SkipCacheLookup); err != nil {
		return err
	} else if err = stream.Send(op); err != nil {
		return err
	}
	s.numExecCalls++
	return nil
}

// WaitExecution is not implemented on this fake.
func (s *Exec) WaitExecution(req *repb.WaitExecutionRequest, stream regrpc.Execution_WaitExecutionServer) (err error) {
	return status.Error(codes.Unimplemented, "method WaitExecution not implemented by test fake")
}
