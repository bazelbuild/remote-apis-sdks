package client

import (
	"context"
	"errors"
	"io"
	"sort"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	log "github.com/golang/glog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	// Redundant imports are required for the google3 mirror. Aliases should not be changed.
	regrpc "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	gerrors "github.com/pkg/errors"
	oppb "google.golang.org/genproto/googleapis/longrunning"
	dpb "google.golang.org/protobuf/types/known/durationpb"
)

const (
	containerImagePropertyName = "container-image"
)

// Action encodes the full details of an action to be sent to the remote execution service for
// execution. It corresponds roughly, but not exactly, to the Action proto used by the Remote
// Execution API.
type Action struct {
	// Args are the command-line arguments to start the process. The first argument is the process
	// name, and the rest are its arguments.
	Args []string
	// EnvVars are the variables to add to the process's environment.
	EnvVars map[string]string
	// InputRoot and InputFiles contain the details of the input tree, in remote execution format.
	// They should normally be constructed through the PackageTree function.
	InputRoot  digest.Digest
	InputFiles map[digest.Digest][]byte
	// OutputFiles is a list of output files requested (full paths).
	OutputFiles []string
	// OutputDirs is a list of output directories requested (full paths).
	OutputDirs []string
	// Docker image is a docker:// URL to the docker image in which execution will take place.
	DockerImage string
	// Timeout is the maximum execution time for the action. Note that it's not an overall timeout on
	// the process, since there may be additional time for transferring files, waiting for a worker to
	// become available, or other overhead.
	//
	// If 0, the server's default timeout is used.
	Timeout time.Duration
	// DoNotCache, if true, indicates that the result of this action should never be cached. It
	// implies SkipCache.
	DoNotCache bool
	// SkipCache, if true, indicates that this action should be executed even if there is a copy of
	// its result in the action cache that could be used instead.
	SkipCache bool
}

// ExecuteAction performs all of the steps necessary to execute an action, including checking the
// cache if applicable, uploading necessary protos and inputs to the CAS, queueing the action, and
// waiting for the result.
//
// Execute may block for a long time while the action is in progress. Currently, two-phase
// queue-wait is not supported; the token necessary to query the job is not provided to users.
//
// This method MAY return a non-nil ActionResult along with a non-nil error if the action failed.
// The ActionResult may include, for example, the stdout/stderr digest from the attempt.
//
// ExecuteAction is a convenience method which wraps both PrepAction and ExecuteAndWait, along with
// other steps such as uploading extra inputs and parsing Operation protos.
func (c *Client) ExecuteAction(ctx context.Context, ac *Action) (*repb.ActionResult, error) {
	log.V(1).Infof("Executing action: %v", ac.Args)

	// Construct the action we're trying to run.
	acDg, res, err := c.PrepAction(ctx, ac)
	if err != nil {
		return nil, err
	}
	// If we found a result in the cache, return that.
	if res != nil {
		return res, nil
	}

	// Upload any remaining inputs.
	if err := c.WriteBlobs(ctx, ac.InputFiles); err != nil {
		return nil, gerrors.WithMessage(err, "uploading input files to the CAS")
	}

	log.V(1).Info("Executing job")
	res, err = c.executeJob(ctx, ac.SkipCache, acDg)
	if err != nil {
		return res, gerrors.WithMessage(err, "executing an action")
	}

	return res, nil
}

// CheckActionCache queries remote action cache, returning an ActionResult or nil if it doesn't exist.
func (c *Client) CheckActionCache(ctx context.Context, acDg *repb.Digest) (*repb.ActionResult, error) {
	res, err := c.GetActionResult(ctx, &repb.GetActionResultRequest{
		InstanceName: c.InstanceName,
		ActionDigest: acDg,
	})
	switch st, _ := status.FromError(err); st.Code() {
	case codes.OK:
		return res, nil
	case codes.NotFound:
		return nil, nil
	default:
		return nil, gerrors.WithMessage(err, "checking the action cache")
	}
}

func (c *Client) executeJob(ctx context.Context, skipCache bool, acDg *repb.Digest) (*repb.ActionResult, error) {
	execReq := &repb.ExecuteRequest{
		InstanceName:    c.InstanceName,
		SkipCacheLookup: skipCache,
		ActionDigest:    acDg,
	}
	op, err := c.ExecuteAndWait(ctx, execReq)
	if err != nil {
		return nil, gerrors.WithMessage(err, "execution error")
	}

	switch r := op.Result.(type) {
	case *oppb.Operation_Error:
		return nil, StatusDetailedError(status.FromProto(r.Error))
	case *oppb.Operation_Response:
		res := new(repb.ExecuteResponse)
		if err := r.Response.UnmarshalTo(res); err != nil {
			return nil, gerrors.WithMessage(err, "extracting ExecuteResponse from execution operation")
		}
		if st := status.FromProto(res.Status); st.Code() != codes.OK {
			return res.Result, gerrors.WithMessage(StatusDetailedError(st), "job failed with error")
		}
		return res.Result, nil
	default:
		return nil, errors.New("unexpected operation result type")
	}
}

// PrepAction constructs the Command and Action protos, checks the action cache if appropriate, and
// uploads the action if the cache was not checked or if there was no cache hit. If successful,
// PrepAction returns the digest of the Action and a (possibly nil) pointer to an ActionResult
// representing the result of the cache check, if any.
func (c *Client) PrepAction(ctx context.Context, ac *Action) (*repb.Digest, *repb.ActionResult, error) {
	comDg, err := c.WriteProto(ctx, buildCommand(ac))
	if err != nil {
		return nil, nil, gerrors.WithMessage(err, "storing Command proto")
	}

	reAc := &repb.Action{
		CommandDigest:   comDg.ToProto(),
		InputRootDigest: ac.InputRoot.ToProto(),
		DoNotCache:      ac.DoNotCache,
	}
	// Only set timeout if it's non-zero, because Timeout needs to be nil for the server to use a
	// default.
	if ac.Timeout != 0 {
		reAc.Timeout = dpb.New(ac.Timeout)
	}

	acBlob, err := proto.Marshal(reAc)
	if err != nil {
		return nil, nil, gerrors.WithMessage(err, "marshalling Action proto")
	}
	acDg := digest.NewFromBlob(acBlob).ToProto()

	// If the result is cacheable, check if it's already in the cache.
	if !ac.DoNotCache || !ac.SkipCache {
		log.V(1).Info("Checking cache")
		res, err := c.CheckActionCache(ctx, acDg)
		if err != nil {
			return nil, nil, err
		}
		if res != nil {
			return acDg, res, nil
		}
	}

	// No cache hit, or we didn't check. Upload the action instead.
	if _, err := c.WriteBlob(ctx, acBlob); err != nil {
		return nil, nil, gerrors.WithMessage(err, "uploading action to the CAS")
	}

	return acDg, nil, nil
}

func buildCommand(ac *Action) *repb.Command {
	cmd := &repb.Command{
		Arguments: ac.Args,
		// Do not use OutputFiles and OutputDirs directly from the Action, as we need to sort them which
		// implies modification.
		OutputFiles:       make([]string, len(ac.OutputFiles)),
		OutputDirectories: make([]string, len(ac.OutputDirs)),
		Platform: &repb.Platform{
			Properties: []*repb.Platform_Property{{Name: containerImagePropertyName, Value: ac.DockerImage}},
		},
	}
	copy(cmd.OutputFiles, ac.OutputFiles)
	copy(cmd.OutputDirectories, ac.OutputDirs)
	sort.Strings(cmd.OutputFiles)
	sort.Strings(cmd.OutputDirectories)
	for name, val := range ac.EnvVars {
		cmd.EnvironmentVariables = append(cmd.EnvironmentVariables, &repb.Command_EnvironmentVariable{Name: name, Value: val})
	}
	sort.Slice(cmd.EnvironmentVariables, func(i, j int) bool { return cmd.EnvironmentVariables[i].Name < cmd.EnvironmentVariables[j].Name })
	return cmd
}

// ExecuteAndWait calls Execute on the underlying client and WaitExecution if necessary. It returns
// the completed operation or an error.
//
// The retry logic is complicated. Assuming retries are enabled, we want the retry to call
// WaitExecution if there's an Operation "in progress", and to call Execute otherwise. In practice
// that means:
//  1. If an error occurs before the first operation is returned, or after the final operation is
//     returned (i.e. the one with op.Done==true), retry by calling Execute again.
//  2. Otherwise, retry by calling WaitExecution with the last operation name.
//
// In addition, we want the retrier to trigger based on certain operation statuses as well as on
// explicit errors. (The shouldRetry function knows which statuses.) We do this by mapping statuses,
// if present, to errors inside the closure and then throwing away such "fake" errors outside the
// closure (if we ran out of retries or if there was never a retrier enabled). The exception is
// deadline-exceeded statuses, which we never give to the retrier (and hence will always propagate
// directly to the caller).
func (c *Client) ExecuteAndWait(ctx context.Context, req *repb.ExecuteRequest) (op *oppb.Operation, err error) {
	return c.ExecuteAndWaitProgress(ctx, req, nil)
}

// ExecuteAndWaitProgress calls Execute on the underlying client and WaitExecution if necessary. It returns
// the completed operation or an error.
// The supplied callback function is called for each message received to update the state of
// the remote action.
func (c *Client) ExecuteAndWaitProgress(ctx context.Context, req *repb.ExecuteRequest, progress func(metadata *repb.ExecuteOperationMetadata)) (op *oppb.Operation, err error) {
	wait := false    // Should we retry by calling WaitExecution instead of Execute?
	opError := false // Are we propagating an Operation status as an error for the retrier's benefit?
	lastOp := &oppb.Operation{}
	closure := func(ctx context.Context) (e error) {
		var res regrpc.Execution_ExecuteClient
		if wait {
			res, e = c.WaitExecution(ctx, &repb.WaitExecutionRequest{Name: lastOp.Name})
		} else {
			res, e = c.Execute(ctx, req)
		}
		if e != nil {
			return e
		}
		for {
			op, e := res.Recv()
			if e == io.EOF {
				break
			}
			if e != nil {
				return e
			}
			wait = !op.Done
			lastOp = op
			if progress != nil {
				metadata := &repb.ExecuteOperationMetadata{}
				if err := op.Metadata.UnmarshalTo(metadata); err == nil {
					progress(metadata)
				}
			}
		}
		st := OperationStatus(lastOp)
		if st != nil {
			opError = true
			if st.Code() == codes.DeadlineExceeded {
				return nil
			}
			return st.Err()
		}
		return nil
	}
	err = c.Retrier.Do(ctx, func() error { return c.CallWithTimeout(ctx, "Execute", closure) })
	if err != nil && !opError {
		if st, ok := status.FromError(err); ok {
			err = StatusDetailedError(st)
		}
		return nil, err
	}

	// In the off chance that the server closes the stream immediately without returning any Operation
	// values and without returning an error, then lastOp will never be modified. Alternatively
	// the server could return an empty operation explicitly prior to closing the stream. Either
	// case is a server error.
	if proto.Equal(lastOp, &oppb.Operation{}) {
		return nil, errors.New("unexpected server behaviour: an empty Operation was returned, or no operation was returned")
	}

	return lastOp, nil
}

// OperationStatus returns an operation error status, if it is present, and nil otherwise.
func OperationStatus(op *oppb.Operation) *status.Status {
	var r *oppb.Operation_Response
	var ok bool
	if r, ok = op.Result.(*oppb.Operation_Response); !ok || r == nil {
		return nil
	}
	respv2 := &repb.ExecuteResponse{}
	if err := r.Response.UnmarshalTo(respv2); err != nil {
		return nil
	}
	if s, ok := status.FromError(status.FromProto(respv2.Status).Err()); ok {
		return s
	}
	return nil
}
