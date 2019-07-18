// Package rexec provides a top-level client for executing remote commands.
package rexec

import (
	"context"
	"fmt"
	"strings"

	"github.com/bazelbuild/remote-apis-sdks/go/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/chunker"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/command"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/outerr"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/tree"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	rc "github.com/bazelbuild/remote-apis-sdks/go/client"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	log "github.com/golang/glog"
)

// Client is a remote execution client.
type Client struct {
	FileMetadataCache tree.FileMetadataCache
	GrpcClient        *rc.Client
}

// ExecutionContext allows more granular control over various stages of command execution.
// At any point, any errors that occurred will be stored in the Result.
type ExecutionContext struct {
	ctx        context.Context
	cmd        *command.Command
	opt        *command.ExecutionOptions
	oe         outerr.OutErr
	client     *Client
	inputBlobs []*chunker.Chunker
	resPb      *repb.ActionResult
	// The metadata of the current execution.
	Metadata *command.Metadata
	// The result of the current execution, if available.
	Result *command.Result
}

// NewExecutionContext starts a new ExecutionContext for a given command.
func (c *Client) NewExecutionContext(ctx context.Context, cmd *command.Command, opt *command.ExecutionOptions, oe outerr.OutErr) (*ExecutionContext, error) {
	cmd.FillDefaultFieldValues()
	if err := cmd.Validate(); err != nil {
		return nil, err
	}
	grpcCtx, err := rc.ContextWithMetadata(ctx, cmd.Identifiers.ToolName, cmd.Identifiers.CommandID, cmd.Identifiers.InvocationID)
	if err != nil {
		return nil, err
	}
	return &ExecutionContext{
		ctx:      grpcCtx,
		cmd:      cmd,
		opt:      opt,
		oe:       oe,
		client:   c,
		Metadata: &command.Metadata{},
	}, nil
}

func (ec *ExecutionContext) downloadStream(raw []byte, dgPb *repb.Digest, write func([]byte)) error {
	if raw != nil {
		write(raw)
	} else if dgPb != nil {
		dg, err := digest.NewFromProto(dgPb)
		if err != nil {
			return err
		}
		bytes, err := ec.client.GrpcClient.ReadBlob(ec.ctx, dg)
		if err != nil {
			return err
		}
		write(bytes)
	}
	return nil
}

func (ec *ExecutionContext) downloadResults() *command.Result {
	if err := ec.downloadStream(ec.resPb.StdoutRaw, ec.resPb.StdoutDigest, ec.oe.WriteOut); err != nil {
		return command.NewRemoteErrorResult(err)
	}
	if err := ec.downloadStream(ec.resPb.StderrRaw, ec.resPb.StderrDigest, ec.oe.WriteErr); err != nil {
		return command.NewRemoteErrorResult(err)
	}
	if ec.opt.DownloadOutputs {
		if err := ec.client.GrpcClient.DownloadActionOutputs(ec.ctx, ec.resPb, ec.cmd.ExecRoot); err != nil {
			return command.NewRemoteErrorResult(err)
		}
	}
	// TODO(olaola): save output stats onto metadata here.
	return command.NewResultFromExitCode((int)(ec.resPb.ExitCode))
}

func (ec *ExecutionContext) computeInputs() error {
	cmdID := ec.cmd.Identifiers.CommandID
	cmdPb := ec.cmd.ToREProto()
	log.V(2).Infof("%s> Command: \n%s\n", cmdID, proto.MarshalTextString(cmdPb))
	chunkSize := int(ec.client.GrpcClient.ChunkMaxSize)
	cmdCh, err := chunker.NewFromProto(cmdPb, chunkSize)
	if err != nil {
		return err
	}
	cmdDg := cmdCh.Digest()
	ec.Metadata.CommandDigest = cmdDg
	log.V(1).Infof("%s> Command digest: %s", cmdID, cmdDg)
	log.V(1).Infof("%s> Computing input Merkle tree...", cmdID)
	root, blobs, stats, err := tree.ComputeMerkleTree(ec.cmd.ExecRoot, ec.cmd.InputSpec, chunkSize, ec.client.FileMetadataCache)
	if err != nil {
		return err
	}
	ec.inputBlobs = blobs
	ec.Metadata.InputFiles = stats.InputFiles
	ec.Metadata.InputDirectories = stats.InputDirectories
	ec.Metadata.TotalInputBytes = stats.TotalInputBytes
	acPb := &repb.Action{
		CommandDigest:   cmdDg.ToProto(),
		InputRootDigest: root.ToProto(),
		DoNotCache:      ec.opt.DoNotCache,
	}
	if ec.cmd.Timeout > 0 {
		acPb.Timeout = ptypes.DurationProto(ec.cmd.Timeout)
	}
	acCh, err := chunker.NewFromProto(acPb, chunkSize)
	if err != nil {
		return err
	}
	acDg := acCh.Digest()
	log.V(1).Infof("%s> Action digest: %s", cmdID, acDg)
	ec.inputBlobs = append(ec.inputBlobs, cmdCh)
	ec.inputBlobs = append(ec.inputBlobs, acCh)
	ec.Metadata.ActionDigest = acDg
	ec.Metadata.TotalInputBytes += cmdDg.Size + acDg.Size
	return nil
}

func (ec *ExecutionContext) computedInputs() bool {
	return ec.Metadata.ActionDigest.Size > 0
}

// GetCachedResult tries to get the command result from the cache. The Result will be nil on a
// cache miss. The ExecutionContext will be ready to execute the action, or, alternatively, to
// update the remote cache with a local result. If the ExecutionOptions do not allow to accept
// remotely cached results, the operation is a noop.
func (ec *ExecutionContext) GetCachedResult() {
	if !ec.computedInputs() {
		if err := ec.computeInputs(); err != nil {
			ec.Result = command.NewLocalErrorResult(err)
			return
		}
	}
	if ec.opt.AcceptCached && !ec.opt.DoNotCache {
		resPb, err := ec.client.GrpcClient.CheckActionCache(ec.ctx, ec.Metadata.ActionDigest.ToProto())
		if err != nil {
			ec.Result = command.NewRemoteErrorResult(err)
			return
		}
		ec.resPb = resPb
	}
	if ec.resPb != nil {
		log.V(1).Infof("%s> Found cached result, downloading outputs...", ec.cmd.Identifiers.CommandID)
		ec.Result = ec.downloadResults()
		if ec.Result.Err == nil {
			ec.Result.Status = command.CacheHitResultStatus
		}
		return
	}
	ec.Result = nil
}

// UpdateCachedResult tries to write local results of the execution to the remote cache.
func (ec *ExecutionContext) UpdateCachedResult() {
	if !ec.computedInputs() {
		if err := ec.computeInputs(); err != nil {
			ec.Result = command.NewLocalErrorResult(err)
			return
		}
	}
	// TODO(olaola): implement this.
}

// ExecuteRemotely tries to execute the command remotely and download the results. It uploads any
// missing inputs first.
func (ec *ExecutionContext) ExecuteRemotely() {
	if !ec.computedInputs() {
		if err := ec.computeInputs(); err != nil {
			ec.Result = command.NewLocalErrorResult(err)
			return
		}
	}
	cmdID := ec.cmd.Identifiers.CommandID
	log.V(1).Infof("%s> Checking inputs to upload...", cmdID)
	// TODO(olaola): compute input cache hit stats.
	if err := ec.client.GrpcClient.UploadIfMissing(ec.ctx, ec.inputBlobs...); err != nil {
		ec.Result = command.NewRemoteErrorResult(err)
		return
	}
	log.V(1).Infof("%s> Executing remotely...\n%s", cmdID, strings.Join(ec.cmd.Args, " "))
	op, err := ec.client.GrpcClient.ExecuteAndWait(ec.ctx, &repb.ExecuteRequest{
		InstanceName:    ec.client.GrpcClient.InstanceName,
		SkipCacheLookup: !ec.opt.AcceptCached || ec.opt.DoNotCache,
		ActionDigest:    ec.Metadata.ActionDigest.ToProto(),
	})
	if err != nil {
		ec.Result = command.NewRemoteErrorResult(err)
		return
	}

	or := op.GetResponse()
	if or == nil {
		ec.Result = command.NewRemoteErrorResult(fmt.Errorf("unexpected operation result type: %v", or))
		return
	}
	resp := &repb.ExecuteResponse{}
	if err := ptypes.UnmarshalAny(or, resp); err != nil {
		ec.Result = command.NewRemoteErrorResult(err)
		return
	}
	ec.resPb = resp.Result
	st := status.FromProto(resp.Status)
	message := resp.Message
	if message != "" && (st.Code() != codes.OK || ec.resPb != nil && ec.resPb.ExitCode != 0) {
		ec.oe.WriteErr([]byte(message + "\n"))
	}

	if ec.resPb != nil {
		log.V(1).Infof("%s> Downloading outputs...", cmdID)
		ec.Result = ec.downloadResults()
		if resp.CachedResult && ec.Result.Err == nil {
			ec.Result.Status = command.CacheHitResultStatus
		}
	}
	if st.Code() == codes.DeadlineExceeded {
		ec.Result = command.NewTimeoutResult()
		return
	}
	if st.Code() != codes.OK {
		ec.Result = command.NewRemoteErrorResult(st.Err())
		return
	}
	if ec.resPb == nil {
		ec.Result = command.NewRemoteErrorResult(fmt.Errorf("execute did not return action result"))
	}
}

// Run executes a command remotely.
func (c *Client) Run(ctx context.Context, cmd *command.Command, opt *command.ExecutionOptions, oe outerr.OutErr) (*command.Result, *command.Metadata) {
	ec, err := c.NewExecutionContext(ctx, cmd, opt, oe)
	if err != nil {
		return command.NewLocalErrorResult(err), &command.Metadata{}
	}
	ec.GetCachedResult()
	if ec.Result != nil {
		return ec.Result, ec.Metadata
	}
	ec.ExecuteRemotely()
	// TODO(olaola): implement the cache-miss-retry loop.
	return ec.Result, ec.Metadata
}
