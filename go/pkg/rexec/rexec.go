// Package rexec provides a top-level client for executing remote commands.
package rexec

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/chunker"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/command"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/filemetadata"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/outerr"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	rc "github.com/bazelbuild/remote-apis-sdks/go/pkg/client"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	log "github.com/golang/glog"
	tspb "github.com/golang/protobuf/ptypes/timestamp"
)

// Client is a remote execution client.
type Client struct {
	FileMetadataCache filemetadata.Cache
	GrpcClient        *rc.Client
}

// Context allows more granular control over various stages of command execution.
// At any point, any errors that occurred will be stored in the Result.
type Context struct {
	ctx         context.Context
	cmd         *command.Command
	opt         *command.ExecutionOptions
	oe          outerr.OutErr
	client      *Client
	inputBlobs  []*chunker.UploadEntry
	cmdUe, acUe *chunker.UploadEntry
	resPb       *repb.ActionResult
	// The metadata of the current execution.
	Metadata *command.Metadata
	// The result of the current execution, if available.
	Result *command.Result
}

// NewContext starts a new Context for a given command.
func (c *Client) NewContext(ctx context.Context, cmd *command.Command, opt *command.ExecutionOptions, oe outerr.OutErr) (*Context, error) {
	cmd.FillDefaultFieldValues()
	if err := cmd.Validate(); err != nil {
		return nil, err
	}
	grpcCtx, err := rc.ContextWithMetadata(ctx, cmd.Identifiers.ToolName, cmd.Identifiers.CommandID, cmd.Identifiers.InvocationID)
	if err != nil {
		return nil, err
	}
	return &Context{
		ctx:      grpcCtx,
		cmd:      cmd,
		opt:      opt,
		oe:       oe,
		client:   c,
		Metadata: &command.Metadata{EventTimes: make(map[string]*command.TimeInterval)},
	}, nil
}

func (ec *Context) downloadStream(raw []byte, dgPb *repb.Digest, write func([]byte)) error {
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

func (ec *Context) setOutputMetadata() {
	if ec.resPb == nil {
		return
	}
	ec.Metadata.OutputFiles = len(ec.resPb.OutputFiles) + len(ec.resPb.OutputFileSymlinks)
	ec.Metadata.OutputDirectories = len(ec.resPb.OutputDirectories) + len(ec.resPb.OutputDirectorySymlinks)
	ec.Metadata.OutputDigests = make(map[string]digest.Digest)
	ec.Metadata.TotalOutputBytes = 0
	for _, file := range ec.resPb.OutputFiles {
		dg := digest.NewFromProtoUnvalidated(file.Digest)
		ec.Metadata.OutputDigests[file.Path] = dg
		ec.Metadata.TotalOutputBytes += dg.Size
	}
	if ec.resPb.StdoutRaw != nil {
		ec.Metadata.TotalOutputBytes += int64(len(ec.resPb.StdoutRaw))
	} else if ec.resPb.StdoutDigest != nil {
		ec.Metadata.TotalOutputBytes += ec.resPb.StdoutDigest.SizeBytes
	}
	if ec.resPb.StderrRaw != nil {
		ec.Metadata.TotalOutputBytes += int64(len(ec.resPb.StderrRaw))
	} else if ec.resPb.StderrDigest != nil {
		ec.Metadata.TotalOutputBytes += ec.resPb.StderrDigest.SizeBytes
	}
}

func (ec *Context) downloadOutErr() *command.Result {
	if err := ec.downloadStream(ec.resPb.StdoutRaw, ec.resPb.StdoutDigest, ec.oe.WriteOut); err != nil {
		return command.NewRemoteErrorResult(err)
	}
	if err := ec.downloadStream(ec.resPb.StderrRaw, ec.resPb.StderrDigest, ec.oe.WriteErr); err != nil {
		return command.NewRemoteErrorResult(err)
	}
	return command.NewResultFromExitCode((int)(ec.resPb.ExitCode))
}

func (ec *Context) downloadOutputs(execRoot string) *command.Result {
	ec.Metadata.EventTimes[command.EventDownloadResults] = &command.TimeInterval{From: time.Now()}
	defer func() { ec.Metadata.EventTimes[command.EventDownloadResults].To = time.Now() }()
	if err := ec.client.GrpcClient.DownloadActionOutputs(ec.ctx, ec.resPb, execRoot, ec.client.FileMetadataCache); err != nil {
		return command.NewRemoteErrorResult(err)
	}
	return command.NewResultFromExitCode((int)(ec.resPb.ExitCode))
}

func (ec *Context) computeInputs() error {
	if ec.Metadata.ActionDigest.Size > 0 {
		// Already computed inputs.
		return nil
	}
	ec.Metadata.EventTimes[command.EventComputeMerkleTree] = &command.TimeInterval{From: time.Now()}
	defer func() { ec.Metadata.EventTimes[command.EventComputeMerkleTree].To = time.Now() }()
	cmdID, executionID := ec.cmd.Identifiers.ExecutionID, ec.cmd.Identifiers.CommandID
	cmdPb := ec.cmd.ToREProto()
	log.V(2).Infof("%s %s> Command: \n%s\n", cmdID, executionID, proto.MarshalTextString(cmdPb))
	var err error
	if ec.cmdUe, err = chunker.EntryFromProto(cmdPb); err != nil {
		return err
	}
	cmdDg := ec.cmdUe.Digest()
	ec.Metadata.CommandDigest = cmdDg
	log.V(1).Infof("%s %s> Command digest: %s", cmdID, executionID, cmdDg)
	log.V(1).Infof("%s %s> Computing input Merkle tree...", cmdID, executionID)
	root, blobs, stats, err := ec.client.GrpcClient.ComputeMerkleTree(ec.cmd.ExecRoot, ec.cmd.InputSpec, ec.client.FileMetadataCache)
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
	if ec.acUe, err = chunker.EntryFromProto(acPb); err != nil {
		return err
	}
	acDg := ec.acUe.Digest()
	log.V(1).Infof("%s %s> Action digest: %s", cmdID, executionID, acDg)
	ec.inputBlobs = append(ec.inputBlobs, ec.cmdUe)
	ec.inputBlobs = append(ec.inputBlobs, ec.acUe)
	ec.Metadata.ActionDigest = acDg
	ec.Metadata.TotalInputBytes += cmdDg.Size + acDg.Size
	return nil
}

// GetCachedResult tries to get the command result from the cache. The Result will be nil on a
// cache miss. The Context will be ready to execute the action, or, alternatively, to
// update the remote cache with a local result. If the ExecutionOptions do not allow to accept
// remotely cached results, the operation is a noop.
func (ec *Context) GetCachedResult() {
	if err := ec.computeInputs(); err != nil {
		ec.Result = command.NewLocalErrorResult(err)
		return
	}
	if ec.opt.AcceptCached && !ec.opt.DoNotCache {
		ec.Metadata.EventTimes[command.EventCheckActionCache] = &command.TimeInterval{From: time.Now()}
		resPb, err := ec.client.GrpcClient.CheckActionCache(ec.ctx, ec.Metadata.ActionDigest.ToProto())
		ec.Metadata.EventTimes[command.EventCheckActionCache].To = time.Now()
		if err != nil {
			ec.Result = command.NewRemoteErrorResult(err)
			return
		}
		ec.resPb = resPb
	}
	if ec.resPb != nil {
		ec.Result = command.NewResultFromExitCode((int)(ec.resPb.ExitCode))
		ec.setOutputMetadata()
		cmdID, executionID := ec.cmd.Identifiers.ExecutionID, ec.cmd.Identifiers.CommandID
		log.V(1).Infof("%s %s> Found cached result, downloading outputs...", cmdID, executionID)
		if ec.opt.DownloadOutErr {
			ec.Result = ec.downloadOutErr()
		}
		if ec.Result.Err == nil && ec.opt.DownloadOutputs {
			ec.Result = ec.downloadOutputs(ec.cmd.ExecRoot)
		}
		if ec.Result.Err == nil {
			ec.Result.Status = command.CacheHitResultStatus
		}
		return
	}
	ec.Result = nil
}

// UpdateCachedResult tries to write local results of the execution to the remote cache.
// TODO(olaola): optional arguments to override values of local outputs, and also stdout/err.
func (ec *Context) UpdateCachedResult() {
	cmdID, executionID := ec.cmd.Identifiers.ExecutionID, ec.cmd.Identifiers.CommandID
	ec.Result = &command.Result{Status: command.SuccessResultStatus}
	if ec.opt.DoNotCache {
		log.V(1).Infof("%s %s> Command is marked do-not-cache, skipping remote caching.", cmdID, executionID)
		return
	}
	if err := ec.computeInputs(); err != nil {
		ec.Result = command.NewLocalErrorResult(err)
		return
	}
	ec.Metadata.EventTimes[command.EventUpdateCachedResult] = &command.TimeInterval{From: time.Now()}
	defer func() { ec.Metadata.EventTimes[command.EventUpdateCachedResult].To = time.Now() }()
	outPaths := append(ec.cmd.OutputFiles, ec.cmd.OutputDirs...)
	blobs, resPb, err := ec.client.GrpcClient.ComputeOutputsToUpload(ec.cmd.ExecRoot, outPaths, ec.client.FileMetadataCache)
	if err != nil {
		ec.Result = command.NewLocalErrorResult(err)
		return
	}
	ec.resPb = resPb
	ec.setOutputMetadata()
	toUpload := []*chunker.UploadEntry{ec.acUe, ec.cmdUe}
	for _, ch := range blobs {
		toUpload = append(toUpload, ch)
	}
	log.V(1).Infof("%s %s> Uploading local outputs...", cmdID, executionID)
	if missing, err := ec.client.GrpcClient.UploadIfMissing(ec.ctx, toUpload...); err != nil {
		ec.Result = command.NewRemoteErrorResult(err)
		return
	} else {
		ec.Metadata.MissingDigests = missing
	}
	log.V(1).Infof("%s %s> Updating remote cache...", cmdID, executionID)
	req := &repb.UpdateActionResultRequest{
		InstanceName: ec.client.GrpcClient.InstanceName,
		ActionDigest: ec.Metadata.ActionDigest.ToProto(),
		ActionResult: resPb,
	}
	if _, err := ec.client.GrpcClient.UpdateActionResult(ec.ctx, req); err != nil {
		ec.Result = command.NewRemoteErrorResult(err)
		return
	}
}

// ExecuteRemotely tries to execute the command remotely and download the results. It uploads any
// missing inputs first.
func (ec *Context) ExecuteRemotely() {
	if err := ec.computeInputs(); err != nil {
		ec.Result = command.NewLocalErrorResult(err)
		return
	}
	cmdID, executionID := ec.cmd.Identifiers.ExecutionID, ec.cmd.Identifiers.CommandID
	log.V(1).Infof("%s %s> Checking inputs to upload...", cmdID, executionID)
	// TODO(olaola): compute input cache hit stats.
	ec.Metadata.EventTimes[command.EventUploadInputs] = &command.TimeInterval{From: time.Now()}
	missing, err := ec.client.GrpcClient.UploadIfMissing(ec.ctx, ec.inputBlobs...)
	ec.Metadata.EventTimes[command.EventUploadInputs].To = time.Now()
	if err != nil {
		ec.Result = command.NewRemoteErrorResult(err)
		return
	}
	ec.Metadata.MissingDigests = missing
	log.V(1).Infof("%s %s> Executing remotely...\n%s", cmdID, executionID, strings.Join(ec.cmd.Args, " "))
	ec.Metadata.EventTimes[command.EventExecuteRemotely] = &command.TimeInterval{From: time.Now()}
	op, err := ec.client.GrpcClient.ExecuteAndWait(ec.ctx, &repb.ExecuteRequest{
		InstanceName:    ec.client.GrpcClient.InstanceName,
		SkipCacheLookup: !ec.opt.AcceptCached || ec.opt.DoNotCache,
		ActionDigest:    ec.Metadata.ActionDigest.ToProto(),
	})
	ec.Metadata.EventTimes[command.EventExecuteRemotely].To = time.Now()
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
	setTimingMetadata(ec.Metadata, resp.Result.GetExecutionMetadata())
	st := status.FromProto(resp.Status)
	message := resp.Message
	if message != "" && (st.Code() != codes.OK || ec.resPb != nil && ec.resPb.ExitCode != 0) {
		ec.oe.WriteErr([]byte(message + "\n"))
	}

	if ec.resPb != nil {
		ec.setOutputMetadata()
		ec.Result = command.NewResultFromExitCode((int)(ec.resPb.ExitCode))
		log.V(1).Infof("%s %s> Downloading outputs...", cmdID, executionID)
		if ec.opt.DownloadOutErr {
			ec.Result = ec.downloadOutErr()
		}
		if ec.Result.Err == nil && ec.opt.DownloadOutputs {
			ec.Result = ec.downloadOutputs(ec.cmd.ExecRoot)
		}
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

// DownloadOutErr downloads the stdout and stderr of the command.
func (ec *Context) DownloadOutErr() {
	st := ec.Result.Status
	ec.Result = ec.downloadOutErr()
	if ec.Result.Err == nil {
		ec.Result.Status = st
	}
}

// DownloadOutputs downloads the outputs of the command in the context to the specified directory.
func (ec *Context) DownloadOutputs(execRoot string) {
	st := ec.Result.Status
	ec.Result = ec.downloadOutputs(execRoot)
	if ec.Result.Err == nil {
		ec.Result.Status = st
	}
}

func timeFromProto(tPb *tspb.Timestamp) time.Time {
	if tPb == nil {
		return time.Time{}
	}
	t, err := ptypes.Timestamp(tPb)
	if err != nil {
		log.Errorf("Failed to parse RBE timestamp: %+v - > %v", tPb, err)
	}
	return t
}

func setEventTimes(cm *command.Metadata, event string, start, end *tspb.Timestamp) {
	cm.EventTimes[event] = &command.TimeInterval{
		From: timeFromProto(start),
		To:   timeFromProto(end),
	}
}

func setTimingMetadata(cm *command.Metadata, em *repb.ExecutedActionMetadata) {
	if em == nil {
		return
	}
	setEventTimes(cm, command.EventServerQueued, em.QueuedTimestamp, em.WorkerStartTimestamp)
	setEventTimes(cm, command.EventServerWorker, em.WorkerStartTimestamp, em.WorkerCompletedTimestamp)
	setEventTimes(cm, command.EventServerWorkerInputFetch, em.InputFetchStartTimestamp, em.InputFetchCompletedTimestamp)
	setEventTimes(cm, command.EventServerWorkerExecution, em.ExecutionStartTimestamp, em.ExecutionCompletedTimestamp)
	setEventTimes(cm, command.EventServerWorkerOutputUpload, em.OutputUploadStartTimestamp, em.OutputUploadCompletedTimestamp)
}

// Run executes a command remotely.
func (c *Client) Run(ctx context.Context, cmd *command.Command, opt *command.ExecutionOptions, oe outerr.OutErr) (*command.Result, *command.Metadata) {
	ec, err := c.NewContext(ctx, cmd, opt, oe)
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
