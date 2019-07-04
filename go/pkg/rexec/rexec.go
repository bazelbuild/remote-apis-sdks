// Package rexec provides a top-level client for executing remote commands.
package rexec

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/bazelbuild/remote-apis-sdks/go/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/command"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/outerr"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	rc "github.com/bazelbuild/remote-apis-sdks/go/client"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	log "github.com/golang/glog"
)

// FileDigestCache is a cache for file contents->digests.
type FileDigestCache interface {
	Get(string) (digest.Digest, error)
}

// NoopFileDigestCache is a non-caching cache (always returns a cache miss).
type NoopFileDigestCache struct{}

// Get computes the digest from the file contents.
func (c *NoopFileDigestCache) Get(path string) (digest.Digest, error) {
	return digest.NewFromFile(path)
}

// Client is a remote execution client.
type Client struct {
	FileDigestCache FileDigestCache
	GrpcClient      *rc.Client
}

func buildCommand(cmd *command.Command) *repb.Command {
	cmdPb := &repb.Command{
		Arguments:         cmd.Args,
		OutputFiles:       make([]string, len(cmd.OutputFiles)),
		OutputDirectories: make([]string, len(cmd.OutputDirs)),
		WorkingDirectory:  cmd.WorkingDir,
	}
	copy(cmdPb.OutputFiles, cmd.OutputFiles)
	copy(cmdPb.OutputDirectories, cmd.OutputDirs)
	sort.Strings(cmdPb.OutputFiles)
	sort.Strings(cmdPb.OutputDirectories)
	for name, val := range cmd.InputSpec.EnvironmentVariables {
		cmdPb.EnvironmentVariables = append(cmdPb.EnvironmentVariables, &repb.Command_EnvironmentVariable{Name: name, Value: val})
	}
	sort.Slice(cmdPb.EnvironmentVariables, func(i, j int) bool { return cmdPb.EnvironmentVariables[i].Name < cmdPb.EnvironmentVariables[j].Name })
	if len(cmd.Platform) > 0 {
		cmdPb.Platform = &repb.Platform{}
		for name, val := range cmd.Platform {
			cmdPb.Platform.Properties = append(cmdPb.Platform.Properties, &repb.Platform_Property{Name: name, Value: val})
		}
		sort.Slice(cmdPb.Platform.Properties, func(i, j int) bool { return cmdPb.Platform.Properties[i].Name < cmdPb.Platform.Properties[j].Name })
	}
	return cmdPb
}

func (c *Client) downloadOutErr(ctx context.Context, resPb *repb.ActionResult, oe outerr.OutErr) error {
	downloads := []struct {
		raw   []byte
		dgPb  *repb.Digest
		write func([]byte)
	}{
		{raw: resPb.StdoutRaw, dgPb: resPb.StdoutDigest, write: oe.WriteOut},
		{raw: resPb.StderrRaw, dgPb: resPb.StderrDigest, write: oe.WriteErr},
	}
	for _, d := range downloads {
		if d.raw != nil {
			d.write(d.raw)
		} else if d.dgPb != nil {
			dg, err := digest.NewFromProto(d.dgPb)
			if err != nil {
				return err
			}
			bytes, err := c.GrpcClient.ReadBlob(ctx, dg)
			if err != nil {
				return err
			}
			d.write(bytes)
		}
	}
	return nil
}

func (c *Client) downloadResults(ctx context.Context, resPb *repb.ActionResult, execRoot string, downloadOutputs bool, oe outerr.OutErr) *command.Result {
	if err := c.downloadOutErr(ctx, resPb, oe); err != nil {
		return command.NewRemoteErrorResult(err)
	}
	if downloadOutputs {
		if err := c.GrpcClient.DownloadActionOutputs(ctx, resPb, execRoot); err != nil {
			return command.NewRemoteErrorResult(err)
		}
	}
	// TODO(olaola): save output stats onto metadata here.
	return command.NewResultFromExitCode((int)(resPb.ExitCode))
}

// Run executes a command remotely.
func (c *Client) Run(ctx context.Context, cmd *command.Command, opt *command.ExecutionOptions, oe outerr.OutErr) (*command.Result, *command.Metadata) {
	cmd.FillDefaultFieldValues()
	cmdID := cmd.Identifiers.CommandID
	meta := &command.Metadata{}
	if err := cmd.Validate(); err != nil {
		return command.NewLocalErrorResult(err), meta
	}
	cmdPb := buildCommand(cmd)
	log.V(2).Infof("%s> Command: \n%s\n", cmdID, proto.MarshalTextString(cmdPb))
	cmdBlob, err := proto.Marshal(cmdPb)
	if err != nil {
		return command.NewLocalErrorResult(err), meta
	}
	cmdDg := digest.NewFromBlob(cmdBlob)
	meta.CommandDigest = cmdDg
	log.V(1).Infof("%s> Command digest: %s", cmdID, cmdDg)
	log.V(1).Infof("%s> Computing input Merkle tree...", cmdID)
	ft, err := rc.BuildTreeFromInputs(cmd.ExecRoot, cmd.InputSpec)
	if err != nil {
		return command.NewLocalErrorResult(err), meta
	}
	// TODO(olaola): compute input stats.
	root, blobs, err := rc.PackageTree(ft)
	if err != nil {
		return command.NewLocalErrorResult(err), meta
	}
	acPb := &repb.Action{
		CommandDigest:   cmdDg.ToProto(),
		InputRootDigest: root.ToProto(),
		DoNotCache:      opt.DoNotCache,
	}
	if cmd.Timeout > 0 {
		acPb.Timeout = ptypes.DurationProto(cmd.Timeout)
	}
	acBlob, err := proto.Marshal(acPb)
	if err != nil {
		return command.NewLocalErrorResult(err), meta
	}
	acDg := digest.NewFromBlob(acBlob)
	meta.ActionDigest = acDg
	log.V(1).Infof("%s> Action digest: %s", cmdID, acDg)
	ctx, err = rc.ContextWithMetadata(ctx, cmd.Identifiers.ToolName, cmdID, cmd.Identifiers.InvocationID)
	if err != nil {
		return command.NewLocalErrorResult(err), meta
	}
	acdgPb := acDg.ToProto()
	var resPb *repb.ActionResult
	acceptCached := opt.AcceptCached && !opt.DoNotCache
	if acceptCached {
		resPb, err = c.GrpcClient.CheckActionCache(ctx, acdgPb)
		if err != nil {
			return command.NewRemoteErrorResult(err), meta
		}
	}
	if resPb != nil {
		log.V(1).Infof("%s> Found cached result, downloading outputs...", cmdID)
		res := c.downloadResults(ctx, resPb, cmd.ExecRoot, opt.DownloadOutputs, oe)
		// TODO(olaola): implement the cache-miss-retry loop.
		if res.Err == nil {
			res.Status = command.CacheHitResultStatus
		}
		return res, meta
	}
	blobs[cmdDg] = cmdBlob
	blobs[acDg] = acBlob
	log.V(1).Infof("%s> Checking inputs to upload...", cmdID)
	// TODO(olaola): compute input cache hit stats.
	if err := c.GrpcClient.WriteBlobs(ctx, blobs); err != nil {
		return command.NewRemoteErrorResult(err), meta
	}
	log.V(1).Infof("%s> Executing remotely...\n%s", cmdID, strings.Join(cmd.Args, " "))
	op, err := c.GrpcClient.ExecuteAndWait(ctx, &repb.ExecuteRequest{
		InstanceName:    c.GrpcClient.InstanceName,
		SkipCacheLookup: !acceptCached,
		ActionDigest:    acdgPb,
	})
	if err != nil {
		return command.NewRemoteErrorResult(err), meta
	}

	or := op.GetResponse()
	if or == nil {
		return command.NewRemoteErrorResult(fmt.Errorf("unexpected operation result type: %v", or)), meta
	}
	resp := &repb.ExecuteResponse{}
	if err := ptypes.UnmarshalAny(or, resp); err != nil {
		return command.NewRemoteErrorResult(err), meta
	}
	resPb = resp.Result
	st := status.FromProto(resp.Status)
	message := resp.Message
	if message != "" && (st.Code() != codes.OK || resPb != nil && resPb.ExitCode != 0) {
		oe.WriteErr([]byte(message + "\n"))
	}

	var res *command.Result
	if resPb != nil {
		log.V(1).Infof("%s> Downloading outputs...", cmdID)
		res = c.downloadResults(ctx, resPb, cmd.ExecRoot, opt.DownloadOutputs, oe)
		if resp.CachedResult {
			res.Status = command.CacheHitResultStatus
		}
	}
	if st.Code() == codes.DeadlineExceeded {
		return command.NewTimeoutResult(), meta
	}
	if st.Code() != codes.OK {
		return command.NewRemoteErrorResult(st.Err()), meta
	}
	if resPb == nil {
		return command.NewRemoteErrorResult(fmt.Errorf("execute did not return action result")), meta
	}
	return res, meta
}
