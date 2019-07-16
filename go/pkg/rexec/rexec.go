// Package rexec provides a top-level client for executing remote commands.
package rexec

import (
	"context"
	"fmt"
	"strings"

	"github.com/bazelbuild/remote-apis-sdks/go/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/chunker"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/command"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/filemetadata"
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

// FileMetadataCache is a cache for file contents->Metadata.
type FileMetadataCache interface {
	Get(string) (*filemetadata.Metadata, error)
}

// Client is a remote execution client.
type Client struct {
	FileMetadataCache FileMetadataCache
	GrpcClient        *rc.Client
}

func (c *Client) downloadStream(ctx context.Context, raw []byte, dgPb *repb.Digest, write func([]byte)) error {
	if raw != nil {
		write(raw)
	} else if dgPb != nil {
		dg, err := digest.NewFromProto(dgPb)
		if err != nil {
			return err
		}
		bytes, err := c.GrpcClient.ReadBlob(ctx, dg)
		if err != nil {
			return err
		}
		write(bytes)
	}
	return nil
}

func (c *Client) downloadResults(ctx context.Context, resPb *repb.ActionResult, execRoot string, downloadOutputs bool, oe outerr.OutErr) *command.Result {
	if err := c.downloadStream(ctx, resPb.StdoutRaw, resPb.StdoutDigest, oe.WriteOut); err != nil {
		return command.NewRemoteErrorResult(err)
	}
	if err := c.downloadStream(ctx, resPb.StderrRaw, resPb.StderrDigest, oe.WriteErr); err != nil {
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
	cmdPb := cmd.ToREProto()
	log.V(2).Infof("%s> Command: \n%s\n", cmdID, proto.MarshalTextString(cmdPb))
	cmdBlob, err := proto.Marshal(cmdPb)
	if err != nil {
		return command.NewLocalErrorResult(err), meta
	}
	chunkSize := int(c.GrpcClient.ChunkMaxSize)
	cmdCh := chunker.NewFromBlob(cmdBlob, chunkSize)
	cmdDg := cmdCh.Digest()
	meta.CommandDigest = cmdDg
	log.V(1).Infof("%s> Command digest: %s", cmdID, cmdDg)
	log.V(1).Infof("%s> Computing input Merkle tree...", cmdID)
	root, blobs, err := tree.ComputeMerkleTree(cmd.ExecRoot, cmd.InputSpec, chunkSize, c.FileMetadataCache)
	if err != nil {
		return command.NewLocalErrorResult(err), meta
	}
	// TODO(olaola): compute input stats.
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
	acCh := chunker.NewFromBlob(acBlob, chunkSize)
	acDg := acCh.Digest()
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
	blobs = append(blobs, cmdCh)
	blobs = append(blobs, acCh)
	log.V(1).Infof("%s> Checking inputs to upload...", cmdID)
	// TODO(olaola): compute input cache hit stats.
	if err := c.GrpcClient.UploadIfMissing(ctx, blobs...); err != nil {
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
