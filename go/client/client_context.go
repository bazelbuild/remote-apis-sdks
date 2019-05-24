package client

// This file attaches metadata to the context of RPC calls.

import (
	"context"

	log "github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/pborman/uuid"
	"google.golang.org/grpc/metadata"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

const (
	// The headers key of our RequestMetadata.
	remoteHeadersKey = "build.bazel.remote.execution.v2.requestmetadata-bin"
)

// ContextWithMetadata attaches metadata to the passed-in context, returning a new
// context. This function should be called in every test method after a context is created. It uses
// the already created context to generate a new one containing the metadata header.
func ContextWithMetadata(ctx context.Context, toolName, actionID, invocationID string) (context.Context, error) {
	if actionID == "" {
		actionID = uuid.New()
		log.Infof("Generated action_id %s for %s", actionID, toolName)
	}
	if invocationID == "" {
		invocationID = uuid.New()
		log.Infof("Generated invocation_id %s for %s %s", invocationID, toolName, actionID)
	}

	meta := &repb.RequestMetadata{
		ActionId:         actionID,
		ToolInvocationId: invocationID,
		ToolDetails:      &repb.ToolDetails{ToolName: toolName},
	}

	// Marshal the proto to a binary buffer
	buf, err := proto.Marshal(meta)
	if err != nil {
		return nil, err
	}

	// metadata package converts the binary buffer to a base64 string, so no need to encode before
	// sending.
	mdPair := metadata.Pairs(remoteHeadersKey, string(buf))
	return metadata.NewOutgoingContext(ctx, mdPair), nil
}
