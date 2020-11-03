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

// LogContextInfof(ctx, x, ...) is equivalent to log.V(x).Infof(...) except it
// also logs context metadata, if available.
func LogContextInfof(ctx context.Context, v log.Level, format string, args ...interface{}) {
	if log.V(v) {
		_, actionID, _, _ := GetContextMetadata(ctx)
		if actionID != "" {
			format = "%s: " + format
			args = append([]interface{}{actionID}, args...)
		}
		log.V(v).Infof(format, args...)
	}
}

// GetContextMetadata parses the metadata from the given context, if it exists.
// If metadata does not exist, empty values are returned.
func GetContextMetadata(ctx context.Context) (toolName, actionID, invocationID string, err error) {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		return "", "", "", nil
	}
	vs := md.Get(remoteHeadersKey)
	if len(vs) == 0 {
		return "", "", "", nil
	}
	buf := []byte(vs[0])
	meta := &repb.RequestMetadata{}
	if err := proto.Unmarshal(buf, meta); err != nil {
		return "", "", "", err
	}
	return meta.ToolDetails.GetToolName(), meta.ActionId, meta.ToolInvocationId, nil
}

// ContextWithMetadata attaches metadata to the passed-in context, returning a new
// context. This function should be called in every test method after a context is created. It uses
// the already created context to generate a new one containing the metadata header.
func ContextWithMetadata(ctx context.Context, toolName, actionID, invocationID string) (context.Context, error) {
	if actionID == "" {
		actionID = uuid.New()
		log.V(2).Infof("Generated action_id %s for %s", actionID, toolName)
	}
	if invocationID == "" {
		invocationID = uuid.New()
		log.V(2).Infof("Generated invocation_id %s for %s %s", invocationID, toolName, actionID)
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
