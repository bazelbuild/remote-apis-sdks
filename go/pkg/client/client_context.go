package client

// This file attaches metadata to the context of RPC calls.

import (
	"context"
	"fmt"

	log "github.com/golang/glog"
	"github.com/pborman/uuid"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

const (
	// The headers key of our RequestMetadata.
	remoteHeadersKey = "build.bazel.remote.execution.v2.requestmetadata-bin"
)

// ContextMetadata is optionally attached to RPC requests.
type ContextMetadata struct {
	// ActionID is an optional id to use to identify an action.
	ActionID string
	// InvocationID is an optional id to use to identify an invocation spanning multiple commands.
	InvocationID string
	// CorrelatedInvocationID is an optional id to use to identify a build spanning multiple invocations.
	CorrelatedInvocationID string
	// ToolName is an optional tool name to pass to the remote server for logging.
	ToolName string
	// ToolVersion is an optional tool version to pass to the remote server for logging.
	ToolVersion string
}

// LogContextInfof is equivalent to log.V(x).Infof(...) except it
// also logs context metadata, if available.
func LogContextInfof(ctx context.Context, v log.Level, format string, args ...interface{}) {
	if log.V(v) {
		m, err := GetContextMetadata(ctx)
		if err != nil && m.ActionID != "" {
			format = "%s: " + format
			args = append([]interface{}{m.ActionID}, args...)
		}
		log.InfoDepth(1, fmt.Sprintf(format, args...))
	}
}

// GetContextMetadata parses the metadata from the given context, if it exists.
// If metadata does not exist, empty values are returned.
func GetContextMetadata(ctx context.Context) (m *ContextMetadata, err error) {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		return &ContextMetadata{}, nil
	}
	vs := md.Get(remoteHeadersKey)
	if len(vs) == 0 {
		return &ContextMetadata{}, nil
	}
	buf := []byte(vs[0])
	meta := &repb.RequestMetadata{}
	if err := proto.Unmarshal(buf, meta); err != nil {
		return nil, err
	}
	return &ContextMetadata{
		ToolName:               meta.ToolDetails.GetToolName(),
		ToolVersion:            meta.ToolDetails.GetToolVersion(),
		ActionID:               meta.ActionId,
		InvocationID:           meta.ToolInvocationId,
		CorrelatedInvocationID: meta.CorrelatedInvocationsId,
	}, nil
}

// ContextWithMetadata attaches metadata to the passed-in context, returning a new
// context. This function should be called in every test method after a context is created. It uses
// the already created context to generate a new one containing the metadata header.
func ContextWithMetadata(ctx context.Context, m *ContextMetadata) (context.Context, error) {
	actionID := m.ActionID
	if actionID == "" {
		actionID = uuid.New()
		log.V(2).Infof("Generated action_id %s for %s", actionID, m.ToolName)
	}
	invocationID := m.InvocationID
	if invocationID == "" {
		invocationID = uuid.New()
		log.V(2).Infof("Generated invocation_id %s for %s %s", invocationID, m.ToolName, actionID)
	}

	meta := &repb.RequestMetadata{
		ActionId:         actionID,
		ToolInvocationId: invocationID,
		ToolDetails: &repb.ToolDetails{
			ToolName:    m.ToolName,
			ToolVersion: m.ToolVersion,
		},
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
