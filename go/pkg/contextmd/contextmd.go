// Package contextmd allows attaching metadata to the context of RPC calls.
package contextmd

import (
	"context"
	"fmt"
	"sort"
	"strings"

	log "github.com/golang/glog"
	"github.com/pborman/uuid"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

const (
	// The headers key of our RequestMetadata.
	remoteHeadersKey     = "build.bazel.remote.execution.v2.requestmetadata-bin"
	defaultMaxHeaderSize = 8 * 1024
)

// Metadata is optionally attached to RPC requests.
type Metadata struct {
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

// Infof is equivalent to log.V(x).Infof(...) except it
// also logs context metadata, if available.
func Infof(ctx context.Context, v log.Level, format string, args ...any) {
	if log.V(v) {
		if m, err := ExtractMetadata(ctx); err == nil && m.ActionID != "" {
			format = "%s: " + format
			args = append([]any{m.ActionID}, args...)
		}
		log.InfoDepth(1, fmt.Sprintf(format, args...))
	}
}

// ExtractMetadata parses the metadata from the given context, if it exists.
// If metadata does not exist, empty values are returned.
func ExtractMetadata(ctx context.Context) (m *Metadata, err error) {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		return &Metadata{}, nil
	}
	vs := md.Get(remoteHeadersKey)
	if len(vs) == 0 {
		return &Metadata{}, nil
	}
	buf := []byte(vs[0])
	meta := &repb.RequestMetadata{}
	if err := proto.Unmarshal(buf, meta); err != nil {
		return nil, err
	}
	return &Metadata{
		ToolName:               meta.ToolDetails.GetToolName(),
		ToolVersion:            meta.ToolDetails.GetToolVersion(),
		ActionID:               meta.ActionId,
		InvocationID:           meta.ToolInvocationId,
		CorrelatedInvocationID: meta.CorrelatedInvocationsId,
	}, nil
}

// WithMetadata attaches metadata to the passed-in context, returning a new
// context. This function should be called in every test method after a context is created. It uses
// the already created context to generate a new one containing the metadata header.
func WithMetadata(ctx context.Context, ms ...*Metadata) (context.Context, error) {
	m := MergeMetadata(ms...)
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

// MergeMetadata returns a new instance that has the tool name, tool version and correlated action id from
// the first argument, and joins a sorted and unique set of action IDs and invocation IDs from all arguments.
// Nil is never returned.
func MergeMetadata(metas ...*Metadata) *Metadata {
	if len(metas) == 0 {
		return &Metadata{}
	}

	md := metas[0]
	actionIds := make(map[string]struct{}, len(metas))
	invocationIds := make(map[string]struct{}, len(metas))
	for _, m := range metas {
		actionIds[m.ActionID] = struct{}{}
		invocationIds[m.InvocationID] = struct{}{}
	}
	md.ActionID = mergeSet(actionIds)
	md.InvocationID = mergeSet(invocationIds)
	return md
}

// FromContexts returns a context derived from the first one with metadata merged from all of ctxs.
//
// If len(ctxs) == 0, ctx is returned as is.
// Returns the first error or nil.
func FromContexts(ctx context.Context, ctxs ...context.Context) (context.Context, error) {
	if len(ctxs) == 0 {
		return ctx, nil
	}

	metas := make([]*Metadata, len(ctxs)+1)
	md, err := ExtractMetadata(ctx)
	if err != nil {
		return ctx, err
	}
	metas[0] = md
	for i, c := range ctxs {
		md, err := ExtractMetadata(c)
		if err != nil {
			return ctx, err
		}
		metas[i+1] = md
	}

	// We cap to a bit less than the maximum header size in order to allow
	// for some proto fields serialization overhead.
	m := capToLimit(MergeMetadata(metas...), defaultMaxHeaderSize-100)
	return WithMetadata(ctx, m)
}

func mergeSet(set map[string]struct{}) string {
	vals := make([]string, 0, len(set))
	for v := range set {
		vals = append(vals, v)
	}
	sort.Strings(vals)
	return strings.Join(vals, ",")
}

// capToLimit ensures total length does not exceed max header size.
func capToLimit(m *Metadata, limit int) *Metadata {
	total := len(m.ToolName) + len(m.ToolVersion) + len(m.ActionID) + len(m.InvocationID) + len(m.CorrelatedInvocationID)
	excess := total - limit
	if excess <= 0 {
		return m
	}
	// We ignore the tool name, because in practice this is a
	// very short constant which makes no sense to truncate.
	diff := len(m.ActionID) - len(m.InvocationID)
	if diff > 0 {
		if diff > excess {
			m.ActionID = m.ActionID[:len(m.ActionID)-excess]
		} else {
			m.ActionID = m.ActionID[:len(m.ActionID)-diff]
			rem := (excess - diff + 1) / 2
			m.ActionID = m.ActionID[:len(m.ActionID)-rem]
			m.InvocationID = m.InvocationID[:len(m.InvocationID)-rem]
		}
	} else {
		diff = -diff
		if diff > excess {
			m.InvocationID = m.InvocationID[:len(m.InvocationID)-excess]
		} else {
			m.InvocationID = m.InvocationID[:len(m.InvocationID)-diff]
			rem := (excess - diff + 1) / 2
			m.InvocationID = m.InvocationID[:len(m.InvocationID)-rem]
			m.ActionID = m.ActionID[:len(m.ActionID)-rem]
		}
	}
	return m
}
