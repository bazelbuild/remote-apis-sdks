package fakes

import (
	"context"
	"fmt"
	"sync"

	"github.com/bazelbuild/remote-apis-sdks/go/digest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

// ActionCache implements the RE ActionCache interface, storing fixed results.
type ActionCache struct {
	mu      sync.RWMutex
	results map[digest.Digest]*repb.ActionResult
	reads   map[digest.Digest]int
	writes  map[digest.Digest]int
}

// NewActionCache returns a new empty ActionCache.
func NewActionCache() *ActionCache {
	c := &ActionCache{}
	c.Clear()
	return c
}

// Clear removes all results from the cache.
func (c *ActionCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.results = make(map[digest.Digest]*repb.ActionResult)
	c.reads = make(map[digest.Digest]int)
	c.writes = make(map[digest.Digest]int)
}

// PutAction sets a fake result for a given action, and returns the action digest.
func (c *ActionCache) PutAction(ac *repb.Action, res *repb.ActionResult) digest.Digest {
	d := digest.TestNewFromMessage(ac)
	c.Put(d, res)
	return d
}

// Put sets a fake result for a given action digest.
func (c *ActionCache) Put(d digest.Digest, res *repb.ActionResult) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.results[d] = res
}

// Get returns a previously saved fake result for the given action digest.
func (c *ActionCache) Get(d digest.Digest) *repb.ActionResult {
	c.mu.RLock()
	defer c.mu.RUnlock()
	res, ok := c.results[d]
	if !ok {
		return nil
	}
	return res
}

// GetReads returns the number of times GetActionResult was called for a given action digest.
// These include both successful and unsuccessful reads.
func (c *ActionCache) GetReads(d digest.Digest) int {
	return c.reads[d]
}

// GetWrites returns the number of times UpdateActionResult was called for a given action digest.
func (c *ActionCache) GetWrites(d digest.Digest) int {
	return c.writes[d]
}

// GetActionResult returns a stored result, if it was found.
func (c *ActionCache) GetActionResult(ctx context.Context, req *repb.GetActionResultRequest) (res *repb.ActionResult, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	dg, err := digest.NewFromProto(req.ActionDigest)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("invalid digest received: %v", req.ActionDigest))
	}
	c.reads[dg]++
	if res, ok := c.results[dg]; ok {
		return res, nil
	}
	return nil, status.Error(codes.NotFound, "")
}

// UpdateActionResult sets/updates a given result.
func (c *ActionCache) UpdateActionResult(ctx context.Context, req *repb.UpdateActionResultRequest) (res *repb.ActionResult, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	dg, err := digest.NewFromProto(req.ActionDigest)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("invalid digest received: %v", req.ActionDigest))
	}
	if req.ActionResult != nil {
		return nil, status.Error(codes.InvalidArgument, "no action result received")
	}
	c.results[dg] = req.ActionResult
	c.writes[dg]++
	return req.ActionResult, nil
}
