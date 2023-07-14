package casng

import (
	"sync"

	"google.golang.org/protobuf/proto"
)

// nodeSliceMap is a mutex-guarded map that supports a synchronized append operation.
type nodeSliceMap struct {
	store map[string][]proto.Message
	mu    sync.RWMutex
}

// load returns the slice associated with the given key or nil.
func (c *nodeSliceMap) load(key string) []proto.Message {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.store[key]
}

// append appends the specified value to the slice associated with the specified key.
func (c *nodeSliceMap) append(key string, m proto.Message) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.store[key] = append(c.store[key], m)
}
