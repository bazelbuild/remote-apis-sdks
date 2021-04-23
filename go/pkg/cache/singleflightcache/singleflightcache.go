// Package singleflightcache implements a cache that supports single-flight value computation.
//
// Single-flight value computation means that for concurrent callers of the cache, only one caller
// will compute the value to avoid redundant work which could be system intensive.
package singleflightcache

import (
	"sync"
)

// Cache is a cache that supports single-flight value computation.
type Cache struct {
	mu    sync.RWMutex // protectes store field itself
	store sync.Map
}

type entry struct {
	compute sync.Once
	val     interface{}
	err     error
}

// LoadOrStore is similar to a sync.Map except that it receives a function that computes the value
// to store instead of the value directly. It ensures that the function is only executed once for
// concurrent callers of the LoadOrStore function.
func (c *Cache) LoadOrStore(key interface{}, valFn func() (interface{}, error)) (interface{}, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	eUntyped, _ := c.store.LoadOrStore(key, &entry{})
	e := eUntyped.(*entry)
	e.compute.Do(func() {
		e.val, e.err = valFn()
	})
	return e.val, e.err
}

// Store forcefully updates the given cache key with val. Note that unlike LoadOrStore,
// Store accepts a value instead of a valFn since it is intended to be only used in
// cases where updates are lightweight and do not involve computing the cache value.
func (c *Cache) Store(key interface{}, val interface{}) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e := &entry{val: val}
	e.compute.Do(func() {}) // mark as computed
	c.store.Store(key, e)
}

// Delete removes a key from the cache.
func (c *Cache) Delete(key interface{}) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	c.store.Delete(key)
}

// Reset invalidates all cache entries.
func (c *Cache) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.store = sync.Map{}
}
