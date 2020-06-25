// Package singleflightcache implements a cache that supports single-flight value computation.
//
// Single-flight value computation means that for concurrent callers of the cache, only one caller
// will compute the value to avoid redundant work which could be system intensive.
package singleflightcache

import (
	"fmt"
	"sync"
)

// Cache is a cache that supports single-flight value computation.
type Cache struct {
	// store is the actual cache where data is stored.
	store sync.Map
	// locks is a map of locks per key. This map is used to ensure only one reader/writer of
	// the key can have write access.
	locks sync.Map
	// mu is a mutex to ensure exclusive access to the locks map in the case of Delete.
	mu sync.RWMutex
}

// LoadOrStore is similar to a sync.Map except that it receives a function that computes the value
// to store instead of the value directly. It ensures that the function is only executed once for
// concurrent callers of the LoadOrStore function.
func (c *Cache) LoadOrStore(key interface{}, valFn func() (interface{}, error)) (interface{}, error) {
	val, ok := c.store.Load(key)
	if ok {
		return val, nil
	}
	// Lock the cache for reading to avoid a race condition with Delete.
	c.mu.RLock()
	defer c.mu.RUnlock()
	mu := &sync.RWMutex{}
	mu.Lock()
	defer mu.Unlock()

	l, loaded := c.locks.LoadOrStore(key, mu)
	lock, ok := l.(*sync.RWMutex)
	if !ok {
		return nil, fmt.Errorf("unexpected type found in lock map")
	}
	if loaded {
		lock.RLock()
		val, ok := c.store.Load(key)
		lock.RUnlock()
		if ok {
			return val, nil
		}
		// If no value is in the cache, then the lock creator failed to set a value. Upgrade
		// the lock to a write lock and re-attempt to compute the value.
		lock.Lock()
		defer lock.Unlock()
	}
	val, err := valFn()
	if err != nil {
		return nil, err
	}
	c.store.Store(key, val)
	return val, nil
}

// Store forcefully updates the given cache key with val. Note that unlike LoadOrStore,
// Store accepts a value instead of a valFn since it is intended to be only used in
// cases where updates are lightweight and do not involve computing the cache value.
func (c *Cache) Store(key interface{}, val interface{}) error {
	// Lock the cache for reading to avoid a race condition with Delete.
	c.mu.RLock()
	defer c.mu.RUnlock()

	l, _ := c.locks.LoadOrStore(key, &sync.RWMutex{})
	lock, ok := l.(*sync.RWMutex)
	if !ok {
		return fmt.Errorf("unexpected type found in lock map")
	}
	lock.Lock()
	defer lock.Unlock()
	c.store.Store(key, val)
	return nil
}

// Delete removes a key from the cache.
func (c *Cache) Delete(key interface{}) {
	if _, exists := c.locks.Load(key); exists {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.locks.Delete(key)
		c.store.Delete(key)
	}
}
