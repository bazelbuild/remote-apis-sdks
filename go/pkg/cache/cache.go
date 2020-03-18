// Package cache implements a cache backend.
package cache

import (
	"fmt"
	"sync"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/cache/singleflightcache"
)

// Cache is a cache backend.
type Cache struct {
	mu     *sync.RWMutex
	caches *sync.Map
}

var (
	instance *Cache
	once     sync.Once
)

// GetInstance retrieves the singleton instance of the cache backend. This is useful in the
// future to enforce memory bounds on the entire cache usage of a program.
func GetInstance() *Cache {
	once.Do(func() {
		instance = &Cache{
			mu:     &sync.RWMutex{},
			caches: &sync.Map{},
		}
	})
	return instance
}

// Reset resets the cache.
func (c *Cache) Reset() {
	c.mu = &sync.RWMutex{}
	c.caches = &sync.Map{}
}

// LoadOrStore attempts to first read a value from the corresponding cache namespace. If no entry
// is found, it will use the return value of the passed fn to store in the cache. Concurrent
// callers of LoadOrStore on the same namespace and key will execute fn once. This is to avoid
// costly redundant work to compute the value to store in cache.
func (c *Cache) LoadOrStore(ns string, key interface{}, fn func() (interface{}, error)) (interface{}, error) {
	// Load first to avoid instantiating a new cache for LoadOrStore.
	nsCache, ok := c.caches.Load(ns)
	if !ok {
		nsCache, _ = c.caches.LoadOrStore(ns, &singleflightcache.Cache{})
	}
	cache, ok := nsCache.(*singleflightcache.Cache)
	if !ok {
		return nil, fmt.Errorf("unexpected type in namespace cache map")
	}
	return cache.LoadOrStore(key, fn)
}

// Delete deletes a value corresponding to the given namespace and key.
func (c *Cache) Delete(ns string, key interface{}) error {
	// Load first to avoid instantiating a new cache for LoadOrStore.
	nsCache, ok := c.caches.Load(ns)
	if !ok {
		nsCache, _ = c.caches.LoadOrStore(ns, &singleflightcache.Cache{})
	}
	cache, ok := nsCache.(*singleflightcache.Cache)
	if !ok {
		return fmt.Errorf("unexpected type in namespace cache map")
	}
	cache.Delete(key)
	return nil
}
