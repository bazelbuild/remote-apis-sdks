// Package cache implements a cache backend.
package cache

import (
	"sync"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/cache/singleflightcache"
)

// Cache is a cache backend.
type Cache struct {
	caches sync.Map
}

var instance Cache

// GetInstance retrieves the singleton instance of the cache backend. This is useful in the
// future to enforce memory bounds on the entire cache usage of a program.
func GetInstance() *Cache {
	return &instance
}

// Reset resets the cache in one namespace.
func (c *Cache) Reset(ns string) {
	c.caches.Delete(ns)
}

// ResetAll resets the cache.
func (c *Cache) ResetAll() {
	*c = Cache{}
}

func (c *Cache) getNSCache(ns string) *singleflightcache.Cache {
	// Load first to avoid instantiating a new cache for LoadOrStore.
	nsCache, ok := c.caches.Load(ns)
	if !ok {
		nsCache, _ = c.caches.LoadOrStore(ns, &singleflightcache.Cache{})
	}
	return nsCache.(*singleflightcache.Cache)
}

// LoadOrStore attempts to first read a value from the corresponding cache namespace. If no entry
// is found, it will use the return value of the passed fn to store in the cache. Concurrent
// callers of LoadOrStore on the same namespace and key will execute fn once. This is to avoid
// costly redundant work to compute the value to store in cache.
func (c *Cache) LoadOrStore(ns string, key interface{}, fn func() (interface{}, error)) (interface{}, error) {
	return c.getNSCache(ns).LoadOrStore(key, fn)
}

// Store is similar to LoadOrStore, except it does not check if a cache entry
// already exists for the given key and simply overwrites the value of the key
// in the cache with the given value.
func (c *Cache) Store(ns string, key interface{}, val interface{}) {
	c.getNSCache(ns).Store(key, val)
}

// Delete deletes a value corresponding to the given namespace and key.
func (c *Cache) Delete(ns string, key interface{}) {
	c.getNSCache(ns).Delete(key)
}
