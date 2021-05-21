package filemetadata

import (
	"path/filepath"
	"sync/atomic"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/cache"
)

var globalCache cache.SingleFlight

// ResetGlobalCache clears the cache globally.
// Applies to all Cache instances created by NewSingleFlightCache.
func ResetGlobalCache() {
	globalCache.Reset()
}

// Cache is a store for file digests that supports invalidation.
type fmCache struct {
	Backend     *cache.SingleFlight
	cacheHits   uint64
	cacheMisses uint64
}

// NewSingleFlightCache returns a singleton-backed in-memory cache, with no validation.
func NewSingleFlightCache() Cache {
	return &fmCache{Backend: &globalCache}
}

// Get retrieves the metadata of the file with the given filename, whether from cache or by
// computing the digest.
func (c *fmCache) Get(filename string) *Metadata {
	abs, err := filepath.Abs(filename)
	if err != nil {
		return &Metadata{Err: err}
	}
	md, ch, err := c.loadMetadata(abs)
	if err != nil {
		return &Metadata{Err: err}
	}
	c.updateMetrics(ch)
	return md
}

// Delete deletes an entry from cache.
func (c *fmCache) Delete(filename string) error {
	abs, err := filepath.Abs(filename)
	if err != nil {
		return err
	}
	c.Backend.Delete(abs)
	return nil
}

// Update updates the cache entry for the filename with the given value.
func (c *fmCache) Update(filename string, cacheEntry *Metadata) error {
	abs, err := filepath.Abs(filename)
	if err != nil {
		return err
	}
	c.Backend.Store(abs, cacheEntry)
	return nil
}

// GetCacheHits returns the number of cache hits.
func (c *fmCache) GetCacheHits() uint64 {
	return atomic.LoadUint64(&c.cacheHits)
}

// GetCacheMisses returns the number of cache misses.
func (c *fmCache) GetCacheMisses() uint64 {
	return atomic.LoadUint64(&c.cacheMisses)
}

func (c *fmCache) loadMetadata(filename string) (*Metadata, bool, error) {
	cacheHit := true
	val, err := c.Backend.LoadOrStore(filename, func() (interface{}, error) {
		cacheHit = false
		return Compute(filename), nil
	})
	if err != nil {
		return nil, false, err
	}
	return val.(*Metadata), cacheHit, nil
}

func (c *fmCache) updateMetrics(cacheHit bool) {
	if cacheHit {
		atomic.AddUint64(&c.cacheHits, 1)
	} else {
		atomic.AddUint64(&c.cacheMisses, 1)
	}
}
