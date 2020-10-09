package filemetadata

import (
	"fmt"
	"path/filepath"
	"sync/atomic"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/cache"
)

const (
	namespace = "filemetadatacache"
)

// Cache is a store for file digests that supports invalidation.
type fmCache struct {
	Backend     *cache.Cache
	cacheHits   uint64
	cacheMisses uint64
}

// NewSingleFlightCache returns a singleton-backed in-memory cache, with no validation.
func NewSingleFlightCache() Cache {
	return &fmCache{Backend: cache.GetInstance()}
}

// Get retrieves the metadata of the file with the given filename, whether from cache or by
// computing the digest.
func (c *fmCache) Get(filename string) *Metadata {
	if err := c.check(); err != nil {
		return &Metadata{Err: err}
	}
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
	if err := c.check(); err != nil {
		return err
	}
	abs, err := filepath.Abs(filename)
	if err != nil {
		return err
	}
	return c.Backend.Delete(namespace, abs)
}

// Update updates the cache entry for the filename with the given value.
func (c *fmCache) Update(filename string, cacheEntry *Metadata) error {
	absFilename, err := filepath.Abs(filename)
	if err != nil {
		return err
	}
	return c.Backend.Store(namespace, absFilename, cacheEntry)
}

// Reset clears the cache.
func (c *fmCache) Reset() {
	c.Backend.Reset()
}

// GetCacheHits returns the number of cache hits.
func (c *fmCache) GetCacheHits() uint64 {
	return c.cacheHits
}

// GetCacheMisses returns the number of cache misses.
func (c *fmCache) GetCacheMisses() uint64 {
	return c.cacheMisses
}

func (c *fmCache) check() error {
	if c.Backend == nil {
		return fmt.Errorf("no backend found for store")
	}
	return nil
}

func (c *fmCache) loadMetadata(filename string) (*Metadata, bool, error) {
	cacheHit := true
	val, err := c.Backend.LoadOrStore(namespace, filename, func() (interface{}, error) {
		cacheHit = false
		return Compute(filename), nil
	})
	if err != nil {
		return nil, false, err
	}
	md, ok := val.(*Metadata)
	if !ok {
		return nil, false, fmt.Errorf("unexpected type stored in the cache")
	}
	return md, cacheHit, nil
}

func (c *fmCache) updateMetrics(cacheHit bool) {
	if cacheHit {
		atomic.AddUint64(&c.cacheHits, 1)
	} else {
		atomic.AddUint64(&c.cacheMisses, 1)
	}
}
