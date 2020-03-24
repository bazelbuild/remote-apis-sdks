package filemetadata

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/cache"
)

const (
	namespace = "filemetadatacache"
)

// Cache is a store for file digests that supports invalidation.
type fmCache struct {
	Backend     *cache.Cache
	CacheHits   uint64
	CacheMisses uint64
	Validate    bool
}

// NewSingleFlightCache returns a singleton-backed in-memory cache, with no validation.
func NewSingleFlightCache() Cache {
	return &fmCache{Backend: cache.GetInstance()}
}

// NewSingleFlightValidatedCache returns a singleton-backed in-memory cache, with validation.
func NewSingleFlightValidatedCache() Cache {
	return &fmCache{Backend: cache.GetInstance(), Validate: true}
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
	if !c.Validate {
		c.updateMetrics(ch)
		return md
	}
	valid, err := c.validate(abs, md.MTime)
	if err != nil {
		return &Metadata{Err: err}
	}
	if valid {
		c.updateMetrics(ch)
		return md
	}
	if err = c.Backend.Delete(namespace, abs); err != nil {
		return &Metadata{Err: err}
	}
	md, ch, err = c.loadMetadata(abs)
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

func (c *fmCache) check() error {
	if c.Backend == nil {
		return fmt.Errorf("no backend found for store")
	}
	return nil
}

func (c *fmCache) validate(filename string, mtime time.Time) (bool, error) {
	file, err := os.Stat(filename)
	if err != nil {
		return false, err
	}
	return file.ModTime().Equal(mtime), nil
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
		atomic.AddUint64(&c.CacheHits, 1)
	} else {
		atomic.AddUint64(&c.CacheMisses, 1)
	}
}
