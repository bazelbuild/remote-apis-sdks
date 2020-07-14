// Package filemetadata contains types of metadata for files, to be used for caching.
package filemetadata

import (
	"fmt"
	"os"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
)

// SymlinkMetadata contains details if the given path is a symlink.
type SymlinkMetadata struct {
	Target     string
	IsDangling bool
}

// Metadata contains details for a particular file.
type Metadata struct {
	Digest       digest.Digest
	IsExecutable bool
	Err          error
	Symlink      *SymlinkMetadata
}

// FileError is the error returned by the Compute function.
type FileError struct {
	IsDirectory bool
	IsNotFound  bool
	Err         error
}

// Error returns the error message.
func (e *FileError) Error() string {
	return e.Err.Error()
}

func isSymlink(filename string) (bool, error) {
	file, err := os.Lstat(filename)
	if err != nil {
		return false, err
	}
	return file.Mode()&os.ModeSymlink != 0, nil
}

// Compute computes a Metadata from a given file path.
// If an error is returned, it will be of type *FileError.
func Compute(filename string) *Metadata {
	md := &Metadata{Digest: digest.Empty}
	file, err := os.Stat(filename)
	if isSym, _ := isSymlink(filename); isSym {
		md.Symlink = &SymlinkMetadata{}
		if dest, rlErr := os.Readlink(filename); rlErr != nil {
			md.Err = &FileError{Err: rlErr}
			return md
		} else {
			// If Readlink was OK, we set Target, even if this could be a dangling symlink.
			md.Symlink.Target = dest
		}

		if err != nil {
			md.Err = &FileError{Err: err}
			md.Symlink.IsDangling = true
			return md
		}
	}

	if err != nil {
		fe := &FileError{Err: err}
		if os.IsNotExist(err) {
			fe.IsNotFound = true
		}
		md.Err = fe
		return md
	}
	mode := file.Mode()
	md.IsExecutable = (mode & 0100) != 0
	if mode.IsDir() {
		md.Err = &FileError{IsDirectory: true, Err: fmt.Errorf("%s is a directory", filename)}
		return md
	}
	md.Digest, md.Err = digest.NewFromFile(filename)
	return md
}

// Cache is a cache for file contents->Metadata.
type Cache interface {
	Get(path string) *Metadata
	Delete(filename string) error
	Update(path string, cacheEntry *Metadata) error
	Reset()
	GetCacheHits() uint64
	GetCacheMisses() uint64
}

type noopCache struct{}

// Get computes the metadata from the file contents.
// If an error is returned, it will be in Metadata.Err of type *FileError.
func (c *noopCache) Get(path string) *Metadata {
	return Compute(path)
}

// Delete removes an entry from the cache. It is a noop for the Noop cache.
func (c *noopCache) Delete(string) error {
	return nil
}

// Update updates a cache entry with the given value. It is a noop for Noop cache.
func (c *noopCache) Update(string, *Metadata) error {
	return nil
}

// Reset clears the cache. It is a noop for the Noop cache.
func (c *noopCache) Reset() {}

// GetCacheHits returns the number of cache hits. It returns 0 for Noop cache.
func (c *noopCache) GetCacheHits() uint64 {
	return 0
}

// GetCacheMisses returns the number of cache misses.
// It returns 0 for Noop cache.
func (c *noopCache) GetCacheMisses() uint64 {
	return 0
}

// NewNoopCache returns a cache that doesn't cache (evaluates on every Get).
func NewNoopCache() Cache {
	return &noopCache{}
}
