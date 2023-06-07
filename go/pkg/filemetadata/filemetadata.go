// Package filemetadata contains types of metadata for files, to be used for caching.
package filemetadata

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/pkg/xattr"
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
	IsDirectory  bool
	MTime        time.Time
	Err          error
	Symlink      *SymlinkMetadata
}

// FileError is the error returned by the Compute function.
type FileError struct {
	IsNotFound bool
	Err        error
}

// External xattr package can be mocked for testing through this interface.
type xattributeAccessorInterface interface {
	isSupported() bool
	getXAttr(path string, name string) ([]byte, error)
}

type xattributeAccessor struct{}

func (x xattributeAccessor) isSupported() bool {
	return xattr.XATTR_SUPPORTED
}

func (x xattributeAccessor) getXAttr(path string, name string) ([]byte, error) {
	return xattr.Get(path, name)
}

var (
	// XattrDigestName is the xattr name for the object digest.
	XattrDigestName string
	// XattrAccess is the object to control access of XattrDigestName.
	XattrAccess xattributeAccessorInterface = xattributeAccessor{}
)

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
		dest, rlErr := os.Readlink(filename)
		if rlErr != nil {
			md.Err = &FileError{Err: rlErr}
			return md
		}
		// If Readlink was OK, we set Target, even if this could be a dangling symlink.
		md.Symlink.Target = dest
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
	md.MTime = file.ModTime()
	md.IsExecutable = (mode & 0100) != 0
	if mode.IsDir() {
		md.IsDirectory = true
		return md
	}

	if len(XattrDigestName) > 0 {
		if !XattrAccess.isSupported() {
			md.Err = &FileError{Err: errors.New("x-attributes are not supported by the system")}
			return md
		}
		xattrValue, err := XattrAccess.getXAttr(filename, XattrDigestName)
		if err == nil {
			xattrStr := string(xattrValue)
			if !strings.Contains(xattrStr, "/") {
				xattrStr = fmt.Sprintf("%s/%d", xattrStr, file.Size())
			}
			md.Digest, err = digest.NewFromString(xattrStr)
			if err != nil {
				md.Err = &FileError{Err: err}
			}
			return md
		}
	}
	md.Digest, err = digest.NewFromFile(filename)
	if err != nil {
		md.Err = &FileError{Err: err}
	}
	return md
}

// Cache is a cache for file contents->Metadata.
type Cache interface {
	Get(path string) *Metadata
	Delete(filename string) error
	Update(path string, cacheEntry *Metadata) error
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
