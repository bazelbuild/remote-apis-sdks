// Package filemetadata contains types of metadata for files, to be used for caching.
package filemetadata

import (
	"fmt"
	"os"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
)

// Metadata contains details for a particular file.
type Metadata struct {
	Digest       digest.Digest
	IsExecutable bool
	MTime        time.Time
	Err          error
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

// Compute computes a Metadata from a given file path.
// If an error is returned, it will be of type *FileError.
func Compute(filename string) *Metadata {
	md := &Metadata{Digest: digest.Empty}
	file, err := os.Stat(filename)
	if err != nil {
		fe := &FileError{Err: err}
		if os.IsNotExist(err) {
			fe.IsNotFound = true
		}
		md.Err = fe
		return md
	}
	md.MTime = file.ModTime()
	mode := file.Mode()
	md.IsExecutable = (mode & 0100) != 0
	if mode.IsDir() {
		md.Err = &FileError{IsDirectory: true, Err: fmt.Errorf("%s is a directory", filename)}
		return md
	}
	md.Digest, md.Err = digest.NewFromFile(filename)
	return md
}

// NoopFileMetadataCache is a non-caching metadata cache (always computes).
type NoopFileMetadataCache struct{}

// Get computes the metadata from the file contents.
// If an error is returned, it will be in Metadata.Err of type *FileError.
func (c *NoopFileMetadataCache) Get(path string) *Metadata {
	return Compute(path)
}
