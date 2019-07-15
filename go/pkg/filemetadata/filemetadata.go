// Package filemetadata contains types of metadata for files, to be used for caching.
package filemetadata

import (
	"os"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/digest"
)

// Metadata contains details for a particular file.
type Metadata struct {
	Digest       digest.Digest
	IsExecutable bool
	MTime        time.Time
}

// Compute computes a Metadata from a given file path.
func Compute(filename string) (*Metadata, error) {
	file, err := os.Stat(filename)
	if err != nil {
		return nil, err
	}
	dg, err := digest.NewFromFile(filename)
	if err != nil {
		return nil, err
	}
	return &Metadata{
		Digest:       dg,
		IsExecutable: (file.Mode() & 0100) != 0,
		MTime:        file.ModTime(),
	}, nil
}

// NoopFileMetadataCache is a non-caching metadata cache (always computes).
type NoopFileMetadataCache struct{}

// Get computes the metadata from the file contents.
func (c *NoopFileMetadataCache) Get(path string) (*Metadata, error) {
	return Compute(path)
}
