// Package uploadinfo provides a way to move metadata and/or actual data on blobs
// to be uploaded.
package uploadinfo

import (
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"google.golang.org/protobuf/proto"
)

const (
	kindBlob = iota
	kindFile
)

// Entry describes an immutable upload entry.
type Entry struct {
	digest digest.Digest
	blob   []byte
	path   string

	// The code now depends on this internal property to distinguish the entry type.
	// A kindBlob entry can have a nil blob field and a kindFile entry can have an empty file path.
	kind int
}

// Kind is a debugging helper function that returns a string representation
// of the entry's kind.
func (e *Entry) Kind() string {
	switch e.kind {
	case kindBlob:
		return "blob"
	case kindFile:
		return "file"
	default:
		return "unknown"
	}
}

// IsBlob returns true iff the entry was created from in-memory bytes.
func (e *Entry) IsBlob() bool {
	return e.kind == kindBlob
}

// IsFile returns true iff the entry was created from a file path.
func (e *Entry) IsFile() bool {
	return e.kind == kindFile
}

// Digest returns a copy of the digest of this Entry.
func (e *Entry) Digest() digest.Digest {
	return e.digest
}

// Blob returns a copy of the bytes of this Entry.
// This method cannot be used to infer the type of this entry.
// A byte slice is always returned (never returns nil).
func (e *Entry) Blob() []byte {
	bytes := make([]byte, len(e.blob))
	copy(bytes, e.blob)
	return bytes
}

// BlobLen is a convenient method to avoid the copy cost of Blob().
// This method cannot be used to infer the type of this entry.
func (e *Entry) BlobLen() int {
	return len(e.blob)
}

// Path returns the file path of this Entry.
// If !e.IsFile(), return an empty string.
func (e *Entry) Path() string {
	return e.path
}

// EntryFromBlob creates an Entry from an in memory blob.
func EntryFromBlob(blob []byte) *Entry {
	return &Entry{
		blob:   blob,
		digest: digest.NewFromBlob(blob),
		kind:   kindBlob,
	}
}

// EntryFromProto creates an Entry from an in memory proto.
func EntryFromProto(msg proto.Message) (*Entry, error) {
	blob, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return EntryFromBlob(blob), nil
}

// EntryFromFile creates an entry from a file in disk.
func EntryFromFile(dg digest.Digest, path string) *Entry {
	return &Entry{
		digest: dg,
		path:   path,
		kind:   kindFile,
	}
}
