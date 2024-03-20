// Package uploadinfo provides a way to move metadata and/or actual data on blobs
// to be uploaded.
package uploadinfo

import (
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"google.golang.org/protobuf/proto"
)

const (
	ueBlob = iota
	uePath
	ueFile
)

// Entry should remain immutable upon creation.
// Should be created using constructor. Only Contents or Path must be set.
// In case of a malformed entry, Contents takes precedence over Path.
type Entry struct {
	Digest   digest.Digest
	Contents []byte
	Path     string

	ueType      int
	virtualFile bool
}

// IsBlob returns whether this Entry is for a blob in memory.
func (ue *Entry) IsBlob() bool {
	return ue.ueType == ueBlob
}

// IsFile returns whether this Entry is for a file in disk.
func (ue *Entry) IsFile() bool {
	return ue.ueType == uePath
}

// IsVirtualFile returns whether this Entry is a virtual file.
func (ue *Entry) IsVirtualFile() bool {
	return ue.virtualFile
}

// EntryFromBlob creates an Entry from an in memory blob.
func EntryFromBlob(blob []byte) *Entry {
	return &Entry{
		Contents: blob,
		Digest:   digest.NewFromBlob(blob),
		ueType:   ueBlob,
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
		Digest: dg,
		Path:   path,
		ueType: uePath,
	}
}

// EntryFromVirtualFile creates an entry from a file not on disk.
// The digest is expected to exist in the CAS.
func EntryFromVirtualFile(dg digest.Digest, path string) *Entry {
	return &Entry{
		Digest:      dg,
		Path:        path,
		ueType:      ueFile,
		virtualFile: true,
	}
}
