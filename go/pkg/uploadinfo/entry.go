// Package chunker provides a way to move metadata and/or actual data on blobs
// to be uploaded.
package uploadinfo

import (
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/golang/protobuf/proto"
)

const (
	UEBlob = iota
	UEPath
)

// Entry should remain immutable upon creation.
// Should be created using constructor. Only Contents or Path must be set.
// In case of a malformed entry, Contents takes precedence over Path.
type Entry struct {
	Digest   digest.Digest
	Contents []byte
	Path     string

	ueType int
}

func (ue *Entry) IsBlob() bool {
	return ue.ueType == UEBlob
}

func (ue *Entry) IsFile() bool {
	return ue.ueType == UEPath
}

func EntryFromBlob(blob []byte) *Entry {
	return &Entry{
		Contents: blob,
		Digest:   digest.NewFromBlob(blob),
		ueType:   UEBlob,
	}
}

func EntryFromProto(msg proto.Message) (*Entry, error) {
	blob, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return EntryFromBlob(blob), nil
}

func EntryFromFile(dg digest.Digest, path string) *Entry {
	return &Entry{
		Digest: dg,
		Path:   path,
		ueType: UEPath,
	}
}
