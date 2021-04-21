package cas

import (
	"bytes"
	"fmt"
	"io"
)

// This file contains various IO utility types and functions.

// readSeekCloser can Read, Seek and Close.
// TODO(nodir): use io.ReadSeekCloser after dropping Go 1.15 support.
type readSeekCloser interface {
	io.ReadSeeker
	io.Closer
}

// byteReader partially implements readSeekCloser on top of []byte.
// Seek supports only Seek(0, 0).
type byteReader struct {
	io.Reader
	content []byte
}

func newByteReader(content []byte) *byteReader {
	return &byteReader{Reader: bytes.NewReader(content), content: content}
}

func (r *byteReader) Seek(offset int64, whence int) (int64, error) {
	if offset != 0 || whence != io.SeekCurrent {
		return 0, fmt.Errorf("unsupported")
	}
	r.Reader = bytes.NewReader(r.content)
	return 0, nil
}

func (r *byteReader) Close() error {
	return nil
}
