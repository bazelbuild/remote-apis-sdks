package cas

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"
)

// This file contains various IO utility types and functions.

// uploadSource is a common interface to in-memory blobs and files.
// SeekStart is like io.Seeker.Seek, but only supports io.SeekStart.
type uploadSource interface {
	io.Reader
	io.Closer
	SeekStart(offset int64) error
}

// byteSliceSource implements uploadSource on top of []byte.
type byteSliceSource struct {
	io.Reader
	content []byte
}

func newByteSliceSource(content []byte) *byteSliceSource {
	return &byteSliceSource{Reader: bytes.NewReader(content), content: content}
}

func (s *byteSliceSource) SeekStart(offset int64) error {
	if offset < 0 || offset >= int64(len(s.content)) {
		return fmt.Errorf("offset out of range")
	}
	s.Reader = bytes.NewReader(s.content[offset:])
	return nil
}

func (s *byteSliceSource) Close() error {
	return nil
}

// fileSource implements uploadSource on top of *os.File, with buffering.
//
// Buffering is done using a reusable bufio.Reader.
// When the fileSource is closed, the bufio.Reader is placed back to a pool.
type fileSource struct {
	f     *os.File
	r     *bufio.Reader
	rPool *sync.Pool
}

func newFileSource(f *os.File, bufReaders *sync.Pool) *fileSource {
	r := bufReaders.Get().(*bufio.Reader)
	r.Reset(f)
	return &fileSource{
		f:     f,
		r:     r,
		rPool: bufReaders,
	}
}

func (f *fileSource) Read(p []byte) (int, error) {
	return f.r.Read(p)
}

func (f *fileSource) SeekStart(offset int64) error {
	if _, err := f.f.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	f.r.Reset(f.f)
	return nil
}

func (f *fileSource) Close() error {
	if err := f.f.Close(); err != nil {
		return err
	}

	// Put it back to the pool only after closing the file, so that we don't try
	// to put it back twice in case Close() is called again.
	f.rPool.Put(f.r)
	return nil
}
