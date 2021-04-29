package cas

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"sync"
)

// This file contains various IO utility types and functions.

// byteSource is a common interface to in-memory blobs and files.
// Rewind() is used to retry uploads.
type byteSource interface {
	io.Reader
	io.Closer
	Rewind() error
}

// fileSource implements byteSource on top of *os.File, with buffering.
//
// It buffering is done using a reusable bufio.Reader.
// When the fileSource is closed, the bufio.Reader is placed back to a pool.
type fileSource struct {
	f          *os.File
	bufReaders *sync.Pool
	bufReader  *bufio.Reader
}

func newFileSource(f *os.File, bufReaders *sync.Pool) *fileSource {
	bufReader := bufReaders.Get().(*bufio.Reader)
	bufReader.Reset(f)
	return &fileSource{
		f:          f,
		bufReaders: bufReaders,
		bufReader:  bufReader,
	}
}

func (f *fileSource) Read(p []byte) (int, error) {
	return f.bufReader.Read(p)
}

func (f *fileSource) Rewind() error {
	if _, err := f.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	f.bufReader.Reset(f.f)
	return nil
}

func (f *fileSource) Close() error {
	if err := f.f.Close(); err != nil {
		return err
	}

	// Put it back to the pool after closing the file, so that we don't try to
	// put it back twice.
	f.bufReaders.Put(f.bufReader)
	return nil
}

// byteSliceSource implements byteSource on top of []byte.
type byteSliceSource struct {
	io.Reader
	content []byte
}

func newByteReader(content []byte) *byteSliceSource {
	return &byteSliceSource{Reader: bytes.NewReader(content), content: content}
}

func (r *byteSliceSource) Rewind() error {
	r.Reader = bytes.NewReader(r.content)
	return nil
}

func (r *byteSliceSource) Close() error {
	return nil
}
