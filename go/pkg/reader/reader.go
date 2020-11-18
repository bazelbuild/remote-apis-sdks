package reader

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

type Initializable interface {
	IsInitialized() bool
	Initialize() error
}

type ReadSeeker interface {
	io.Reader
	Initializable
	SeekOffset(offset int64)
	Close() error
}

type fileSeeker struct {
	reader *bufio.Reader

	f           *os.File
	path        string
	buffSize    int
	seekOffset  int64
	initialized bool

	mu sync.Mutex
}

// NewFileReadSeeker wraps a buffered file reader with Seeking functionality.
// Notice that Seek calls un-set the reader and require Initialize calls. This
// is to avoid potentially unnecessary disk IO.
func NewFileReadSeeker(path string, buffsize int) ReadSeeker {
	return &fileSeeker{
		f:           nil,
		path:        path,
		buffSize:    buffsize,
		seekOffset:  0,
		initialized: false,
	}
}

// Read implements io.Reader.
func (fio *fileSeeker) Read(p []byte) (int, error) {
	if !fio.IsInitialized() {
		return 0, errors.New("Not yet initialized")
	}

	return fio.reader.Read(p)
}

// Seek is a simplified version of io.Seeker. It only supports offsets from the
// beginning of the file, and it errors lazily at the next Initialize.
func (fio *fileSeeker) SeekOffset(offset int64) {
	fio.mu.Lock()
	defer fio.mu.Unlock()

	fio.seekOffset = offset
	fio.initialized = false
	fio.reader = nil
}

// IsInitialized indicates whether this reader is ready. If false, Read calls
// will fail.
func (fio *fileSeeker) IsInitialized() bool {
	fio.mu.Lock()
	defer fio.mu.Unlock()

	return fio.initialized
}

// Initialize does the required IO pre-work for Read calls to function.
func (fio *fileSeeker) Initialize() error {
	fio.mu.Lock()
	defer fio.mu.Unlock()

	if fio.initialized {
		return errors.New("Already initialized")
	}

	if fio.f == nil {
		var err error
		fio.f, err = os.Open(fio.path)
		if err != nil {
			return err
		}
	}

	off, err := fio.f.Seek(fio.seekOffset, io.SeekStart)
	if err != nil {
		return err
	}
	if off != fio.seekOffset {
		return errors.New(fmt.Sprintf("File seeking ended at %d. Expected %d,", off, fio.seekOffset))
	}

	if fio.reader == nil {
		fio.reader = bufio.NewReaderSize(fio.f, fio.buffSize)
	} else {
		fio.reader.Reset(fio.f)
	}
	fio.initialized = true
	return nil
}

// Close closes the open file handle if it hasn't been closed yet.
func (fio *fileSeeker) Close() error {
	fio.mu.Lock()
	defer fio.mu.Unlock()

	if fio.f == nil {
		return nil
	}
	if err := fio.f.Close(); err != nil {
		return fio.f.Close()
	}
	fio.initialized = false
	return nil
}
