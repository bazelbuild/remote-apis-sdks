package reader

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/klauspost/compress/zstd"
)

type Initializable interface {
	IsInitialized() bool
	Initialize() error
}

type ReadSeeker interface {
	io.Reader
	io.Closer
	Initializable
	SeekOffset(offset int64) error
}

type fileSeeker struct {
	reader *bufio.Reader

	f           *os.File
	path        string
	buffSize    int
	seekOffset  int64
	initialized bool
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

// Close closes the reader. It still can be reopened with Initialize().
func (fio *fileSeeker) Close() (err error) {
	fio.initialized = false
	if fio.f != nil {
		err = fio.f.Close()
	}
	fio.f = nil
	fio.reader = nil
	return err
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
func (fio *fileSeeker) SeekOffset(offset int64) error {
	fio.seekOffset = offset
	fio.initialized = false
	fio.reader = nil
	return nil
}

// IsInitialized indicates whether this reader is ready. If false, Read calls
// will fail.
func (fio *fileSeeker) IsInitialized() bool {
	return fio.initialized
}

// Initialize does the required IO pre-work for Read calls to function.
func (fio *fileSeeker) Initialize() error {
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

type compressedSeeker struct {
	fs   ReadSeeker
	encd *zstd.Encoder
	// This keeps the compressed data
	buf *bytes.Buffer
}

// NewCompressedFileSeeker creates a ReadSeeker based on a file path.
func NewCompressedFileSeeker(path string, buffsize int) (ReadSeeker, error) {
	return NewCompressedSeeker(NewFileReadSeeker(path, buffsize))
}

// NewCompressedSeeker wraps a ReadSeeker to compress its data on the fly.
func NewCompressedSeeker(fs ReadSeeker) (ReadSeeker, error) {
	if _, ok := fs.(*compressedSeeker); ok {
		return nil, errors.New("trying to double compress files")
	}

	buf := bytes.NewBuffer(nil)
	encd, err := zstd.NewWriter(buf)
	return &compressedSeeker{
		fs:   fs,
		encd: encd,
		buf:  buf,
	}, err
}

func (cfs *compressedSeeker) Read(p []byte) (int, error) {
	var n int
	var errR, errW error
	for cfs.buf.Len() < len(p) && errR == nil && errW == nil {
		// Read is allowed to use the entity of p as a scratchpad.
		n, errR = cfs.fs.Read(p)
		// errW must be non-nil if written bytes != from n.
		_, errW = cfs.encd.Write(p[:n])
	}
	m, errR2 := cfs.buf.Read(p)

	var retErr error
	if errR != nil {
		retErr = errR
	} else if errW != nil {
		retErr = errW
	} else {
		retErr = errR2
	}

	if retErr != nil {
		cfs.encd.Close()
	}

	return m, retErr
}

func (cfs *compressedSeeker) SeekOffset(offset int64) error {
	cfs.buf.Reset()
	if err := cfs.encd.Close(); err != nil {
		return err
	}

	cfs.encd.Reset(cfs.buf)
	if err := cfs.fs.SeekOffset(offset); err != nil {
		return err
	}

	return nil
}

func (cfs *compressedSeeker) IsInitialized() bool { return cfs.fs.IsInitialized() }
func (cfs *compressedSeeker) Initialize() error   { return cfs.fs.Initialize() }

func (cfs *compressedSeeker) Close() error { return cfs.fs.Close() }
