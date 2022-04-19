package reader

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/mostynb/zstdpool-syncpool"
)

// Initializable is an interface containing methods to initialize a ReadSeeker.
type Initializable interface {
	IsInitialized() bool
	Initialize() error
}

// ReadSeeker is an interface used to capture a file reader with seek functionality.
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
		return fmt.Errorf("File seeking ended at %d. Expected %d,", off, fio.seekOffset)
	}

	if fio.reader == nil {
		fio.reader = bufio.NewReaderSize(fio.f, fio.buffSize)
	} else {
		fio.reader.Reset(fio.f)
	}
	fio.initialized = true
	return nil
}

// The zstd encoder lib will async write to the buffer, so we need
// to lock access to actually check for contents.
type syncedBuffer struct {
	mu  sync.Mutex
	buf *bytes.Buffer
}

func (sb *syncedBuffer) Read(p []byte) (int, error) {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	return sb.buf.Read(p)
}

func (sb *syncedBuffer) Write(p []byte) (int, error) {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	return sb.buf.Write(p)
}

func (sb *syncedBuffer) Len() int {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	return sb.buf.Len()
}

func (sb *syncedBuffer) Reset() {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	sb.buf.Reset()
}

type compressedSeeker struct {
	fs    ReadSeeker
	encdW *syncpool.EncoderWrapper
	// This keeps the compressed data
	buf *syncedBuffer
	err error
}

var encoderInit sync.Once
var encoders *sync.Pool

// NewCompressedFileSeeker creates a ReadSeeker based on a file path.
func NewCompressedFileSeeker(path string, buffsize int) (ReadSeeker, error) {
	return NewCompressedSeeker(NewFileReadSeeker(path, buffsize))
}

// NewCompressedSeeker wraps a ReadSeeker to compress its data on the fly.
func NewCompressedSeeker(fs ReadSeeker) (ReadSeeker, error) {
	if _, ok := fs.(*compressedSeeker); ok {
		return nil, errors.New("trying to double compress files")
	}

	encoderInit.Do(func() {
		encoders = syncpool.NewEncoderPool(zstd.WithEncoderConcurrency(1))
	})

	buf := bytes.NewBuffer(nil)
	sb := &syncedBuffer{buf: buf}

	encdIntf := encoders.Get()
	encdW, ok := encdIntf.(*syncpool.EncoderWrapper)
	if !ok || encdW == nil {
		return nil, errors.New("failed creating new encoder")
	}

	encdW.Encoder.Reset(sb)
	return &compressedSeeker{
		fs:    fs,
		encdW: encdW,
		buf:   sb,
		err:   nil,
	}, nil
}

func (cfs *compressedSeeker) Read(p []byte) (int, error) {
	if cfs.err != nil {
		return 0, cfs.err
	}

	var n int
	var errR, errW error
	for cfs.encdW != nil && cfs.buf.Len() < len(p) && errR == nil && errW == nil {
		// Read is allowed to use the entity of p as a scratchpad.
		n, errR = cfs.fs.Read(p)
		// errW must be non-nil if written bytes != from n.
		_, errW = cfs.encdW.Encoder.Write(p[:n])
	}

	// We have to treat EOF differently. It's the only "everything is going according
	// to plan" error. EOF implies our uncompressed input data has finished reading,
	// but it doesn't mean that we've finished preparing/returning our compressed data.
	// We only want to return EOF to the user when the _compressed_ data is finished reading.
	if errR != nil && errR != io.EOF {
		cfs.err = errR
	} else if errW != nil {
		cfs.err = errW
	}

	// When the buffer ends, or in case of _any_ error, we compress all the bytes we
	// had available. The encoder requires a Close call to finish writing compressed
	// data smaller than zstd's window size.
	var errC error
	if cfs.err != nil || errR == io.EOF {
		errC = cfs.encdW.Encoder.Close()
		encoders.Put(cfs.encdW)
		cfs.encdW = nil
	}

	m, errR2 := cfs.buf.Read(p)

	if cfs.err == nil {
		if errC != nil {
			cfs.err = errC
		} else if errR2 != nil {
			cfs.err = errR2
		}
	}

	return m, cfs.err
}

func (cfs *compressedSeeker) SeekOffset(offset int64) error {
	cfs.buf.Reset()
	if cfs.encdW == nil {
		encdIntf := encoders.Get()
		var ok bool
		cfs.encdW, ok = encdIntf.(*syncpool.EncoderWrapper)
		if !ok || cfs.encdW == nil {
			return errors.New("failed to get a new encoder")
		}
	} else if err := cfs.encdW.Encoder.Close(); err != nil {
		encoders.Put(cfs.encdW)
		cfs.encdW = nil
		return err
	}

	cfs.buf.Reset()
	cfs.encdW.Encoder.Reset(cfs.buf)
	if err := cfs.fs.SeekOffset(offset); err != nil {
		return err
	}

	return nil
}

func (cfs *compressedSeeker) IsInitialized() bool { return cfs.fs.IsInitialized() }
func (cfs *compressedSeeker) Initialize() error   { return cfs.fs.Initialize() }

// No need for close to close the encoder - that's handled by Read.
func (cfs *compressedSeeker) Close() error { return cfs.fs.Close() }
