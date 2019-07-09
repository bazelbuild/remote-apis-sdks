// Package chunker provides a way to chunk an input into uploadable-size byte slices.
package chunker

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/bazelbuild/remote-apis-sdks/go/digest"
)

// DefaultChunkSize is the default chunk size for ByteStream.Write RPCs.
const DefaultChunkSize = 1024 * 1024

// IOBufferSize regulates how many bytes at a time the Chunker will read from a file source.
var IOBufferSize = 10 * 1024 * 1024

// ErrEOF is returned when Next is called when HasNext is false.
var ErrEOF = errors.New("ErrEOF")

// Chunker can be used to chunk an input into uploadable-size byte slices.
// A single Chunker is NOT thread-safe; it should be used by a single uploader thread.
type Chunker struct {
	chunkSize   int
	reader      *bufio.Reader
	contents    []byte
	digest      digest.Digest
	offset      int64
	initialized bool
	path        string
}

// NewFromBlob initializes a Chunker from the provided bytes buffer.
func NewFromBlob(blob []byte, chunkSize int) *Chunker {
	if chunkSize < 1 {
		chunkSize = DefaultChunkSize
	}
	return &Chunker{
		contents:  blob,
		chunkSize: chunkSize,
		digest:    digest.NewFromBlob(blob),
	}
}

// NewFromFile initializes a Chunker from the provided file.
// The provided Digest has to match the contents of the file! If the size of the actual contents is
// shorter than the provided Digest size, the Chunker will error on Next(), but otherwise the
// results are unspecified.
func NewFromFile(path string, dg digest.Digest, chunkSize int) *Chunker {
	if chunkSize < 1 {
		chunkSize = DefaultChunkSize
	}
	if chunkSize > IOBufferSize {
		chunkSize = IOBufferSize
	}
	return &Chunker{
		chunkSize: chunkSize,
		digest:    dg,
		path:      path,
	}
}

// String returns an identifiable representation of the Chunker.
func (c *Chunker) String() string {
	size := fmt.Sprintf("<%d bytes>", c.Size())
	if c.path == "" {
		return size
	}
	return fmt.Sprintf("%s: %s", size, c.path)
}

// Size returns the size in bytes of the full data of this chunker.
func (c *Chunker) Size() int64 {
	return c.digest.Size
}

// Offset returns the current Chunker offset.
func (c *Chunker) Offset() int64 {
	return c.offset
}

func (c *Chunker) bytesLeft() int64 {
	return c.digest.Size - c.offset
}

// ChunkSize returns the maximum size of each chunk.
func (c *Chunker) ChunkSize() int {
	return c.chunkSize
}

// Reset the Chunker state to when it was newly constructed.
// Useful for upload retries.
// TODO(olaola): implement Seek(offset) when we have resumable uploads.
func (c *Chunker) Reset() {
	c.reader = nil
	c.offset = 0
	c.initialized = false
}

// FullData returns the overall (non-chunked) underlying data. The Chunker is Reset.
// It is supposed to be used for batch uploading small inputs.
func (c *Chunker) FullData() ([]byte, error) {
	c.Reset()
	if c.contents != nil {
		return c.contents, nil
	}
	var err error
	// Cache contents so that the next call to FullData() doesn't result in file read.
	c.contents, err = ioutil.ReadFile(c.path)
	return c.contents, err
}

// HasNext returns whether a subsequent call to Next will return a valid chunk. Always true for a
// newly created Chunker.
func (c *Chunker) HasNext() bool {
	return !c.initialized || c.bytesLeft() > 0
}

// Chunk is a piece of a byte[] blob suitable for being uploaded. The first Chunk returned by a
// Chunker will contain the Digest of the whole data. In all other cases, the Digest will be nil.
type Chunk struct {
	Digest *digest.Digest
	Offset int64
	Data   []byte
}

var emptyChunk = &Chunk{Digest: &digest.Empty}

// Next returns the next chunk of data or error. ErrEOF is returned if and only if HasNext is false.
// Chunk.Data will be empty if and only if the full underlying data is empty (in which case it will
// be the only chunk returned). Chunk.Digest will only be filled for the first chunk.
func (c *Chunker) Next() (*Chunk, error) {
	if !c.HasNext() {
		return nil, ErrEOF
	}
	c.initialized = true
	if c.digest.Size == 0 {
		return emptyChunk, nil
	}
	if c.contents == nil && c.reader == nil {
		f, err := os.Open(c.path)
		if err != nil {
			return nil, err
		}
		c.reader = bufio.NewReaderSize(f, IOBufferSize)
	}
	bytesLeft := c.bytesLeft()
	bytesToSend := c.chunkSize
	if bytesLeft < int64(bytesToSend) {
		bytesToSend = int(bytesLeft)
	}
	var data []byte
	if c.reader != nil {
		if c.offset == 0 && c.digest.Size <= int64(IOBufferSize) {
			data = make([]byte, c.digest.Size)
			c.contents = data // Cache the full contents to avoid Reads on future Resets.
		} else {
			data = make([]byte, bytesToSend)
		}
		n, err := io.ReadFull(c.reader, data)
		if err != nil {
			return nil, err
		}
		if n < bytesToSend {
			return nil, fmt.Errorf("only read %d bytes from %s, expected %d", n, c.path, bytesToSend)
		}
	} else {
		// Contents are immutable so it's okay to return a slice.
		data = c.contents[c.offset : int(c.offset)+bytesToSend]
	}
	res := &Chunk{
		Offset: c.offset,
		Data:   data[:bytesToSend],
	}
	if c.offset == 0 {
		res.Digest = &c.digest
	}
	c.offset += int64(bytesToSend)
	return res, nil
}
