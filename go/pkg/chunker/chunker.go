// Package chunker provides a way to chunk an input into uploadable-size byte slices.
package chunker

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/reader"
	"github.com/golang/protobuf/proto"
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
	chunkSize int
	r         reader.ReadSeeker
	// An optional cache of the full data. It will be present in these cases:
	// * The Chunker was initialized from a []byte.
	// * Chunker.FullData was called at least once.
	// * Next() was called and the read was less than IOBufferSize.
	// Once contents are initialized, they are immutable.
	contents []byte
	// The digest carried here is for easy of data access *only*. It is never
	// checked anywhere in the Chunker logic.
	digest     digest.Digest
	offset     int64
	reachedEOF bool
	path       string
}

// NewFromBlob initializes a Chunker from the provided bytes buffer.
func NewFromBlob(blob []byte, chunkSize int) *Chunker {
	if chunkSize < 1 {
		chunkSize = DefaultChunkSize
	}
	contents := make([]byte, len(blob))
	copy(contents, blob)
	return &Chunker{
		contents:  contents,
		chunkSize: chunkSize,
		digest:    digest.NewFromBlob(blob),
	}
}

// NewFromFile initializes a Chunker from the provided file.
// The provided Digest does NOT have to match the contents of the file! It is
// for informational purposes only. Chunker won't check Digest information at
// any time. This means that late errors due to mismatch of digest and
// contents are possible.
func NewFromFile(path string, dg digest.Digest, chunkSize int) *Chunker {
	if chunkSize < 1 {
		chunkSize = DefaultChunkSize
	}
	if chunkSize > IOBufferSize {
		chunkSize = IOBufferSize
	}
	return &Chunker{
		r:         reader.NewFileReadSeeker(path, IOBufferSize),
		chunkSize: chunkSize,
		digest:    dg,
		path:      path,
	}
}

// NewFromProto initializes a Chunker from the marshalled proto message.
func NewFromProto(msg proto.Message, chunkSize int) (*Chunker, error) {
	blob, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return NewFromBlob(blob, chunkSize), nil
}

// String returns an identifiable representation of the Chunker.
func (c *Chunker) String() string {
	size := fmt.Sprintf("<%d bytes>", c.digest.Size)
	if c.path == "" {
		return size
	}
	return fmt.Sprintf("%s: %s", size, c.path)
}

// Digest returns the digest of the full data of this chunker.
func (c *Chunker) Digest() digest.Digest {
	return c.digest
}

// Offset returns the current Chunker offset.
func (c *Chunker) Offset() int64 {
	return c.offset
}

// ChunkSize returns the maximum size of each chunk.
func (c *Chunker) ChunkSize() int {
	return c.chunkSize
}

// Reset the Chunker state to when it was newly constructed.
// Useful for upload retries.
// TODO(olaola): implement Seek(offset) when we have resumable uploads.
func (c *Chunker) Reset() {
	if c.r != nil {
		c.r.SeekOffset(0)
	}
	c.offset = 0
	c.reachedEOF = false
}

// FullData returns the overall (non-chunked) underlying data. The Chunker is Reset.
// It is supposed to be used for batch uploading small inputs.
func (c *Chunker) FullData() ([]byte, error) {
	c.Reset()
	if c.contents != nil {
		return c.contents, nil
	}
	var err error
	if !c.r.IsInitialized() {
		err = c.r.Initialize()
	}
	if err != nil {
		return nil, err
	}
	// Cache contents so that the next call to FullData() doesn't result in file read.
	c.contents, err = ioutil.ReadAll(c.r)
	return c.contents, err
}

// HasNext returns whether a subsequent call to Next will return a valid chunk. Always true for a
// newly created Chunker.
func (c *Chunker) HasNext() bool {
	return !c.reachedEOF
}

// Chunk is a piece of a byte[] blob suitable for being uploaded.
type Chunk struct {
	Offset int64
	Data   []byte
}

// Next returns the next chunk of data or error. ErrEOF is returned if and only if HasNext is false.
// Chunk.Data will be empty if and only if the full underlying data is empty (in which case it will
// be the only chunk returned). Chunk.Digest will only be filled for the first chunk.
func (c *Chunker) Next() (*Chunk, error) {
	if !c.HasNext() {
		return nil, ErrEOF
	}
	if c.digest.Size == 0 {
		c.reachedEOF = true
		return &Chunk{}, nil
	}

	var data []byte
	if c.contents != nil {
		// As long as we have data in memory, it's much more efficient to return
		// a view slice than to copy it around. Contents are immutable so it's okay
		// to return the slice.
		endRead := int(c.offset) + c.chunkSize
		if endRead >= len(c.contents) {
			endRead = len(c.contents)
			c.reachedEOF = true
		}
		data = c.contents[c.offset:endRead]
	} else {
		if !c.r.IsInitialized() {
			err := c.r.Initialize()
			if err != nil {
				return nil, err
			}
		}

		// We don't need to check the amount of bytes read, as ReadFull will yell if
		// it's diff than len(data).
		data = make([]byte, c.chunkSize)
		n, err := io.ReadFull(c.r, data)
		data = data[:n]
		// Cache the contents to avoid further IO for small files.
		if err == io.ErrUnexpectedEOF || err == io.EOF {
			if c.offset == 0 {
				c.contents = data
			}
			c.reachedEOF = true
		} else if err != nil {
			return nil, err
		}
	}

	res := &Chunk{
		Offset: c.offset,
		Data:   data,
	}
	c.offset += int64(len(data))
	return res, nil
}
