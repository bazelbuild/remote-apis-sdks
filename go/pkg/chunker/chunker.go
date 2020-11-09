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
	"github.com/klauspost/compress/zstd"
)

// DefaultChunkSize is the default chunk size for ByteStream.Write RPCs.
const DefaultChunkSize = 1024 * 1024

// IOBufferSize regulates how many bytes at a time the Chunker will read from a file source.
var IOBufferSize = 10 * 1024 * 1024

// ErrEOF is returned when Next is called when HasNext is false.
var ErrEOF = errors.New("ErrEOF")

// Compressor for full blobs
var fullCompressor *zstd.Encoder

type UploadEntry struct {
	digest   digest.Digest
	contents []byte
	path     string
}

// Digest returns the digest of the full data of this chunker.
func (ue *UploadEntry) Digest() digest.Digest {
	return ue.digest
}

func EntryFromBlob(blob []byte) *UploadEntry {
	if blob == nil {
		blob = make([]byte, 0)
	}
	return &UploadEntry{
		contents: blob,
		digest:   digest.NewFromBlob(blob),
	}
}

func EntryFromProto(msg proto.Message) (*UploadEntry, error) {
	blob, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return EntryFromBlob(blob), nil
}

func EntryFromFile(dg digest.Digest, path string) *UploadEntry {
	return &UploadEntry{
		digest: dg,
		path:   path,
	}
}

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
	contents   []byte
	path       string
	offset     int64
	reachedEOF bool
	ue         *UploadEntry
	compressed bool
}

func New(ue *UploadEntry, compressed bool, chunkSize int) (*Chunker, error) {
	if compressed {
		return nil, errors.New("compression is not supported yet")
	}
	if chunkSize < 1 {
		chunkSize = DefaultChunkSize
	}
	var c *Chunker
	if ue.contents != nil {
		c = chunkerFromBlob(ue.contents, compressed)
	} else if ue.path != "" {
		var err error
		c, err = chunkerFromFile(ue.path, compressed)
		if err != nil {
			return nil, err
		}

		if chunkSize > IOBufferSize {
			chunkSize = IOBufferSize
		}
	} else {
		return nil, errors.New("Invalid UEntry. Content and path cannot both be nil.")
	}

	c.chunkSize = chunkSize
	c.ue = ue
	return c, nil
}

func chunkerFromBlob(blob []byte, compressed bool) *Chunker {
	var contents []byte
	if compressed {
		contents = fullCompressor.EncodeAll(blob, nil)
	} else {
		contents = make([]byte, len(blob))
		copy(contents, blob)
	}

	return &Chunker{
		contents: contents,
	}
}

func chunkerFromFile(path string, compressed bool) (*Chunker, error) {
	var r reader.ReadSeeker
	if compressed {
		var err error
		r, err = reader.NewCompressedFileSeeker(path, IOBufferSize)
		if err != nil {
			return nil, err
		}
	} else {
		r = reader.NewFileReadSeeker(path, IOBufferSize)
	}

	return &Chunker{
		r:    r,
		path: path,
	}, nil
}

// String returns an identifiable representation of the Chunker.
func (c *Chunker) String() string {
	size := fmt.Sprintf("<%d bytes>", c.ue.digest.Size)
	if c.path == "" {
		return size
	}
	return fmt.Sprintf("%s: %s", size, c.path)
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
		// We're ignoring the error here, as not to change the fn signature.
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
	if c.ue.digest.Size == 0 {
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
