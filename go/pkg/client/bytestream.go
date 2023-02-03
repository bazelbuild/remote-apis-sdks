package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"

	log "github.com/golang/glog"
	"github.com/pkg/errors"
	bspb "google.golang.org/genproto/googleapis/bytestream"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/chunker"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/uploadinfo"
)

// ByteStreamWriteOption sets options required for resumable upload.
type ByteStreamWriteOption interface {
	Apply(*ByteStreamWriteOpts)
}

// ByteSteramOptFinishWrite is a boolean flag to set FinishWrite
// in ByteStream.WriteRequest.
type ByteSteramOptFinishWrite bool

// Apply sets ByteSteramOptFinishWrite in ByteStreamWriteOpts.
func (o ByteSteramOptFinishWrite) Apply(opts *ByteStreamWriteOpts) {
	opts.FinishWrite = bool(o)
}

// ByteStreamOptOffset is offset for resumable upload.
type ByteStreamOptOffset int64

// Apply sets ByteStreamOptOffset in ByteStreamWriteOpts.
func (o ByteStreamOptOffset) Apply(opts *ByteStreamWriteOpts) {
	opts.LastLogicalOffset = int64(o)
}

// WriteBytes uploads a byte slice.
func (c *Client) WriteBytes(ctx context.Context, name string, data []byte) error {
	ue := uploadinfo.EntryFromBlob(data)
	ch, err := chunker.New(ue, false, int(c.ChunkMaxSize))
	if err != nil {
		return err
	}
	_, err = c.writeChunked(ctx, name, ch)
	return err
}

// WriteBytesWithOffset uploads a byte slice with a given resource name to the CAS
// at an arbitrary offset.
func (c *Client) WriteBytesWithOffset(ctx context.Context, name string, data []byte, opts ...ByteStreamWriteOption) (int64, error) {
	ue := uploadinfo.EntryFromBlob(data)
	ch, err := chunker.New(ue, false, int(c.ChunkMaxSize))
	if err != nil {
		return 0, err
	}
	return c.writeChunked(ctx, name, ch, opts...)
}

// writeChunked uploads chunked data with a given resource name to the CAS.
func (c *Client) writeChunked(ctx context.Context, name string, ch *chunker.Chunker, rawOpts ...ByteStreamWriteOption) (int64, error) {
	if len(rawOpts) == 1 {
		return 0, errors.Errorf("Missing an arugment")
	}
	opts := &ByteStreamWriteOpts{}
	var resume bool
	for _, o := range rawOpts {
		resume = true
		o.Apply(opts)
	}

	var totalBytes int64
	closure := func() error {
		// Retry by starting the stream from the beginning.
		if err := ch.Reset(); err != nil {
			return errors.Wrap(err, "failed to Reset")
		}
		totalBytes = int64(0)

		stream, err := c.Write(ctx)
		if err != nil {
			return err
		}
		for ch.HasNext() {
			req := &bspb.WriteRequest{ResourceName: name}
			chunk, err := ch.Next()
			if err != nil {
				return err
			}
			req.WriteOffset = chunk.Offset
			// If LastLogicaloffset is provided as an option, we want uploading bytes
			// at the last logcial offset + the chunk offset which is relative to Chunker passed in.
			if resume {
				req.WriteOffset += opts.LastLogicalOffset
			}
			req.Data = chunk.Data

			if (!resume && !ch.HasNext()) || (resume && !ch.HasNext() && opts.FinishWrite) {
				req.FinishWrite = true
			}
			err = c.CallWithTimeout(ctx, "Write", func(_ context.Context) error { return stream.Send(req) })
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			totalBytes += int64(len(req.Data))
		}
		if _, err := stream.CloseAndRecv(); err != nil {
			return err
		}
		return nil
	}
	err := c.Retrier.Do(ctx, closure)
	return totalBytes, err
}

// ReadBytes fetches a resource's contents into a byte slice.
//
// ReadBytes panics with ErrTooLarge if an attempt is made to read a resource with contents too
// large to fit into a byte array.
func (c *Client) ReadBytes(ctx context.Context, name string) ([]byte, error) {
	buf := &bytes.Buffer{}
	_, err := c.readStreamedRetried(ctx, name, 0, 0, buf)
	return buf.Bytes(), err
}

// ReadResourceToFile fetches a resource's contents, saving it into a file.
//
// The provided resource name must be a child resource of this client's instance,
// e.g. '/blobs/abc-123/45' (NOT 'projects/foo/bar/baz').
//
// The number of bytes read is returned.
func (c *Client) ReadResourceToFile(ctx context.Context, name, fpath string) (int64, error) {
	return c.readToFile(ctx, c.InstanceName+name, fpath)
}

func (c *Client) readToFile(ctx context.Context, name string, fpath string) (int64, error) {
	f, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, c.RegularMode)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	return c.readStreamedRetried(ctx, name, 0, 0, f)
}

// readStreamed reads from a bytestream and copies the result to the provided Writer, starting
// offset bytes into the stream and reading at most limit bytes (or no limit if limit==0). The
// offset must be non-negative, and an error may be returned if the offset is past the end of the
// stream. The limit must be non-negative, although offset+limit may exceed the length of the
// stream.
func (c *Client) readStreamed(ctx context.Context, name string, offset, limit int64, w io.Writer) (int64, error) {
	stream, err := c.Read(ctx, &bspb.ReadRequest{
		ResourceName: name,
		ReadOffset:   offset,
		ReadLimit:    limit,
	})
	if err != nil {
		return 0, err
	}

	var n int64
	for {
		var resp *bspb.ReadResponse
		err := c.CallWithTimeout(ctx, "Read", func(_ context.Context) error {
			r, err := stream.Recv()
			resp = r
			return err
		})
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}
		log.V(3).Infof("Read: resource:%s offset:%d len(data):%d", name, offset, len(resp.Data))
		nm, err := w.Write(resp.Data)
		if err != nil {
			// Wrapping the error to ensure it may never get retried.
			return int64(nm), fmt.Errorf("failed to write to output stream: %v", err)
		}
		sz := len(resp.Data)
		if nm != sz {
			return int64(nm), fmt.Errorf("received %d bytes but could only write %d", sz, nm)
		}
		n += int64(sz)
		if limit > 0 {
			limit -= int64(sz)
			if limit <= 0 {
				break
			}
		}
	}
	return n, nil
}

func (c *Client) readStreamedRetried(ctx context.Context, name string, offset, limit int64, w io.Writer) (int64, error) {
	var n int64
	closure := func() error {
		m, err := c.readStreamed(ctx, name, offset+n, limit, w)
		n += m
		return err
	}
	return n, c.Retrier.Do(ctx, closure)
}
