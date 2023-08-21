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

// WriteBytes uploads a byte slice.
func (c *Client) WriteBytes(ctx context.Context, name string, data []byte) error {
	ue := uploadinfo.EntryFromBlob(data)
	ch, err := chunker.New(ue, false, int(c.ChunkMaxSize))
	if err != nil {
		return err
	}
	_, err = c.writeChunked(ctx, name, ch, false, 0)
	return err
}

// WriteBytesAtRemoteOffset uploads a byte slice with a given resource name to the CAS
// at an arbitrary offset but retries still resend from the initial Offset. As of now(2023-02-08),
// ByteStream.WriteRequest.FinishWrite and an arbitrary offset are supported for uploads with LogStream
// resource name. If doNotFinalize is set to true, ByteStream.WriteRequest.FinishWrite will be set to false.
func (c *Client) WriteBytesAtRemoteOffset(ctx context.Context, name string, data []byte, doNotFinalize bool, initialOffset int64) (int64, error) {
	ue := uploadinfo.EntryFromBlob(data)
	ch, err := chunker.New(ue, false, int(c.ChunkMaxSize))
	if err != nil {
		return 0, errors.Wrap(err, "failed to create a chunk")
	}
	writtenBytes, err := c.writeChunked(ctx, name, ch, doNotFinalize, initialOffset)
	if err != nil {
		return 0, err
	}
	return writtenBytes, nil
}

// writeChunked uploads chunked data with a given resource name to the CAS.
func (c *Client) writeChunked(ctx context.Context, name string, ch *chunker.Chunker, doNotFinalize bool, initialOffset int64) (int64, error) {
	var totalBytes int64
	closure := func() error {
		// Retry by starting the stream from the beginning.
		if err := ch.Reset(); err != nil {
			return errors.Wrap(err, "failed to Reset")
		}
		totalBytes = int64(0)
		// TODO(olaola): implement resumable uploads. initialOffset passed in allows to
		// start writing data at an arbitrary offset, but retries still restart from initialOffset.

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
			req.WriteOffset = chunk.Offset + initialOffset
			req.Data = chunk.Data

			if !ch.HasNext() && !doNotFinalize {
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

// ReadResourceTo writes a resource's contents to a Writer.
func (c *Client) ReadResourceTo(ctx context.Context, name string, w io.Writer) (int64, error) {
	return c.readStreamedRetried(ctx, name, 0, 0, w)
}

// ReadResourceToFile fetches a resource's contents, saving it into a file.
//
// The provided resource name must be a child resource of this client's instance,
// e.g. '/blobs/abc-123/45' (NOT 'projects/foo/bar/baz').
//
// The number of bytes read is returned.
func (c *Client) ReadResourceToFile(ctx context.Context, name, fpath string) (int64, error) {
	rname, err := c.ResourceName(name)
	if err != nil {
		return 0, err
	}
	return c.readToFile(ctx, rname, fpath)
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
