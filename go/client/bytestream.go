package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"

	log "github.com/golang/glog"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

// WriteBytes uploads a byte slice.
func (c *Client) WriteBytes(ctx context.Context, name string, data []byte) error {
	cancelCtx, cancel := context.WithCancel(ctx)
	opts := c.rpcOpts()
	defer cancel()
	closure := func() error {
		// Use lower-level Write in order to not retry twice.
		stream, err := c.byteStream.Write(cancelCtx, opts...)
		if err != nil {
			return err
		}
		var offset int64
		arr := data // Save a local copy that gets altered in the loop.
		first := true
		for len(arr) > 0 || first { // Iterate at least once, so we can upload 0-sized data.
			first = false
			req := &bspb.WriteRequest{ResourceName: name}
			if offset > 0 {
				req.ResourceName = ""
			}
			req.WriteOffset = offset
			chunkSize := int64(c.chunkMaxSize)
			dataLen := int64(len(arr))
			if chunkSize > dataLen {
				chunkSize = dataLen
			}
			req.Data = arr[:chunkSize]
			arr = arr[chunkSize:]
			if len(arr) == 0 {
				req.FinishWrite = true
			}
			log.V(3).Infof("Sending: resource:%s offset:%d len(data):%d", req.ResourceName, req.WriteOffset, len(req.Data))
			err := stream.Send(req)
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Error("after regular stream send: ", err)
				return err
			}
			offset += chunkSize
		}
		if _, err := stream.CloseAndRecv(); err != nil {
			return err
		}
		return nil
	}
	return c.retrier.do(cancelCtx, closure)
}

// ReadBytes fetches a resource's contents into a byte slice.
//
// ReadBytes panics with ErrTooLarge if an attempt is made to read a resource with contents too
// large to fit into a byte array.
func (c *Client) ReadBytes(ctx context.Context, name string) ([]byte, error) {
	buf := &bytes.Buffer{}
	_, err := c.readStreamed(ctx, name, 0, 0, buf)
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
	f, err := os.Create(fpath)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	return c.readStreamed(ctx, name, 0, 0, f)
}

// readStreamed reads from a bytestream and copies the result to the provided Writer, starting
// offset bytes into the stream and reading at most limit bytes (or no limit if limit==0). The
// offset must be non-negative, and an error may be returned if the offset is past the end of the
// stream. The limit must be non-negative, although offset+limit may exceed the length of the
// stream.
func (c *Client) readStreamed(ctx context.Context, name string, offset, limit int64, w io.Writer) (n int64, e error) {
	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	closure := func() error {
		// Use lower-level Read in order to not retry twice.
		stream, err := c.byteStream.Read(cancelCtx, &bspb.ReadRequest{
			ResourceName: name,
			ReadOffset:   offset + n,
			ReadLimit:    limit,
		})
		if err != nil {
			return err
		}

		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			log.V(3).Infof("Read: resource:%s offset:%d len(data):%d", name, offset+n, len(resp.Data))
			nm, err := w.Write(resp.Data)
			if err != nil {
				// Wrapping the error to ensure it may never get retried.
				return fmt.Errorf("failed to write to output stream: %v", err)
			}
			sz := len(resp.Data)
			if nm != sz {
				return fmt.Errorf("received %d bytes but could only write %d", sz, nm)
			}
			n += int64(sz)
			if limit > 0 {
				limit -= int64(sz)
				if limit <= 0 {
					break
				}
			}
		}
		return nil
	}
	e = c.retrier.do(cancelCtx, closure)
	return n, e
}
