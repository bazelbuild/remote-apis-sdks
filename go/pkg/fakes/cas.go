package fakes

import (
	"bytes"
	"context"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/client"
	"github.com/bazelbuild/remote-apis-sdks/go/digest"
	"github.com/pborman/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	regrpc "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	bsgrpc "google.golang.org/genproto/googleapis/bytestream"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

// Reader implements ByteStream's Read interface, returning one blob.
type Reader struct {
	// Blob is the blob being read.
	Blob []byte
	// Chunks is a list of chunk sizes, in the order they are produced. The sum must be equal to the
	// length of blob.
	Chunks []int
}

// Validate ensures that a Reader has the chunk sizes set correctly.
func (f *Reader) Validate(t *testing.T) {
	t.Helper()
	sum := 0
	for _, c := range f.Chunks {
		if c < 0 {
			t.Errorf("Invalid chunk specification: chunk with negative size %d", c)
		}
		sum += c
	}
	if sum != len(f.Blob) {
		t.Errorf("Invalid chunk specification: chunk sizes sum to %d but blob is length %d", sum, len(f.Blob))
	}
}

// Read implements the corresponding RE API function.
func (f *Reader) Read(req *bspb.ReadRequest, stream bsgrpc.ByteStream_ReadServer) error {
	path := strings.Split(req.ResourceName, "/")
	if len(path) != 4 || path[0] != "instance" || path[1] != "blobs" {
		return status.Error(codes.InvalidArgument, "test fake expected resource name of the form \"instance/blobs/<hash>/<size>\"")
	}
	dg := digest.NewFromBlob(f.Blob)
	if path[2] != dg.Hash || path[3] != strconv.FormatInt(dg.Size, 10) {
		return status.Errorf(codes.NotFound, "test fake only has blob with digest %s, but %s/%s was requested", dg, path[2], path[3])
	}

	offset := req.ReadOffset
	limit := req.ReadLimit
	blob := f.Blob
	chunks := f.Chunks
	for len(chunks) > 0 {
		buf := blob[:chunks[0]]
		if offset >= int64(len(buf)) {
			offset -= int64(len(buf))
		} else {
			if offset > 0 {
				buf = buf[offset:]
				offset = 0
			}
			if limit > 0 {
				if limit < int64(len(buf)) {
					buf = buf[:limit]
				}
				limit -= int64(len(buf))
			}
			if err := stream.Send(&bspb.ReadResponse{Data: buf}); err != nil {
				return err
			}
			if limit == 0 && req.ReadLimit != 0 {
				break
			}
		}
		blob = blob[chunks[0]:]
		chunks = chunks[1:]
	}
	return nil
}

// Write implements the corresponding RE API function.
func (f *Reader) Write(bsgrpc.ByteStream_WriteServer) error {
	return status.Error(codes.Unimplemented, "test fake does not implement method")
}

// QueryWriteStatus implements the corresponding RE API function.
func (f *Reader) QueryWriteStatus(context.Context, *bspb.QueryWriteStatusRequest) (*bspb.QueryWriteStatusResponse, error) {
	return nil, status.Error(codes.Unimplemented, "test fake does not implement method")
}

// Writer expects to receive Write calls and fills the buffer.
type Writer struct {
	// Buf is a buffer that is set to the contents of a Write call after one is received.
	Buf []byte
	// Err is a copy of the error returned by Write.
	Err error
}

// Write implements the corresponding RE API function.
func (f *Writer) Write(stream bsgrpc.ByteStream_WriteServer) (err error) {
	// Store the error so we can verify that the client didn't drop the stream early, meaning the
	// request won't error.
	defer func() { f.Err = err }()

	off := int64(0)
	buf := new(bytes.Buffer)

	req, err := stream.Recv()
	if err == io.EOF {
		return status.Error(codes.InvalidArgument, "no write request received")
	}
	if err != nil {
		return err
	}

	path := strings.Split(req.ResourceName, "/")
	if len(path) != 6 || path[0] != "instance" || path[1] != "uploads" || path[3] != "blobs" {
		return status.Error(codes.InvalidArgument, "test fake expected resource name of the form \"instance/uploads/<uuid>/blobs/<hash>/<size>\"")
	}
	size, err := strconv.ParseInt(path[5], 10, 64)
	if err != nil {
		return status.Error(codes.InvalidArgument, "test fake expected resource name of the form \"instance/uploads/<uuid>/blobs/<hash>/<size>\"")
	}
	dg, e := digest.New(path[4], size)
	if e != nil {
		return status.Error(codes.InvalidArgument, "test fake expected valid digest as part of resource name of the form \"instance/uploads/<uuid>/blobs/<hash>/<size>\"")
	}
	if uuid.Parse(path[2]) == nil {
		return status.Error(codes.InvalidArgument, "test fake expected resource name of the form \"instance/uploads/<uuid>/blobs/<hash>/<size>\"")
	}

	res := req.ResourceName
	done := false
	for {
		if req.ResourceName != res && req.ResourceName != "" {
			return status.Errorf(codes.InvalidArgument, "follow-up request had resource name %q different from original %q", req.ResourceName, res)
		}
		if req.WriteOffset != off {
			return status.Errorf(codes.InvalidArgument, "request had incorrect offset %d, expected %d", req.WriteOffset, off)
		}
		if done {
			return status.Errorf(codes.InvalidArgument, "received write request after the client finished writing")
		}
		// 2 MB is the protocol max.
		if len(req.Data) > 2*1024*1024 {
			return status.Errorf(codes.InvalidArgument, "data chunk greater than 2MB")
		}

		// bytes.Buffer.Write can't error
		_, _ = buf.Write(req.Data)
		off += int64(len(req.Data))
		if req.FinishWrite {
			done = true
		}

		req, err = stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}

	if !done {
		return status.Errorf(codes.InvalidArgument, "reached end of stream before the client finished writing")
	}

	f.Buf = buf.Bytes()
	cDg := digest.NewFromBlob(f.Buf)
	if dg != cDg {
		return status.Errorf(codes.InvalidArgument, "mismatched digest: received %s, computed %s", dg, cDg)
	}
	return stream.SendAndClose(&bspb.WriteResponse{CommittedSize: dg.Size})
}

// Read implements the corresponding RE API function.
func (f *Writer) Read(*bspb.ReadRequest, bsgrpc.ByteStream_ReadServer) error {
	return status.Error(codes.Unimplemented, "test fake does not implement method")
}

// QueryWriteStatus implements the corresponding RE API function.
func (f *Writer) QueryWriteStatus(context.Context, *bspb.QueryWriteStatusRequest) (*bspb.QueryWriteStatusResponse, error) {
	return nil, status.Error(codes.Unimplemented, "test fake does not implement method")
}

// CAS is a fake CAS that implements FindMissingBlobs, Read and Write, storing stored blobs
// in a map. It also counts the number of requests to store received, for validating batching logic.
type CAS struct {
	blobs       map[digest.Digest][]byte
	reads       map[digest.Digest]int
	writes      map[digest.Digest]int
	mu          sync.RWMutex
	batchReqs   int
	writeReqs   int
	concReqs    int
	maxConcReqs int
}

// NewCAS returns a new empty fake CAS.
func NewCAS() *CAS {
	c := &CAS{}
	c.Clear()
	return c
}

// Clear removes all results from the cache.
func (f *CAS) Clear() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.blobs = make(map[digest.Digest][]byte)
	f.reads = make(map[digest.Digest]int)
	f.writes = make(map[digest.Digest]int)
	f.batchReqs = 0
	f.writeReqs = 0
	f.concReqs = 0
	f.maxConcReqs = 0
}

// Put adds a given blob to the cache and returns its digest.
func (f *CAS) Put(blob []byte) digest.Digest {
	f.mu.Lock()
	defer f.mu.Unlock()
	d := digest.NewFromBlob(blob)
	f.blobs[d] = blob
	return d
}

// Get returns the bytes corresponding to the given digest, and whether it was found.
func (f *CAS) Get(d digest.Digest) ([]byte, bool) {
	res, ok := f.blobs[d]
	return res, ok
}

// BlobReads returns the total number of read requests for a particular digest.
func (f *CAS) BlobReads(d digest.Digest) int {
	return f.reads[d]
}

// BlobWrites returns the total number of update requests for a particular digest.
func (f *CAS) BlobWrites(d digest.Digest) int {
	return f.writes[d]
}

// BatchReqs returns the total number of BatchUpdateBlobs requests to this fake.
func (f *CAS) BatchReqs() int {
	return f.batchReqs
}

// WriteReqs returns the total number of Write requests to this fake.
func (f *CAS) WriteReqs() int {
	return f.writeReqs
}

// MaxConcurrency returns the maximum number of concurrent Write/Batch requests to this fake.
func (f *CAS) MaxConcurrency() int {
	return f.maxConcReqs
}

// FindMissingBlobs implements the corresponding RE API function.
func (f *CAS) FindMissingBlobs(ctx context.Context, req *repb.FindMissingBlobsRequest) (*repb.FindMissingBlobsResponse, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if req.InstanceName != "instance" {
		return nil, status.Error(codes.InvalidArgument, "test fake expected instance name \"instance\"")
	}
	resp := new(repb.FindMissingBlobsResponse)
	for _, dg := range req.BlobDigests {
		if _, ok := f.blobs[digest.NewFromProtoUnvalidated(dg)]; !ok {
			resp.MissingBlobDigests = append(resp.MissingBlobDigests, dg)
		}
	}
	return resp, nil
}

// BatchUpdateBlobs implements the corresponding RE API function.
func (f *CAS) BatchUpdateBlobs(ctx context.Context, req *repb.BatchUpdateBlobsRequest) (*repb.BatchUpdateBlobsResponse, error) {
	f.mu.Lock()
	f.batchReqs++
	f.concReqs++
	defer func() {
		f.mu.Lock()
		f.concReqs--
		f.mu.Unlock()
	}()
	if f.concReqs > f.maxConcReqs {
		f.maxConcReqs = f.concReqs
	}
	f.mu.Unlock()

	if req.InstanceName != "instance" {
		return nil, status.Error(codes.InvalidArgument, "test fake expected instance name \"instance\"")
	}

	var tot int64
	for _, r := range req.Requests {
		tot += r.Digest.SizeBytes
	}
	if tot > client.MaxBatchSz {
		return nil, status.Errorf(codes.InvalidArgument, "test fake received batch update for more than the maximum of %d bytes: %d bytes", client.MaxBatchSz, tot)
	}

	var resps []*repb.BatchUpdateBlobsResponse_Response
	for _, r := range req.Requests {
		dg := digest.NewFromBlob(r.Data)
		rdg := digest.NewFromProtoUnvalidated(r.Digest)
		if dg != rdg {
			resps = append(resps, &repb.BatchUpdateBlobsResponse_Response{
				Digest: r.Digest,
				Status: status.Newf(codes.InvalidArgument, "Digest mismatch: digest of data was %s but digest of content was %s",
					dg, rdg).Proto(),
			})
			continue
		}
		f.mu.Lock()
		f.blobs[dg] = r.Data
		f.writes[dg]++
		f.mu.Unlock()
		resps = append(resps, &repb.BatchUpdateBlobsResponse_Response{
			Digest: r.Digest,
			Status: status.New(codes.OK, "").Proto(),
		})
	}
	return &repb.BatchUpdateBlobsResponse{Responses: resps}, nil
}

// BatchReadBlobs implements the corresponding RE API function.
func (f *CAS) BatchReadBlobs(ctx context.Context, req *repb.BatchReadBlobsRequest) (*repb.BatchReadBlobsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "test fake does not implement method")
}

// GetTree implements the corresponding RE API function.
func (f *CAS) GetTree(*repb.GetTreeRequest, regrpc.ContentAddressableStorage_GetTreeServer) error {
	return status.Error(codes.Unimplemented, "test fake does not implement method")
}

// Write implements the corresponding RE API function.
func (f *CAS) Write(stream bsgrpc.ByteStream_WriteServer) (err error) {
	f.mu.Lock()
	f.writeReqs++
	f.concReqs++
	defer func() {
		f.mu.Lock()
		f.concReqs--
		f.mu.Unlock()
	}()
	if f.concReqs > f.maxConcReqs {
		f.maxConcReqs = f.concReqs
	}
	f.mu.Unlock()

	off := int64(0)
	buf := new(bytes.Buffer)

	req, err := stream.Recv()
	if err == io.EOF {
		return status.Error(codes.InvalidArgument, "no write request received")
	}
	if err != nil {
		return err
	}

	path := strings.Split(req.ResourceName, "/")
	if len(path) != 6 || path[0] != "instance" || path[1] != "uploads" || path[3] != "blobs" {
		return status.Error(codes.InvalidArgument, "test fake expected resource name of the form \"instance/uploads/<uuid>/blobs/<hash>/<size>\"")
	}
	size, err := strconv.ParseInt(path[5], 10, 64)
	if err != nil {
		return status.Error(codes.InvalidArgument, "test fake expected resource name of the form \"instance/uploads/<uuid>/blobs/<hash>/<size>\"")
	}
	dg, err := digest.New(path[4], size)
	if err != nil {
		return status.Error(codes.InvalidArgument, "test fake expected a valid digest as part of the resource name: \"instance/uploads/<uuid>/blobs/<hash>/<size>\"")
	}
	if uuid.Parse(path[2]) == nil {
		return status.Error(codes.InvalidArgument, "test fake expected resource name of the form \"instance/uploads/<uuid>/blobs/<hash>/<size>\"")
	}

	res := req.ResourceName
	done := false
	for {
		if req.ResourceName != res && req.ResourceName != "" {
			return status.Errorf(codes.InvalidArgument, "follow-up request had resource name %q different from original %q", req.ResourceName, res)
		}
		if req.WriteOffset != off {
			return status.Errorf(codes.InvalidArgument, "request had incorrect offset %d, expected %d", req.WriteOffset, off)
		}
		if done {
			return status.Errorf(codes.InvalidArgument, "received write request after the client finished writing")
		}
		// 2 MB is the protocol max.
		if len(req.Data) > 2*1024*1024 {
			return status.Errorf(codes.InvalidArgument, "data chunk greater than 2MB")
		}

		// bytes.Buffer.Write can't error
		_, _ = buf.Write(req.Data)
		off += int64(len(req.Data))
		if req.FinishWrite {
			done = true
		}

		req, err = stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}

	if !done {
		return status.Errorf(codes.InvalidArgument, "reached end of stream before the client finished writing")
	}

	f.mu.Lock()
	f.blobs[dg] = buf.Bytes()
	f.writes[dg]++
	f.mu.Unlock()
	cDg := digest.NewFromBlob(buf.Bytes())
	if dg != cDg {
		return status.Errorf(codes.InvalidArgument, "mismatched digest: received %s, computed %s", dg, cDg)
	}
	return stream.SendAndClose(&bspb.WriteResponse{CommittedSize: dg.Size})
}

// Read implements the corresponding RE API function.
func (f *CAS) Read(req *bspb.ReadRequest, stream bsgrpc.ByteStream_ReadServer) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if req.ReadOffset != 0 || req.ReadLimit != 0 {
		return status.Error(codes.Unimplemented, "test fake does not implement read_offset or limit")
	}

	path := strings.Split(req.ResourceName, "/")
	if len(path) != 4 || path[0] != "instance" || path[1] != "blobs" {
		return status.Error(codes.InvalidArgument, "test fake expected resource name of the form \"instance/blobs/<hash>/<size>\"")
	}
	size, err := strconv.Atoi(path[3])
	if err != nil {
		return status.Error(codes.InvalidArgument, "test fake expected resource name of the form \"instance/blobs/<hash>/<size>\"")
	}
	dg := digest.TestNew(path[2], int64(size))
	blob, ok := f.blobs[dg]
	f.reads[dg]++
	if !ok {
		return status.Errorf(codes.NotFound, "test fake missing blob with digest %s was requested", dg)
	}

	return stream.Send(&bspb.ReadResponse{Data: blob})
}

// QueryWriteStatus implements the corresponding RE API function.
func (f *CAS) QueryWriteStatus(context.Context, *bspb.QueryWriteStatusRequest) (*bspb.QueryWriteStatusResponse, error) {
	return nil, status.Error(codes.Unimplemented, "test fake does not implement method")
}
