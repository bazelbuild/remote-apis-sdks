package fakes

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/chunker"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/client"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/uploadinfo"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	// Redundant imports are required for the google3 mirror. Aliases should not be changed.
	regrpc "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	bsgrpc "google.golang.org/genproto/googleapis/bytestream"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

// MultiCAS is a fake CAS that implements FindMissingBlobs, Read and Write, storing stored blobs
// in a map. It also counts the number of requests to store received, for validating batching logic.
// This version allows a configurable instance name.
type MultiCAS struct {
	// InstanceName is the expected instance name for all requests.
	InstanceName string
	// Maximum batch byte size to verify requests against.
	BatchSize         int
	ReqSleepDuration  time.Duration
	ReqSleepRandomize bool
	PerDigestBlockFn  map[digest.Digest]func()
	blobs             map[digest.Digest][]byte
	reads             map[digest.Digest]int
	writes            map[digest.Digest]int
	missingReqs       map[digest.Digest]int
	mu                sync.RWMutex
	batchReqs         int
	writeReqs         int
	concReqs          int
	maxConcReqs       int
}

// NewMultiCAS returns a new empty fake CAS with a specific instance name.
func NewMultiCAS(instanceName string) *MultiCAS {
	c := &MultiCAS{
		InstanceName:     instanceName,
		BatchSize:        client.DefaultMaxBatchSize,
		PerDigestBlockFn: make(map[digest.Digest]func()),
	}

	c.Clear()
	return c
}

// Clear removes all results from the cache.
func (f *MultiCAS) Clear() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.blobs = map[digest.Digest][]byte{
		// For https://github.com/bazelbuild/remote-apis/blob/6345202a036a297b22b0a0e7531ef702d05f2130/build/bazel/remote/execution/v2/remote_execution.proto#L249
		digest.Empty: {},
	}
	f.reads = make(map[digest.Digest]int)
	f.writes = make(map[digest.Digest]int)
	f.missingReqs = make(map[digest.Digest]int)
	f.batchReqs = 0
	f.writeReqs = 0
	f.concReqs = 0
	f.maxConcReqs = 0
}

// Put adds a given blob to the cache and returns its digest.
func (f *MultiCAS) Put(blob []byte) digest.Digest {
	f.mu.Lock()
	defer f.mu.Unlock()
	d := digest.NewFromBlob(blob)
	f.blobs[d] = blob
	return d
}

// Get returns the bytes corresponding to the given digest, and whether it was found.
func (f *MultiCAS) Get(d digest.Digest) ([]byte, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	res, ok := f.blobs[d]
	return res, ok
}

// BlobReads returns the total number of read requests for a particular digest.
func (f *MultiCAS) BlobReads(d digest.Digest) int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.reads[d]
}

// BlobWrites returns the total number of update requests for a particular digest.
func (f *MultiCAS) BlobWrites(d digest.Digest) int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.writes[d]
}

// BlobMissingReqs returns the total number of GetMissingBlobs requests for a particular digest.
func (f *MultiCAS) BlobMissingReqs(d digest.Digest) int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.missingReqs[d]
}

// BatchReqs returns the total number of BatchUpdateBlobs requests to this fake.
func (f *MultiCAS) BatchReqs() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.batchReqs
}

// WriteReqs returns the total number of Write requests to this fake.
func (f *MultiCAS) WriteReqs() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.writeReqs
}

// MaxConcurrency returns the maximum number of concurrent Write/Batch requests to this fake.
func (f *MultiCAS) MaxConcurrency() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.maxConcReqs
}

// FindMissingBlobs implements the corresponding RE API function.
func (f *MultiCAS) FindMissingBlobs(ctx context.Context, req *repb.FindMissingBlobsRequest) (*repb.FindMissingBlobsResponse, error) {
	f.maybeSleep()
	f.mu.Lock()
	defer f.mu.Unlock()

	if req.InstanceName != f.InstanceName {
		return nil, status.Errorf(codes.InvalidArgument, "test fake expected instance name %q, got %q", f.InstanceName, req.InstanceName)
	}
	resp := new(repb.FindMissingBlobsResponse)
	for _, dg := range req.BlobDigests {
		d := digest.NewFromProtoUnvalidated(dg)
		f.missingReqs[d]++
		if _, ok := f.blobs[d]; !ok {
			resp.MissingBlobDigests = append(resp.MissingBlobDigests, dg)
		}
	}
	return resp, nil
}

func (f *MultiCAS) maybeBlock(dg digest.Digest) {
	if fn, ok := f.PerDigestBlockFn[dg]; ok {
		fn()
	}
}

func (f *MultiCAS) maybeSleep() {
	if f.ReqSleepDuration != 0 {
		d := f.ReqSleepDuration
		if f.ReqSleepRandomize {
			d = time.Duration(rand.Float32()*float32(d.Microseconds())) * time.Microsecond
		}
		time.Sleep(d)
	}
}

// BatchUpdateBlobs implements the corresponding RE API function.
func (f *MultiCAS) BatchUpdateBlobs(ctx context.Context, req *repb.BatchUpdateBlobsRequest) (*repb.BatchUpdateBlobsResponse, error) {
	f.maybeSleep()
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

	if req.InstanceName != f.InstanceName {
		return nil, status.Errorf(codes.InvalidArgument, "test fake expected instance name %q, got %q", f.InstanceName, req.InstanceName)
	}

	reqBlob, _ := proto.Marshal(req)
	size := len(reqBlob)
	if size > f.BatchSize {
		return nil, status.Errorf(codes.InvalidArgument, "test fake received batch update for more than the maximum of %d bytes: %d bytes", f.BatchSize, size)
	}

	var resps []*repb.BatchUpdateBlobsResponse_Response
	for _, r := range req.Requests {
		if r.Compressor == repb.Compressor_ZSTD {
			d, err := zstdDecoder.DecodeAll(r.Data, nil)
			if err != nil {
				resps = append(resps, &repb.BatchUpdateBlobsResponse_Response{
					Digest: r.Digest,
					Status: status.Newf(codes.InvalidArgument, "invalid blob: could not decompress: %s", err).Proto(),
				})
				continue
			}
			r.Data = d
		}

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
func (f *MultiCAS) BatchReadBlobs(ctx context.Context, req *repb.BatchReadBlobsRequest) (*repb.BatchReadBlobsResponse, error) {
	f.maybeSleep()
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

	if req.InstanceName != f.InstanceName {
		return nil, status.Errorf(codes.InvalidArgument, "test fake expected instance name %q, got %q", f.InstanceName, req.InstanceName)
	}

	reqBlob, _ := proto.Marshal(req)
	size := len(reqBlob)
	if size > f.BatchSize {
		return nil, status.Errorf(codes.InvalidArgument, "test fake received batch read for more than the maximum of %d bytes: %d bytes", f.BatchSize, size)
	}

	var resps []*repb.BatchReadBlobsResponse_Response
	for _, dgPb := range req.Digests {
		dg := digest.NewFromProtoUnvalidated(dgPb)
		f.mu.Lock()
		data, ok := f.blobs[dg]
		f.mu.Unlock()
		if !ok {
			resps = append(resps, &repb.BatchReadBlobsResponse_Response{
				Digest: dgPb,
				Status: status.Newf(codes.NotFound, "digest %s was not found in the fake CAS", dg).Proto(),
			})
			continue
		}
		f.mu.Lock()
		f.reads[dg]++
		f.mu.Unlock()

		useZSTDCompression := false
		compressor := repb.Compressor_IDENTITY
		for _, c := range req.AcceptableCompressors {
			if c == repb.Compressor_ZSTD {
				compressor = repb.Compressor_ZSTD
				useZSTDCompression = true
				break
			}
		}
		if useZSTDCompression {
			data = zstdEncoder.EncodeAll(data, nil)
		}
		resps = append(resps, &repb.BatchReadBlobsResponse_Response{
			Digest:     dgPb,
			Status:     status.New(codes.OK, "").Proto(),
			Data:       data,
			Compressor: compressor,
		})
	}
	return &repb.BatchReadBlobsResponse{Responses: resps}, nil
}

// GetTree implements the corresponding RE API function.
func (f *MultiCAS) GetTree(req *repb.GetTreeRequest, stream regrpc.ContentAddressableStorage_GetTreeServer) error {
	f.maybeSleep()
	if req.InstanceName != f.InstanceName {
		return status.Errorf(codes.InvalidArgument, "test fake expected instance name %q, got %q", f.InstanceName, req.InstanceName)
	}
	rootDigest, err := digest.NewFromProto(req.RootDigest)
	if err != nil {
		return fmt.Errorf("unable to parsse root digest %v", req.RootDigest)
	}
	blob, ok := f.Get(rootDigest)
	if !ok {
		return fmt.Errorf("root digest %v not found", rootDigest)
	}
	rootDir := &repb.Directory{}
	proto.Unmarshal(blob, rootDir)

	var res []*repb.Directory
	queue := []*repb.Directory{rootDir}
	for len(queue) > 0 {
		ele := queue[0]
		res = append(res, ele)
		queue = queue[1:]

		for _, dir := range ele.GetDirectories() {
			fd, err := digest.NewFromProto(dir.GetDigest())
			if err != nil {
				return fmt.Errorf("unable to parse directory digest %v", dir.GetDigest())
			}
			blob, ok := f.Get(fd)
			if !ok {
				return fmt.Errorf("directory digest %v not found", fd)
			}
			directory := &repb.Directory{}
			proto.Unmarshal(blob, directory)
			queue = append(queue, directory)
		}
	}

	resp := &repb.GetTreeResponse{
		Directories: res,
	}
	return stream.Send(resp)
}

// Write implements the corresponding RE API function.
func (f *MultiCAS) Write(stream bsgrpc.ByteStream_WriteServer) (err error) {
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
	if (len(path) != 6 && len(path) != 7) || path[0] != f.InstanceName || path[1] != "uploads" || (path[3] != "blobs" && path[3] != "compressed-blobs") {
		return status.Errorf(codes.InvalidArgument, "test fake expected resource name of the form \"%s/uploads//blobs|compressed-blobs///\"", f.InstanceName)
	}
	// indexOffset for all 4+ paths - `compressed-blobs` paths have one more element.
	indexOffset := 0
	if path[3] == "compressed-blobs" {
		indexOffset = 1
		if path[4] != "zstd" {
			return status.Error(codes.InvalidArgument, "test fake expected valid compressor, eg zstd")
		}
	}
	size, err := strconv.ParseInt(path[5+indexOffset], 10, 64)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "test fake expected resource name of the form \"%s/uploads//blobs|compressed-blobs///\"", f.InstanceName)
	}
	dg, err := digest.New(path[4+indexOffset], size)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "test fake expected a valid digest as part of the resource name: \"%s/uploads//blobs|compressed-blobs///\"", f.InstanceName)
	}
	if _, err := uuid.Parse(path[2]); err != nil {
		return status.Errorf(codes.InvalidArgument, "test fake expected resource name of the form \"%s/uploads//blobs|compressed-blobs///\"", f.InstanceName)
	}

	f.maybeSleep()
	f.maybeBlock(dg)
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

	uncompressedBuf := buf.Bytes()
	if path[3] == "compressed-blobs" {
		if path[4] != "zstd" {
			return status.Errorf(codes.InvalidArgument, "%s compressor isn't supported", path[4])
		}
		var err error
		uncompressedBuf, err = zstdDecoder.DecodeAll(buf.Bytes(), nil)
		if err != nil {
			return status.Errorf(codes.InvalidArgument, "served bytes can't be decompressed: %v", err)
		}
	}

	f.mu.Lock()
	f.blobs[dg] = uncompressedBuf
	f.writes[dg]++
	f.mu.Unlock()
	cDg := digest.NewFromBlob(uncompressedBuf)
	if dg != cDg {
		return status.Errorf(codes.InvalidArgument, "mismatched digest: received %s, computed %s", dg, cDg)
	}
	return stream.SendAndClose(&bspb.WriteResponse{CommittedSize: dg.Size})
}

// Read implements the corresponding RE API function.
func (f *MultiCAS) Read(req *bspb.ReadRequest, stream bsgrpc.ByteStream_ReadServer) error {
	if req.ReadOffset < 0 {
		return status.Error(codes.InvalidArgument, "test fake expected a positive value for offset")
	}
	if req.ReadLimit != 0 {
		return status.Error(codes.Unimplemented, "test fake does not implement limit")
	}

	path := strings.Split(req.ResourceName, "/")
	if (len(path) != 4 && len(path) != 5) || path[0] != f.InstanceName || (path[1] != "blobs" && path[1] != "compressed-blobs") {
		return status.Errorf(codes.InvalidArgument, "test fake expected resource name of the form \"%s/blobs|compressed-blobs///\"", f.InstanceName)
	}
	// indexOffset for all 2+ paths - `compressed-blobs` has one more URI element.
	indexOffset := 0
	if path[1] == "compressed-blobs" {
		indexOffset = 1
	}

	size, err := strconv.Atoi(path[3+indexOffset])
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "test fake expected resource name of the form \"%s/blobs|compressed-blobs///\"", f.InstanceName)
	}
	dg := digest.TestNew(path[2+indexOffset], int64(size))
	f.maybeSleep()
	f.maybeBlock(dg)
	f.mu.Lock()
	blob, ok := f.blobs[dg]
	f.reads[dg]++
	f.mu.Unlock()
	if !ok {
		return status.Errorf(codes.NotFound, "test fake missing blob with digest %s was requested", dg)
	}

	if path[1] == "compressed-blobs" {
		if path[2] != "zstd" {
			return status.Error(codes.InvalidArgument, "test fake expected valid compressor, eg zstd")
		}
		blob = zstdEncoder.EncodeAll(blob, nil)
	}
	ue := uploadinfo.EntryFromBlob(blob)
	ch, err := chunker.New(ue, false, 2*1024*1024)
	if err != nil {
		return status.Errorf(codes.Internal, "test fake failed to create chunker: %v", err)
	}

	resp := &bspb.ReadResponse{}
	var offset int64
	for ch.HasNext() {
		chunk, err := ch.Next()
		if err != nil {
			return err
		}
		// Seek to req.ReadOffset.
		offset += int64(len(chunk.Data))
		if offset < req.ReadOffset {
			continue
		}
		// Scale the offset to the chunk.
		offset = offset - req.ReadOffset         // The chunk tail that we want.
		offset = int64(len(chunk.Data)) - offset // The chunk head that we don't want.
		if offset < 0 {
			// The chunk is past the offset.
			offset = 0
		}
		resp.Data = chunk.Data[int(offset):]
		err = stream.Send(resp)
		if err != nil {
			return err
		}
	}
	return nil
}

// QueryWriteStatus implements the corresponding RE API function.
func (f *MultiCAS) QueryWriteStatus(context.Context, *bspb.QueryWriteStatusRequest) (*bspb.QueryWriteStatusResponse, error) {
	return nil, status.Error(codes.Unimplemented, "test fake does not implement method")
}
