package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/chunker"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/filemetadata"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/uploadinfo"
	"github.com/golang/protobuf/proto"
	"github.com/klauspost/compress/zstd"
	"github.com/pborman/uuid"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	log "github.com/golang/glog"
)

// DefaultCompressedBytestreamThreshold is the default threshold, in bytes, for
// transferring blobs compressed on ByteStream.Write RPCs.
const DefaultCompressedBytestreamThreshold = -1

// DefaultMaxHeaderSize is the defaut maximum gRPC header size.
const DefaultMaxHeaderSize = 8 * 1024

// MovedBytesMetadata represents the bytes moved in CAS related requests.
type MovedBytesMetadata struct {
	// Requested is the sum of the sizes in bytes for all the uncompressed
	// blobs needed by the execution. It includes bytes that might have
	// been deduped and thus not passed through the wire.
	Requested int64
	// LogicalMoved is the sum of the sizes in bytes of the uncompressed
	// versions of the blobs passed through the wire. It does not included
	// bytes for blobs that were de-duped.
	LogicalMoved int64
	// RealMoved is the the sum of sizes in bytes for all blobs passed
	// through the wire in the format they were passed through (eg
	// compressed).
	RealMoved int64
	// Cached is amount of logical bytes that we did not have to move
	// through the wire because they were de-duped.
	Cached int64
}

func (mbm *MovedBytesMetadata) addFrom(other *MovedBytesMetadata) *MovedBytesMetadata {
	if other == nil {
		return mbm
	}
	mbm.Requested += other.Requested
	mbm.LogicalMoved += other.LogicalMoved
	mbm.RealMoved += other.RealMoved
	mbm.Cached += other.Cached
	return mbm
}

const logInterval = 25

type requestMetadata struct {
	toolName     string
	actionID     string
	invocationID string
}

type uploadRequest struct {
	ue     *uploadinfo.Entry
	meta   *requestMetadata
	wait   chan<- *uploadResponse
	cancel bool
}

type uploadResponse struct {
	digest     digest.Digest
	bytesMoved int64
	err        error
	missing    bool
}

type uploadState struct {
	ue  *uploadinfo.Entry
	err error

	// mu protects clients anc cancel. The fields need protection since they are updated by upload
	// whenever new clients join, and iterated on by updateAndNotify in the end of each upload.
	// It does NOT protect data or error, because they do not need protection -
	// they are only modified when a state object is created, and by updateAndNotify which is called
	// exactly once for a given state object (this is the whole point of the algorithm).
	mu      sync.Mutex
	clients []chan<- *uploadResponse
	cancel  func()
}

func (c *Client) findBlobState(ctx context.Context, dgs []digest.Digest) (missing []digest.Digest, present []digest.Digest, err error) {
	dgMap := make(map[digest.Digest]bool)
	for _, d := range dgs {
		dgMap[d] = true
	}
	missing, err = c.MissingBlobs(ctx, dgs)
	for _, d := range missing {
		delete(dgMap, d)
	}
	for d := range dgMap {
		present = append(present, d)
	}
	return missing, present, err
}

func (c *Client) uploadProcessor() {
	var buffer []*uploadRequest
	ticker := time.NewTicker(time.Duration(c.UnifiedUploadTickDuration))
	for {
		select {
		case req, ok := <-c.casUploadRequests:
			if !ok {
				// Client is exiting. Notify remaining uploads to prevent deadlocks.
				ticker.Stop()
				if buffer != nil {
					for _, r := range buffer {
						r.wait <- &uploadResponse{err: context.Canceled}
					}
				}
				return
			}
			if !req.cancel {
				buffer = append(buffer, req)
				if len(buffer) >= int(c.UnifiedUploadBufferSize) {
					c.upload(buffer)
					buffer = nil
				}
				continue
			}
			// Cancellation request.
			var newBuffer []*uploadRequest
			for _, r := range buffer {
				if r.ue != req.ue || r.wait != req.wait {
					newBuffer = append(newBuffer, r)
				}
			}
			buffer = newBuffer
			st, ok := c.casUploads[req.ue.Digest]
			if ok {
				st.mu.Lock()
				var remainingClients []chan<- *uploadResponse
				for _, w := range st.clients {
					if w != req.wait {
						remainingClients = append(remainingClients, w)
					}
				}
				st.clients = remainingClients
				if len(st.clients) == 0 {
					log.V(3).Infof("Cancelling Write %v", req.ue.Digest)
					if st.cancel != nil {
						st.cancel()
					}
					delete(c.casUploads, req.ue.Digest)
				}
				st.mu.Unlock()
			}
		case <-ticker.C:
			if buffer != nil {
				c.upload(buffer)
				buffer = nil
			}
		}
	}
}

func updateAndNotify(st *uploadState, bytesMoved int64, err error, missing bool) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.err = err
	for _, cl := range st.clients {
		cl <- &uploadResponse{
			digest:     st.ue.Digest,
			bytesMoved: bytesMoved,
			missing:    missing,
			err:        err,
		}

		// We only report this data to the first client to prevent double accounting.
		bytesMoved = 0
		missing = false
	}
	st.clients = nil
	st.ue = nil
}

func getUnifiedLabel(labels map[string]bool) string {
	var keys []string
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}

// capToLimit ensures total length does not exceed max header size.
func capToLimit(m *requestMetadata, limit int) {
	total := len(m.toolName) + len(m.actionID) + len(m.invocationID)
	excess := total - limit
	if excess <= 0 {
		return
	}
	// We ignore the tool name, because in practice this is a
	// very short constant which makes no sense to truncate.
	diff := len(m.actionID) - len(m.invocationID)
	if diff > 0 {
		if diff > excess {
			m.actionID = m.actionID[:len(m.actionID)-excess]
		} else {
			m.actionID = m.actionID[:len(m.actionID)-diff]
			rem := (excess - diff + 1) / 2
			m.actionID = m.actionID[:len(m.actionID)-rem]
			m.invocationID = m.invocationID[:len(m.invocationID)-rem]
		}
	} else {
		diff = -diff
		if diff > excess {
			m.invocationID = m.invocationID[:len(m.invocationID)-excess]
		} else {
			m.invocationID = m.invocationID[:len(m.invocationID)-diff]
			rem := (excess - diff + 1) / 2
			m.invocationID = m.invocationID[:len(m.invocationID)-rem]
			m.actionID = m.actionID[:len(m.actionID)-rem]
		}
	}
}

func getUnifiedMetadata(metas []*requestMetadata) *requestMetadata {
	toolNames := make(map[string]bool)
	actionIDs := make(map[string]bool)
	invocationIDs := make(map[string]bool)
	for _, m := range metas {
		toolNames[m.toolName] = true
		actionIDs[m.actionID] = true
		invocationIDs[m.invocationID] = true
	}
	m := &requestMetadata{
		toolName:     getUnifiedLabel(toolNames),
		actionID:     getUnifiedLabel(actionIDs),
		invocationID: getUnifiedLabel(invocationIDs),
	}
	// We cap to a bit less than the maximum header size in order to allow
	// for some proto fields serialization overhead.
	capToLimit(m, DefaultMaxHeaderSize-100)
	return m
}

func (c *Client) upload(reqs []*uploadRequest) {
	// Collect new uploads.
	newStates := make(map[digest.Digest]*uploadState)
	var newUploads []digest.Digest
	var metas []*requestMetadata
	log.V(2).Infof("Upload is processing %d requests", len(reqs))
	for _, req := range reqs {
		dg := req.ue.Digest
		st, ok := c.casUploads[dg]
		if ok {
			st.mu.Lock()
			if len(st.clients) > 0 {
				st.clients = append(st.clients, req.wait)
			} else {
				req.wait <- &uploadResponse{err: st.err, missing: false} // Digest is only needed when missing=true
			}
			st.mu.Unlock()
		} else {
			st = &uploadState{
				clients: []chan<- *uploadResponse{req.wait},
				ue:      req.ue,
			}
			c.casUploads[dg] = st
			newUploads = append(newUploads, dg)
			metas = append(metas, req.meta)
			newStates[dg] = st
		}
	}

	unifiedMeta := getUnifiedMetadata(metas)
	var err error
	ctx := context.Background()
	if unifiedMeta.actionID != "" {
		ctx, err = ContextWithMetadata(context.Background(), unifiedMeta.toolName, unifiedMeta.actionID, unifiedMeta.invocationID)
	}
	if err != nil {
		for _, st := range newStates {
			updateAndNotify(st, 0, err, false)
		}
		return
	}
	missing, present, err := c.findBlobState(ctx, newUploads)
	if err != nil {
		for _, st := range newStates {
			updateAndNotify(st, 0, err, false)
		}
		return
	}
	for _, dg := range present {
		updateAndNotify(newStates[dg], 0, nil, false)
	}

	LogContextInfof(ctx, log.Level(2), "%d new items to store", len(missing))
	var batches [][]digest.Digest
	if c.useBatchOps {
		batches = c.makeBatches(ctx, missing, true)
	} else {
		LogContextInfof(ctx, log.Level(2), "Uploading them individually")
		for i := range missing {
			LogContextInfof(ctx, log.Level(3), "Creating single batch of blob %s", missing[i])
			batches = append(batches, missing[i:i+1])
		}
	}

	for i, batch := range batches {
		i, batch := i, batch // https://golang.org/doc/faq#closures_and_goroutines
		go func() {
			if c.casUploaders.Acquire(ctx, 1) == nil {
				defer c.casUploaders.Release(1)
			}
			if i%logInterval == 0 {
				LogContextInfof(ctx, log.Level(2), "%d batches left to store", len(batches)-i)
			}
			if len(batch) > 1 {
				LogContextInfof(ctx, log.Level(3), "Uploading batch of %d blobs", len(batch))
				bchMap := make(map[digest.Digest][]byte)
				totalBytesMap := make(map[digest.Digest]int64)
				for _, dg := range batch {
					st := newStates[dg]
					ch, err := chunker.New(st.ue, false, int(c.ChunkMaxSize))
					if err != nil {
						updateAndNotify(st, 0, err, true)
						continue
					}
					data, err := ch.FullData()
					if err != nil {
						updateAndNotify(st, 0, err, true)
						continue
					}
					bchMap[dg] = data
					totalBytesMap[dg] = int64(len(data))
				}
				err := c.BatchWriteBlobs(ctx, bchMap)
				for dg := range bchMap {
					updateAndNotify(newStates[dg], totalBytesMap[dg], err, true)
				}
			} else {
				LogContextInfof(ctx, log.Level(3), "Uploading single blob with digest %s", batch[0])
				st := newStates[batch[0]]
				st.mu.Lock()
				if len(st.clients) == 0 { // Already cancelled.
					log.V(3).Infof("Blob upload for digest %s was canceled", batch[0])
					st.mu.Unlock()
					return
				}
				cCtx, cancel := context.WithCancel(ctx)
				st.cancel = cancel
				st.mu.Unlock()
				dg := st.ue.Digest
				log.V(3).Infof("Uploading single blob with digest %s", batch[0])
				ch, err := chunker.New(st.ue, c.shouldCompress(dg.Size), int(c.ChunkMaxSize))
				if err != nil {
					updateAndNotify(st, 0, err, true)
				}
				totalBytes, err := c.writeChunked(cCtx, c.writeRscName(dg), ch)
				updateAndNotify(st, totalBytes, err, true)
			}
		}()
	}
}

// This function is only used when UnifiedUploads is false. It will be removed
// once UnifiedUploads=true is stable.
func (c *Client) uploadNonUnified(ctx context.Context, data ...*uploadinfo.Entry) ([]digest.Digest, int64, error) {
	var dgs []digest.Digest
	ueList := make(map[digest.Digest]*uploadinfo.Entry)
	for _, ue := range data {
		dg := ue.Digest
		if _, ok := ueList[dg]; !ok {
			dgs = append(dgs, dg)
			ueList[dg] = ue
		}
	}

	missing, err := c.MissingBlobs(ctx, dgs)
	if err != nil {
		return nil, 0, err
	}
	LogContextInfof(ctx, log.Level(2), "%d items to store", len(missing))
	var batches [][]digest.Digest
	if c.useBatchOps {
		batches = c.makeBatches(ctx, missing, true)
	} else {
		LogContextInfof(ctx, log.Level(2), "Uploading them individually")
		for i := range missing {
			LogContextInfof(ctx, log.Level(3), "Creating single batch of blob %s", missing[i])
			batches = append(batches, missing[i:i+1])
		}
	}

	totalBytesTransferred := int64(0)

	eg, eCtx := errgroup.WithContext(ctx)
	for i, batch := range batches {
		i, batch := i, batch // https://golang.org/doc/faq#closures_and_goroutines
		eg.Go(func() error {
			if err := c.casUploaders.Acquire(eCtx, 1); err != nil {
				return err
			}
			defer c.casUploaders.Release(1)
			if i%logInterval == 0 {
				LogContextInfof(ctx, log.Level(2), "%d batches left to store", len(batches)-i)
			}
			if len(batch) > 1 {
				LogContextInfof(ctx, log.Level(3), "Uploading batch of %d blobs", len(batch))
				bchMap := make(map[digest.Digest][]byte)
				for _, dg := range batch {
					ue := ueList[dg]
					ch, err := chunker.New(ue, false, int(c.ChunkMaxSize))
					if err != nil {
						return err
					}

					data, err := ch.FullData()
					if err != nil {
						return err
					}
					bchMap[dg] = data
					atomic.AddInt64(&totalBytesTransferred, int64(len(data)))
				}
				if err := c.BatchWriteBlobs(eCtx, bchMap); err != nil {
					return err
				}
			} else {
				LogContextInfof(ctx, log.Level(3), "Uploading single blob with digest %s", batch[0])
				ue := ueList[batch[0]]
				dg := ue.Digest
				ch, err := chunker.New(ue, c.shouldCompress(dg.Size), int(c.ChunkMaxSize))
				if err != nil {
					return err
				}
				written, err := c.writeChunked(eCtx, c.writeRscName(dg), ch)
				if err != nil {
					return err
				}
				atomic.AddInt64(&totalBytesTransferred, written)
			}
			if eCtx.Err() != nil {
				return eCtx.Err()
			}
			return nil
		})
	}

	LogContextInfof(ctx, log.Level(2), "Waiting for remaining jobs")
	err = eg.Wait()
	LogContextInfof(ctx, log.Level(2), "Done")
	if err != nil {
		LogContextInfof(ctx, log.Level(2), "Upload error: %v", err)
	}

	return missing, totalBytesTransferred, err
}

func (c *Client) cancelPendingRequests(reqs []*uploadRequest) {
	for _, req := range reqs {
		c.casUploadRequests <- &uploadRequest{
			cancel: true,
			ue:     req.ue,
			wait:   req.wait,
		}
	}
}

// UploadIfMissing stores a number of uploadable items.
// It first queries the CAS to see which items are missing and only uploads those that are.
// Returns a slice of the missing digests and the sum of total bytes moved - may be different
// from logical bytes moved (ie sum of digest sizes) due to compression.
func (c *Client) UploadIfMissing(ctx context.Context, data ...*uploadinfo.Entry) ([]digest.Digest, int64, error) {
	if !c.UnifiedUploads {
		return c.uploadNonUnified(ctx, data...)
	}
	uploads := len(data)
	LogContextInfof(ctx, log.Level(2), "Request to upload %d blobs", uploads)

	if uploads == 0 {
		return nil, 0, nil
	}
	toolName, actionID, invocationID, err := GetContextMetadata(ctx)
	if err != nil {
		return nil, 0, err
	}
	meta := &requestMetadata{
		toolName:     toolName,
		actionID:     actionID,
		invocationID: invocationID,
	}
	wait := make(chan *uploadResponse, uploads)
	var missing []digest.Digest
	var reqs []*uploadRequest
	for _, ue := range data {
		req := &uploadRequest{
			ue:   ue,
			meta: meta,
			wait: wait,
		}
		reqs = append(reqs, req)
		select {
		case <-ctx.Done():
			LogContextInfof(ctx, log.Level(2), "Upload canceled")
			c.cancelPendingRequests(reqs)
			return nil, 0, ctx.Err()
		case c.casUploadRequests <- req:
			continue
		}
	}
	totalBytesMoved := int64(0)
	for uploads > 0 {
		select {
		case <-ctx.Done():
			c.cancelPendingRequests(reqs)
			return nil, 0, ctx.Err()
		case resp := <-wait:
			if resp.err != nil {
				return nil, 0, resp.err
			}
			if resp.missing {
				missing = append(missing, resp.digest)
			}
			totalBytesMoved += resp.bytesMoved
			uploads--
		}
	}
	return missing, totalBytesMoved, nil
}

// WriteBlobs stores a large number of blobs from a digest-to-blob map. It's intended for use on the
// result of PackageTree. Unlike with the single-item functions, it first queries the CAS to
// see which blobs are missing and only uploads those that are.
// TODO(olaola): rethink the API of this layer:
// * Do we want to allow []byte uploads, or require the user to construct Chunkers?
// * How to consistently distinguish in the API between should we use GetMissing or not?
// * Should BatchWrite be a public method at all?
func (c *Client) WriteBlobs(ctx context.Context, blobs map[digest.Digest][]byte) error {
	var uEntries []*uploadinfo.Entry
	for _, blob := range blobs {
		uEntries = append(uEntries, uploadinfo.EntryFromBlob(blob))
	}
	_, _, err := c.UploadIfMissing(ctx, uEntries...)
	return err
}

// WriteProto marshals and writes a proto.
func (c *Client) WriteProto(ctx context.Context, msg proto.Message) (digest.Digest, error) {
	bytes, err := proto.Marshal(msg)
	if err != nil {
		return digest.Empty, err
	}
	return c.WriteBlob(ctx, bytes)
}

// WriteBlob uploads a blob to the CAS.
func (c *Client) WriteBlob(ctx context.Context, blob []byte) (digest.Digest, error) {
	ue := uploadinfo.EntryFromBlob(blob)
	dg := ue.Digest
	ch, err := chunker.New(ue, c.shouldCompress(dg.Size), int(c.ChunkMaxSize))
	if err != nil {
		return dg, err
	}
	_, err = c.writeChunked(ctx, c.writeRscName(dg), ch)
	return dg, err
}

type writeDummyCloser struct {
	io.Writer
}

func (w *writeDummyCloser) Close() error { return nil }

// maybeCompressReadBlob will, depending on the client configuration, set the blobs to be
// read compressed. It returns the appropriate resource name.
func (c *Client) maybeCompressReadBlob(d digest.Digest, w io.Writer) (string, io.WriteCloser, chan error, error) {
	if !c.shouldCompress(d.Size) {
		// If we aren't compressing the data, theere's nothing to wait on.
		dummyDone := make(chan error, 1)
		dummyDone <- nil
		return c.resourceNameRead(d.Hash, d.Size), &writeDummyCloser{w}, dummyDone, nil
	}
	cw, done, err := NewCompressedWriteBuffer(w)
	if err != nil {
		return "", nil, nil, err
	}
	return c.resourceNameCompressedRead(d.Hash, d.Size), cw, done, nil
}

// BatchWriteBlobs uploads a number of blobs to the CAS. They must collectively be below the
// maximum total size for a batch upload, which is about 4 MB (see MaxBatchSize). Digests must be
// computed in advance by the caller. In case multiple errors occur during the blob upload, the
// last error will be returned.
func (c *Client) BatchWriteBlobs(ctx context.Context, blobs map[digest.Digest][]byte) error {
	var reqs []*repb.BatchUpdateBlobsRequest_Request
	var sz int64
	for k, b := range blobs {
		sz += int64(k.Size)
		reqs = append(reqs, &repb.BatchUpdateBlobsRequest_Request{
			Digest: k.ToProto(),
			Data:   b,
		})
	}
	if sz > int64(c.MaxBatchSize) {
		return fmt.Errorf("batch update of %d total bytes exceeds maximum of %d", sz, c.MaxBatchSize)
	}
	if len(blobs) > int(c.MaxBatchDigests) {
		return fmt.Errorf("batch update of %d total blobs exceeds maximum of %d", len(blobs), c.MaxBatchDigests)
	}
	opts := c.RPCOpts()
	closure := func() error {
		var resp *repb.BatchUpdateBlobsResponse
		err := c.CallWithTimeout(ctx, "BatchUpdateBlobs", func(ctx context.Context) (e error) {
			resp, e = c.cas.BatchUpdateBlobs(ctx, &repb.BatchUpdateBlobsRequest{
				InstanceName: c.InstanceName,
				Requests:     reqs,
			}, opts...)
			return e
		})
		if err != nil {
			return err
		}

		numErrs, errDg, errMsg := 0, new(repb.Digest), ""
		var failedReqs []*repb.BatchUpdateBlobsRequest_Request
		var retriableError error
		allRetriable := true
		for _, r := range resp.Responses {
			st := status.FromProto(r.Status)
			if st.Code() != codes.OK {
				e := st.Err()
				if c.Retrier.ShouldRetry(e) {
					failedReqs = append(failedReqs, &repb.BatchUpdateBlobsRequest_Request{
						Digest: r.Digest,
						Data:   blobs[digest.NewFromProtoUnvalidated(r.Digest)],
					})
					retriableError = e
				} else {
					allRetriable = false
				}
				numErrs++
				errDg = r.Digest
				errMsg = r.Status.Message
			}
		}
		reqs = failedReqs
		if numErrs > 0 {
			if allRetriable {
				return retriableError // Retriable errors only, retry the failed requests.
			}
			return fmt.Errorf("uploading blobs as part of a batch resulted in %d failures, including blob %s: %s", numErrs, errDg, errMsg)
		}
		return nil
	}
	return c.Retrier.Do(ctx, closure)
}

// BatchDownloadBlobs downloads a number of blobs from the CAS to memory. They must collectively be below the
// maximum total size for a batch read, which is about 4 MB (see MaxBatchSize). Digests must be
// computed in advance by the caller. In case multiple errors occur during the blob read, the
// last error will be returned.
func (c *Client) BatchDownloadBlobs(ctx context.Context, dgs []digest.Digest) (map[digest.Digest][]byte, error) {
	if len(dgs) > int(c.MaxBatchDigests) {
		return nil, fmt.Errorf("batch read of %d total blobs exceeds maximum of %d", len(dgs), c.MaxBatchDigests)
	}
	req := &repb.BatchReadBlobsRequest{InstanceName: c.InstanceName}
	var sz int64
	foundEmpty := false
	for _, dg := range dgs {
		if dg.Size == 0 {
			foundEmpty = true
			continue
		}
		sz += int64(dg.Size)
		req.Digests = append(req.Digests, dg.ToProto())
	}
	if sz > int64(c.MaxBatchSize) {
		return nil, fmt.Errorf("batch read of %d total bytes exceeds maximum of %d", sz, c.MaxBatchSize)
	}
	res := make(map[digest.Digest][]byte)
	if foundEmpty {
		res[digest.Empty] = nil
	}
	opts := c.RPCOpts()
	closure := func() error {
		var resp *repb.BatchReadBlobsResponse
		err := c.CallWithTimeout(ctx, "BatchReadBlobs", func(ctx context.Context) (e error) {
			resp, e = c.cas.BatchReadBlobs(ctx, req, opts...)
			return e
		})
		if err != nil {
			return err
		}

		numErrs, errDg, errMsg := 0, &repb.Digest{}, ""
		var failedDgs []*repb.Digest
		var retriableError error
		allRetriable := true
		for _, r := range resp.Responses {
			st := status.FromProto(r.Status)
			if st.Code() != codes.OK {
				e := st.Err()
				if c.Retrier.ShouldRetry(e) {
					failedDgs = append(failedDgs, r.Digest)
					retriableError = e
				} else {
					allRetriable = false
				}
				numErrs++
				errDg = r.Digest
				errMsg = r.Status.Message
			} else {
				res[digest.NewFromProtoUnvalidated(r.Digest)] = r.Data
			}
		}
		req.Digests = failedDgs
		if numErrs > 0 {
			if allRetriable {
				return retriableError // Retriable errors only, retry the failed digests.
			}
			return fmt.Errorf("downloading blobs as part of a batch resulted in %d failures, including blob %s: %s", numErrs, errDg, errMsg)
		}
		return nil
	}
	return res, c.Retrier.Do(ctx, closure)
}

// makeBatches splits a list of digests into batches of size no more than the maximum.
//
// First, we sort all the blobs, then we make each batch by taking the largest available blob and
// then filling in with as many small blobs as we can fit. This is a naive approach to the knapsack
// problem, and may have suboptimal results in some cases, but it results in deterministic batches,
// runs in O(n log n) time, and avoids most of the pathological cases that result from scanning from
// one end of the list only.
//
// The input list is sorted in-place; additionally, any blob bigger than the maximum will be put in
// a batch of its own and the caller will need to ensure that it is uploaded with Write, not batch
// operations.
func (c *Client) makeBatches(ctx context.Context, dgs []digest.Digest, optimizeSize bool) [][]digest.Digest {
	var batches [][]digest.Digest
	LogContextInfof(ctx, log.Level(2), "Batching %d digests", len(dgs))
	if optimizeSize {
		sort.Slice(dgs, func(i, j int) bool {
			return dgs[i].Size < dgs[j].Size
		})
	}
	for len(dgs) > 0 {
		var batch []digest.Digest
		if optimizeSize {
			batch = []digest.Digest{dgs[len(dgs)-1]}
			dgs = dgs[:len(dgs)-1]
		} else {
			batch = []digest.Digest{dgs[0]}
			dgs = dgs[1:]
		}
		requestOverhead := marshalledFieldSize(int64(len(c.InstanceName)))
		sz := requestOverhead + marshalledRequestSize(batch[0])
		var nextSize int64
		if len(dgs) > 0 {
			nextSize = marshalledRequestSize(dgs[0])
		}
		for len(dgs) > 0 && len(batch) < int(c.MaxBatchDigests) && nextSize <= int64(c.MaxBatchSize)-sz { // nextSize+sz possibly overflows so subtract instead.
			sz += nextSize
			batch = append(batch, dgs[0])
			dgs = dgs[1:]
			if len(dgs) > 0 {
				nextSize = marshalledRequestSize(dgs[0])
			}
		}
		LogContextInfof(ctx, log.Level(3), "Created batch of %d blobs with total size %d", len(batch), sz)
		batches = append(batches, batch)
	}
	LogContextInfof(ctx, log.Level(2), "%d batches created", len(batches))
	return batches
}

func marshalledFieldSize(size int64) int64 {
	return 1 + int64(proto.SizeVarint(uint64(size))) + size
}

func marshalledRequestSize(d digest.Digest) int64 {
	// An additional BatchUpdateBlobsRequest_Request includes the Digest and data fields,
	// as well as the message itself. Every field has a 1-byte size tag, followed by
	// the varint field size for variable-sized fields (digest hash and data).
	// Note that the BatchReadBlobsResponse_Response field is similar, but includes
	// and additional Status proto which can theoretically be unlimited in size.
	// We do not account for it here, relying on the Client setting a large (100MB)
	// limit for incoming messages.
	digestSize := marshalledFieldSize(int64(len(d.Hash)))
	if d.Size > 0 {
		digestSize += 1 + int64(proto.SizeVarint(uint64(d.Size)))
	}
	reqSize := marshalledFieldSize(digestSize)
	if d.Size > 0 {
		reqSize += marshalledFieldSize(int64(d.Size))
	}
	return marshalledFieldSize(reqSize)
}

// ReadBlob fetches a blob from the CAS into a byte slice.
// Returns the size of the blob and the amount of bytes moved through the wire.
func (c *Client) ReadBlob(ctx context.Context, d digest.Digest) ([]byte, *MovedBytesMetadata, error) {
	return c.readBlob(ctx, d, 0, 0)
}

// ReadBlobRange fetches a partial blob from the CAS into a byte slice, starting from offset bytes
// and including at most limit bytes (or no limit if limit==0). The offset must be non-negative and
// no greater than the size of the entire blob. The limit must not be negative, but offset+limit may
// be greater than the size of the entire blob.
func (c *Client) ReadBlobRange(ctx context.Context, d digest.Digest, offset, limit int64) ([]byte, *MovedBytesMetadata, error) {
	return c.readBlob(ctx, d, offset, limit)
}

// Returns the size of the blob and the amount of bytes moved through the wire.
func (c *Client) readBlob(ctx context.Context, dg digest.Digest, offset, limit int64) ([]byte, *MovedBytesMetadata, error) {
	// int might be 32-bit, in which case we could have a blob whose size is representable in int64
	// but not int32, and thus can't fit in a slice. We can check for this by casting and seeing if
	// the result is negative, since 32 bits is big enough wrap all out-of-range values of int64 to
	// negative numbers. If int is 64-bits, the cast is a no-op and so the condition will always fail.
	if int(dg.Size) < 0 {
		return nil, nil, fmt.Errorf("digest size %d is too big to fit in a byte slice", dg.Size)
	}
	if offset > dg.Size {
		return nil, nil, fmt.Errorf("offset %d out of range for a blob of size %d", offset, dg.Size)
	}
	if offset < 0 {
		return nil, nil, fmt.Errorf("offset %d may not be negative", offset)
	}
	if limit < 0 {
		return nil, nil, fmt.Errorf("limit %d may not be negative", limit)
	}
	sz := dg.Size - offset
	if limit > 0 && limit < sz {
		sz = limit
	}
	// Pad size so bytes.Buffer does not reallocate.
	buf := bytes.NewBuffer(make([]byte, 0, sz+bytes.MinRead))
	stats, err := c.readBlobStreamed(ctx, dg, offset, limit, &bufferWriter{buf})
	return buf.Bytes(), stats, err
}

// ReadBlobToFile fetches a blob with a provided digest name from the CAS, saving it into a file.
// It returns the number of bytes read.
func (c *Client) ReadBlobToFile(ctx context.Context, d digest.Digest, fpath string) (*MovedBytesMetadata, error) {
	f, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, c.RegularMode)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return c.readBlobStreamed(ctx, d, 0, 0, &fileWriter{f})
}

// NewCompressedWriteBuffer creates wraps a io.Writer contained compressed contents to write
// decompressed contents.
func NewCompressedWriteBuffer(w io.Writer) (io.WriteCloser, chan error, error) {
	// Our Bytestream abstraction uses a Writer so that the bytestream interface can "write"
	// the data upstream. However, the zstd library only has an interface from a reader.
	// Instead of writing a different bytestream version that returns a reader, we're piping
	// the writer data.
	r, nw := io.Pipe()

	// TODO(rubensf): Reuse decoders when possible to save the effort of starting/closing goroutines.
	decoder, err := zstd.NewReader(r)
	if err != nil {
		return nil, nil, err
	}

	done := make(chan error)
	go func() {
		// WriteTo will block until the reader is closed - or, in this
		// case, the pipe writer, so we have to launch our compressor in a
		// separate thread. As such, we also need a way to signal the main
		// thread that the decoding has finished - which will have some delay
		// from the last Write call.
		_, err := decoder.WriteTo(w)
		if err != nil {
			// Because WriteTo returned early, the pipe writers still
			// have to go somewhere or they'll block execution.
			io.Copy(ioutil.Discard, r)
		}
		decoder.Close()
		done <- err
	}()

	return nw, done, nil
}

// writerTracker is useful as an midware before writing to a Read caller's
// underlying data. Since cas.go should be responsible for sanity checking data,
// and potentially having to re-open files on disk to do a checking earlier
// on the call stack, we dup the writes through a digest creator and track
// how much data was written.
type writerTracker struct {
	w  io.Writer
	pw *io.PipeWriter
	dg digest.Digest
	// Tracked independently of the digest as we might want to retry
	// on partial reads.
	n     int64
	ready chan error
}

func newWriteTracker(w io.Writer) *writerTracker {
	pr, pw := io.Pipe()
	wt := &writerTracker{
		pw:    pw,
		w:     w,
		ready: make(chan error, 1),
		n:     0,
	}

	go func() {
		var err error
		wt.dg, err = digest.NewFromReader(pr)
		wt.ready <- err
	}()

	return wt
}

func (wt *writerTracker) Write(p []byte) (int, error) {
	// Any error on this write will be reflected on the
	// pipe reader end when trying to calculate the digest.
	// Additionally, if we are not downloading the entire
	// blob, we can't even verify the digest to begin with.
	// So we can ignore errors on this pipewriter.
	wt.pw.Write(p)
	n, err := wt.w.Write(p)
	wt.n += int64(n)
	return n, err
}

// Close closes the pipe - which triggers the the end of the
// digest creation.
func (wt *writerTracker) Close() error {
	return wt.pw.Close()
}

type resettableWriter interface {
	io.Writer
	Reset() error
}

type fileWriter struct {
	f *os.File
}

func (fw *fileWriter) Write(p []byte) (int, error) {
	return fw.f.Write(p)
}

func (fw *fileWriter) Reset() error {
	err := fw.f.Truncate(0)
	if err != nil {
		return err
	}
	_, err = fw.f.Seek(0, 0)
	return err
}

type bufferWriter struct {
	b *bytes.Buffer
}

func (bw *bufferWriter) Write(p []byte) (int, error) {
	return bw.b.Write(p)
}

func (bw *bufferWriter) Reset() error {
	bw.b.Reset()
	return nil
}

var DigestMismatchError = errors.New("CAS fetch digest mismatch")

func (c *Client) readBlobStreamed(ctx context.Context, d digest.Digest, offset, limit int64, w resettableWriter) (*MovedBytesMetadata, error) {
	stats := &MovedBytesMetadata{}
	stats.Requested = d.Size
	if d.Size == 0 {
		// Do not download empty blobs.
		return stats, nil
	}
	sz := d.Size - offset
	if limit > 0 && limit < sz {
		sz = limit
	}
	if err := w.Reset(); err != nil {
		return stats, err
	}
	wt := newWriteTracker(w)
	defer func() { stats.LogicalMoved = wt.n }()
	closure := func() (err error) {
		name, wc, done, e := c.maybeCompressReadBlob(d, wt)
		if e != nil {
			return e
		}

		defer func() {
			errC := wc.Close()
			errD := <-done
			close(done)

			if err != nil && errC != nil {
				err = errC
			}
			if err != nil && errD != nil {
				err = fmt.Errorf("Failed to finalize writing downloaded data downstream: %v", err)
			}
		}()

		wireBytes, err := c.readStreamed(ctx, name, offset+wt.n, limit, wc)
		stats.RealMoved += wireBytes
		if err != nil {
			return err
		}
		return nil
	}
	// Only retry on transient backend issues.
	if err := c.Retrier.Do(ctx, closure); err != nil {
		return stats, err
	}
	if wt.n != sz {
		return stats, fmt.Errorf("%w: partial read of digest %s returned %d bytes", DigestMismatchError, wt.dg, sz)
	}

	// Incomplete reads only, since we can't reliably calculate hash without the full blob
	if d.Size == sz {
		// Signal for writeTracker to take the digest of the data.
		if err := wt.Close(); err != nil {
			return stats, err
		}
		// Wait for the digest to be ready.
		if err := <-wt.ready; err != nil {
			return stats, err
		}
		close(wt.ready)
		if wt.dg != d {
			return stats, fmt.Errorf("%w: calculated digest %s != expected digest %s", DigestMismatchError, wt.dg, d)
		}
	}

	return stats, nil
}

// ReadProto reads a blob from the CAS and unmarshals it into the given message.
// Returns the size of the proto and the amount of bytes moved through the wire.
func (c *Client) ReadProto(ctx context.Context, d digest.Digest, msg proto.Message) (*MovedBytesMetadata, error) {
	bytes, stats, err := c.ReadBlob(ctx, d)
	if err != nil {
		return stats, err
	}
	return stats, proto.Unmarshal(bytes, msg)
}

// MissingBlobs queries the CAS to determine if it has the listed blobs. It returns a list of the
// missing blobs.
func (c *Client) MissingBlobs(ctx context.Context, ds []digest.Digest) ([]digest.Digest, error) {
	var batches [][]digest.Digest
	var missing []digest.Digest
	var resultMutex sync.Mutex
	const maxQueryLimit = 10000
	for len(ds) > 0 {
		batchSize := maxQueryLimit
		if len(ds) < maxQueryLimit {
			batchSize = len(ds)
		}
		var batch []digest.Digest
		for i := 0; i < batchSize; i++ {
			batch = append(batch, ds[i])
		}
		ds = ds[batchSize:]
		LogContextInfof(ctx, log.Level(3), "Created query batch of %d blobs", len(batch))
		batches = append(batches, batch)
	}
	LogContextInfof(ctx, log.Level(3), "%d query batches created", len(batches))

	eg, eCtx := errgroup.WithContext(ctx)
	for i, batch := range batches {
		i, batch := i, batch // https://golang.org/doc/faq#closures_and_goroutines
		eg.Go(func() error {
			if err := c.casUploaders.Acquire(eCtx, 1); err != nil {
				return err
			}
			defer c.casUploaders.Release(1)
			if i%logInterval == 0 {
				LogContextInfof(ctx, log.Level(3), "%d missing batches left to query", len(batches)-i)
			}
			var batchPb []*repb.Digest
			for _, dg := range batch {
				batchPb = append(batchPb, dg.ToProto())
			}
			req := &repb.FindMissingBlobsRequest{
				InstanceName: c.InstanceName,
				BlobDigests:  batchPb,
			}
			resp, err := c.FindMissingBlobs(eCtx, req)
			if err != nil {
				return err
			}
			resultMutex.Lock()
			for _, d := range resp.MissingBlobDigests {
				missing = append(missing, digest.NewFromProtoUnvalidated(d))
			}
			resultMutex.Unlock()
			if eCtx.Err() != nil {
				return eCtx.Err()
			}
			return nil
		})
	}
	LogContextInfof(ctx, log.Level(3), "Waiting for remaining query jobs")
	err := eg.Wait()
	LogContextInfof(ctx, log.Level(3), "Done")
	return missing, err
}

func (c *Client) resourceNameRead(hash string, sizeBytes int64) string {
	return fmt.Sprintf("%s/blobs/%s/%d", c.InstanceName, hash, sizeBytes)
}

// TODO(rubensf): Converge compressor to proto in https://github.com/bazelbuild/remote-apis/pull/168 once
// that gets merged in.
func (c *Client) resourceNameCompressedRead(hash string, sizeBytes int64) string {
	return fmt.Sprintf("%s/compressed-blobs/zstd/%s/%d", c.InstanceName, hash, sizeBytes)
}

// ResourceNameWrite generates a valid write resource name.
func (c *Client) ResourceNameWrite(hash string, sizeBytes int64) string {
	return fmt.Sprintf("%s/uploads/%s/blobs/%s/%d", c.InstanceName, uuid.New(), hash, sizeBytes)
}

// ResourceNameCompressedWrite generates a valid write resource name.
// TODO(rubensf): Converge compressor to proto in https://github.com/bazelbuild/remote-apis/pull/168 once
// that gets merged in.
func (c *Client) ResourceNameCompressedWrite(hash string, sizeBytes int64) string {
	return fmt.Sprintf("%s/uploads/%s/compressed-blobs/zstd/%s/%d", c.InstanceName, uuid.New(), hash, sizeBytes)
}

// GetDirectoryTree returns the entire directory tree rooted at the given digest (which must target
// a Directory stored in the CAS).
func (c *Client) GetDirectoryTree(ctx context.Context, d *repb.Digest) (result []*repb.Directory, err error) {
	pageTok := ""
	result = []*repb.Directory{}
	closure := func(ctx context.Context) error {
		stream, err := c.GetTree(ctx, &repb.GetTreeRequest{
			InstanceName: c.InstanceName,
			RootDigest:   d,
			PageToken:    pageTok,
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
			pageTok = resp.NextPageToken
			result = append(result, resp.Directories...)
		}
		return nil
	}
	if err := c.Retrier.Do(ctx, func() error { return c.CallWithTimeout(ctx, "GetTree", closure) }); err != nil {
		return nil, err
	}
	return result, nil
}

// FlattenActionOutputs collects and flattens all the outputs of an action.
// It downloads the output directory metadata, if required, but not the leaf file blobs.
func (c *Client) FlattenActionOutputs(ctx context.Context, ar *repb.ActionResult) (map[string]*TreeOutput, error) {
	outs := make(map[string]*TreeOutput)
	for _, file := range ar.OutputFiles {
		outs[file.Path] = &TreeOutput{
			Path:         file.Path,
			Digest:       digest.NewFromProtoUnvalidated(file.Digest),
			IsExecutable: file.IsExecutable,
		}
	}
	for _, sm := range ar.OutputFileSymlinks {
		outs[sm.Path] = &TreeOutput{
			Path:          sm.Path,
			SymlinkTarget: sm.Target,
		}
	}
	for _, sm := range ar.OutputDirectorySymlinks {
		outs[sm.Path] = &TreeOutput{
			Path:          sm.Path,
			SymlinkTarget: sm.Target,
		}
	}
	for _, dir := range ar.OutputDirectories {
		t := &repb.Tree{}
		if _, err := c.ReadProto(ctx, digest.NewFromProtoUnvalidated(dir.TreeDigest), t); err != nil {
			return nil, err
		}
		dirouts, err := c.FlattenTree(t, dir.Path)
		if err != nil {
			return nil, err
		}
		for _, out := range dirouts {
			outs[out.Path] = out
		}
	}
	return outs, nil
}

// DownloadDirectory downloads the entire directory of given digest.
// It returns the number of logical and real bytes downloaded, which may be different from sum
// of sizes of the files due to dedupping and compression.
func (c *Client) DownloadDirectory(ctx context.Context, d digest.Digest, execRoot string, cache filemetadata.Cache) (map[string]*TreeOutput, *MovedBytesMetadata, error) {
	dir := &repb.Directory{}
	stats := &MovedBytesMetadata{}

	protoStats, err := c.ReadProto(ctx, d, dir)
	stats.addFrom(protoStats)
	if err != nil {
		return nil, stats, fmt.Errorf("digest %v cannot be mapped to a directory proto: %v", d, err)
	}

	dirs, err := c.GetDirectoryTree(ctx, d.ToProto())
	if err != nil {
		return nil, stats, err
	}

	outputs, err := c.FlattenTree(&repb.Tree{
		Root:     dir,
		Children: dirs,
	}, "")
	if err != nil {
		return nil, stats, err
	}

	outStats, err := c.downloadOutputs(ctx, outputs, execRoot, cache)
	stats.addFrom(outStats)
	return outputs, stats, err
}

// DownloadActionOutputs downloads the output files and directories in the given action result. It returns the amount of downloaded bytes.
// It returns the number of logical and real bytes downloaded, which may be different from sum
// of sizes of the files due to dedupping and compression.
func (c *Client) DownloadActionOutputs(ctx context.Context, resPb *repb.ActionResult, execRoot string, cache filemetadata.Cache) (*MovedBytesMetadata, error) {
	outs, err := c.FlattenActionOutputs(ctx, resPb)
	if err != nil {
		return nil, err
	}
	// Remove the existing output directories before downloading.
	for _, dir := range resPb.OutputDirectories {
		if err := os.RemoveAll(filepath.Join(execRoot, dir.Path)); err != nil {
			return nil, err
		}
	}
	return c.downloadOutputs(ctx, outs, execRoot, cache)
}

func (c *Client) downloadOutputs(ctx context.Context, outs map[string]*TreeOutput, execRoot string, cache filemetadata.Cache) (*MovedBytesMetadata, error) {
	var symlinks, copies []*TreeOutput
	downloads := make(map[digest.Digest]*TreeOutput)
	fullStats := &MovedBytesMetadata{}
	for _, out := range outs {
		path := filepath.Join(execRoot, out.Path)
		if out.IsEmptyDirectory {
			if err := os.MkdirAll(path, c.DirMode); err != nil {
				return fullStats, err
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(path), c.DirMode); err != nil {
			return fullStats, err
		}
		// We create the symbolic links after all regular downloads are finished, because dangling
		// links will not work.
		if out.SymlinkTarget != "" {
			symlinks = append(symlinks, out)
			continue
		}
		if _, ok := downloads[out.Digest]; ok {
			copies = append(copies, out)
			// All copies are effectivelly cached
			fullStats.Requested += out.Digest.Size
			fullStats.Cached += out.Digest.Size
		} else {
			downloads[out.Digest] = out
		}
	}
	stats, err := c.DownloadFiles(ctx, execRoot, downloads)
	fullStats.addFrom(stats)
	if err != nil {
		return fullStats, err
	}

	for _, output := range downloads {
		path := output.Path
		md := &filemetadata.Metadata{
			Digest:       output.Digest,
			IsExecutable: output.IsExecutable,
		}
		if err := cache.Update(path, md); err != nil {
			return fullStats, err
		}
	}
	for _, out := range copies {
		perm := c.RegularMode
		if out.IsExecutable {
			perm = c.ExecutableMode
		}
		src := downloads[out.Digest]
		if src.IsEmptyDirectory {
			return fullStats, fmt.Errorf("unexpected empty directory: %s", src.Path)
		}
		if err := copyFile(execRoot, execRoot, src.Path, out.Path, perm); err != nil {
			return fullStats, err
		}
	}
	for _, out := range symlinks {
		if err := os.Symlink(out.SymlinkTarget, filepath.Join(execRoot, out.Path)); err != nil {
			return fullStats, err
		}
	}
	return fullStats, nil
}

func copyFile(srcExecRoot, dstExecRoot, from, to string, mode os.FileMode) error {
	src := filepath.Join(srcExecRoot, from)
	s, err := os.Open(src)
	if err != nil {
		return err
	}
	defer s.Close()

	dst := filepath.Join(dstExecRoot, to)
	t, err := os.OpenFile(dst, os.O_RDWR|os.O_CREATE, mode)
	if err != nil {
		return err
	}
	defer t.Close()
	_, err = io.Copy(t, s)
	return err
}

type downloadRequest struct {
	digest   digest.Digest
	execRoot string
	// TODO(olaola): use channels for cancellations instead of embedding download context.
	context context.Context
	output  *TreeOutput
	meta    *requestMetadata
	wait    chan<- *downloadResponse
}

type downloadResponse struct {
	stats *MovedBytesMetadata
	err   error
}

func (c *Client) downloadProcessor() {
	var buffer []*downloadRequest
	ticker := time.NewTicker(time.Duration(c.UnifiedDownloadTickDuration))
	for {
		select {
		case ch, ok := <-c.casDownloadRequests:
			if !ok {
				// Client is exiting. Notify remaining downloads to prevent deadlocks.
				ticker.Stop()
				if buffer != nil {
					for _, r := range buffer {
						r.wait <- &downloadResponse{err: context.Canceled}
					}
				}
				return
			}
			buffer = append(buffer, ch)
			if len(buffer) >= int(c.UnifiedDownloadBufferSize) {
				c.download(buffer)
				buffer = nil
			}
		case <-ticker.C:
			if buffer != nil {
				c.download(buffer)
				buffer = nil
			}
		}
	}
}

func afterDownload(batch []digest.Digest, reqs map[digest.Digest][]*downloadRequest, bytesMoved map[digest.Digest]*MovedBytesMetadata, err error) {
	if err != nil {
		log.Errorf("Error downloading %v: %v", batch[0], err)
	}
	for _, dg := range batch {
		rs, ok := reqs[dg]
		if !ok {
			log.Errorf("Precondition failed: download request not found in input %v.", dg)
		}
		stats, ok := bytesMoved[dg]
		// If there's no real bytes moved it likely means there was an error moving these.
		for i, r := range rs {
			// bytesMoved will be zero for error cases.
			// We only report it to the first client to prevent double accounting.
			r.wait <- &downloadResponse{stats: stats, err: err}
			if i == 0 {
				// Prevent races by not writing to the original stats.
				newStats := &MovedBytesMetadata{}
				newStats.Requested = stats.Requested
				newStats.Cached = stats.LogicalMoved
				newStats.RealMoved = 0
				newStats.LogicalMoved = 0

				stats = newStats
			}
		}
	}
}

func (c *Client) downloadBatch(ctx context.Context, batch []digest.Digest, reqs map[digest.Digest][]*downloadRequest) {
	LogContextInfof(ctx, log.Level(3), "Downloading batch of %d files", len(batch))
	bchMap, err := c.BatchDownloadBlobs(ctx, batch)
	if err != nil {
		afterDownload(batch, reqs, map[digest.Digest]*MovedBytesMetadata{}, err)
		return
	}
	for _, dg := range batch {
		stats := &MovedBytesMetadata{
			Requested:    dg.Size,
			LogicalMoved: dg.Size,
			// There's no compression for batch requests, and there's no such thing as "partial" data for
			// a blob since they're all inlined in the response.
			RealMoved: dg.Size,
		}
		data := bchMap[dg]
		for i, r := range reqs[dg] {
			perm := c.RegularMode
			if r.output.IsExecutable {
				perm = c.ExecutableMode
			}
			// bytesMoved will be zero for error cases.
			// We only report it to the first client to prevent double accounting.
			r.wait <- &downloadResponse{
				stats: stats,
				err:   ioutil.WriteFile(filepath.Join(r.execRoot, r.output.Path), data, perm),
			}
			if i == 0 {
				// Prevent races by not writing to the original stats.
				newStats := &MovedBytesMetadata{}
				newStats.Requested = stats.Requested
				newStats.Cached = stats.LogicalMoved
				newStats.RealMoved = 0
				newStats.LogicalMoved = 0

				stats = newStats
			}
		}
	}
}

func (c *Client) downloadSingle(ctx context.Context, dg digest.Digest, reqs map[digest.Digest][]*downloadRequest) (err error) {
	// The lock is released when all file copies are finished.
	// We cannot release the lock after each individual file copy, because
	// the caller might move the file, and we don't have the contents in memory.
	bytesMoved := map[digest.Digest]*MovedBytesMetadata{}
	defer afterDownload([]digest.Digest{dg}, reqs, bytesMoved, err)
	rs := reqs[dg]
	if len(rs) < 1 {
		return fmt.Errorf("Failed precondition: cannot find %v in reqs map", dg)
	}
	r := rs[0]
	rs = rs[1:]
	path := filepath.Join(r.execRoot, r.output.Path)
	LogContextInfof(ctx, log.Level(3), "Downloading single file with digest %s to %s", r.output.Digest, path)
	stats, err := c.ReadBlobToFile(ctx, r.output.Digest, path)
	bytesMoved[r.output.Digest] = stats
	if err != nil {
		return err
	}
	if r.output.IsExecutable {
		if err := os.Chmod(path, c.ExecutableMode); err != nil {
			return err
		}
	}
	for _, cp := range rs {
		perm := c.RegularMode
		if cp.output.IsExecutable {
			perm = c.ExecutableMode
		}
		if err := copyFile(r.execRoot, cp.execRoot, r.output.Path, cp.output.Path, perm); err != nil {
			return err
		}
	}
	return err
}

func (c *Client) download(data []*downloadRequest) {
	// It is possible to have multiple same files download to different locations.
	// This will download once and copy to the other locations.
	reqs := make(map[digest.Digest][]*downloadRequest)
	var metas []*requestMetadata
	for _, r := range data {
		rs := reqs[r.digest]
		rs = append(rs, r)
		reqs[r.digest] = rs
		metas = append(metas, r.meta)
	}

	var dgs []digest.Digest

	if bool(c.useBatchOps) && bool(c.UtilizeLocality) {
		paths := make([]*TreeOutput, 0, len(data))
		for _, r := range data {
			paths = append(paths, r.output)
		}

		// This is to utilize locality in disk when writing files.
		sort.Slice(paths, func(i, j int) bool {
			return paths[i].Path < paths[j].Path
		})

		for _, path := range paths {
			dgs = append(dgs, path.Digest)
		}
	} else {
		for dg := range reqs {
			dgs = append(dgs, dg)
		}
	}

	unifiedMeta := getUnifiedMetadata(metas)
	var err error
	ctx := context.Background()
	if unifiedMeta.actionID != "" {
		ctx, err = ContextWithMetadata(context.Background(), unifiedMeta.toolName, unifiedMeta.actionID, unifiedMeta.invocationID)
	}
	if err != nil {
		afterDownload(dgs, reqs, map[digest.Digest]*MovedBytesMetadata{}, err)
		return
	}

	LogContextInfof(ctx, log.Level(2), "%d digests to download (%d reqs)", len(dgs), len(reqs))
	var batches [][]digest.Digest
	if c.useBatchOps {
		batches = c.makeBatches(ctx, dgs, !bool(c.UtilizeLocality))
	} else {
		LogContextInfof(ctx, log.Level(2), "Downloading them individually")
		for i := range dgs {
			LogContextInfof(ctx, log.Level(3), "Creating single batch of blob %s", dgs[i])
			batches = append(batches, dgs[i:i+1])
		}
	}

	for i, batch := range batches {
		i, batch := i, batch // https://golang.org/doc/faq#closures_and_goroutines
		go func() {
			if c.casDownloaders.Acquire(ctx, 1) == nil {
				defer c.casDownloaders.Release(1)
			}
			if i%logInterval == 0 {
				LogContextInfof(ctx, log.Level(2), "%d batches left to download", len(batches)-i)
			}
			if len(batch) > 1 {
				c.downloadBatch(ctx, batch, reqs)
			} else {
				rs := reqs[batch[0]]
				downloadCtx := ctx
				if len(rs) == 1 {
					// We have only one download request for this digest.
					// Download on same context as the issuing request, to support proper cancellation.
					downloadCtx = rs[0].context
				}
				c.downloadSingle(downloadCtx, batch[0], reqs)
			}
		}()
	}
}

// This is a legacy function used only when UnifiedDownloads=false.
// It will be removed when UnifiedDownloads=true is stable.
// Returns the number of logical and real bytes downloaded, which may be
// different from sum of sizes of the files due to compression.
func (c *Client) downloadNonUnified(ctx context.Context, execRoot string, outputs map[digest.Digest]*TreeOutput) (*MovedBytesMetadata, error) {
	var dgs []digest.Digest
	// statsMu protects stats across threads.
	statsMu := sync.Mutex{}
	fullStats := &MovedBytesMetadata{}

	if bool(c.useBatchOps) && bool(c.UtilizeLocality) {
		paths := make([]*TreeOutput, 0, len(outputs))
		for _, output := range outputs {
			paths = append(paths, output)
		}

		// This is to utilize locality in disk when writing files.
		sort.Slice(paths, func(i, j int) bool {
			return paths[i].Path < paths[j].Path
		})

		for _, path := range paths {
			dgs = append(dgs, path.Digest)
			fullStats.Requested += path.Digest.Size
		}
	} else {
		for dg := range outputs {
			dgs = append(dgs, dg)
			fullStats.Requested += dg.Size
		}
	}

	LogContextInfof(ctx, log.Level(2), "%d items to download", len(dgs))
	var batches [][]digest.Digest
	if c.useBatchOps {
		batches = c.makeBatches(ctx, dgs, !bool(c.UtilizeLocality))
	} else {
		LogContextInfof(ctx, log.Level(2), "Downloading them individually")
		for i := range dgs {
			LogContextInfof(ctx, log.Level(3), "Creating single batch of blob %s", dgs[i])
			batches = append(batches, dgs[i:i+1])
		}
	}

	eg, eCtx := errgroup.WithContext(ctx)
	for i, batch := range batches {
		i, batch := i, batch // https://golang.org/doc/faq#closures_and_goroutines
		eg.Go(func() error {
			if err := c.casDownloaders.Acquire(eCtx, 1); err != nil {
				return err
			}
			defer c.casDownloaders.Release(1)
			if i%logInterval == 0 {
				LogContextInfof(ctx, log.Level(2), "%d batches left to download", len(batches)-i)
			}
			if len(batch) > 1 {
				LogContextInfof(ctx, log.Level(3), "Downloading batch of %d files", len(batch))
				bchMap, err := c.BatchDownloadBlobs(eCtx, batch)
				for _, dg := range batch {
					data := bchMap[dg]
					out := outputs[dg]
					perm := c.RegularMode
					if out.IsExecutable {
						perm = c.ExecutableMode
					}
					if err := ioutil.WriteFile(filepath.Join(execRoot, out.Path), data, perm); err != nil {
						return err
					}
					statsMu.Lock()
					fullStats.LogicalMoved += int64(len(data))
					fullStats.RealMoved += int64(len(data))
					statsMu.Unlock()
				}
				if err != nil {
					return err
				}
			} else {
				out := outputs[batch[0]]
				path := filepath.Join(execRoot, out.Path)
				LogContextInfof(ctx, log.Level(3), "Downloading single file with digest %s to %s", out.Digest, path)
				stats, err := c.ReadBlobToFile(ctx, out.Digest, path)
				statsMu.Lock()
				fullStats.addFrom(stats)
				statsMu.Unlock()
				if err != nil {
					return err
				}
				if out.IsExecutable {
					if err := os.Chmod(path, c.ExecutableMode); err != nil {
						return err
					}
				}
			}
			if eCtx.Err() != nil {
				return eCtx.Err()
			}
			return nil
		})
	}

	LogContextInfof(ctx, log.Level(3), "Waiting for remaining jobs")
	err := eg.Wait()
	LogContextInfof(ctx, log.Level(3), "Done")
	return fullStats, err
}

// DownloadFiles downloads the output files under |execRoot|.
// It returns the number of logical and real bytes downloaded, which may be different from sum
// of sizes of the files due to dedupping and compression.
func (c *Client) DownloadFiles(ctx context.Context, execRoot string, outputs map[digest.Digest]*TreeOutput) (*MovedBytesMetadata, error) {
	stats := &MovedBytesMetadata{}

	if !c.UnifiedDownloads {
		return c.downloadNonUnified(ctx, execRoot, outputs)
	}
	count := len(outputs)
	if count == 0 {
		return stats, nil
	}
	toolName, actionID, invocationID, err := GetContextMetadata(ctx)
	if err != nil {
		return stats, err
	}
	meta := &requestMetadata{
		toolName:     toolName,
		actionID:     actionID,
		invocationID: invocationID,
	}
	wait := make(chan *downloadResponse, count)
	for dg, out := range outputs {
		r := &downloadRequest{
			digest:   dg,
			context:  ctx,
			execRoot: execRoot,
			output:   out,
			meta:     meta,
			wait:     wait,
		}
		select {
		case <-ctx.Done():
			LogContextInfof(ctx, log.Level(2), "Download canceled")
			return stats, ctx.Err()
		case c.casDownloadRequests <- r:
			continue
		}
	}

	// Wait for all downloads to finish.
	for count > 0 {
		select {
		case <-ctx.Done():
			LogContextInfof(ctx, log.Level(2), "Download canceled")
			return stats, ctx.Err()
		case resp := <-wait:
			if resp.err != nil {
				return stats, resp.err
			}
			stats.addFrom(resp.stats)
			count--
		}
	}
	return stats, nil
}

func (c *Client) shouldCompress(sizeBytes int64) bool {
	return int64(c.CompressedBytestreamThreshold) >= 0 && int64(c.CompressedBytestreamThreshold) <= sizeBytes
}

func (c *Client) writeRscName(dg digest.Digest) string {
	if c.shouldCompress(dg.Size) {
		return c.ResourceNameCompressedWrite(dg.Hash, dg.Size)
	}
	return c.ResourceNameWrite(dg.Hash, dg.Size)
}
