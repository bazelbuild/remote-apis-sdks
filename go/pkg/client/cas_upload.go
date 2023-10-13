package client

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/casng"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/chunker"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/contextmd"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/impath"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/uploadinfo"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	log "github.com/golang/glog"
	"github.com/klauspost/compress/zstd"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// MissingBlobs queries the CAS to determine if it has the specified blobs.
// Returns a slice of missing blobs.
func (c *Client) MissingBlobs(ctx context.Context, digests []digest.Digest) ([]digest.Digest, error) {
	var missing []digest.Digest
	var resultMutex sync.Mutex
	batches := c.makeQueryBatches(ctx, digests)
	eg, eCtx := errgroup.WithContext(ctx)
	for i, batch := range batches {
		i, batch := i, batch // https://golang.org/doc/faq#closures_and_goroutines
		eg.Go(func() error {
			if err := c.casUploaders.Acquire(eCtx, 1); err != nil {
				return err
			}
			defer c.casUploaders.Release(1)
			if i%logInterval == 0 {
				contextmd.Infof(ctx, log.Level(3), "%d missing batches left to query", len(batches)-i)
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
	contextmd.Infof(ctx, log.Level(3), "Waiting for remaining query jobs")
	err := eg.Wait()
	contextmd.Infof(ctx, log.Level(3), "Done")
	return missing, err
}

// UploadIfMissing writes the missing blobs from those specified to the CAS.
//
// The blobs are first matched against existing ones and only the missing blobs are written.
// Returns a slice of missing digests that were written and the sum of total bytes moved, which
// may be different from logical bytes moved (i.e. sum of digest sizes) due to compression.
func (c *Client) UploadIfMissing(ctx context.Context, entries ...*uploadinfo.Entry) ([]digest.Digest, int64, error) {
	if c.UnifiedUploads {
		return c.uploadUnified(ctx, entries...)
	}
	return c.uploadNonUnified(ctx, entries...)
}

// WriteBlobs is a proxy method for UploadIfMissing that facilitates specifying a map of
// digest-to-blob. It's intended for use with PackageTree.
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

// WriteBlob (over)writes a blob to the CAS regardless if it already exists.
func (c *Client) WriteBlob(ctx context.Context, blob []byte) (digest.Digest, error) {
	ue := uploadinfo.EntryFromBlob(blob)
	dg := ue.Digest
	if dg.IsEmpty() {
		contextmd.Infof(ctx, log.Level(2), "Skipping upload of empty blob %s", dg)
		return dg, nil
	}
	ch, err := chunker.New(ue, c.shouldCompressEntry(ue), int(c.ChunkMaxSize))
	if err != nil {
		return dg, err
	}
	_, err = c.writeChunked(ctx, c.writeRscName(ue), ch, false, 0)
	return dg, err
}

// WriteProto is a proxy method for WriteBlob that allows specifying a proto to write.
func (c *Client) WriteProto(ctx context.Context, msg proto.Message) (digest.Digest, error) {
	bytes, err := proto.Marshal(msg)
	if err != nil {
		return digest.Empty, err
	}
	return c.WriteBlob(ctx, bytes)
}

// zstdEncoder is a shared instance that should only be used in stateless mode, i.e. only by calling EncodeAll()
var zstdEncoder, _ = zstd.NewWriter(nil)

// BatchWriteBlobs (over)writes specified blobs to the CAS, regardless if they already exist.
//
// The collective size must be below the maximum total size for a batch upload, which
// is about 4 MB (see MaxBatchSize).
// In case multiple errors occur during the blob upload, the last error is returned.
func (c *Client) BatchWriteBlobs(ctx context.Context, blobs map[digest.Digest][]byte) error {
	var reqs []*repb.BatchUpdateBlobsRequest_Request
	var sz int64
	for k, b := range blobs {
		r := &repb.BatchUpdateBlobsRequest_Request{
			Digest: k.ToProto(),
			Data:   b,
		}
		if bool(c.useBatchCompression) && c.shouldCompress(k.Size) {
			r.Data = zstdEncoder.EncodeAll(r.Data, nil)
			r.Compressor = repb.Compressor_ZSTD
			sz += int64(len(r.Data))
		} else {
			sz += int64(k.Size)
		}
		reqs = append(reqs, r)
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
				e := StatusDetailedError(st)
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
				errMsg = e.Error()
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

// ResourceNameWrite generates a valid write resource name.
func (c *Client) ResourceNameWrite(hash string, sizeBytes int64) string {
	rname, _ := c.ResourceName("uploads", uuid.New(), "blobs", hash, strconv.FormatInt(sizeBytes, 10))
	return rname
}

// ResourceNameCompressedWrite generates a valid write resource name.
// TODO(rubensf): Converge compressor to proto in https://github.com/bazelbuild/remote-apis/pull/168 once
// that gets merged in.
func (c *Client) ResourceNameCompressedWrite(hash string, sizeBytes int64) string {
	rname, _ := c.ResourceName("uploads", uuid.New(), "compressed-blobs", "zstd", hash, strconv.FormatInt(sizeBytes, 10))
	return rname
}

func (c *Client) writeRscName(ue *uploadinfo.Entry) string {
	if c.shouldCompressEntry(ue) {
		return c.ResourceNameCompressedWrite(ue.Digest.Hash, ue.Digest.Size)
	}
	return c.ResourceNameWrite(ue.Digest.Hash, ue.Digest.Size)
}

type uploadRequest struct {
	ue     *uploadinfo.Entry
	meta   *contextmd.Metadata
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

func (c *Client) uploadUnified(ctx context.Context, entries ...*uploadinfo.Entry) ([]digest.Digest, int64, error) {
	contextmd.Infof(ctx, log.Level(2), "Request to upload %d blobs", len(entries))

	if len(entries) == 0 {
		return nil, 0, nil
	}
	meta, err := contextmd.ExtractMetadata(ctx)
	if err != nil {
		return nil, 0, err
	}
	wait := make(chan *uploadResponse, len(entries))
	var dgs []digest.Digest
	dedupDgs := make(map[digest.Digest]bool, len(entries))
	for _, ue := range entries {
		if _, ok := dedupDgs[ue.Digest]; !ok {
			dgs = append(dgs, ue.Digest)
			dedupDgs[ue.Digest] = true
		}
	}
	missing, err := c.MissingBlobs(ctx, dgs)
	if err != nil {
		return nil, 0, err
	}
	missingDgs := make(map[digest.Digest]bool, len(missing))
	for _, dg := range missing {
		missingDgs[dg] = true
	}
	var reqs []*uploadRequest
	for _, ue := range entries {
		if _, ok := missingDgs[ue.Digest]; !ok {
			continue
		}
		if ue.Digest.IsEmpty() {
			contextmd.Infof(ctx, log.Level(2), "Skipping upload of empty entry %s", ue.Digest)
			continue
		}
		req := &uploadRequest{
			ue:   ue,
			meta: meta,
			wait: wait,
		}
		reqs = append(reqs, req)
		select {
		case <-ctx.Done():
			contextmd.Infof(ctx, log.Level(2), "Upload canceled")
			c.cancelPendingRequests(reqs)
			return nil, 0, fmt.Errorf("context cancelled: %w", ctx.Err())
		case c.casUploadRequests <- req:
			continue
		}
	}
	totalBytesMoved := int64(0)
	finalMissing := make([]digest.Digest, 0, len(reqs))
	for i := 0; i < len(reqs); i++ {
		select {
		case <-ctx.Done():
			c.cancelPendingRequests(reqs)
			return nil, 0, fmt.Errorf("context cancelled: %w", ctx.Err())
		case resp := <-wait:
			if resp.err != nil {
				return nil, 0, resp.err
			}
			if resp.missing {
				finalMissing = append(finalMissing, resp.digest)
			}
			totalBytesMoved += resp.bytesMoved
		}
	}
	return finalMissing, totalBytesMoved, nil
}

func (c *Client) uploadProcessor(ctx context.Context) {
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
					c.upload(ctx, buffer)
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
				c.upload(ctx, buffer)
				buffer = nil
			}
		}
	}
}

func (c *Client) upload(ctx context.Context, reqs []*uploadRequest) {
	// Collect new uploads.
	newStates := make(map[digest.Digest]*uploadState)
	var newUploads []digest.Digest
	var metas []*contextmd.Metadata
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

	unifiedMeta := contextmd.MergeMetadata(metas...)
	var err error
	if unifiedMeta.ActionID != "" {
		ctx, err = contextmd.WithMetadata(ctx, unifiedMeta)
	}
	if err != nil {
		for _, st := range newStates {
			updateAndNotify(st, 0, err, false)
		}
		return
	}

	contextmd.Infof(ctx, log.Level(2), "%d new items to store", len(newUploads))
	var batches [][]digest.Digest
	if c.useBatchOps {
		batches = c.makeBatches(ctx, newUploads, true)
	} else {
		contextmd.Infof(ctx, log.Level(2), "Uploading them individually")
		for i := range newUploads {
			contextmd.Infof(ctx, log.Level(3), "Creating single batch of blob %s", newUploads[i])
			batches = append(batches, newUploads[i:i+1])
		}
	}

	for i, batch := range batches {
		i, batch := i, batch // https://golang.org/doc/faq#closures_and_goroutines
		go func() {
			if c.casUploaders.Acquire(ctx, 1) == nil {
				defer c.casUploaders.Release(1)
			}
			if i%logInterval == 0 {
				contextmd.Infof(ctx, log.Level(2), "%d batches left to store", len(batches)-i)
			}
			if len(batch) > 1 {
				contextmd.Infof(ctx, log.Level(3), "Uploading batch of %d blobs", len(batch))
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
				contextmd.Infof(ctx, log.Level(3), "Uploading single blob with digest %s", batch[0])
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
				log.V(3).Infof("Uploading single blob with digest %s", batch[0])
				ch, err := chunker.New(st.ue, c.shouldCompressEntry(st.ue), int(c.ChunkMaxSize))
				if err != nil {
					updateAndNotify(st, 0, err, true)
				}
				totalBytes, err := c.writeChunked(cCtx, c.writeRscName(st.ue), ch, false, 0)
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
		if dg.IsEmpty() {
			contextmd.Infof(ctx, log.Level(2), "Skipping upload of empty blob %s", dg)
			continue
		}
		if _, ok := ueList[dg]; !ok {
			dgs = append(dgs, dg)
			ueList[dg] = ue
		}
	}

	missing, err := c.MissingBlobs(ctx, dgs)
	if err != nil {
		return nil, 0, err
	}
	contextmd.Infof(ctx, log.Level(2), "%d items to store", len(missing))
	var batches [][]digest.Digest
	if c.useBatchOps {
		batches = c.makeBatches(ctx, missing, true)
	} else {
		contextmd.Infof(ctx, log.Level(2), "Uploading them individually")
		for i := range missing {
			contextmd.Infof(ctx, log.Level(3), "Creating single batch of blob %s", missing[i])
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
				contextmd.Infof(ctx, log.Level(2), "%d batches left to store", len(batches)-i)
			}
			if len(batch) > 1 {
				contextmd.Infof(ctx, log.Level(3), "Uploading batch of %d blobs", len(batch))
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

					if dg.Size != int64(len(data)) {
						return errors.Errorf("blob size changed while uploading, given:%d now:%d for %s", dg.Size, int64(len(data)), ue.Path)
					}

					bchMap[dg] = data
					atomic.AddInt64(&totalBytesTransferred, int64(len(data)))
				}
				if err := c.BatchWriteBlobs(eCtx, bchMap); err != nil {
					return err
				}
			} else {
				contextmd.Infof(ctx, log.Level(3), "Uploading single blob with digest %s", batch[0])
				ue := ueList[batch[0]]
				ch, err := chunker.New(ue, c.shouldCompressEntry(ue), int(c.ChunkMaxSize))
				if err != nil {
					return err
				}
				written, err := c.writeChunked(eCtx, c.writeRscName(ue), ch, false, 0)
				if err != nil {
					return fmt.Errorf("failed to upload %s: %w", ue.Path, err)
				}
				atomic.AddInt64(&totalBytesTransferred, written)
			}
			if eCtx.Err() != nil {
				return eCtx.Err()
			}
			return nil
		})
	}

	contextmd.Infof(ctx, log.Level(2), "Waiting for remaining jobs")
	err = eg.Wait()
	contextmd.Infof(ctx, log.Level(2), "Done")
	if err != nil {
		contextmd.Infof(ctx, log.Level(2), "Upload error: %v", err)
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

// NgUploadTree delegates to UploadTree of the casng package.
func (c *Client) NgUploadTree(ctx context.Context, execRoot impath.Absolute, workingDir, remoteWorkingDir impath.Relative, reqs ...casng.UploadRequest) (rootDigest digest.Digest, uploaded []digest.Digest, stats casng.Stats, err error) {
	return c.ngCasUploader.UploadTree(ctx, execRoot, workingDir, remoteWorkingDir, reqs...)
}

// NgUpload delegates to Upload of the casng package.
func (c *Client) NgUpload(ctx context.Context, reqs ...casng.UploadRequest) ([]digest.Digest, casng.Stats, error) {
	return c.ngCasUploader.Upload(ctx, reqs...)
}

// IsCasNG returns true if casng feature flag is turned on.
func (c *Client) IsCasNG() bool {
	return c.useCasNg
}
