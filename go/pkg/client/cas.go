package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/chunker"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/filemetadata"
	"github.com/golang/protobuf/proto"
	"github.com/pborman/uuid"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	log "github.com/golang/glog"
)

const logInterval = 25

type requestMetadata struct {
	toolName     string
	actionID     string
	invocationID string
}

type uploadRequest struct {
	ue   *chunker.UploadEntry
	meta *requestMetadata
	wait chan<- *uploadResponse
}

type uploadResponse struct {
	digest  digest.Digest
	err     error
	missing bool
}

type uploadState struct {
	ue  *chunker.UploadEntry
	err error

	// mu protects clients. The field needs protection since it is updated by upload
	// whenever new clients join, and iterated on by updateAndNotify in the end of each upload.
	// It does NOT protect data or error, because they do not need protection -
	// they are only modified when a state object is created, and by updateAndNotify which is called
	// exactly once for a given state object (this is the whole point of the algorithm).
	mu      sync.Mutex
	clients []chan<- *uploadResponse
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
	ticker := time.NewTicker(50 * time.Millisecond)
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
			buffer = append(buffer, req)
			if len(buffer) >= casChanBufferSize {
				c.upload(buffer)
				buffer = nil
			}
		case <-ticker.C:
			if buffer != nil {
				c.upload(buffer)
				buffer = nil
			}
		}
	}
}

func updateAndNotify(st *uploadState, err error, missing bool) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.err = err
	for i, cl := range st.clients {
		cl <- &uploadResponse{
			digest:  st.ue.Digest(),
			missing: i == 0 && missing, // Only first client is reported the digest missing.
			err:     err,
		}
	}
	st.clients = nil
	st.ue = nil
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
	var tnKeys, aiKeys, iiKeys []string
	for toolName := range toolNames {
		tnKeys = append(tnKeys, toolName)
	}
	for actionID := range actionIDs {
		aiKeys = append(aiKeys, actionID)
	}
	for invocationID := range invocationIDs {
		iiKeys = append(iiKeys, invocationID)
	}
	sort.Strings(tnKeys)
	sort.Strings(aiKeys)
	sort.Strings(iiKeys)
	return &requestMetadata{
		toolName:     strings.Join(tnKeys, ","),
		actionID:     strings.Join(aiKeys, ","),
		invocationID: strings.Join(iiKeys, ","),
	}
}

func (c *Client) upload(reqs []*uploadRequest) {
	// Collect new uploads.
	newStates := make(map[digest.Digest]*uploadState)
	var newUploads []digest.Digest
	var metas []*requestMetadata
	for _, req := range reqs {
		dg := req.ue.Digest()
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
			updateAndNotify(st, err, false)
		}
		return
	}
	missing, present, err := c.findBlobState(ctx, newUploads)
	if err != nil {
		for _, st := range newStates {
			updateAndNotify(st, err, false)
		}
		return
	}
	for _, dg := range present {
		updateAndNotify(newStates[dg], nil, false)
	}

	log.V(2).Infof("%d new items to store", len(missing))
	var batches [][]digest.Digest
	if c.useBatchOps {
		batches = c.makeBatches(missing, true)
	} else {
		log.V(2).Info("Uploading them individually")
		for i := range missing {
			log.V(3).Infof("Creating single batch of blob %s", missing[i])
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
				log.V(2).Infof("%d batches left to store", len(batches)-i)
			}
			if len(batch) > 1 {
				log.V(3).Infof("Uploading batch of %d blobs", len(batch))
				bchMap := make(map[digest.Digest][]byte)
				for _, dg := range batch {
					st := newStates[dg]
					ch, err := chunker.NewFromUEntry(st.ue, false, int(c.ChunkMaxSize))
					if err != nil {
						updateAndNotify(st, err, true)
						continue
					}
					data, err := ch.FullData()
					if err != nil {
						updateAndNotify(st, err, true)
						continue
					}
					bchMap[dg] = data
				}
				err := c.BatchWriteBlobs(ctx, bchMap)
				for dg := range bchMap {
					updateAndNotify(newStates[dg], err, true)
				}
			} else {
				log.V(3).Infof("Uploading single blob with digest %s", batch[0])
				st := newStates[batch[0]]
				ch, err := chunker.NewFromUEntry(st.ue, false, int(c.ChunkMaxSize))
				if err != nil {
					updateAndNotify(st, err, true)
				}
				dg := st.ue.Digest()
				err = c.WriteChunked(ctx, c.ResourceNameWrite(dg.Hash, dg.Size), ch)
				updateAndNotify(st, err, true)
			}
		}()
	}
}

// This function is only used when UnifiedCASOps is false. It will be removed
// once UnifiedCASOps=true is stable.
func (c *Client) uploadNonUnified(ctx context.Context, data ...*chunker.UploadEntry) ([]digest.Digest, error) {
	var dgs []digest.Digest
	chunkers := make(map[digest.Digest]*chunker.Chunker)
	for _, ue := range data {
		dg := ue.Digest()
		if _, ok := chunkers[dg]; !ok {
			dgs = append(dgs, dg)
			ch, err := chunker.NewFromUEntry(ue, false, int(c.ChunkMaxSize))
			if err != nil {
				return nil, err
			}
			chunkers[dg] = ch
		}
	}

	missing, err := c.MissingBlobs(ctx, dgs)
	if err != nil {
		return nil, err
	}
	log.V(2).Infof("%d items to store", len(missing))
	var batches [][]digest.Digest
	if c.useBatchOps {
		batches = c.makeBatches(missing, true)
	} else {
		log.V(2).Info("Uploading them individually")
		for i := range missing {
			log.V(3).Infof("Creating single batch of blob %s", missing[i])
			batches = append(batches, missing[i:i+1])
		}
	}

	eg, eCtx := errgroup.WithContext(ctx)
	for i, batch := range batches {
		i, batch := i, batch // https://golang.org/doc/faq#closures_and_goroutines
		eg.Go(func() error {
			if err := c.casUploaders.Acquire(eCtx, 1); err != nil {
				return err
			}
			defer c.casUploaders.Release(1)
			if i%logInterval == 0 {
				log.V(2).Infof("%d batches left to store", len(batches)-i)
			}
			if len(batch) > 1 {
				log.V(3).Infof("Uploading batch of %d blobs", len(batch))
				bchMap := make(map[digest.Digest][]byte)
				for _, dg := range batch {
					data, err := chunkers[dg].FullData()
					if err != nil {
						return err
					}
					bchMap[dg] = data
				}
				if err := c.BatchWriteBlobs(eCtx, bchMap); err != nil {
					return err
				}
			} else {
				log.V(3).Infof("Uploading single blob with digest %s", batch[0])
				ch := chunkers[batch[0]]
				dg := batch[0]
				if err := c.WriteChunked(eCtx, c.ResourceNameWrite(dg.Hash, dg.Size), ch); err != nil {
					return err
				}
			}
			if eCtx.Err() != nil {
				return eCtx.Err()
			}
			return nil
		})
	}

	log.V(2).Info("Waiting for remaining jobs")
	err = eg.Wait()
	log.V(2).Info("Done")

	return missing, err
}

// UploadIfMissing stores a number of uploadable items.
// It first queries the CAS to see which items are missing and only uploads those that are.
// Returns a slice of the missing digests.
func (c *Client) UploadIfMissing(ctx context.Context, data ...*chunker.UploadEntry) ([]digest.Digest, error) {
	if !c.UnifiedCASOps {
		return c.uploadNonUnified(ctx, data...)
	}
	uploads := len(data)
	log.V(2).Infof("Request to upload %d blobs", uploads)

	if uploads == 0 {
		return nil, nil
	}
	toolName, actionID, invocationID, err := GetContextMetadata(ctx)
	if err != nil {
		return nil, err
	}
	meta := &requestMetadata{
		toolName:     toolName,
		actionID:     actionID,
		invocationID: invocationID,
	}
	wait := make(chan *uploadResponse, uploads)
	var missing []digest.Digest
	for _, ue := range data {
		req := &uploadRequest{
			ue:   ue,
			meta: meta,
			wait: wait,
		}
		select {
		case <-ctx.Done():
			log.V(2).Infof("Upload canceled")
			return nil, ctx.Err()
		case c.casUploadRequests <- req:
			continue
		}
	}
	for uploads > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case resp := <-wait:
			if resp.err != nil {
				return nil, resp.err
			}
			if resp.missing {
				missing = append(missing, resp.digest)
			}
			uploads--
		}
	}
	return missing, nil
}

// WriteBlobs stores a large number of blobs from a digest-to-blob map. It's intended for use on the
// result of PackageTree. Unlike with the single-item functions, it first queries the CAS to
// see which blobs are missing and only uploads those that are.
// TODO(olaola): rethink the API of this layer:
// * Do we want to allow []byte uploads, or require the user to construct Chunkers?
// * How to consistently distinguish in the API between should we use GetMissing or not?
// * Should BatchWrite be a public method at all?
func (c *Client) WriteBlobs(ctx context.Context, blobs map[digest.Digest][]byte) error {
	var uEntries []*chunker.UploadEntry
	for _, blob := range blobs {
		uEntries = append(uEntries, chunker.EntryFromBlob(blob))
	}
	_, err := c.UploadIfMissing(ctx, uEntries...)
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
	ue := chunker.EntryFromBlob(blob)
	dg := ue.Digest()
	ch, err := chunker.NewFromUEntry(ue, false, int(c.ChunkMaxSize))
	if err != nil {
		return dg, err
	}
	return dg, c.WriteChunked(ctx, c.ResourceNameWrite(dg.Hash, dg.Size), ch)
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
func (c *Client) makeBatches(dgs []digest.Digest, optimizeSize bool) [][]digest.Digest {
	var batches [][]digest.Digest
	log.V(2).Infof("Batching %d digests", len(dgs))
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
		log.V(3).Infof("Created batch of %d blobs with total size %d", len(batch), sz)
		batches = append(batches, batch)
	}
	log.V(2).Infof("%d batches created", len(batches))
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
func (c *Client) ReadBlob(ctx context.Context, d digest.Digest) ([]byte, error) {
	return c.readBlob(ctx, d.Hash, d.Size, 0, 0)
}

// ReadBlobRange fetches a partial blob from the CAS into a byte slice, starting from offset bytes
// and including at most limit bytes (or no limit if limit==0). The offset must be non-negative and
// no greater than the size of the entire blob. The limit must not be negative, but offset+limit may
// be greater than the size of the entire blob.
func (c *Client) ReadBlobRange(ctx context.Context, d digest.Digest, offset, limit int64) ([]byte, error) {
	return c.readBlob(ctx, d.Hash, d.Size, offset, limit)
}

func (c *Client) readBlob(ctx context.Context, hash string, sizeBytes, offset, limit int64) ([]byte, error) {
	// int might be 32-bit, in which case we could have a blob whose size is representable in int64
	// but not int32, and thus can't fit in a slice. We can check for this by casting and seeing if
	// the result is negative, since 32 bits is big enough wrap all out-of-range values of int64 to
	// negative numbers. If int is 64-bits, the cast is a no-op and so the condition will always fail.
	if int(sizeBytes) < 0 {
		return nil, fmt.Errorf("digest size %d is too big to fit in a byte slice", sizeBytes)
	}
	if offset > sizeBytes {
		return nil, fmt.Errorf("offset %d out of range for a blob of size %d", offset, sizeBytes)
	}
	if offset < 0 {
		return nil, fmt.Errorf("offset %d may not be negative", offset)
	}
	if limit < 0 {
		return nil, fmt.Errorf("limit %d may not be negative", limit)
	}
	sz := sizeBytes - offset
	if limit > 0 && limit < sz {
		sz = limit
	}
	sz += bytes.MinRead // Pad size so bytes.Buffer does not reallocate.
	buf := bytes.NewBuffer(make([]byte, 0, sz))
	_, err := c.readBlobStreamed(ctx, hash, sizeBytes, offset, limit, buf)
	return buf.Bytes(), err
}

// ReadBlobToFile fetches a blob with a provided digest name from the CAS, saving it into a file.
// It returns the number of bytes read.
func (c *Client) ReadBlobToFile(ctx context.Context, d digest.Digest, fpath string) (int64, error) {
	return c.readBlobToFile(ctx, d.Hash, d.Size, fpath)
}

func (c *Client) readBlobToFile(ctx context.Context, hash string, sizeBytes int64, fpath string) (int64, error) {
	n, err := c.readToFile(ctx, c.resourceNameRead(hash, sizeBytes), fpath)
	if err != nil {
		return n, err
	}
	if n != sizeBytes {
		return n, fmt.Errorf("CAS fetch read %d bytes but %d were expected", n, sizeBytes)
	}
	return n, nil
}

// ReadBlobStreamed fetches a blob with a provided digest from the CAS.
// It streams into an io.Writer, and returns the number of bytes read.
func (c *Client) ReadBlobStreamed(ctx context.Context, d digest.Digest, w io.Writer) (int64, error) {
	return c.readBlobStreamed(ctx, d.Hash, d.Size, 0, 0, w)
}

func (c *Client) readBlobStreamed(ctx context.Context, hash string, sizeBytes, offset, limit int64, w io.Writer) (int64, error) {
	if sizeBytes == 0 {
		// Do not download empty blobs.
		return 0, nil
	}
	n, err := c.readStreamed(ctx, c.resourceNameRead(hash, sizeBytes), offset, limit, w)
	if err != nil {
		return n, err
	}
	sz := sizeBytes - offset
	if limit > 0 && limit < sz {
		sz = limit
	}
	if n != sz {
		return n, fmt.Errorf("CAS fetch read %d bytes but %d were expected", n, sz)
	}
	return n, nil
}

// ReadProto reads a blob from the CAS and unmarshals it into the given message.
func (c *Client) ReadProto(ctx context.Context, d digest.Digest, msg proto.Message) error {
	bytes, err := c.ReadBlob(ctx, d)
	if err != nil {
		return err
	}
	return proto.Unmarshal(bytes, msg)
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
		log.V(3).Infof("Created query batch of %d blobs", len(batch))
		batches = append(batches, batch)
	}
	log.V(3).Infof("%d query batches created", len(batches))

	eg, eCtx := errgroup.WithContext(ctx)
	for i, batch := range batches {
		i, batch := i, batch // https://golang.org/doc/faq#closures_and_goroutines
		eg.Go(func() error {
			if err := c.casUploaders.Acquire(eCtx, 1); err != nil {
				return err
			}
			defer c.casUploaders.Release(1)
			if i%logInterval == 0 {
				log.V(3).Infof("%d missing batches left to query", len(batches)-i)
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
	log.V(3).Info("Waiting for remaining query jobs")
	err := eg.Wait()
	log.V(3).Info("Done")
	return missing, err
}

func (c *Client) resourceNameRead(hash string, sizeBytes int64) string {
	return fmt.Sprintf("%s/blobs/%s/%d", c.InstanceName, hash, sizeBytes)
}

// ResourceNameWrite generates a valid write resource name.
func (c *Client) ResourceNameWrite(hash string, sizeBytes int64) string {
	return fmt.Sprintf("%s/uploads/%s/blobs/%s/%d", c.InstanceName, uuid.New(), hash, sizeBytes)
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
		if err := c.ReadProto(ctx, digest.NewFromProtoUnvalidated(dir.TreeDigest), t); err != nil {
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
func (c *Client) DownloadDirectory(ctx context.Context, d digest.Digest, execRoot string, cache filemetadata.Cache) (map[string]*TreeOutput, error) {
	dir := &repb.Directory{}
	if err := c.ReadProto(ctx, d, dir); err != nil {
		return nil, fmt.Errorf("digest %v cannot be mapped to a directory proto: %v", d, err)
	}

	dirs, err := c.GetDirectoryTree(ctx, d.ToProto())
	if err != nil {
		return nil, err
	}

	outputs, err := c.FlattenTree(&repb.Tree{
		Root:     dir,
		Children: dirs,
	}, "")
	if err != nil {
		return nil, err
	}

	return outputs, c.downloadOutputs(ctx, outputs, execRoot, cache)
}

// DownloadActionOutputs downloads the output files and directories in the given action result.
func (c *Client) DownloadActionOutputs(ctx context.Context, resPb *repb.ActionResult, execRoot string, cache filemetadata.Cache) error {
	outs, err := c.FlattenActionOutputs(ctx, resPb)
	if err != nil {
		return err
	}
	// Remove the existing output directories before downloading.
	for _, dir := range resPb.OutputDirectories {
		if err := os.RemoveAll(filepath.Join(execRoot, dir.Path)); err != nil {
			return err
		}
	}
	return c.downloadOutputs(ctx, outs, execRoot, cache)
}

func (c *Client) downloadOutputs(ctx context.Context, outs map[string]*TreeOutput, execRoot string, cache filemetadata.Cache) error {
	var symlinks, copies []*TreeOutput
	downloads := make(map[digest.Digest]*TreeOutput)
	for _, out := range outs {
		path := filepath.Join(execRoot, out.Path)
		if out.IsEmptyDirectory {
			if err := os.MkdirAll(path, c.DirMode); err != nil {
				return err
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(path), c.DirMode); err != nil {
			return err
		}
		// We create the symbolic links after all regular downloads are finished, because dangling
		// links will not work.
		if out.SymlinkTarget != "" {
			symlinks = append(symlinks, out)
			continue
		}
		if _, ok := downloads[out.Digest]; ok {
			copies = append(copies, out)
		} else {
			downloads[out.Digest] = out
		}
	}
	if err := c.DownloadFiles(ctx, execRoot, downloads); err != nil {
		return err
	}
	for _, output := range downloads {
		path := output.Path
		md := &filemetadata.Metadata{
			Digest:       output.Digest,
			IsExecutable: output.IsExecutable,
		}
		if err := cache.Update(path, md); err != nil {
			return err
		}
	}
	for _, out := range copies {
		perm := c.RegularMode
		if out.IsExecutable {
			perm = c.ExecutableMode
		}
		src := downloads[out.Digest]
		if src.IsEmptyDirectory {
			return fmt.Errorf("unexpected empty directory: %s", src.Path)
		}
		if err := copyFile(execRoot, execRoot, src.Path, out.Path, perm); err != nil {
			return err
		}
	}
	for _, out := range symlinks {
		if err := os.Symlink(out.SymlinkTarget, filepath.Join(execRoot, out.Path)); err != nil {
			return err
		}
	}
	return nil
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
	output   *TreeOutput
	meta     *requestMetadata
	wait     chan<- error
}

func (c *Client) downloadProcessor() {
	var buffer []*downloadRequest
	ticker := time.NewTicker(50 * time.Millisecond)
	for {
		select {
		case ch, ok := <-c.casDownloadRequests:
			if !ok {
				// Client is exiting. Notify remaining downloads to prevent deadlocks.
				ticker.Stop()
				if buffer != nil {
					for _, r := range buffer {
						r.wait <- context.Canceled
					}
				}
				return
			}
			buffer = append(buffer, ch)
			if len(buffer) >= casChanBufferSize {
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

func afterDownload(batch []digest.Digest, reqs map[digest.Digest][]*downloadRequest, err error) {
	if err != nil {
		log.Errorf("Error downloading %v: %v", batch[0], err)
	}
	for _, dg := range batch {
		rs, ok := reqs[dg]
		if !ok {
			log.Errorf("Precondition failed: download request not found in input %v.", dg)
		}
		for _, r := range rs {
			r.wait <- err
		}
	}
}

func (c *Client) downloadBatch(ctx context.Context, batch []digest.Digest, reqs map[digest.Digest][]*downloadRequest) {
	log.V(3).Infof("Downloading batch of %d files", len(batch))
	bchMap, err := c.BatchDownloadBlobs(ctx, batch)
	if err != nil {
		afterDownload(batch, reqs, err)
		return
	}
	for dg, data := range bchMap {
		for _, r := range reqs[dg] {
			perm := c.RegularMode
			if r.output.IsExecutable {
				perm = c.ExecutableMode
			}
			r.wait <- ioutil.WriteFile(filepath.Join(r.execRoot, r.output.Path), data, perm)
		}
	}
}

func (c *Client) downloadSingle(ctx context.Context, dg digest.Digest, reqs map[digest.Digest][]*downloadRequest) (err error) {
	// The lock is released when all file copies are finished.
	// We cannot release the lock after each individual file copy, because
	// the caller might move the file, and we don't have the contents in memory.
	defer func() { afterDownload([]digest.Digest{dg}, reqs, err) }()
	rs := reqs[dg]
	if len(rs) < 1 {
		return fmt.Errorf("Failed precondition: cannot find %v in reqs map", dg)
	}
	r := rs[0]
	rs = rs[1:]
	path := filepath.Join(r.execRoot, r.output.Path)
	log.V(3).Infof("Downloading single file with digest %s to %s", r.output.Digest, path)
	if _, err := c.ReadBlobToFile(ctx, r.output.Digest, path); err != nil {
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
	log.V(2).Infof("%d digests to download (%d reqs)", len(dgs), len(reqs))
	var batches [][]digest.Digest
	if c.useBatchOps {
		batches = c.makeBatches(dgs, !bool(c.UtilizeLocality))
	} else {
		log.V(2).Info("Downloading them individually")
		for i := range dgs {
			log.V(3).Infof("Creating single batch of blob %s", dgs[i])
			batches = append(batches, dgs[i:i+1])
		}
	}

	unifiedMeta := getUnifiedMetadata(metas)
	var err error
	ctx := context.Background()
	if unifiedMeta.actionID != "" {
		ctx, err = ContextWithMetadata(context.Background(), unifiedMeta.toolName, unifiedMeta.actionID, unifiedMeta.invocationID)
	}
	if err != nil {
		afterDownload(dgs, reqs, err)
		return
	}
	for i, batch := range batches {
		i, batch := i, batch // https://golang.org/doc/faq#closures_and_goroutines
		go func() {
			if c.casDownloaders.Acquire(ctx, 1) == nil {
				defer c.casDownloaders.Release(1)
			}
			if i%logInterval == 0 {
				log.V(2).Infof("%d batches left to download", len(batches)-i)
			}
			if len(batch) > 1 {
				c.downloadBatch(ctx, batch, reqs)
			} else {
				c.downloadSingle(ctx, batch[0], reqs)
			}
		}()
	}
}

// This is a legacy function used only when UnifiedCASOps=false.
// It will be removed when UnifiedCASOps=true is stable.
func (c *Client) downloadNonUnified(ctx context.Context, execRoot string, outputs map[digest.Digest]*TreeOutput) error {
	var dgs []digest.Digest

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
		}
	} else {
		for dg := range outputs {
			dgs = append(dgs, dg)
		}
	}

	log.V(2).Infof("%d items to download", len(dgs))
	var batches [][]digest.Digest
	if c.useBatchOps {
		batches = c.makeBatches(dgs, !bool(c.UtilizeLocality))
	} else {
		log.V(2).Info("Downloading them individually")
		for i := range dgs {
			log.V(3).Infof("Creating single batch of blob %s", dgs[i])
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
				log.V(2).Infof("%d batches left to download", len(batches)-i)
			}
			if len(batch) > 1 {
				log.V(3).Infof("Downloading batch of %d files", len(batch))
				bchMap, err := c.BatchDownloadBlobs(eCtx, batch)
				if err != nil {
					return err
				}
				for dg, data := range bchMap {
					out := outputs[dg]
					perm := c.RegularMode
					if out.IsExecutable {
						perm = c.ExecutableMode
					}
					if err := ioutil.WriteFile(filepath.Join(execRoot, out.Path), data, perm); err != nil {
						return err
					}
				}
			} else {
				out := outputs[batch[0]]
				path := filepath.Join(execRoot, out.Path)
				log.V(3).Infof("Downloading single file with digest %s to %s", out.Digest, path)
				if _, err := c.ReadBlobToFile(ctx, out.Digest, path); err != nil {
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

	log.V(3).Info("Waiting for remaining jobs")
	err := eg.Wait()
	log.V(3).Info("Done")
	return err
}

// DownloadFiles downloads the output files under |execRoot|.
func (c *Client) DownloadFiles(ctx context.Context, execRoot string, outputs map[digest.Digest]*TreeOutput) error {
	if !c.UnifiedCASOps {
		return c.downloadNonUnified(ctx, execRoot, outputs)
	}
	count := len(outputs)
	if count == 0 {
		return nil
	}
	toolName, actionID, invocationID, err := GetContextMetadata(ctx)
	if err != nil {
		return err
	}
	meta := &requestMetadata{
		toolName:     toolName,
		actionID:     actionID,
		invocationID: invocationID,
	}
	wait := make(chan error, count)
	for dg, out := range outputs {
		r := &downloadRequest{
			digest:   dg,
			execRoot: execRoot,
			output:   out,
			meta:     meta,
			wait:     wait,
		}
		select {
		case <-ctx.Done():
			log.V(2).Infof("Download canceled")
			return ctx.Err()
		case c.casDownloadRequests <- r:
			continue
		}
	}

	// Wait for all downloads to finish.
	for count > 0 {
		select {
		case <-ctx.Done():
			log.V(2).Infof("Download canceled")
			return ctx.Err()
		case err := <-wait:
			if err != nil {
				return err
			}
			count--
		}
	}
	return nil
}
