package casng

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/contextmd"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/errors"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/impath"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/walker"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"
	"github.com/pborman/uuid"
	"google.golang.org/grpc/status"

	slo "github.com/bazelbuild/remote-apis-sdks/go/pkg/symlinkopts"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	log "github.com/golang/glog"
)

// UploadRequest represents a path to start uploading from.
//
// If the path is a directory, its entire tree is traversed and only files that are not excluded by the filter are uploaded.
// Symlinks are handled according to the SymlinkOptions field.
type UploadRequest struct {
	// Digest is for pre-digested requests. This digest is trusted to be the one for the associated Bytes or Path.
	//
	// If not set, it will be calculated.
	// If set, it implies that this request is a single blob. I.e. either Bytes is set or Path is a regular file and both SymlinkOptions and Exclude are ignored.
	Digest digest.Digest

	// Bytes is meant for small blobs. Using a large slice of bytes might cause memory thrashing.
	//
	// If Bytes is nil, BytesFileMode is ignored and Path is used for traversal.
	// If Bytes is not nil (may be empty), Path is used as the corresponding path for the bytes content and is not used for traversal.
	Bytes []byte

	// BytesFileMode describes the bytes content. It is ignored if Bytes is not set.
	BytesFileMode fs.FileMode

	// Path is used to access and read files if Bytes is nil. Otherwise, Bytes is assumed to be the paths content (even if empty).
	//
	// This must not be equal to impath.Root since this is considered a zero value (Path not set).
	// If Bytes is not nil and Path is not set, a node cannot be constructed and therefore no node is cached.
	Path impath.Absolute

	// SymlinkOptions are used to handle symlinks when Path is set and Bytes is not.
	SymlinkOptions slo.Options

	// Exclude is used to exclude paths during traversal when Path is set and Bytes is not.
	//
	// The filter ID is used in the keys of the node cache, even when Bytes is set.
	// Using the same ID for effectively different filters will cause erroneous cache hits.
	// Using a different ID for effectively identical filters will reduce cache hit rates and increase digestion compute cost.
	Exclude walker.Filter

	// Internal fields.

	// id identifies this request internally for logging purposes.
	id string
	// reader is used to keep a large file open while being handed over between workers.
	reader io.ReadSeekCloser
	// ctx is the requester's context which is used to extract metadata from and abort in-flight tasks for this request.
	ctx context.Context
	// tag identifies the requester of this request.
	tag string
	// done is used internally to signal that no further requests are expected for the associated tag.
	// This allows the processor to notify the client once all buffered requests are processed.
	// Once a tag is associated with done=true, sending subsequent requests for that tag might cause races.
	done bool
	// digsetOnly indicates that this request is for digestion only.
	digestOnly bool
}

// UploadResponse represents an upload result for a single request (which may represent a tree of files).
type UploadResponse struct {
	// Digest identifies the blob associated with this response.
	// May be empty (created from an empty byte slice or from a composite literal), in which case Err is set.
	Digest digest.Digest

	// Stats may be zero if this response has not been updated yet. It should be ignored if Err is set.
	// If this response has been processed, then either CacheHitCount or CacheHitMiss is not zero.
	Stats Stats

	// Err indicates the error encountered while processing the request associated with Digest.
	// If set, Stats should be ignored.
	Err error

	// reqs is used internally to identify the requests that are related to this response.
	reqs []string
	// tags is used internally to identify the clients that are interested in this response.
	tags []string
	// done is used internally to signal that this is the last response for the associated tags.
	done bool
	// endofWalk is used internally to signal that this response includes stats only for the associated tags.
	endOfWalk bool
}

// uploadRequestBundleItem is a tuple of an upload request and a list of clients interested in the response.
type uploadRequestBundleItem struct {
	req  *repb.BatchUpdateBlobsRequest_Request
	tags []string
	reqs []string
}

// uploadRequestBundle is used to aggregate (unify) requests by digest.
type uploadRequestBundle = map[digest.Digest]uploadRequestBundleItem

// Upload is a non-blocking call that uploads incoming files to the CAS if necessary.
//
// To properly stop this call, close in and cancel ctx, then wait for the returned channel to close.
// The channel in must be closed as a termination signal. Cancelling ctx is not enough.
// The uploader's context is used to make remote calls using metadata from ctx.
// Metadata unification assumes all requests share the same correlated invocation ID.
//
// The consumption speed is subject to the concurrency and timeout configurations of the gRPC call.
// All received requests will have corresponding responses sent on the returned channel.
//
// Requests are unified across a window of time defined by the BundleTimeout value of the gRPC configuration.
// The unification is affected by the order of the requests, bundle limits (length, size, timeout) and the upload speed.
// With infinite speed and limits, every blob will be uploaded exactly once. On the other extreme, every blob is uploaded
// alone and no unification takes place.
// In the average case, blobs that make it into the same bundle will be grouped by digest. Once a digest is processed, each requester of that
// digest receives a copy of the coorresponding UploadResponse.
//
// This method must not be called after cancelling the uploader's context.
func (u *StreamingUploader) Upload(ctx context.Context, in <-chan UploadRequest) <-chan UploadResponse {
	return u.streamPipe(ctx, in)
}

// streamPipe is used by both the streaming and the batching interfaces.
// Each request will be enriched with internal fields for control and logging purposes.
func (u *uploader) streamPipe(ctx context.Context, in <-chan UploadRequest) <-chan UploadResponse {
	ch := make(chan UploadResponse)

	// If this was called after the the uploader was terminated, short the circuit and return.
	if u.done {
		go func() {
			defer close(ch)
			r := UploadResponse{Err: ErrTerminatedUploader}
			for range in {
				ch <- r
			}
		}()
		return ch
	}

	// Register a new requester with the internal processor.
	// This broker should not remove the subscription until the sender tells it to.
	tag, resChan := u.uploadPubSub.sub()

	// Forward the requests to the internal processor.
	u.uploadSenderWg.Add(1)
	go func() {
		contextmd.Infof(ctx, log.Level(1), "[casng] upload.stream_pipe.sender.start; tag=%s", tag)
		defer contextmd.Infof(ctx, log.Level(1), "[casng] upload.stream_pipe.sender.stop; tag=%s", tag)
		defer u.uploadSenderWg.Done()
		for r := range in {
			r.tag = tag
			r.ctx = ctx
			r.id = uuid.New()
			u.digesterCh <- r
		}
		// Let the processor know that no further requests are expected.
		u.digesterCh <- UploadRequest{tag: tag, done: true}
	}()

	// Receive responses from the internal processor.
	// Once the sender above sends a done-tagged request, the processor will send a done-tagged response.
	u.receiverWg.Add(1)
	go func() {
		contextmd.Infof(ctx, log.Level(1), "[casng] upload.stream_pipe.receiver.start; tag=%s", tag)
		defer contextmd.Infof(ctx, log.Level(1), "[casng] upload.stream_pipe.receiver.stop; tag=%s", tag)
		defer u.receiverWg.Done()
		defer close(ch)
		for rawR := range resChan {
			r := rawR.(UploadResponse)
			if r.done {
				u.uploadPubSub.unsub(tag)
				continue
			}
			ch <- r
		}
	}()

	return ch
}

// uploadBatcher handles files that can fit into a batching request.
func (u *uploader) batcher(ctx context.Context) {
	log.V(1).Info("[casng] upload.batcher.start")
	defer log.V(1).Info("[casng] uploader.batcher.stop")

	bundle := make(uploadRequestBundle)
	bundleSize := u.uploadRequestBaseSize
	bundleCtx := ctx // context with unified metadata.

	// handle is a closure that shares read/write access to the bundle variables with its parent.
	// This allows resetting the bundle and associated variables in one call rather than having to repeat the reset
	// code after every call to this function.
	handle := func() {
		if len(bundle) < 1 {
			return
		}
		// Block the batcher if the concurrency limit is reached.
		startTime := time.Now()
		if !u.uploadThrottler.acquire(ctx) {
			// Ensure responses are dispatched before aborting.
			for d, item := range bundle {
				u.dispatcherResCh <- UploadResponse{
					Digest: d,
					Stats:  Stats{BytesRequested: d.Size},
					Err:    context.Canceled,
					tags:   item.tags,
					reqs:   item.reqs,
				}
			}
			return
		}
		log.V(3).Infof("[casng] upload.batcher.throttle.duration; start=%d, end=%d", startTime.UnixNano(), time.Now().UnixNano())

		u.workerWg.Add(1)
		go func(ctx context.Context, b uploadRequestBundle) {
			defer u.workerWg.Done()
			defer u.uploadThrottler.release()
			// TODO: cancel ctx if all requesters have cancelled their contexts.
			u.callBatchUpload(ctx, b)
		}(bundleCtx, bundle)

		bundle = make(uploadRequestBundle)
		bundleSize = u.uploadRequestBaseSize
		bundleCtx = ctx
	}

	bundleTicker := time.NewTicker(u.batchRPCCfg.BundleTimeout)
	defer bundleTicker.Stop()
	for {
		select {
		// The dispatcher guarantees that the dispatched blob is not oversized.
		case req, ok := <-u.batcherCh:
			if !ok {
				return
			}
			log.V(3).Infof("[casng] upload.batcher.req; digest=%s, req=%s, tag=%s", req.Digest, req.id, req.tag)

			// Unify.
			item, ok := bundle[req.Digest]
			if ok {
				// Duplicate tags are allowed to ensure the requester can match the number of responses to the number of requests.
				item.tags = append(item.tags, req.tag)
				item.reqs = append(item.reqs, req.id)
				bundle[req.Digest] = item
				log.V(3).Infof("[casng] upload.batcher.unified; digest=%s, bundle=%d, req=%s, tag=%s", req.Digest, len(item.tags), req.id, req.tag)
				continue
			}

			// It's possible for files to be considered medium and large, but still fit into a batch request.
			// Load the bytes without blocking the batcher by deferring the blob.
			if len(req.Bytes) == 0 {
				log.V(3).Infof("[casng] upload.batcher.file; digest=%s, path=%s, req=%s, tag=%s", req.Digest, req.Path, req.id, req.tag)
				u.workerWg.Add(1)
				go func(req UploadRequest) (err error) {
					defer u.workerWg.Done()
					defer func() {
						if err != nil {
							u.dispatcherResCh <- UploadResponse{
								Digest: req.Digest,
								Err:    err,
								tags:   []string{req.tag},
								reqs:   []string{req.id},
							}
							return
						}
						// Send after releasing resources.
						u.batcherCh <- req
					}()
					r := req.reader
					if r == nil {
						startTime := time.Now()
						if !u.ioThrottler.acquire(req.ctx) {
							return req.ctx.Err()
						}
						log.V(3).Infof("[casng] upload.batcher.io_throttle.duration; start=%d, end=%d, req=%s, tag=%s", startTime.UnixNano(), time.Now().UnixNano(), req.id, req.tag)
						defer u.ioThrottler.release()
						f, err := os.Open(req.Path.String())
						if err != nil {
							return errors.Join(ErrIO, err)
						}
						r = f
					} else {
						// If this blob was from a large file, ensure IO holds are released.
						defer u.releaseIOTokens()
					}
					defer func() {
						if errClose := r.Close(); err != nil {
							err = errors.Join(ErrIO, errClose, err)
						}
					}()
					bytes, err := io.ReadAll(r)
					if err != nil {
						return errors.Join(ErrIO, err)
					}
					req.Bytes = bytes
					return nil
				}(req)
				continue
			}

			// If the blob doesn't fit in the current bundle, cycle it.
			rSize := u.uploadRequestItemBaseSize + len(req.Bytes)
			if bundleSize+rSize >= u.batchRPCCfg.BytesLimit {
				log.V(3).Infof("[casng] upload.batcher.bundle.size; bytes=%d, excess=%d", bundleSize, rSize)
				handle()
			}

			item.tags = append(item.tags, req.tag)
			item.req = &repb.BatchUpdateBlobsRequest_Request{
				Digest: req.Digest.ToProto(),
				Data:   req.Bytes, // TODO: add compression support as in https://github.com/bazelbuild/remote-apis-sdks/pull/443/files
			}
			bundle[req.Digest] = item
			bundleSize += rSize
			bundleCtx, _ = contextmd.FromContexts(bundleCtx, req.ctx) // ignore non-essential error.

			// If the bundle is full, cycle it.
			if len(bundle) >= u.batchRPCCfg.ItemsLimit {
				log.V(3).Infof("[casng] upload.batcher.bundle.full; count=%d", len(bundle))
				handle()
			}
		case <-bundleTicker.C:
			if len(bundle) > 0 {
				log.V(3).Infof("[casng] upload.batcher.bundle.timeout; count=%d", len(bundle))
			}
			handle()
		}
	}
}

func (u *uploader) callBatchUpload(ctx context.Context, bundle uploadRequestBundle) {
	req := &repb.BatchUpdateBlobsRequest{InstanceName: u.instanceName}
	req.Requests = make([]*repb.BatchUpdateBlobsRequest_Request, 0, len(bundle))
	for _, item := range bundle {
		req.Requests = append(req.Requests, item.req)
	}

	var uploaded []digest.Digest
	failed := make(map[digest.Digest]error)
	digestRetryCount := make(map[digest.Digest]int64)

	startTime := time.Now()
	err := retry.WithPolicy(ctx, u.batchRPCCfg.RetryPredicate, u.batchRPCCfg.RetryPolicy, func() error {
		// This call can have partial failures. Only retry retryable failed requests.
		ctx, ctxCancel := context.WithTimeout(ctx, u.batchRPCCfg.Timeout)
		defer ctxCancel()
		res, errCall := u.cas.BatchUpdateBlobs(ctx, req)
		reqErr := errCall // return this error if nothing is retryable.
		req.Requests = nil
		for _, r := range res.Responses {
			if errItem := status.FromProto(r.Status).Err(); errItem != nil {
				if retry.TransientOnly(errItem) {
					d := digest.NewFromProtoUnvalidated(r.Digest)
					req.Requests = append(req.Requests, bundle[d].req)
					digestRetryCount[d]++
					reqErr = errItem // return any retryable error if there is one.
					continue
				}
				// Permanent error.
				failed[digest.NewFromProtoUnvalidated(r.Digest)] = errItem
				continue
			}
			uploaded = append(uploaded, digest.NewFromProtoUnvalidated(r.Digest))
		}
		if l := len(req.Requests); l > 0 {
			log.V(3).Infof("[casng] upload.batcher.call.retry; len=%d", l)
		}
		return reqErr
	})
	log.V(3).Infof("[casng] upload.batcher.grpc.duration; start=%d, end=%d", startTime.UnixNano(), time.Now().UnixNano())
	log.V(3).Infof("[casng] upload.batcher.call.result; uploaded=%d, failed=%d, req_failed=%d", len(uploaded), len(failed), len(bundle)-len(uploaded)-len(failed))

	// Report uploaded.
	for _, d := range uploaded {
		s := Stats{
			BytesRequested:      d.Size,
			LogicalBytesMoved:   d.Size,
			TotalBytesMoved:     d.Size,
			EffectiveBytesMoved: d.Size,
			LogicalBytesBatched: d.Size,
			CacheMissCount:      1,
			BatchedCount:        1,
		}
		if r := digestRetryCount[d]; r > 0 {
			s.TotalBytesMoved = d.Size * (r + 1)
		}
		u.dispatcherResCh <- UploadResponse{
			Digest: d,
			Stats:  s,
			tags:   bundle[d].tags,
			reqs:   bundle[d].reqs,
		}
		if log.V(3) {
			log.Infof("[casng] upload.batcher.res.uploaded; digest=%s, req=%s, tag=%s", d, strings.Join(bundle[d].reqs, "|"), strings.Join(bundle[d].tags, "|"))
		}
		delete(bundle, d)
	}

	// Report individually failed requests.
	for d, dErr := range failed {
		s := Stats{
			BytesRequested:    d.Size,
			LogicalBytesMoved: d.Size,
			TotalBytesMoved:   d.Size,
			CacheMissCount:    1,
			BatchedCount:      1,
		}
		if r := digestRetryCount[d]; r > 0 {
			s.TotalBytesMoved = d.Size * (r + 1)
		}
		u.dispatcherResCh <- UploadResponse{
			Digest: d,
			Stats:  s,
			Err:    errors.Join(ErrGRPC, dErr),
			tags:   bundle[d].tags,
			reqs:   bundle[d].reqs,
		}
		if log.V(3) {
			log.Infof("[casng] upload.batcher.res.failed; digest=%s, req=%s, tag=%s", d, strings.Join(bundle[d].reqs, "|"), strings.Join(bundle[d].tags, "|"))
		}
		delete(bundle, d)
	}

	if len(bundle) == 0 {
		log.V(3).Infof("[casng] upload.batcher.pub.duration; start=%d, end=%d", startTime.UnixNano(), time.Now().UnixNano())
		return
	}

	if err == nil {
		err = fmt.Errorf("[casng] server did not return a response for %d requests", len(bundle))
	}
	err = errors.Join(ErrGRPC, err)

	// Report failed requests due to call failure.
	for d, item := range bundle {
		s := Stats{
			BytesRequested:  d.Size,
			TotalBytesMoved: d.Size,
			CacheMissCount:  1,
			BatchedCount:    1,
		}
		if r := digestRetryCount[d]; r > 0 {
			s.TotalBytesMoved = d.Size * (r + 1)
		}
		u.dispatcherResCh <- UploadResponse{
			Digest: d,
			Stats:  s,
			Err:    err,
			tags:   item.tags,
			reqs:   item.reqs,
		}
		if log.V(3) {
			log.Infof("[casng] upload.batcher.res.failed.call; digest=%s, req=%s, tag=%s", d, strings.Join(bundle[d].reqs, "|"), strings.Join(bundle[d].tags, "|"))
		}
	}
	log.V(3).Infof("[casng] upload.batcher.pub.duration; start=%d, end=%d", startTime.UnixNano(), time.Now().UnixNano())
}

// streamer handles files that do not fit into a batching request.
// Unlike the batched call, querying the CAS is not required because the API handles this automatically.
// See https://github.com/bazelbuild/remote-apis/blob/0cd22f7b466ced15d7803e8845d08d3e8d2c51bc/build/bazel/remote/execution/v2/remote_execution.proto#L250-L254
// For files above the large threshold, this call assumes the io and large io holds are already acquired and will release them accordingly.
// For other files, only an io hold is acquired and released in this call.
func (u *uploader) streamer(ctx context.Context) {
	log.V(1).Info("[casng] upload.streamer.start")
	defer log.V(1).Info("[casng] upload.streamer.stop")

	// Unify duplicate requests.
	digestTags := make(map[digest.Digest][]string)
	digestReqs := make(map[digest.Digest][]string)
	streamResCh := make(chan UploadResponse)
	pending := 0
	for {
		select {
		// The dispatcher closes this channel when it's done dispatching, which happens after the streamer
		// had sent all pending responses.
		case req, ok := <-u.streamerCh:
			if !ok {
				return
			}
			shouldReleaseIOTokens := req.reader != nil
			log.V(3).Infof("[casng] upload.streamer.req; digest=%s, large=%t, req=%s, tag=%s, pending=%d", req.Digest, shouldReleaseIOTokens, req.id, req.tag, pending)

			digestReqs[req.Digest] = append(digestReqs[req.Digest], req.id)
			tags := digestTags[req.Digest]
			tags = append(tags, req.tag)
			digestTags[req.Digest] = tags
			if len(tags) > 1 {
				// Already in-flight. Release duplicate resources if it's a large file.
				log.V(3).Infof("[casng] upload.streamer.unified; digest=%s, req=%s, tag=%s, bundle=%d", req.Digest, req.id, req.tag, len(tags))
				if shouldReleaseIOTokens {
					u.releaseIOTokens()
				}
				continue
			}

			var name string
			if req.Digest.Size >= u.ioCfg.CompressionSizeThreshold {
				log.V(3).Infof("[casng] upload.streamer.compress; digest=%s, req=%s, tag=%s", req.Digest, req.id, req.tag)
				name = MakeCompressedWriteResourceName(u.instanceName, req.Digest.Hash, req.Digest.Size)
			} else {
				name = MakeWriteResourceName(u.instanceName, req.Digest.Hash, req.Digest.Size)
			}

			pending++
			// Block the streamer if the gRPC call is being throttled.
			startTime := time.Now()
			if !u.streamThrottle.acquire(ctx) {
				if shouldReleaseIOTokens {
					u.releaseIOTokens()
				}
				// Ensure the response is dispatched before aborting.
				u.workerWg.Add(1)
				go func(req UploadRequest) {
					defer u.workerWg.Done()
					streamResCh <- UploadResponse{Digest: req.Digest, Stats: Stats{BytesRequested: req.Digest.Size}, Err: ctx.Err()}
				}(req)
				continue
			}
			log.V(3).Infof("[casng] upload.streamer.throttle.duration; start=%d, end=%d, tag=%s", startTime.UnixNano(), time.Now().UnixNano(), req.tag)
			u.workerWg.Add(1)
			go func(req UploadRequest) {
				defer u.workerWg.Done()
				s, err := u.callStream(req.ctx, name, req)
				// Release before sending on the channel to avoid blocking without actually using the gRPC resources.
				u.streamThrottle.release()
				streamResCh <- UploadResponse{Digest: req.Digest, Stats: s, Err: err}
			}(req)
		case r := <-streamResCh:
			startTime := time.Now()
			r.tags = digestTags[r.Digest]
			delete(digestTags, r.Digest)
			r.reqs = digestReqs[r.Digest]
			delete(digestReqs, r.Digest)
			u.dispatcherResCh <- r
			pending--
			if log.V(3) {
				log.Infof("[casng] upload.streamer.res; digest=%s, req=%s, tag=%s, pending=%d", r.Digest, strings.Join(r.reqs, "|"), strings.Join(r.tags, "|"), pending)
			}
			// Covers waiting on the dispatcher.
			log.V(3).Infof("[casng] upload.streamer.pub.duration; start=%d, end=%d", startTime.UnixNano(), time.Now().UnixNano())
		}
	}
}

func (u *uploader) callStream(ctx context.Context, name string, req UploadRequest) (stats Stats, err error) {
	var reader io.Reader

	// In the off chance that the blob is mis-constructed (more than one content field is set), start
	// with b.reader to ensure any held locks are released.
	switch {
	// Large file.
	case req.reader != nil:
		reader = req.reader
		defer func() {
			if errClose := req.reader.Close(); errClose != nil {
				err = errors.Join(ErrIO, errClose, err)
			}
			// IO holds were acquired during digestion for large files and are expected to be released here.
			u.releaseIOTokens()
		}()

	// Small file, a proto message (node), or an empty file.
	case len(req.Bytes) > 0:
		reader = bytes.NewReader(req.Bytes)

	// Medium file.
	default:
		startTime := time.Now()
		if !u.ioThrottler.acquire(ctx) {
			return
		}
		log.V(3).Infof("[casng] upload.streamer.io_throttle.duration; start=%d, end=%d, req=%s, tag=%s", startTime.UnixNano(), time.Now().UnixNano(), req.id, req.tag)
		defer u.ioThrottler.release()

		f, errOpen := os.Open(req.Path.String())
		if errOpen != nil {
			return Stats{BytesRequested: req.Digest.Size}, errors.Join(ErrIO, errOpen)
		}
		defer func() {
			if errClose := f.Close(); errClose != nil {
				err = errors.Join(ErrIO, errClose, err)
			}
		}()
		reader = f
	}

	return u.writeBytes(ctx, name, reader, req.Digest.Size, 0, true)
}
