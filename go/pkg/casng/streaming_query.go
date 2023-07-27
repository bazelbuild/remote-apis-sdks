package casng

// The query processor provides a streaming interface to query the CAS for digests.
//
// Multiple concurrent clients can use the same uploader instance at the same time.
// The processor bundles requests from multiple concurrent clients to amortize the cost of querying
// the batching API. That is, it attempts to bundle the maximum possible number of digests in a single gRPC call.
//
// This is done using 3 factors: the size (bytes) limit, the items limit, and a time limit.
// If any of these limits is reached, the processor will dispatch the call and start a new bundle.
// This means that a request can be delayed by the processor (not including network and server time) up to the time limit.
// However, in high throughput sessions, the processor will dispatch calls sooner.
//
// To properly manage multiple concurrent clients while providing the bundling behaviour, the processor becomes a serialization point.
// That is, all requests go through a single channel. To minimize blocking and leverage server concurrency, the processor loop
// is optimized for high throughput and it launches gRPC calls concurrently.
// In other words, it's many-one-many relationship, where many clients send to one processor which sends to many workers.
//
// To avoid forcing another serialization point through the processor, each worker notifies relevant clients of the results
// it acquired from the server. In this case, it's a one-many relationship, where one worker sends to many clients.
//
// All in all, the design implements a many-one-many-one-many pipeline.
// Many clients send to one processor, which sends to many workers; each worker sends to many clients.
//
// Each client is provided with a channel they send their requests on. The handler of that channel, marks each request
// with a unique tag and forwards it to the processor.
//
// The processor receives multiple requests, each potentially with a different tag.
// Each worker receives a bundle of requests that may contain multiple tags.
//
// To facilitate the routing between workers and clients, a simple pubsub implementation is used.
// Each instance, a broker, manages routing messages between multiple subscribers (clients) and multiple publishers (workers).
// Each client gets their own channel on which they receive messages marked for them.
// Each publisher specifies which clients the messages should be routed to.
// The broker attempts at-most-once delivery.
//
// The client handler manages the pubsub subscription by waiting until a matching number of responses was received, after which
// it cancels the subscription.

import (
	"context"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/contextmd"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/errors"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	log "github.com/golang/glog"
	"github.com/pborman/uuid"
	"google.golang.org/protobuf/proto"
)

// MissingBlobsResponse represents a query result for a single digest.
//
// If Err is not nil, Missing is false.
type MissingBlobsResponse struct {
	Digest  digest.Digest
	Missing bool
	Err     error
}

// missingBlobRequest associates a digest with its requester's context.
type missingBlobRequest struct {
	digest digest.Digest
	id     string
	tag    string
	ctx    context.Context
}

// MissingBlobs is a non-blocking call that queries the CAS for incoming digests.
//
// This method is useful when digests are calculated and dispatched on the fly.
// For a large list of known digests, consider using the batching uploader.
//
// To properly stop this call, close in and cancel ctx, then wait for the returned channel to close.
// The channel in must be closed as a termination signal. Cancelling ctx is not enough.
// The uploader's context is used to make remote calls using metadata from ctx.
// Metadata unification assumes all requests share the same correlated invocation ID.
//
// The digests are unified (aggregated/bundled) based on ItemsLimit, BytesLimit and BundleTimeout of the gRPC config.
// The returned channel is unbuffered and will be closed after the input channel is closed and all sent requests get their corresponding responses.
// This could indicate completion or cancellation (in case the context was canceled).
// Slow consumption speed on the returned channel affects the consumption speed on in.
//
// This method must not be called after cancelling the uploader's context.
func (u *StreamingUploader) MissingBlobs(ctx context.Context, in <-chan digest.Digest) <-chan MissingBlobsResponse {
	pipeIn := make(chan missingBlobRequest)
	out := u.missingBlobsPipe(pipeIn)
	u.clientSenderWg.Add(1)
	go func() {
		defer u.clientSenderWg.Done()
		defer close(pipeIn)
		for d := range in {
			pipeIn <- missingBlobRequest{digest: d, ctx: ctx, id: uuid.New()}
		}
	}()
	return out
}

// missingBlobsPipe is a shared implementation between batching and streaming interfaces.
func (u *uploader) missingBlobsPipe(in <-chan missingBlobRequest) <-chan MissingBlobsResponse {
	ch := make(chan MissingBlobsResponse)

	// If this was called after the the uploader was terminated, short the circuit and return.
	if u.done {
		go func() {
			defer close(ch)
			res := MissingBlobsResponse{Err: ErrTerminatedUploader}
			for req := range in {
				res.Digest = req.digest
				ch <- res
			}
		}()
		return ch
	}

	tag, resCh := u.queryPubSub.sub()
	pendingCh := make(chan int)

	// Sender. It terminates when in is closed, at which point it sends 0 as a termination signal to the counter.
	u.querySenderWg.Add(1)
	go func() {
		defer u.querySenderWg.Done()

		log.V(1).Info("[casng] query.streamer.sender.start")
		defer log.V(1).Info("[casng] query.streamer.sender.stop")

		for r := range in {
			r.tag = tag
			u.queryCh <- r
			pendingCh <- 1
		}
		pendingCh <- 0
	}()

	// Receiver. It terminates with resCh is closed, at which point it closes the returned channel.
	u.receiverWg.Add(1)
	go func() {
		defer u.receiverWg.Done()
		defer close(ch)

		log.V(1).Info("[casng] query.streamer.receiver.start")
		defer log.V(1).Info("[casng] query.streamer.receiver.stop")

		// Continue to drain until the broker closes the channel.
		for {
			r, ok := <-resCh
			if !ok {
				return
			}
			ch <- r.(MissingBlobsResponse)
			pendingCh <- -1
		}
	}()

	// Counter. It terminates when count hits 0 after receiving a done signal from the sender.
	// Upon termination, it sends a signal to pubsub to terminate the subscription which closes resCh.
	u.workerWg.Add(1)
	go func() {
		defer u.workerWg.Done()
		defer u.queryPubSub.unsub(tag)

		log.V(1).Info("[casng] query.streamer.counter.start")
		defer log.V(1).Info("[casng] query.streamer.counter.stop")

		pending := 0
		done := false
		for x := range pendingCh {
			if x == 0 {
				done = true
			}
			pending += x
			// If the sender is done and all the requests are done, let the receiver and the broker terminate.
			if pending == 0 && done {
				return
			}
		}
	}()

	return ch
}

// digestStrings is used to bundle up (unify) concurrent requests for the same digest from different requesters (tags).
// It's also used to associate digests with request IDs for loggging purposes.
type digestStrings map[digest.Digest][]string

// queryProcessor is the fan-in handler that manages the bundling and dispatching of incoming requests.
func (u *uploader) queryProcessor(ctx context.Context) {
	log.V(1).Info("[casng] query.processor.start")
	defer log.V(1).Info("[casng] query.processor.stop")

	bundle := make(digestStrings)
	reqs := make(digestStrings)
	bundleCtx := ctx // context with unified metadata.
	bundleSize := u.queryRequestBaseSize

	handle := func() {
		if len(bundle) < 1 {
			return
		}
		// Block the entire processor if the concurrency limit is reached.
		startTime := time.Now()
		if !u.queryThrottler.acquire(ctx) {
			// Ensure responses are dispatched before aborting.
			for d := range bundle {
				u.queryPubSub.pub(MissingBlobsResponse{
					Digest: d,
					Err:    ctx.Err(),
				}, bundle[d]...)
			}
			log.V(3).Infof("[casng] query.cancel")
			return
		}
		log.V(3).Infof("[casng] query.throttle.duration; start=%d, end=%d", startTime.UnixNano(), time.Now().UnixNano())

		u.workerWg.Add(1)
		go func(ctx context.Context, b, r digestStrings) {
			defer u.workerWg.Done()
			defer u.queryThrottler.release()
			u.callMissingBlobs(ctx, b, r)
		}(bundleCtx, bundle, reqs)

		bundle = make(digestStrings)
		reqs = make(digestStrings)
		bundleSize = u.queryRequestBaseSize
		bundleCtx = ctx
	}

	bundleTicker := time.NewTicker(u.queryRPCCfg.BundleTimeout)
	defer bundleTicker.Stop()
	for {
		select {
		case req, ok := <-u.queryCh:
			if !ok {
				return
			}
			startTime := time.Now()

			log.V(3).Infof("[casng] query.processor.req; digest=%s, req=%s, tag=%s, bundle=%d", req.digest, req.id, req.tag, len(bundle))
			dSize := proto.Size(req.digest.ToProto())

			// Check oversized items.
			if u.queryRequestBaseSize+dSize > u.queryRPCCfg.BytesLimit {
				u.queryPubSub.pub(MissingBlobsResponse{
					Digest: req.digest,
					Err:    ErrOversizedItem,
				}, req.tag)
				// Covers waiting on subscribers.
				log.V(3).Infof("[casng] query.pub.duration; start=%d, end=%d, req=%s, tag=%s", startTime.UnixNano(), time.Now().UnixNano(), req.id, req.tag)
				continue
			}

			// Check size threshold.
			if bundleSize+dSize >= u.queryRPCCfg.BytesLimit {
				log.V(3).Infof("[casng] query.processor.bundle.size; bytes=%d, excess=%d", bundleSize, dSize)
				handle()
			}

			// Duplicate tags are allowed to ensure the requester can match the number of responses to the number of requests.
			bundle[req.digest] = append(bundle[req.digest], req.tag)
			bundleSize += dSize
			bundleCtx, _ = contextmd.FromContexts(bundleCtx, req.ctx) // ignore non-essential error.

			// Check length threshold.
			if len(bundle) >= u.queryRPCCfg.ItemsLimit {
				log.V(3).Infof("[casng] query.processor.bundle.full; count=%d", len(bundle))
				handle()
			}
		case <-bundleTicker.C:
			if len(bundle) > 0 {
				log.V(3).Infof("[casng] query.processor.bundle.timeout; count=%d", len(bundle))
			}
			handle()
		}
	}
}

// callMissingBlobs calls the gRPC endpoint and notifies requesters of the results.
// It assumes ownership of its arguments. digestTags is the primary one. digestReqs is used for logging purposes.
func (u *uploader) callMissingBlobs(ctx context.Context, digestTags, digestReqs digestStrings) {
	digests := make([]*repb.Digest, 0, len(digestTags))
	for d := range digestTags {
		digests = append(digests, d.ToProto())
	}

	req := &repb.FindMissingBlobsRequest{
		InstanceName: u.instanceName,
		BlobDigests:  digests,
	}

	var res *repb.FindMissingBlobsResponse
	var err error
	startTime := time.Now()
	err = retry.WithPolicy(ctx, u.queryRPCCfg.RetryPredicate, u.queryRPCCfg.RetryPolicy, func() error {
		ctx, ctxCancel := context.WithTimeout(ctx, u.queryRPCCfg.Timeout)
		defer ctxCancel()
		res, err = u.cas.FindMissingBlobs(ctx, req)
		return err
	})
	log.V(3).Infof("[casng] query.grpc.duration; start=%d, end=%d", startTime.UnixNano(), time.Now().UnixNano())

	var missing []*repb.Digest
	if res != nil {
		missing = res.MissingBlobDigests
	}
	if err != nil {
		err = errors.Join(ErrGRPC, err)
		missing = digests
	}

	startTime = time.Now()
	// Report missing.
	for _, dpb := range missing {
		d := digest.NewFromProtoUnvalidated(dpb)
		u.queryPubSub.pub(MissingBlobsResponse{
			Digest:  d,
			Missing: err == nil,
			Err:     err,
		}, digestTags[d]...)
		delete(digestTags, d)
	}

	// Report non-missing.
	for d := range digestTags {
		u.queryPubSub.pub(MissingBlobsResponse{
			Digest:  d,
			Missing: false,
		}, digestTags[d]...)
	}
	log.V(3).Infof("[casng] query.pub.duration; start=%d, end=%d", startTime.UnixNano(), time.Now().UnixNano())
}
