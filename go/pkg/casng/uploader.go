// Package casng provides a CAS client implementation with the following incomplete list of features:
//   - Streaming interface to upload files during the digestion process rather than after.
//   - Unified uploads and downloads.
//   - Simplifed public API.
package casng

// This file includes the implementation for uploading blobs to the CAS.
//
// The following diagram illustrates the overview of the design implemented in this package.
// The request follows a linear path through the system: request -> digest -> query -> upload -> response.
// Each box represents a processor with its own state to manage concurrent requests and proper messaging with other processors.
/*


                               Dispatcher
                    ┌─────────────────────────┐
                    │                         │
     ┌───────────┐  │ ┌─────┐       ┌──────┐  │ Digest
     │           │  │ │     │ Digest│ Pipe ├──┼───────┐
     │ Digester  ├──┼─► Req ├───────► Req  │  │       │
     │           │  │ └─────┘       └──────┘  │  ┌────▼─────┐
     └─────▲─────┘  │                         │  │          │
   Upload  │        │                         │  │  Query   │
   Request │        │                         │  │ Processor│
           │        │                         │  │          │
      ┌────┴───┐    │ ┌─────┐ Cache ┌──────┐  │  └────┬─────┘
      │        ◄────┼─┤ Res │  Hit  │ Pipe │  │       │
      │  User  │    │ │     ◄───────┤ Res  ◄──┼───────┘
      │        │    │ └▲──▲─┘       └┬────┬┘  │  Query
      └────────┘    │  │  │     Small│    │   │ Response
                    │  │  │     Blob │    │   │
                    └──┼──┼──────────┼────┼───┘
                       │  │          │    │
                       │  │ ┌────────▼─┐  │Large
                       │  │ │  Batcher │  │Blob
                       │  └─┤   gRPC   │  │
                       │    └──────────┘  │
                       │                  │
                       │    ┌──────────┐  │
                       │    │ Streamer │  │
                       └────┤   gRPC   ◄──┘
                            └──────────┘
*/
// The overall streaming flow is as follows:
//   digester        -> dispatcher/req
//   dispatcher/req  -> dispatcher/pipe
//   dispatcher/pipe -> query processor
//   query processor -> dispatcher/pipe
//   dispatcher/pipe -> dispatcher/res (cache hit)
//   dispatcher/res  -> requester (cache hit)
//   dispatcher/pipe -> batcher (small file)
//   dispatcher/pipe -> streamer (medium and large file)
//   batcher         -> dispatcher/res
//   streamer        -> dispatcher/res
//   dispatcher/res  -> requester
//
// The termination sequence is as follows:
//   user cancels the batching or the streaming context, not the uploader's context, and closes input streaming channels.
//       cancelling the context triggers aborting in-flight requests.
//   user cancels uploader's context: cancels pending digestions and gRPC processors blocked on throttlers.
//   client senders (top level) terminate.
//   the digester channel is closed, and a termination signal is sent to the dispatcher.
//   the dispatcher terminates its sender and propagates the signal to its piper.
//   the dispatcher's piper propagtes the signal to the intermediate query streamer.
//   the intermediate query streamer terimnates and propagates the signal to the query processor and dispatcher's piper.
//   the query processor terminates.
//   the dispatcher's piper terminates.
//   the dispatcher's counter termiantes (after observing all the remaining blobs) and propagates the signal to the receiver.
//   the dispatcher's receiver terminates.
//   the dispatcher terminates and propagates the signal to the batcher and the streamer.
//   the batcher and the streamer terminate.
//   user waits for the termination signal: return from batching uploader or response channel closed from streaming uploader.
//       this ensures the whole pipeline is drained properly.
//
// A note about logging:
//  Level 1 is used for top-level functions, typically called once during the lifetime of the process or initiated by the user.
//  Level 2 is used for internal functions that may be called per request.
//  Level 3 is used for internal functions that may be called multiple times per request. Duration logs are also level 3 to avoid the overhead in level 4.
//  Level 4 is used for messages with large objects.
//  Level 5 is used for messages that require custom processing (extra compute).
//
// Log messages are formatted to be grep-friendly. You can do things like:
//   grep info.log -e 'casng'
//   grep info.log -e 'upload.digester'
//   grep info.log -e 'req=request_id'
//   grep info.log -e 'tag=requester_id'
//
// To get a csv file of durations, enable verbosity level 3 and use the command:
//   grep info.log -e 'casng.*duration;' | cut -d ' ' -f 6-8 | sed -e 's/; start=/,/' -e 's/, end=/,/' -e 's/,$//' > /tmp/durations.csv

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"sync"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	// Redundant imports are required for the google3 mirror. Aliases should not be changed.
	regrpc "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	log "github.com/golang/glog"
	"github.com/klauspost/compress/zstd"
	"github.com/pborman/uuid"

	// Alias should not be changed because it's used as is for the google3 mirror.
	bsgrpc "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/protobuf/proto"
)

var (
	// ErrNilClient indicates an invalid nil argument.
	ErrNilClient = errors.New("client cannot be nil")

	// ErrCompression indicates an error in the compression routine.
	ErrCompression = errors.New("compression error")

	// ErrIO indicates an error in an IO routine.
	ErrIO = errors.New("io error")

	// ErrGRPC indicates an error in a gRPC routine.
	ErrGRPC = errors.New("grpc error")

	// ErrOversizedItem indicates an item that is too large to fit into the set byte limit for the corresponding gRPC call.
	ErrOversizedItem = errors.New("oversized item")

	// ErrTerminatedUploader indicates an attempt to use a terminated uploader.
	ErrTerminatedUploader = errors.New("cannot use a terminated uploader")
)

// MakeWriteResourceName returns a valid resource name for writing an uncompressed blob.
func MakeWriteResourceName(instanceName, hash string, size int64) string {
	return fmt.Sprintf("%s/uploads/%s/blobs/%s/%d", instanceName, uuid.New(), hash, size)
}

// MakeCompressedWriteResourceName returns a valid resource name for writing a compressed blob.
func MakeCompressedWriteResourceName(instanceName, hash string, size int64) string {
	return fmt.Sprintf("%s/uploads/%s/compressed-blobs/zstd/%s/%d", instanceName, uuid.New(), hash, size)
}

// IsCompressedWriteResourceName returns true if the name was generated using MakeCompressedWriteResourceName.
func IsCompressedWriteResourceName(name string) bool {
	return strings.Contains(name, "compressed-blobs/zstd")
}

// BatchingUploader provides a blocking interface to query and upload to the CAS.
type BatchingUploader struct {
	*uploader
}

// StreamingUploader provides an non-blocking interface to query and upload to the CAS
type StreamingUploader struct {
	*uploader
}

// uploader represents the state of an uploader implementation.
type uploader struct {
	cas          regrpc.ContentAddressableStorageClient
	byteStream   bsgrpc.ByteStreamClient
	instanceName string

	queryRPCCfg  GRPCConfig
	batchRPCCfg  GRPCConfig
	streamRPCCfg GRPCConfig

	// gRPC throttling controls.
	queryThrottler  *throttler // Controls concurrent calls to the query API.
	uploadThrottler *throttler // Controls concurrent calls to the batch API.
	streamThrottle  *throttler // Controls concurrent calls to the byte streaming API.

	// IO controls.
	ioCfg            IOConfig
	buffers          sync.Pool
	zstdEncoders     sync.Pool
	walkThrottler    *throttler // Controls concurrent file system walks.
	ioThrottler      *throttler // Controls total number of open files.
	ioLargeThrottler *throttler // Controls total number of open large files.
	// nodeCache allows digesting each path only once.
	// Concurrent walkers claim a path by storing a sync.WaitGroup reference, which allows other walkers to defer
	// digesting that path until the first walker stores the digest once it's computed.
	// The keys are unique per walk, which means two walkers with different filters may cache the same path twice, but each copy could
	// have a different node associated with it.
	// However, regular files will have duplicate nodes in this cache.
	nodeCache sync.Map
	// fileNodeCache is similar to nodeCache, but only holds file nodes. The keys are real paths and are not unique across walks.
	// This cache ensures that regular files are only digested once, even across walks with different exclusion filters.
	// It also ensures that nodeCache does not have duplicate nodes for identical files.
	// In other words, nodeCache might hold different views of the same directory node, but fileNodeCache will always hold the canonical file node for the corresponding real path.
	// Since nodes are pointer-like references, the shared memory cost between the two caches is limited to keys and addresses.
	fileNodeCache sync.Map
	// dirChildren is shared between all callers. However, since a directory is owned by a single
	// walker at a time, there is no concurrent read/write to this map, but there might be concurrent reads.
	dirChildren               nodeSliceMap
	queryRequestBaseSize      int
	uploadRequestBaseSize     int
	uploadRequestItemBaseSize int

	// Concurrency controls.
	clientSenderWg   sync.WaitGroup          // Batching API producers.
	querySenderWg    sync.WaitGroup          // Query streaming API producers.
	uploadSenderWg   sync.WaitGroup          // Upload streaming API producers.
	processorWg      sync.WaitGroup          // Internal routers.
	receiverWg       sync.WaitGroup          // Consumers.
	workerWg         sync.WaitGroup          // Short-lived intermediate producers/consumers.
	walkerWg         sync.WaitGroup          // Tracks all walkers.
	queryCh          chan missingBlobRequest // Fan-in channel for query requests.
	digesterCh       chan UploadRequest      // Fan-in channel for upload requests.
	dispatcherReqCh  chan UploadRequest      // Fan-in channel for dispatched requests.
	dispatcherPipeCh chan UploadRequest      // A pipe channel for presence checking before uploading.
	dispatcherResCh  chan UploadResponse     // Fan-in channel for responses.
	batcherCh        chan UploadRequest      // Fan-in channel for unified requests to the batching API.
	streamerCh       chan UploadRequest      // Fan-in channel for unified requests to the byte streaming API.
	queryPubSub      *pubsub                 // Fan-out broker for query responses.
	uploadPubSub     *pubsub                 // Fan-out broker for upload responses.

	logBeatDoneCh chan struct{}
	done          bool
}

// Node looks up a node from the node cache which is populated during digestion.
// The node is either an repb.FileNode, repb.DirectoryNode, or repb.SymlinkNode.
//
// Returns nil if no node corresponds to req.
func (u *uploader) Node(req UploadRequest) proto.Message {
	key := req.Path.String() + req.Exclude.String()
	n, ok := u.nodeCache.Load(key)
	if !ok {
		return nil
	}
	node, ok := n.(proto.Message)
	if !ok {
		return nil
	}
	return node
}

// NewBatchingUploader creates a new instance of the batching uploader.
// WIP: While this is intended to replace the uploader in the client and cas packages, it is not yet ready for production envionrments.
//
// The specified configs must be compatible with the capabilities of the server that the specified clients are connected to.
// ctx is used to make unified calls and terminate saturated throttlers and in-flight workers.
// ctx must be cancelled after all batching calls have returned to properly shutdown the uploader. It is only used for cancellation (not used with remote calls).
// gRPC timeouts are multiplied by retries. Batched RPCs are retried per batch. Streaming PRCs are retried per chunk.
func NewBatchingUploader(
	ctx context.Context, cas regrpc.ContentAddressableStorageClient, byteStream bsgrpc.ByteStreamClient, instanceName string,
	queryCfg, batchCfg, streamCfg GRPCConfig, ioCfg IOConfig,
) (*BatchingUploader, error) {
	uploader, err := newUploader(ctx, cas, byteStream, instanceName, queryCfg, batchCfg, streamCfg, ioCfg)
	if err != nil {
		return nil, err
	}
	return &BatchingUploader{uploader: uploader}, nil
}

// NewStreamingUploader creates a new instance of the streaming uploader.
// WIP: While this is intended to replace the uploader in the client and cas packages, it is not yet ready for production envionrments.
//
// The specified configs must be compatible with the capabilities of the server which the specified clients are connected to.
// ctx is used to make unified calls and terminate saturated throttlers and in-flight workers.
// ctx must be cancelled after all response channels have been closed to properly shutdown the uploader. It is only used for cancellation (not used with remote calls).
// gRPC timeouts are multiplied by retries. Batched RPCs are retried per batch. Streaming PRCs are retried per chunk.
func NewStreamingUploader(
	ctx context.Context, cas regrpc.ContentAddressableStorageClient, byteStream bsgrpc.ByteStreamClient, instanceName string,
	queryCfg, batchCfg, streamCfg GRPCConfig, ioCfg IOConfig,
) (*StreamingUploader, error) {
	uploader, err := newUploader(ctx, cas, byteStream, instanceName, queryCfg, batchCfg, streamCfg, ioCfg)
	if err != nil {
		return nil, err
	}
	return &StreamingUploader{uploader: uploader}, nil
}

// TODO: support uploading repb.Tree.
// TODO: support node properties as in https://github.com/bazelbuild/remote-apis-sdks/pull/475
func newUploader(
	ctx context.Context, cas regrpc.ContentAddressableStorageClient, byteStream bsgrpc.ByteStreamClient, instanceName string,
	queryCfg, uploadCfg, streamCfg GRPCConfig, ioCfg IOConfig,
) (*uploader, error) {
	if cas == nil || byteStream == nil {
		return nil, ErrNilClient
	}
	if err := validateGrpcConfig(&queryCfg); err != nil {
		return nil, err
	}
	if err := validateGrpcConfig(&uploadCfg); err != nil {
		return nil, err
	}
	if err := validateGrpcConfig(&streamCfg); err != nil {
		return nil, err
	}
	if err := validateIOConfig(&ioCfg); err != nil {
		return nil, err
	}

	u := &uploader{
		cas:          cas,
		byteStream:   byteStream,
		instanceName: instanceName,

		queryRPCCfg:  queryCfg,
		batchRPCCfg:  uploadCfg,
		streamRPCCfg: streamCfg,

		queryThrottler:  newThrottler(int64(queryCfg.ConcurrentCallsLimit)),
		uploadThrottler: newThrottler(int64(uploadCfg.ConcurrentCallsLimit)),
		streamThrottle:  newThrottler(int64(streamCfg.ConcurrentCallsLimit)),

		ioCfg: ioCfg,
		buffers: sync.Pool{
			New: func() any {
				buf := make([]byte, ioCfg.BufferSize)
				return &buf
			},
		},
		zstdEncoders: sync.Pool{
			New: func() any {
				// Providing a nil writer implies that the encoder needs to be (re)initilaized with a writer using enc.Reset(w) before using it.
				enc, _ := zstd.NewWriter(nil)
				return enc
			},
		},
		walkThrottler:    newThrottler(int64(ioCfg.ConcurrentWalksLimit)),
		ioThrottler:      newThrottler(int64(ioCfg.OpenFilesLimit)),
		ioLargeThrottler: newThrottler(int64(ioCfg.OpenLargeFilesLimit)),
		dirChildren:      nodeSliceMap{store: make(map[string][]proto.Message)},

		queryCh:          make(chan missingBlobRequest),
		queryPubSub:      newPubSub(),
		digesterCh:       make(chan UploadRequest),
		dispatcherReqCh:  make(chan UploadRequest),
		dispatcherPipeCh: make(chan UploadRequest),
		dispatcherResCh:  make(chan UploadResponse),
		batcherCh:        make(chan UploadRequest),
		streamerCh:       make(chan UploadRequest),
		uploadPubSub:     newPubSub(),

		queryRequestBaseSize:      proto.Size(&repb.FindMissingBlobsRequest{InstanceName: instanceName, BlobDigests: []*repb.Digest{}}),
		uploadRequestBaseSize:     proto.Size(&repb.BatchUpdateBlobsRequest{InstanceName: instanceName, Requests: []*repb.BatchUpdateBlobsRequest_Request{}}),
		uploadRequestItemBaseSize: proto.Size(&repb.BatchUpdateBlobsRequest_Request{Digest: digest.NewFromBlob([]byte("abc")).ToProto(), Data: []byte{}}),

		logBeatDoneCh: make(chan struct{}),
	}
	log.V(1).Infof("[casng] uploader.new; cfg_query=%+v, cfg_batch=%+v, cfg_stream=%+v, cfg_io=%+v", queryCfg, uploadCfg, streamCfg, ioCfg)

	u.processorWg.Add(1)
	go func() {
		u.queryProcessor(ctx)
		u.processorWg.Done()
	}()

	u.processorWg.Add(1)
	go func() {
		u.digester(ctx)
		u.processorWg.Done()
	}()

	// Initializing the query streamer here to ensure wait groups are initialized before returning from this constructor call.
	queryCh := make(chan missingBlobRequest)
	queryResCh := u.missingBlobsPipe(queryCh)
	u.processorWg.Add(1)
	go func() {
		u.dispatcher(queryCh, queryResCh)
		u.processorWg.Done()
	}()

	u.processorWg.Add(1)
	go func() {
		u.batcher(ctx)
		u.processorWg.Done()
	}()

	u.processorWg.Add(1)
	go func() {
		u.streamer(ctx)
		u.processorWg.Done()
	}()

	go u.close(ctx)
	go u.logBeat()
	return u, nil
}

func (u *uploader) close(ctx context.Context) {
	// The context must be cancelled first.
	<-ctx.Done()
	// It's possible for a client to make a call between receiving the context cancellation
	// signal and storing the done boolean value. Races are also possible.
	// However, this is not a problem because the termination sequence below ensures
	// all producers are terminated before releasing resources.
	u.done = true

	startTime := time.Now()

	// 1st, batching API senders should stop producing requests.
	// These senders are terminated by the user.
	log.V(1).Infof("[casng] uploader: waiting for client senders")
	u.clientSenderWg.Wait()

	// 2nd, streaming API upload senders should stop producing queries and requests.
	// These senders are terminated by the user.
	log.V(1).Infof("[casng] uploader: waiting for upload senders")
	u.uploadSenderWg.Wait()
	close(u.digesterCh) // The digester will propagate the termination signal.

	// 3rd, streaming API query senders should stop producing queries.
	// This propagates from the uploader's pipe, hence, the uploader must stop first.
	log.V(1).Infof("[casng] uploader: waiting for query senders")
	u.querySenderWg.Wait()
	close(u.queryCh) // Terminate the query processor.

	// 4th, internal routres should flush all remaining requests.
	log.V(1).Infof("[casng] uploader: waiting for processors")
	u.processorWg.Wait()

	// 5th, internal brokers should flush all remaining messages.
	log.V(1).Infof("[casng] uploader: waiting for brokers")
	u.queryPubSub.wait()
	u.uploadPubSub.wait()

	// 6th, receivers should have drained their channels by now.
	log.V(1).Infof("[casng] uploader: waiting for receivers")
	u.receiverWg.Wait()

	// 7th, workers should have terminated by now.
	log.V(1).Infof("[casng] uploader: waiting for workers")
	u.workerWg.Wait()

	close(u.logBeatDoneCh)
	log.V(3).Infof("[casng] upload.close.duration: start=%d, end=%d", startTime.UnixNano(), time.Now().UnixNano())
}

func (u *uploader) logBeat() {
	var interval time.Duration
	if log.V(3) {
		interval = time.Second
	} else if log.V(2) {
		interval = 30 * time.Second
	} else if log.V(1) {
		interval = time.Minute
	} else {
		return
	}

	log.Infof("[casng] beat.start; interval=%v", interval)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	i := 0
	for {
		select {
		case <-u.logBeatDoneCh:
			log.Infof("[casng] beat.stop; interval=%v, count=%d", interval, i)
			return
		case <-ticker.C:
		}

		i++
		log.Infof("[casng] beat; #%d, upload_subs=%d, query_subs=%d, walkers=%d, batching=%d, streaming=%d, querying=%d, open_files=%d, large_open_files=%d",
			i, u.uploadPubSub.len(), u.queryPubSub.len(), u.walkThrottler.len(), u.uploadThrottler.len(), u.streamThrottle.len(), u.queryThrottler.len(), u.ioThrottler.len(), u.ioLargeThrottler.len())
	}
}

// releaseIOTokens releases from both ioThrottler and ioLargeThrottler.
func (u *uploader) releaseIOTokens() {
	u.ioThrottler.release()
	u.ioLargeThrottler.release()
}

func isExec(mode fs.FileMode) bool {
	return mode&0100 != 0
}
