package casng

import (
	"context"
	"io"
	"io/fs"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/impath"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/walker"

	// "github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"
	slo "github.com/bazelbuild/remote-apis-sdks/go/pkg/symlinkopts"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	// "google.golang.org/grpc/status"
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
	panic("not yet implemented")
}

// uploadBatcher handles files that can fit into a batching request.
func (u *uploader) batcher() {
}

func (u *uploader) callBatchUpload(ctx context.Context, bundle uploadRequestBundle) {
	panic("not yet implemented")
}

// uploadStreamer handles files that do not fit into a batching request.
// Unlike the batched call, querying the CAS is not required because the API handles this automatically.
// See https://github.com/bazelbuild/remote-apis/blob/0cd22f7b466ced15d7803e8845d08d3e8d2c51bc/build/bazel/remote/execution/v2/remote_execution.proto#L250-L254
// For files above the large threshold, this call assumes the io and large io holds are already acquired and will release them accordingly.
// For other files, only an io hold is acquired and released in this call.
func (u *uploader) streamer() {
}

func (u *uploader) callStream(ctx context.Context, name string, req UploadRequest) (stats Stats, err error) {
	panic("not yet implemented")
}
