package casng

import (
	"io"
	"io/fs"
	"sync"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/impath"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/walker"
	slo "github.com/bazelbuild/remote-apis-sdks/go/pkg/symlinkopts"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	log "github.com/golang/glog"
	"google.golang.org/protobuf/proto"
)

// blob is returned by digestFile with only one of its fields set.
type blob struct {
	r io.ReadSeekCloser
	b []byte
}

// digester reveives upload requests from multiple concurrent requesters.
// For each request, a file system walk is started concurrently to digest and forward blobs to the dispatcher.
// If the request is for an already digested blob, it is forwarded to the dispatcher.
// The number of concurrent requests is limited to the number of concurrent file system walks.
func (u *uploader) digester() {
	log.V(1).Info("[casng] upload.digester.start")
	defer log.V(1).Info("[casng] upload.digester.stop")

	// The digester receives requests from a stream pipe, and sends digested blobs to the dispatcher.
	//
	// Once the digester receives a done-tagged request from a requester, it will send a done-tagged blob to the dispatcher
	// after all related walks are done.
	//
	// Once the uploader's context is cancelled, the digester will terminate after all the pending walks are done (implies all requesters are notified).

	defer func() {
		u.walkerWg.Wait()
		// Let the dispatcher know that the digester has terminated by sending an untagged done blob.
		u.dispatcherReqCh <- UploadRequest{done: true}
	}()

	requesterWalkWg := make(map[string]*sync.WaitGroup)
	for req := range u.digesterCh {
		startTime := time.Now()
		// If the requester will not be sending any further requests, wait for in-flight walks from previous requests
		// then tell the dispatcher to forward the signal once all dispatched blobs are done.
		if req.done {
			log.V(2).Infof("[casng] upload.digester.req.done; tag=%s", req.tag)
			wg := requesterWalkWg[req.tag]
			if wg == nil {
				log.V(2).Infof("[casng] upload.digester.req.done: no more pending walks; tag=%s", req.tag)
				// Let the dispatcher know that this requester is done.
				u.dispatcherReqCh <- req
				// Covers waiting on the dispatcher.
				log.V(3).Infof("[casng] upload.digester.req.duration; start=%d, end=%d, tag=%s", startTime.UnixNano(), time.Now().UnixNano(), req.tag)
				continue
			}
			// Remove the wg to ensure a new one is used if the requester decides to send more requests.
			// Otherwise, races on the wg might happen.
			requesterWalkWg[req.tag] = nil
			u.workerWg.Add(1)
			// Wait for the walkers to finish dispatching blobs then tell the dispatcher that no further blobs are expected from this requester.
			go func(tag string) {
				defer u.workerWg.Done()
				log.V(2).Infof("[casng] upload.digester.walk.wait.start; tag=%s", tag)
				wg.Wait()
				log.V(2).Infof("[casng] upload.digester.walk.wait.done; tag=%s", tag)
				u.dispatcherReqCh <- UploadRequest{tag: tag, done: true}
			}(req.tag)
			continue
		}

		// If it's a bytes request, do not traverse the path.
		if req.Bytes != nil {
			if req.Digest.Hash == "" {
				req.Digest = digest.NewFromBlob(req.Bytes)
			}
			// If path is set, construct and cache the corresponding node.
			if req.Path.String() != impath.Root {
				name := req.Path.Base().String()
				digest := req.Digest.ToProto()
				var node proto.Message
				if req.BytesFileMode&fs.ModeDir != 0 {
					node = &repb.DirectoryNode{Digest: digest, Name: name}
				} else {
					node = &repb.FileNode{Digest: digest, Name: name, IsExecutable: isExec(req.BytesFileMode)}
				}
				key := req.Path.String() + req.Exclude.String()
				u.nodeCache.Store(key, node)
				// This node cannot be added to the u.dirChildren cache because the cache is owned by the walker callback.
				// Parent nodes may have already been generated and cached in u.nodeCache; updating the u.dirChildren cache will not regenerate them.
			}
			log.V(3).Infof("[casng] upload.digester.req; bytes=%d, path=%s, req=%s, tag=%s", len(req.Bytes), req.Path, req.id, req.tag)
		}

		if req.Digest.Hash != "" {
			u.dispatcherReqCh <- req
			// Covers waiting on the node cache and waiting on the dispatcher.
			log.V(3).Infof("[casng] upload.digester.req.duration; start=%d, end=%d, req=%s, tag=%s", startTime.UnixNano(), time.Now().UnixNano(), req.id, req.tag)
			continue
		}

		log.V(3).Infof("[casng] upload.digester.req; path=%s, filter=%s, slo=%s, req=%s, tag=%s", req.Path, req.Exclude, req.SymlinkOptions, req.id, req.tag)
		// Wait if too many walks are in-flight.
		startTimeThrottle := time.Now()
		if !u.walkThrottler.acquire(req.ctx) {
			log.V(3).Infof("[casng] upload.digester.walk.throttle.duration; start=%d, end=%d, req=%s, tag=%s", startTimeThrottle.UnixNano(), time.Now().UnixNano(), req.id, req.tag)
			continue
		}
		log.V(3).Infof("[casng] upload.digester.walk.throttle.duration; start=%d, end=%d, req=%s, tag=%s", startTimeThrottle.UnixNano(), time.Now().UnixNano(), req.id, req.tag)
		wg := requesterWalkWg[req.tag]
		if wg == nil {
			wg = &sync.WaitGroup{}
			requesterWalkWg[req.tag] = wg
		}
		wg.Add(1)
		u.walkerWg.Add(1)
		go func(r UploadRequest) {
			defer u.walkerWg.Done()
			defer wg.Done()
			defer u.walkThrottler.release()
			u.digest(r)
		}(req)
	}
}

// digest initiates a file system walk to digest files and dispatch them for uploading.
func (u *uploader) digest(req UploadRequest) {
	panic("not yet implemented")
}

// digestSymlink follows the target and/or constructs a symlink node.
//
// The returned node doesn't include ancenstory information. The symlink name is just the base of path, while the target is relative to the symlink name.
// For example: if the root is /a, the symlink is b/c and the target is /a/foo, the name will be c and the target will be ../foo.
// Note that the target includes hierarchy information, without specific names.
// Another example: if the root is /a, the symilnk is b/c and the target is foo, the name will be c, and the target will be foo.
func digestSymlink(root impath.Absolute, path impath.Absolute, slo slo.Options) (*repb.SymlinkNode, walker.SymlinkAction, error) {
	panic("not yet implemented")
}

// digestDirectory constructs a hash-deterministic directory node and returns it along with the corresponding bytes of the directory proto.
//
// No syscalls are made in this method.
// Only the base of path is used. No ancenstory information is included in the returned node.
func digestDirectory(path impath.Absolute, children []proto.Message) (*repb.DirectoryNode, []byte, error) {
	panic("not yet implemented")
}

// digestFile constructs a file node and returns it along with the blob to be dispatched.
//
// No ancenstory information is included in the returned node. Only the base of path is used.
//
// One token of ioThrottler is acquired upon calling this function.
// If the file size <= smallFileSizeThreshold, the token is released before returning.
// Otherwise, the caller must assume ownership of the token and release it.
//
// If the file size >= largeFileSizeThreshold, one token of ioLargeThrottler is acquired.
// The caller must assume ownership of that token and release it.
//
// If the returned err is not nil, both tokens are released before returning.
func (u *uploader) digestFile(path impath.Absolute, info fs.FileInfo, closeLargeFile bool, reqID string, tag string, walkID string) (node *repb.FileNode, blb *blob, err error) {
	panic("not yet implemented")
}
