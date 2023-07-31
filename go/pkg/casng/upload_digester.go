package casng

// The digester performs a file system walk in depth-first-search style by visiting a directory
// after visiting all of its descendents. This allows digesting the directory content
// before digesting the directory itself to produce a merkle tree.
//
// The digester can do concurrent walks that might share directories, in which case the access
// is coordinated by allowing one walk to claim a file or directory, preventing other concurrent
// walks from accessing it at the same time. While claimed, all other walks differ visiting
// the calimed path and continue traversing other paths. If no other paths are to be traversed, the walker
// simply blocks waiting for the owning walker to complete digestion and release the path for other to claim.
//
// Each digested path is cached in a shared node cache. The node cache is keyed by file path and walker filter ID.
// This key ensures that two different walkers with different filters do not share any entries in the cache.
// This allows each walker to have a different view for the same directory and still get a correct merkle tree.
// For example, a walker might visit files f1 and f2 inside directory d, while another walker only visits f3.
// In this example, which walker gets to access d after the other won't get a cache hit from the node cache.
// The compute cost for this cache miss is limited to generating a meta struct that describes d and digesting it.
// It doesn't involve accessing the file system beyond stating the directory.

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/errors"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/filemetadata"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/impath"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/walker"
	slo "github.com/bazelbuild/remote-apis-sdks/go/pkg/symlinkopts"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	log "github.com/golang/glog"
	"github.com/pborman/uuid"
	"github.com/pkg/xattr"
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
func (u *uploader) digester(ctx context.Context) {
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
			log.V(3).Infof("[casng] upload.digester.req; bytes=%d, path=%s, filter_id=%s, req=%s, tag=%s", len(req.Bytes), req.Path, req.Exclude, req.id, req.tag)
		}

		if req.Digest.Hash != "" {
			u.dispatcherReqCh <- req
			// Covers waiting on the node cache and waiting on the dispatcher.
			log.V(3).Infof("[casng] upload.digester.req.duration; start=%d, end=%d, req=%s, tag=%s", startTime.UnixNano(), time.Now().UnixNano(), req.id, req.tag)
			continue
		}

		log.V(3).Infof("[casng] upload.digester.req; path=%s, filter_id=%s, slo=%s, req=%s, tag=%s", req.Path, req.Exclude, req.SymlinkOptions, req.id, req.tag)
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
			u.digest(ctx, r)
		}(req)
	}
}

// digest initiates a file system walk to digest files and dispatch them for uploading.
func (u *uploader) digest(ctx context.Context, req UploadRequest) {
	walkID := uuid.New()
	if log.V(3) {
		startTime := time.Now()
		defer func() {
			log.V(3).Infof("[casng] upload.digester.visit.duration; start=%d, end=%d, root=%s, req=%s, tag=%s, walk=%s", startTime.UnixNano(), time.Now().UnixNano(), req.Path, req.id, req.tag, walkID)
		}()
	}

	stats := Stats{}
	var err error
	deferredWg := make(map[string]*sync.WaitGroup)
	walker.DepthFirst(req.Path, req.Exclude, walker.Callback{
		Err: func(path impath.Absolute, realPath impath.Absolute, errVisit error) bool {
			log.V(3).Infof("[casng] upload.digester.visit.err; path=%s, real_path=%s, err=%v, req=%s, tag=%s, walk=%s", path, realPath, errVisit, req.id, req.tag, walkID)
			err = errors.Join(errVisit, err)
			return false
		},
		Pre: func(path impath.Absolute, realPath impath.Absolute) (walker.PreAction, bool) {
			log.V(3).Infof("[casng] upload.digester.visit.pre; path=%s, real_path=%s, filter_id=%s, req=%s, tag=%s, walk=%s", path, realPath, req.Exclude, req.id, req.tag, walkID)

			select {
			// If the request was aborted, abort.
			case <-req.ctx.Done():
				log.V(3).Infof("upload.digester.req.cancel; tag=%s, walk=%s", req.tag, walkID)
				return walker.SkipPath, false
			// This is intended to short-circuit a cancelled uploader. It is not intended to abort by cancelling the uploader's context.
			case <-ctx.Done():
				log.V(3).Infof("upload.digester.cancel; tag=%s, walk=%s", req.tag, walkID)
				return walker.SkipPath, false
			default:
			}

			// A cache hit here indicates a cyclic symlink with the same requester or multiple requesters attempting to upload the exact same path with an identical filter.
			// In both cases, deferring is the right call. Once the request is processed, all requestters will revisit the path to get the digestion result.
			// If the path was not cached before, claim it by marking it as in-flight.
			key := path.String() + req.Exclude.String()
			wg := &sync.WaitGroup{}
			wg.Add(1)
			m, ok := u.nodeCache.LoadOrStore(key, wg)
			if !ok {
				// Claimed. Access it.
				log.V(3).Infof("[casng] upload.digester.visit.claimed; path=%s, real_path=%s, filter_id=%s, req=%s, tag=%s, walk=%s", path, realPath, req.Exclude, req.id, req.tag, walkID)
				return walker.Access, true
			}

			// Defer if in-flight. Wait if already deferred since there is nothing else to do.
			if wg, ok := m.(*sync.WaitGroup); ok {
				if deferredWg[key] == nil {
					deferredWg[key] = wg
					log.V(3).Infof("[casng] upload.digester.visit.defer; path=%s, real_path=%s, req=%s, tag=%s, walk=%s", path, realPath, req.id, req.tag, walkID)
					return walker.Defer, true
				}
				log.V(3).Infof("[casng] upload.digester.visit.wait; path=%s, real_path=%s, req=%s, tag=%s, walk=%s", path, realPath, req.id, req.tag, walkID)
				startTime := time.Now()
				wg.Wait()
				log.V(3).Infof("[casng] upload.digester.visit.wait.duration; start=%d, end=%d, path=%s, real_path=%s, req=%s, tag=%s, walk=%s", startTime.UnixNano(), time.Now().UnixNano(), path, realPath, req.id, req.tag, walkID)
				delete(deferredWg, key)
				return walker.Defer, true
			}

			node := m.(proto.Message) // Guaranteed assertion because the cache is an internal field.
			log.V(3).Infof("[casng] upload.digester.visit.cached; path=%s, real_path=%s, filter_id=%s, req=%s, tag=%s, walk=%s", path, realPath, req.Exclude, req.id, req.tag, walkID)

			// Forward it to correctly account for a cache hit or upload if the original blob is blocked elsewhere.
			switch node := node.(type) {
			case *repb.FileNode:
				u.dispatcherReqCh <- UploadRequest{Path: realPath, Digest: digest.NewFromProtoUnvalidated(node.Digest), id: req.id, tag: req.tag, ctx: req.ctx, digestOnly: req.digestOnly}
			case *repb.DirectoryNode:
				// The blob of the directory node is the bytes of a repb.Directory message.
				// Generate and forward it. If it was uploaded before, it'll be reported as a cache hit.
				// Otherwise, it means the previous attempt to upload it failed and it is going to be retried.
				node, b, errDigest := digestDirectory(path, u.dirChildren.load(key))
				if errDigest != nil {
					err = errors.Join(errDigest, err)
					return walker.SkipPath, false
				}
				u.dispatcherReqCh <- UploadRequest{Bytes: b, Digest: digest.NewFromProtoUnvalidated(node.Digest), id: req.id, tag: req.tag, ctx: req.ctx, digestOnly: req.digestOnly}
			case *repb.SymlinkNode:
				// It was already appended as a child to its parent. Nothing to forward.
			default:
				log.Errorf("[casng] cached node has unexpected type: %T; req=%s, tag=%s, walk=%s", node, req.id, req.tag, walkID)
				return walker.SkipPath, false
			}

			return walker.SkipPath, true
		},
		Post: func(path impath.Absolute, realPath impath.Absolute, info fs.FileInfo) (ok bool) {
			log.V(3).Infof("[casng] upload.digester.visit.post; path=%s, real_path=%s, filter_id=%s, req=%s, tag=%s, walk=%s", path, realPath, req.Exclude, req.id, req.tag, walkID)

			select {
			case <-req.ctx.Done():
				log.V(3).Infof("upload.digester.req.cancel; req=%s, tag=%s, walk=%s", req.id, req.tag, walkID)
				return false
			// This is intended to short-circuit a cancelled uploader. It is not intended to abort by cancelling the uploader's context.
			case <-ctx.Done():
				log.V(3).Infof("upload.digester.cancel; req=%s, tag=%s, walk=%s", req.id, req.tag, walkID)
				return false
			default:
			}

			key := path.String() + req.Exclude.String()
			parentKey := path.Dir().String() + req.Exclude.String()

			// In post-access, the cache should have this walker's own wait group.
			// Capture it here before it's overwritten with the actual result.
			wg, pathClaimed := u.nodeCache.Load(key)
			if !pathClaimed {
				err = errors.Join(fmt.Errorf("attempted to post-access a non-claimed path %q; req=%s, tag=%s, walk=%s", path, req.id, req.tag, walkID), err)
				return false
			}
			defer func() {
				if !ok {
					// Unclaim the path by deleting this walker's wait group.
					u.nodeCache.Delete(key)
				}
				wg.(*sync.WaitGroup).Done()
			}()

			switch {
			case info.Mode().IsDir():
				stats.DigestCount++
				stats.InputDirCount++
				// All the descendants have already been visited (DFS).
				node, b, errDigest := digestDirectory(path, u.dirChildren.load(key))
				if errDigest != nil {
					err = errors.Join(errDigest, err)
					return false
				}
				u.dirChildren.append(parentKey, node)
				u.dispatcherReqCh <- UploadRequest{Bytes: b, Digest: digest.NewFromProtoUnvalidated(node.Digest), id: req.id, tag: req.tag, ctx: req.ctx, digestOnly: req.digestOnly}
				u.nodeCache.Store(key, node)
				log.V(3).Infof("[casng] upload.digester.visit.post.dir; path=%s, real_path=%s, digset=%v, filter_id=%s, req=%s, tag=%s, walk=%s", path, realPath, node.Digest, req.Exclude, req.id, req.tag, walkID)
				return true

			case info.Mode().IsRegular():
				stats.DigestCount++
				stats.InputFileCount++
				node, blb, errDigest := u.digestFile(ctx, realPath, info, req.digestOnly, req.id, req.tag, walkID)
				if errDigest != nil {
					err = errors.Join(errDigest, err)
					return false
				}
				node.Name = path.Base().String()
				u.nodeCache.Store(key, node)
				u.dirChildren.append(parentKey, node)
				if blb == nil {
					log.V(3).Infof("[casng] upload.digester.visit.post.file.cached; path=%s, real_path=%s, digest=%v, filter_id=%s, req=%s, tag=%s, walk=%s", path, realPath, node.Digest, req.Exclude, req.id, req.tag, walkID)
					return true
				}
				u.dispatcherReqCh <- UploadRequest{Bytes: blb.b, reader: blb.r, Digest: digest.NewFromProtoUnvalidated(node.Digest), id: req.id, tag: req.tag, ctx: req.ctx, digestOnly: req.digestOnly}
				log.V(3).Infof("[casng] upload.digester.visit.post.file; path=%s, real_path=%s, digest=%v, filter_id=%s, req=%s, tag=%s, walk=%s", path, realPath, node.Digest, req.Exclude, req.id, req.tag, walkID)
				return true

			default:
				// Ignore everything else (e.g. sockets and pipes).
				log.V(3).Infof("[casng] upload.digester.visit.post.other; path=%s, real_path=%s, req=%s, tag=%s, walk=%s", path, realPath, req.id, req.tag, walkID)
			}
			return true
		},
		Symlink: func(path impath.Absolute, realPath impath.Absolute, _ fs.FileInfo) (action walker.SymlinkAction, ok bool) {
			log.V(3).Infof("[casng] upload.digester.visit.symlink; path=%s, real_path=%s, slo=%s, filter_id=%s, req=%s, tag=%s, walk=%s", path, realPath, req.SymlinkOptions, req.Exclude, req.id, req.tag, walkID)

			select {
			case <-req.ctx.Done():
				log.V(3).Infof("upload.digester.req.cancel; req=%s, tag=%s, walk=%s", req.id, req.tag, walkID)
				return walker.SkipSymlink, false
			// This is intended to short-circuit a cancelled uploader. It is not intended to abort by cancelling the uploader's context.
			case <-ctx.Done():
				log.V(3).Infof("upload.digester.cancel; req=%s, tag=%s, walk=%s", req.id, req.tag, walkID)
				return walker.SkipSymlink, false
			default:
			}

			key := path.String() + req.Exclude.String()
			parentKey := path.Dir().String() + req.Exclude.String()

			// In symlink post-access, the cache should have this walker's own wait group.
			// Capture it here before it's overwritten with the actual result.
			wg, pathClaimed := u.nodeCache.Load(key)
			if !pathClaimed {
				err = errors.Join(fmt.Errorf("attempted to post-access a non-claimed path %q; req=%s, tag=%s, walk=%s", path, req.id, req.tag, walkID), err)
				return walker.SkipSymlink, false
			}
			defer func() {
				// If there was a digestion error, unclaim the path.
				if !ok {
					u.nodeCache.Delete(key)
				}
				wg.(*sync.WaitGroup).Done()
			}()

			stats.DigestCount++
			stats.InputSymlinkCount++
			node, nextStep, errDigest := digestSymlink(req.Path, realPath, req.SymlinkOptions)
			if errDigest != nil {
				err = errors.Join(errDigest, err)
				return walker.SkipSymlink, false
			}
			if node != nil {
				node.Name = path.Base().String()
				u.dirChildren.append(parentKey, node)
				u.nodeCache.Store(key, node)
			} else {
				// Unclaim so that other walkers can add the target to their queue.
				u.nodeCache.Delete(key)
			}
			log.V(3).Infof("[casng] upload.digester.visit.symlink.symlink; path=%s, real_path=%s, slo=%s, step=%s, filter_id=%s, req=%s, tag=%s, walk=%s", path, realPath, req.SymlinkOptions, nextStep, req.Exclude, req.id, req.tag, walkID)
			return nextStep, true
		},
	})

	// Special case: this response didn't have a corresponding blob. The dispatcher should not decrement its counter.
	// err includes any IO errors that happened during the walk.
	u.dispatcherResCh <- UploadResponse{endOfWalk: true, tags: []string{req.tag}, reqs: []string{req.id}, Stats: stats, Err: err}
}

// digestSymlink follows the target and/or constructs a symlink node.
//
// The returned node doesn't include ancenstory information. The symlink name is just the base of path, while the target is relative to the symlink name.
// For example: if the root is /a, the symlink is b/c and the target is /a/foo, the name will be c and the target will be ../foo.
// Note that the target includes hierarchy information, without specific names.
// Another example: if the root is /a, the symilnk is b/c and the target is foo, the name will be c, and the target will be foo.
func digestSymlink(root impath.Absolute, path impath.Absolute, slo slo.Options) (*repb.SymlinkNode, walker.SymlinkAction, error) {
	if slo.Skip() {
		return nil, walker.SkipSymlink, nil
	}

	// Replace symlink with target.
	if slo.Resolve() {
		return nil, walker.Replace, nil
	}

	target, err := os.Readlink(path.String())
	if err != nil {
		return nil, walker.SkipSymlink, err
	}

	// Cannot use the target name for syscalls since it might be relative to the symlink directory, not the cwd of the process.
	var targetRelative string
	if filepath.IsAbs(target) {
		targetRelative, err = filepath.Rel(path.Dir().String(), target)
		if err != nil {
			return nil, walker.SkipSymlink, err
		}
	} else {
		targetRelative = target
		target = filepath.Join(path.Dir().String(), targetRelative)
	}

	if slo.ResolveExternal() {
		if _, err := impath.Descendant(root, impath.MustAbs(target)); err != nil {
			return nil, walker.Replace, nil
		}
	}

	if slo.NoDangling() {
		_, err := os.Lstat(target)
		if err != nil {
			return nil, walker.SkipSymlink, err
		}
	}

	var node *repb.SymlinkNode
	nextStep := walker.SkipSymlink

	// If the symlink itself is wanted, capture it.
	if slo.Preserve() {
		node = &repb.SymlinkNode{
			Name:   path.Base().String(),
			Target: targetRelative,
		}
	}

	// If the target is wanted, tell the walker to follow it.
	if slo.IncludeTarget() {
		nextStep = walker.Follow
	}

	return node, nextStep, nil
}

// digestDirectory constructs a hash-deterministic directory node and returns it along with the corresponding bytes of the directory proto.
//
// No syscalls are made in this method.
// Only the base of path is used. No ancenstory information is included in the returned node.
func digestDirectory(path impath.Absolute, children []proto.Message) (*repb.DirectoryNode, []byte, error) {
	dir := &repb.Directory{}
	node := &repb.DirectoryNode{
		Name: path.Base().String(),
	}
	for _, child := range children {
		switch n := child.(type) {
		case *repb.FileNode:
			dir.Files = append(dir.Files, n)
		case *repb.DirectoryNode:
			dir.Directories = append(dir.Directories, n)
		case *repb.SymlinkNode:
			dir.Symlinks = append(dir.Symlinks, n)
		}
	}
	// Sort children to get a deterministic hash.
	sort.Slice(dir.Files, func(i, j int) bool { return dir.Files[i].Name < dir.Files[j].Name })
	sort.Slice(dir.Directories, func(i, j int) bool { return dir.Directories[i].Name < dir.Directories[j].Name })
	sort.Slice(dir.Symlinks, func(i, j int) bool { return dir.Symlinks[i].Name < dir.Symlinks[j].Name })
	b, err := proto.Marshal(dir)
	if err != nil {
		return nil, nil, err
	}
	node.Digest = digest.NewFromBlob(b).ToProto()
	return node, b, nil
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
func (u *uploader) digestFile(ctx context.Context, path impath.Absolute, info fs.FileInfo, closeLargeFile bool, reqID string, tag string, walkID string) (node *repb.FileNode, blb *blob, err error) {
	// Always return a clone to ensure the cached version remains owned by the cache.
	defer func() {
		if node != nil {
			node = proto.Clone(node).(*repb.FileNode)
		}
	}()

	// Check the cache first. If not cached or previously claimed, claim it.
	wg := &sync.WaitGroup{}
	wg.Add(1)
	// Keep trying to claim it unless it gets cached.
	for {
		m, ok := u.fileNodeCache.LoadOrStore(path, wg)
		// Claimed.
		if !ok {
			break
		}
		// Cached.
		if n, ok := m.(*repb.FileNode); ok {
			return n, nil, nil
		}
		// Previously calimed; wait for it.
		m.(*sync.WaitGroup).Wait()
	}
	// Not cached or previously calimed; Compute it.
	defer wg.Done()
	defer func() {
		// In case of an error, unclaim it.
		if err != nil {
			u.fileNodeCache.Delete(path)
			return
		}
		u.fileNodeCache.Store(path, node)
	}()

	node = &repb.FileNode{
		Name:         path.Base().String(),
		IsExecutable: isExec(info.Mode()),
	}

	// TODO: do not import filemetadata.
	if filemetadata.XattrDigestName != "" {
		if !xattr.XATTR_SUPPORTED {
			return nil, nil, fmt.Errorf("[casng] failed to read digest from x-attribute for %q: x-attributes are not supported by the system; req=%s, tag=%s, walk=%s", path, reqID, tag, walkID)
		}
		xattrValue, err := xattr.Get(path.String(), filemetadata.XattrDigestName)
		if err != nil {
			return nil, nil, fmt.Errorf("[casng] failed to read digest from x-attribute for %q: %v; req=%s, tag=%s, walk=%s", path, err, reqID, tag, walkID)
		}
		xattrStr := string(xattrValue)
		if !strings.ContainsRune(xattrStr, '/') {
			xattrStr = fmt.Sprintf("%s/%d", xattrStr, info.Size())
		}
		dg, err := digest.NewFromString(xattrStr)
		if err != nil {
			return nil, nil, fmt.Errorf("[casng] failed to parse digest from x-attribute for %q: %v; req=%s, tag=%s, walk=%s", path, err, reqID, tag, walkID)
		}
		node.Digest = dg.ToProto()
		log.V(3).Infof("[casng] upload.digester.file.xattr; path=%s, size=%d, req=%s, tag=%s, walk=%s", path, info.Size(), reqID, tag, walkID)
		return node, &blob{}, nil
	}

	// Start by acquiring a token for the large file.
	// This avoids consuming too many tokens from ioThrottler only to get blocked waiting for ioLargeThrottler which would starve non-large files.
	// This assumes ioThrottler has more tokens than ioLargeThrottler.
	if info.Size() > u.ioCfg.LargeFileSizeThreshold {
		startTime := time.Now()
		if !u.ioLargeThrottler.acquire(ctx) {
			return nil, nil, ctx.Err()
		}
		log.V(3).Infof("[casng] upload.digester.io_large_throttle.duration; start=%d, end=%d, req=%s, tag=%s, walk=%s", startTime.UnixNano(), time.Now().UnixNano(), reqID, tag, walkID)
		defer func() {
			// Only release if the file was closed. Otherwise, the caller assumes ownership of the token.
			if blb == nil || blb.r == nil {
				u.ioLargeThrottler.release()
			}
		}()
	}

	startTime := time.Now()
	if !u.ioThrottler.acquire(ctx) {
		return nil, nil, ctx.Err()
	}
	log.V(3).Infof("[casng] upload.digester.io_throttle.duration; start=%d, end=%d, req=%s, tag=%s, walk=%s", startTime.UnixNano(), time.Now().UnixNano(), reqID, tag, walkID)
	defer func() {
		// Only release if the file was closed. Otherwise, the caller assumes ownership of the token.
		if blb == nil || blb.r == nil {
			u.ioThrottler.release()
		}
	}()

	// Small: in-memory blob.
	if info.Size() <= u.ioCfg.SmallFileSizeThreshold {
		log.V(3).Infof("[casng] upload.digester.file.small; path=%s, size=%d, req=%s, tag=%s, walk=%s", path, info.Size(), reqID, tag, walkID)
		f, err := os.Open(path.String())
		if err != nil {
			return nil, nil, err
		}
		defer func() {
			if errClose := f.Close(); errClose != nil {
				err = errors.Join(errClose, err)
			}
		}()

		b, err := io.ReadAll(f)
		if err != nil {
			return nil, nil, err
		}
		dg := digest.NewFromBlob(b)
		node.Digest = dg.ToProto()
		return node, &blob{b: b}, nil
	}

	// Medium: blob with path.
	if info.Size() < u.ioCfg.LargeFileSizeThreshold {
		log.V(3).Infof("[casng] upload.digester.file.medium; path=%s, size=%d, req=%s, tag=%s, walk=%s", path, info.Size(), reqID, tag, walkID)
		dg, errDigest := digest.NewFromFile(path.String())
		if errDigest != nil {
			return nil, nil, errDigest
		}
		node.Digest = dg.ToProto()
		return node, &blob{}, nil
	}

	// Large: blob with a reader.
	log.V(3).Infof("[casng] upload.digester.file.large; path=%s, size=%d, req=%s, tag=%s, walk=%s", path, info.Size(), reqID, tag, walkID)
	f, err := os.Open(path.String())
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if err != nil || closeLargeFile {
			if errClose := f.Close(); errClose != nil {
				err = errors.Join(errClose, err)
			}
			// Ensures IO holds are released in subsequent deferred functions.
			blb = nil
		}
	}()

	dg, errDigest := digest.NewFromReader(f)
	if errDigest != nil {
		return nil, nil, errDigest
	}

	// Reset the offset for the streamer.
	_, errSeek := f.Seek(0, io.SeekStart)
	if errSeek != nil {
		return nil, nil, errSeek
	}

	node.Digest = dg.ToProto()
	// The streamer is responsible for closing the file and releasing both ioThrottler and ioLargeThrottler.
	return node, &blob{r: f}, nil
}
