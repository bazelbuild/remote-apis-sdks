package casng

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/contextmd"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/errors"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/impath"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/walker"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/symlinkopts"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	log "github.com/golang/glog"
	"github.com/klauspost/compress/zstd"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/protobuf/proto"
)

// namedDigest is used to conveniently extract the base name and its digest from repb.FileNode, repb.DirectoryNode and repb.SymlinkNode.
type namedDigest interface {
	GetName() string
	GetDigest() *repb.Digest
}

// MissingBlobs queries the CAS for digests and returns a slice of the missing ones.
//
// This method is useful when a large number of digests is already known. For other use cases, consider the streaming uploader.
// This method does not use internal processors and does not use the uploader's context. It is safe to use even if the uploader's context is cancelled.
//
// The digests are batched based on ItemLimits of the gRPC config. BytesLimit and BundleTimeout are not used in this method.
// Errors from a batch do not affect other batches, but all digests from such bad batches will be reported as missing by this call.
// In other words, if an error is returned, any digest that is not in the returned slice is not missing.
// If no error is returned, the returned slice contains all the missing digests.
func (u *BatchingUploader) MissingBlobs(ctx context.Context, digests []digest.Digest) ([]digest.Digest, error) {
	contextmd.Infof(ctx, log.Level(1), "[casng] batch.query; len=%d", len(digests))
	if len(digests) == 0 {
		return nil, nil
	}

	// Deduplicate and split into batches.
	var batches [][]*repb.Digest
	var batch []*repb.Digest
	dgSet := make(map[digest.Digest]struct{})
	for _, d := range digests {
		if _, ok := dgSet[d]; ok {
			continue
		}
		dgSet[d] = struct{}{}
		batch = append(batch, d.ToProto())
		if len(batch) >= u.queryRPCCfg.ItemsLimit {
			batches = append(batches, batch)
			batch = nil
		}
	}
	if len(batch) > 0 {
		batches = append(batches, batch)
	}
	if len(batches) == 0 {
		return nil, nil
	}
	contextmd.Infof(ctx, log.Level(1), "[casng] batch.query.deduped; len=%d", len(dgSet))

	// Call remote.
	missing := make([]digest.Digest, 0, len(dgSet))
	var err error
	var res *repb.FindMissingBlobsResponse
	var errRes error
	req := &repb.FindMissingBlobsRequest{InstanceName: u.instanceName}
	for _, batch := range batches {
		req.BlobDigests = batch
		errRes = retry.WithPolicy(ctx, u.queryRPCCfg.RetryPredicate, u.queryRPCCfg.RetryPolicy, func() error {
			ctx, ctxCancel := context.WithTimeout(ctx, u.queryRPCCfg.Timeout)
			defer ctxCancel()
			res, errRes = u.cas.FindMissingBlobs(ctx, req)
			return errRes
		})
		if res == nil {
			res = &repb.FindMissingBlobsResponse{}
		}
		if errRes != nil {
			err = errors.Join(errRes, err)
			res.MissingBlobDigests = batch
		}
		for _, d := range res.MissingBlobDigests {
			missing = append(missing, digest.NewFromProtoUnvalidated(d))
		}
	}
	contextmd.Infof(ctx, log.Level(1), "[casng] batch.query.done; missing=%d", len(missing))

	if err != nil {
		err = errors.Join(ErrGRPC, err)
	}
	return missing, err
}

// WriteBytes uploads all the bytes of r directly to the resource name starting remotely at offset.
//
// r must return io.EOF to terminate the call.
//
// ctx is used to make and cancel remote calls.
// This method does not use the uploader's context which means it is safe to call even after that context is cancelled.
//
// Compression is turned on based on the resource name.
// size is used to report stats. It must reflect the actual number of bytes r has to give.
// The server is notified to finalize the resource name and subsequent writes may not succeed.
// The errors returned are either from the context, ErrGRPC, ErrIO, or ErrCompression. More errors may be wrapped inside.
// If an error was returned, the returned stats may indicate that all the bytes were sent, but that does not guarantee that the server committed all of them.
func (u *BatchingUploader) WriteBytes(ctx context.Context, name string, r io.Reader, size, offset int64) (Stats, error) {
	startTime := time.Now()
	if !u.streamThrottle.acquire(ctx) {
		return Stats{}, ctx.Err()
	}
	defer u.streamThrottle.release()
	log.V(3).Infof("[casng] upload.write_bytes.throttle.duration; start=%d, end=%d", startTime.UnixNano(), time.Now().UnixNano())
	return u.writeBytes(ctx, name, r, size, offset, true)
}

// WriteBytesPartial is the same as WriteBytes, but does not notify the server to finalize the resource name.
func (u *BatchingUploader) WriteBytesPartial(ctx context.Context, name string, r io.Reader, size, offset int64) (Stats, error) {
	startTime := time.Now()
	if !u.streamThrottle.acquire(ctx) {
		return Stats{}, ctx.Err()
	}
	defer u.streamThrottle.release()
	log.V(3).Infof("[casng] upload.write_bytes.throttle.duration; start=%d, end=%d", startTime.UnixNano(), time.Now().UnixNano())
	return u.writeBytes(ctx, name, r, size, offset, false)
}

func (u *uploader) writeBytes(ctx context.Context, name string, r io.Reader, size, offset int64, finish bool) (Stats, error) {
	contextmd.Infof(ctx, log.Level(1), "[casng] upload.write_bytes; name=%s, size=%d, offset=%d, finish=%t", name, size, offset, finish)
	defer contextmd.Infof(ctx, log.Level(1), "[casng] upload.write_bytes.done; name=%s, size=%d, offset=%d, finish=%t", name, size, offset, finish)
	if log.V(3) {
		startTime := time.Now()
		defer func() {
			log.Infof("[casng] upload.write_bytes.duration; start=%d, end=%d, name=%s, size=%d, chunk_size=%d", startTime.UnixNano(), time.Now().UnixNano(), name, size, u.ioCfg.BufferSize)
		}()
	}

	var stats Stats
	// Read raw bytes if compression is disabled.
	src := r

	// If compression is enabled, plug in the encoder via a pipe.
	var errEnc error
	var nRawBytes int64 // Track the actual number of the consumed raw bytes.
	var encWg sync.WaitGroup
	var withCompression bool // Used later to ensure the pipe is closed.
	if IsCompressedWriteResourceName(name) {
		contextmd.Infof(ctx, log.Level(1), "[casng] upload.write_bytes.compressing; name=%s, size=%d", name, size)
		withCompression = true
		pr, pw := io.Pipe()
		// Closing pr always returns a nil error, but also sends ErrClosedPipe to pw.
		defer pr.Close()
		src = pr // Read compressed bytes instead of raw bytes.

		enc := u.zstdEncoders.Get().(*zstd.Encoder)
		defer u.zstdEncoders.Put(enc)
		// (Re)initialize the encoder with this writer.
		enc.Reset(pw)
		// Get it going.
		encWg.Add(1)
		go func() {
			defer encWg.Done()
			// Closing pw always returns a nil error, but also sends an EOF to pr.
			defer pw.Close()

			// The encoder will theoretically read continuously. However, pw will block it
			// while pr is not reading from the other side.
			// In other words, the chunk size of the encoder's output is controlled by the reader.
			nRawBytes, errEnc = enc.ReadFrom(r)
			// Closing the encoder is necessary to flush remaining bytes.
			errClose := enc.Close()
			if errClose != nil {
				errEnc = errors.Join(errClose, errEnc)
			}
			if errors.Is(errEnc, io.ErrClosedPipe) {
				// pr was closed first, which means the actual error is on that end.
				errEnc = nil
			}
		}()
	}

	ctx, ctxCancel := context.WithCancel(ctx)
	defer ctxCancel()

	stream, errStream := u.byteStream.Write(ctx)
	if errStream != nil {
		return stats, errors.Join(ErrGRPC, errStream)
	}

	buf := *(u.buffers.Get().(*[]byte))
	defer u.buffers.Put(&buf)

	cacheHit := false
	var err error
	req := &bspb.WriteRequest{
		ResourceName: name,
		WriteOffset:  offset,
	}
	for {
		n, errRead := src.Read(buf)
		if errRead != nil && errRead != io.EOF {
			err = errors.Join(ErrIO, errRead, err)
			break
		}

		n64 := int64(n)
		stats.EffectiveBytesMoved += n64

		req.Data = buf[:n]
		req.FinishWrite = finish && errRead == io.EOF
		errStream := retry.WithPolicy(ctx, u.streamRPCCfg.RetryPredicate, u.streamRPCCfg.RetryPolicy, func() error {
			timer := time.NewTimer(u.streamRPCCfg.Timeout)
			// Ensure the timer goroutine terminates if Send does not timeout.
			success := make(chan struct{})
			defer close(success)
			go func() {
				select {
				case <-timer.C:
					ctxCancel() // Cancel the stream to allow Send to return.
				case <-success:
				}
			}()
			stats.TotalBytesMoved += n64
			return stream.Send(req)
		})
		// The server says the content for the specified resource already exists.
		if errStream == io.EOF {
			cacheHit = true
			break
		}

		if errStream != nil {
			err = errors.Join(ErrGRPC, errStream, err)
			break
		}

		req.WriteOffset += n64

		// The reader is done (interrupted or completed).
		if errRead == io.EOF {
			break
		}
	}

	// In case of a cache hit or an error, the pipe must be closed to terminate the encoder's goroutine
	// which would have otherwise terminated after draining the reader.
	if srcCloser, ok := src.(io.Closer); ok && withCompression {
		if errClose := srcCloser.Close(); errClose != nil {
			err = errors.Join(ErrIO, errClose, err)
		}
	}

	// This theoretically will block until the encoder's goroutine has returned, which is the happy path.
	// If the reader failed without the encoder's knowledge, closing the pipe will trigger the encoder to terminate, which is done above.
	// In any case, waiting here is necessary because the encoder's goroutine currently owns errEnc and nRawBytes.
	encWg.Wait()
	if errEnc != nil {
		err = errors.Join(ErrCompression, errEnc, err)
	}

	// Capture stats before processing errors.
	stats.BytesRequested = size
	stats.LogicalBytesMoved = stats.EffectiveBytesMoved
	if withCompression {
		// nRawBytes may be smaller than compressed bytes (additional headers without effective compression).
		stats.LogicalBytesMoved = nRawBytes
	}
	if cacheHit {
		stats.LogicalBytesCached = size
	}
	stats.LogicalBytesStreamed = stats.LogicalBytesMoved
	if cacheHit {
		stats.CacheHitCount = 1
	} else {
		stats.CacheMissCount = 1
	}
	if err == nil {
		stats.StreamedCount = 1
	}

	res, errClose := stream.CloseAndRecv()
	if errClose != nil {
		return stats, errors.Join(ErrGRPC, errClose, err)
	}

	// CommittedSize is based on the uncompressed size of the blob.
	if !cacheHit && res.CommittedSize != size {
		err = errors.Join(ErrGRPC, fmt.Errorf("committed size mismatch: got %d, want %d", res.CommittedSize, size), err)
	}

	return stats, err
}

// Upload processes reqs for upload.
//
// Blobs that already exist in the CAS are not uploaded.
// Any path that is not a regular file, a directory or a symlink file is skipped (e.g. sockets and pipes).
//
// Cancelling ctx gracefully aborts the upload process.
//
// Requests are unified across a window of time defined by the BundleTimeout value of the gRPC configuration.
// The unification is affected by the order of the requests, bundle limits (length, size, timeout) and the upload speed.
// With infinite speed and limits, every blob will be uploaded exactly once. On the other extreme, every blob is uploaded
// alone and no unification takes place.
// In the average case, blobs that make it into the same bundle will be unified (deduplicated).
//
// Returns a slice of the digests of the blobs that were uploaded (did not exist in the CAS).
// If the returned error is nil, any digest that is not in the returned slice was already in the CAS.
// If the returned error is not nil, the returned slice may be incomplete (fatal error) and every digest
// in it may or may not have been successfully uploaded (individual errors).
// The returned error wraps a number of errors proportional to the length of the specified slice.
//
// This method must not be called after cancelling the uploader's context.
func (u *BatchingUploader) Upload(ctx context.Context, reqs ...UploadRequest) ([]digest.Digest, Stats, error) {
	contextmd.Infof(ctx, log.Level(1), "[casng] upload; reqs=%d", len(reqs))
	defer contextmd.Infof(ctx, log.Level(1), "[casng] upload.done; reqs=%d", len(reqs))

	var stats Stats

	if len(reqs) == 0 {
		return nil, stats, nil
	}

	var undigested []UploadRequest
	digested := make(map[digest.Digest]UploadRequest)
	var digests []digest.Digest
	for _, r := range reqs {
		if r.Digest.Hash == "" {
			undigested = append(undigested, r)
			continue
		}
		digested[r.Digest] = r
		digests = append(digests, r.Digest)
	}
	missing, err := u.MissingBlobs(ctx, digests)
	if err != nil {
		return nil, stats, err
	}
	contextmd.Infof(ctx, log.Level(1), "[casng] upload; missing=%d, undigested=%d", len(missing), len(undigested))

	reqs = undigested
	for _, d := range missing {
		reqs = append(reqs, digested[d])
		delete(digested, d)
	}
	for d := range digested {
		stats.BytesRequested += d.Size
		stats.LogicalBytesCached += d.Size
		stats.CacheHitCount++
		stats.DigestCount++
	}
	if len(reqs) == 0 {
		contextmd.Infof(ctx, log.Level(1), "[casng] upload: nothing is missing")
		return nil, stats, nil
	}

	contextmd.Infof(ctx, log.Level(1), "[casng] upload: uploading; reqs=%d", len(reqs))
	ch := make(chan UploadRequest)
	resCh := u.streamPipe(ctx, ch)

	u.clientSenderWg.Add(1)
	go func() {
		defer close(ch) // let the streamer terminate.
		defer u.clientSenderWg.Done()

		contextmd.Infof(ctx, log.Level(1), "[casng] upload.sender.start")
		defer contextmd.Infof(ctx, log.Level(1), "[casng] upload.sender.stop")

		for _, r := range reqs {
			select {
			case ch <- r:
			case <-ctx.Done():
				return
			}
		}
	}()

	var uploaded []digest.Digest
	for r := range resCh {
		if r.Err != nil {
			err = errors.Join(r.Err, err)
		}
		stats.Add(r.Stats)
		// For pure stat responses, i.e. r.Digest is zero, CacheMissCount will be equal to 0.
		if r.Stats.CacheMissCount > 0 {
			uploaded = append(uploaded, r.Digest)
		}
	}

	return uploaded, stats, err
}

// DigestTree returns the digest of the merkle tree for root.
func (u *BatchingUploader) DigestTree(ctx context.Context, root impath.Absolute, slo symlinkopts.Options, exclude walker.Filter) (digest.Digest, Stats, error) {
	ch := make(chan UploadRequest)
	resCh := u.streamPipe(ctx, ch)

	req := UploadRequest{Path: root, SymlinkOptions: slo, Exclude: exclude, ctx: ctx, digestOnly: true}
	select {
	// In both cases, the channel must be closed before proceeding beyong the select statement to ensure
	// the proper closure of resCh.
	case ch <- req:
		close(ch)
	case <-ctx.Done():
		close(ch)
		return digest.Digest{}, Stats{}, ctx.Err()
	}

	var stats Stats
	var err error
	for r := range resCh {
		stats.Add(r.Stats)
		if r.Err != nil {
			err = errors.Join(r.Err, err)
		}
	}
	rootNode := u.Node(req)
	if rootNode == nil {
		err = errors.Join(fmt.Errorf("root node was not found for %q", root), err)
		return digest.Digest{}, stats, err
	}
	dg := digest.NewFromProtoUnvalidated(rootNode.(namedDigest).GetDigest())
	return dg, stats, err
}

// UploadTree is a convenient method to upload a tree described with multiple requests.
//
// This is useful when the list of paths is known and the root might have too many descendants such that traversing and filtering might add a significant overhead.
//
// All requests must share the same filter. Digest fields on the requests are ignored to ensure proper hierarchy caching via the internal digestion process.
// remoteWorkingDir replaces workingDir inside the merkle tree such that the server is only aware of remoteWorkingDir.
func (u *BatchingUploader) UploadTree(ctx context.Context, execRoot impath.Absolute, workingDir, remoteWorkingDir impath.Relative, reqs ...UploadRequest) (rootDigest digest.Digest, uploaded []digest.Digest, stats Stats, err error) {
	contextmd.Infof(ctx, log.Level(1), "[casng] upload.tree; reqs=%d", len(reqs))
	defer contextmd.Infof(ctx, log.Level(1), "[casng] upload.tree.done")

	if len(reqs) == 0 {
		return
	}

	// 1st, Preprocess the set to deduplicate by ancestor and generate a deterministic filter ID.

	// Multiple reqs sets may share some of the paths which would cause the the u.dirChildren lookup below to mix children from two different sets which would corrupt the merkle tree.
	// Updating the filterID for the set to a deterministic one ensures it gets its unique keys that are still shared between identical sets.
	// The deterministic ID is the digest of the sorted list of paths concatenated with the ID of the original filter.
	filter := reqs[0].Exclude
	filterID := filter.String()
	paths := make([]string, 0, len(reqs)+1)
	paths = append(paths, filterID)

	// An ancestor directory takes precedence over all of its descendants to ensure unlisted files are also included in traversal.
	// Sorting the requests by path length ensures ancestors appear before descendants.
	sort.Slice(reqs, func(i, j int) bool { return len(reqs[i].Path.String()) < len(reqs[j].Path.String()) })
	// Fast lookup for potentially shared paths between requests.
	pathSeen := make(map[impath.Absolute]bool, len(reqs))
	localPrefix := execRoot.Append(workingDir)
	i := 0
	for _, r := range reqs {
		if r.Exclude.String() != filterID {
			err = fmt.Errorf("cannot create a tree from requests with different exclusion filters: %q and %q", filterID, r.Exclude.String())
			return
		}

		// Clear the digest to ensure proper hierarchy caching in the digester.
		r.Digest.Hash = ""
		r.Digest.Size = 0

		if pathSeen[r.Path] {
			continue
		}
		pathSeen[r.Path] = true
		parent := r.Path.Dir()
		skip := false
		for ; parent.String() != execRoot.String() && parent.String() != localPrefix.String(); parent = parent.Dir() {
			if pathSeen[parent] {
				// First iteration, parent == r.Path, and this means a duplicate request because the list is sorted (If not sorted, it may also mean a redundant ancestor).
				// Other iterations, it means this request is redundant because an ancestor request already exist.
				skip = true
				break
			}
		}
		if skip {
			continue
		}
		paths = append(paths, r.Path.String())

		// Shift left to accumulate included requests at the beginning of the array to avoid allocating another one.
		reqs[i] = r
		i++
	}

	// Reslice to take included (shifted) requests only.
	reqs = reqs[:i]
	// paths is already sorted because reqs was sorted.
	filterID = digest.NewFromBlob([]byte(strings.Join(paths, ""))).String()
	filterIDFunc := func() string { return filterID }
	for i := range reqs {
		r := &reqs[i]
		r.Exclude.ID = filterIDFunc
	}
	contextmd.Infof(ctx, log.Level(1), "[casng] upload.tree; filter_id=%s, got=%d, uploading=%d", filterID, len(reqs), i)

	// 2nd, Upload the requests first to digest the files and cache the nodes.
	uploaded, stats, err = u.Upload(ctx, reqs...)
	if err != nil {
		return
	}

	// 3rd, Compute the shared ancestor nodes and upload them.

	// This block creates a flattened tree of the paths in reqs rooted at execRoot.
	// Each key is an absolute path to a node in the tree and its value is a list of absolute paths that any of them can be a key as well.
	// Example: /a: [/a/b /a/c], /a/b: [/a/b/foo.go], /a/c: [/a/c/bar.go]
	dirChildren := make(map[impath.Absolute]map[impath.Absolute]proto.Message, len(pathSeen))
	for _, r := range reqs {
		// Each request in reqs must correspond to a cached node.
		node := u.Node(r)
		if node == nil {
			err = fmt.Errorf("[casng] upload.tree; cannot construct the merkle tree with a missing node for path %q", r.Path)
			return
		}

		// Add this leaf node to its ancestors.
		// Start by swapping the working directory with the remote one.
		rp, errIm := ReplaceWorkingDir(r.Path, execRoot, workingDir, remoteWorkingDir)
		if errIm != nil {
			err = errors.Join(fmt.Errorf("[casng] upload.tree; cannot construct the merkle tree with a path outside the root: path=%q, root=%q, working_dir=%q, remote_working_dir=%q", r.Path, execRoot, workingDir, remoteWorkingDir), errIm)
			return
		}
		parent := rp
		for {
			rp = parent
			parent = parent.Dir()
			children := dirChildren[parent]
			if children == nil {
				children = make(map[impath.Absolute]proto.Message, len(reqs)) // over-allocating to avoid copying on growth.
				dirChildren[parent] = children
			}
			// If the parent already has this child, then no need to traverse up the tree again.
			_, ancestrySeen := children[rp]
			children[rp] = node
			// Only the immediate parent should have the leaf node.
			node = nil

			// Stop if ancestors are already processed and do not go beyond the root.
			if ancestrySeen || parent.String() == execRoot.String() {
				break
			}
		}
	}

	// This block generates directory nodes for shared ancestors starting from leaf nodes (DFS-style).
	dirReqs := make([]UploadRequest, 0, len(dirChildren))
	stack := make([]impath.Absolute, 0, len(dirChildren))
	stack = append(stack, execRoot)
	var logPathDigest map[string]*repb.Digest
	if log.V(5) {
		logPathDigest = make(map[string]*repb.Digest, len(dirChildren))
	}
	for len(stack) > 0 {
		// Peek.
		dir := stack[len(stack)-1]

		// Depth first.
		children := dirChildren[dir]
		pending := false
		for child, node := range children {
			if node == nil {
				pending = true
				stack = append(stack, child)
			}
		}
		if pending {
			continue
		}

		// Pop.
		stack = stack[:len(stack)-1]
		childrenNodes := make([]proto.Message, 0, len(children))
		for _, n := range children {
			childrenNodes = append(childrenNodes, n)
			if log.V(5) {
				nd := n.(namedDigest)
				logPathDigest[dir.Append(impath.MustRel(nd.GetName())).String()] = nd.GetDigest()
			}
		}

		node, b, errDigest := digestDirectory(dir, childrenNodes)
		if errDigest != nil {
			err = errDigest
			return
		}
		dirReqs = append(dirReqs, UploadRequest{Bytes: b, Digest: digest.NewFromProtoUnvalidated(node.Digest)})
		if log.V(5) {
			logPathDigest[dir.String()] = node.GetDigest()
		}
		if dir.String() == execRoot.String() {
			rootDigest = digest.NewFromProtoUnvalidated(node.Digest)
			break
		}
		// Attach the node to its parent if it's not the exec root.
		dirChildren[dir.Dir()][dir] = node
	}

	// Upload the blobs of the shared ancestors.
	contextmd.Infof(ctx, log.Level(1), "[casng] upload.tree; dirs=%d, filter_id=%s", len(dirReqs), filterID)
	moreUploaded, moreStats, moreErr := u.Upload(ctx, dirReqs...)
	if moreErr != nil {
		err = moreErr
	}
	stats.Add(moreStats)
	uploaded = append(uploaded, moreUploaded...)

	// b/291294771: remove after deprecation.
	if log.V(5) {
		s, ok := ctx.Value("ng_tree").(*string)
		if !ok {
			return
		}
		paths := make([]string, 0, len(reqs))
		for _, r := range reqs {
			paths = append(paths, r.Path.String())
		}
		dirs := make([]string, 0, len(dirChildren))
		for p := range dirChildren {
			dirs = append(dirs, p.String())
		}
		sort.Strings(dirs)
		treePaths := make([]string, 0, len(logPathDigest))
		for p := range logPathDigest {
			treePaths = append(treePaths, p)
		}
		sort.Strings(treePaths)
		sb := strings.Builder{}
		sb.WriteString("  filter_id=")
		sb.WriteString(filterID)
		sb.WriteString("\n")
		sb.WriteString("  paths:\n  ")
		sb.WriteString(strings.Join(paths, "\n  "))
		sb.WriteString("\n  dir_paths:\n  ")
		sb.WriteString(strings.Join(dirs, "\n  "))
		sb.WriteString("\n  tree:\n")
		for _, p := range treePaths {
			pp, _ := filepath.Rel(execRoot.String(), p)
			sb.WriteString(fmt.Sprintf("  %s: %s\n", pp, logPathDigest[p]))
		}
		*s = sb.String()
	}

	return
}

// ReplaceWorkingDir swaps remoteWorkingDir for workingDir in path which must be prefixed by root.
// workingDir is assumed to be prefixed by root, and the returned path will be a descendant of root, but not necessarily a descendant of remoteWorkingDir.
// Example: path=/root/out/foo.c, root=/root, workdingDir=out/reclient, remoteWorkingDir=set_by_reclient/a, result=/root/set_by_reclient/foo.c
func ReplaceWorkingDir(path, root impath.Absolute, workingDir, remoteWorkingDir impath.Relative) (impath.Absolute, error) {
	if !strings.HasPrefix(path.String(), root.String()) {
		return impath.Absolute{}, fmt.Errorf("cannot replace working dir for path %q because it is not prefixed by %q", path, root)
	}

	p := strings.TrimPrefix(path.String(), root.String()) // if root == '/', p is now relative
	// if root isn't the system root, there would be a leading path separator.
	p = strings.TrimPrefix(p, string(filepath.Separator))

	p, err := filepath.Rel(workingDir.String(), p)
	if err != nil {
		return impath.Absolute{}, err
	}

	rp, err := impath.Rel(remoteWorkingDir.String(), p)
	if err != nil {
		return impath.Absolute{}, err
	}
	return root.Append(rp), nil
}
