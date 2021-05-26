package cas

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/golang/protobuf/proto"
	"github.com/klauspost/compress/zstd"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/support/bundler"
	"google.golang.org/grpc/status"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/cache"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

// ErrFilteredSymlinkTarget is returned when a symlink's target was filtered out
// via PathSpec.Exclude or ErrSkip, while the symlink itself wasn't.
var ErrFilteredSymlinkTarget = errors.New("symlink's target was filtered out")

// zstdEncoders is a pool of ZStd encoders.
// Clients of this pool must call Close() on the encoder after using the
// encoder.
var zstdEncoders = sync.Pool{
	New: func() interface{} {
		enc, _ := zstd.NewWriter(nil)
		return enc
	},
}

// PathSpec specifies a subset of the file system.
type PathSpec struct {
	// Path to the file or a directory to upload.
	// Must be absolute.
	Path string

	// Exclude is a file/dir filter. If Exclude is not nil and the
	// absolute path of a file/dir match this regexp, then the file/dir is skipped.
	// Forward-slash-separated paths are matched aginst the regexp: PathExclude
	// does not have to be conditional on the OS.
	// If the Path is a directory, then the filter is evaluated against each file
	// in the subtree.
	// See ErrSkip comments for more details on semantics regarding excluding symlinks .
	Exclude *regexp.Regexp
}

// TransferStats is upload/download statistics.
type TransferStats struct {
	CacheHits   DigestStat
	CacheMisses DigestStat

	Streamed DigestStat // streamed transfers
	Batched  DigestStat // batched transfers
}

// DigestStat is aggregated statistics over a set of digests.
type DigestStat struct {
	Digests int64 // number of unique digests
	Bytes   int64 // total sum of of digest sizes

	// TODO(nodir): add something like TransferBytes, i.e. how much was actually transfered
}

// UploadOptions is optional configuration for Upload function.
// The default options are the zero value of this struct.
type UploadOptions struct {
	// PreserveSymlinks specifies whether to preserve symlinks or convert them
	// to regular files.
	PreserveSymlinks bool

	// AllowDanglingSymlinks specifies whether to upload dangling links or halt
	// the upload with an error.
	//
	// This field is ignored if PreserveSymlinks is false, which is the default.
	AllowDanglingSymlinks bool

	// Prelude is called for each file/dir to be read and uploaded.
	// If it returns an error which is ErrSkip according to errors.Is, then the
	// file/dir is not processed.
	// If it returns another error, then the upload is halted with that error.
	//
	// Prelude might be called multiple times for the same file if different
	// PathSpecs directly/indirectly refer to the same file, but with different
	// PathSpec.Exclude.
	//
	// Prelude is called from different goroutines.
	Prelude func(absPath string, mode os.FileMode) error
}

// ErrSkip when returned by UploadOptions.Prelude, means the file/dir must be
// not be uploaded.
//
// Note that if UploadOptions.PreserveSymlinks is true and the ErrSkip is
// returned for a symlink target, but not the symlink itself, then it may
// result in a dangling symlink.
var ErrSkip = errors.New("skip file")

// Upload uploads all files/directories specified by pathC.
//
// Close pathC to indicate that there are no more files/dirs to upload.
// When pathC is closed, Upload finishes uploading the remaining files/dirs and
// exits successfully.
//
// If ctx is canceled, the Upload returns with an error.
func (c *Client) Upload(ctx context.Context, opt UploadOptions, pathC <-chan *PathSpec) (stats *TransferStats, err error) {
	eg, ctx := errgroup.WithContext(ctx)
	// Do not exit until all sub-goroutines exit, to prevent goroutine leaks.
	defer eg.Wait()

	u := &uploader{
		Client:        c,
		UploadOptions: opt,
		eg:            eg,
	}

	// Initialize checkBundler, which checks if a blob is present on the server.
	var wgChecks sync.WaitGroup
	u.checkBundler = bundler.NewBundler(&uploadItem{}, func(items interface{}) {
		wgChecks.Add(1)
		// Handle errors and context cancelation via errgroup.
		eg.Go(func() error {
			defer wgChecks.Done()
			return u.check(ctx, items.([]*uploadItem))
		})
	})
	// Given that all digests are small (no more than 40 bytes), the count limit
	// is the bottleneck.
	// We might run into the request size limits only if we have >100K digests.
	u.checkBundler.BundleCountThreshold = u.Config.FindMissingBlobs.MaxItems

	// Initialize batchBundler, which uploads blobs in batches.
	u.batchBundler = bundler.NewBundler(&repb.BatchUpdateBlobsRequest_Request{}, func(subReq interface{}) {
		// Handle errors and context cancelation via errgroup.
		eg.Go(func() error {
			return u.uploadBatch(ctx, subReq.([]*repb.BatchUpdateBlobsRequest_Request))
		})
	})
	// Limit the sum of sub-request sizes to (maxRequestSize - requestOverhead).
	// Subtract 1KB to be on the safe side.
	u.batchBundler.BundleByteLimit = c.Config.BatchUpdateBlobs.MaxSizeBytes - int(marshalledFieldSize(int64(len(c.InstanceName)))) - 1000
	u.batchBundler.BundleCountThreshold = c.Config.BatchUpdateBlobs.MaxItems

	// Start processing path specs.
	eg.Go(func() error {
		// Before exiting this main goroutine, ensure all the work has been completed.
		// Just waiting for u.eg isn't enough because some work may be temporarily
		// in a bundler.
		defer func() {
			u.wgFS.Wait()
			u.checkBundler.Flush() // only after FS walk is done.
			wgChecks.Wait()        // only after checkBundler is flushed
			u.batchBundler.Flush() // only after wgChecks is done.
		}()

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case ps, ok := <-pathC:
				if !ok {
					return nil
				}
				if err := u.startProcessing(ctx, ps); err != nil {
					return err
				}
			}
		}
	})
	return &u.stats, eg.Wait()
}

// uploader implements a concurrent multi-stage pipeline to read blobs from the
// file system, check their presence on the server and then upload if necessary.
// Common blobs are deduplicated.
//
// uploader.eg is used to schedule work, while concurrency of individual
// expensive operations is controlled via separate semaphores.
//
// Special care is taken for large files: they are read sequentially, opened
// only once per file, and read with large IO size.
//
// Note: uploader shouldn't store semaphores/locks that protect global
// resources, such as file system. They should be stored in the Client instead.
type uploader struct {
	*Client
	UploadOptions
	eg    *errgroup.Group
	stats TransferStats

	// wgFS is used to wait for all FS walking to finish.
	wgFS sync.WaitGroup

	// fsCache contains already-processed files.
	fsCache cache.SingleFlight

	// checkBundler bundles digests that need to be checked for presence on the
	// server.
	checkBundler *bundler.Bundler
	seenDigests  sync.Map // TODO: consider making it more global

	// batchBundler bundles blobs that can be uploaded using UploadBlobs RPC.
	batchBundler *bundler.Bundler
}

// startProcessing adds the item to the appropriate stage depending on its type.
func (u *uploader) startProcessing(ctx context.Context, ps *PathSpec) error {
	if !filepath.IsAbs(ps.Path) {
		return errors.Errorf("%q is not absolute", ps.Path)
	}

	// Schedule a file system walk.
	u.wgFS.Add(1)
	u.eg.Go(func() error {
		defer u.wgFS.Done()
		// Do not use os.Stat() here. We want to know if it is a symlink.
		info, err := os.Lstat(ps.Path)
		if err != nil {
			return errors.WithStack(err)
		}

		_, err = u.visitPath(ctx, ps.Path, info, ps.Exclude)
		return errors.Wrapf(err, "%q", ps.Path)
	})
	return nil
}

// visitPath visits the file/dir depending on its type (regular, dir, symlink).
// Visits each file only once.
//
// If the file should be skipped, then returns (nil, nil).
func (u *uploader) visitPath(ctx context.Context, absPath string, info os.FileInfo, pathExclude *regexp.Regexp) (dirEntry proto.Message, err error) {
	// First, check if the file passes all filters.
	if pathExclude != nil && pathExclude.MatchString(filepath.ToSlash(absPath)) {
		return nil, nil
	}
	// Call the Prelude only after checking the pathExclude.
	if u.Prelude != nil {
		switch err := u.Prelude(absPath, info.Mode()); {
		case errors.Is(err, ErrSkip):
			return nil, nil
		case err != nil:
			return nil, err
		}
	}

	// Make a cache key.
	type cacheKeyType struct {
		AbsPath       string
		ExcludeRegexp string
	}
	cacheKey := cacheKeyType{
		AbsPath: absPath,
	}
	// Incorporate the pathExclude, unless it is a regular file.
	// If it is a regular file, then there's no need to include pathExclude in the
	// cache key, as we already know the regex does not match the file and the
	// exclusion isn't propagated.
	if pathExclude != nil && !info.Mode().IsRegular() {
		cacheKey.ExcludeRegexp = pathExclude.String()
	}

	node, err := u.fsCache.LoadOrStore(cacheKey, func() (interface{}, error) {
		switch {
		case info.Mode()&os.ModeSymlink == os.ModeSymlink:
			return u.visitSymlink(ctx, absPath, pathExclude)
		case info.Mode().IsDir():
			return u.visitDir(ctx, absPath, pathExclude)
		case info.Mode().IsRegular():
			// Code above assumes that pathExclude is not used here.
			return u.visitRegularFile(ctx, absPath, info)
		default:
			return nil, fmt.Errorf("unexpected file mode %s", info.Mode())
		}
	})
	if err != nil {
		return nil, err
	}
	return node.(proto.Message), nil
}

// visitRegularFile computes the hash of a regular file and schedules a presence
// check.
//
// It distinguishes three categories of file sizes:
//  - small: small files are buffered in memory entirely, thus read only once.
//    See also ClientConfig.SmallFileThreshold.
//  - medium: the hash is computed, the file is closed and a presence check is
//    scheduled.
//  - large: the hash is computed, the file is rewinded without closing and
//    streamed via ByteStream.
//    If the file is already present on the server, the ByteStream preempts
//    the stream with EOF and WriteResponse.CommittedSize == Digest.Size.
//    Rewinding helps locality: there is no delay between reading the file for
//    the first and the second times.
//    Only one large file is processed at a time because most GCE disks are
//    network disks. Reading many large files concurrently appears to saturate
//    the network and slows down the progress.
//    See also ClientConfig.LargeFileThreshold.
func (u *uploader) visitRegularFile(ctx context.Context, absPath string, info os.FileInfo) (*repb.FileNode, error) {
	isLarge := info.Size() >= u.Config.LargeFileThreshold

	// Lock the mutex before acquiring a semaphore to avoid hogging the latter.
	if isLarge {
		// Read only one large file at a time.
		u.muLargeFile.Lock()
		defer u.muLargeFile.Unlock()
	}

	if err := u.semFileIO.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	defer u.semFileIO.Release(1)

	f, err := u.openFileSource(absPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	ret := &repb.FileNode{
		Name:         info.Name(),
		IsExecutable: (info.Mode() & 0100) != 0,
	}

	if info.Size() <= u.Config.SmallFileThreshold {
		// This file is small enough to buffer it entirely.
		contents, err := ioutil.ReadAll(f)
		if err != nil {
			return nil, err
		}
		item := uploadItemFromBlob(absPath, contents)
		ret.Digest = item.Digest
		return ret, u.scheduleCheck(ctx, item)
	}

	// It is a medium or large file.

	// Compute the hash.
	dig, err := digest.NewFromReader(f)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to compute hash")
	}
	ret.Digest = dig.ToProto()

	item := &uploadItem{
		Title:  absPath,
		Digest: ret.Digest,
	}

	if isLarge {
		// Large files are special: locality is important - we want to re-read the
		// file ASAP.
		// Also we are not going to use BatchUploads anyway, so we can take
		// advantage of ByteStream's built-in presence check.
		// https://github.com/bazelbuild/remote-apis/blob/0cd22f7b466ced15d7803e8845d08d3e8d2c51bc/build/bazel/remote/execution/v2/remote_execution.proto#L250-L254

		item.Open = func() (uploadSource, error) {
			return f, f.SeekStart(0)
		}
		return ret, u.stream(ctx, item, true)
	}

	// Schedule a check and close the file (in defer).
	// item.Open will reopen the file.

	item.Open = func() (uploadSource, error) {
		return u.openFileSource(absPath)
	}
	return ret, u.scheduleCheck(ctx, item)
}

func (u *uploader) openFileSource(absPath string) (uploadSource, error) {
	f, err := os.Open(absPath)
	if err != nil {
		return nil, err
	}
	return newFileSource(f, &u.fileBufReaders), nil
}

// visitDir reads a directory and its descendants. The function blocks until
// each descendant is visited, but the visitation happens concurrently, using
// u.eg.
func (u *uploader) visitDir(ctx context.Context, absPath string, pathExclude *regexp.Regexp) (*repb.DirectoryNode, error) {
	var mu sync.Mutex
	dir := &repb.Directory{}
	var subErr error
	var wgChildren sync.WaitGroup

	// This sub-function exist to avoid holding the semaphore while waiting for
	// children.
	err := func() error {
		if err := u.semFileIO.Acquire(ctx, 1); err != nil {
			return err
		}
		defer u.semFileIO.Release(1)

		f, err := os.Open(absPath)
		if err != nil {
			return err
		}
		defer f.Close()

		// Check the context, since file IO functions don't.
		for ctx.Err() == nil {
			infos, err := f.Readdir(128)
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}

			for _, info := range infos {
				info := info
				absChild := joinFilePathsFast(absPath, info.Name())
				wgChildren.Add(1)
				u.wgFS.Add(1)
				u.eg.Go(func() error {
					defer wgChildren.Done()
					defer u.wgFS.Done()
					node, err := u.visitPath(ctx, absChild, info, pathExclude)
					mu.Lock()
					defer mu.Unlock()
					if err != nil {
						subErr = err
						return err
					}

					switch node := node.(type) {
					case *repb.FileNode:
						dir.Files = append(dir.Files, node)
					case *repb.DirectoryNode:
						dir.Directories = append(dir.Directories, node)
					case *repb.SymlinkNode:
						dir.Symlinks = append(dir.Symlinks, node)
					case nil:
						// This file should be ignored.
					default:
						// This condition is impossible because all functions in this file
						// return one of the three types above.
						panic(fmt.Sprintf("unexpected node type %T", node))
					}
					return nil
				})
			}
		}
		return nil
	}()
	if err != nil {
		return nil, err
	}

	wgChildren.Wait()
	if subErr != nil {
		return nil, errors.Wrapf(subErr, "failed to read the directory %q entirely", absPath)
	}

	item := uploadItemFromDirMsg(absPath, dir)
	if err := u.scheduleCheck(ctx, item); err != nil {
		return nil, err
	}
	return &repb.DirectoryNode{
		Name:   filepath.Base(absPath),
		Digest: item.Digest,
	}, nil
}

// visitSymlink converts a symlink to a directory node and schedules visitation
// of the target file.
// If u.PreserveSymlinks is true, then returns a SymlinkNode, otherwise
// returns the directory node of the target file.
func (u *uploader) visitSymlink(ctx context.Context, absPath string, pathExclude *regexp.Regexp) (proto.Message, error) {
	target, err := os.Readlink(absPath)
	if err != nil {
		return nil, errors.Wrapf(err, "os.ReadLink")
	}

	// Determine absolute and relative paths of the target.
	var absTarget, relTarget string
	symlinkDir := filepath.Dir(absPath)
	target = filepath.Clean(target) // target may end with slash
	if filepath.IsAbs(target) {
		absTarget = target
		if relTarget, err = filepath.Rel(symlinkDir, absTarget); err != nil {
			return nil, err
		}
	} else {
		relTarget = target
		// Note: we can't use joinFilePathsFast here because relTarget may start
		// with "../".
		absTarget = filepath.Join(symlinkDir, relTarget)
	}

	symlinkNode := &repb.SymlinkNode{
		Name:   filepath.Base(absPath),
		Target: filepath.ToSlash(relTarget),
	}

	targetInfo, err := os.Lstat(absTarget)
	switch {
	case os.IsNotExist(err) && u.PreserveSymlinks && u.AllowDanglingSymlinks:
		// Special case for preserved dangling links.
		return symlinkNode, nil
	case err != nil:
		return nil, errors.WithStack(err)
	}

	switch targetNode, err := u.visitPath(ctx, absTarget, targetInfo, pathExclude); {
	case err != nil:
		return nil, err
	case !u.PreserveSymlinks:
		return targetNode, nil
	case targetNode == nil && !u.AllowDanglingSymlinks:
		// The target got skipped via Prelude or PathSpec.Exclude,
		// resulting in a dangling symlink, which is not allowed.
		return nil, errors.Wrapf(ErrFilteredSymlinkTarget, "path: %q, target: %q", absPath, target)
	default:
		// Note: even though we throw away targetNode, it was still important to visit the target.
		return symlinkNode, nil
	}
}

// uploadItem is a blob to potentially upload.
type uploadItem struct {
	Title  string
	Digest *repb.Digest
	Open   func() (uploadSource, error)
}

func (item *uploadItem) ReadAll() ([]byte, error) {
	r, err := item.Open()
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return ioutil.ReadAll(r)
}

// scheduleCheck schedules a blob presence check on the server. If it fails,
// then the blob is uploaded.
func (u *uploader) scheduleCheck(ctx context.Context, item *uploadItem) error {
	if u.testScheduleCheck != nil {
		return u.testScheduleCheck(ctx, item)
	}

	// Do not check the same digest twice.
	cacheKey := digest.NewFromProtoUnvalidated(item.Digest)
	if _, ok := u.seenDigests.LoadOrStore(cacheKey, struct{}{}); ok {
		return nil
	}
	return u.checkBundler.AddWait(ctx, item, 0)
}

// check checks which items are present on the server, and schedules upload for
// the missing ones.
func (u *uploader) check(ctx context.Context, items []*uploadItem) error {
	if err := u.semFindMissingBlobs.Acquire(ctx, 1); err != nil {
		return err
	}

	req := &repb.FindMissingBlobsRequest{
		InstanceName: u.InstanceName,
		BlobDigests:  make([]*repb.Digest, len(items)),
	}
	byDigest := make(map[digest.Digest]*uploadItem, len(items))
	totalBytes := int64(0)
	for i, item := range items {
		req.BlobDigests[i] = item.Digest
		byDigest[digest.NewFromProtoUnvalidated(item.Digest)] = item
		totalBytes += item.Digest.SizeBytes
	}

	var res *repb.FindMissingBlobsResponse
	err := u.unaryRPC(ctx, &u.Config.FindMissingBlobs, func(ctx context.Context) (err error) {
		res, err = u.cas.FindMissingBlobs(ctx, req)
		return
	})
	if err != nil {
		return err
	}

	missingBytes := int64(0)
	for _, d := range res.MissingBlobDigests {
		missingBytes += d.SizeBytes
		item := byDigest[digest.NewFromProtoUnvalidated(d)]
		if err := u.scheduleUpload(ctx, item); err != nil {
			return errors.Wrapf(err, "%q", item.Title)
		}
	}
	atomic.AddInt64(&u.stats.CacheMisses.Digests, int64(len(res.MissingBlobDigests)))
	atomic.AddInt64(&u.stats.CacheMisses.Bytes, missingBytes)
	atomic.AddInt64(&u.stats.CacheHits.Digests, int64(len(items)-len(res.MissingBlobDigests)))
	atomic.AddInt64(&u.stats.CacheHits.Bytes, totalBytes-missingBytes)
	return nil
}

func (u *uploader) scheduleUpload(ctx context.Context, item *uploadItem) error {
	// Check if this blob can be uploaded in a batch.
	if marshalledRequestSize(item.Digest) > int64(u.batchBundler.BundleByteLimit) {
		// There is no way this blob can fit in a batch request.
		u.eg.Go(func() error {
			return errors.Wrap(u.stream(ctx, item, false), item.Title)
		})
		return nil
	}

	// Since this blob is small enough, just read it entirely.
	contents, err := item.ReadAll()
	if err != nil {
		return errors.Wrapf(err, "failed to read the item")
	}
	req := &repb.BatchUpdateBlobsRequest_Request{Digest: item.Digest, Data: contents}
	return u.batchBundler.AddWait(ctx, req, proto.Size(req))
}

// uploadBatch uploads blobs in using BatchUpdateBlobs RPC.
func (u *uploader) uploadBatch(ctx context.Context, reqs []*repb.BatchUpdateBlobsRequest_Request) error {
	if err := u.semBatchUpdateBlobs.Acquire(ctx, 1); err != nil {
		return err
	}
	defer u.semBatchUpdateBlobs.Release(1)

	reqMap := make(map[digest.Digest]*repb.BatchUpdateBlobsRequest_Request, len(reqs))
	for _, r := range reqs {
		reqMap[digest.NewFromProtoUnvalidated(r.Digest)] = r
	}

	req := &repb.BatchUpdateBlobsRequest{
		InstanceName: u.InstanceName,
		Requests:     reqs,
	}
	return u.unaryRPC(ctx, &u.Config.BatchUpdateBlobs, func(ctx context.Context) error {
		res, err := u.cas.BatchUpdateBlobs(ctx, req)
		if err != nil {
			return err
		}

		bytesTransferred := int64(0)
		digestsTransferred := int64(0)
		var retriableErr error
		req.Requests = req.Requests[:0] // reset for the next attempt
		for _, r := range res.Responses {
			if err := status.FromProto(r.Status).Err(); err != nil {
				if !retry.TransientOnly(err) {
					return err
				}
				// This error is retriable. Save it to return later, and
				// save the failed sub-request for the next attempt.
				retriableErr = err
				req.Requests = append(req.Requests, reqMap[digest.NewFromProtoUnvalidated(r.Digest)])
				continue
			}
			bytesTransferred += r.Digest.SizeBytes
			digestsTransferred++
		}
		atomic.AddInt64(&u.stats.Batched.Bytes, bytesTransferred)
		atomic.AddInt64(&u.stats.Batched.Digests, digestsTransferred)
		return retriableErr
	})
}

// stream uploads the item using ByteStream service.
//
// If the blob is already uploaded, then the function returns quickly and
// without an error.
func (u *uploader) stream(ctx context.Context, item *uploadItem, updateCacheStats bool) error {
	if err := u.semByteStreamWrite.Acquire(ctx, 1); err != nil {
		return err
	}
	defer u.semByteStreamWrite.Release(1)

	// Open the item.
	r, err := item.Open()
	if err != nil {
		return err
	}
	defer r.Close()

	rewind := false
	return u.withRetries(ctx, func(ctx context.Context) error {
		// TODO(nodir): add support for resumable uploads.

		// Do not rewind if this is the first attempt.
		if rewind {
			if err := r.SeekStart(0); err != nil {
				return err
			}
		}
		rewind = true

		if u.Config.CompressedBytestreamThreshold < 0 || item.Digest.SizeBytes < u.Config.CompressedBytestreamThreshold {
			// No compression.
			return u.streamFromReader(ctx, r, item.Digest, false, updateCacheStats)
		}

		// Compress using an in-memory pipe. This is mostly to accomodate the fact
		// that zstd package expects a writer.
		// Note that using io.Pipe() means we buffer only bytes that were not uploaded yet.
		pr, pw := io.Pipe()

		enc := zstdEncoders.Get().(*zstd.Encoder)
		defer func() {
			enc.Close()
			zstdEncoders.Put(enc)
		}()
		enc.Reset(pw)

		// Read from disk and make RPCs concurrently.
		eg, ctx := errgroup.WithContext(ctx)
		eg.Go(func() error {
			switch _, err := enc.ReadFrom(r); {
			case err == io.ErrClosedPipe:
				// The other goroutine exited before we finished encoding.
				// Might be a cache hit or context cancelation.
				// In any case, the other goroutine has the actual error, so return nil
				// here.
				return nil
			case err != nil:
				return errors.Wrapf(err, "failed to read the file/blob")
			}

			if err := enc.Close(); err != nil {
				return errors.Wrapf(err, "failed to close the zstd encoder")
			}
			return pw.Close()
		})
		eg.Go(func() error {
			defer pr.Close()
			return u.streamFromReader(ctx, pr, item.Digest, true, updateCacheStats)
		})
		return eg.Wait()
	})
}

func (u *uploader) streamFromReader(ctx context.Context, r io.Reader, digest *repb.Digest, compressed, updateCacheStats bool) error {
	ctx, cancel, withTimeout := withPerCallTimeout(ctx, u.Config.ByteStreamWrite.Timeout)
	defer cancel()

	stream, err := u.byteStream.Write(ctx)
	if err != nil {
		return err
	}
	defer stream.CloseSend()

	req := &bspb.WriteRequest{}
	if compressed {
		req.ResourceName = fmt.Sprintf("%s/uploads/%s/compressed-blobs/zstd/%s/%d", u.InstanceName, uuid.New(), digest.Hash, digest.SizeBytes)
	} else {
		req.ResourceName = fmt.Sprintf("%s/uploads/%s/blobs/%s/%d", u.InstanceName, uuid.New(), digest.Hash, digest.SizeBytes)
	}

	buf := u.streamBufs.Get().(*[]byte)
	defer u.streamBufs.Put(buf)

chunkLoop:
	for {
		// Before reading, check if the context if canceled.
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Read the next chunk from the pipe.
		// Use ReadFull to ensure we aren't sending tiny blobs over RPC.
		n, err := io.ReadFull(r, *buf)
		switch {
		case err == io.EOF || err == io.ErrUnexpectedEOF:
			req.FinishWrite = true
		case err != nil:
			return err
		}
		req.Data = (*buf)[:n] // must limit by `:n` in ErrUnexpectedEOF case

		// Send the chunk.
		withTimeout(func() {
			err = stream.Send(req)
		})
		switch {
		case err == io.EOF:
			// The server closed the stream.
			// Most likely the file is already uploaded, see the CommittedSize check below.
			break chunkLoop
		case err != nil:
			return err
		case req.FinishWrite:
			break chunkLoop
		}

		// Prepare the next request.
		req.ResourceName = "" // send the resource name only in the first request
		req.WriteOffset += int64(len(req.Data))
	}

	// Finalize the request.
	switch res, err := stream.CloseAndRecv(); {
	case err != nil:
		return err
	case res.CommittedSize != digest.SizeBytes:
		return fmt.Errorf("unexpected commitSize: got %d, want %d", res.CommittedSize, digest.SizeBytes)
	}

	// Update stats.
	cacheHit := !req.FinishWrite
	if !cacheHit {
		atomic.AddInt64(&u.stats.Streamed.Bytes, digest.SizeBytes)
		atomic.AddInt64(&u.stats.Streamed.Digests, 1)
	}
	if updateCacheStats {
		st := &u.stats.CacheMisses
		if cacheHit {
			st = &u.stats.CacheHits
		}
		atomic.AddInt64(&st.Bytes, digest.SizeBytes)
		atomic.AddInt64(&st.Digests, 1)
	}
	return nil
}

// uploadItemFromDirMsg creates an upload item for a directory.
// Sorts directory entries.
func uploadItemFromDirMsg(title string, dir *repb.Directory) *uploadItem {
	// Normalize the dir before marshaling, for determinism.
	sort.Slice(dir.Files, func(i, j int) bool {
		return dir.Files[i].Name < dir.Files[j].Name
	})
	sort.Slice(dir.Directories, func(i, j int) bool {
		return dir.Directories[i].Name < dir.Directories[j].Name
	})
	sort.Slice(dir.Symlinks, func(i, j int) bool {
		return dir.Symlinks[i].Name < dir.Symlinks[j].Name
	})

	blob, err := proto.Marshal(dir)
	if err != nil {
		panic(err) // impossible
	}
	return uploadItemFromBlob(title, blob)
}

func uploadItemFromBlob(title string, blob []byte) *uploadItem {
	item := &uploadItem{
		Title:  title,
		Digest: digest.NewFromBlob(blob).ToProto(),
		Open: func() (uploadSource, error) {
			return newByteSliceSource(blob), nil
		},
	}
	if item.Title == "" {
		item.Title = fmt.Sprintf("digest %s/%d", item.Digest.Hash, item.Digest.SizeBytes)
	}
	return item
}

const pathSep = string(filepath.Separator)

// joinFilePathsFast is a faster version of filepath.Join because it does not
// call filepath.Clean.
func joinFilePathsFast(a, b string) string {
	return a + pathSep + b
}

func marshalledFieldSize(size int64) int64 {
	return 1 + int64(proto.SizeVarint(uint64(size))) + size
}

func marshalledRequestSize(d *repb.Digest) int64 {
	// An additional BatchUpdateBlobsRequest_Request includes the Digest and data fields,
	// as well as the message itself. Every field has a 1-byte size tag, followed by
	// the varint field size for variable-sized fields (digest hash and data).
	// Note that the BatchReadBlobsResponse_Response field is similar, but includes
	// and additional Status proto which can theoretically be unlimited in size.
	// We do not account for it here, relying on the Client setting a large (100MB)
	// limit for incoming messages.
	digestSize := marshalledFieldSize(int64(len(d.Hash)))
	if d.SizeBytes > 0 {
		digestSize += 1 + int64(proto.SizeVarint(uint64(d.SizeBytes)))
	}
	reqSize := marshalledFieldSize(digestSize)
	if d.SizeBytes > 0 {
		reqSize += marshalledFieldSize(int64(d.SizeBytes))
	}
	return marshalledFieldSize(reqSize)
}
