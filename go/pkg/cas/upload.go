package cas

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/support/bundler"
	"google.golang.org/grpc/status"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/cache"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

// UploadInput is one of inputs to Client.Upload function.
//
// It can be either a reference to a file/dir (see Path) or it can be an
// in-memory blob (see Content).
type UploadInput struct {
	// Path to the file or a directory to upload.
	// If empty, the Content is uploaded instead.
	//
	// Must be absolute or relative to CWD.
	Path string

	// Contents to upload.
	// Ignored if Path is not empty.
	Content []byte

	// TODO(nodir): add Predicate.
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
}

// Upload uploads all inputs. It exits when inputC is closed or ctx is canceled.
func (c *Client) Upload(ctx context.Context, opt UploadOptions, inputC <-chan *UploadInput) (stats *TransferStats, err error) {
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
	u.checkBundler.BundleCountThreshold = u.FindMissingBlobs.MaxItems

	// Initialize batchBundler, which uploads blobs in batches.
	u.batchBundler = bundler.NewBundler(&repb.BatchUpdateBlobsRequest_Request{}, func(subReq interface{}) {
		// Handle errors and context cancelation via errgroup.
		eg.Go(func() error {
			return u.uploadBatch(ctx, subReq.([]*repb.BatchUpdateBlobsRequest_Request))
		})
	})
	// Limit the sum of sub-request sizes to (maxRequestSize - requestOverhead).
	// Subtract 1KB to be on the safe side.
	u.batchBundler.BundleByteLimit = c.BatchUpdateBlobs.MaxSizeBytes - int(marshalledFieldSize(int64(len(c.InstanceName)))) - 1000
	u.batchBundler.BundleCountThreshold = c.BatchUpdateBlobs.MaxItems

	// Start processing input.
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
			case in, ok := <-inputC:
				if !ok {
					return nil
				}
				if err := u.startProcessing(ctx, in); err != nil {
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
func (u *uploader) startProcessing(ctx context.Context, in *UploadInput) error {
	if in.Path == "" {
		// Simple case: the blob is in memory.
		return u.scheduleCheck(ctx, uploadItemFromBlob("", in.Content))
	}

	// Schedule a file system walk.
	u.wgFS.Add(1)
	u.eg.Go(func() error {
		defer u.wgFS.Done()
		// Compute the absolute path only once per directory tree.
		absPath, err := filepath.Abs(in.Path)
		if err != nil {
			return errors.Wrapf(err, "failed to get absolute path")
		}

		// Do not use os.Stat() here. We want to know if it is a symlink.
		info, err := os.Lstat(absPath)
		if err != nil {
			return errors.WithStack(err)
		}

		_, err = u.visitFile(ctx, absPath, info)
		return err
	})
	return nil
}

// visitFile visits the file/dir depending on its type (regular, dir, symlink).
// Visits each file only once.
func (u *uploader) visitFile(ctx context.Context, absPath string, info os.FileInfo) (dirEntry proto.Message, err error) {
	node, err := u.fsCache.LoadOrStore(absPath, func() (interface{}, error) {
		switch {
		case info.Mode()&os.ModeSymlink == os.ModeSymlink:
			return u.visitSymlink(ctx, absPath)
		case info.Mode().IsDir():
			return u.visitDir(ctx, absPath)
		case info.Mode().IsRegular():
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
//    They are treated same as UploadInput with Contents and without Path.
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
	isLarge := info.Size() >= u.LargeFileThreshold

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

	f, err := os.Open(absPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	ret := &repb.FileNode{
		Name:         info.Name(),
		IsExecutable: (info.Mode() & 0100) != 0,
	}

	if info.Size() <= u.SmallFileThreshold {
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
	buf := u.fileIOBufs.Get().([]byte)
	dig, err := digest.NewFromReaderWithBuffer(f, buf)
	u.fileIOBufs.Put(buf)
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

		item.Open = func() (readSeekCloser, error) {
			_, err := f.Seek(0, io.SeekStart)
			return f, err
		}
		panic("not implemented")
		// TODO(nodir): implement.
	}

	// Schedule a check and close the file (in defer).
	// item.Open will reopen the file.

	item.Open = func() (readSeekCloser, error) {
		return os.Open(absPath)
	}
	return ret, u.scheduleCheck(ctx, item)
}

// visitDir reads a directory and its descendants. The function blocks until
// each descendant is visited, but the visitation happens concurrently, using
// u.eg.
func (u *uploader) visitDir(ctx context.Context, absPath string) (*repb.DirectoryNode, error) {
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
					node, err := u.visitFile(ctx, absChild, info)
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
func (u *uploader) visitSymlink(ctx context.Context, absPath string) (proto.Message, error) {
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
	// TODO(nodir): add an option to return an error if a symlink target outside of
	// execution root is detected.

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

	node, err := u.visitFile(ctx, absTarget, targetInfo)
	if u.PreserveSymlinks {
		// Note: even though we throw away `node`, it was still important to visit the target.
		return symlinkNode, err
	}
	return node, err
}

// uploadItem is a blob to potentially upload.
type uploadItem struct {
	Title  string
	Digest *repb.Digest
	Open   func() (readSeekCloser, error)
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
	// TODO(nodir): limit concurrency.
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
	err := u.unaryRPC(ctx, &u.FindMissingBlobs, func(ctx context.Context) (err error) {
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
		// TODO(nodir): implement streaming.
		panic("not implemented")
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
	return u.unaryRPC(ctx, &u.BatchUpdateBlobs, func(ctx context.Context) error {
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
		Open: func() (readSeekCloser, error) {
			return newByteReader(blob), nil
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
