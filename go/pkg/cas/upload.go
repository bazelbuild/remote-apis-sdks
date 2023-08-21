package cas

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime/trace"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/golang/glog"
	"github.com/klauspost/compress/zstd"
	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/support/bundler"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/cache"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

// zstdEncoders is a pool of ZStd encoders.
// Clients of this pool must call Close() on the encoder after using the
// encoder.
var zstdEncoders = sync.Pool{
	New: func() interface{} {
		enc, _ := zstd.NewWriter(nil)
		return enc
	},
}

// UploadInput specifies a file or directory to upload.
type UploadInput struct {
	// Path to the file or a directory to upload.
	// Must be absolute.
	Path string

	// Allowlist is a filter for files/directories under Path.
	// If a file is not a present in Allowlist and does not reside in a directory
	// present in the Allowlist, then the file is ignored.
	// This is equivalent to deleting all not-matched files/dirs before
	// uploading.
	//
	// Each path in the Allowlist must be relative to UploadInput.Path.
	//
	// Must be empty if Path points to a regular file.
	Allowlist []string

	// Exclude is a file/dir filter. If Exclude is not nil and the
	// absolute path of a file/dir match this regexp, then the file/dir is skipped.
	// Forward-slash-separated paths are matched against the regexp: PathExclude
	// does not have to be conditional on the OS.
	// If the Path is a directory, then the filter is evaluated against each file
	// in the subtree.
	// See ErrSkip comments for more details on semantics regarding excluding symlinks .
	Exclude *regexp.Regexp

	cleanPath      string
	cleanAllowlist []string

	// pathInfo is result of Lstat(UploadInput.Path)
	pathInfo os.FileInfo

	// tree maps from a file/dir path to its digest and a directory node.
	// The path is relative to UploadInput.Path.
	//
	// Once digests are computed successfully, guaranteed to have key ".".
	// If allowlist is not empty, then also has a key for each clean allowlisted
	// path, as well as each intermediate directory between the root and an
	// allowlisted dir.
	//
	// The main purpose of this field is an UploadInput-local cache that couldn't
	// be placed in uploader.fsCache because of UploadInput-specific parameters
	// that are hard to incorporate into the cache key, namedly the allowlist.
	tree                map[string]*digested
	digestsComputed     chan struct{}
	digestsComputedInit sync.Once
	u                   *uploader
}

// Digest returns the digest computed for a file/dir.
// The relPath is relative to UploadInput.Path. Use "." for the digest of the
// UploadInput.Path itself.
//
// Digest is safe to call only after the channel returned by DigestsComputed()
// is closed.
//
// If the digest is unknown, returns (nil, err), where err is ErrDigestUnknown
// according to errors.Is.
// If the file is a danging symlink, then its digest is unknown.
func (in *UploadInput) Digest(relPath string) (digest.Digest, error) {
	if in.cleanPath == "" {
		return digest.Digest{}, errors.Errorf("Digest called too soon")
	}

	relPath = filepath.Clean(relPath)

	// Check if this is the root or one of the intermediate nodes in the partial
	// Merkle tee.
	if dig, ok := in.tree[relPath]; ok {
		return digest.NewFromProtoUnvalidated(dig.digest), nil
	}

	absPath := filepath.Join(in.cleanPath, relPath)

	// TODO(nodir): cache this syscall, perhaps using filemetadata package.
	info, err := os.Lstat(absPath)
	if err != nil {
		return digest.Digest{}, errors.WithStack(err)
	}

	key := makeFSCacheKey(absPath, info.Mode().IsRegular(), in.Exclude)
	switch val, err, loaded := in.u.fsCache.Load(key); {
	case !loaded:
		return digest.Digest{}, errors.Wrapf(ErrDigestUnknown, "digest not found for %#v", absPath)
	case err != nil:
		return digest.Digest{}, errors.WithStack(err)
	default:
		return digest.NewFromProtoUnvalidated(val.(*digested).digest), nil
	}
}

func (in *UploadInput) ensureDigestsComputedInited() chan struct{} {
	in.digestsComputedInit.Do(func() {
		in.digestsComputed = make(chan struct{})
	})
	return in.digestsComputed
}

// DigestsComputed returns a channel which is closed when all digests, including
// descendants, are computed.
// It is guaranteed to be closed by the time Client.Upload() returns successfully.
//
// DigestsComputed() is always safe to call.
func (in *UploadInput) DigestsComputed() <-chan struct{} {
	return in.ensureDigestsComputedInited()
}

var oneDot = []string{"."}

// init initializes internal fields.
func (in *UploadInput) init(u *uploader) error {
	in.u = u

	if !filepath.IsAbs(in.Path) {
		return errors.Errorf("%q is not absolute", in.Path)
	}
	in.cleanPath = filepath.Clean(in.Path)

	// Do not use os.Stat() here. We want to know if it is a symlink.
	var err error
	if in.pathInfo, err = os.Lstat(in.cleanPath); err != nil {
		return errors.WithStack(err)
	}

	// Process the allowlist.
	in.tree = make(map[string]*digested, 1+len(in.Allowlist))
	switch {
	case len(in.Allowlist) == 0:
		in.cleanAllowlist = oneDot

	case in.pathInfo.Mode().IsRegular():
		return errors.Errorf("the Allowlist is not supported for regular files")

	default:
		in.cleanAllowlist = make([]string, len(in.Allowlist))
		for i, subPath := range in.Allowlist {
			if filepath.IsAbs(subPath) {
				return errors.Errorf("the allowlisted path %q is not relative", subPath)
			}

			cleanSubPath := filepath.Clean(subPath)
			if cleanSubPath == ".." || strings.HasPrefix(cleanSubPath, parentDirPrefix) {
				return errors.Errorf("the allowlisted path %q is not contained by %q", subPath, in.Path)
			}
			in.cleanAllowlist[i] = cleanSubPath
		}
	}
	return nil
}

// partialMerkleTree ensures that for each node in in.tree, not included by any
// other node, all its ancestors are also present in the tree. For example, if
// the tree contains only "foo/bar" and "foo/baz", then partialMerkleTree adds
// "foo" and ".". The latter is the root.
//
// All tree keys must be clean relative paths.
// Returns prepared *uploadItems that represent the ancestors that were added to
// the tree.
func (in *UploadInput) partialMerkleTree() (added []*uploadItem) {
	// Establish parent->child edges.
	children := map[string]map[string]struct{}{}
	for relPath := range in.tree {
		for relPath != "." {
			parent := dirNameRelFast(relPath)
			if childSet, ok := children[parent]; ok {
				childSet[relPath] = struct{}{}
			} else {
				children[parent] = map[string]struct{}{relPath: {}}
			}
			relPath = parent
		}
	}

	// Add the missing ancestors by traversing in post-order.
	var dfs func(relPath string) proto.Message
	dfs = func(relPath string) proto.Message {
		if dig, ok := in.tree[relPath]; ok {
			return dig.dirEntry
		}

		dir := &repb.Directory{}
		for child := range children[relPath] {
			addDirEntry(dir, dfs(child))
		}

		// Prepare an uploadItem.
		absPath := joinFilePathsFast(in.cleanPath, relPath)
		item := uploadItemFromDirMsg(absPath, dir) // normalizes the dir
		added = append(added, item)

		// Compute a directory entry for the parent.
		node := &repb.DirectoryNode{
			Name:   filepath.Base(absPath),
			Digest: item.Digest,
		}

		in.tree[relPath] = &digested{dirEntry: node, digest: item.Digest}
		return node
	}
	dfs(".")
	return added
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
	Bytes   int64 // total sum of digest sizes

	// TODO(nodir): add something like TransferBytes, i.e. how much was actually transferred
}

// UploadOptions is optional configuration for Upload function.
// The default options are the zero value of this struct.
type UploadOptions struct {
	// PreserveSymlinks specifies whether to preserve symlinks or convert them
	// to regular files. This doesn't upload target of symlinks, caller needs
	// to specify targets explicitly if those are necessary too.
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
	// UploadInputs directly/indirectly refer to the same file, but with different
	// UploadInput.Exclude.
	//
	// Prelude is called from different goroutines.
	Prelude func(absPath string, mode os.FileMode) error
}

// digested is a result of preprocessing a file/dir.
type digested struct {
	dirEntry proto.Message // FileNode, DirectoryNode or SymlinkNode
	digest   *repb.Digest  // may be nil, e.g. for dangling symlinks
}

var (
	// ErrSkip when returned by UploadOptions.Prelude, means the file/dir must be
	// not be uploaded.
	//
	// Note that if UploadOptions.PreserveSymlinks is true and the ErrSkip is
	// returned for a symlink target, but not the symlink itself, then it may
	// result in a dangling symlink.
	ErrSkip = errors.New("skip file")

	// ErrDigestUnknown indicates that the requested digest is unknown.
	// Use errors.Is instead of direct equality check.
	ErrDigestUnknown = errors.New("the requested digest is unknown")
)

// UploadResult is the result of a Client.Upload call.
// It provides file/dir digests and statistics.
type UploadResult struct {
	Stats TransferStats
	u     *uploader
}

// Upload uploads all files/directories specified by inputC.
//
// Upload assumes ownership of UploadInputs received from inputC.
// They must not be mutated after sending.
//
// Close inputC to indicate that there are no more files/dirs to upload.
// When inputC is closed, Upload finishes uploading the remaining files/dirs and
// exits successfully.
//
// If ctx is canceled, the Upload returns with an error.
func (c *Client) Upload(ctx context.Context, opt UploadOptions, inputC <-chan *UploadInput) (*UploadResult, error) {
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
			case in, ok := <-inputC:
				if !ok {
					return nil
				}
				log.Infof("start startProcessing %s", in.Path)
				if err := u.startProcessing(ctx, in); err != nil {
					return err
				}
				log.Infof("finish startProcessing %s", in.Path)
			}
		}
	})

	return &UploadResult{Stats: u.stats, u: u}, errors.WithStack(eg.Wait())
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
	// A key can be produced by makeFSCacheKey.
	// The values are of type *digested.
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
	if !filepath.IsAbs(in.Path) {
		return errors.Errorf("%q is not absolute", in.Path)
	}

	if err := in.init(u); err != nil {
		return errors.WithStack(err)
	}

	// Schedule a file system walk.
	u.wgFS.Add(1)
	u.eg.Go(func() error {
		defer u.wgFS.Done()

		// Concurrently visit each allowlisted path, and use the results to
		// construct a partial Merkle tree. Note that we are not visiting
		// the entire in.cleanPath, which may be much larger than the union of the
		// allowlisted paths.
		log.Infof("start localEg %s", in.Path)
		localEg, ctx := errgroup.WithContext(ctx)
		var treeMu sync.Mutex
		for _, relPath := range in.cleanAllowlist {
			relPath := relPath
			// Schedule a file system walk.
			localEg.Go(func() error {
				absPath := in.cleanPath
				info := in.pathInfo
				if relPath != "." {
					absPath = joinFilePathsFast(in.cleanPath, relPath)
					var err error
					// TODO(nodir): cache this syscall too.
					if info, err = os.Lstat(absPath); err != nil {
						return errors.WithStack(err)
					}
				}

				switch dig, err := u.visitPath(ctx, absPath, info, in.Exclude); {
				case err != nil:
					return errors.Wrapf(err, "%q", absPath)
				case dig != nil:
					treeMu.Lock()
					in.tree[relPath] = dig
					treeMu.Unlock()
				}
				return nil
			})
		}
		if err := localEg.Wait(); err != nil {
			return errors.WithStack(err)
		}
		log.Infof("done localEg %s", in.Path)
		// At this point, all allowlisted paths are digest'ed, and we only need to
		// compute a partial Merkle tree and upload the implied ancestors.
		for _, item := range in.partialMerkleTree() {
			if err := u.scheduleCheck(ctx, item); err != nil {
				return err
			}
		}

		// The entire tree is digested. Notify the caller.
		close(in.ensureDigestsComputedInited())
		return nil
	})
	return nil
}

// makeFSCacheKey returns a key for u.fsCache.
func makeFSCacheKey(absPath string, isRegularFile bool, pathExclude *regexp.Regexp) interface{} {
	// The structure of the cache key is incapsulated by this function.
	type cacheKey struct {
		AbsPath       string
		ExcludeRegexp string
	}

	key := cacheKey{
		AbsPath: absPath,
	}

	if isRegularFile {
		// This is a regular file.
		// Its digest depends only on the file path (assuming content didn't change),
		// so the cache key is complete. Just return it.
		return key
	}
	// This is a directory and/or a symlink, so the digest also depends on fs-walk
	// settings. Incroporate those too.

	if pathExclude != nil {
		key.ExcludeRegexp = pathExclude.String()
	}
	return key
}

// visitPath visits the file/dir depending on its type (regular, dir, symlink).
// Visits each file only once.
//
// If the file should be skipped, then returns (nil, nil).
// The returned digested.digest may also be nil if the symlink is dangling.
func (u *uploader) visitPath(ctx context.Context, absPath string, info os.FileInfo, pathExclude *regexp.Regexp) (*digested, error) {
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

	cacheKey := makeFSCacheKey(absPath, info.Mode().IsRegular(), pathExclude)
	cached, err := u.fsCache.LoadOrStore(cacheKey, func() (interface{}, error) {
		switch {
		case info.Mode()&os.ModeSymlink == os.ModeSymlink:
			return u.visitSymlink(ctx, absPath, pathExclude)

		case info.Mode().IsDir():
			node, err := u.visitDir(ctx, absPath, pathExclude)
			return &digested{dirEntry: node, digest: node.GetDigest()}, err

		case info.Mode().IsRegular():
			// Note: makeFSCacheKey assumes that pathExclude is not used here.
			node, err := u.visitRegularFile(ctx, absPath, info)
			return &digested{dirEntry: node, digest: node.GetDigest()}, err

		default:
			return nil, fmt.Errorf("unexpected file mode %s", info.Mode())
		}
	})
	if err != nil {
		return nil, err
	}
	return cached.(*digested), nil
}

// visitRegularFile computes the hash of a regular file and schedules a presence
// check.
//
// It distinguishes three categories of file sizes:
//   - small: small files are buffered in memory entirely, thus read only once.
//     See also ClientConfig.SmallFileThreshold.
//   - medium: the hash is computed, the file is closed and a presence check is
//     scheduled.
//   - large: the hash is computed, the file is rewinded without closing and
//     streamed via ByteStream.
//     If the file is already present on the server, the ByteStream preempts
//     the stream with EOF and WriteResponse.CommittedSize == Digest.Size.
//     Rewinding helps locality: there is no delay between reading the file for
//     the first and the second times.
//     Only one large file is processed at a time because most GCE disks are
//     network disks. Reading many large files concurrently appears to saturate
//     the network and slows down the progress.
//     See also ClientConfig.LargeFileThreshold.
func (u *uploader) visitRegularFile(ctx context.Context, absPath string, info os.FileInfo) (*repb.FileNode, error) {
	isLarge := info.Size() >= u.Config.LargeFileThreshold

	// Lock the mutex before acquiring a semaphore to avoid hogging the latter.
	if isLarge {
		// Read only a few large files at a time.
		if err := u.semLargeFile.Acquire(ctx, 1); err != nil {
			return nil, errors.WithStack(err)
		}
		defer u.semLargeFile.Release(1)
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
		contents, err := io.ReadAll(f)
		if err != nil {
			return nil, err
		}
		item := uploadItemFromBlob(absPath, contents)
		ret.Digest = item.Digest
		return ret, u.scheduleCheck(ctx, item)
	}

	// It is a medium or large file.

	tctx, task := trace.NewTask(ctx, "medium or large file")
	defer task.End()
	trace.Log(tctx, "file", info.Name())

	// Compute the hash.
	now := time.Now()
	region := trace.StartRegion(tctx, "digest")
	dig, err := digest.NewFromReader(f)
	region.End()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to compute hash")
	}
	log.Infof("compute digest %s: %s", info.Name(), time.Since(now))
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

		if res, err := u.findMissingBlobs(ctx, []*uploadItem{item}); err != nil {
			return nil, errors.Wrapf(err, "failed to check existence")
		} else if len(res.MissingBlobDigests) == 0 {
			log.Infof("the file already exists. do not upload %s", absPath)
			atomic.AddInt64(&u.stats.CacheHits.Digests, 1)
			atomic.AddInt64(&u.stats.CacheHits.Bytes, ret.Digest.SizeBytes)
			return ret, nil
		}

		item.Open = func() (uploadSource, error) {
			return f, f.SeekStart(0)
		}
		return ret, u.stream(tctx, item, true)
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
					digested, err := u.visitPath(ctx, absChild, info, pathExclude)
					mu.Lock()
					defer mu.Unlock()

					switch {
					case err != nil:
						subErr = err
						return err
					case digested == nil:
						// This file should be ignored.
						return nil
					}

					addDirEntry(dir, digested.dirEntry)
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
//
// The returned digested.digest is nil if u.PreserveSymlinks is set.
func (u *uploader) visitSymlink(ctx context.Context, absPath string, pathExclude *regexp.Regexp) (*digested, error) {
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

	if u.PreserveSymlinks && u.AllowDanglingSymlinks {
		return &digested{dirEntry: symlinkNode}, nil
	}

	// Need to check symlink if AllowDanglingSymlinks is not set.
	targetInfo, err := os.Lstat(absTarget)
	if err != nil {
		return nil, errors.Wrapf(err, "lstat to target of symlink (%s -> %s) has error", absPath, relTarget)
	}

	// TODO: detect cycles by symlink if needs to follow symlinks in this case.
	if u.PreserveSymlinks {
		return &digested{dirEntry: symlinkNode}, nil
	}

	return u.visitPath(ctx, absTarget, targetInfo, pathExclude)
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
	return io.ReadAll(r)
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

func (u *uploader) findMissingBlobs(ctx context.Context, items []*uploadItem) (res *repb.FindMissingBlobsResponse, err error) {
	if err := u.semFindMissingBlobs.Acquire(ctx, 1); err != nil {
		return nil, errors.WithStack(err)
	}
	defer u.semFindMissingBlobs.Release(1)

	req := &repb.FindMissingBlobsRequest{
		InstanceName: u.InstanceName,
		BlobDigests:  make([]*repb.Digest, len(items)),
	}

	for i, item := range items {
		req.BlobDigests[i] = item.Digest
	}

	err = u.unaryRPC(ctx, &u.Config.FindMissingBlobs, func(ctx context.Context) (err error) {
		res, err = u.cas.FindMissingBlobs(ctx, req)
		return
	})
	return res, err
}

// check checks which items are present on the server, and schedules upload for
// the missing ones.
func (u *uploader) check(ctx context.Context, items []*uploadItem) error {
	res, err := u.findMissingBlobs(ctx, items)
	if err != nil {
		return err
	}
	byDigest := make(map[digest.Digest]*uploadItem, len(items))
	totalBytes := int64(0)
	for _, item := range items {
		byDigest[digest.NewFromProtoUnvalidated(item.Digest)] = item
		totalBytes += item.Digest.SizeBytes
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

	ctx, task := trace.NewTask(ctx, "uploader.stream")
	defer task.End()

	log.Infof("start stream upload %s, size %d", item.Title, item.Digest.SizeBytes)
	now := time.Now()
	defer func() {
		log.Infof("finish stream upload %s, size %d: %s", item.Title, item.Digest.SizeBytes, time.Since(now))
	}()

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

		// Compress using an in-memory pipe. This is mostly to accommodate the fact
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

func (u *uploader) streamFromReader(ctx context.Context, r io.Reader, digest *repb.Digest, compressed, updateCacheStats bool) (rerr error) {
	ctx, cancel, withTimeout := withPerCallTimeout(ctx, u.Config.ByteStreamWrite.Timeout)
	defer cancel()

	stream, err := u.byteStream.Write(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if _, err := stream.CloseAndRecv(); rerr == nil && err != io.EOF {
			rerr = err
		}
	}()

	req := &bspb.WriteRequest{}
	instanceSegment := u.InstanceName + "/"
	if instanceSegment == "/" {
		instanceSegment = ""
	}
	if compressed {
		req.ResourceName = fmt.Sprintf("%suploads/%s/compressed-blobs/zstd/%s/%d", instanceSegment, uuid.New(), digest.Hash, digest.SizeBytes)
	} else {
		req.ResourceName = fmt.Sprintf("%suploads/%s/blobs/%s/%d", instanceSegment, uuid.New(), digest.Hash, digest.SizeBytes)
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
		region := trace.StartRegion(ctx, "ReadFull in streamFromReader")
		n, err := io.ReadFull(r, *buf)
		region.End()
		switch {
		case err == io.EOF || err == io.ErrUnexpectedEOF:
			req.FinishWrite = true
		case err != nil:
			return err
		}
		req.Data = (*buf)[:n] // must limit by `:n` in ErrUnexpectedEOF case

		// Send the chunk.
		withTimeout(func() {
			trace.WithRegion(ctx, "stream.Send", func() {
				err = stream.Send(req)
			})
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

const (
	pathSep         = string(filepath.Separator)
	parentDirPrefix = ".." + pathSep
)

// joinFilePathsFast is a faster version of filepath.Join because it does not
// call filepath.Clean. Assumes arguments are clean according to filepath.Clean specs.
func joinFilePathsFast(a, b string) string {
	if b == "." {
		return a
	}
	if strings.HasSuffix(a, pathSep) {
		// May happen if a is the root.
		return a + b
	}
	return a + pathSep + b
}

// dirNameRelFast is a faster version of filepath.Dir because it does not call
// filepath.Clean. Assumes the argument is clean and relative.
// Does not work for absolute paths.
func dirNameRelFast(relPath string) string {
	i := strings.LastIndex(relPath, pathSep)
	if i < 0 {
		return "."
	}
	return relPath[:i]
}

func marshalledFieldSize(size int64) int64 {
	return 1 + int64(protowire.SizeVarint(uint64(size))) + size
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
		digestSize += 1 + int64(protowire.SizeVarint(uint64(d.SizeBytes)))
	}
	reqSize := marshalledFieldSize(digestSize)
	if d.SizeBytes > 0 {
		reqSize += marshalledFieldSize(int64(d.SizeBytes))
	}
	return marshalledFieldSize(reqSize)
}

func addDirEntry(dir *repb.Directory, node proto.Message) {
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
}
