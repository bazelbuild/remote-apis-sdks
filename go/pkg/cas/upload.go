package cas

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
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

	// If not nil and returns false for a file, then it is not uploaded.
	//
	// The function participates in a cache key, so try to reuse the same functions.
	Predicate FilePredicate

	// TODO(nodir): add AllowDanglingSymlinks
	// TODO(nodir): add PreserveSymlinks.
}

// FilePredicate is a condition for a file.
type FilePredicate func(absName string, mode os.FileMode) bool

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

// Upload uploads all inputs. It exits when inputC is closed or ctx is canceled.
func (c *Client) Upload(ctx context.Context, inputC <-chan *UploadInput) (stats *TransferStats, err error) {
	eg, ctx := errgroup.WithContext(ctx)
	// Do not exit until all sub-goroutines exit, to prevent goroutine leaks.
	defer eg.Wait()

	u := &uploader{
		Client: c,
		eg:     eg,

		fsSem: semaphore.NewWeighted(int64(c.FSConcurrency)),
	}

	// Start processing input.
	eg.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case in, ok := <-inputC:
				if !ok {
					return nil
				}
				u.eg.Go(func() error {
					return errors.Wrapf(u.startProcessing(ctx, in), "%q", in.Path)
				})
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
type uploader struct {
	*Client
	eg    *errgroup.Group
	stats TransferStats

	// already-processed files.
	// Types fsCacheKey and fsCacheValue are used for keys/values respectively.
	fsCache sync.Map
	// controls concurrency of file IO.
	// TODO(nodir): ensure it does not hurt streaming.
	fsSem *semaphore.Weighted
	// ensures only one large file is read at a time.
	// TODO(nodir): ensure this doesn't hurt performance on SSDs.
	muLargeFile sync.Mutex
}

// startProcessing adds the item to the appropriate stage depending on its type.
func (u *uploader) startProcessing(ctx context.Context, in *UploadInput) error {
	if in.Path == "" {
		// Simple case: the blob is in memory.
		return u.scheduleCheck(ctx, uploadItemFromBlob("", in.Content))
	}

	// Compute the absolute path only once per directory tree.
	absPath, err := filepath.Abs(in.Path)
	if err != nil {
		return err
	}

	// Do not use os.Stat() here. We want to know if it is a symlink.
	info, err := os.Lstat(absPath)
	if err != nil {
		return err
	}

	// visitFile below assumes the file passes the predicate, so evaluate it here.
	if in.Predicate != nil && !in.Predicate(absPath, info.Mode()) {
		return nil
	}

	_, err = u.visitFile(ctx, absPath, info, in.Predicate)
	return err
}

// fsCacheKey is the key type for uploader.fsCache.
type fsCacheKey struct {
	AbsName      string
	PredicatePtr uintptr
}

// fsCacheValue is the value type for uploader.fsCache.
type fsCacheValue struct {
	compute  sync.Once
	dirEntry proto.Message // *repb.FileNode, *repb.DirectoryNode or *repb.SymlinkNode.
	err      error
}

// visitFile visits the file/dir depending on its type (regular, dir, symlink).
// Visits each file only once per predicate function.
func (u *uploader) visitFile(ctx context.Context, absName string, info os.FileInfo, predicate FilePredicate) (dirEntry interface{}, err error) {
	cacheKey := fsCacheKey{AbsName: absName, PredicatePtr: reflect.ValueOf(predicate).Pointer()}
	entryRaw, _ := u.fsCache.LoadOrStore(cacheKey, &fsCacheValue{})
	e := entryRaw.(*fsCacheValue)
	e.compute.Do(func() {
		switch {
		case info.Mode()&os.ModeSymlink == os.ModeSymlink:
			e.dirEntry, e.err = u.visitSymlink(ctx, absName, predicate)
		case info.Mode().IsDir():
			e.dirEntry, e.err = u.visitDir(ctx, absName, predicate)
		case info.Mode().IsRegular():
			e.dirEntry, e.err = u.visitRegularFile(ctx, absName, info)
		default:
			e.err = fmt.Errorf("unexpected file mode %s", info.Mode())
		}
	})
	return e.dirEntry, e.err
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
func (u *uploader) visitRegularFile(ctx context.Context, absName string, info os.FileInfo) (*repb.FileNode, error) {
	isLarge := info.Size() >= u.LargeFileThreshold

	// Lock the mutex before acquiring a semaphore to avoid hogging the latter.
	if isLarge {
		// Read only one large file at a time.
		u.muLargeFile.Lock()
		defer u.muLargeFile.Unlock()
	}

	if err := u.fsSem.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	defer u.fsSem.Release(1)

	f, err := os.Open(absName)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	ret := &repb.FileNode{
		Name:         info.Name(),
		Digest:       &repb.Digest{SizeBytes: info.Size()},
		IsExecutable: (info.Mode() & 0100) != 0,
	}

	if info.Size() <= u.SmallFileThreshold {
		// This file is small enough to buffer it entirely.
		contents, err := ioutil.ReadAll(f)
		if err != nil {
			return nil, err
		}
		item := uploadItemFromBlob(absName, contents)
		ret.Digest = item.Digest
		return ret, u.scheduleCheck(ctx, item)
	}

	// It is a medium or large file.

	// Compute the hash.
	h := digest.HashFn.New()
	// TODO(nodir): reuse the buffer.
	buf := make([]byte, u.FileIOSize)
	if _, err := io.CopyBuffer(h, f, buf); err != nil {
		return nil, err
	}
	ret.Digest.Hash = hex.EncodeToString(h.Sum(nil))

	item := &uploadItem{
		Title:  absName,
		Digest: ret.Digest,
	}

	if isLarge {
		// Large files are special: locality is important - we want to re-read the
		// file ASAP.
		// Also we are not going to use BatchUploads anyway, so we can take
		// advantage of ByteStream's built-in presence check.

		item.Open = func() (readSeekCloser, error) {
			_, err := f.Seek(0, io.SeekStart)
			return f, err
		}
		panic("not implemented")
		// TODO(nodir): implement.
	}

	// Schedule a check and close the file (in defer).
	// item.open will reopen the file.

	item.Open = func() (readSeekCloser, error) {
		return os.Open(absName)
	}
	return ret, u.scheduleCheck(ctx, item)
}

// visitDir reads a directory and its descendants, subject to the
// predicate. The function blocks until each descendant is visited, but the
// visitation happens concurrently, using u.eg.
func (u *uploader) visitDir(ctx context.Context, absName string, predicate FilePredicate) (*repb.DirectoryNode, error) {
	var mu sync.Mutex
	dir := &repb.Directory{}
	var subErr error
	var wg sync.WaitGroup

	// This sub-function exist to avoid holding the semaphore while waiting for
	// children.
	err := func() error {
		if err := u.fsSem.Acquire(ctx, 1); err != nil {
			return err
		}
		defer u.fsSem.Release(1)

		f, err := os.Open(absName)
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
				absChild := filepath.Join(absName, info.Name())
				if predicate != nil && !predicate(absChild, info.Mode()) {
					continue
				}
				wg.Add(1)
				u.eg.Go(func() error {
					defer wg.Done()
					node, err := u.visitFile(ctx, absChild, info, predicate)
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
						panic("impossible")
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

	// Wait for children.
	wg.Wait()
	if subErr != nil {
		return nil, errors.Wrapf(subErr, "failed to read the directory %q entirely", absName)
	}

	item := uploadItemFromDirMsg(absName, dir)
	if err := u.scheduleCheck(ctx, item); err != nil {
		return nil, err
	}
	return &repb.DirectoryNode{
		Name:   filepath.Base(absName),
		Digest: item.Digest,
	}, nil
}

// visitSymlink converts a symlink to a SymlinkNode and schedules visitation
// of the target file.
func (u *uploader) visitSymlink(ctx context.Context, absName string, predicate FilePredicate) (*repb.SymlinkNode, error) {
	// TODO(nodir): implement
	panic("not supported")
}

// uploadItem is a blob to potentially upload.
type uploadItem struct {
	Title  string
	Digest *repb.Digest
	Open   func() (readSeekCloser, error)
}

// scheduleCheck schedules a blob presence check on the server. If it fails,
// then the blob is uploaded.
func (u *uploader) scheduleCheck(ctx context.Context, item *uploadItem) error {
	if u.testScheduleCheck != nil {
		return u.testScheduleCheck(ctx, item)
	}

	// TODO(nodir): implement.
	panic("not implemented")
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
		item.Title = fmt.Sprintf("blob %s", item.Digest.Hash)
	}
	return item
}
