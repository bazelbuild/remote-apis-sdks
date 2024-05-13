// Package diskcache implements a local disk LRU CAS cache.
package diskcache

import (
	"container/heap"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	log "github.com/golang/glog"
)

type key struct {
	digest digest.Digest
	isCas  bool
}

// An qitem is something we manage in a priority queue.
type qitem struct {
	key   key
	lat   time.Time    // The last accessed time of the file.
	index int          // The index of the item in the heap.
	mu    sync.RWMutex // Protects the data-structure consistency for the given digest.
}

// A priorityQueue implements heap.Interface and holds qitems.
type priorityQueue struct {
	items []*qitem
	n     int
}

func (q *priorityQueue) Len() int {
	return q.n
}

func (q *priorityQueue) Less(i, j int) bool {
	// We want Pop to give us the oldest item.
	return q.items[i].lat.Before(q.items[j].lat)
}

func (q priorityQueue) Swap(i, j int) {
	q.items[i], q.items[j] = q.items[j], q.items[i]
	q.items[i].index = i
	q.items[j].index = j
}

func (q *priorityQueue) Push(x any) {
	if q.n == cap(q.items) {
		// Resize the queue
		old := q.items
		q.items = make([]*qitem, 2*cap(old)) // Initial capacity needs to be > 0.
		copy(q.items, old)
	}
	item := x.(*qitem)
	item.index = q.n
	q.items[item.index] = item
	q.n++
}

func (q *priorityQueue) Pop() any {
	item := q.items[q.n-1]
	q.items[q.n-1] = nil // avoid memory leak
	item.index = -1      // for safety
	q.n--
	return item
}

// bumps item to the head of the queue.
func (q *priorityQueue) Bump(item *qitem) {
	// Sanity check, necessary because of possible racing between Bump and GC:
	if item.index < 0 || item.index >= q.n || q.items[item.index].key != item.key {
		return
	}
	item.lat = time.Now()
	heap.Fix(q, item.index)
}

// DiskCache is a local disk LRU CAS and Action Cache cache.
type DiskCache struct {
	root             string         // path to the root directory of the disk cache.
	maxCapacityBytes uint64         // if disk size exceeds this, old items will be evicted as needed.
	mu               sync.Mutex     // protects the queue.
	store            sync.Map       // map of keys to qitems.
	queue            *priorityQueue // keys by last accessed time.
	ctx              context.Context
	shutdown         chan bool
	shutdownOnce     sync.Once
	gcReq            chan bool
	gcDone           chan bool
	statMu           sync.Mutex
	stats            *DiskCacheStats
}

type DiskCacheStats struct {
	TotalSizeBytes         int64
	TotalNumFiles          int64
	NumFilesStored         int64
	TotalStoredBytes       int64
	NumFilesGCed           int64
	TotalGCedSizeBytes     int64
	NumCacheHits           int64
	NumCacheMisses         int64
	TotalCacheHitSizeBytes int64
	InitTime               time.Duration
	TotalGCTime            time.Duration
	TotalGCDiskOpsTime     time.Duration
}

func New(ctx context.Context, root string, maxCapacityBytes uint64) (*DiskCache, error) {
	res := &DiskCache{
		root:             root,
		maxCapacityBytes: maxCapacityBytes,
		ctx:              ctx,
		queue: &priorityQueue{
			items: make([]*qitem, 1000),
		},
		gcReq:    make(chan bool, 1),
		shutdown: make(chan bool),
		gcDone:   make(chan bool),
		stats:    &DiskCacheStats{},
	}
	start := time.Now()
	defer func() { atomic.AddInt64((*int64)(&res.stats.InitTime), int64(time.Since(start))) }()
	heap.Init(res.queue)
	if err := os.MkdirAll(root, os.ModePerm); err != nil {
		return nil, err
	}
	// We use Git's directory/file naming structure as inspiration:
	// https://git-scm.com/book/en/v2/Git-Internals-Git-Objects#:~:text=The%20subdirectory%20is%20named%20with%20the%20first%202%20characters%20of%20the%20SHA%2D1%2C%20and%20the%20filename%20is%20the%20remaining%2038%20characters.
	eg, eCtx := errgroup.WithContext(ctx)
	for i := 0; i < 256; i++ {
		prefixDir := filepath.Join(root, fmt.Sprintf("%02x", i))
		eg.Go(func() error {
			if eCtx.Err() != nil {
				return eCtx.Err()
			}
			if err := os.MkdirAll(prefixDir, os.ModePerm); err != nil {
				return err
			}
			return filepath.WalkDir(prefixDir, func(path string, d fs.DirEntry, err error) error {
				// We log and continue on all errors, because cache read errors are not critical.
				if err != nil {
					return fmt.Errorf("error reading cache directory: %v", err)
				}
				if d.IsDir() {
					return nil
				}
				subdir := filepath.Base(filepath.Dir(path))
				k, err := getKeyFromFileName(subdir + d.Name())
				if err != nil {
					return fmt.Errorf("error parsing cached file name %s: %v", path, err)
				}
				info, err := d.Info()
				if err != nil {
					return fmt.Errorf("error getting file info of %s: %v", path, err)
				}
				it := &qitem{
					key: k,
					lat: fileInfoToAccessTime(info),
				}
				size, err := res.getItemSize(k)
				if err != nil {
					return fmt.Errorf("error getting file size of %s: %v", path, err)
				}
				res.store.Store(k, it)
				atomic.AddInt64(&res.stats.TotalSizeBytes, size)
				atomic.AddInt64(&res.stats.TotalNumFiles, 1)
				res.mu.Lock()
				heap.Push(res.queue, it)
				res.mu.Unlock()
				return nil
			})
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	go res.daemon()
	return res, nil
}

func (d *DiskCache) getItemSize(k key) (int64, error) {
	if k.isCas {
		return k.digest.Size, nil
	}
	fname := d.getPath(k)
	info, err := os.Stat(fname)
	if err != nil {
		return 0, fmt.Errorf("error getting info for %s: %v", fname, err)
	}
	return info.Size(), nil
}

// Terminates the GC daemon, waiting for it to complete. No further Store* calls to the DiskCache should be made.
func (d *DiskCache) Shutdown() {
	d.shutdownOnce.Do(func() {
		d.shutdown <- true
		<-d.gcDone
		log.Infof("DiskCacheStats: %+v", d.stats)
	})
}

func (d *DiskCache) GetStats() *DiskCacheStats {
	// Return a copy for safety.
	return &DiskCacheStats{
		TotalSizeBytes:         atomic.LoadInt64(&d.stats.TotalSizeBytes),
		TotalNumFiles:          atomic.LoadInt64(&d.stats.TotalNumFiles),
		NumFilesStored:         atomic.LoadInt64(&d.stats.NumFilesStored),
		TotalStoredBytes:       atomic.LoadInt64(&d.stats.TotalStoredBytes),
		NumFilesGCed:           atomic.LoadInt64(&d.stats.NumFilesGCed),
		TotalGCedSizeBytes:     atomic.LoadInt64(&d.stats.TotalGCedSizeBytes),
		NumCacheHits:           atomic.LoadInt64(&d.stats.NumCacheHits),
		NumCacheMisses:         atomic.LoadInt64(&d.stats.NumCacheMisses),
		TotalCacheHitSizeBytes: atomic.LoadInt64(&d.stats.TotalCacheHitSizeBytes),
		InitTime:               time.Duration(atomic.LoadInt64((*int64)(&d.stats.InitTime))),
		TotalGCTime:            time.Duration(atomic.LoadInt64((*int64)(&d.stats.TotalGCTime))),
	}
}

func getKeyFromFileName(fname string) (key, error) {
	pair := strings.Split(fname, ".")
	if len(pair) != 2 {
		return key{}, fmt.Errorf("expected file name in the form hash[_ac].size, got %s", fname)
	}
	size, err := strconv.ParseInt(pair[1], 10, 64)
	if err != nil {
		return key{}, fmt.Errorf("invalid size in digest %s: %s", fname, err)
	}
	hash, isAc := strings.CutSuffix(pair[0], "_ac")
	dg, err := digest.New(hash, size)
	if err != nil {
		return key{}, fmt.Errorf("invalid digest from file name %s: %v", fname, err)
	}
	return key{digest: dg, isCas: !isAc}, nil
}

func (d *DiskCache) getPath(k key) string {
	suffix := ""
	if !k.isCas {
		suffix = "_ac"
	}
	return filepath.Join(d.root, k.digest.Hash[:2], fmt.Sprintf("%s%s.%d", k.digest.Hash[2:], suffix, k.digest.Size))
}

func (d *DiskCache) StoreCas(dg digest.Digest, path string) error {
	if dg.Size > int64(d.maxCapacityBytes) {
		return fmt.Errorf("blob size %d exceeds DiskCache capacity %d", dg.Size, d.maxCapacityBytes)
	}
	it := &qitem{
		key: key{digest: dg, isCas: true},
		lat: time.Now(),
	}
	it.mu.Lock()
	defer it.mu.Unlock()
	_, exists := d.store.LoadOrStore(it.key, it)
	if exists {
		return nil
	}
	d.mu.Lock()
	heap.Push(d.queue, it)
	d.mu.Unlock()
	if err := copyFile(path, d.getPath(it.key), dg.Size); err != nil {
		return err
	}
	d.statMu.Lock()
	d.stats.TotalSizeBytes += dg.Size
	d.stats.TotalStoredBytes += dg.Size
	newSize := uint64(d.stats.TotalSizeBytes)
	d.stats.TotalNumFiles++
	d.stats.NumFilesStored++
	d.statMu.Unlock()
	if newSize > d.maxCapacityBytes {
		select {
		case d.gcReq <- true:
		default:
		}
	}
	return nil
}

func (d *DiskCache) StoreActionCache(dg digest.Digest, ar *repb.ActionResult) error {
	bytes, err := proto.Marshal(ar)
	if err != nil {
		return err
	}
	size := uint64(len(bytes))
	if size > d.maxCapacityBytes {
		return fmt.Errorf("message size %d exceeds DiskCache capacity %d", size, d.maxCapacityBytes)
	}
	it := &qitem{
		key: key{digest: dg, isCas: false},
		lat: time.Now(),
	}
	it.mu.Lock()
	defer it.mu.Unlock()
	d.store.Store(it.key, it)

	d.mu.Lock()
	heap.Push(d.queue, it)
	d.mu.Unlock()
	if err := os.WriteFile(d.getPath(it.key), bytes, 0644); err != nil {
		return err
	}
	d.statMu.Lock()
	d.stats.TotalSizeBytes += int64(size)
	d.stats.TotalStoredBytes += int64(size)
	newSize := uint64(d.stats.TotalSizeBytes)
	d.stats.TotalNumFiles++
	d.stats.NumFilesStored++
	d.statMu.Unlock()
	if newSize > d.maxCapacityBytes {
		select {
		case d.gcReq <- true:
		default:
		}
	}
	return nil
}

func (d *DiskCache) gc() {
	start := time.Now()
	// Evict old entries until total size is below cap.
	var numFilesGCed, totalGCedSizeBytes int64
	var totalGCDiskOpsTime time.Duration
	for uint64(atomic.LoadInt64(&d.stats.TotalSizeBytes)) > d.maxCapacityBytes {
		d.mu.Lock()
		it := heap.Pop(d.queue).(*qitem)
		d.mu.Unlock()
		size, err := d.getItemSize(it.key)
		if err != nil {
			log.Errorf("error getting item size for %v: %v", it.key, err)
			size = 0
		}
		atomic.AddInt64(&d.stats.TotalSizeBytes, -size)
		numFilesGCed++
		totalGCedSizeBytes += size
		it.mu.Lock()
		diskOpsStart := time.Now()
		// We only delete the files, and not the prefix directories, because the prefixes are not worth worrying about.
		if err := os.Remove(d.getPath(it.key)); err != nil {
			log.Errorf("Error removing file: %v", err)
		}
		totalGCDiskOpsTime += time.Since(diskOpsStart)
		d.store.Delete(it.key)
		it.mu.Unlock()
	}
	d.statMu.Lock()
	d.stats.NumFilesGCed += numFilesGCed
	d.stats.TotalNumFiles -= numFilesGCed
	d.stats.TotalGCedSizeBytes += totalGCedSizeBytes
	d.stats.TotalGCDiskOpsTime += time.Duration(totalGCDiskOpsTime)
	d.stats.TotalGCTime += time.Since(start)
	d.statMu.Unlock()
}

func (d *DiskCache) daemon() {
	defer func() { d.gcDone <- true }()
	for {
		select {
		case <-d.shutdown:
			return
		case <-d.ctx.Done():
			return
		case <-d.gcReq:
			d.gc()
		}
	}
}

// Copy file contents retaining the source permissions.
func copyFile(src, dst string, size int64) error {
	srcInfo, err := os.Stat(src)
	if err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if err := out.Chmod(srcInfo.Mode()); err != nil {
		return err
	}
	defer out.Close()
	n, err := io.Copy(out, in)
	if err != nil {
		return err
	}
	// Required sanity check: if the file is being concurrently deleted, we may not always copy everything.
	if n != size {
		return fmt.Errorf("copy of %s to %s failed: src/dst size mismatch: wanted %d, got %d", src, dst, size, n)
	}
	return nil
}

// If the digest exists in the disk cache, copy the file contents to the given path.
func (d *DiskCache) LoadCas(dg digest.Digest, path string) bool {
	k := key{digest: dg, isCas: true}
	iUntyped, loaded := d.store.Load(k)
	if !loaded {
		atomic.AddInt64(&d.stats.NumCacheMisses, 1)
		return false
	}
	it := iUntyped.(*qitem)
	it.mu.RLock()
	err := copyFile(d.getPath(k), path, dg.Size)
	it.mu.RUnlock()
	if err != nil {
		// It is not possible to prevent a race with GC; hence, we return false on copy errors.
		atomic.AddInt64(&d.stats.NumCacheMisses, 1)
		return false
	}

	d.mu.Lock()
	d.queue.Bump(it)
	d.mu.Unlock()
	d.statMu.Lock()
	d.stats.NumCacheHits++
	d.stats.TotalCacheHitSizeBytes += dg.Size
	d.statMu.Unlock()
	return true
}

func (d *DiskCache) LoadActionCache(dg digest.Digest) (ar *repb.ActionResult, loaded bool) {
	k := key{digest: dg, isCas: false}
	iUntyped, loaded := d.store.Load(k)
	if !loaded {
		atomic.AddInt64(&d.stats.NumCacheMisses, 1)
		return nil, false
	}
	it := iUntyped.(*qitem)
	it.mu.RLock()
	ar = &repb.ActionResult{}
	size, err := d.loadActionResult(k, ar)
	if err != nil {
		// It is not possible to prevent a race with GC; hence, we return false on load errors.
		it.mu.RUnlock()
		atomic.AddInt64(&d.stats.NumCacheMisses, 1)
		return nil, false
	}
	it.mu.RUnlock()

	d.mu.Lock()
	d.queue.Bump(it)
	d.mu.Unlock()
	d.statMu.Lock()
	d.stats.NumCacheHits++
	d.stats.TotalCacheHitSizeBytes += int64(size)
	d.statMu.Unlock()
	return ar, true
}

func (d *DiskCache) loadActionResult(k key, ar *repb.ActionResult) (int, error) {
	bytes, err := os.ReadFile(d.getPath(k))
	if err != nil {
		return 0, err
	}
	n := len(bytes)
	// Required sanity check: sometimes the read pretends to succeed, but doesn't, if
	// the file is being concurrently deleted. Empty ActionResult is advised against in
	// the RE-API: https://github.com/bazelbuild/remote-apis/blob/main/build/bazel/remote/execution/v2/remote_execution.proto#L1052
	if n == 0 {
		return n, fmt.Errorf("read empty ActionResult for %v", k.digest)
	}
	if err := proto.Unmarshal(bytes, ar); err != nil {
		return n, fmt.Errorf("error unmarshalling %v as ActionResult: %v", bytes, err)
	}
	return n, nil
}
