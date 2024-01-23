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
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	log "github.com/golang/glog"
	"google.golang.org/protobuf/proto"
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

const maxConcurrentRequests = 1000

// DiskCache is a local disk LRU CAS and Action Cache cache.
type DiskCache struct {
	root             string         // path to the root directory of the disk cache.
	maxCapacityBytes uint64         // if disk size exceeds this, old items will be evicted as needed.
	mu               sync.Mutex     // protects the queue.
	store            sync.Map       // map of keys to qitems.
	queue            *priorityQueue // keys by last accessed time.
	sizeBytes        int64          // total size.
	ctx              context.Context
	shutdown         chan bool
	gcTick           uint64
	gcReq            chan uint64
	testGcTicks      chan uint64
}

func New(ctx context.Context, root string, maxCapacityBytes uint64) *DiskCache {
	res := &DiskCache{
		root:             root,
		maxCapacityBytes: maxCapacityBytes,
		ctx:              ctx,
		queue: &priorityQueue{
			items: make([]*qitem, 1000),
		},
		gcReq:    make(chan uint64, maxConcurrentRequests),
		shutdown: make(chan bool),
	}
	heap.Init(res.queue)
	_ = os.MkdirAll(root, os.ModePerm)
	_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		// We log and continue on all errors, because cache read errors are not critical.
		if err != nil {
			log.Errorf("Error reading cache directory: %v", err)
			return nil
		}
		if d.IsDir() {
			return nil
		}
		fname := d.Name()
		k, err := res.getKeyFromFileName(fname)
		if err != nil {
			log.Errorf("Error parsing cached file name %s: %v", fname, err)
			return nil
		}
		atime, err := GetLastAccessTime(filepath.Join(root, fname))
		if err != nil {
			log.Errorf("Error getting last accessed time of %s: %v", fname, err)
			return nil
		}
		it := &qitem{
			key: k,
			lat: atime,
		}
		size, err := res.getItemSize(k)
		if err != nil {
			log.Errorf("Error getting file size of %s: %v", fname, err)
			return nil
		}
		res.store.Store(k, it)
		atomic.AddInt64(&res.sizeBytes, size)
		heap.Push(res.queue, it)
		return nil
	})
	go res.gc()
	return res
}

func (d *DiskCache) getItemSize(k key) (int64, error) {
	if k.isCas {
		return k.digest.Size, nil
	}
	fname := d.getPath(k)
	info, err := os.Stat(fname)
	if err != nil {
		return 0, fmt.Errorf("Error getting info for %s: %v", fname, err)
	}
	return info.Size(), nil
}

// Releases resources and terminates the GC daemon. Should be the last call to the DiskCache.
func (d *DiskCache) Shutdown() {
	d.shutdown <- true
}

func (d *DiskCache) TotalSizeBytes() uint64 {
	return uint64(atomic.LoadInt64(&d.sizeBytes))
}

// This function is defined in https://pkg.go.dev/strings#CutPrefix
// It is copy/pasted here as a hack, because I failed to upgrade the *Reclient* repo to the latest Go 1.20.7.
func CutPrefix(s, prefix string) (after string, found bool) {
	if !strings.HasPrefix(s, prefix) {
		return s, false
	}
	return s[len(prefix):], true
}

func (d *DiskCache) getKeyFromFileName(fname string) (key, error) {
	pair := strings.Split(fname, ".")
	if len(pair) != 2 {
		return key{}, fmt.Errorf("Expected file name in the form [ac_]hash/size, got %s", fname)
	}
	size, err := strconv.ParseInt(pair[1], 10, 64)
	if err != nil {
		return key{}, fmt.Errorf("invalid size in digest %s: %s", fname, err)
	}
	hash, isAc := CutPrefix(pair[0], "ac_")
	dg, err := digest.New(hash, size)
	if err != nil {
		return key{}, fmt.Errorf("invalid digest from file name %s: %v", fname, err)
	}
	return key{digest: dg, isCas: !isAc}, nil
}

func (d *DiskCache) getPath(k key) string {
	prefix := ""
	if !k.isCas {
		prefix = "ac_"
	}
	return filepath.Join(d.root, fmt.Sprintf("%s%s.%d", prefix, k.digest.Hash, k.digest.Size))
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
	newSize := uint64(atomic.AddInt64(&d.sizeBytes, dg.Size))
	if newSize > d.maxCapacityBytes {
		select {
		case d.gcReq <- atomic.AddUint64(&d.gcTick, 1):
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
	newSize := uint64(atomic.AddInt64(&d.sizeBytes, int64(size)))
	if newSize > d.maxCapacityBytes {
		select {
		case d.gcReq <- atomic.AddUint64(&d.gcTick, 1):
		default:
		}
	}
	return nil
}

func (d *DiskCache) gc() {
	for {
		select {
		case <-d.shutdown:
			return
		case <-d.ctx.Done():
			return
		case t := <-d.gcReq:
			// Evict old entries until total size is below cap.
			for uint64(atomic.LoadInt64(&d.sizeBytes)) > d.maxCapacityBytes {
				d.mu.Lock()
				it := heap.Pop(d.queue).(*qitem)
				d.mu.Unlock()
				size, err := d.getItemSize(it.key)
				if err != nil {
					log.Errorf("error getting item size for %v: %v", it.key, err)
					size = 0
				}
				atomic.AddInt64(&d.sizeBytes, -size)
				it.mu.Lock()
				if err := os.Remove(d.getPath(it.key)); err != nil {
					log.Errorf("Error removing file: %v", err)
				}
				d.store.Delete(it.key)
				it.mu.Unlock()
			}
			if d.testGcTicks != nil {
				select {
				case d.testGcTicks <- t:
				default:
				}
			}
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
	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}
	// Required sanity check: sometimes the copy pretends to succeed, but doesn't, if
	// the file is being concurrently deleted.
	dstInfo, err := os.Stat(dst)
	if err != nil {
		return err
	}
	if dstInfo.Size() != size {
		return fmt.Errorf("copy of %s to %s failed: src/dst size mismatch: wanted %d, got %d", src, dst, size, dstInfo.Size())
	}
	return nil
}

// If the digest exists in the disk cache, copy the file contents to the given path.
func (d *DiskCache) LoadCas(dg digest.Digest, path string) bool {
	k := key{digest: dg, isCas: true}
	iUntyped, loaded := d.store.Load(k)
	if !loaded {
		return false
	}
	it := iUntyped.(*qitem)
	it.mu.RLock()
	if err := copyFile(d.getPath(k), path, dg.Size); err != nil {
		// It is not possible to prevent a race with GC; hence, we return false on copy errors.
		it.mu.RUnlock()
		return false
	}
	it.mu.RUnlock()

	d.mu.Lock()
	d.queue.Bump(it)
	d.mu.Unlock()
	return true
}

func (d *DiskCache) LoadActionCache(dg digest.Digest) (ar *repb.ActionResult, loaded bool) {
	k := key{digest: dg, isCas: false}
	iUntyped, loaded := d.store.Load(k)
	if !loaded {
		return nil, false
	}
	it := iUntyped.(*qitem)
	it.mu.RLock()
	ar = &repb.ActionResult{}
	if err := d.loadActionResult(k, ar); err != nil {
		// It is not possible to prevent a race with GC; hence, we return false on load errors.
		it.mu.RUnlock()
		return nil, false
	}
	it.mu.RUnlock()

	d.mu.Lock()
	d.queue.Bump(it)
	d.mu.Unlock()
	return ar, true
}

func (d *DiskCache) loadActionResult(k key, ar *repb.ActionResult) error {
	bytes, err := os.ReadFile(d.getPath(k))
	if err != nil {
		return err
	}
	// Required sanity check: sometimes the read pretends to succeed, but doesn't, if
	// the file is being concurrently deleted. Empty ActionResult is advised against in
	// the RE-API: https://github.com/bazelbuild/remote-apis/blob/main/build/bazel/remote/execution/v2/remote_execution.proto#L1052
	if len(bytes) == 0 {
		return fmt.Errorf("read empty ActionResult for %v", k.digest)
	}
	if err := proto.Unmarshal(bytes, ar); err != nil {
		return fmt.Errorf("error unmarshalling %v as ActionResult: %v", bytes, err)
	}
	return nil
}
