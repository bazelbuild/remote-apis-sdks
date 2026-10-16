package client

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/filemetadata"
	"github.com/google/go-cmp/cmp"
)

func TestGetAbsPath(t *testing.T) {
	tests := []struct {
		desc    string
		base    string
		relPath string
		wantErr bool
		wantRes string
	}{
		{
			desc:    "simple file under base",
			base:    "/tmp/output",
			relPath: "subdir/file.txt",
			wantRes: "/tmp/output/subdir/file.txt",
		},
		{
			desc:    "base itself",
			base:    "/tmp/output",
			relPath: ".",
			wantRes: "/tmp/output",
		},
		{
			desc:    "absolute path rejected",
			base:    "/tmp/output",
			relPath: "/etc/passwd",
			wantErr: true,
		},
		{
			desc:    "full escape rejected",
			base:    "/tmp/output",
			relPath: "../../etc/passwd",
			wantErr: true,
		},
		{
			// Sibling directory whose name starts with the same prefix as base.
			// e.g. base="/tmp/output", sibling="/tmp/outputevil"
			// Previously HasPrefix check was tricked into allowing this.
			desc:    "sibling directory with shared prefix rejected",
			base:    "/tmp/output",
			relPath: "../outputevil/secret",
			wantErr: true,
		},
		{
			desc:    "another sibling prefix bypass rejected",
			base:    "/home/user/build/output",
			relPath: "../outputextra/malware.sh",
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			res, err := getAbsPath(tc.base, tc.relPath)
			if tc.wantErr {
				if err == nil {
					t.Errorf("getAbsPath(%q, %q) = %q, want error", tc.base, tc.relPath, res)
				}
				return
			}
			if err != nil {
				t.Errorf("getAbsPath(%q, %q) unexpected error: %v", tc.base, tc.relPath, err)
				return
			}
			if res != tc.wantRes {
				t.Errorf("getAbsPath(%q, %q) = %q, want %q", tc.base, tc.relPath, res, tc.wantRes)
			}
		})
	}
}

func TestGetTargetRelPath(t *testing.T) {
	execRoot := "/execRoot"
	defaultSym := "symDir/sym"
	tests := []struct {
		desc            string
		path            string
		symMeta         *filemetadata.SymlinkMetadata
		wantErr         bool
		wantRelExecRoot string
		wantRelSymDir   string
	}{
		{
			desc:            "basic",
			path:            defaultSym,
			symMeta:         &filemetadata.SymlinkMetadata{Target: "foo"},
			wantRelExecRoot: "symDir/foo",
			wantRelSymDir:   "foo",
		},
		{
			desc: "relative target path under exec root",
			path: defaultSym,
			// /execRoot/symDir/../dir/foo ==> /execRoot/dir/foo
			symMeta:         &filemetadata.SymlinkMetadata{Target: "../dir/foo"},
			wantRelExecRoot: "dir/foo",
			wantRelSymDir:   "../dir/foo",
		},
		{
			desc: "relative target path escaping exec root",
			path: defaultSym,
			// /execRoot/symDir/../../foo ==> /foo
			symMeta: &filemetadata.SymlinkMetadata{Target: "../../foo"},
			wantErr: true,
		},
		{
			desc: "deeper relative target path",
			path: "base/sub/sym",
			// /execRoot/base/sub/../../foo ==> /execRoot/foo
			symMeta:         &filemetadata.SymlinkMetadata{Target: "../../foo"},
			wantRelExecRoot: "foo",
			wantRelSymDir:   "../../foo",
		},
		{
			desc:            "absolute target path under exec root",
			path:            "base/sym",
			symMeta:         &filemetadata.SymlinkMetadata{Target: execRoot + "/base/foo"},
			wantRelExecRoot: "base/foo",
			wantRelSymDir:   "foo",
		},
		{
			desc:    "abs target to rel target",
			path:    "base/sub1/sub2/sym",
			symMeta: &filemetadata.SymlinkMetadata{Target: execRoot + "/dir/foo"},
			// symlinkAbsDir: /execRoot/base/sub1/sub2
			// targetAbs: /execRoot/dir/foo
			// target rel to symlink: ../../../dir/foo
			wantRelExecRoot: "dir/foo",
			wantRelSymDir:   "../../../dir/foo",
		},
		{
			desc:    "absolute target path escaping exec root",
			path:    defaultSym,
			symMeta: &filemetadata.SymlinkMetadata{Target: "/another/dir/foo"},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			relExecRoot, relSymDir, err := getTargetRelPath(execRoot, tc.path, tc.symMeta.Target)
			if (err != nil) != tc.wantErr {
				t.Errorf("getTargetRelPath(path=%q) error: expected=%v got=%v", tc.path, tc.wantErr, err)
			}
			if err == nil && (relExecRoot != tc.wantRelExecRoot || relSymDir != tc.wantRelSymDir) {
				t.Errorf("getTargetRelPath(path=%q) result: expected=(%v,%v) got=(%v,%v)", tc.path, tc.wantRelExecRoot, tc.wantRelSymDir, relExecRoot, relSymDir)
			}
		})
	}
}

func TestEvalParentSymlinks(t *testing.T) {
	cache := filemetadata.NewSingleFlightCache()

	mkPath := func(path string) string {
		if filepath.Separator == '/' {
			return path
		}
		return filepath.Join(strings.Split(path, "/")...)
	}

	testCases := []struct {
		name string
		// List of relative file (no directories) paths with no intermediate symlinks.
		// All paths start under the "root" directory. To go outside, use `..`.
		// To denote a symlink, use the format: "symlink->target", e.g. "a/b->bb".
		// To denote a symlink with an absolute path for its target, prefix the target with a forward slash. E.g. "a/b->/bb".
		// Absolute symlinks also start under "root". To go outside, use `..`, e.g. `a/b->/../root2/bb`.
		fs []string
		// The path that includes intermediate symlinks.
		path         string
		materialize  bool
		wantPath     string
		wantSymlinks []string
		wantErr      bool
	}{
		{
			name: "one_relative_simple",
			fs: []string{
				"wd/a->aa",
				"wd/aa/b.go",
			},
			path:         mkPath("wd/a/b.go"),
			materialize:  false,
			wantPath:     mkPath("wd/aa/b.go"),
			wantSymlinks: []string{mkPath("wd/a")},
		},
		{
			name: "one_relative_basename_symlink",
			fs: []string{
				"wd/a->aa",
				"wd/aa/b.go->c.go",
				"wd/aa/c.go",
			},
			path:         mkPath("wd/a/b.go"),
			materialize:  false,
			wantPath:     mkPath("wd/aa/b.go"),
			wantSymlinks: []string{mkPath("wd/a")},
		},
		{
			name: "one_relative",
			fs: []string{
				"wd/a->../wd2/aa",
				"wd2/aa/b.go",
			},
			path:         mkPath("wd/a/b.go"),
			materialize:  false,
			wantPath:     mkPath("wd2/aa/b.go"),
			wantSymlinks: []string{mkPath("wd/a")},
		},
		{
			name: "one_absolute_simple",
			fs: []string{
				"wd/a->/wd/aa",
				"wd/aa/b.go",
			},
			path:         mkPath("wd/a/b.go"),
			materialize:  false,
			wantPath:     mkPath("wd/aa/b.go"),
			wantSymlinks: []string{mkPath("wd/a")},
		},
		{
			name: "one_absolute",
			fs: []string{
				"wd/a->/wd2/aa",
				"wd2/aa/b.go",
			},
			path:         mkPath("wd/a/b.go"),
			materialize:  false,
			wantPath:     mkPath("wd2/aa/b.go"),
			wantSymlinks: []string{mkPath("wd/a")},
		},
		{
			name: "multiple_relative",
			fs: []string{
				"wd/a->aa",
				"wd/aa/b->bb",
				"wd/aa/bb/c.go",
			},
			path:        mkPath("wd/a/b/c.go"),
			materialize: false,
			wantPath:    mkPath("wd/aa/bb/c.go"),
			wantSymlinks: []string{
				mkPath("wd/a"),
				mkPath("wd/aa/b"),
			},
		},
		{
			name: "multiple_absolute",
			fs: []string{
				"wd/a->/wd/aa",
				"wd/aa/b->/wd/aa/bb",
				"wd/aa/bb/c.go",
			},
			path:        mkPath("wd/a/b/c.go"),
			materialize: false,
			wantPath:    mkPath("wd/aa/bb/c.go"),
			wantSymlinks: []string{
				mkPath("wd/a"),
				mkPath("wd/aa/b"),
			},
		},
		{
			name: "one_relative_materialize_simple",
			fs: []string{
				"wd/a->../../root2/aa",
				"../root2/aa/b.go",
			},
			path:         mkPath("wd/a/b.go"),
			materialize:  true,
			wantPath:     mkPath("wd/a/b.go"),
			wantSymlinks: nil,
		},
		{
			name: "one_absolute_materialize_simple",
			fs: []string{
				"wd/a->/../root2/aa",
				"../root2/aa/b.go",
			},
			path:         mkPath("wd/a/b.go"),
			materialize:  true,
			wantPath:     mkPath("wd/a/b.go"),
			wantSymlinks: nil,
		},
		{
			name: "multiple_relative_materialize_simple",
			fs: []string{
				"wd/a->../../root2/aa",
				"../root2/aa/b->../../root3/aaa/bb",
				"../root3/aaa/bb/c.go",
			},
			path:         mkPath("wd/a/b/c.go"),
			materialize:  true,
			wantPath:     mkPath("wd/a/b/c.go"),
			wantSymlinks: nil,
		},
		{
			name: "multiple_absolute_materialize_simple",
			fs: []string{
				"wd/a->/../root2/aa",
				"../root2/aa/b->/../root3/aaa/bb",
				"../root3/aaa/bb/c.go",
			},
			path:         mkPath("wd/a/b/c.go"),
			materialize:  true,
			wantPath:     mkPath("wd/a/b/c.go"),
			wantSymlinks: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			root := filepath.Join(tmp, "root")
			for _, p := range tc.fs {
				slParts := strings.Split(p, "->")
				absPath := filepath.Join(root, mkPath(slParts[0]))
				dir := filepath.Dir(absPath)
				err := os.MkdirAll(dir, 0777)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}

				if len(slParts) > 1 {
					target := mkPath(slParts[1])
					if target[0] == '/' {
						target = filepath.Join(root, target[1:])
					}
					err = os.Symlink(target, absPath)
				} else {
					err = os.WriteFile(absPath, nil, 0777)
				}
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}

			evaledPath, symlinks, err := evalParentSymlinks(root, tc.path, tc.materialize, cache)
			if tc.wantErr && err == nil {
				t.Fatalf("expected an error, but did not get one")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			evaledPath = filepath.Clean(evaledPath)
			if evaledPath != tc.wantPath {
				t.Errorf("path mismatch: got %q, want %q", evaledPath, tc.wantPath)
			}
			sort.Strings(symlinks)
			sort.Strings(tc.wantSymlinks)
			if diff := cmp.Diff(tc.wantSymlinks, symlinks); diff != "" {
				t.Errorf("symlinks mismatch: got +, want -\n%s", diff)
			}
		})
	}
}

type pfCache struct {
	filemetadata.Cache
	mu      sync.Mutex
	calls   map[string]int
	entered chan string   // If non-nil, Get sends its path here first.
	gate    chan struct{} // If non-nil, Get blocks until it is closed.
}

func newPFCache() *pfCache {
	return &pfCache{Cache: filemetadata.NewNoopCache(), calls: map[string]int{}}
}

func (c *pfCache) Get(path string) *filemetadata.Metadata {
	c.mu.Lock()
	c.calls[path]++
	c.mu.Unlock()
	if c.entered != nil {
		c.entered <- path
	}
	if c.gate != nil {
		<-c.gate
	}
	return c.Cache.Get(path)
}

func (c *pfCache) count(path string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls[path]
}

func (c *pfCache) total() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, v := range c.calls {
		n += v
	}
	return n
}

func writePFFiles(t *testing.T, n int) (string, []string) {
	t.Helper()
	execRoot := t.TempDir()
	var rels []string
	for i := 0; i < n; i++ {
		rel := fmt.Sprintf("f%d", i)
		if err := os.WriteFile(filepath.Join(execRoot, rel), []byte(rel), 0644); err != nil {
			t.Fatal(err)
		}
		rels = append(rels, rel)
	}
	return execRoot, rels
}

func pfIsClosed(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestMetaPrefetcherNil(t *testing.T) {
	execRoot, rels := writePFFiles(t, 1)
	c := newPFCache()
	var p *metaPrefetcher
	p.prefetch(rels...)
	p.prefetchOne(rels[0])
	abs := filepath.Join(execRoot, rels[0])
	if md := p.get(c, rels[0], abs); md == nil || md.Err != nil {
		t.Fatalf("get() = %+v, want valid metadata", md)
	}
	if got := c.count(abs); got != 1 {
		t.Errorf("got %d lookups, want 1", got)
	}
	p.stop()
	p.stop()
}

func TestMetaPrefetcherStopIdempotentAndDisablesPrefetch(t *testing.T) {
	execRoot, rels := writePFFiles(t, 3)
	c := newPFCache()
	p := newMetaPrefetcher(c, execRoot, &TreeSymlinkOpts{}, 2)
	p.stop()
	p.stop()               // Must not double-close the channel.
	p.prefetchOne(rels[0]) // Must not send on the closed channel.
	p.prefetch(rels...)
	if len(p.pending) != 0 {
		t.Errorf("got %d pending after stop, want 0", len(p.pending))
	}
	abs := filepath.Join(execRoot, rels[0])
	if md := p.get(c, rels[0], abs); md == nil || md.Err != nil {
		t.Fatalf("get() after stop = %+v, want valid metadata", md)
	}
	if got := c.total(); got != 1 {
		t.Errorf("got %d lookups, want 1 (fallback only)", got)
	}
}

func TestMetaPrefetcherDuplicates(t *testing.T) {
	execRoot, rels := writePFFiles(t, 1)
	c := newPFCache()
	p := newMetaPrefetcher(c, execRoot, &TreeSymlinkOpts{}, 2)
	defer p.stop()
	rel, abs := rels[0], filepath.Join(execRoot, rels[0])
	p.prefetchOne(rel)
	p.prefetchOne(rel)
	p.prefetch(rel, rel, "")
	if len(p.pending) != 1 {
		t.Fatalf("got %d pending, want 1", len(p.pending))
	}
	<-p.pending[rel].done     // Ensure worker claimed and completed it.
	md1 := p.get(c, rel, abs) // Uses the future.
	md2 := p.get(c, rel, abs) // Future consumed: falls back, like serial's second lookup.
	if got := c.count(abs); got != 2 {
		t.Errorf("got %d lookups, want 2 (matches serial)", got)
	}
	if md1.Digest != md2.Digest {
		t.Errorf("digests differ: %v vs %v", md1.Digest, md2.Digest)
	}
}

func TestMetaPrefetcherHit(t *testing.T) {
	execRoot, rels := writePFFiles(t, 1)
	c := newPFCache()
	p := newMetaPrefetcher(c, execRoot, &TreeSymlinkOpts{}, 2)
	defer p.stop()
	rel, abs := rels[0], filepath.Join(execRoot, rels[0])
	p.prefetchOne(rel)
	f := p.pending[rel]
	if f == nil {
		t.Fatalf("expected future for %s in pending", rel)
	}
	<-f.done // Ensure worker completed it before get so get can't steal.
	md := p.get(c, rel, abs)
	if md == nil || md.Err != nil {
		t.Fatalf("get() = %+v, want valid metadata", md)
	}
	if got := c.count(abs); got != 1 {
		t.Errorf("got %d lookups, want 1", got)
	}
}

func TestMetaPrefetcherGetPathMismatch(t *testing.T) {
	execRoot, rels := writePFFiles(t, 2)
	c := newPFCache()
	p := newMetaPrefetcher(c, execRoot, &TreeSymlinkOpts{}, 2)
	defer p.stop()
	absA, absB := filepath.Join(execRoot, rels[0]), filepath.Join(execRoot, rels[1])
	p.prefetchOne(rels[0])
	<-p.pending[rels[0]].done     // Ensure worker has prefetched A to test path mismatch fallback.
	md := p.get(c, rels[0], absB) // Resolved differently than prefetched: must not reuse A's digest.
	if want := filemetadata.Compute(absB); md.Digest != want.Digest {
		t.Errorf("got digest %v, want %v (of %s)", md.Digest, want.Digest, absB)
	}
	if len(p.pending) != 0 {
		t.Errorf("future not consumed")
	}
	if c.count(absA) != 1 || c.count(absB) != 1 {
		t.Errorf("got lookups A=%d B=%d, want 1 each", c.count(absA), c.count(absB))
	}
}

func TestMetaPrefetcherMissingAndUnqueued(t *testing.T) {
	execRoot := t.TempDir()
	c := newPFCache()
	p := newMetaPrefetcher(c, execRoot, &TreeSymlinkOpts{}, 2)
	defer p.stop()
	p.prefetchOne("nope")
	if md := p.get(c, "nope", filepath.Join(execRoot, "nope")); md == nil || md.Err == nil {
		t.Errorf("get(missing) = %+v, want metadata with an error", md)
	}
	if md := p.get(c, "never_queued", filepath.Join(execRoot, "never_queued")); md == nil {
		t.Errorf("get(unqueued) = nil")
	}
}

// More paths than the channel holds with a single worker: prefetch must block, not drop or deadlock.
func TestMetaPrefetcherBackpressure(t *testing.T) {
	n := prefetchQueueSize + 500
	execRoot := t.TempDir()
	c := newPFCache()
	p := newMetaPrefetcher(c, execRoot, &TreeSymlinkOpts{}, 1)
	defer p.stop()
	rels := make([]string, n)
	for i := range rels {
		rels[i] = fmt.Sprintf("missing%d", i) // Lookups of missing files are fast.
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.prefetch(rels...)
	}()
	select {
	case <-done:
	case <-time.After(time.Minute):
		t.Fatal("prefetch did not return: possible deadlock")
	}
	if len(p.pending) != n {
		t.Fatalf("got %d pending, want %d (nothing dropped)", len(p.pending), n)
	}
	for _, r := range rels {
		p.get(c, r, filepath.Join(execRoot, r))
	}
	if got := c.total(); got != n {
		t.Errorf("got %d lookups, want %d (each exactly once)", got, n)
	}
}

// stop with work still queued must skip the queued lookups, close every future and not hang.
func TestMetaPrefetcherStopWithQueuedWork(t *testing.T) {
	const workers, n = 2, 100
	execRoot, rels := writePFFiles(t, n)
	c := newPFCache()
	c.entered = make(chan string, n)
	c.gate = make(chan struct{})
	p := newMetaPrefetcher(c, execRoot, &TreeSymlinkOpts{}, workers)
	p.prefetch(rels...)
	var futures []*metaFuture
	for _, f := range p.pending {
		futures = append(futures, f)
	}
	for i := 0; i < workers; i++ {
		<-c.entered // Every worker is now blocked inside cache.Get.
	}
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		p.stop()
	}()
	for !p.stopped.Load() {
		time.Sleep(time.Millisecond)
	}
	close(c.gate)
	select {
	case <-stopped:
	case <-time.After(time.Minute):
		t.Fatal("stop did not return: possible deadlock")
	}
	for _, f := range futures {
		if !pfIsClosed(f.done) {
			t.Errorf("future for %s not closed after stop", f.relPath)
		}
	}
	if got := c.total(); got != workers {
		t.Errorf("got %d lookups, want %d (queued work skipped after stop)", got, workers)
	}
}

func TestMetaPrefetcherZeroWorkers(t *testing.T) {
	if p := newMetaPrefetcher(newPFCache(), t.TempDir(), &TreeSymlinkOpts{}, 0); p != nil {
		t.Fatal("newMetaPrefetcher(workers=0) != nil; blocking prefetch would deadlock")
	}
}

type gatedAbsPathCache struct {
	filemetadata.Cache
	gatedPath string
	entered   chan string
	gate      chan struct{}
}

func (c *gatedAbsPathCache) Get(path string) *filemetadata.Metadata {
	if path == c.gatedPath {
		if c.entered != nil {
			c.entered <- path
		}
		<-c.gate
	}
	return c.Cache.Get(path)
}

func TestMetaPrefetcherClaimByGet(t *testing.T) {
	execRoot, rels := writePFFiles(t, 2)
	abs0, abs1 := filepath.Join(execRoot, rels[0]), filepath.Join(execRoot, rels[1])

	c := newPFCache()
	gate := make(chan struct{})
	gateCache := &gatedAbsPathCache{
		Cache:     c,
		gatedPath: abs0,
		entered:   make(chan string, 1),
		gate:      gate,
	}

	// 1 worker: will pick up rels[0] and block inside Get on gate.
	p := newMetaPrefetcher(gateCache, execRoot, &TreeSymlinkOpts{}, 1)
	defer p.stop()

	p.prefetch(rels[0], rels[1])

	// Wait until the single worker is blocked on abs0.
	select {
	case <-gateCache.entered:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for worker to enter lookup of abs0")
	}

	// rels[1] is still queued in work channel. Calling get for rels[1] must claim it and return
	// immediately without waiting for gate to be closed. Wrap in timeout so regression fails instead of hanging.
	type getResult struct {
		md *filemetadata.Metadata
	}
	done := make(chan getResult, 1)
	go func() {
		md := p.get(gateCache, rels[1], abs1)
		done <- getResult{md: md}
	}()

	var res getResult
	select {
	case res = <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("get(%s) hung or waited for worker while gate was closed", rels[1])
	}

	if res.md == nil || res.md.Err != nil {
		t.Fatalf("get(%s) = %+v, want valid metadata", rels[1], res.md)
	}

	// Unblock the worker so it finishes abs0 and drains rels[1].
	close(gate)

	abs0Meta := p.get(gateCache, rels[0], abs0)
	if abs0Meta == nil || abs0Meta.Err != nil {
		t.Fatalf("get(%s) = %+v, want valid metadata", rels[0], abs0Meta)
	}

	p.stop()

	// Both paths must have been looked up exactly once: abs0 by worker, abs1 by get.
	if got := c.count(abs0); got != 1 {
		t.Errorf("got %d lookups for abs0, want 1", got)
	}
	if got := c.count(abs1); got != 1 {
		t.Errorf("got %d lookups for abs1, want 1", got)
	}
}
