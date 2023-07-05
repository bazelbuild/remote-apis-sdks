package walker_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/impath"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/walker"
	"github.com/google/go-cmp/cmp"
)

const (
	actionPre = iota
	actionPost
	actionSymlink
)

type (
	symlinks  = map[string]string
	actionVal struct {
		action int
		cancel bool
	}
	actions     = map[int]actionVal
	pathActions = map[string]actions
	pathCount   = map[string]int
)

func TestWalker(t *testing.T) {
	tests := []struct {
		name             string
		paths            []string // Use a trailing slash to mark directories.
		symlinks         symlinks
		root             string
		filter           walker.Filter
		pathActions      pathActions
		wantRealCount    pathCount
		wantDesiredCount pathCount
		wantErr          error
	}{
		{
			name:          "single_file",
			paths:         []string{"foo.c"},
			wantRealCount: pathCount{"foo.c": 2},
		},
		{
			name:          "empty_dir",
			paths:         []string{"foo"},
			wantRealCount: pathCount{"foo": 2},
		},
		{
			name:          "dir_single_file",
			paths:         []string{"foo/bar.c"},
			wantRealCount: pathCount{"foo": 2, "foo/bar.c": 2},
		},
		{
			name: "single_level",
			paths: []string{
				"foo/bar.c",
				"foo/baz.c",
			},
			wantRealCount: pathCount{"foo": 2, "foo/bar.c": 2, "foo/baz.c": 2},
		},
		{
			name: "two_levels_simple",
			paths: []string{
				"foo/a.z",
				"foo/bar/b.z",
			},
			wantRealCount: pathCount{"foo": 2, "foo/a.z": 2, "foo/bar": 2, "foo/bar/b.z": 2},
		},
		{
			name: "two_levels",
			paths: []string{
				"foo/a.z",
				"foo/b.z",
				"foo/bar/c.z",
				"foo/bar/baz/d.z",
				"foo/bar/baz/e.z",
			},
			wantRealCount: pathCount{
				"foo":             2,
				"foo/a.z":         2,
				"foo/b.z":         2,
				"foo/bar":         2,
				"foo/bar/baz":     2,
				"foo/bar/c.z":     2,
				"foo/bar/baz/d.z": 2,
				"foo/bar/baz/e.z": 2,
			},
		},
		{
			name:          "skip_file_by_path",
			paths:         []string{"foo.c"},
			filter:        walker.Filter{Path: func(path string) bool { return strings.HasSuffix(path, "foo.c") }},
			wantRealCount: pathCount{},
		},
		{
			name:          "path_cancel",
			paths:         []string{"foo.c"},
			pathActions:   pathActions{"foo.c": actions{actionPre: {cancel: true}}},
			wantRealCount: pathCount{"foo.c": 1},
		},
		{
			name:          "single_file_deferred",
			paths:         []string{"foo.c"},
			pathActions:   pathActions{"foo.c": actions{actionPre: {action: int(walker.Defer)}}},
			wantRealCount: pathCount{"foo.c": 3},
		},
		{
			name:          "single_dir_deferred",
			paths:         []string{"foo/"},
			pathActions:   pathActions{"foo": actions{actionPre: {action: int(walker.Defer)}}},
			wantRealCount: pathCount{"foo": 3},
		},
		{
			name: "deferred",
			paths: []string{
				"foo/a.z",
				"foo/b.z",
				"foo/bar/c.z",
				"foo/bar/baz/d.z",
				"foo/bar/baz/e.z",
			},
			pathActions: pathActions{
				"foo/b.z":         actions{actionPre: {action: int(walker.Defer)}},
				"foo/bar/baz/e.z": actions{actionPre: {action: int(walker.Defer)}},
			},
			wantRealCount: pathCount{
				"foo":             2,
				"foo/a.z":         2,
				"foo/b.z":         3,
				"foo/bar":         2,
				"foo/bar/baz":     2,
				"foo/bar/c.z":     2,
				"foo/bar/baz/d.z": 2,
				"foo/bar/baz/e.z": 3,
			},
		},
		{
			name:     "file_symlink",
			symlinks: symlinks{"foo.c": "bar.c"},
			wantRealCount: pathCount{
				"foo.c": 2,
				"bar.c": 2,
			},
		},
		{
			name:     "dir_symlink",
			paths:    []string{"foo/bar.c"},
			symlinks: symlinks{"foo.c": "foo/"},
			root:     "foo.c",
			wantRealCount: pathCount{
				"foo.c":     2,
				"foo/bar.c": 2,
				"foo":       2,
			},
		},
		{
			name:     "nested_symlink",
			paths:    []string{"foo/bar.c"},
			symlinks: symlinks{"foo/baz.c": "a.z"},
			root:     "foo", // Otherwise it is nondeterministic which top-level path is selected.
			wantRealCount: pathCount{
				"foo":       2,
				"foo/bar.c": 2,
				"foo/baz.c": 2,
				"a.z":       2,
			},
		},
		{
			name:        "skip_symlink",
			paths:       []string{"foo/bar.c"},
			symlinks:    symlinks{"foo.c": "foo/"},
			pathActions: pathActions{"foo.c": actions{actionSymlink: {action: int(walker.SkipSymlink)}}},
			wantRealCount: pathCount{
				"foo.c": 2,
			},
		},
		{
			name:     "relative_symlink",
			symlinks: symlinks{"foo/bar.c": "./baz.c"},
			wantRealCount: pathCount{
				"foo":       2,
				"foo/bar.c": 2,
				"foo/baz.c": 4, // 2 as a child of foo, and 2 as a symlink target.
			},
		},
		{
			name:        "replace_single_symlink",
			symlinks:    symlinks{"foo.c": "bar.c"},
			root:        "foo.c",
			pathActions: pathActions{"foo.c": actions{actionSymlink: {action: int(walker.Replace)}}},
			wantRealCount: pathCount{
				"foo.c": 2,
				"bar.c": 2,
			},
			wantDesiredCount: pathCount{
				"foo.c": 2,
			},
		},
		{
			name: "replace_symlink_dir",
			paths: []string{
				"bar/a.z",
				"bar/b.z",
				"bar/c/d.z",
			},
			symlinks:    symlinks{"foo": "bar/"},
			root:        "foo",
			pathActions: pathActions{"foo": actions{actionSymlink: {action: int(walker.Replace)}}},
			wantRealCount: pathCount{
				"foo":       2,
				"bar":       2,
				"bar/a.z":   2,
				"bar/b.z":   2,
				"bar/c":     2,
				"bar/c/d.z": 2,
			},
			wantDesiredCount: pathCount{
				"foo":       2,
				"foo/a.z":   2,
				"foo/b.z":   2,
				"foo/c":     2,
				"foo/c/d.z": 2,
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			tmp, root, fsLayout := makeFs(t, test.paths, test.symlinks)
			if test.root != "" {
				root = filepath.Join(tmp, test.root)
			}
			var realSeq []string
			dpvc := pathCount{} // desired path visit count
			walker.DepthFirst(impath.MustAbs(root), test.filter, walker.Callback{
				Err: func(_ impath.Absolute, _ impath.Absolute, err error) bool {
					t.Errorf("unexpected error: %v", err)
					return false
				},
				Pre: func(path impath.Absolute, realPath impath.Absolute) (walker.PreAction, bool) {
					p, _ := filepath.Rel(tmp, realPath.String())
					realSeq = append(realSeq, p)
					dp, _ := filepath.Rel(tmp, path.String())
					if dp != p {
						dpvc[dp]++
					}

					v := test.pathActions[p][actionPre]
					// Only defer once to avoid infinite loops.
					if v.action == int(walker.Defer) {
						test.pathActions[p][actionPre] = actionVal{action: int(walker.Access), cancel: v.cancel}
					}
					return walker.PreAction(v.action), !v.cancel
				},
				Post: func(path impath.Absolute, realPath impath.Absolute, _ fs.FileInfo) bool {
					p, _ := filepath.Rel(tmp, realPath.String())
					realSeq = append(realSeq, p)
					dp, _ := filepath.Rel(tmp, path.String())
					if dp != p {
						dpvc[dp]++
					}
					return !test.pathActions[p][actionPost].cancel
				},
				Symlink: func(path impath.Absolute, realPath impath.Absolute, _ fs.FileInfo) (walker.SymlinkAction, bool) {
					p, _ := filepath.Rel(tmp, realPath.String())
					realSeq = append(realSeq, p)
					dp, _ := filepath.Rel(tmp, path.String())
					if dp != p {
						dpvc[dp]++
					}
					v := test.pathActions[p][actionSymlink]
					return walker.SymlinkAction(v.action), !v.cancel
				},
			})
			pvc := validateSequence(t, realSeq, fsLayout)
			if diff := cmp.Diff(test.wantRealCount, pvc); diff != "" {
				t.Errorf("path visit count mismatch (-want +got):\n%s", diff)
			}
			if len(test.wantDesiredCount) == 0 && len(dpvc) == 0 {
				return
			}
			if diff := cmp.Diff(test.wantDesiredCount, dpvc); diff != "" {
				t.Errorf("desired path visit count mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func makeFs(t *testing.T, paths []string, symlinks symlinks) (string, string, map[string][]string) {
	t.Helper()

	if len(paths) == 0 && len(symlinks) == 0 {
		t.Fatalf("paths and symlinks cannot be both empty")
	}

	tmp := t.TempDir()

	// Map each dir to its children.
	fsLayout := map[string][]string{}
	for _, p := range paths {
		createFile(t, tmp, p)
		parent := filepath.Dir(p)
		fsLayout[parent] = append(fsLayout[parent], p)
	}
	for p, content := range symlinks {
		trg := content
		// If the symlink content is relative to the symlink itself, append it to its parent.
		// Otherwise, make it absolute.
		if strings.HasPrefix(content, "./") {
			content = content[2:]
			trg = filepath.Join(filepath.Dir(p), content)
		} else {
			content = filepath.Join(tmp, content)
		}
		createFile(t, tmp, trg)
		err := os.Symlink(content, filepath.Join(tmp, p))
		if err != nil {
			t.Errorf("io error: %v", err)
		}

		parent := filepath.Dir(p)
		fsLayout[parent] = append(fsLayout[parent], p)
		parent = filepath.Dir(trg)
		fsLayout[parent] = append(fsLayout[parent], trg)
	}
	// Recursively parse out parents from other parents.
	// E.g. if the map has the keys foo/bar and bar/baz, it should also include
	// the keys foo and bar as parent directories.
	moreParents := true
	for moreParents {
		moreParents = false
		for p := range fsLayout {
			parent := filepath.Dir(p)
			c := fsLayout[parent]
			if len(c) > 0 {
				continue
			}
			if parent != "." {
				moreParents = true
			}
			fsLayout[parent] = append(c, p)
		}
	}
	t.Logf("filesystem layout: %v", fsLayout)
	// The . must be the root of all directories.
	if _, ok := fsLayout["."]; !ok {
		t.Fatalf("root not present in pathChildren at . directory; is there an absolute path in the list? %v", fsLayout)
	}
	root := fsLayout["."][0]

	return tmp, filepath.Join(tmp, root), fsLayout
}

func createFile(t *testing.T, parent, p string) {
	// Check for suffix before joining since filepath.Join removes trailing slashes.
	d := p
	if !strings.HasSuffix(p, "/") {
		d = filepath.Dir(p)
	}
	if err := os.MkdirAll(filepath.Join(parent, d), 0766); err != nil {
		t.Fatalf("io error: %v", err)
	}
	if p == d {
		return
	}
	if err := os.WriteFile(filepath.Join(parent, p), nil, 0666); err != nil {
		t.Fatalf("io error: %v", err)
	}
}

// validateSequence checks that every path is visited after its children.
func validateSequence(t *testing.T, seq []string, fsLayout map[string][]string) pathCount {
	t.Helper()

	t.Logf("validating sequence: %v\n", seq)
	pathVisitCount := pathCount{}
	pendingParent := map[string]bool{}
	for _, p := range seq {
		pathVisitCount[p]++
		parent := filepath.Dir(p)
		// Parent should be visited after this child.
		pendingParent[parent] = true
		// If this child is itself a parent, mark it as done.
		delete(pendingParent, p)
	}
	delete(pendingParent, ".")
	if len(pendingParent) > 0 {
		pending := make([]string, 0, len(pendingParent))
		for p := range pendingParent {
			pending = append(pending, p)
		}
		t.Errorf("some paths were not fully visited: %v", pending)
	}
	return pathVisitCount
}
