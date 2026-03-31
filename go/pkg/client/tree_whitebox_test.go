package client

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/filemetadata"
	"github.com/google/go-cmp/cmp"
)

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

func TestGetAbsPath(t *testing.T) {
	tmpDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(tmpDir, "base", "sub"), 0755); err != nil {
		t.Fatal(err)
	}

	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("os.Getwd failed: %v", err)
	}
	cwd = filepath.Clean(cwd)

	absBaseInTmp := filepath.Join(tmpDir, "base")

	tests := []struct {
		name      string
		base      string
		path      string
		wantError bool
		want      string
	}{
		// Absolute base
		{
			name: "abs_base,_rel_path",
			base: absBaseInTmp,
			path: "sub",
			want: filepath.Join(absBaseInTmp, "sub"),
		},
		{
			name: "abs_base,_rel_path_cleaned",
			base: absBaseInTmp,
			path: "sub/../other",
			want: filepath.Join(absBaseInTmp, "other"),
		},
		{
			name:      "abs_base,_rel_path_escaping",
			base:      absBaseInTmp,
			path:      "../other",
			wantError: true,
		},
		{
			name: "abs_base,_abs_path",
			base: absBaseInTmp,
			path: filepath.Join(absBaseInTmp, "sub"),
			want: filepath.Join(absBaseInTmp, "sub"),
		},
		{
			name:      "abs_base,_abs_path_escaping",
			base:      tmpDir,
			path:      cwd,
			wantError: true,
		},
		// Relative base "."
		{
			name: "dot_base,_rel_path",
			base: ".",
			path: "foo",
			want: filepath.Join(cwd, "foo"),
		},
		{
			name:      "dot_base,_rel_path_escaping",
			base:      ".",
			path:      "../foo",
			wantError: true,
		},
		{
			name: "dot_base,_abs_path_in_cwd",
			base: ".",
			path: filepath.Join(cwd, "foo"),
			want: filepath.Join(cwd, "foo"),
		},
		{
			name:      "dot_base,_abs_path_outside_cwd",
			base:      ".",
			path:      tmpDir,
			wantError: true,
		},
		{
			name: "dot_base,_dot_path",
			base: ".",
			path: ".",
			want: cwd,
		},
		// Relative base "foo/bar"
		{
			name: "rel_base_dir,_rel_path",
			base: "foo/bar",
			path: "baz",
			want: filepath.Join(cwd, "foo", "bar", "baz"),
		},
		{
			name:      "rel_base_dir,_rel_path_escaping",
			base:      "foo/bar",
			path:      "../../baz",
			wantError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := getAbsPath(tc.base, tc.path)
			if tc.wantError {
				if err == nil {
					t.Errorf("getAbsPath(%q, %q) succeeded, want error", tc.base, tc.path)
				}
				return
			}
			if err != nil {
				t.Fatalf("getAbsPath(%q, %q) failed: %v", tc.base, tc.path, err)
			}
			if got != tc.want {
				t.Errorf("getAbsPath(%q, %q) = %q, want %q", tc.base, tc.path, got, tc.want)
			}
		})
	}
}
