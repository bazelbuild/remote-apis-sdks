package client

import (
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/filemetadata"
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
			relExecRoot, relSymDir, err := getTargetRelPath(execRoot, tc.path, tc.symMeta)
			if (err != nil) != tc.wantErr {
				t.Errorf("getTargetRelPath(path=%q) error: expected=%v got=%v", tc.path, tc.wantErr, err)
			}
			if err == nil && (relExecRoot != tc.wantRelExecRoot || relSymDir != tc.wantRelSymDir) {
				t.Errorf("getTargetRelPath(path=%q) result: expected=(%v,%v) got=(%v,%v)", tc.path, tc.wantRelExecRoot, tc.wantRelSymDir, relExecRoot, relSymDir)
			}
		})
	}
}
