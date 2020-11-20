package client

import "testing"

func TestGetTargetRelPath(t *testing.T) {
	execRoot := "/execRoot"
	defaultSymlinkNormDir := "symDir"
	tests := []struct {
		desc           string
		symlinkNormDir string
		target         string
		wantErr        bool
		relTarget      string
	}{
		{
			desc:           "basic",
			symlinkNormDir: defaultSymlinkNormDir,
			target:         "foo",
			relTarget:      "foo",
		},
		{
			desc:           "relative target path under exec root",
			symlinkNormDir: defaultSymlinkNormDir,
			// /execRoot/symDir/../dir/foo ==> /execRoot/dir/foo
			target:    "../dir/foo",
			relTarget: "../dir/foo",
		},
		{
			desc:           "relative target path escaping exec root",
			symlinkNormDir: defaultSymlinkNormDir,
			// /execRoot/symDir/../../foo ==> /foo
			target:  "../../foo",
			wantErr: true,
		},
		{
			desc:           "deeper relative target path",
			symlinkNormDir: "base/sub",
			// /execRoot/base/sub/../../foo ==> /execRoot/foo
			target:    "../../foo",
			relTarget: "../../foo",
		},
		{
			desc:           "absolute target path under exec root",
			symlinkNormDir: "base",
			target:         execRoot + "/base/foo",
			relTarget:      "foo",
		},
		{
			desc:           "abs target to rel target",
			symlinkNormDir: "base/sub1/sub2",
			target:         execRoot + "/dir/foo",
			// symlinkAbsDir: /execRoot/base/sub1/sub2
			// targetAbs: /execRoot/dir/foo
			// target rel to symlink: ../../../dir/foo
			relTarget: "../../../dir/foo",
		},
		{
			desc:           "absolute target path escaping exec root",
			symlinkNormDir: defaultSymlinkNormDir,
			target:         "/another/dir/foo",
			wantErr:        true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			res, err := getTargetRelPath(execRoot, tc.symlinkNormDir, tc.target)
			if (err != nil) != tc.wantErr {
				t.Errorf("getTargetRelPath(target=%q) error: expected=%v got=%v", tc.target, tc.wantErr, err)
			}
			if err == nil && res != tc.relTarget {
				t.Errorf("getTargetRelPath(target=%q) result: expected=%v got=%v", tc.target, tc.relTarget, res)
			}
		})
	}
}
