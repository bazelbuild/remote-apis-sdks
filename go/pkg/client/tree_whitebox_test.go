package client

import "testing"

func TestGetTargetRelPath(t *testing.T) {
	execRoot := "/execRoot/dir"
	symlink := "sym"
	tests := []struct {
		desc      string
		target    string
		wantErr   bool
		relTarget string
	}{
		{
			desc:      "basic",
			target:    "foo",
			relTarget: "foo",
		},
		{
			desc:      "there and back again",
			target:    "../dir/sub/foo",
			relTarget: "sub/foo",
		},
		{
			desc:    "relative target path escaping exec root",
			target:  "../foo",
			wantErr: true,
		},
		{
			desc:      "absolute target path under exec root",
			target:    execRoot + "/sub/foo",
			relTarget: "sub/foo",
		},
		{
			desc:    "absolute target path escaping exec root",
			target:  "/another/dir/foo",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			res, err := getTargetRelPath(execRoot, symlink, tc.target)
			if (err != nil) != tc.wantErr {
				t.Errorf("getTargetRelPath(target=%q) error: expected=%v got=%v", tc.target, tc.wantErr, err)
			}
			if err == nil && res != tc.relTarget {
				t.Errorf("getTargetRelPath(target=%q) result: expected=%v got=%v", tc.target, tc.relTarget, res)
			}
		})
	}
}
