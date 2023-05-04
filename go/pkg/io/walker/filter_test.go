package walker_test

import (
	"io/fs"
	"regexp"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/walker"
)

func TestFilterMatch(t *testing.T) {
	tests := []struct {
		name      string
		filter    walker.Filter
		paths     []string
		modes     []fs.FileMode
		wantsPath []bool
		wantsFile []bool
	}{
		{
			name:      "zero",
			filter:    walker.Filter{},
			paths:     []string{"/foo", "bar/baz"},
			modes:     []fs.FileMode{1, 2},
			wantsPath: []bool{false, false},
			wantsFile: []bool{false, false},
		},
		{
			name:      "regexp_only",
			filter:    walker.Filter{Regexp: regexp.MustCompile("/bar/")},
			paths:     []string{"/foo", "/bar/baz"},
			modes:     []fs.FileMode{1, 2},
			wantsPath: []bool{false, true},
			wantsFile: []bool{false, false},
		},
		{
			name:      "mode_only",
			filter:    walker.Filter{Mode: 4},
			paths:     []string{"/foo", "/bar/baz"},
			modes:     []fs.FileMode{1, 4},
			wantsPath: []bool{false, false},
			wantsFile: []bool{false, false},
		},
		{
			name:      "regexp_and_mode",
			filter:    walker.Filter{Regexp: regexp.MustCompile("/bar/"), Mode: 4},
			paths:     []string{"/foo", "/bar/baz"},
			modes:     []fs.FileMode{1, 4},
			wantsPath: []bool{false, false},
			wantsFile: []bool{false, true},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			for i := 0; i < len(test.paths); i++ {
				r := test.filter.Path(test.paths[i])
				if r != test.wantsPath[i] {
					t.Errorf("result mismatch for path %q: want %t, got %t", test.paths[i], test.wantsPath[i], r)
				}

				r = test.filter.File(test.paths[i], test.modes[i])
				if r != test.wantsFile[i] {
					t.Errorf("result mismatch for file %q with mode %d: want %t, got %t", test.paths[i], test.modes[i], test.wantsFile[i], r)
				}
			}
		})
	}
}

func TestFilterString(t *testing.T) {
	tests := []struct {
		name   string
		filter walker.Filter
		want   string
	}{
		{
			name:   "empty",
			filter: walker.Filter{},
			want:   "",
		},
		{
			name:   "regexp_only",
			filter: walker.Filter{Regexp: regexp.MustCompile("/bar/.*")},
			want:   "/bar/.*",
		},
		{
			name:   "regexp_and_mode",
			filter: walker.Filter{Regexp: regexp.MustCompile("/bar/.*"), Mode: 4},
			want:   "/bar/.*;4",
		},
		{
			name:   "mode_only",
			filter: walker.Filter{Mode: 4},
			want:   ";4",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			id := test.filter.String()
			if id != test.want {
				t.Errorf("invalid string: want %q, got %q", test.want, id)
			}
		})
	}
}
