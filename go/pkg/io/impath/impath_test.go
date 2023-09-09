package impath_test

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/impath"
)

func Test_Abs(t *testing.T) {
	tests := []struct {
		name  string
		parts []string
		want  string
		err   error
	}{
		{
			name:  "valid",
			parts: []string{impath.Root, "foo", "bar", "baz"},
			want:  filepath.Join(impath.Root, "foo", "bar", "baz"),
			err:   nil,
		},
		{
			name:  "not_absolute",
			parts: []string{"foo", "bar", "baz"},
			want:  impath.Root,
			err:   impath.ErrNotAbsolute,
		},
		{
			name:  "not_clean",
			parts: []string{impath.Root, "foo", "bar", "..", "baz"},
			want:  filepath.Join(impath.Root, "foo", "baz"),
			err:   nil,
		},
		{
			name:  "some_empty",
			parts: []string{impath.Root, "", "foo", "", "bar", "..", "baz"},
			want:  filepath.Join(impath.Root, "foo", "baz"),
			err:   nil,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			got, err := impath.Abs(test.parts...)
			if !errors.Is(err, test.err) {
				t.Errorf("unexpected error: %v", err)
			}
			if got.String() != test.want {
				t.Errorf("path mismatch: want %q, got %q", test.want, got)
			}
		})
	}
}

func Test_Rel(t *testing.T) {
	tests := []struct {
		name  string
		parts []string
		want  string
		err   error
	}{
		{
			name:  "valid",
			parts: []string{"foo", "bar", "baz"},
			want:  filepath.Join("foo", "bar", "baz"),
			err:   nil,
		},
		{
			name:  "not_relative",
			parts: []string{impath.Root, "foo", "bar", "baz"},
			want:  "",
			err:   impath.ErrNotRelative,
		},
		{
			name:  "not_clean",
			parts: []string{"foo", "bar", "..", "baz"},
			want:  filepath.Join("foo", "baz"),
			err:   nil,
		},
		{
			name:  "some_empty",
			parts: []string{"", "foo", "", "bar", "..", "baz"},
			want:  filepath.Join("foo", "baz"),
			err:   nil,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			got, err := impath.Rel(test.parts...)
			if !errors.Is(err, test.err) {
				t.Errorf("unexpected error: %v", err)
			}
			if got.String() != test.want {
				t.Errorf("path mismatch: want %q, got %q", test.want, got)
			}
		})
	}
}

func Test_AbsAppend(t *testing.T) {
	tests := []struct {
		name   string
		parent impath.Absolute
		parts  []impath.Relative
		want   string
		err    error
	}{
		{
			name:   "valid",
			parent: impath.MustAbs(impath.Root, "foo"),
			parts:  []impath.Relative{impath.MustRel("bar"), impath.MustRel("baz")},
			want:   filepath.Join(impath.Root, "foo", "bar", "baz"),
			err:    nil,
		},
		{
			name:   "not_clean",
			parent: impath.MustAbs(impath.Root, "foo"),
			parts:  []impath.Relative{impath.MustRel("bar"), impath.MustRel(".."), impath.MustRel("baz")},
			want:   filepath.Join(impath.Root, "foo", "baz"),
			err:    nil,
		},
		{
			name:   "zero",
			parent: impath.Absolute{},
			parts:  []impath.Relative{impath.MustRel("bar"), impath.MustRel("baz")},
			want:   filepath.Join(impath.Root, "bar", "baz"),
			err:    impath.ErrNotAbsolute,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			got := test.parent.Append(test.parts...)
			if got.String() != test.want {
				t.Errorf("path mismatch: want %q, got %q", test.want, got)
			}
		})
	}
}

func Test_RelAppend(t *testing.T) {
	tests := []struct {
		name  string
		parts []impath.Relative
		want  string
	}{
		{
			name:  "valid",
			parts: []impath.Relative{impath.MustRel("bar"), impath.MustRel("baz")},
			want:  filepath.Join("bar", "baz"),
		},
		{
			name:  "not_clean",
			parts: []impath.Relative{impath.MustRel("bar"), impath.MustRel(".."), impath.MustRel("baz")},
			want:  "baz",
		},
		{
			name:  "empty_elements",
			parts: []impath.Relative{{}, impath.MustRel("bar"), {}, impath.MustRel("baz"), {}},
			want:  filepath.Join("bar", "baz"),
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			got := test.parts[0].Append(test.parts[1:]...)
			if got.String() != test.want {
				t.Errorf("path mismatch: want %q, got %q", test.want, got)
			}
		})
	}
}

func Test_Descendant(t *testing.T) {
	tests := []struct {
		name   string
		base   impath.Absolute
		target impath.Absolute
		want   string
		err    error
	}{
		{
			name:   "valid",
			base:   impath.MustAbs(impath.Root, "a", "b"),
			target: impath.MustAbs(impath.Root, "a", "b", "c", "d"),
			want:   filepath.Join("c/d"),
			err:    nil,
		},
		{
			name:   "not_descendent",
			base:   impath.MustAbs(impath.Root, "a", "b"),
			target: impath.MustAbs(impath.Root, "c", "d"),
			want:   "",
			err:    impath.ErrNotDescendant,
		},
		{
			name:   "zero_base",
			base:   impath.Absolute{},
			target: impath.MustAbs(impath.Root, "c", "d"),
			want:   filepath.Join("c", "d"),
			err:    nil,
		},
		{
			name:   "zero_target",
			base:   impath.MustAbs(impath.Root, "a", "b"),
			target: impath.Absolute{},
			want:   "",
			err:    impath.ErrNotDescendant,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			got, err := impath.Descendant(test.base, test.target)
			if !errors.Is(err, test.err) {
				t.Errorf("unexpected error: %v", err)
			}
			if got.String() != test.want {
				t.Errorf("path mismatch: want %q, got %q", test.want, got)
			}
		})
	}
}

func Test_Dir(t *testing.T) {
	pathAbs := impath.MustAbs(impath.Root, "a", "b")
	wantAbs := impath.MustAbs(impath.Root, "a")
	gotAbs := pathAbs.Dir()
	if wantAbs.String() != gotAbs.String() {
		t.Errorf("path mismatch: want %q, got %q", wantAbs, gotAbs)
	}

	pathRel := impath.MustRel("a", "b")
	wantRel := impath.MustRel("a")
	gotRel := pathRel.Dir()
	if wantRel.String() != gotRel.String() {
		t.Errorf("path mismatch: want %q, got %q", wantAbs, gotRel)
	}
}
