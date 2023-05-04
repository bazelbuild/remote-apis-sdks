// Package impath (immutable path) provides immutable and distinguishable types for absolute and relative paths.
// This allows for strong guarantees at compile time, improves legibility and reduces maintenance burden.
package impath

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/errors"
)

var (
	// ErrNotAbsolute indicates that the path is not absolute.
	ErrNotAbsolute = errors.New("path is not absolute")

	// ErrNotRelative indicates that the path is not relative.
	ErrNotRelative = errors.New("path is not relative")

	// ErrNotDescendant indicates that the target is a descendant of base.
	ErrNotDescendant = errors.New("target is not a descendant of base")

	// Root is / on unix-like systems and c:\ on Windows.
	Root = os.Getenv("SYSTEMDRIVE") + string(os.PathSeparator)
)

// Absolute represents an immutable absolute path.
// The zero value is system root, which is `/` on Unix and `C:\` on Windows.
type Absolute struct {
	path string
}

// Relative represents an immutable relative path.
// The zero value is the empty path, which is an alias to the current directory `.`.
type Relative struct {
	path string
}

// Dir is a convenient method that returns all path elements except the last.
func (p Absolute) Dir() Absolute {
	return Absolute{path: filepath.Dir(p.path)}
}

// Dir is a convenient method that returns all path elements except the last.
func (p Relative) Dir() Relative {
	return Relative{path: filepath.Dir(p.path)}
}

// Base is a convenient method that returns the last path element.
func (p Absolute) Base() Absolute {
	return Absolute{path: filepath.Base(p.path)}
}

// Base is a convenient method that returns the last path element.
func (p Relative) Base() Relative {
	return Relative{path: filepath.Base(p.path)}
}

// String implements the Stringer interface and returns the path as a string.
func (p Absolute) String() string {
	pstr := string(p.path)
	if pstr == "" {
		return Root
	}
	return pstr
}

// String implements the Stringer interface and returns the path as a string.
func (p Relative) String() string {
	return string(p.path)
}

func join(base string, elements []Relative) []string {
	paths := make([]string, len(elements)+1)
	paths[0] = base
	for i, p := range elements {
		paths[i+1] = p.String()
	}
	return paths
}

// Append is a convenient method to join additional elements to this path.
func (p Absolute) Append(elements ...Relative) Absolute {
	paths := join(p.String(), elements)
	return Absolute{path: filepath.Join(paths...)}
}

// Append is a convenient method to join additional elements to this path.
func (p Relative) Append(elements ...Relative) Relative {
	paths := join(p.String(), elements)
	return Relative{path: filepath.Join(paths...)}
}

var (
	zeroAbs = Absolute{}
	zeroRel = Relative{}
)

// Abs creates a new absolute and clean path from the specified elements.
//
// If the specified elements do not join to a valid absolute path, ErrNotAbsolute is returned.
func Abs(elements ...string) (Absolute, error) {
	p := filepath.Join(elements...)
	if filepath.IsAbs(p) {
		return Absolute{path: p}, nil
	}
	return zeroAbs, errors.Join(ErrNotAbsolute, fmt.Errorf("path %q", p))
}

// MustAbs is a convenient wrapper of ToAbs that is useful for constant paths.
//
// If the specified elements do not join to a valid absolute path, this function panics.
func MustAbs(elements ...string) Absolute {
	p, err := Abs(elements...)
	if err != nil {
		panic(err)
	}
	return p
}

// Rel creates a new relative and clean path from the specified elements.
//
// If the specified elements do not join to a valid relative path, ErrNotRelative is returned.
func Rel(elements ...string) (Relative, error) {
	p := filepath.Join(elements...)
	if filepath.IsAbs(p) {
		return zeroRel, errors.Join(ErrNotRelative, fmt.Errorf("path %q", p))
	}
	return Relative{path: p}, nil
}

// MustRel is a convenient wrapper of ToRel that is useful for constant paths.
//
// If the specified elements do not join to a valid relative path, this function panics.
func MustRel(elements ...string) Relative {
	p, err := Rel(elements...)
	if err != nil {
		panic(err)
	}
	return p
}

// Descendant returns a relative path to the specified base path such that
// when joined together with the base using filepath.Join(base, path), the result
// is lexically equivalent to the specified target path.
//
// The returned error is nil, ErrNotRelative, or ErrNotDescendant.
// ErrNotRelative indicates that the target cannot be made relative to base.
// ErrNotDescendant indicates the target is not a descendant of base, even though it is relative.
func Descendant(base Absolute, target Absolute) (Relative, error) {
	p, err := filepath.Rel(base.String(), target.String())
	if err != nil {
		// On unix, this should never happen since all absolute paths share the same root.
		// On Windows, it's possible for two absolute paths to have different roots (different drive letters).
		return zeroRel, errors.Join(ErrNotRelative, err)
	}
	if strings.HasPrefix(p, "..") {
		return zeroRel, errors.Join(ErrNotDescendant, fmt.Errorf("target %q is not a descendant of base %q", target, base))
	}
	return Relative{path: p}, nil
}
