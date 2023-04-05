// Package exppath (explicit path) provides an unambiguous interface to work with relative and absolute paths.
// The structures are immutable and allow strong guarantees at compile time.
// Using interface types for exported symbols helps guarantee that any object created using this package is always valid.
package exppath

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

// ErrBadPath indicates an invalid path based on the context it is returned from.
var ErrBadPath = errors.New("exppath: invalid path")

// Abs represents an immutable absolute path.
type Abs interface {
	// Abs is an alias to String that is used to make this interface unique.
	Abs() string
	// String returns the path.
	String() string
	Base() Abs
	Dir() Abs
}

// Rel represents an immutable relative path.
type Rel interface {
	// Rel is an alias to String that is used to make this interface unique.
	Rel() string
	// String returns the path.
	String() string
	Base() Rel
	Dir() Rel
}

type abs struct {
	path string
}

type rel struct {
	path string
}

// Abs is an alias to String().
func (p *abs) Abs() string {
	return p.path
}

// String returns the string representation of the path.
func (p *abs) String() string {
	return p.path
}

// Base is convenient proxy for filepath.Base.
func (p *abs) Base() Abs {
	return &abs{path: filepath.Base(p.path)}
}

// Dir is convenient proxy for filepath.Dir.
func (p *abs) Dir() Abs {
	return &abs{path: filepath.Dir(p.path)}
}

// Rel is an alias to String().
func (p *rel) Rel() string {
	return p.path
}

// String returns the string representation of the path.
func (p *rel) String() string {
	return p.path
}

func (p *rel) Base() Rel {
	return &rel{path: filepath.Base(p.path)}
}

func (p *rel) Dir() Rel {
	return &rel{path: filepath.Dir(p.path)}
}

// NewAbs creates a new absolute and clean path from the specified path.
// If the specified path is not absolute, ErrBadPath is returned.
func NewAbs(pathParts ...string) (Abs, error) {
	path := filepath.Join(pathParts...)
	if filepath.IsAbs(path) {
		return &abs{path: path}, nil
	}
	return nil, ErrBadPath
}

// NewRel creates a new relative and clean path from the specified path which must not be absolute.
// If the specified path is not relative, ErrBadPath is returned.
func NewRel(pathParts ...string) (Rel, error) {
	path := filepath.Join(pathParts...)
	if filepath.IsAbs(path) {
		return nil, ErrBadPath
	}
	return &rel{path: path}, nil
}

// JoinAbs is a convenient method to join multiple paths into an absolute path.
func JoinAbs(root Abs, parts ...Rel) Abs {
	ps := make([]string, len(parts)+1)
	ps[0] = root.String()
	for i, p := range parts {
		ps[i+1] = p.String()
	}
	return &abs{path: filepath.Join(ps...)}
}

// JoinRel is a convenient method to join multiple paths into a relative path.
func JoinRel(parts ...Rel) Rel {
	ps := make([]string, len(parts))
	for i, p := range parts {
		ps[i] = p.String()
	}
	return &rel{path: filepath.Join(ps...)}

}

// Descendant returns a relative path to the specified base path such that
// when joined together with the base using filepath.Join(base, path), the result
// is lexically equivalent to the specified target path.
// An error is returned if the specified path cannot be made relative to the specified base
// using filepath.Rel(base, target), or the target path is not a descendent of the base.
func Descendant(base Abs, target Abs) (Rel, error) {
	path, err := filepath.Rel(base.String(), target.String())
	if err != nil {
		return nil, err
	}
	if strings.HasPrefix(path, "..") {
		return nil, fmt.Errorf("path %q is not a descendant of %q", target, base)
	}
	return &rel{path: path}, nil
}

// MustRel is a convenient proxy method for filepath.Rel.
// It must only be called with arguments that have previously passed
// through filepath.Rel without errors.
func MustRel(base Abs, target Abs) Rel {
	path, err := filepath.Rel(base.String(), target.String())
	if err != nil {
		panic(err)
	}
	return &rel{path: path}
}
