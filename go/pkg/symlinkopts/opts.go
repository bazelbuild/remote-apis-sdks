// Package symlinkopts provides an efficient interface to create unambiguous symlink options.
package symlinkopts

import (
	"fmt"
	"strings"
)

// Options represents a set of options for handling symlinks. The zero value is equivalent to skipping all symlinks.
type Options struct {
	preserve        bool
	noDangling      bool
	includeTarget   bool
	resolve         bool
	resolveExternal bool
}

// String returns a string representation of the options.
//
// The string has at most five letters:
//
//	P for Preserve
//	N for No Dangling
//	I for Include Target
//	R for Replace
//	E for Replace External
func (o Options) String() string {
	var b strings.Builder
	if o.preserve {
		fmt.Fprint(&b, "P")
	}
	if o.noDangling {
		fmt.Fprint(&b, "N")
	}
	if o.includeTarget {
		fmt.Fprint(&b, "I")
	}
	if o.resolve {
		fmt.Fprint(&b, "R")
	}
	if o.resolveExternal {
		fmt.Fprint(&b, "E")
	}
	return b.String()
}

// Preserve returns true if the options include the corresponding property.
func (o Options) Preserve() bool {
	return o.preserve
}

// NoDangling returns true if the options include the corresponding property.
func (o Options) NoDangling() bool {
	return o.noDangling
}

// IncludeTarget returns true if the options include the corresponding property.
func (o Options) IncludeTarget() bool {
	return o.includeTarget
}

// Resolve returns true if the options include the corresponding property.
func (o Options) Resolve() bool {
	return o.resolve
}

// ResolveExternal returns true if the options include the corresponding property.
func (o Options) ResolveExternal() bool {
	return o.resolveExternal
}

// Skip returns true if the options indicate that symlinks should be skipped.
func (o Options) Skip() bool {
	return !o.preserve && !o.noDangling && !o.includeTarget && !o.resolve && !o.resolveExternal
}

// ResolveAlways return the correct set of options to always resolve symlinks.
//
// This implies that symlinks are followed and no dangling symlinks are allowed.
// Every symlink will be replaced by its target. For example, if foo/bar was a symlink
// to the regular file baz, then foo/bar will become a regular file with the content of baz.
func ResolveAlways() Options {
	return Options{
		resolve:         true,
		resolveExternal: true,
		includeTarget:   true,
		noDangling:      true,
	}
}

// ResolveExternalOnly returns the correct set of options to only resolve symlinks
// if the target is outside the root directory. Otherwise, the symlink is preserved.
//
// This implies that all symlinks are followed, therefore, no dangling links are allowed.
// Otherwise, it's not possible to guarantee that all required files are under the root.
// Targets of non-external symlinks are not included.
func ResolveExternalOnly() Options {
	return Options{
		preserve:        true,
		resolveExternal: true,
		noDangling:      true,
	}
}

// ResolveExternalOnlyWithTarget is like ResolveExternalOnly but targets of non-external symlinks are included.
func ResolveExternalOnlyWithTarget() Options {
	return Options{
		preserve:        true,
		resolveExternal: true,
		includeTarget:   true,
		noDangling:      true,
	}
}

// PreserveWithTarget returns the correct set of options to preserve all symlinks and include the targets.
//
// This implies that dangling links are not allowed.
func PreserveWithTarget() Options {
	return Options{
		preserve:      true,
		includeTarget: true,
		noDangling:    true,
	}
}

// PreserveNoDangling returns the correct set of options to preserve all symlinks without targets.
//
// Targets need to be explicitly included.
// Dangling links are not allowed.
func PreserveNoDangling() Options {
	return Options{
		preserve:   true,
		noDangling: true,
	}
}

// PreserveAllowDangling returns the correct set of options to preserve all symlinks without targets.
//
// Targets need to be explicitly included.
// Dangling links are allowed.
func PreserveAllowDangling() Options {
	return Options{preserve: true}
}

// Skip is the zero value for Options which is equivalent to skipping all symlinks (as if they did not exist).
func Skip() Options {
	return Options{}
}
