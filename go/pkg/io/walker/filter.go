package walker

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"regexp"
)

// Filter specifies a filter for paths during traversal.
// The zero value matches nothing.
type Filter struct {
	// Regexp specifies what paths should match with this filter.
	//
	// The file separator must be the forwrad slash. All paths will have
	// their separators converted to forward slash before matching with this regexp.
	//
	// If nil, any path will match.
	Regexp *regexp.Regexp

	// Mode is matched using the equality operator.
	Mode fs.FileMode
}

// Path is used when the filter is a regexp only.
//
// If the filter's mode is set (not zero), false is returned regardless of the regexp value.
// If the regexp is nil, false is returned.
// If path matches the regexp, true is returned.
func (f Filter) Path(path string) bool {
	if f.Regexp == nil || f.Mode != 0 {
		return false
	}
	return f.match(path, f.Mode)
}

// File matches path and mode.
//
// If either of the filter's regexp or mode is not set, false is returned.
// Only if both path and mode match the filter's, true is returned.
func (f Filter) File(path string, mode fs.FileMode) bool {
	if f.Regexp == nil || f.Mode == 0 {
		return false
	}
	return f.match(path, mode)
}

func (f Filter) match(path string, mode fs.FileMode) bool {
	return mode == f.Mode && f.Regexp.MatchString(filepath.ToSlash(path))
}

// String returns a string representation of the filter.
//
// If this is a zero or a nil filter, the empty string is returned.
// A zero filter has regexp compiled from the empty string and 0 mode.
//
// It can be used as a stable identifier. However, keep in mind that
// multiple regular expressions may yield the same automaton. I.e. even
// if two filters have different identifiers, they may still yield the same
// traversal result.
func (f Filter) String() string {
	// zero filter: ""
	// regexp only: "<regexp>"
	// mode only: ";<mode>"
	// both: "<regexp>;<mode>"

	if f.Regexp == nil && f.Mode == 0 {
		return ""
	}

	reStr := ""
	if f.Regexp != nil {
		reStr = f.Regexp.String()
	}
	if f.Mode == 0 {
		return reStr
	}
	return fmt.Sprintf("%s;%d", reStr, f.Mode)
}
