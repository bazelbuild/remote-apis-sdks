package walker

import "io/fs"

// Filter defines an interface for matching paths during traversal.
// The matching semantics, whether matches are included or excluded, is defined by the consumer.
type Filter struct {
	// Path accepts a path, absolute or relative, and returns true if it's a match.
	Path func(path string) bool
	// File accepts a path, absolute or relative, and a mode and returns true if it's a match.
	File func(path string, mode fs.FileMode) bool
	// ID returns a unique identifier for this filter.
	// The identifier may be used by consumers to deduplicate filters so it must be unique within the consumers scope.
	ID func() string
}

// MatchPath delegates to Path if set. Otherwise, returns false.
func (f *Filter) MatchPath(path string) bool {
	if f == nil || f.Path == nil {
		return false
	}
	return f.Path(path)
}

// MatchFile delegates to File if set. Otherwise, returns false.
func (f *Filter) MatchFile(path string, mode fs.FileMode) bool {
	if f == nil || f.File == nil {
		return false
	}
	return f.File(path, mode)
}

// String delegates to ID if set. Otherwise, returns the empty string.
func (f Filter) String() string {
	// Value receiver is used to ensure the method is called on both a pointer and value receiver.
	if f.ID == nil {
		return ""
	}
	return f.ID()
}
