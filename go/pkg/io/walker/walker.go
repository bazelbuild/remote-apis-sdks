// Package walker provides a simple interface for traversing the filesystem by abstracting away IO and implementation naunces.
package walker

import (
	"io"
	"io/fs"
	"os"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/io/impath"
)

// Internal shared actions.
const (
	aRead = iota
	aSkip
	aDefer
	aReplace
	aCancel
)

type (
	// PreAction is an enum that defines the valid actions for the pre-access callback.
	PreAction int

	// SymlinkAction is an enum that defines the valid actions for a the symlink callback.
	SymlinkAction int
)

const (
	// Access indicates that the walker should stat the path.
	Access PreAction = aRead

	// SkipPath indicates that the walker should skip the path and continue traversing.
	SkipPath PreAction = aSkip

	// Defer indicates that the walker should reschedule the path for another visit and continue traversing.
	Defer PreAction = aDefer

	// Follow indicates that the walker should follow the symlink.
	Follow SymlinkAction = aRead

	// Replace indicates that the walker should follow the symlink, but report every path from that sub-traversal as a descendant path of the symlink's path.
	//
	// For example: if /foo/bar was a symlink to /a/b, the walker would report the path of b as /foo/bar, and the path of a/b/c.d as /foo/bar/c.d.
	// This behavior is equivalent to copying the entire tree of the target in place of the symlink.
	Replace SymlinkAction = aReplace

	// SkipSymlink indicates that the walker should continue treversing without following the symlink.
	SkipSymlink SymlinkAction = aSkip
)

// Callback defines the implementations that the client should provide for the walker.
// Returning false from any callback cancels the entire walk.
//
// Defining a set of callbacks instead of a single shared one allows ensuring valid actions are returned at compile time.
// This is more robust than using implicit default actions or propagating errors at runtime.
type Callback struct {
	// Pre is called before accessing the path. Returning false cancels the entire walk.
	Pre func(path impath.Absolute, realPath impath.Absolute) (PreAction, bool)

	// Symlink is called for symlinks. Returning false cancels the entire walk.
	Symlink func(path impath.Absolute, realPath impath.Absolute, info fs.FileInfo) (SymlinkAction, bool)

	// Post is called for non-symlinks. Returning false cancels the entire walk.
	Post func(path impath.Absolute, realPath impath.Absolute, info fs.FileInfo) bool

	// Err is called when an io error is encountered. Returning false cancels the entire walk.
	Err func(path impath.Absolute, realPath impath.Absolute, err error) bool
}

type elem struct {
	info fs.FileInfo
	// realPath is the path used to perform syscalls with.
	realPath impath.Absolute
	// path is what a symlink target (or target descendant) should look like when it's replaced. Otherwise, it is identical to realPath.
	path impath.Absolute
	// a deferred parent is a directory that has already been pre-accessed and processed, but is still pending a post-access.
	deferredParent bool
}

// DepthFirst walks the filesystem tree rooted at the specified root in depth-first-search style traversal.
// That is, every directory is visited after all its descendants have been visited.
//
// Before visiting a path, the Pre callback is called. At this point, the client may instruct the walker to:
//
//	Skip the path;
//	Access and stat the path; or
//	Defer the path to be vistied after all its siblings have been visited.
//
// Deferred paths block traversing back up the tree since a parent requires all its children to be processed before itself to honour the DFS contract.
//
// If Pre returns Access, then either Symlink or Post is called after making a syscall to stat the path.
//
// If the stat syscall returned an error, it is passed to Err callback to let the client decide whether to skip the path or cancel the entire walk.
//
// If Symlink was called, the client may instruct the walker to:
//
//	Follow the symlink;
//	Replace the symlink with its target; or
//	Continue traversing without following the symlink.
//
// The client may at any point return false from any callback to cancel the entire walk.
func DepthFirst(root impath.Absolute, exclude Filter, cb Callback) {
	pending := &stack{}
	pending.push(elem{realPath: root, path: root})
	// parentIndex keeps track of the last directory so deferred children can be scheduled to be visited before it.
	parentIndex := &stack{}

	for pending.len() > 0 {
		e := pending.pop().(elem)
		pi := -1 // If root is deferred, it would be inserted at index 0.
		if parentIndex.len() > 0 {
			pi = parentIndex.peek().(int)
		}

		// If we're back to processing the directory, remove it from the parenthood chain.
		if pending.len() == pi {
			parentIndex.pop()
		}

		// If it's a previously pre-accessed directory, process its children.
		if !e.deferredParent && e.info != nil && e.info.IsDir() {
			deferred, action := processDir(e, exclude, cb)
			if action == aCancel {
				return
			}
			if action == aSkip {
				continue
			}
			// If there are deferred children, queue them to be visited before their parent.
			if len(deferred) > 0 {
				e.deferredParent = true
				// Remember the parent index to insert deferred children after it in the stack.
				parentIndex.push(pending.len())
				// Reschdule the parent to be visited after its children.
				pending.push(e)
				// Schedule the children to be visited before their parent.
				pending.push(deferred...)
				continue
			}
		}

		// For new paths, pre-access.
		// For pre-accessed paths (including processed directories), post-access.
		deferredElem, action := visit(e, exclude, cb)
		if action == aCancel {
			return
		}
		if action == aSkip {
			continue
		}
		if action == aDefer {
			// Reschedule a visit for this path before its parent.
			pending.insert(e, pi+1)
			continue
		}
		// If it's a pre-accessed directory or a followed symlink, schedule processing and post-access.
		if deferredElem != nil {
			pending.push(*deferredElem)
			continue
		}
	}
}

// visit performs pre-access, stat, and/or post-access depending on the state of the element.
//
// If aDefer is returned, it refers to the element from the arguments, not the returned reference.
// If the returned element reference is not nil, the returned int (action) is always aRead.
//
// Return values:
//
//	*elem is nil unless the visit has a deferred path, which is either a directory that was not post-accessed or a symlink target.
//	int is an action that is one of aRead, aDefer, aSkip, or aCancel.
func visit(e elem, exclude Filter, cb Callback) (*elem, int) {
	// If the filter applies to the path only, use it here.
	if exclude.MatchPath(e.path.String()) {
		return nil, aSkip
	}

	// Pre-access.
	if e.info == nil {
		if action, ok := cb.Pre(e.path, e.realPath); !ok {
			return nil, aCancel
		} else if action == aDefer || action == aSkip {
			return nil, int(action)
		}

		info, err := os.Lstat(e.realPath.String())
		if err != nil {
			if ok := cb.Err(e.path, e.realPath, err); !ok {
				return nil, aCancel
			}
			return nil, aSkip
		}

		// If the filter applies to path and mode, use it here.
		if exclude.MatchFile(e.path.String(), info.Mode()) {
			return nil, aSkip
		}

		e.info = info
		// If it's a directory, defer it to process its children before post-accessing it.
		if info.IsDir() {
			return &e, aRead
		}
	}

	// Post-access.
	if e.info.Mode()&fs.ModeSymlink != fs.ModeSymlink {
		if ok := cb.Post(e.path, e.realPath, e.info); !ok {
			return nil, aCancel
		}
		return nil, aRead
	}

	action, ok := cb.Symlink(e.path, e.realPath, e.info)
	if !ok {
		return nil, aCancel
	}
	if action == aSkip {
		return nil, aSkip
	}

	content, errTarget := os.Readlink(e.realPath.String())
	if errTarget != nil {
		if ok := cb.Err(e.path, e.realPath, errTarget); !ok {
			return nil, aCancel
		}
		return nil, aSkip
	}

	// If the symlink content is a relative path, append it to the symlink's directory to make it absolute.
	realTarget, errIm := impath.Abs(content)
	target := realTarget
	if errIm != nil {
		rel := impath.MustRel(content)
		realTarget = e.realPath.Dir().Append(rel)
		target = e.path.Dir().Append(rel)
	}

	deferredElem := &elem{realPath: realTarget, path: target}
	if action == Replace {
		deferredElem.path = e.path
	}

	return deferredElem, aRead
}

// processDir accepts a pre-accessed directory and visits all of its children.
//
// All the files (non-directories) will be completely visited within this call.
// All the directories will be pre-accessed, but not post-accessed.
// Return values:
//
//	[]any is nil unless the directory had deferred-children, which includes directories and client-deferred paths.
//	int is an action that is forwarded from the visit.
func processDir(dirElem elem, exclude Filter, cb Callback) ([]any, int) {
	var deferred []any

	f, errOpen := os.Open(dirElem.realPath.String())
	if errOpen != nil {
		if ok := cb.Err(dirElem.path, dirElem.realPath, errOpen); !ok {
			return deferred, aCancel
		}
		return deferred, aSkip
	}
	// Ignoring the error here is acceptable because the file was not modified in any way.
	// The only bad side effect may be a dangling descriptor that leaks until the process is terminated.
	defer f.Close()

	for {
		names, errRead := f.Readdirnames(128)

		if errRead != nil && errRead != io.EOF {
			if ok := cb.Err(dirElem.path, dirElem.realPath, errOpen); !ok {
				return deferred, aCancel
			}
			return deferred, aSkip
		}

		// Iterate before checking for EOF to avoid missing the last batch.
		for _, name := range names {
			// name is relative.
			relName := impath.MustRel(name)
			rp := dirElem.realPath.Append(relName)
			p := dirElem.path.Append(relName)

			// Pre-access.
			e := elem{realPath: rp, path: p}
			deferredElem, action := visit(e, exclude, cb)
			if action == aCancel {
				return deferred, aCancel
			}
			if action == aSkip {
				continue
			}
			if action == aDefer {
				deferred = append(deferred, e)
				continue
			}
			if deferredElem != nil {
				deferred = append(deferred, *deferredElem)
				continue
			}
		}

		if errRead == io.EOF {
			return deferred, aRead
		}
	}
}

// String return a textual version of the action.
func (s PreAction) String() string {
	switch s {
	case Access:
		return "Access"
	case SkipPath:
		return "Skip"
	case Defer:
		return "Defer"
	default:
		return "Unknown"
	}
}

// String return a textual version of the action.
func (a SymlinkAction) String() string {
	switch a {
	case Follow:
		return "Follow"
	case Replace:
		return "Replace"
	case SkipSymlink:
		return "Skip"
	default:
		return "Unknown"
	}
}
