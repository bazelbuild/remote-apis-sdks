// Package tree provides functionality for constructing a Merkle tree of uploadable inputs.
package client

import (
	"errors"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/chunker"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/command"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/filemetadata"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

// treeNode represents a file tree, which is an intermediate representation used to encode a Merkle
// tree later. It corresponds roughly to a *repb.Directory, but with pointers, not digests, used to
// refer to other nodes.
type treeNode struct {
	files    map[string]*fileNode
	dirs     map[string]*treeNode
	symlinks map[string]*symlinkNode
}

type fileNode struct {
	chunker      *chunker.Chunker
	isExecutable bool
}

type symlinkNode struct {
	target string
}

type fileSysNode struct {
	file                 *fileNode
	emptyDirectoryMarker bool
	symlink              *symlinkNode
}

// TreeStats contains various stats/metadata of the constructed Merkle tree.
// Note that these stats count the overall input tree, even if some parts of it are not unique.
// For example, if a file "foo" of 10 bytes occurs 5 times in the tree, it will be counted as 5
// InputFiles and 50 TotalInputBytes.
type TreeStats struct {
	// The total number of input files.
	InputFiles int
	// The total number of input directories.
	InputDirectories int
	// The total number of input symlinks
	InputSymlinks int
	// The overall number of bytes from all the inputs.
	TotalInputBytes int64
	// TODO(olaola): number of FileMetadata cache hits/misses go here.
}

// TreeSymlinkOpts controls how symlinks are handled when constructing a tree.
type TreeSymlinkOpts struct {
	// By default, a symlink is converted into its targeted file.
	// If true, preserve the symlink.
	Preserved bool
}

// shouldIgnore returns whether a given input should be excluded based on the given InputExclusions,
func shouldIgnore(inp string, t command.InputType, excl []*command.InputExclusion) bool {
	for _, r := range excl {
		if r.Type != command.UnspecifiedInputType && r.Type != t {
			continue
		}
		if m, _ := regexp.MatchString(r.Regex, inp); m {
			return true
		}
	}
	return false
}

// loadFiles reads all files specified by the given InputSpec (descending into subdirectories
// recursively), and loads their contents into the provided map.
func loadFiles(execRoot string, excl []*command.InputExclusion, path string, fs map[string]*fileSysNode, chunkSize int, cache filemetadata.Cache, opts TreeSymlinkOpts) error {
	absPath := filepath.Clean(filepath.Join(execRoot, path))
	normPath, err := getRelPath(execRoot, absPath)
	if err != nil {
		return err
	}
	meta := cache.Get(absPath)
	isSymlink := meta.Symlink != nil
	if isSymlink && meta.Symlink.IsDangling {
		if !opts.Preserved {
			return nil
		}
		// Right now we never treat a dangling symlink as an error. If we
		// choose not to preserve the symlink, it is simply skipped. Otherwise
		// we will create a corresponding symlink node in the tree.
	} else if meta.Err != nil {
		return meta.Err
	}
	t := command.FileInputType
	if isSymlink && opts.Preserved {
		t = command.SymlinkInputType
	} else if meta.IsDirectory {
		t = command.DirectoryInputType
	}
	if shouldIgnore(absPath, t, excl) {
		return nil
	}
	if t == command.FileInputType {
		fs[normPath] = &fileSysNode{
			file: &fileNode{
				chunker:      chunker.NewFromFile(absPath, meta.Digest, chunkSize),
				isExecutable: meta.IsExecutable,
			},
		}
		return nil
	} else if t == command.SymlinkInputType {
		fs[path] = &fileSysNode{
			// TODO: How to handle a target with absolute path?
			symlink: &symlinkNode{target: meta.Symlink.Target},
		}
		// NOTE: The target is not followed. It is the users' responsibility
		// to explicitly list the target in their InputSpec. Also note that if
		// a directory containing the target is specified, the entirety of that
		// directory will be followed.
		return nil
	}
	// Directory
	files, err := ioutil.ReadDir(absPath)
	if err != nil {
		return err
	}

	if len(files) == 0 {
		fs[normPath] = &fileSysNode{emptyDirectoryMarker: true}
		return nil
	}
	for _, f := range files {
		if e := loadFiles(execRoot, excl, filepath.Join(normPath, f.Name()), fs, chunkSize, cache, config); e != nil {
			return e
		}
	}
	return nil
}

// ComputeMerkleTree packages an InputSpec into uploadable inputs, returned as Chunkers.
func (c *Client) ComputeMerkleTree(execRoot string, is *command.InputSpec, chunkSize int, cache filemetadata.Cache) (root digest.Digest, inputs []*chunker.Chunker, stats *TreeStats, err error) {
	stats = &TreeStats{}
	fs := make(map[string]*fileSysNode)
	for _, i := range is.VirtualInputs {
		if i.Path == "" {
			return digest.Empty, nil, nil, errors.New("empty Path in VirtualInputs")
		}
		path := i.Path
		absPath := filepath.Clean(filepath.Join(execRoot, path))
		normPath, err := getRelPath(execRoot, absPath)
		if err != nil {
			return digest.Empty, nil, nil, err
		}
		if i.IsEmptyDirectory {
			fs[normPath] = &fileSysNode{emptyDirectoryMarker: true}
			continue
		}
		fs[normPath] = &fileSysNode{
			file: &fileNode{
				chunker:      chunker.NewFromBlob(i.Contents, chunkSize),
				isExecutable: i.IsExecutable,
			},
		}
	}
	for _, i := range is.Inputs {
		if i == "" {
			return digest.Empty, nil, nil, errors.New("empty Input, use \".\" for entire exec root")
		}
		if e := loadFiles(execRoot, is.InputExclusions, i, fs, chunkSize, cache, c.TreeSymlinkOpts); e != nil {
			return digest.Empty, nil, nil, e
		}
	}
	ft := buildTree(fs)
	var blobs map[digest.Digest]*chunker.Chunker
	root, blobs, err = packageTree(ft, chunkSize, stats)
	if err != nil {
		return digest.Empty, nil, nil, err
	}
	for _, ch := range blobs {
		inputs = append(inputs, ch)
	}
	return root, inputs, stats, nil
}

func buildTree(files map[string]*fileSysNode) *treeNode {
	root := &treeNode{}
	for name, fn := range files {
		segs := strings.Split(name, string(filepath.Separator))
		// The last segment is the filename, so split it off.
		segs, base := segs[0:len(segs)-1], segs[len(segs)-1]

		node := root
		for _, s := range segs {
			if node.dirs == nil {
				node.dirs = make(map[string]*treeNode)
			}
			child := node.dirs[s]
			if child == nil {
				child = &treeNode{}
				node.dirs[s] = child
			}
			node = child
		}

		if fn.emptyDirectoryMarker {
			if node.dirs == nil {
				node.dirs = make(map[string]*treeNode)
			}
			node.dirs[base] = &treeNode{}
			continue
		}
		if fn.file != nil {
			if node.files == nil {
				node.files = make(map[string]*fileNode)
			}
			node.files[base] = fn.file
		} else {
			if node.symlinks == nil {
				node.symlinks = make(map[string]*symlinkNode)
			}
			node.symlinks[base] = fn.symlink
		}
	}
	return root
}

func packageTree(t *treeNode, chunkSize int, stats *TreeStats) (root digest.Digest, blobs map[digest.Digest]*chunker.Chunker, err error) {
	dir := &repb.Directory{}
	blobs = make(map[digest.Digest]*chunker.Chunker)

	for name, child := range t.dirs {
		dg, childBlobs, err := packageTree(child, chunkSize, stats)
		if err != nil {
			return digest.Empty, nil, err
		}
		dir.Directories = append(dir.Directories, &repb.DirectoryNode{Name: name, Digest: dg.ToProto()})
		for d, b := range childBlobs {
			blobs[d] = b
		}
	}
	sort.Slice(dir.Directories, func(i, j int) bool { return dir.Directories[i].Name < dir.Directories[j].Name })

	for name, fn := range t.files {
		dg := fn.chunker.Digest()
		dir.Files = append(dir.Files, &repb.FileNode{Name: name, Digest: dg.ToProto(), IsExecutable: fn.isExecutable})
		blobs[dg] = fn.chunker
		stats.InputFiles++
		stats.TotalInputBytes += dg.Size
	}
	sort.Slice(dir.Files, func(i, j int) bool { return dir.Files[i].Name < dir.Files[j].Name })

	for name, sn := range t.symlinks {
		dir.Symlinks = append(dir.Symlinks, &repb.SymlinkNode{Name: name, Target: sn.target})
		stats.InputSymlinks++
	}
	sort.Slice(dir.Symlinks, func(i, j int) bool { return dir.Symlinks[i].Name < dir.Symlinks[j].Name })

	ch, err := chunker.NewFromProto(dir, chunkSize)
	if err != nil {
		return digest.Empty, nil, err
	}
	dg := ch.Digest()
	blobs[dg] = ch
	stats.TotalInputBytes += dg.Size
	stats.InputDirectories++
	return dg, blobs, nil
}

// TreeOutput represents a leaf output node in a nested directory structure (a file, a symlink, or an empty directory).
type TreeOutput struct {
	Digest           digest.Digest
	Path             string
	IsExecutable     bool
	IsEmptyDirectory bool
	SymlinkTarget    string
}

// FlattenTree takes a Tree message and calculates the relative paths of all the files to
// the tree root. Note that only files/symlinks/empty directories are included in the returned slice,
// not the intermediate directories. Directories containing only other directories will be omitted.
func (c *Client) FlattenTree(tree *repb.Tree, rootPath string) (map[string]*TreeOutput, error) {
	root, err := digest.NewFromMessage(tree.Root)
	if err != nil {
		return nil, err
	}
	dirs := make(map[digest.Digest]*repb.Directory)
	dirs[root] = tree.Root
	for _, ch := range tree.Children {
		dg, e := digest.NewFromMessage(ch)
		if e != nil {
			return nil, e
		}
		dirs[dg] = ch
	}
	return flattenTree(root, rootPath, dirs)
}

func flattenTree(root digest.Digest, rootPath string, dirs map[digest.Digest]*repb.Directory) (map[string]*TreeOutput, error) {
	// Create a queue of unprocessed directories, along with their flattened
	// path names.
	type queueElem struct {
		d digest.Digest
		p string
	}
	queue := []*queueElem{}
	queue = append(queue, &queueElem{d: root, p: rootPath})

	// Process the queue, recording all flattened TreeOutputs as we go.
	flatFiles := make(map[string]*TreeOutput)
	for len(queue) > 0 {
		flatDir := queue[0]
		queue = queue[1:]

		dir, ok := dirs[flatDir.d]
		if !ok {
			return nil, fmt.Errorf("couldn't find directory %s with digest %s", flatDir.p, flatDir.d)
		}

		// Check whether this is an empty directory.
		if len(dir.Files)+len(dir.Directories)+len(dir.Symlinks) == 0 {
			flatFiles[flatDir.p] = &TreeOutput{
				Path:             flatDir.p,
				Digest:           digest.Empty,
				IsEmptyDirectory: true,
			}
			continue
		}
		// Add files to the set to return
		for _, file := range dir.Files {
			out := &TreeOutput{
				Path:         filepath.Join(flatDir.p, file.Name),
				Digest:       digest.NewFromProtoUnvalidated(file.Digest),
				IsExecutable: file.IsExecutable,
			}
			flatFiles[out.Path] = out
		}

		// Add symlinks to the set to return
		for _, sm := range dir.Symlinks {
			out := &TreeOutput{
				Path:          filepath.Join(flatDir.p, sm.Name),
				SymlinkTarget: sm.Target,
			}
			flatFiles[out.Path] = out
		}

		// Add subdirectories to the queue
		for _, subdir := range dir.Directories {
			digest := digest.NewFromProtoUnvalidated(subdir.Digest)
			name := filepath.Join(flatDir.p, subdir.Name)
			queue = append(queue, &queueElem{d: digest, p: name})
		}
	}
	return flatFiles, nil
}

func packageDirectories(t *treeNode, chunkSize int) (root *repb.Directory, children map[digest.Digest]*repb.Directory, files map[digest.Digest]*chunker.Chunker, err error) {
	root = &repb.Directory{}
	children = make(map[digest.Digest]*repb.Directory)
	files = make(map[digest.Digest]*chunker.Chunker)

	for name, child := range t.dirs {
		chRoot, chDirs, childFiles, err := packageDirectories(child, chunkSize)
		if err != nil {
			return nil, nil, nil, err
		}
		ch, err := chunker.NewFromProto(chRoot, chunkSize)
		if err != nil {
			return nil, nil, nil, err
		}
		dg := ch.Digest()
		root.Directories = append(root.Directories, &repb.DirectoryNode{Name: name, Digest: dg.ToProto()})
		for d, b := range childFiles {
			files[d] = b
		}
		children[dg] = chRoot
		for d, b := range chDirs {
			children[d] = b
		}
	}
	sort.Slice(root.Directories, func(i, j int) bool { return root.Directories[i].Name < root.Directories[j].Name })

	for name, fn := range t.files {
		dg := fn.chunker.Digest()
		root.Files = append(root.Files, &repb.FileNode{Name: name, Digest: dg.ToProto(), IsExecutable: fn.isExecutable})
		files[dg] = fn.chunker
	}
	sort.Slice(root.Files, func(i, j int) bool { return root.Files[i].Name < root.Files[j].Name })
	return root, children, files, nil
}

func getRelPath(base, path string) (string, error) {
	rel, err := filepath.Rel(base, path)
	if err != nil || strings.HasPrefix(rel, "..") {
		return "", fmt.Errorf("path %v is not under %v", path, base)
	}
	return rel, nil
}

// ComputeOutputsToUpload transforms the provided local output paths into uploadable Chunkers.
// The paths have to be relative to execRoot.
// It also populates the remote ActionResult, packaging output directories as trees where required.
func (c *Client) ComputeOutputsToUpload(execRoot string, paths []string, chunkSize int, cache filemetadata.Cache) (map[digest.Digest]*chunker.Chunker, *repb.ActionResult, error) {
	outs := make(map[digest.Digest]*chunker.Chunker)
	resPb := &repb.ActionResult{}
	for _, path := range paths {
		absPath := filepath.Clean(filepath.Join(execRoot, path))
		normPath, err := getRelPath(execRoot, absPath)
		if err != nil {
			return nil, nil, err
		}
		meta := cache.Get(absPath)
		if meta.Err != nil {
			if e, ok := meta.Err.(*filemetadata.FileError); ok && e.IsNotFound {
				continue // Ignore missing outputs.
			}
			return nil, nil, meta.Err
		}
		if !meta.IsDirectory {
			// A regular file.
			ch := chunker.NewFromFile(absPath, meta.Digest, chunkSize)
			outs[meta.Digest] = ch
			resPb.OutputFiles = append(resPb.OutputFiles, &repb.OutputFile{Path: normPath, Digest: meta.Digest.ToProto(), IsExecutable: meta.IsExecutable})
			continue
		}
		// A directory.
		fs := make(map[string]*fileSysNode)
		if e := loadFiles(absPath, nil, "", fs, chunkSize, cache, c.TreeSymlinkOpts); e != nil {
			return nil, nil, e
		}
		ft := buildTree(fs)

		treePb := &repb.Tree{}
		rootDir, childDirs, files, err := packageDirectories(ft, chunkSize)
		if err != nil {
			return nil, nil, err
		}
		ch, err := chunker.NewFromProto(rootDir, chunkSize)
		if err != nil {
			return nil, nil, err
		}
		outs[ch.Digest()] = ch
		treePb.Root = rootDir
		for _, c := range childDirs {
			treePb.Children = append(treePb.Children, c)
		}
		ch, err = chunker.NewFromProto(treePb, chunkSize)
		if err != nil {
			return nil, nil, err
		}
		outs[ch.Digest()] = ch
		for _, ch := range files {
			outs[ch.Digest()] = ch
		}
		resPb.OutputDirectories = append(resPb.OutputDirectories, &repb.OutputDirectory{Path: normPath, TreeDigest: ch.Digest().ToProto()})
		// Upload the child directories individually as well
		chRoot, _ := chunker.NewFromProto(treePb.Root, chunkSize)
		outs[chRoot.Digest()] = chRoot
		for _, child := range treePb.Children {
			chChild, _ := chunker.NewFromProto(child, chunkSize)
			outs[chChild.Digest()] = chChild
		}
	}
	return outs, resPb, nil
}
