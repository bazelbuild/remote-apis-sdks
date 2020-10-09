// Package tree provides functionality for constructing a Merkle tree of uploadable inputs.
package tree

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
	Files map[string]*fileNode
	Dirs  map[string]*treeNode
}

type fileNode struct {
	Chunker              *chunker.Chunker
	IsExecutable         bool
	EmptyDirectoryMarker bool
}

// Stats contains various stats/metadata of the constructed Merkle tree.
// Note that these stats count the overall input tree, even if some parts of it are not unique.
// For example, if a file "foo" of 10 bytes occurs 5 times in the tree, it will be counted as 5
// InputFiles and 50 TotalInputBytes.
type Stats struct {
	// The total number of input files.
	InputFiles int
	// The total number of input directories.
	InputDirectories int
	// The overall number of bytes from all the inputs.
	TotalInputBytes int64
	// TODO(olaola): number of FileMetadata cache hits/misses go here.
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
func loadFiles(execRoot string, excl []*command.InputExclusion, path string, fs map[string]*fileNode, chunkSize int, cache filemetadata.Cache) error {
	absPath := filepath.Clean(filepath.Join(execRoot, path))
	meta := cache.Get(absPath)
	t := command.FileInputType
	if smd := meta.Symlink; smd != nil && smd.IsDangling {
		return nil
	}
	if meta.Err != nil {
		e, ok := meta.Err.(*filemetadata.FileError)
		if !ok {
			return meta.Err
		}
		if !e.IsDirectory {
			return meta.Err
		}
		t = command.DirectoryInputType
	}
	if shouldIgnore(absPath, t, excl) {
		return nil
	}
	if t == command.FileInputType {
		fs[path] = &fileNode{
			Chunker:      chunker.NewFromFile(absPath, meta.Digest, chunkSize),
			IsExecutable: meta.IsExecutable,
		}
		return nil
	}
	// Directory
	files, err := ioutil.ReadDir(absPath)
	if err != nil {
		return err
	}

	if len(files) == 0 {
		fs[path] = &fileNode{EmptyDirectoryMarker: true}
		return nil
	}
	for _, f := range files {
		if e := loadFiles(execRoot, excl, filepath.Join(path, f.Name()), fs, chunkSize, cache); e != nil {
			return e
		}
	}
	return nil
}

// ComputeMerkleTree packages an InputSpec into uploadable inputs, returned as Chunkers.
func ComputeMerkleTree(execRoot string, is *command.InputSpec, chunkSize int, cache filemetadata.Cache) (root digest.Digest, inputs []*chunker.Chunker, stats *Stats, err error) {
	stats = &Stats{}
	fs := make(map[string]*fileNode)
	for _, i := range is.VirtualInputs {
		if i.Path == "" {
			return digest.Empty, nil, nil, errors.New("empty Path in VirtualInputs")
		}
		if i.IsEmptyDirectory {
			fs[i.Path] = &fileNode{EmptyDirectoryMarker: true}
			continue
		}
		fs[i.Path] = &fileNode{
			Chunker:      chunker.NewFromBlob(i.Contents, chunkSize),
			IsExecutable: i.IsExecutable,
		}
	}
	for _, i := range is.Inputs {
		if i == "" {
			return digest.Empty, nil, nil, errors.New("empty Input, use \".\" for entire exec root")
		}
		if e := loadFiles(execRoot, is.InputExclusions, i, fs, chunkSize, cache); e != nil {
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

func buildTree(files map[string]*fileNode) *treeNode {
	root := &treeNode{}
	for name, fn := range files {
		segs := strings.Split(filepath.Clean(name), string(filepath.Separator))
		// The last segment is the filename, so split it off.
		segs, base := segs[0:len(segs)-1], segs[len(segs)-1]

		node := root
		for _, s := range segs {
			if node.Dirs == nil {
				node.Dirs = make(map[string]*treeNode)
			}
			child := node.Dirs[s]
			if child == nil {
				child = &treeNode{}
				node.Dirs[s] = child
			}
			node = child
		}

		if fn.EmptyDirectoryMarker {
			if node.Dirs == nil {
				node.Dirs = make(map[string]*treeNode)
			}
			node.Dirs[base] = &treeNode{}
			continue
		}
		if node.Files == nil {
			node.Files = make(map[string]*fileNode)
		}
		node.Files[base] = fn
	}
	return root
}

func packageTree(t *treeNode, chunkSize int, stats *Stats) (root digest.Digest, blobs map[digest.Digest]*chunker.Chunker, err error) {
	dir := &repb.Directory{}
	blobs = make(map[digest.Digest]*chunker.Chunker)

	for name, child := range t.Dirs {
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

	for name, fn := range t.Files {
		dg := fn.Chunker.Digest()
		dir.Files = append(dir.Files, &repb.FileNode{Name: name, Digest: dg.ToProto(), IsExecutable: fn.IsExecutable})
		blobs[dg] = fn.Chunker
		stats.InputFiles++
		stats.TotalInputBytes += dg.Size
	}
	sort.Slice(dir.Files, func(i, j int) bool { return dir.Files[i].Name < dir.Files[j].Name })

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

// Output represents a leaf output node in a nested directory structure (a file, a symlink, or an empty directory).
type Output struct {
	Digest           digest.Digest
	Path             string
	IsExecutable     bool
	IsEmptyDirectory bool
	SymlinkTarget    string
}

// FlattenTree takes a Tree message and calculates the relative paths of all the files to
// the tree root. Note that only files/symlinks/empty directories are included in the returned slice,
// not the intermediate directories. Directories containing only other directories will be omitted.
func FlattenTree(tree *repb.Tree, rootPath string) (map[string]*Output, error) {
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

func flattenTree(root digest.Digest, rootPath string, dirs map[digest.Digest]*repb.Directory) (map[string]*Output, error) {
	// Create a queue of unprocessed directories, along with their flattened
	// path names.
	type queueElem struct {
		d digest.Digest
		p string
	}
	queue := []*queueElem{}
	queue = append(queue, &queueElem{d: root, p: rootPath})

	// Process the queue, recording all flattened Outputs as we go.
	flatFiles := make(map[string]*Output)
	for len(queue) > 0 {
		flatDir := queue[0]
		queue = queue[1:]

		dir, ok := dirs[flatDir.d]
		if !ok {
			return nil, fmt.Errorf("couldn't find directory %s with digest %s", flatDir.p, flatDir.d)
		}

		// Check whether this is an empty directory.
		if len(dir.Files)+len(dir.Directories)+len(dir.Symlinks) == 0 {
			flatFiles[flatDir.p] = &Output{
				Path:             flatDir.p,
				Digest:           digest.Empty,
				IsEmptyDirectory: true,
			}
			continue
		}
		// Add files to the set to return
		for _, file := range dir.Files {
			out := &Output{
				Path:         filepath.Join(flatDir.p, file.Name),
				Digest:       digest.NewFromProtoUnvalidated(file.Digest),
				IsExecutable: file.IsExecutable,
			}
			flatFiles[out.Path] = out
		}

		// Add symlinks to the set to return
		for _, sm := range dir.Symlinks {
			out := &Output{
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

	for name, child := range t.Dirs {
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

	for name, fn := range t.Files {
		dg := fn.Chunker.Digest()
		root.Files = append(root.Files, &repb.FileNode{Name: name, Digest: dg.ToProto(), IsExecutable: fn.IsExecutable})
		files[dg] = fn.Chunker
	}
	sort.Slice(root.Files, func(i, j int) bool { return root.Files[i].Name < root.Files[j].Name })
	return root, children, files, nil
}

// ComputeOutputsToUpload transforms the provided local output paths into uploadable Chunkers.
// The paths have to be relative to execRoot.
// It also populates the remote ActionResult, packaging output directories as trees where required.
func ComputeOutputsToUpload(execRoot string, paths []string, chunkSize int, cache filemetadata.Cache) (map[digest.Digest]*chunker.Chunker, *repb.ActionResult, error) {
	outs := make(map[digest.Digest]*chunker.Chunker)
	resPb := &repb.ActionResult{}
	for _, path := range paths {
		absPath := filepath.Clean(filepath.Join(execRoot, path))
		normPath, err := filepath.Rel(execRoot, absPath)
		if err != nil {
			return nil, nil, fmt.Errorf("path %v is not under exec root %v: %v", path, execRoot, err)
		}
		meta := cache.Get(absPath)
		if meta.Err == nil {
			// A regular file.
			ch := chunker.NewFromFile(absPath, meta.Digest, chunkSize)
			outs[meta.Digest] = ch
			resPb.OutputFiles = append(resPb.OutputFiles, &repb.OutputFile{Path: normPath, Digest: meta.Digest.ToProto(), IsExecutable: meta.IsExecutable})
			continue
		}
		e, ok := meta.Err.(*filemetadata.FileError)
		if !ok {
			return nil, nil, meta.Err
		}
		if e.IsNotFound {
			continue // Ignore missing outputs.
		}
		if !e.IsDirectory {
			return nil, nil, meta.Err
		}
		// A directory.
		fs := make(map[string]*fileNode)
		if e := loadFiles(absPath, nil, "", fs, chunkSize, cache); e != nil {
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
