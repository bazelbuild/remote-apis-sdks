package client

// This module provides functionality for constructing a Merkle tree of uploadable inputs.
import (
	"errors"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/command"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/filemetadata"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/uploadinfo"

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
	ue           *uploadinfo.Entry
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
	// If true, the symlink target (if not dangling) is followed.
	FollowsTarget bool
}

// DefaultTreeSymlinkOpts returns a default DefaultTreeSymlinkOpts object.
func DefaultTreeSymlinkOpts() *TreeSymlinkOpts {
	return &TreeSymlinkOpts{
		FollowsTarget: true,
	}
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

// getTargetRelPath returns the part of the target relative to the symlink's
// directory, iff the target is under execRoot. Otherwise it returns an error.
func getTargetRelPath(execRoot, symlinkNormDir, target string) (string, error) {
	symlinkAbsDir := filepath.Clean(filepath.Join(execRoot, symlinkNormDir))
	if !filepath.IsAbs(target) {
		target = filepath.Clean(filepath.Join(symlinkAbsDir, target))
	}
	if _, err := getRelPath(execRoot, target); err != nil {
		return "", err
	}
	return filepath.Rel(symlinkAbsDir, target)
}

// preprocessSymlink returns two things: if the routine should continue, and if
// there is an error to be reported back.
func preprocessSymlink(execRoot, symlinkNormDir string, meta *filemetadata.SymlinkMetadata, opts *TreeSymlinkOpts) (bool, error) {
	if meta.IsDangling {
		// For now, we do not treat a dangling symlink as an error. In the case
		// where the symlink is not preserved (i.e. needs to be converted to a
		// file), we simply ignore this path in the finalized tree.
		return opts.Preserved, nil
	}
	if !opts.Preserved {
		// We will convert the symlink to a normal file, so it doesn't matter
		// where target is under execRoot or not.
		return true, nil
	}

	if _, err := getTargetRelPath(execRoot, symlinkNormDir, meta.Target); err != nil {
		return false, err
	}
	return true, nil
}

// loadFiles reads all files specified by the given InputSpec (descending into subdirectories
// recursively), and loads their contents into the provided map.
func loadFiles(execRoot string, excl []*command.InputExclusion, path string, fs map[string]*fileSysNode, cache filemetadata.Cache, opts *TreeSymlinkOpts) error {
	if opts == nil {
		opts = DefaultTreeSymlinkOpts()
	}
	absPath := filepath.Clean(filepath.Join(execRoot, path))
	meta := cache.Get(absPath)
	isSymlink := meta.Symlink != nil
	t := command.FileInputType
	if isSymlink && opts.Preserved {
		// An implication of this is that, if a path is a symlink to a
		// directory, then the symlink attribute takes precedence.
		t = command.SymlinkInputType
	} else if meta.IsDirectory {
		t = command.DirectoryInputType
	}
	if shouldIgnore(absPath, t, excl) {
		return nil
	}
	normPath, err := getRelPath(execRoot, absPath)
	if err != nil {
		return err
	}
	symlinkNormDir := ""
	if isSymlink {
		symlinkNormDir = filepath.Dir(normPath)
		cont, err := preprocessSymlink(execRoot, symlinkNormDir, meta.Symlink, opts)
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	} else if meta.Err != nil {
		return meta.Err
	}
	if t == command.FileInputType {
		fs[normPath] = &fileSysNode{
			file: &fileNode{
				ue:           uploadinfo.EntryFromFile(meta.Digest, absPath),
				isExecutable: meta.IsExecutable,
			},
		}
		return nil
	} else if t == command.SymlinkInputType {
		relTarget, err := getTargetRelPath(execRoot, symlinkNormDir, meta.Symlink.Target)
		if err != nil {
			return err
		}
		fs[normPath] = &fileSysNode{
			// We cannot directly use meta.Symlink.Target, because it could be
			// an absolute path. Since the remote worker will map the exec root
			// to a different directory, we must strip away the local exec root.
			// See https://github.com/bazelbuild/remote-apis-sdks/pull/229#discussion_r524830458
			symlink: &symlinkNode{target: relTarget},
		}
		if meta.Symlink.IsDangling || !opts.FollowsTarget {
			return nil
		}
		return loadFiles(execRoot, excl, filepath.Clean(filepath.Join(symlinkNormDir, relTarget)), fs, cache, opts)
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
		if e := loadFiles(execRoot, excl, filepath.Join(normPath, f.Name()), fs, cache, opts); e != nil {
			return e
		}
	}
	return nil
}

// ComputeMerkleTree packages an InputSpec into uploadable inputs, returned as uploadinfo.Entrys
func (c *Client) ComputeMerkleTree(execRoot string, is *command.InputSpec, cache filemetadata.Cache) (root digest.Digest, inputs []*uploadinfo.Entry, stats *TreeStats, err error) {
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
				ue:           uploadinfo.EntryFromBlob(i.Contents),
				isExecutable: i.IsExecutable,
			},
		}
	}
	for _, i := range is.Inputs {
		if i == "" {
			return digest.Empty, nil, nil, errors.New("empty Input, use \".\" for entire exec root")
		}
		if e := loadFiles(execRoot, is.InputExclusions, i, fs, cache, c.TreeSymlinkOpts); e != nil {
			return digest.Empty, nil, nil, e
		}
	}
	ft := buildTree(fs)
	var blobs map[digest.Digest]*uploadinfo.Entry
	root, blobs, err = packageTree(ft, stats)
	if err != nil {
		return digest.Empty, nil, nil, err
	}
	for _, ue := range blobs {
		inputs = append(inputs, ue)
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

func packageTree(t *treeNode, stats *TreeStats) (root digest.Digest, blobs map[digest.Digest]*uploadinfo.Entry, err error) {
	dir := &repb.Directory{}
	blobs = make(map[digest.Digest]*uploadinfo.Entry)

	for name, child := range t.dirs {
		dg, childBlobs, err := packageTree(child, stats)
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
		dg := fn.ue.Digest
		dir.Files = append(dir.Files, &repb.FileNode{Name: name, Digest: dg.ToProto(), IsExecutable: fn.isExecutable})
		blobs[dg] = fn.ue
		stats.InputFiles++
		stats.TotalInputBytes += dg.Size
	}
	sort.Slice(dir.Files, func(i, j int) bool { return dir.Files[i].Name < dir.Files[j].Name })

	for name, sn := range t.symlinks {
		dir.Symlinks = append(dir.Symlinks, &repb.SymlinkNode{Name: name, Target: sn.target})
		stats.InputSymlinks++
	}
	sort.Slice(dir.Symlinks, func(i, j int) bool { return dir.Symlinks[i].Name < dir.Symlinks[j].Name })

	ue, err := uploadinfo.EntryFromProto(dir)
	if err != nil {
		return digest.Empty, nil, err
	}
	dg := ue.Digest
	blobs[dg] = ue
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
	for _, ue := range tree.Children {
		dg, e := digest.NewFromMessage(ue)
		if e != nil {
			return nil, e
		}
		dirs[dg] = ue
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

func packageDirectories(t *treeNode) (root *repb.Directory, children map[digest.Digest]*repb.Directory, files map[digest.Digest]*uploadinfo.Entry, err error) {
	root = &repb.Directory{}
	children = make(map[digest.Digest]*repb.Directory)
	files = make(map[digest.Digest]*uploadinfo.Entry)

	for name, child := range t.dirs {
		chRoot, chDirs, childFiles, err := packageDirectories(child)
		if err != nil {
			return nil, nil, nil, err
		}
		ue, err := uploadinfo.EntryFromProto(chRoot)
		if err != nil {
			return nil, nil, nil, err
		}
		dg := ue.Digest
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
		dg := fn.ue.Digest
		root.Files = append(root.Files, &repb.FileNode{Name: name, Digest: dg.ToProto(), IsExecutable: fn.isExecutable})
		files[dg] = fn.ue
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
func (c *Client) ComputeOutputsToUpload(execRoot, workingDir string, paths []string, cache filemetadata.Cache) (map[digest.Digest]*uploadinfo.Entry, *repb.ActionResult, error) {
	outs := make(map[digest.Digest]*uploadinfo.Entry)
	resPb := &repb.ActionResult{}
	for _, path := range paths {
		absPath := filepath.Clean(filepath.Join(execRoot, workingDir, path))
		_, err := getRelPath(execRoot, absPath)
		if err != nil {
			return nil, nil, err
		}
		normPath, err := filepath.Rel(filepath.Join(execRoot, workingDir), absPath)
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
			ue := uploadinfo.EntryFromFile(meta.Digest, absPath)
			outs[meta.Digest] = ue
			resPb.OutputFiles = append(resPb.OutputFiles, &repb.OutputFile{Path: normPath, Digest: meta.Digest.ToProto(), IsExecutable: meta.IsExecutable})
			continue
		}
		// A directory.
		fs := make(map[string]*fileSysNode)
		if e := loadFiles(absPath, nil, "", fs, cache, c.TreeSymlinkOpts); e != nil {
			return nil, nil, e
		}
		ft := buildTree(fs)

		treePb := &repb.Tree{}
		rootDir, childDirs, files, err := packageDirectories(ft)
		if err != nil {
			return nil, nil, err
		}
		ue, err := uploadinfo.EntryFromProto(rootDir)
		if err != nil {
			return nil, nil, err
		}
		outs[ue.Digest] = ue
		treePb.Root = rootDir
		for _, c := range childDirs {
			treePb.Children = append(treePb.Children, c)
		}
		ue, err = uploadinfo.EntryFromProto(treePb)
		if err != nil {
			return nil, nil, err
		}
		outs[ue.Digest] = ue
		for _, ue := range files {
			outs[ue.Digest] = ue
		}
		resPb.OutputDirectories = append(resPb.OutputDirectories, &repb.OutputDirectory{Path: normPath, TreeDigest: ue.Digest.ToProto()})
		// Upload the child directories individually as well
		ueRoot, _ := uploadinfo.EntryFromProto(treePb.Root)
		outs[ueRoot.Digest] = ueRoot
		for _, child := range treePb.Children {
			ueChild, _ := uploadinfo.EntryFromProto(child)
			outs[ueChild.Digest] = ueChild
		}
	}
	return outs, resPb, nil
}
