package client

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/bazelbuild/remote-apis-sdks/go/digest"
	"github.com/golang/protobuf/proto"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

// FileTree represents a file tree, which is an intermediate representation used to encode a Merkle
// tree later. It corresponds roughly to a *repb.Directory, but with pointers, not digests, used to
// refer to other nodes.
type FileTree struct {
	Files map[string][]byte
	Dirs  map[string]*FileTree
}

// BuildTree builds a FileTree out of a list of files. The tree isn't checked for validity if it is
// passed into PackageTree; that is when an error will be given.
func BuildTree(files map[string][]byte) *FileTree {
	// This is not the fastest way to build a Merkle tree, but it should do for the intended uses of
	// this library.
	root := &FileTree{}
	for name, cont := range files {
		segs := strings.Split(name, "/")
		// The last segment is the filename, so split it off.
		segs, base := segs[0:len(segs)-1], segs[len(segs)-1]

		node := root
		for _, s := range segs {
			if node.Dirs == nil {
				node.Dirs = make(map[string]*FileTree)
			}
			child := node.Dirs[s]
			if child == nil {
				child = &FileTree{}
				node.Dirs[s] = child
			}
			node = child
		}

		if node.Files == nil {
			node.Files = make(map[string][]byte)
		}
		node.Files[base] = cont
	}
	return root
}

// PackageTree packages a tree for upload to the CAS. It returns the digest of the root Directory,
// as well as the encoded blob forms of all the nodes and files.  They are provided in a
// digest->blob map for easier composition and recursion. An empty filename, or a file and
// directory with the same name are errors.
func PackageTree(t *FileTree) (root *repb.Digest, blobs map[digest.Key][]byte, err error) {
	if t == nil {
		return nil, nil, errors.New("nil FileTree while packaging tree")
	}
	dir := &repb.Directory{}
	blobs = make(map[digest.Key][]byte)

	for name, child := range t.Dirs {
		if name == "" {
			return nil, nil, errors.New("empty directory name while packaging tree")
		}
		dg, childBlobs, err := PackageTree(child)
		if err != nil {
			return nil, nil, err
		}
		dir.Directories = append(dir.Directories, &repb.DirectoryNode{Name: name, Digest: dg})
		for k, b := range childBlobs {
			blobs[k] = b
		}
	}
	sort.Slice(dir.Directories, func(i, j int) bool { return dir.Directories[i].Name < dir.Directories[j].Name })

	for name, cont := range t.Files {
		if name == "" {
			return nil, nil, errors.New("empty file name while packaging tree")
		}
		if _, ok := t.Dirs[name]; ok {
			return nil, nil, errors.New("directory and file with the same name while packaging tree")
		}
		dg := digest.FromBlob(cont)
		dir.Files = append(dir.Files, &repb.FileNode{Name: name, Digest: dg, IsExecutable: true})
		blobs[digest.ToKey(dg)] = cont
	}
	sort.Slice(dir.Files, func(i, j int) bool { return dir.Files[i].Name < dir.Files[j].Name })

	encDir, err := proto.Marshal(dir)
	if err != nil {
		return nil, nil, err
	}
	dg := digest.FromBlob(encDir)
	blobs[digest.ToKey(dg)] = encDir
	return dg, blobs, nil
}

// Output represents a leaf output node in a nested directory structure (either a file or a
// symlink).
type Output struct {
	Digest        digest.Key
	Path          string
	IsExecutable  bool
	SymlinkTarget string
}

// FlattenTree takes a Tree message and calculates the relative paths of all the files to
// the tree root. Note that only files are included in the returned slice, not the intermediate
// directories. Empty directories will be skipped, and directories containing only other directories
// will be omitted as well.
func FlattenTree(tree *repb.Tree, rootPath string) (map[string]*Output, error) {
	root, err := digest.FromProto(tree.Root)
	if err != nil {
		return nil, err
	}
	dirs := make(map[digest.Key]*repb.Directory)
	dirs[digest.ToKey(root)] = tree.Root
	for _, ch := range tree.Children {
		dg, e := digest.FromProto(ch)
		if e != nil {
			return nil, e
		}
		dirs[digest.ToKey(dg)] = ch
	}
	return flattenTree(root, rootPath, dirs)
}

func flattenTree(root *repb.Digest, rootPath string, dirs map[digest.Key]*repb.Directory) (map[string]*Output, error) {
	// Create a queue of unprocessed directories, along with their flattened
	// path names.
	type queueElem struct {
		d digest.Key
		p string
	}
	queue := []*queueElem{}
	queue = append(queue, &queueElem{d: digest.ToKey(root), p: rootPath})

	// Process the queue, recording all flattened Outputs as we go.
	flatFiles := make(map[string]*Output)
	for len(queue) > 0 {
		flatDir := queue[0]
		queue = queue[1:]

		dir, ok := dirs[flatDir.d]
		if !ok {
			return nil, fmt.Errorf("couldn't find directory %s with digest %v", flatDir.p, flatDir.d)
		}

		// Add files to the set to return
		for _, file := range dir.Files {
			out := &Output{
				Path:         filepath.Join(flatDir.p, file.Name),
				Digest:       digest.ToKey(file.Digest),
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
			digest := digest.ToKey(subdir.Digest)
			name := filepath.Join(flatDir.p, subdir.Name)
			queue = append(queue, &queueElem{d: digest, p: name})
		}
	}
	return flatFiles, nil
}
