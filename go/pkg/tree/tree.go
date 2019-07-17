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

	"github.com/bazelbuild/remote-apis-sdks/go/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/chunker"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/command"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/filemetadata"
	"github.com/golang/protobuf/proto"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

// FileMetadataCache is a cache for file contents->Metadata.
type FileMetadataCache interface {
	Get(path string) (*filemetadata.Metadata, error)
}

// treeNode represents a file tree, which is an intermediate representation used to encode a Merkle
// tree later. It corresponds roughly to a *repb.Directory, but with pointers, not digests, used to
// refer to other nodes.
type treeNode struct {
	Files map[string]*chunker.Chunker
	Dirs  map[string]*treeNode
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
func loadFiles(execRoot string, excl []*command.InputExclusion, path string, fs map[string]*chunker.Chunker, chunkSize int, cache FileMetadataCache) error {
	absPath := filepath.Clean(filepath.Join(execRoot, path))
	meta, err := cache.Get(absPath)
	t := command.FileInputType
	if err != nil {
		if e, ok := err.(*filemetadata.FileError); !ok || !e.IsDirectory {
			return err
		}
		t = command.DirectoryInputType
	}
	if shouldIgnore(absPath, t, excl) {
		return nil
	}
	if t == command.FileInputType {
		fs[path] = chunker.NewFromFile(absPath, meta.Digest, chunkSize)
		return nil
	}
	// Directory
	files, err := ioutil.ReadDir(absPath)
	if err != nil {
		return err
	}

	for _, f := range files {
		if e := loadFiles(execRoot, excl, filepath.Join(path, f.Name()), fs, chunkSize, cache); e != nil {
			return e
		}
	}
	return nil
}

// ComputeMerkleTree packages an InputSpec into uploadable inputs, returned as Chunkers.
func ComputeMerkleTree(execRoot string, is *command.InputSpec, chunkSize int, cache FileMetadataCache) (root digest.Digest, inputs []*chunker.Chunker, err error) {
	fs := make(map[string]*chunker.Chunker)
	for _, i := range is.Inputs {
		if i == "" {
			return digest.Empty, nil, errors.New("empty Input, use \".\" for entire exec root")
		}
		if e := loadFiles(execRoot, is.InputExclusions, i, fs, chunkSize, cache); e != nil {
			return digest.Empty, nil, e
		}
	}
	for _, i := range is.VirtualInputs {
		if i.Path == "" {
			return digest.Empty, nil, errors.New("empty Path in VirtualInputs")
		}
		fs[i.Path] = chunker.NewFromBlob(i.Contents, chunkSize)
	}
	ft := buildTree(fs)
	var blobs map[digest.Digest]*chunker.Chunker
	root, blobs, err = packageTree(ft, chunkSize)
	if err != nil {
		return digest.Empty, nil, err
	}
	for _, ch := range blobs {
		inputs = append(inputs, ch)
	}
	return root, inputs, nil
}

func buildTree(files map[string]*chunker.Chunker) *treeNode {
	root := &treeNode{}
	for name, ch := range files {
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

		if node.Files == nil {
			node.Files = make(map[string]*chunker.Chunker)
		}
		node.Files[base] = ch
	}
	return root
}

func packageTree(t *treeNode, chunkSize int) (root digest.Digest, blobs map[digest.Digest]*chunker.Chunker, err error) {
	dir := &repb.Directory{}
	blobs = make(map[digest.Digest]*chunker.Chunker)

	for name, child := range t.Dirs {
		dg, childBlobs, err := packageTree(child, chunkSize)
		if err != nil {
			return digest.Empty, nil, err
		}
		dir.Directories = append(dir.Directories, &repb.DirectoryNode{Name: name, Digest: dg.ToProto()})
		for d, b := range childBlobs {
			blobs[d] = b
		}
	}
	sort.Slice(dir.Directories, func(i, j int) bool { return dir.Directories[i].Name < dir.Directories[j].Name })

	for name, ch := range t.Files {
		dg := ch.Digest()
		dir.Files = append(dir.Files, &repb.FileNode{Name: name, Digest: dg.ToProto(), IsExecutable: true})
		blobs[dg] = ch
	}
	sort.Slice(dir.Files, func(i, j int) bool { return dir.Files[i].Name < dir.Files[j].Name })

	encDir, err := proto.Marshal(dir)
	if err != nil {
		return digest.Empty, nil, err
	}
	ch := chunker.NewFromBlob(encDir, chunkSize)
	dg := ch.Digest()
	blobs[dg] = ch
	return dg, blobs, nil
}

// Output represents a leaf output node in a nested directory structure (either a file or a
// symlink).
type Output struct {
	Digest        digest.Digest
	Path          string
	IsExecutable  bool
	SymlinkTarget string
}

// FlattenTree takes a Tree message and calculates the relative paths of all the files to
// the tree root. Note that only files are included in the returned slice, not the intermediate
// directories. Empty directories will be skipped, and directories containing only other directories
// will be omitted as well.
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
