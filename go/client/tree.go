package client

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/bazelbuild/remote-apis-sdks/go/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/command"
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
func loadFiles(execRoot string, is *command.InputSpec, path string, fs map[string][]byte) error {
	absPath := filepath.Join(execRoot, path)
	fi, err := os.Stat(absPath)
	if err != nil {
		return err
	}
	var t command.InputType
	switch mode := fi.Mode(); {
	case mode.IsDir():
		t = command.DirectoryInputType
	case mode.IsRegular():
		t = command.FileInputType
	default:
		return fmt.Errorf("unsupported input type: %s, %v", absPath, mode)
	}

	if shouldIgnore(absPath, t, is.InputExclusions) {
		return nil
	}
	if t == command.FileInputType {
		b, err := ioutil.ReadFile(absPath)
		if err != nil {
			return err
		}
		fs[path] = b
		return nil
	}
	// Directory
	files, err := ioutil.ReadDir(absPath)
	if err != nil {
		return err
	}

	for _, f := range files {
		if e := loadFiles(execRoot, is, filepath.Join(path, f.Name()), fs); e != nil {
			return e
		}
	}
	return nil
}

// BuildTreeFromInputs builds a FileTree out of a command.InputSpec.
// TODO(olaola): there are a few problems with this function. The biggest is the memory usage: it
// loads all file contents into memory (but this applies to the entire current rc.Client interface!)
// In addition:
// * It does not use a file digest cache.
// * It ignores empty directory trees, only packaging/uploading files and their parents.
// * It does not ignore missing inputs (we might want it as a temporary hack!)
// * It handles input symlinks by creating copies instead of using RE API symlinks.
func BuildTreeFromInputs(execRoot string, is *command.InputSpec) (*FileTree, error) {
	fs := make(map[string][]byte)
	for _, i := range is.Inputs {
		if e := loadFiles(execRoot, is, i, fs); e != nil {
			return nil, e
		}
	}
	return BuildTree(fs), nil
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
func PackageTree(t *FileTree) (root digest.Digest, blobs map[digest.Digest][]byte, err error) {
	if t == nil {
		return digest.Empty, nil, errors.New("nil FileTree while packaging tree")
	}
	dir := &repb.Directory{}
	blobs = make(map[digest.Digest][]byte)

	for name, child := range t.Dirs {
		if name == "" {
			return digest.Empty, nil, errors.New("empty directory name while packaging tree")
		}
		dg, childBlobs, err := PackageTree(child)
		if err != nil {
			return digest.Empty, nil, err
		}
		dir.Directories = append(dir.Directories, &repb.DirectoryNode{Name: name, Digest: dg.ToProto()})
		for d, b := range childBlobs {
			blobs[d] = b
		}
	}
	sort.Slice(dir.Directories, func(i, j int) bool { return dir.Directories[i].Name < dir.Directories[j].Name })

	for name, cont := range t.Files {
		if name == "" {
			return digest.Empty, nil, errors.New("empty file name while packaging tree")
		}
		if _, ok := t.Dirs[name]; ok {
			return digest.Empty, nil, errors.New("directory and file with the same name while packaging tree")
		}
		dg := digest.NewFromBlob(cont)
		dir.Files = append(dir.Files, &repb.FileNode{Name: name, Digest: dg.ToProto(), IsExecutable: true})
		blobs[dg] = cont
	}
	sort.Slice(dir.Files, func(i, j int) bool { return dir.Files[i].Name < dir.Files[j].Name })

	encDir, err := proto.Marshal(dir)
	if err != nil {
		return digest.Empty, nil, err
	}
	dg := digest.NewFromBlob(encDir)
	blobs[dg] = encDir
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
