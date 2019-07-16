package tree

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/chunker"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/command"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/filemetadata"
	"github.com/golang/protobuf/proto"
	"github.com/google/go-cmp/cmp"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

func mustMarshal(t *testing.T, p proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(p)
	if err != nil {
		t.Fatalf("error marshalling proto during test setup: %s", err)
	}
	return b
}

type inputPath struct {
	path          string
	fileContents  []byte
	isSymlink     bool
	isAbsolute    bool
	symlinkTarget string
}

func construct(dir string, ips []*inputPath) error {
	for _, ip := range ips {
		path := filepath.Join(dir, ip.path)
		if ip.isSymlink {
			target := ip.symlinkTarget
			if ip.isAbsolute {
				target = filepath.Join(dir, target)
			}
			if err := os.Symlink(target, path); err != nil {
				return err
			}
			continue
		}
		// Regular file.
		if err := os.MkdirAll(filepath.Dir(path), 0777); err != nil {
			return err
		}
		if err := ioutil.WriteFile(path, ip.fileContents, 0777); err != nil {
			return err
		}
	}
	return nil
}

func TestComputeMerkleTree(t *testing.T) {
	fooBlob, barBlob := []byte("foo"), []byte("bar")
	fooDg, barDg := digest.NewFromBlob(fooBlob), digest.NewFromBlob(barBlob)
	fooDgPb, barDgPb := fooDg.ToProto(), barDg.ToProto()

	fooDir := &repb.Directory{Files: []*repb.FileNode{{Name: "foo", Digest: fooDgPb, IsExecutable: true}}}
	barDir := &repb.Directory{Files: []*repb.FileNode{{Name: "bar", Digest: barDgPb, IsExecutable: true}}}
	foobarDir := &repb.Directory{Files: []*repb.FileNode{
		{Name: "bar", Digest: barDgPb, IsExecutable: true},
		{Name: "foo", Digest: fooDgPb, IsExecutable: true},
	}}

	fooDirBlob, barDirBlob, foobarDirBlob := mustMarshal(t, fooDir), mustMarshal(t, barDir), mustMarshal(t, foobarDir)
	fooDirDg, barDirDg, foobarDirDg := digest.NewFromBlob(fooDirBlob), digest.NewFromBlob(barDirBlob), digest.NewFromBlob(foobarDirBlob)
	fooDirDgPb, barDirDgPb := fooDirDg.ToProto(), barDirDg.ToProto()

	tests := []struct {
		desc  string
		input []*inputPath
		spec  *command.InputSpec
		// The expected results are calculated by marshalling rootDir, then expecting the result to be
		// the digest of rootDir plus a map containing rootDir's marshalled blob and all the additional
		// blobs.
		rootDir         *repb.Directory
		additionalBlobs [][]byte
	}{
		{
			desc:            "Empty directory",
			input:           nil,
			spec:            &command.InputSpec{},
			rootDir:         &repb.Directory{},
			additionalBlobs: nil,
		},
		{
			desc: "Files at root",
			input: []*inputPath{
				{path: "foo", fileContents: fooBlob},
				{path: "bar", fileContents: barBlob},
			},
			spec: &command.InputSpec{
				Inputs: []string{"foo", "bar"},
			},
			rootDir:         foobarDir,
			additionalBlobs: [][]byte{fooBlob, barBlob},
		},
		{
			desc: "File below root",
			input: []*inputPath{
				{path: "fooDir/foo", fileContents: fooBlob},
				{path: "barDir/bar", fileContents: barBlob},
			},
			spec: &command.InputSpec{
				Inputs: []string{"fooDir", "barDir"},
			},
			rootDir: &repb.Directory{Directories: []*repb.DirectoryNode{
				{Name: "barDir", Digest: barDirDgPb},
				{Name: "fooDir", Digest: fooDirDgPb},
			}},
			additionalBlobs: [][]byte{fooBlob, barBlob, fooDirBlob, barDirBlob},
		},
		{
			desc: "File absolute symlink",
			input: []*inputPath{
				{path: "fooDir/foo", fileContents: fooBlob},
				{path: "bar", isSymlink: true, isAbsolute: true, symlinkTarget: "fooDir/foo"},
			},
			spec: &command.InputSpec{
				Inputs: []string{"fooDir", "bar"},
			},
			rootDir: &repb.Directory{
				Directories: []*repb.DirectoryNode{{Name: "fooDir", Digest: fooDirDgPb}},
				Files:       []*repb.FileNode{{Name: "bar", Digest: fooDgPb, IsExecutable: true}},
			},
			additionalBlobs: [][]byte{fooBlob, fooDirBlob},
		},
		{
			desc: "File relative symlink",
			input: []*inputPath{
				{path: "fooDir/foo", fileContents: fooBlob},
				{path: "bar", isSymlink: true, symlinkTarget: "fooDir/foo"},
			},
			spec: &command.InputSpec{
				Inputs: []string{"fooDir", "bar"},
			},
			rootDir: &repb.Directory{
				Directories: []*repb.DirectoryNode{{Name: "fooDir", Digest: fooDirDgPb}},
				Files:       []*repb.FileNode{{Name: "bar", Digest: fooDgPb, IsExecutable: true}},
			},
			additionalBlobs: [][]byte{fooBlob, fooDirBlob},
		},
		{
			desc: "Directory absolute symlink",
			input: []*inputPath{
				{path: "fooDir/foo", fileContents: fooBlob},
				{path: "barDirTarget/bar", fileContents: barBlob},
				{path: "barDir", isSymlink: true, isAbsolute: true, symlinkTarget: "barDirTarget"},
			},
			spec: &command.InputSpec{
				Inputs: []string{"fooDir", "barDir"},
			},
			rootDir: &repb.Directory{Directories: []*repb.DirectoryNode{
				{Name: "barDir", Digest: barDirDgPb},
				{Name: "fooDir", Digest: fooDirDgPb},
			}},
			additionalBlobs: [][]byte{fooBlob, barBlob, fooDirBlob, barDirBlob},
		},
		{
			desc: "Directory relative symlink",
			input: []*inputPath{
				{path: "fooDir/foo", fileContents: fooBlob},
				{path: "barDirTarget/bar", fileContents: barBlob},
				{path: "barDir", isSymlink: true, symlinkTarget: "barDirTarget"},
			},
			spec: &command.InputSpec{
				Inputs: []string{"fooDir", "barDir"},
			},
			rootDir: &repb.Directory{Directories: []*repb.DirectoryNode{
				{Name: "barDir", Digest: barDirDgPb},
				{Name: "fooDir", Digest: fooDirDgPb},
			}},
			additionalBlobs: [][]byte{fooBlob, barBlob, fooDirBlob, barDirBlob},
		},
		{
			desc: "De-duplicating files",
			input: []*inputPath{
				{path: "fooDir/foo", fileContents: fooBlob},
				{path: "foobarDir/foo", fileContents: fooBlob},
				{path: "foobarDir/bar", fileContents: barBlob},
			},
			spec: &command.InputSpec{
				Inputs: []string{"fooDir", "foobarDir"},
			},
			rootDir: &repb.Directory{Directories: []*repb.DirectoryNode{
				{Name: "fooDir", Digest: fooDirDgPb},
				{Name: "foobarDir", Digest: foobarDirDg.ToProto()},
			}},
			additionalBlobs: [][]byte{fooBlob, barBlob, fooDirBlob, foobarDirBlob},
		},
		{
			desc: "De-duplicating directories",
			input: []*inputPath{
				{path: "fooDir1/foo", fileContents: fooBlob},
				{path: "fooDir2/foo", fileContents: fooBlob},
			},
			spec: &command.InputSpec{
				Inputs: []string{"fooDir1", "fooDir2"},
			},
			rootDir: &repb.Directory{Directories: []*repb.DirectoryNode{
				{Name: "fooDir1", Digest: fooDirDgPb},
				{Name: "fooDir2", Digest: fooDirDgPb},
			}},
			additionalBlobs: [][]byte{fooBlob, fooDirBlob},
		},
		{
			desc: "De-duplicating files with directories",
			input: []*inputPath{
				{path: "fooDirBlob", fileContents: fooDirBlob},
				{path: "fooDir/foo", fileContents: fooBlob},
			},
			spec: &command.InputSpec{
				Inputs: []string{"fooDirBlob", "fooDir"},
			},
			rootDir: &repb.Directory{
				Directories: []*repb.DirectoryNode{{Name: "fooDir", Digest: fooDirDgPb}},
				Files:       []*repb.FileNode{{Name: "fooDirBlob", Digest: fooDirDgPb, IsExecutable: true}},
			},
			additionalBlobs: [][]byte{fooBlob, fooDirBlob},
		},
		{
			desc: "File exclusions",
			input: []*inputPath{
				{path: "fooDir/foo", fileContents: fooBlob},
				{path: "fooDir/foo.txt", fileContents: fooBlob},
				{path: "barDir/bar", fileContents: barBlob},
				{path: "barDir/bar.txt", fileContents: barBlob},
			},
			spec: &command.InputSpec{
				Inputs: []string{"fooDir", "barDir"},
				InputExclusions: []*command.InputExclusion{
					&command.InputExclusion{Regex: `txt$`, Type: command.FileInputType},
				},
			},
			rootDir: &repb.Directory{Directories: []*repb.DirectoryNode{
				{Name: "barDir", Digest: barDirDgPb},
				{Name: "fooDir", Digest: fooDirDgPb},
			}},
			additionalBlobs: [][]byte{fooBlob, barBlob, fooDirBlob, barDirBlob},
		},
		{
			desc: "Directory exclusions",
			input: []*inputPath{
				{path: "foo", fileContents: fooBlob},
				{path: "fooDir/foo", fileContents: fooBlob},
				{path: "barDir/bar", fileContents: barBlob},
			},
			spec: &command.InputSpec{
				Inputs: []string{"foo", "fooDir", "barDir"},
				InputExclusions: []*command.InputExclusion{
					&command.InputExclusion{Regex: `foo`, Type: command.DirectoryInputType},
				},
			},
			rootDir: &repb.Directory{
				Directories: []*repb.DirectoryNode{{Name: "barDir", Digest: barDirDgPb}},
				Files:       []*repb.FileNode{{Name: "foo", Digest: fooDgPb, IsExecutable: true}},
			},
			additionalBlobs: [][]byte{fooBlob, barBlob, barDirBlob},
		},
		{
			desc: "All type exclusions",
			input: []*inputPath{
				{path: "foo", fileContents: fooBlob},
				{path: "fooDir/foo", fileContents: fooBlob},
				{path: "barDir/bar", fileContents: barBlob},
			},
			spec: &command.InputSpec{
				Inputs: []string{"foo", "fooDir", "barDir"},
				InputExclusions: []*command.InputExclusion{
					&command.InputExclusion{Regex: `foo`, Type: command.UnspecifiedInputType},
				},
			},
			rootDir: &repb.Directory{
				Directories: []*repb.DirectoryNode{{Name: "barDir", Digest: barDirDgPb}},
			},
			additionalBlobs: [][]byte{barBlob, barDirBlob},
		},
		{
			desc: "Virtual inputs",
			spec: &command.InputSpec{
				VirtualInputs: []*command.VirtualInput{
					&command.VirtualInput{Path: "fooDir/foo", Contents: fooBlob},
					&command.VirtualInput{Path: "barDir/bar", Contents: barBlob},
				},
			},
			rootDir: &repb.Directory{Directories: []*repb.DirectoryNode{
				{Name: "barDir", Digest: barDirDgPb},
				{Name: "fooDir", Digest: fooDirDgPb},
			}},
			additionalBlobs: [][]byte{fooBlob, barBlob, fooDirBlob, barDirBlob},
		},
		{
			// NOTE: The use of maps in our implementation means that the traversal order is unstable. The
			// outputs are required to be in lexicographic order, so if ComputeMerkleTree is not sorting
			// correctly, this test will fail (except in the rare occasion the traversal order is
			// coincidentally correct).
			desc: "Correct sorting",
			input: []*inputPath{
				{path: "a", fileContents: fooBlob},
				{path: "b", fileContents: fooBlob},
				{path: "c", fileContents: fooBlob},
				{path: "d", fileContents: fooBlob},
				{path: "e", fileContents: fooBlob},
				{path: "f", fileContents: fooBlob},
				{path: "g/foo", fileContents: fooBlob},
				{path: "h/foo", fileContents: fooBlob},
				{path: "i/foo", fileContents: fooBlob},
				{path: "j/foo", fileContents: fooBlob},
				{path: "k/foo", fileContents: fooBlob},
				{path: "l/foo", fileContents: fooBlob},
			},
			spec: &command.InputSpec{
				Inputs: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"},
			},
			rootDir: &repb.Directory{
				Files: []*repb.FileNode{
					{Name: "a", Digest: fooDgPb, IsExecutable: true},
					{Name: "b", Digest: fooDgPb, IsExecutable: true},
					{Name: "c", Digest: fooDgPb, IsExecutable: true},
					{Name: "d", Digest: fooDgPb, IsExecutable: true},
					{Name: "e", Digest: fooDgPb, IsExecutable: true},
					{Name: "f", Digest: fooDgPb, IsExecutable: true},
				},
				Directories: []*repb.DirectoryNode{
					{Name: "g", Digest: fooDirDgPb},
					{Name: "h", Digest: fooDirDgPb},
					{Name: "i", Digest: fooDirDgPb},
					{Name: "j", Digest: fooDirDgPb},
					{Name: "k", Digest: fooDirDgPb},
					{Name: "l", Digest: fooDirDgPb},
				},
			},
			additionalBlobs: [][]byte{fooBlob, fooDirBlob},
		},
	}

	for _, tc := range tests {
		root, err := ioutil.TempDir("", tc.desc)
		if err != nil {
			t.Fatalf("failed to make temp dir: %v", err)
		}
		defer os.RemoveAll(root)
		if err := construct(root, tc.input); err != nil {
			t.Fatalf("failed to construct input dir structure: %v", err)
		}

		t.Run(tc.desc, func(t *testing.T) {
			wantBlobs := make(map[digest.Digest][]byte)
			rootBlob := mustMarshal(t, tc.rootDir)
			rootDg := digest.NewFromBlob(rootBlob)
			wantBlobs[rootDg] = rootBlob
			for _, b := range tc.additionalBlobs {
				wantBlobs[digest.NewFromBlob(b)] = b
			}

			gotBlobs := make(map[digest.Digest][]byte)
			gotRootDg, inputs, err := ComputeMerkleTree(root, tc.spec, chunker.DefaultChunkSize, &filemetadata.NoopFileMetadataCache{})
			if err != nil {
				t.Errorf("ComputeMerkleTree(...) = gave error %v, want success", err)
			}
			for _, ch := range inputs {
				blob, err := ch.FullData()
				if err != nil {
					t.Errorf("chunker %v FullData() returned error %v", ch, err)
				}
				gotBlobs[ch.Digest()] = blob
			}
			if diff := cmp.Diff(rootDg, gotRootDg); diff != "" {
				t.Errorf("ComputeMerkleTree(...) gave diff (want -> got) on root:\n%s", diff)
				if gotRootBlob, ok := gotBlobs[gotRootDg]; ok {
					gotRoot := new(repb.Directory)
					if err := proto.Unmarshal(gotRootBlob, gotRoot); err != nil {
						t.Errorf("  When unpacking root blob, got error: %s", err)
					} else {
						diff := cmp.Diff(tc.rootDir, gotRoot)
						t.Errorf("  Diff between unpacked roots (want -> got):\n%s", diff)
					}
				} else {
					t.Errorf("  Root digest gotten not present in blobs map")
				}
			}
			if diff := cmp.Diff(wantBlobs, gotBlobs); diff != "" {
				t.Errorf("ComputeMerkleTree(...) gave diff (want -> got) on blobs:\n%s", diff)
			}
		})
	}
}

func TestComputeMerkleTreeErrors(t *testing.T) {
	tests := []struct {
		desc  string
		input []*inputPath
		spec  *command.InputSpec
	}{
		{
			desc: "empty input",
			spec: &command.InputSpec{Inputs: []string{""}},
		},
		{
			desc: "empty virtual input",
			spec: &command.InputSpec{
				VirtualInputs: []*command.VirtualInput{
					&command.VirtualInput{Path: "", Contents: []byte("foo")},
				},
			},
		},
		{
			desc: "missing input",
			spec: &command.InputSpec{Inputs: []string{"foo"}},
		},
		{
			desc: "missing nested input",
			input: []*inputPath{
				{path: "a", fileContents: []byte("a")},
				{path: "dir/a", fileContents: []byte("a")},
			},
			spec: &command.InputSpec{Inputs: []string{"a", "dir", "dir/b"}},
		},
	}

	for _, tc := range tests {
		root, err := ioutil.TempDir("", tc.desc)
		if err != nil {
			t.Fatalf("failed to make temp dir: %v", err)
		}
		defer os.RemoveAll(root)
		if err := construct(root, tc.input); err != nil {
			t.Fatalf("failed to construct input dir structure: %v", err)
		}
		t.Run(tc.desc, func(t *testing.T) {
			if _, _, err := ComputeMerkleTree(root, tc.spec, chunker.DefaultChunkSize, &filemetadata.NoopFileMetadataCache{}); err == nil {
				t.Errorf("ComputeMerkleTree(%v) succeeded, want error", tc.spec)
			}
		})
	}
}

type stubFileMetadataCache struct {
	Digest digest.Digest
	Err    error
}

// Get implements the FileMetadataCache interface.
func (s *stubFileMetadataCache) Get(_ string) (*filemetadata.Metadata, error) {
	if s.Err != nil {
		return nil, s.Err
	}
	return &filemetadata.Metadata{Digest: s.Digest}, nil
}

func TestComputeMerkleTreeUseCache(t *testing.T) {
	fooBlob := []byte("foo")
	fooDg := digest.NewFromBlob(fooBlob)
	fooDgPb := fooDg.ToProto()
	fooDir := &repb.Directory{Files: []*repb.FileNode{{Name: "foo", Digest: fooDgPb, IsExecutable: true}}}

	cache := &stubFileMetadataCache{Digest: fooDg}
	spec := &command.InputSpec{Inputs: []string{"foo"}}
	gotRootDg, _, err := ComputeMerkleTree("", spec, chunker.DefaultChunkSize, cache)
	if err != nil {
		t.Errorf("ComputeMerkleTree(%v) gave unexpected error %v, want nil", spec, err)
	}
	rootDg := digest.NewFromBlob(mustMarshal(t, fooDir))
	if rootDg != gotRootDg {
		t.Errorf("ComputeMerkleTree(...) gave root digest diff, want %v, got %v", rootDg, gotRootDg)
	}
}

func TestComputeMerkleTreeUseCacheError(t *testing.T) {
	err := errors.New("some unknown error")
	cache := &stubFileMetadataCache{Err: err}
	spec := &command.InputSpec{Inputs: []string{"foo"}}
	_, _, e := ComputeMerkleTree("", spec, chunker.DefaultChunkSize, cache)
	if e != err {
		t.Errorf("ComputeMerkleTree(%v) gave unexpected error %v, want %v", spec, e, err)
	}
}

func TestFlattenTreeRepeated(t *testing.T) {
	// Directory structure:
	// <root>
	//  +-baz -> digest 1003/10 (rw)
	//  +-a
	//    + b
	//      + c    ## empty subdir
	//      +-foo -> digest 1001/1 (rw)
	//      +-bar -> digest 1002/2 (rwx)
	//  + b
	//    + c    ## empty subdir
	//    +-foo -> digest 1001/1 (rw)
	//    +-bar -> digest 1002/2 (rwx)
	//  + c    ## empty subdir
	fooDigest := digest.TestNew("1001", 1)
	barDigest := digest.TestNew("1002", 2)
	bazDigest := digest.TestNew("1003", 10)
	dirC := &repb.Directory{}
	cDigest := digest.TestNewFromMessage(dirC)
	dirB := &repb.Directory{
		Files: []*repb.FileNode{
			{Name: "foo", Digest: fooDigest.ToProto(), IsExecutable: false},
			{Name: "bar", Digest: barDigest.ToProto(), IsExecutable: true},
		},
		Directories: []*repb.DirectoryNode{
			{Name: "c", Digest: cDigest.ToProto()},
		},
	}
	bDigest := digest.TestNewFromMessage(dirB)
	dirA := &repb.Directory{
		Directories: []*repb.DirectoryNode{
			{Name: "b", Digest: bDigest.ToProto()},
		}}
	aDigest := digest.TestNewFromMessage(dirA)
	root := &repb.Directory{
		Directories: []*repb.DirectoryNode{
			{Name: "a", Digest: aDigest.ToProto()},
			{Name: "b", Digest: bDigest.ToProto()},
			{Name: "c", Digest: cDigest.ToProto()},
		},
		Files: []*repb.FileNode{
			{Name: "baz", Digest: bazDigest.ToProto()},
		},
	}
	tree := &repb.Tree{
		Root:     root,
		Children: []*repb.Directory{dirA, dirB, dirC},
	}
	outputs, err := FlattenTree(tree, "x")
	if err != nil {
		t.Errorf("FlattenTree gave error %v", err)
	}
	wantOutputs := map[string]*Output{
		"x/baz":     &Output{Digest: bazDigest},
		"x/a/b/foo": &Output{Digest: fooDigest},
		"x/a/b/bar": &Output{Digest: barDigest, IsExecutable: true},
		"x/b/foo":   &Output{Digest: fooDigest},
		"x/b/bar":   &Output{Digest: barDigest, IsExecutable: true},
	}
	if len(outputs) != len(wantOutputs) {
		t.Errorf("FlattenTree gave wrong number of outputs: want %d, got %d", len(wantOutputs), len(outputs))
	}
	for path, wantOut := range wantOutputs {
		got, ok := outputs[path]
		if !ok {
			t.Errorf("expected output %s is missing", path)
		}
		if got.Path != path {
			t.Errorf("FlattenTree keyed %s output with %s path", got.Path, path)
		}
		if wantOut.Digest != got.Digest {
			t.Errorf("FlattenTree gave digest diff on %s: want %v, got: %v", path, wantOut.Digest, got.Digest)
		}
		if wantOut.IsExecutable != got.IsExecutable {
			t.Errorf("FlattenTree gave IsExecutable diff on %s: want %v, got: %v", path, wantOut.IsExecutable, got.IsExecutable)
		}
	}
}