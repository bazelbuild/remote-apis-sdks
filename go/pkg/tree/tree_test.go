package tree

import (
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
	"github.com/google/go-cmp/cmp/cmpopts"

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

type callCountingMetadataCache struct {
	calls    map[string]int
	cache    *filemetadata.NoopFileMetadataCache
	execRoot string
	t        *testing.T
}

func newCallCountingMetadataCache(execRoot string, t *testing.T) *callCountingMetadataCache {
	return &callCountingMetadataCache{
		calls:    make(map[string]int),
		cache:    &filemetadata.NoopFileMetadataCache{},
		execRoot: execRoot,
		t:        t,
	}
}

func (c *callCountingMetadataCache) Get(path string) (*filemetadata.Metadata, error) {
	c.t.Helper()
	p, err := filepath.Rel(c.execRoot, path)
	if err != nil {
		c.t.Errorf("expected %v to be under %v", path, c.execRoot)
	}
	c.calls[p]++
	return c.cache.Get(path)
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
		wantCacheCalls  map[string]int
		// The expected wantStats.TotalInputBytes is calculated by adding the marshalled rootDir size.
		wantStats *Stats
	}{
		{
			desc:            "Empty directory",
			input:           nil,
			spec:            &command.InputSpec{},
			rootDir:         &repb.Directory{},
			additionalBlobs: nil,
			wantStats: &Stats{
				InputDirectories: 1,
			},
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
			wantCacheCalls: map[string]int{
				"foo": 1,
				"bar": 1,
			},
			wantStats: &Stats{
				InputDirectories: 1,
				InputFiles:       2,
				TotalInputBytes:  fooDg.Size + barDg.Size,
			},
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
			wantCacheCalls: map[string]int{
				"fooDir":     1,
				"fooDir/foo": 1,
				"barDir":     1,
				"barDir/bar": 1,
			},
			wantStats: &Stats{
				InputDirectories: 3,
				InputFiles:       2,
				TotalInputBytes:  fooDg.Size + barDg.Size + fooDirDg.Size + barDirDg.Size,
			},
		},
		{
			desc: "Normalizing input paths",
			input: []*inputPath{
				{path: "fooDir/foo", fileContents: fooBlob},
				{path: "fooDir/otherDir/foo", fileContents: fooBlob},
				{path: "barDir/bar", fileContents: barBlob},
			},
			spec: &command.InputSpec{
				Inputs: []string{"fooDir/../fooDir/foo", "barDir//bar"},
			},
			rootDir: &repb.Directory{Directories: []*repb.DirectoryNode{
				{Name: "barDir", Digest: barDirDgPb},
				{Name: "fooDir", Digest: fooDirDgPb},
			}},
			additionalBlobs: [][]byte{fooBlob, barBlob, fooDirBlob, barDirBlob},
			wantCacheCalls: map[string]int{
				"fooDir/foo": 1,
				"barDir/bar": 1,
			},
			wantStats: &Stats{
				InputDirectories: 3,
				InputFiles:       2,
				TotalInputBytes:  fooDg.Size + barDg.Size + fooDirDg.Size + barDirDg.Size,
			},
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
			wantCacheCalls: map[string]int{
				"fooDir":     1,
				"fooDir/foo": 1,
				"bar":        1,
			},
			wantStats: &Stats{
				InputDirectories: 2,
				InputFiles:       2,
				TotalInputBytes:  2*fooDg.Size + fooDirDg.Size,
			},
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
			wantCacheCalls: map[string]int{
				"fooDir":     1,
				"fooDir/foo": 1,
				"bar":        1,
			},
			wantStats: &Stats{
				InputDirectories: 2,
				InputFiles:       2,
				TotalInputBytes:  2*fooDg.Size + fooDirDg.Size,
			},
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
			wantCacheCalls: map[string]int{
				"fooDir":     1,
				"fooDir/foo": 1,
				"barDir":     1,
				"barDir/bar": 1,
			},
			wantStats: &Stats{
				InputDirectories: 3,
				InputFiles:       2,
				TotalInputBytes:  fooDg.Size + fooDirDg.Size + barDg.Size + barDirDg.Size,
			},
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
			wantCacheCalls: map[string]int{
				"fooDir":     1,
				"fooDir/foo": 1,
				"barDir":     1,
				"barDir/bar": 1,
			},
			wantStats: &Stats{
				InputDirectories: 3,
				InputFiles:       2,
				TotalInputBytes:  fooDg.Size + fooDirDg.Size + barDg.Size + barDirDg.Size,
			},
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
			wantCacheCalls: map[string]int{
				"fooDir":        1,
				"fooDir/foo":    1,
				"foobarDir":     1,
				"foobarDir/foo": 1,
				"foobarDir/bar": 1,
			},
			wantStats: &Stats{
				InputDirectories: 3,
				InputFiles:       3,
				TotalInputBytes:  2*fooDg.Size + fooDirDg.Size + barDg.Size + foobarDirDg.Size,
			},
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
			wantCacheCalls: map[string]int{
				"fooDir1":     1,
				"fooDir1/foo": 1,
				"fooDir2":     1,
				"fooDir2/foo": 1,
			},
			wantStats: &Stats{
				InputDirectories: 3,
				InputFiles:       2,
				TotalInputBytes:  2*fooDg.Size + 2*fooDirDg.Size,
			},
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
			wantCacheCalls: map[string]int{
				"fooDirBlob": 1,
				"fooDir":     1,
				"fooDir/foo": 1,
			},
			wantStats: &Stats{
				InputDirectories: 2,
				InputFiles:       2,
				TotalInputBytes:  fooDg.Size + 2*fooDirDg.Size,
			},
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
			wantCacheCalls: map[string]int{
				"fooDir":         1,
				"fooDir/foo":     1,
				"fooDir/foo.txt": 1,
				"barDir":         1,
				"barDir/bar":     1,
				"barDir/bar.txt": 1,
			},
			wantStats: &Stats{
				InputDirectories: 3,
				InputFiles:       2,
				TotalInputBytes:  fooDg.Size + fooDirDg.Size + barDg.Size + barDirDg.Size,
			},
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
			wantCacheCalls: map[string]int{
				"foo":        1,
				"fooDir":     1,
				"barDir":     1,
				"barDir/bar": 1,
			},
			wantStats: &Stats{
				InputDirectories: 2,
				InputFiles:       2,
				TotalInputBytes:  fooDg.Size + barDg.Size + barDirDg.Size,
			},
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
			wantCacheCalls: map[string]int{
				"foo":        1,
				"fooDir":     1,
				"barDir":     1,
				"barDir/bar": 1,
			},
			wantStats: &Stats{
				InputDirectories: 2,
				InputFiles:       1,
				TotalInputBytes:  barDg.Size + barDirDg.Size,
			},
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
			wantStats: &Stats{
				InputDirectories: 3,
				InputFiles:       2,
				TotalInputBytes:  fooDg.Size + fooDirDg.Size + barDg.Size + barDirDg.Size,
			},
		},
		{
			desc: "Normalizing virtual inputs paths",
			spec: &command.InputSpec{
				VirtualInputs: []*command.VirtualInput{
					&command.VirtualInput{Path: "fooDir/../fooDir/foo", Contents: fooBlob},
					&command.VirtualInput{Path: "barDir///bar", Contents: barBlob},
				},
			},
			rootDir: &repb.Directory{Directories: []*repb.DirectoryNode{
				{Name: "barDir", Digest: barDirDgPb},
				{Name: "fooDir", Digest: fooDirDgPb},
			}},
			additionalBlobs: [][]byte{fooBlob, barBlob, fooDirBlob, barDirBlob},
			wantStats: &Stats{
				InputDirectories: 3,
				InputFiles:       2,
				TotalInputBytes:  fooDg.Size + fooDirDg.Size + barDg.Size + barDirDg.Size,
			},
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
			wantCacheCalls: map[string]int{
				"a":     1,
				"b":     1,
				"c":     1,
				"d":     1,
				"e":     1,
				"f":     1,
				"g":     1,
				"h":     1,
				"i":     1,
				"j":     1,
				"k":     1,
				"l":     1,
				"g/foo": 1,
				"h/foo": 1,
				"i/foo": 1,
				"j/foo": 1,
				"k/foo": 1,
				"l/foo": 1,
			},
			wantStats: &Stats{
				InputDirectories: 7,
				InputFiles:       12,
				TotalInputBytes:  12*fooDg.Size + 6*fooDirDg.Size,
			},
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
			tc.wantStats.TotalInputBytes += int64(len(rootBlob))
			for _, b := range tc.additionalBlobs {
				wantBlobs[digest.NewFromBlob(b)] = b
			}

			gotBlobs := make(map[digest.Digest][]byte)
			cache := newCallCountingMetadataCache(root, t)
			gotRootDg, inputs, stats, err := ComputeMerkleTree(root, tc.spec, chunker.DefaultChunkSize, cache)
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
			if diff := cmp.Diff(tc.wantCacheCalls, cache.calls, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("ComputeMerkleTree(...) gave diff on file metadata cache access (want -> got) on blobs:\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantStats, stats); diff != "" {
				t.Errorf("ComputeMerkleTree(...) gave diff on stats (want -> got) on blobs:\n%s", diff)
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
			if _, _, _, err := ComputeMerkleTree(root, tc.spec, chunker.DefaultChunkSize, &filemetadata.NoopFileMetadataCache{}); err == nil {
				t.Errorf("ComputeMerkleTree(%v) succeeded, want error", tc.spec)
			}
		})
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
