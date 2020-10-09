package tree

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/chunker"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/command"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/filemetadata"
	"github.com/golang/protobuf/proto"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

var (
	fooBlob, barBlob = []byte("foo"), []byte("bar")
	fooDg, barDg     = digest.NewFromBlob(fooBlob), digest.NewFromBlob(barBlob)
	fooDgPb, barDgPb = fooDg.ToProto(), barDg.ToProto()

	fooDir    = &repb.Directory{Files: []*repb.FileNode{{Name: "foo", Digest: fooDgPb, IsExecutable: true}}}
	barDir    = &repb.Directory{Files: []*repb.FileNode{{Name: "bar", Digest: barDgPb}}}
	foobarDir = &repb.Directory{Files: []*repb.FileNode{
		{Name: "bar", Digest: barDgPb},
		{Name: "foo", Digest: fooDgPb, IsExecutable: true},
	}}

	fooDirBlob, barDirBlob, foobarDirBlob = mustMarshal(fooDir), mustMarshal(barDir), mustMarshal(foobarDir)
	fooDirDg, barDirDg, foobarDirDg       = digest.NewFromBlob(fooDirBlob), digest.NewFromBlob(barDirBlob), digest.NewFromBlob(foobarDirBlob)
	fooDirDgPb, barDirDgPb                = fooDirDg.ToProto(), barDirDg.ToProto()
)

func mustMarshal(p proto.Message) []byte {
	b, err := proto.Marshal(p)
	if err != nil {
		panic("error marshalling proto during test setup: %s" + err.Error())
	}
	return b
}

type inputPath struct {
	path          string
	emptyDir      bool
	fileContents  []byte
	isExecutable  bool
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
		if ip.emptyDir {
			if err := os.MkdirAll(path, 0777); err != nil {
				return err
			}
			continue
		}
		// Regular file.
		if err := os.MkdirAll(filepath.Dir(path), 0777); err != nil {
			return err
		}
		perm := os.FileMode(0666)
		if ip.isExecutable {
			perm = os.FileMode(0777)
		}
		if err := ioutil.WriteFile(path, ip.fileContents, perm); err != nil {
			return err
		}
	}
	return nil
}

type callCountingMetadataCache struct {
	calls    map[string]int
	cache    filemetadata.Cache
	execRoot string
	t        *testing.T
}

func newCallCountingMetadataCache(execRoot string, t *testing.T) *callCountingMetadataCache {
	return &callCountingMetadataCache{
		calls:    make(map[string]int),
		cache:    filemetadata.NewNoopCache(),
		execRoot: execRoot,
		t:        t,
	}
}

func (c *callCountingMetadataCache) Get(path string) *filemetadata.Metadata {
	c.t.Helper()
	p, err := filepath.Rel(c.execRoot, path)
	if err != nil {
		c.t.Errorf("expected %v to be under %v", path, c.execRoot)
	}
	c.calls[p]++
	return c.cache.Get(path)
}

func (c *callCountingMetadataCache) Delete(path string) error {
	c.t.Helper()
	p, err := filepath.Rel(c.execRoot, path)
	if err != nil {
		c.t.Errorf("expected %v to be under %v", path, c.execRoot)
	}
	c.calls[p]++
	return c.cache.Delete(path)
}

func (c *callCountingMetadataCache) Update(path string, ce *filemetadata.Metadata) error {
	c.t.Helper()
	p, err := filepath.Rel(c.execRoot, path)
	if err != nil {
		c.t.Errorf("expected %v to be under %v", path, c.execRoot)
	}
	c.calls[p]++
	return c.cache.Update(path, ce)
}

func (c *callCountingMetadataCache) Reset() {
	c.t.Helper()
	c.cache.Reset()
}

func (c *callCountingMetadataCache) GetCacheHits() uint64 {
	return 0
}

func (c *callCountingMetadataCache) GetCacheMisses() uint64 {
	return 0
}

func TestComputeMerkleTreeEmptySubdirs(t *testing.T) {
	fileBlob := []byte("bla")
	fileDg := digest.NewFromBlob(fileBlob)
	fileDgPb := fileDg.ToProto()
	emptyDirDgPb := digest.Empty.ToProto()
	cDir := &repb.Directory{
		Files:       []*repb.FileNode{{Name: "file", Digest: fileDgPb}},
		Directories: []*repb.DirectoryNode{{Name: "empty", Digest: emptyDirDgPb}},
	}
	cDirBlob := mustMarshal(cDir)
	cDirDg := digest.NewFromBlob(cDirBlob)
	cDirDgPb := cDirDg.ToProto()
	bDir := &repb.Directory{
		Directories: []*repb.DirectoryNode{
			{Name: "c", Digest: cDirDgPb},
			{Name: "empty", Digest: emptyDirDgPb},
		},
	}
	bDirBlob := mustMarshal(bDir)
	bDirDg := digest.NewFromBlob(bDirBlob)
	bDirDgPb := bDirDg.ToProto()
	aDir := &repb.Directory{
		Directories: []*repb.DirectoryNode{
			{Name: "b", Digest: bDirDgPb},
			{Name: "empty", Digest: emptyDirDgPb},
		},
	}
	aDirBlob := mustMarshal(aDir)
	aDirDg := digest.NewFromBlob(aDirBlob)

	ips := []*inputPath{
		{path: "empty", emptyDir: true},
		{path: "b/empty", emptyDir: true},
		{path: "b/c/empty", emptyDir: true},
		{path: "b/c/file", fileContents: fileBlob},
	}
	root, err := ioutil.TempDir("", t.Name())
	if err != nil {
		t.Fatalf("failed to make temp dir: %v", err)
	}
	defer os.RemoveAll(root)
	if err := construct(root, ips); err != nil {
		t.Fatalf("failed to construct input dir structure: %v", err)
	}
	inputSpec := &command.InputSpec{Inputs: []string{"b", "empty"}}
	wantBlobs := map[digest.Digest][]byte{
		aDirDg:       aDirBlob,
		bDirDg:       bDirBlob,
		cDirDg:       cDirBlob,
		fileDg:       fileBlob,
		digest.Empty: []byte{},
	}

	gotBlobs := make(map[digest.Digest][]byte)
	cache := newCallCountingMetadataCache(root, t)
	gotRootDg, inputs, stats, err := ComputeMerkleTree(root, inputSpec, chunker.DefaultChunkSize, cache)
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
	if diff := cmp.Diff(aDirDg, gotRootDg); diff != "" {
		t.Errorf("ComputeMerkleTree(...) gave diff (-want +got) on root:\n%s", diff)
		if gotRootBlob, ok := gotBlobs[gotRootDg]; ok {
			gotRoot := new(repb.Directory)
			if err := proto.Unmarshal(gotRootBlob, gotRoot); err != nil {
				t.Errorf("  When unpacking root blob, got error: %s", err)
			} else {
				diff := cmp.Diff(aDir, gotRoot)
				t.Errorf("  Diff between unpacked roots (-want +got):\n%s", diff)
			}
		} else {
			t.Errorf("  Root digest gotten not present in blobs map")
		}
	}
	if diff := cmp.Diff(wantBlobs, gotBlobs); diff != "" {
		t.Errorf("ComputeMerkleTree(...) gave diff (-want +got) on blobs:\n%s", diff)
	}
	wantCacheCalls := map[string]int{
		"empty":     1,
		"b":         1,
		"b/empty":   1,
		"b/c":       1,
		"b/c/empty": 1,
		"b/c/file":  1,
	}
	if diff := cmp.Diff(wantCacheCalls, cache.calls, cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("ComputeMerkleTree(...) gave diff on file metadata cache access (-want +got) on blobs:\n%s", diff)
	}
	wantStats := &Stats{
		InputDirectories: 6,
		InputFiles:       1,
		TotalInputBytes:  fileDg.Size + aDirDg.Size + bDirDg.Size + cDirDg.Size,
	}
	if diff := cmp.Diff(wantStats, stats); diff != "" {
		t.Errorf("ComputeMerkleTree(...) gave diff on stats (-want +got) on blobs:\n%s", diff)
	}
}

func TestComputeMerkleTreeEmptyStructureVirtualInputs(t *testing.T) {
	emptyDirDgPb := digest.Empty.ToProto()
	cDir := &repb.Directory{
		Directories: []*repb.DirectoryNode{{Name: "empty", Digest: emptyDirDgPb}},
	}
	cDirBlob := mustMarshal(cDir)
	cDirDg := digest.NewFromBlob(cDirBlob)
	cDirDgPb := cDirDg.ToProto()
	bDir := &repb.Directory{
		Directories: []*repb.DirectoryNode{
			{Name: "c", Digest: cDirDgPb},
			{Name: "empty", Digest: emptyDirDgPb},
		},
	}
	bDirBlob := mustMarshal(bDir)
	bDirDg := digest.NewFromBlob(bDirBlob)
	bDirDgPb := bDirDg.ToProto()
	aDir := &repb.Directory{
		Directories: []*repb.DirectoryNode{
			{Name: "b", Digest: bDirDgPb},
			{Name: "empty", Digest: emptyDirDgPb},
		},
	}
	aDirBlob := mustMarshal(aDir)
	aDirDg := digest.NewFromBlob(aDirBlob)

	root, err := ioutil.TempDir("", t.Name())
	if err != nil {
		t.Fatalf("failed to make temp dir: %v", err)
	}
	defer os.RemoveAll(root)
	inputSpec := &command.InputSpec{VirtualInputs: []*command.VirtualInput{
		&command.VirtualInput{Path: "b/c/empty", IsEmptyDirectory: true},
		&command.VirtualInput{Path: "b/empty", IsEmptyDirectory: true},
		&command.VirtualInput{Path: "empty", IsEmptyDirectory: true},
	}}
	wantBlobs := map[digest.Digest][]byte{
		aDirDg:       aDirBlob,
		bDirDg:       bDirBlob,
		cDirDg:       cDirBlob,
		digest.Empty: []byte{},
	}

	gotBlobs := make(map[digest.Digest][]byte)
	cache := newCallCountingMetadataCache(root, t)
	gotRootDg, inputs, stats, err := ComputeMerkleTree(root, inputSpec, chunker.DefaultChunkSize, cache)
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
	if diff := cmp.Diff(aDirDg, gotRootDg); diff != "" {
		t.Errorf("ComputeMerkleTree(...) gave diff (-want +got) on root:\n%s", diff)
		if gotRootBlob, ok := gotBlobs[gotRootDg]; ok {
			gotRoot := new(repb.Directory)
			if err := proto.Unmarshal(gotRootBlob, gotRoot); err != nil {
				t.Errorf("  When unpacking root blob, got error: %s", err)
			} else {
				diff := cmp.Diff(aDir, gotRoot)
				t.Errorf("  Diff between unpacked roots (-want +got):\n%s", diff)
			}
		} else {
			t.Errorf("  Root digest gotten not present in blobs map")
		}
	}
	if diff := cmp.Diff(wantBlobs, gotBlobs); diff != "" {
		t.Errorf("ComputeMerkleTree(...) gave diff (-want +got) on blobs:\n%s", diff)
	}
	if len(cache.calls) != 0 {
		t.Errorf("ComputeMerkleTree(...) gave diff on file metadata cache access (want 0, got %v)", cache.calls)
	}
	wantStats := &Stats{
		InputDirectories: 6,
		TotalInputBytes:  aDirDg.Size + bDirDg.Size + cDirDg.Size,
	}
	if diff := cmp.Diff(wantStats, stats); diff != "" {
		t.Errorf("ComputeMerkleTree(...) gave diff on stats (-want +got) on blobs:\n%s", diff)
	}
}

func TestComputeMerkleTree(t *testing.T) {
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
				{path: "foo", fileContents: fooBlob, isExecutable: true},
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
				{path: "fooDir/foo", fileContents: fooBlob, isExecutable: true},
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
				{path: "fooDir/foo", fileContents: fooBlob, isExecutable: true},
				{path: "fooDir/otherDir/foo", fileContents: fooBlob, isExecutable: true},
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
				{path: "fooDir/foo", fileContents: fooBlob, isExecutable: true},
				{path: "foo", isSymlink: true, isAbsolute: true, symlinkTarget: "fooDir/foo"},
			},
			spec: &command.InputSpec{
				Inputs: []string{"fooDir", "foo"},
			},
			rootDir: &repb.Directory{
				Directories: []*repb.DirectoryNode{{Name: "fooDir", Digest: fooDirDgPb}},
				Files:       []*repb.FileNode{{Name: "foo", Digest: fooDgPb, IsExecutable: true}},
			},
			additionalBlobs: [][]byte{fooBlob, fooDirBlob},
			wantCacheCalls: map[string]int{
				"fooDir":     1,
				"fooDir/foo": 1,
				"foo":        1,
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
				{path: "fooDir/foo", fileContents: fooBlob, isExecutable: true},
				{path: "foo", isSymlink: true, symlinkTarget: "fooDir/foo"},
			},
			spec: &command.InputSpec{
				Inputs: []string{"fooDir", "foo"},
			},
			rootDir: &repb.Directory{
				Directories: []*repb.DirectoryNode{{Name: "fooDir", Digest: fooDirDgPb}},
				Files:       []*repb.FileNode{{Name: "foo", Digest: fooDgPb, IsExecutable: true}},
			},
			additionalBlobs: [][]byte{fooBlob, fooDirBlob},
			wantCacheCalls: map[string]int{
				"fooDir":     1,
				"fooDir/foo": 1,
				"foo":        1,
			},
			wantStats: &Stats{
				InputDirectories: 2,
				InputFiles:       2,
				TotalInputBytes:  2*fooDg.Size + fooDirDg.Size,
			},
		},
		{
			desc: "File invalid symlink",
			input: []*inputPath{
				{path: "fooDir/foo", fileContents: fooBlob, isExecutable: true},
				{path: "foo", isSymlink: true, symlinkTarget: "fooDir/foo"},
				{path: "bar", isSymlink: true, symlinkTarget: "fooDir/bar"},
			},
			spec: &command.InputSpec{
				Inputs: []string{"fooDir", "foo"},
			},
			rootDir: &repb.Directory{
				Directories: []*repb.DirectoryNode{{Name: "fooDir", Digest: fooDirDgPb}},
				Files:       []*repb.FileNode{{Name: "foo", Digest: fooDgPb, IsExecutable: true}},
			},
			additionalBlobs: [][]byte{fooBlob, fooDirBlob},
			wantCacheCalls: map[string]int{
				"fooDir":     1,
				"fooDir/foo": 1,
				"foo":        1,
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
				{path: "fooDir/foo", fileContents: fooBlob, isExecutable: true},
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
				{path: "fooDir/foo", fileContents: fooBlob, isExecutable: true},
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
				{path: "fooDir/foo", fileContents: fooBlob, isExecutable: true},
				{path: "foobarDir/foo", fileContents: fooBlob, isExecutable: true},
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
				{path: "fooDir1/foo", fileContents: fooBlob, isExecutable: true},
				{path: "fooDir2/foo", fileContents: fooBlob, isExecutable: true},
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
				{path: "fooDirBlob", fileContents: fooDirBlob, isExecutable: true},
				{path: "fooDir/foo", fileContents: fooBlob, isExecutable: true},
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
				{path: "fooDir/foo", fileContents: fooBlob, isExecutable: true},
				{path: "fooDir/foo.txt", fileContents: fooBlob, isExecutable: true},
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
				{path: "foo", fileContents: fooBlob, isExecutable: true},
				{path: "fooDir/foo", fileContents: fooBlob, isExecutable: true},
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
				{path: "foo", fileContents: fooBlob, isExecutable: true},
				{path: "fooDir/foo", fileContents: fooBlob, isExecutable: true},
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
					&command.VirtualInput{Path: "fooDir/foo", Contents: fooBlob, IsExecutable: true},
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
			desc: "Physical inputs supercede virtual inputs",
			input: []*inputPath{
				{path: "fooDir/foo", fileContents: fooBlob, isExecutable: true},
				{path: "barDir/bar", fileContents: barBlob},
			},
			spec: &command.InputSpec{
				Inputs: []string{"fooDir", "barDir"},
				VirtualInputs: []*command.VirtualInput{
					&command.VirtualInput{Path: "fooDir/foo", Contents: barBlob, IsExecutable: true},
					&command.VirtualInput{Path: "barDir/bar", IsEmptyDirectory: true},
				},
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
			desc: "Normalizing virtual inputs paths",
			spec: &command.InputSpec{
				VirtualInputs: []*command.VirtualInput{
					&command.VirtualInput{Path: "fooDir/../fooDir/foo", Contents: fooBlob, IsExecutable: true},
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
				{path: "a", fileContents: fooBlob, isExecutable: true},
				{path: "b", fileContents: fooBlob, isExecutable: true},
				{path: "c", fileContents: fooBlob, isExecutable: true},
				{path: "d", fileContents: barBlob},
				{path: "e", fileContents: barBlob},
				{path: "f", fileContents: barBlob},
				{path: "g/foo", fileContents: fooBlob, isExecutable: true},
				{path: "h/foo", fileContents: fooBlob, isExecutable: true},
				{path: "i/foo", fileContents: fooBlob, isExecutable: true},
				{path: "j/bar", fileContents: barBlob},
				{path: "k/bar", fileContents: barBlob},
				{path: "l/bar", fileContents: barBlob},
			},
			spec: &command.InputSpec{
				Inputs: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"},
			},
			rootDir: &repb.Directory{
				Files: []*repb.FileNode{
					{Name: "a", Digest: fooDgPb, IsExecutable: true},
					{Name: "b", Digest: fooDgPb, IsExecutable: true},
					{Name: "c", Digest: fooDgPb, IsExecutable: true},
					{Name: "d", Digest: barDgPb},
					{Name: "e", Digest: barDgPb},
					{Name: "f", Digest: barDgPb},
				},
				Directories: []*repb.DirectoryNode{
					{Name: "g", Digest: fooDirDgPb},
					{Name: "h", Digest: fooDirDgPb},
					{Name: "i", Digest: fooDirDgPb},
					{Name: "j", Digest: barDirDgPb},
					{Name: "k", Digest: barDirDgPb},
					{Name: "l", Digest: barDirDgPb},
				},
			},
			additionalBlobs: [][]byte{fooBlob, fooDirBlob, barBlob, barDirBlob},
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
				"j/bar": 1,
				"k/bar": 1,
				"l/bar": 1,
			},
			wantStats: &Stats{
				InputDirectories: 7,
				InputFiles:       12,
				TotalInputBytes:  12*fooDg.Size + 3*fooDirDg.Size + 3*barDirDg.Size,
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
			rootBlob := mustMarshal(tc.rootDir)
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
				t.Errorf("ComputeMerkleTree(...) gave diff (-want +got) on root:\n%s", diff)
				if gotRootBlob, ok := gotBlobs[gotRootDg]; ok {
					gotRoot := new(repb.Directory)
					if err := proto.Unmarshal(gotRootBlob, gotRoot); err != nil {
						t.Errorf("  When unpacking root blob, got error: %s", err)
					} else {
						diff := cmp.Diff(tc.rootDir, gotRoot)
						t.Errorf("  Diff between unpacked roots (-want +got):\n%s", diff)
					}
				} else {
					t.Errorf("  Root digest gotten not present in blobs map")
				}
			}
			if diff := cmp.Diff(wantBlobs, gotBlobs); diff != "" {
				t.Errorf("ComputeMerkleTree(...) gave diff (-want +got) on blobs:\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantCacheCalls, cache.calls, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("ComputeMerkleTree(...) gave diff on file metadata cache access (-want +got) on blobs:\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantStats, stats); diff != "" {
				t.Errorf("ComputeMerkleTree(...) gave diff on stats (-want +got) on blobs:\n%s", diff)
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
			if _, _, _, err := ComputeMerkleTree(root, tc.spec, chunker.DefaultChunkSize, filemetadata.NewNoopCache()); err == nil {
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
		"x/a/b/c":   &Output{IsEmptyDirectory: true, Digest: digest.Empty},
		"x/a/b/foo": &Output{Digest: fooDigest},
		"x/a/b/bar": &Output{Digest: barDigest, IsExecutable: true},
		"x/b/c":     &Output{IsEmptyDirectory: true, Digest: digest.Empty},
		"x/b/foo":   &Output{Digest: fooDigest},
		"x/b/bar":   &Output{Digest: barDigest, IsExecutable: true},
		"x/c":       &Output{IsEmptyDirectory: true, Digest: digest.Empty},
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

func TestComputeOutputsToUploadFiles(t *testing.T) {
	tests := []struct {
		desc           string
		input          []*inputPath
		paths          []string
		wantResult     *repb.ActionResult
		wantBlobs      [][]byte
		wantCacheCalls map[string]int
	}{
		{
			desc:       "Empty paths",
			input:      nil,
			wantResult: &repb.ActionResult{},
		},
		{
			desc: "Missing output",
			input: []*inputPath{
				{path: "foo", fileContents: fooBlob, isExecutable: true},
			},
			paths:     []string{"foo", "bar"},
			wantBlobs: [][]byte{fooBlob},
			wantResult: &repb.ActionResult{
				OutputFiles: []*repb.OutputFile{&repb.OutputFile{Path: "foo", Digest: fooDgPb, IsExecutable: true}},
			},
			wantCacheCalls: map[string]int{
				"bar": 1,
				"foo": 1,
			},
		},
		{
			desc: "Two files",
			input: []*inputPath{
				{path: "foo", fileContents: fooBlob, isExecutable: true},
				{path: "bar", fileContents: barBlob},
			},
			paths:     []string{"foo", "bar"},
			wantBlobs: [][]byte{fooBlob, barBlob},
			wantResult: &repb.ActionResult{
				OutputFiles: []*repb.OutputFile{
					// Note the outputs are not sorted.
					&repb.OutputFile{Path: "foo", Digest: fooDgPb, IsExecutable: true},
					&repb.OutputFile{Path: "bar", Digest: barDgPb},
				},
			},
			wantCacheCalls: map[string]int{
				"bar": 1,
				"foo": 1,
			},
		},
		{
			desc: "Duplicate file contents",
			input: []*inputPath{
				{path: "foo", fileContents: fooBlob, isExecutable: true},
				{path: "bar", fileContents: fooBlob},
			},
			paths:     []string{"foo", "bar"},
			wantBlobs: [][]byte{fooBlob},
			wantResult: &repb.ActionResult{
				OutputFiles: []*repb.OutputFile{
					// Note the outputs are not sorted.
					&repb.OutputFile{Path: "foo", Digest: fooDgPb, IsExecutable: true},
					&repb.OutputFile{Path: "bar", Digest: fooDgPb},
				},
			},
			wantCacheCalls: map[string]int{
				"bar": 1,
				"foo": 1,
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
			for _, b := range tc.wantBlobs {
				wantBlobs[digest.NewFromBlob(b)] = b
			}

			gotBlobs := make(map[digest.Digest][]byte)
			cache := newCallCountingMetadataCache(root, t)
			chunkers, gotResult, err := ComputeOutputsToUpload(root, tc.paths, chunker.DefaultChunkSize, cache)
			if err != nil {
				t.Errorf("ComputeOutputsToUpload(...) = gave error %v, want success", err)
			}
			for _, ch := range chunkers {
				blob, err := ch.FullData()
				if err != nil {
					t.Errorf("chunker %v FullData() returned error %v", ch, err)
				}
				gotBlobs[ch.Digest()] = blob
			}
			if diff := cmp.Diff(wantBlobs, gotBlobs); diff != "" {
				t.Errorf("ComputeOutputsToUpload(...) gave diff (-want +got) on blobs:\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantCacheCalls, cache.calls, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("ComputeOutputsToUpload(...) gave diff on file metadata cache access (-want +got) on blobs:\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantResult, gotResult, cmp.Comparer(proto.Equal)); diff != "" {
				t.Errorf("ComputeOutputsToUpload(...) gave diff on action result (-want +got) on blobs:\n%s", diff)
			}
		})
	}
}

func TestComputeOutputsToUploadDirectories(t *testing.T) {
	tests := []struct {
		desc  string
		input []*inputPath
		// The blobs are everything else outside of the Tree proto itself.
		wantBlobs        [][]byte
		wantTreeRoot     *repb.Directory
		wantTreeChildren []*repb.Directory
		wantCacheCalls   map[string]int
	}{
		{
			desc: "Two files",
			input: []*inputPath{
				{path: "a/b/fooDir/foo", fileContents: fooBlob, isExecutable: true},
				{path: "a/b/fooDir/bar", fileContents: barBlob},
			},
			wantBlobs:    [][]byte{fooBlob, barBlob},
			wantTreeRoot: foobarDir,
			wantCacheCalls: map[string]int{
				"a/b/fooDir":     2,
				"a/b/fooDir/bar": 1,
				"a/b/fooDir/foo": 1,
			},
		},
		{
			desc: "Duplicate file contents",
			input: []*inputPath{
				{path: "a/b/fooDir/foo", fileContents: fooBlob, isExecutable: true},
				{path: "a/b/fooDir/bar", fileContents: fooBlob, isExecutable: true},
			},
			wantBlobs: [][]byte{fooBlob, fooBlob},
			wantTreeRoot: &repb.Directory{Files: []*repb.FileNode{
				{Name: "bar", Digest: fooDgPb, IsExecutable: true},
				{Name: "foo", Digest: fooDgPb, IsExecutable: true},
			}},
			wantCacheCalls: map[string]int{
				"a/b/fooDir":     2,
				"a/b/fooDir/bar": 1,
				"a/b/fooDir/foo": 1,
			},
		},
		{
			desc: "Duplicate subdirectories",
			input: []*inputPath{
				{path: "a/b/fooDir/dir1/foo", fileContents: fooBlob, isExecutable: true},
				{path: "a/b/fooDir/dir2/foo", fileContents: fooBlob, isExecutable: true},
			},
			wantBlobs: [][]byte{fooBlob},
			wantTreeRoot: &repb.Directory{Directories: []*repb.DirectoryNode{
				{Name: "dir1", Digest: fooDirDgPb},
				{Name: "dir2", Digest: fooDirDgPb},
			}},
			wantTreeChildren: []*repb.Directory{fooDir},
			wantCacheCalls: map[string]int{
				"a/b/fooDir":          2,
				"a/b/fooDir/dir1":     1,
				"a/b/fooDir/dir1/foo": 1,
				"a/b/fooDir/dir2":     1,
				"a/b/fooDir/dir2/foo": 1,
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
			for _, b := range tc.wantBlobs {
				wantBlobs[digest.NewFromBlob(b)] = b
			}

			gotBlobs := make(map[digest.Digest][]byte)
			cache := newCallCountingMetadataCache(root, t)
			chunkers, gotResult, err := ComputeOutputsToUpload(root, []string{"a/b/fooDir"}, chunker.DefaultChunkSize, cache)
			if err != nil {
				t.Fatalf("ComputeOutputsToUpload(...) = gave error %v, want success", err)
			}
			for _, ch := range chunkers {
				blob, err := ch.FullData()
				if err != nil {
					t.Errorf("chunker %v FullData() returned error %v", ch, err)
				}
				gotBlobs[ch.Digest()] = blob
			}
			if diff := cmp.Diff(tc.wantCacheCalls, cache.calls, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("ComputeOutputsToUpload(...) gave diff on file metadata cache access (-want +got) on blobs:\n%s", diff)
			}
			if len(gotResult.OutputDirectories) != 1 {
				t.Fatalf("ComputeOutputsToUpload(...) expected result with an output directory, got %+v", gotResult)
			}
			dir := gotResult.OutputDirectories[0]
			if dir.Path != "a/b/fooDir" {
				t.Errorf("ComputeOutputsToUpload(...) gave result dir path %s, want a/b/fooDir:\n", dir.Path)
			}
			dg := digest.NewFromProtoUnvalidated(dir.TreeDigest)
			treeBlob, ok := gotBlobs[dg]
			if !ok {
				t.Fatalf("ComputeOutputsToUpload(...) tree proto with digest %+v not uploaded", dg)
			}
			wantBlobs[dg] = treeBlob
			rootBlob := mustMarshal(tc.wantTreeRoot)
			wantBlobs[digest.NewFromBlob(rootBlob)] = rootBlob
			if diff := cmp.Diff(wantBlobs, gotBlobs); diff != "" {
				t.Errorf("ComputeOutputsToUpload(...) gave diff (-want +got) on blobs:\n%s", diff)
			}
			tree := &repb.Tree{}
			if err := proto.Unmarshal(treeBlob, tree); err != nil {
				t.Errorf("ComputeOutputsToUpload(...) failed unmarshalling tree blob from %v: %v\n", treeBlob, err)
			}
			if diff := cmp.Diff(tc.wantTreeRoot, tree.Root, cmp.Comparer(proto.Equal)); diff != "" {
				t.Errorf("ComputeOutputsToUpload(...) gave diff (-want +got) on tree root:\n%s", diff)
			}
			wantChildren := make(map[digest.Digest]*repb.Directory)
			for _, d := range tc.wantTreeChildren {
				wantChildren[digest.TestNewFromMessage(d)] = d
			}
			gotChildren := make(map[digest.Digest]*repb.Directory)
			for _, d := range tree.Children {
				gotChildren[digest.TestNewFromMessage(d)] = d
			}
			if diff := cmp.Diff(wantChildren, gotChildren, cmp.Comparer(proto.Equal)); diff != "" {
				t.Errorf("ComputeOutputsToUpload(...) gave diff (-want +got) on tree children:\n%s", diff)
			}
		})
	}
}
