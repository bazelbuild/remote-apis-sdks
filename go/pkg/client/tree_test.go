package client_test

import (
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	cpb "github.com/bazelbuild/remote-apis-sdks/go/api/command"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/chunker"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/client"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/command"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/fakes"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/filemetadata"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/uploadinfo"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

var (
	fooBlob, barBlob, bazBlob = []byte("foo"), []byte("bar"), []byte("baz")
	fooDg, barDg, bazDg       = digest.NewFromBlob(fooBlob), digest.NewFromBlob(barBlob), digest.NewFromBlob(bazBlob)
	fooDgPb, barDgPb, bazDgPb = fooDg.ToProto(), barDg.ToProto(), bazDg.ToProto()
	fooProperties             = &cpb.NodeProperties{Properties: []*cpb.NodeProperty{{Name: "fooName", Value: "fooValue"}}}

	fooDir    = &repb.Directory{Files: []*repb.FileNode{{Name: "foo", Digest: fooDgPb, IsExecutable: true, NodeProperties: command.NodePropertiesToAPI(fooProperties)}}}
	barDir    = &repb.Directory{Files: []*repb.FileNode{{Name: "bar", Digest: barDgPb}}}
	bazDir    = &repb.Directory{Files: []*repb.FileNode{{Name: "baz", Digest: bazDgPb}}}
	vBarDir   = &repb.Directory{Directories: []*repb.DirectoryNode{{Name: "baz", Digest: digest.Empty.ToProto()}}}
	foobarDir = &repb.Directory{Files: []*repb.FileNode{
		{Name: "bar", Digest: barDgPb},
		{Name: "foo", Digest: fooDgPb, IsExecutable: true, NodeProperties: command.NodePropertiesToAPI(fooProperties)},
	}}

	fooDirBlob, barDirBlob, foobarDirBlob, bazDirBlob, vBarDirBlob = mustMarshal(fooDir), mustMarshal(barDir), mustMarshal(foobarDir), mustMarshal(bazDir), mustMarshal(vBarDir)
	fooDirDg, barDirDg, foobarDirDg, bazDirDg, vBarDirDg           = digest.NewFromBlob(fooDirBlob), digest.NewFromBlob(barDirBlob), digest.NewFromBlob(foobarDirBlob), digest.NewFromBlob(bazDirBlob), digest.NewFromBlob(vBarDirBlob)
	fooDirDgPb, barDirDgPb, foobarDirDgPb, bazDirDgPb, vBarDirDgPb = fooDirDg.ToProto(), barDirDg.ToProto(), foobarDirDg.ToProto(), bazDirDg.ToProto(), vBarDirDg.ToProto()
)

func mustMarshal(p proto.Message) []byte {
	b, err := proto.Marshal(p)
	if err != nil {
		panic("error marshalling proto during test setup: %s" + err.Error())
	}
	return b
}

func newDigest(t *testing.T, hash string, size int64) digest.Digest {
	dg, err := digest.New(hash, size)
	if err != nil {
		t.Fatalf("unexpected error while creating digest: %v", err)
	}
	return dg
}

type inputPath struct {
	path             string
	emptyDir         bool
	fileContents     []byte
	isExecutable     bool
	isSymlink        bool
	isAbsolute       bool
	relSymlinkTarget string
}

func construct(dir string, ips []*inputPath) error {
	for _, ip := range ips {
		path := filepath.Join(dir, ip.path)
		if ip.emptyDir {
			if err := os.MkdirAll(path, 0777); err != nil {
				return err
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(path), 0777); err != nil {
			return err
		}
		if ip.isSymlink {
			target := ip.relSymlinkTarget
			if ip.isAbsolute {
				target = filepath.Join(dir, target)
			}
			if err := os.Symlink(target, path); err != nil {
				return err
			}
			continue
		}
		// Regular file.
		perm := os.FileMode(0666)
		if ip.isExecutable {
			perm = os.FileMode(0777)
		}
		if err := os.WriteFile(path, ip.fileContents, perm); err != nil {
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

func (c *callCountingMetadataCache) GetCacheHits() uint64 {
	return 0
}

func (c *callCountingMetadataCache) GetCacheMisses() uint64 {
	return 0
}

func TestComputeMerkleTreeRemoteWorkingDir(t *testing.T) {
	callComputeMerkleTree := func(files, inputs []string, virtualInputs []*command.VirtualInput, localWorkingDir, remoteWorkingDir string) (digest.Digest, map[string]int) {
		root := t.TempDir()
		inputPaths := []*inputPath{}
		for _, file := range files {
			inputPaths = append(inputPaths, &inputPath{path: file, fileContents: []byte(filepath.Base(file)), isExecutable: true})
		}
		if err := construct(root, inputPaths); err != nil {
			t.Fatalf("failed to construct input dir structure: %v", err)
		}
		cache := newCallCountingMetadataCache(root, t)
		spec := &command.InputSpec{Inputs: inputs, VirtualInputs: virtualInputs}

		e, cleanup := fakes.NewTestEnv(t)
		defer cleanup()

		rootDg, _, _, err := e.Client.GrpcClient.ComputeMerkleTree(context.Background(), root, localWorkingDir, remoteWorkingDir, spec, cache)
		if err != nil {
			t.Errorf("ComputeMerkleTree(...) = gave error %q, want success", err)
		}
		return rootDg, cache.calls
	}
	// call ComputeMerkleTree with working dir = "out/bar" and remote working dir not overridden
	// inputs contain a file and a non-empty dir in the working directory, and a file and a non-empty dir outside of the working directory
	referenceDg, _ := callComputeMerkleTree([]string{"out/bar/a", "out/bar/foo/b", "c", "bar/baz/d"},
		[]string{"out/bar/a", "out/bar/foo", "c", "bar/baz"}, []*command.VirtualInput{{Path: "out/bar/baz", IsEmptyDirectory: true}}, "out/bar", "")
	tests := []struct {
		localWorkingDir  string
		remoteWorkingDir string
		files            []string
		inputs           []string
		virtualInputs    []*command.VirtualInput
		wantCacheCalls   map[string]int
	}{
		{
			localWorkingDir:  "out/foo1",
			remoteWorkingDir: "out/bar",
			files:            []string{"out/foo1/a", "out/foo1/foo/b", "c", "bar/baz/d"},
			inputs:           []string{"out/foo1/a", "out/foo1/foo", "c", "bar/baz"},
			virtualInputs:    []*command.VirtualInput{{Path: "out/foo1/baz", IsEmptyDirectory: true}},
			// ensures that file metadata cache is queried for local paths (not remote ones)
			wantCacheCalls: map[string]int{"out/foo1/a": 1, "out/foo1/foo": 1, "out/foo1/foo/b": 1, "c": 1, "bar/baz": 1, "bar/baz/d": 1},
		},
		{
			localWorkingDir:  "out/bar",
			remoteWorkingDir: "out/bar",
			files:            []string{"out/bar/a", "out/bar/foo/b", "c", "bar/baz/d"},
			inputs:           []string{"out/bar/a", "out/bar/foo", "c", "bar/baz"},
			virtualInputs:    []*command.VirtualInput{{Path: "out/bar/baz", IsEmptyDirectory: true}},
			wantCacheCalls:   map[string]int{"out/bar/a": 1, "out/bar/foo": 1, "out/bar/foo/b": 1, "c": 1, "bar/baz": 1, "bar/baz/d": 1},
		},
	}
	for _, tc := range tests {
		gotDg, gotCacheCalls := callComputeMerkleTree(tc.files, tc.inputs, tc.virtualInputs, tc.localWorkingDir, tc.remoteWorkingDir)
		if diff := cmp.Diff(referenceDg, gotDg); diff != "" {
			t.Errorf("ComputeMerkleTree with workingDir=%q andRemoteWorkingDir=%q returned different root digest than expected (-want +got)\n%s", tc.localWorkingDir, tc.remoteWorkingDir, diff)
		}
		if diff := cmp.Diff(tc.wantCacheCalls, gotCacheCalls); diff != "" {
			t.Errorf("ComputeMerkleTree with workingDir=%q andRemoteWorkingDir=%q made unexpected file metadata cache calls (-want +got)\n%s", tc.localWorkingDir, tc.remoteWorkingDir, diff)
		}
	}
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
	root := t.TempDir()
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

	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()

	gotRootDg, inputs, stats, err := e.Client.GrpcClient.ComputeMerkleTree(context.Background(), root, "", "", inputSpec, cache)
	if err != nil {
		t.Errorf("ComputeMerkleTree(...) = gave error %v, want success", err)
	}
	for _, ue := range inputs {
		ch, err := chunker.New(ue, false, int(e.Client.GrpcClient.ChunkMaxSize))
		if err != nil {
			t.Fatalf("chunker.New(ue): failed to create chunker from UploadEntry: %v", err)
		}
		blob, err := ch.FullData()
		if err != nil {
			t.Errorf("chunker %v FullData() returned error %v", ch, err)
		}
		gotBlobs[ue.Digest] = blob
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
	wantStats := &client.TreeStats{
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

	root := t.TempDir()
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

	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()

	gotRootDg, inputs, stats, err := e.Client.GrpcClient.ComputeMerkleTree(context.Background(), root, "", "", inputSpec, cache)
	if err != nil {
		t.Errorf("ComputeMerkleTree(...) = gave error %v, want success", err)
	}
	for _, ue := range inputs {
		ch, err := chunker.New(ue, false, int(e.Client.GrpcClient.ChunkMaxSize))
		if err != nil {
			t.Fatalf("chunker.New(ue): failed to create chunker from UploadEntry: %v", err)
		}
		blob, err := ch.FullData()
		if err != nil {
			t.Errorf("chunker %v FullData() returned error %v", ch, err)
		}
		gotBlobs[ue.Digest] = blob
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
	wantStats := &client.TreeStats{
		InputDirectories: 6,
		TotalInputBytes:  aDirDg.Size + bDirDg.Size + cDirDg.Size,
	}
	if diff := cmp.Diff(wantStats, stats); diff != "" {
		t.Errorf("ComputeMerkleTree(...) gave diff on stats (-want +got) on blobs:\n%s", diff)
	}
}

func TestComputeMerkleTreeEmptyRoot(t *testing.T) {
	root := t.TempDir()
	inputSpec := &command.InputSpec{
		Inputs: []string{"."},
	}
	cache := newCallCountingMetadataCache(root, t)

	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()

	gotRootDg, inputs, stats, err := e.Client.GrpcClient.ComputeMerkleTree(context.Background(), root, "", "", inputSpec, cache)
	if err != nil {
		t.Errorf("ComputeMerkleTree(...) = gave error %v, want success", err)
	}
	if diff := cmp.Diff(digest.Empty, gotRootDg); diff != "" {
		t.Errorf("ComputeMerkleTree(...) gave diff (-want +got) on root:\n%s", diff)
	}
	if len(inputs) != 1 {
		t.Errorf("ComputeMerkleTree(...) should only include one input:\n%v", inputs)
	}
	wantInput := uploadinfo.EntryFromBlob([]byte{})
	if diff := cmp.Diff(wantInput, inputs[0], cmp.AllowUnexported(uploadinfo.Entry{})); diff != "" {
		t.Errorf("ComputeMerkleTree(...) gave diff on input (-want +got) on blobs:\n%s", diff)
	}
	wantStats := &client.TreeStats{InputDirectories: 1}
	if diff := cmp.Diff(wantStats, stats); diff != "" {
		t.Errorf("ComputeMerkleTree(...) gave diff on stats (-want +got) on blobs:\n%s", diff)
	}
}

func TestComputeMerkleTree(t *testing.T) {
	foobarSymDir := &repb.Directory{Symlinks: []*repb.SymlinkNode{{Name: "foobarSymDir", Target: "../foobarDir"}}}
	foobarSymDirBlob := mustMarshal(foobarSymDir)
	foobarSymDirDg := digest.NewFromBlob(foobarSymDirBlob)
	foobarSymDirDgPb := foobarSymDirDg.ToProto()

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
		wantStats *client.TreeStats
		treeOpts  *client.TreeSymlinkOpts
	}{
		{
			desc:            "Empty directory",
			input:           nil,
			spec:            &command.InputSpec{},
			rootDir:         &repb.Directory{},
			additionalBlobs: nil,
			wantStats: &client.TreeStats{
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
				Inputs:              []string{"foo", "bar"},
				InputNodeProperties: map[string]*cpb.NodeProperties{"foo": fooProperties},
			},
			rootDir:         foobarDir,
			additionalBlobs: [][]byte{fooBlob, barBlob},
			wantCacheCalls: map[string]int{
				"foo": 1,
				"bar": 1,
			},
			wantStats: &client.TreeStats{
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
				Inputs:              []string{"fooDir", "barDir"},
				InputNodeProperties: map[string]*cpb.NodeProperties{"fooDir/foo": fooProperties},
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
			wantStats: &client.TreeStats{
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
				Inputs:              []string{"fooDir/../fooDir/foo", "//barDir//bar"},
				InputNodeProperties: map[string]*cpb.NodeProperties{"fooDir/foo": fooProperties},
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
			wantStats: &client.TreeStats{
				InputDirectories: 3,
				InputFiles:       2,
				TotalInputBytes:  fooDg.Size + barDg.Size + fooDirDg.Size + barDirDg.Size,
			},
		},
		{
			desc: "File absolute symlink",
			input: []*inputPath{
				{path: "fooDir/foo", fileContents: fooBlob, isExecutable: true},
				{path: "foo", isSymlink: true, isAbsolute: true, relSymlinkTarget: "fooDir/foo"},
			},
			spec: &command.InputSpec{
				Inputs:              []string{"fooDir", "foo"},
				InputNodeProperties: map[string]*cpb.NodeProperties{"fooDir/foo": fooProperties},
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
			wantStats: &client.TreeStats{
				InputDirectories: 2,
				InputFiles:       2,
				TotalInputBytes:  2*fooDg.Size + fooDirDg.Size,
			},
		},
		{
			desc: "File relative symlink",
			input: []*inputPath{
				{path: "fooDir/foo", fileContents: fooBlob, isExecutable: true},
				{path: "foo", isSymlink: true, relSymlinkTarget: "fooDir/foo"},
			},
			spec: &command.InputSpec{
				Inputs:              []string{"fooDir", "foo"},
				InputNodeProperties: map[string]*cpb.NodeProperties{"fooDir/foo": fooProperties},
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
			wantStats: &client.TreeStats{
				InputDirectories: 2,
				InputFiles:       2,
				TotalInputBytes:  2*fooDg.Size + fooDirDg.Size,
			},
		},
		{
			desc: "File relative symlink (preserved)",
			input: []*inputPath{
				{path: "fooDir/foo", fileContents: fooBlob, isExecutable: true},
				{path: "fooSym", isSymlink: true, relSymlinkTarget: "fooDir/foo"},
			},
			spec: &command.InputSpec{
				// The symlink target will be traversed recursively.
				Inputs:              []string{"fooSym"},
				InputNodeProperties: map[string]*cpb.NodeProperties{"fooDir/foo": fooProperties},
			},
			rootDir: &repb.Directory{
				Directories: []*repb.DirectoryNode{{Name: "fooDir", Digest: fooDirDgPb}},
				Symlinks:    []*repb.SymlinkNode{{Name: "fooSym", Target: "fooDir/foo"}},
			},
			additionalBlobs: [][]byte{fooBlob, fooDirBlob},
			wantCacheCalls: map[string]int{
				"fooDir":     1,
				"fooDir/foo": 1,
				"fooSym":     1,
			},
			wantStats: &client.TreeStats{
				InputDirectories: 2,
				InputFiles:       1,
				InputSymlinks:    1,
				TotalInputBytes:  fooDg.Size + fooDirDg.Size,
			},
			treeOpts: &client.TreeSymlinkOpts{
				Preserved:     true,
				FollowsTarget: true,
			},
		},
		{
			desc: "File relative symlink (preserved based on InputSpec)",
			input: []*inputPath{
				{path: "fooDir/foo", fileContents: fooBlob, isExecutable: true},
				{path: "fooSym", isSymlink: true, relSymlinkTarget: "fooDir/foo"},
			},
			spec: &command.InputSpec{
				// The symlink target will be traversed recursively.
				Inputs:              []string{"fooSym"},
				SymlinkBehavior:     command.PreserveSymlink,
				InputNodeProperties: map[string]*cpb.NodeProperties{"fooDir/foo": fooProperties},
			},
			rootDir: &repb.Directory{
				Directories: []*repb.DirectoryNode{{Name: "fooDir", Digest: fooDirDgPb}},
				Symlinks:    []*repb.SymlinkNode{{Name: "fooSym", Target: "fooDir/foo"}},
			},
			additionalBlobs: [][]byte{fooBlob, fooDirBlob},
			wantCacheCalls: map[string]int{
				"fooDir":     1,
				"fooDir/foo": 1,
				"fooSym":     1,
			},
			wantStats: &client.TreeStats{
				InputDirectories: 2,
				InputFiles:       1,
				InputSymlinks:    1,
				TotalInputBytes:  fooDg.Size + fooDirDg.Size,
			},
			treeOpts: &client.TreeSymlinkOpts{
				FollowsTarget: true,
			},
		},
		{
			desc: "File relative symlink (preserved but not followed)",
			input: []*inputPath{
				{path: "fooDir/foo", fileContents: fooBlob, isExecutable: true},
				{path: "fooSym", isSymlink: true, relSymlinkTarget: "fooDir/foo"},
			},
			spec: &command.InputSpec{
				Inputs:              []string{"fooSym"},
				InputNodeProperties: map[string]*cpb.NodeProperties{"fooDir/foo": fooProperties},
			},
			rootDir: &repb.Directory{
				Directories: nil,
				Symlinks:    []*repb.SymlinkNode{{Name: "fooSym", Target: "fooDir/foo"}},
			},
			wantCacheCalls: map[string]int{
				"fooSym": 1,
			},
			wantStats: &client.TreeStats{
				InputDirectories: 1,
				InputFiles:       0,
				InputSymlinks:    1,
				TotalInputBytes:  0,
			},
			treeOpts: &client.TreeSymlinkOpts{
				Preserved: true,
			},
		},
		{
			desc: "File absolute symlink (preserved)",
			input: []*inputPath{
				{path: "fooDir/foo", fileContents: fooBlob, isExecutable: true},
				{path: "fooSym", isSymlink: true, isAbsolute: true, relSymlinkTarget: "fooDir/foo"},
			},
			spec: &command.InputSpec{
				// The symlink target will be traversed recursively.
				Inputs:              []string{"fooSym"},
				InputNodeProperties: map[string]*cpb.NodeProperties{"fooDir/foo": fooProperties},
			},
			rootDir: &repb.Directory{
				Directories: []*repb.DirectoryNode{{Name: "fooDir", Digest: fooDirDgPb}},
				Symlinks:    []*repb.SymlinkNode{{Name: "fooSym", Target: "fooDir/foo"}},
			},
			additionalBlobs: [][]byte{fooBlob, fooDirBlob},
			wantCacheCalls: map[string]int{
				"fooDir":     1,
				"fooDir/foo": 1,
				"fooSym":     1,
			},
			wantStats: &client.TreeStats{
				InputDirectories: 2,
				InputFiles:       1,
				InputSymlinks:    1,
				TotalInputBytes:  fooDg.Size + fooDirDg.Size,
			},
			treeOpts: &client.TreeSymlinkOpts{
				Preserved:     true,
				FollowsTarget: true,
			},
		},
		{
			desc: "File invalid symlink",
			input: []*inputPath{
				{path: "fooDir/foo", fileContents: fooBlob, isExecutable: true},
				{path: "foo", isSymlink: true, relSymlinkTarget: "fooDir/foo"},
				{path: "bar", isSymlink: true, relSymlinkTarget: "fooDir/bar"},
			},
			spec: &command.InputSpec{
				Inputs:              []string{"fooDir", "foo"},
				InputNodeProperties: map[string]*cpb.NodeProperties{"fooDir/foo": fooProperties},
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
			wantStats: &client.TreeStats{
				InputDirectories: 2,
				InputFiles:       2,
				TotalInputBytes:  2*fooDg.Size + fooDirDg.Size,
			},
		},
		{
			desc: "Dangling symlink is preserved",
			input: []*inputPath{
				{path: "fooDir/foo", fileContents: fooBlob, isExecutable: true},
				{path: "invalidSym", isSymlink: true, relSymlinkTarget: "fooDir/invalid"},
			},
			spec: &command.InputSpec{
				Inputs:              []string{"fooDir", "invalidSym"},
				InputNodeProperties: map[string]*cpb.NodeProperties{"fooDir/foo": fooProperties},
			},
			rootDir: &repb.Directory{
				Directories: []*repb.DirectoryNode{{Name: "fooDir", Digest: fooDirDgPb}},
				Files:       nil,
				Symlinks:    []*repb.SymlinkNode{{Name: "invalidSym", Target: "fooDir/invalid"}},
			},
			additionalBlobs: [][]byte{fooBlob, fooDirBlob},
			wantCacheCalls: map[string]int{
				"fooDir":     2,
				"fooDir/foo": 1,
				"invalidSym": 1,
			},
			wantStats: &client.TreeStats{
				InputDirectories: 2,
				InputFiles:       1,
				InputSymlinks:    1,
				TotalInputBytes:  fooDg.Size + fooDirDg.Size,
			},
			treeOpts: &client.TreeSymlinkOpts{
				Preserved: true,
			},
		},
		{
			desc: "Directory absolute symlink",
			input: []*inputPath{
				{path: "fooDir/foo", fileContents: fooBlob, isExecutable: true},
				{path: "barDirTarget/bar", fileContents: barBlob},
				{path: "barDir", isSymlink: true, isAbsolute: true, relSymlinkTarget: "barDirTarget"},
			},
			spec: &command.InputSpec{
				Inputs:              []string{"fooDir", "barDir"},
				InputNodeProperties: map[string]*cpb.NodeProperties{"fooDir/foo": fooProperties},
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
			wantStats: &client.TreeStats{
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
				{path: "barDir", isSymlink: true, relSymlinkTarget: "barDirTarget"},
			},
			spec: &command.InputSpec{
				Inputs:              []string{"fooDir", "barDir"},
				InputNodeProperties: map[string]*cpb.NodeProperties{"fooDir/foo": fooProperties},
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
			wantStats: &client.TreeStats{
				InputDirectories: 3,
				InputFiles:       2,
				TotalInputBytes:  fooDg.Size + fooDirDg.Size + barDg.Size + barDirDg.Size,
			},
		},
		{
			desc: "Directory absolute symlink (preserved)",
			input: []*inputPath{
				{path: "foobarDir/foo", fileContents: fooBlob, isExecutable: true},
				{path: "foobarDir/bar", fileContents: barBlob},
				{path: "base/foobarSymDir", isSymlink: true, isAbsolute: true, relSymlinkTarget: "foobarDir"},
			},
			spec: &command.InputSpec{
				// The symlink target will be traversed recursively.
				Inputs:              []string{"base/foobarSymDir"},
				InputNodeProperties: map[string]*cpb.NodeProperties{"foobarDir/foo": fooProperties},
			},
			rootDir: &repb.Directory{
				Directories: []*repb.DirectoryNode{{Name: "base", Digest: foobarSymDirDgPb}, {Name: "foobarDir", Digest: foobarDirDgPb}},
			},
			additionalBlobs: [][]byte{fooBlob, barBlob, foobarDirBlob, foobarSymDirBlob},
			wantCacheCalls: map[string]int{
				"base":              1,
				"foobarDir":         3,
				"foobarDir/foo":     1,
				"foobarDir/bar":     1,
				"base/foobarSymDir": 1,
			},
			wantStats: &client.TreeStats{
				InputDirectories: 3,
				InputFiles:       2,
				InputSymlinks:    1,
				TotalInputBytes:  fooDg.Size + barDg.Size + foobarDirDg.Size + foobarSymDirDg.Size,
			},
			treeOpts: &client.TreeSymlinkOpts{
				Preserved:     true,
				FollowsTarget: true,
			},
		},
		{
			desc: "Directory relative symlink (preserved)",
			input: []*inputPath{
				{path: "foobarDir/foo", fileContents: fooBlob, isExecutable: true},
				{path: "foobarDir/bar", fileContents: barBlob},
				{path: "base/foobarSymDir", isSymlink: true, relSymlinkTarget: "../foobarDir"},
			},
			spec: &command.InputSpec{
				// The symlink target will be traversed recursively.
				Inputs:              []string{"base/foobarSymDir"},
				InputNodeProperties: map[string]*cpb.NodeProperties{"foobarDir/foo": fooProperties},
			},
			rootDir: &repb.Directory{
				Directories: []*repb.DirectoryNode{{Name: "base", Digest: foobarSymDirDgPb}, {Name: "foobarDir", Digest: foobarDirDgPb}},
			},
			additionalBlobs: [][]byte{fooBlob, barBlob, foobarDirBlob, foobarSymDirBlob},
			wantCacheCalls: map[string]int{
				"base":              1,
				"foobarDir":         3,
				"foobarDir/foo":     1,
				"foobarDir/bar":     1,
				"base/foobarSymDir": 1,
			},
			wantStats: &client.TreeStats{
				InputDirectories: 3,
				InputFiles:       2,
				InputSymlinks:    1,
				TotalInputBytes:  fooDg.Size + barDg.Size + foobarDirDg.Size + foobarSymDirDg.Size,
			},
			treeOpts: &client.TreeSymlinkOpts{
				Preserved:     true,
				FollowsTarget: true,
			},
		},
		{
			desc: "Directory relative symlink (materialized from outside exec root)",
			input: []*inputPath{
				{path: "../foo", fileContents: fooBlob, isExecutable: true},
				{path: "fooSym", isSymlink: true, relSymlinkTarget: "../foo"},
				{path: "barDir/bar", fileContents: barBlob},
				{path: "barSym", isSymlink: true, relSymlinkTarget: "barDir/bar"},
			},
			spec: &command.InputSpec{
				Inputs: []string{"fooSym", "barSym"},
			},
			rootDir: &repb.Directory{
				Directories: []*repb.DirectoryNode{
					{Name: "barDir", Digest: barDirDgPb},
				},
				Files: []*repb.FileNode{
					{Name: "fooSym", Digest: fooDgPb, IsExecutable: true},
				},
				Symlinks: []*repb.SymlinkNode{
					{Name: "barSym", Target: "barDir/bar"},
				},
			},
			additionalBlobs: [][]byte{fooBlob, barDirBlob, barBlob},
			wantCacheCalls: map[string]int{
				"barDir":     1,
				"fooSym":     1,
				"barSym":     1,
				"barDir/bar": 1,
			},
			wantStats: &client.TreeStats{
				InputDirectories: 2,
				InputFiles:       2,
				InputSymlinks:    1,
				TotalInputBytes:  fooDg.Size + barDirDg.Size + barDg.Size,
			},
			treeOpts: &client.TreeSymlinkOpts{
				Preserved:                  true,
				FollowsTarget:              true,
				MaterializeOutsideExecRoot: true,
			},
		},
		{
			desc: "Intermediate directory relative symlink (preserved)",
			input: []*inputPath{
				{path: "foobarDir/foo", fileContents: fooBlob, isExecutable: true},
				{path: "foobarDir/bar", fileContents: barBlob},
				{path: "foobarSymDir", isSymlink: true, relSymlinkTarget: "foobarDir"},
			},
			spec: &command.InputSpec{
				Inputs:              []string{"foobarSymDir/foo", "foobarSymDir/bar"},
				InputNodeProperties: map[string]*cpb.NodeProperties{"foobarDir/foo": fooProperties},
			},
			rootDir: &repb.Directory{
				// foobarSymDir should not be a directory.
				Directories: []*repb.DirectoryNode{{Name: "foobarDir", Digest: foobarDirDgPb}},
				Symlinks:    []*repb.SymlinkNode{{Name: "foobarSymDir", Target: "foobarDir"}},
			},
			additionalBlobs: [][]byte{fooBlob, barBlob, foobarDirBlob},
			wantCacheCalls: map[string]int{
				"foobarDir/foo": 1, // 1 via the symlink
				"foobarDir/bar": 1,
				"foobarSymDir":  3, // 1 as added symlink ancestor, 2 as input ancestor
				// foobarDir should not have been followed as a target of the symlink since it was not an explicit input.
			},
			wantStats: &client.TreeStats{
				InputDirectories: 2, // Root and foobarDir
				InputFiles:       2,
				InputSymlinks:    1,
				TotalInputBytes:  fooDg.Size + barDg.Size + foobarDirDg.Size,
			},
			treeOpts: &client.TreeSymlinkOpts{
				Preserved:     true,
				FollowsTarget: true,
			},
		},
		{
			desc: "Intermediate directory relative symlink and input (preserved)",
			input: []*inputPath{
				{path: "foobarDir/foo", fileContents: fooBlob, isExecutable: true},
				{path: "foobarDir/bar", fileContents: barBlob},
				{path: "foobarSymDir", isSymlink: true, relSymlinkTarget: "foobarDir"},
			},
			spec: &command.InputSpec{
				// The directory symlink is also an input.
				// Must appear last to cover the corner case where it is seen as an ancestor before as an input.
				Inputs:              []string{"foobarSymDir/foo", "foobarSymDir/bar", "foobarSymDir"},
				InputNodeProperties: map[string]*cpb.NodeProperties{"foobarDir/foo": fooProperties},
			},
			rootDir: &repb.Directory{
				// foobarSymDir should not be a directory.
				Directories: []*repb.DirectoryNode{{Name: "foobarDir", Digest: foobarDirDgPb}},
				Symlinks:    []*repb.SymlinkNode{{Name: "foobarSymDir", Target: "foobarDir"}},
			},
			additionalBlobs: [][]byte{fooBlob, barBlob, foobarDirBlob},
			wantCacheCalls: map[string]int{
				"foobarDir/foo": 2, // 1 via the dir and 1 via the symlink
				"foobarDir/bar": 2,
				"foobarDir":     3, // 1 as input, 2 as input ancestor
				"foobarSymDir":  4, // 1 as input, 1 as added symlink ancestor, 2 as input ancestor
			},
			wantStats: &client.TreeStats{
				InputDirectories: 2, // Root and foobarDir
				InputFiles:       2,
				InputSymlinks:    1,
				TotalInputBytes:  fooDg.Size + barDg.Size + foobarDirDg.Size,
			},
			treeOpts: &client.TreeSymlinkOpts{
				Preserved:     true,
				FollowsTarget: true,
			},
		},
		{
			desc: "Intermediate directory relative symlink (preserved, materialize)",
			input: []*inputPath{
				{path: "../foobarDirOrig/foo", fileContents: fooBlob, isExecutable: true},
				{path: "../foobarDirOrig/bar", fileContents: barBlob},
				{path: "foobarDir", isSymlink: true, relSymlinkTarget: "../foobarDirOrig"},
			},
			spec: &command.InputSpec{
				Inputs:              []string{"foobarDir", "foobarDir/foo", "foobarDir/bar"},
				InputNodeProperties: map[string]*cpb.NodeProperties{"foobarDir/foo": fooProperties},
			},
			rootDir: &repb.Directory{
				// foobarDir should be materialized as a directory.
				Directories: []*repb.DirectoryNode{{Name: "foobarDir", Digest: foobarDirDgPb}},
			},
			additionalBlobs: [][]byte{fooBlob, barBlob, foobarDirBlob},
			wantCacheCalls: map[string]int{
				"foobarDir/foo": 2,
				"foobarDir/bar": 2,
				"foobarDir":     5, // 2 as input ancestor, 1 as input, 2 as nested input ancestor
			},
			wantStats: &client.TreeStats{
				InputDirectories: 2, // Root and foobarDir
				InputFiles:       2,
				TotalInputBytes:  fooDg.Size + barDg.Size + foobarDirDg.Size,
			},
			treeOpts: &client.TreeSymlinkOpts{
				Preserved:                  true,
				FollowsTarget:              true,
				MaterializeOutsideExecRoot: true,
			},
		},
		{
			desc: "Intermediate directory absolute symlink and input (preserved)",
			input: []*inputPath{
				{path: "foobarDir/foo", fileContents: fooBlob, isExecutable: true},
				{path: "foobarDir/bar", fileContents: barBlob},
				{path: "foobarSymDir", isSymlink: true, isAbsolute: true, relSymlinkTarget: "foobarDir"},
			},
			spec: &command.InputSpec{
				Inputs:              []string{"foobarSymDir", "foobarSymDir/foo", "foobarSymDir/bar"},
				InputNodeProperties: map[string]*cpb.NodeProperties{"foobarDir/foo": fooProperties},
			},
			rootDir: &repb.Directory{
				// foobarSymDir should not be a directory.
				Directories: []*repb.DirectoryNode{{Name: "foobarDir", Digest: foobarDirDgPb}},
				Symlinks:    []*repb.SymlinkNode{{Name: "foobarSymDir", Target: "foobarDir"}},
			},
			additionalBlobs: [][]byte{fooBlob, barBlob, foobarDirBlob},
			wantCacheCalls: map[string]int{
				"foobarDir/foo": 2, // 1 via the dir and 1 via the symlink
				"foobarDir/bar": 2,
				"foobarDir":     3, // 1 as target of the symlink, 2 as input ancestor
				"foobarSymDir":  4, // 1 as input, 1 as added symlink ancestor, 2 as input ancestor
			},
			wantStats: &client.TreeStats{
				InputDirectories: 2, // Root and foobarDir
				InputFiles:       2,
				InputSymlinks:    1,
				TotalInputBytes:  fooDg.Size + barDg.Size + foobarDirDg.Size,
			},
			treeOpts: &client.TreeSymlinkOpts{
				Preserved:     true,
				FollowsTarget: true,
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
				Inputs:              []string{"fooDir", "foobarDir"},
				InputNodeProperties: map[string]*cpb.NodeProperties{"fooDir/foo": fooProperties, "foobarDir/foo": fooProperties},
			},
			rootDir: &repb.Directory{Directories: []*repb.DirectoryNode{
				{Name: "fooDir", Digest: fooDirDgPb},
				{Name: "foobarDir", Digest: foobarDirDgPb},
			}},
			additionalBlobs: [][]byte{fooBlob, barBlob, fooDirBlob, foobarDirBlob},
			wantCacheCalls: map[string]int{
				"fooDir":        1,
				"fooDir/foo":    1,
				"foobarDir":     1,
				"foobarDir/foo": 1,
				"foobarDir/bar": 1,
			},
			wantStats: &client.TreeStats{
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
				Inputs:              []string{"fooDir1", "fooDir2"},
				InputNodeProperties: map[string]*cpb.NodeProperties{"fooDir1/foo": fooProperties, "fooDir2/foo": fooProperties},
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
			wantStats: &client.TreeStats{
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
				Inputs:              []string{"fooDirBlob", "fooDir"},
				InputNodeProperties: map[string]*cpb.NodeProperties{"fooDir/foo": fooProperties},
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
			wantStats: &client.TreeStats{
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
				InputNodeProperties: map[string]*cpb.NodeProperties{"fooDir/foo": fooProperties},
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
			wantStats: &client.TreeStats{
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
			wantStats: &client.TreeStats{
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
			wantStats: &client.TreeStats{
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
				InputNodeProperties: map[string]*cpb.NodeProperties{"fooDir/foo": fooProperties},
			},
			rootDir: &repb.Directory{Directories: []*repb.DirectoryNode{
				{Name: "barDir", Digest: barDirDgPb},
				{Name: "fooDir", Digest: fooDirDgPb},
			}},
			additionalBlobs: [][]byte{fooBlob, barBlob, fooDirBlob, barDirBlob},
			wantStats: &client.TreeStats{
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
				InputNodeProperties: map[string]*cpb.NodeProperties{"fooDir/foo": fooProperties},
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
			wantStats: &client.TreeStats{
				InputDirectories: 3,
				InputFiles:       2,
				TotalInputBytes:  fooDg.Size + fooDirDg.Size + barDg.Size + barDirDg.Size,
			},
		},
		{
			desc: "Virtual inputs as ancestors of physical inputs",
			input: []*inputPath{
				{path: "fooDir/foo", fileContents: fooBlob, isExecutable: true},
				{path: "barDir/bar", fileContents: barBlob},
			},
			spec: &command.InputSpec{
				Inputs: []string{"fooDir", "barDir"},
				VirtualInputs: []*command.VirtualInput{
					&command.VirtualInput{Path: "barDir", IsEmptyDirectory: true},
				},
				InputNodeProperties: map[string]*cpb.NodeProperties{"fooDir/foo": fooProperties},
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
			wantStats: &client.TreeStats{
				InputDirectories: 3,
				InputFiles:       2,
				TotalInputBytes:  fooDg.Size + fooDirDg.Size + barDg.Size + barDirDg.Size,
			},
		},
		{
			desc: "Virtual inputs as children of physical inputs",
			input: []*inputPath{
				{path: "fooDir/foo", fileContents: fooBlob, isExecutable: true},
				{path: "bar", fileContents: barBlob},
			},
			spec: &command.InputSpec{
				Inputs: []string{"fooDir", "bar"},
				VirtualInputs: []*command.VirtualInput{
					&command.VirtualInput{Path: "bar/baz", IsEmptyDirectory: true},
				},
				InputNodeProperties: map[string]*cpb.NodeProperties{"fooDir/foo": fooProperties},
			},
			rootDir: &repb.Directory{
				Directories: []*repb.DirectoryNode{
					{Name: "bar", Digest: vBarDirDgPb},
					{Name: "fooDir", Digest: fooDirDgPb},
				},
				Files: []*repb.FileNode{{Name: "bar", Digest: barDgPb}},
			},
			additionalBlobs: [][]byte{fooBlob, barBlob, fooDirBlob, vBarDirBlob, []byte{}},
			wantCacheCalls: map[string]int{
				"fooDir":     1,
				"fooDir/foo": 1,
				"bar":        1,
			},
			wantStats: &client.TreeStats{
				InputDirectories: 4,
				InputFiles:       2,
				TotalInputBytes:  fooDg.Size + fooDirDg.Size + barDg.Size + vBarDirDg.Size,
			},
		},
		{
			desc: "Virtual inputs as ancestors of virtual inputs",
			input: []*inputPath{
				{path: "fooDir/foo", fileContents: fooBlob, isExecutable: true},
				{path: "bar", fileContents: barBlob},
			},
			spec: &command.InputSpec{
				Inputs: []string{"fooDir", "bar"},
				VirtualInputs: []*command.VirtualInput{
					&command.VirtualInput{Path: "bar/baz", IsEmptyDirectory: true},
					&command.VirtualInput{Path: "bar", IsEmptyDirectory: true},
				},
				InputNodeProperties: map[string]*cpb.NodeProperties{"fooDir/foo": fooProperties},
			},
			rootDir: &repb.Directory{
				Directories: []*repb.DirectoryNode{
					{Name: "bar", Digest: vBarDirDgPb},
					{Name: "fooDir", Digest: fooDirDgPb},
				},
				Files: []*repb.FileNode{{Name: "bar", Digest: barDgPb}},
			},
			additionalBlobs: [][]byte{fooBlob, barBlob, fooDirBlob, vBarDirBlob, []byte{}},
			wantCacheCalls: map[string]int{
				"fooDir":     1,
				"fooDir/foo": 1,
				"bar":        1,
			},
			wantStats: &client.TreeStats{
				InputDirectories: 4,
				InputFiles:       2,
				TotalInputBytes:  fooDg.Size + fooDirDg.Size + barDg.Size + vBarDirDg.Size,
			},
		},
		{
			desc: "Normalizing virtual inputs paths",
			spec: &command.InputSpec{
				VirtualInputs: []*command.VirtualInput{
					&command.VirtualInput{Path: "//fooDir/../fooDir/foo", Contents: fooBlob, IsExecutable: true},
					&command.VirtualInput{Path: "barDir///bar", Contents: barBlob},
				},
				InputNodeProperties: map[string]*cpb.NodeProperties{"fooDir/foo": fooProperties},
			},
			rootDir: &repb.Directory{Directories: []*repb.DirectoryNode{
				{Name: "barDir", Digest: barDirDgPb},
				{Name: "fooDir", Digest: fooDirDgPb},
			}},
			additionalBlobs: [][]byte{fooBlob, barBlob, fooDirBlob, barDirBlob},
			wantStats: &client.TreeStats{
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
				InputNodeProperties: map[string]*cpb.NodeProperties{
					"g/foo": fooProperties,
					"h/foo": fooProperties,
					"i/foo": fooProperties,
				},
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
			wantStats: &client.TreeStats{
				InputDirectories: 7,
				InputFiles:       12,
				TotalInputBytes:  12*fooDg.Size + 3*fooDirDg.Size + 3*barDirDg.Size,
			},
		},
	}

	for _, tc := range tests {
		root := t.TempDir()
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

			e, cleanup := fakes.NewTestEnv(t)
			defer cleanup()
			tc.treeOpts.Apply(e.Client.GrpcClient)

			gotRootDg, inputs, stats, err := e.Client.GrpcClient.ComputeMerkleTree(context.Background(), root, "", "", tc.spec, cache)
			if err != nil {
				t.Errorf("ComputeMerkleTree(...) = gave error %q, want success", err)
			}
			for _, ue := range inputs {
				ch, err := chunker.New(ue, false, int(e.Client.GrpcClient.ChunkMaxSize))
				if err != nil {
					t.Fatalf("chunker.New(ue): failed to create chunker from UploadEntry: %v", err)
				}
				blob, err := ch.FullData()
				if err != nil {
					t.Errorf("chunker %v FullData() returned error %v", ch, err)
				}
				gotBlobs[ue.Digest] = blob
			}
			if diff := cmp.Diff(rootDg, gotRootDg); diff != "" {
				t.Errorf("ComputeMerkleTree(...) gave diff (-want +got) on root:\n%s", diff)
				if gotRootBlob, ok := gotBlobs[gotRootDg]; ok {
					gotRoot := new(repb.Directory)
					if err := proto.Unmarshal(gotRootBlob, gotRoot); err != nil {
						t.Errorf("  When unpacking root blob, got error: %s", err)
					} else {
						diff := cmp.Diff(tc.rootDir, gotRoot, protocmp.Transform())
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
		desc     string
		input    []*inputPath
		spec     *command.InputSpec
		treeOpts *client.TreeSymlinkOpts
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
		{
			desc: "Preserved symlink escaping exec root",
			input: []*inputPath{
				{path: "../foo", fileContents: fooBlob, isExecutable: true},
				{path: "escapingFoo", isSymlink: true, relSymlinkTarget: "../foo"},
			},
			spec: &command.InputSpec{
				Inputs: []string{"escapingFoo"},
			},
			treeOpts: &client.TreeSymlinkOpts{
				Preserved: true,
			},
		},
		{
			desc: "Materialization of dangling symlink pointing outside exec root fails",
			input: []*inputPath{
				{path: "danglingSym", isSymlink: true, relSymlinkTarget: "../doesNotExist"},
			},
			spec: &command.InputSpec{
				Inputs: []string{"danglingSym"},
			},
			treeOpts: &client.TreeSymlinkOpts{
				MaterializeOutsideExecRoot: true,
				Preserved:                  true,
			},
		},
	}

	for _, tc := range tests {
		root := t.TempDir()
		if err := construct(root, tc.input); err != nil {
			t.Fatalf("failed to construct input dir structure: %v", err)
		}
		t.Run(tc.desc, func(t *testing.T) {
			e, cleanup := fakes.NewTestEnv(t)
			defer cleanup()
			tc.treeOpts.Apply(e.Client.GrpcClient)

			if _, _, _, err := e.Client.GrpcClient.ComputeMerkleTree(context.Background(), root, "", "", tc.spec, filemetadata.NewNoopCache()); err == nil {
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

	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()

	outputs, err := e.Client.GrpcClient.FlattenTree(tree, "x")
	if err != nil {
		t.Errorf("FlattenTree gave error %v", err)
	}
	wantOutputs := map[string]*client.TreeOutput{
		"x/baz":     &client.TreeOutput{Digest: bazDigest},
		"x/a/b/c":   &client.TreeOutput{IsEmptyDirectory: true, Digest: digest.Empty},
		"x/a/b/foo": &client.TreeOutput{Digest: fooDigest},
		"x/a/b/bar": &client.TreeOutput{Digest: barDigest, IsExecutable: true},
		"x/b/c":     &client.TreeOutput{IsEmptyDirectory: true, Digest: digest.Empty},
		"x/b/foo":   &client.TreeOutput{Digest: fooDigest},
		"x/b/bar":   &client.TreeOutput{Digest: barDigest, IsExecutable: true},
		"x/c":       &client.TreeOutput{IsEmptyDirectory: true, Digest: digest.Empty},
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
		wd             string
		paths          []string
		nodeProperties map[string]*cpb.NodeProperties
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
			paths:          []string{"foo", "bar"},
			nodeProperties: map[string]*cpb.NodeProperties{"foo": fooProperties},
			wantBlobs:      [][]byte{fooBlob},
			wantResult: &repb.ActionResult{
				OutputFiles: []*repb.OutputFile{&repb.OutputFile{Path: "foo", Digest: fooDgPb, IsExecutable: true, NodeProperties: command.NodePropertiesToAPI(fooProperties)}},
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
			paths:          []string{"foo", "bar"},
			nodeProperties: map[string]*cpb.NodeProperties{"foo": fooProperties},
			wantBlobs:      [][]byte{fooBlob, barBlob},
			wantResult: &repb.ActionResult{
				OutputFiles: []*repb.OutputFile{
					// Note the outputs are not sorted.
					&repb.OutputFile{Path: "foo", Digest: fooDgPb, IsExecutable: true, NodeProperties: command.NodePropertiesToAPI(fooProperties)},
					&repb.OutputFile{Path: "bar", Digest: barDgPb},
				},
			},
			wantCacheCalls: map[string]int{
				"bar": 1,
				"foo": 1,
			},
		},
		{
			desc: "Two files under working dir",
			input: []*inputPath{
				{path: "wd/foo", fileContents: fooBlob, isExecutable: true},
				{path: "bar", fileContents: barBlob},
			},
			paths:          []string{"foo", "../bar"},
			nodeProperties: map[string]*cpb.NodeProperties{"foo": fooProperties},
			wd:             "wd",
			wantBlobs:      [][]byte{fooBlob, barBlob},
			wantResult: &repb.ActionResult{
				OutputFiles: []*repb.OutputFile{
					// Note the outputs are not sorted.
					&repb.OutputFile{Path: "foo", Digest: fooDgPb, IsExecutable: true, NodeProperties: command.NodePropertiesToAPI(fooProperties)},
					&repb.OutputFile{Path: "../bar", Digest: barDgPb},
				},
			},
			wantCacheCalls: map[string]int{
				"bar":    1,
				"wd/foo": 1,
			},
		},
		{
			desc: "Symlink",
			input: []*inputPath{
				{path: "bar", fileContents: barBlob},
				{path: "dir1/dir2/bar", isSymlink: true, relSymlinkTarget: "../../bar"},
			},
			paths:     []string{"dir1/dir2/bar"},
			wantBlobs: [][]byte{barBlob},
			wantResult: &repb.ActionResult{
				OutputFiles: []*repb.OutputFile{
					&repb.OutputFile{Path: "dir1/dir2/bar", Digest: barDgPb},
				},
			},
			wantCacheCalls: map[string]int{
				"dir1/dir2/bar": 1,
			},
		},
		{
			desc: "Duplicate file contents",
			input: []*inputPath{
				{path: "foo", fileContents: fooBlob, isExecutable: true},
				{path: "bar", fileContents: fooBlob},
			},
			paths:          []string{"foo", "bar"},
			nodeProperties: map[string]*cpb.NodeProperties{"foo": fooProperties},
			wantBlobs:      [][]byte{fooBlob},
			wantResult: &repb.ActionResult{
				OutputFiles: []*repb.OutputFile{
					// Note the outputs are not sorted.
					&repb.OutputFile{Path: "foo", Digest: fooDgPb, IsExecutable: true, NodeProperties: command.NodePropertiesToAPI(fooProperties)},
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
		root := t.TempDir()
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
			e, cleanup := fakes.NewTestEnv(t)
			defer cleanup()

			inputs, gotResult, err := e.Client.GrpcClient.ComputeOutputsToUpload(root, tc.wd, tc.paths, cache, command.UnspecifiedSymlinkBehavior, tc.nodeProperties)
			if err != nil {
				t.Errorf("ComputeOutputsToUpload(...) = gave error %v, want success", err)
			}
			for _, ue := range inputs {
				ch, err := chunker.New(ue, false, int(e.Client.GrpcClient.ChunkMaxSize))
				if err != nil {
					t.Fatalf("chunker.New(ue): failed to create chunker from UploadEntry: %v", err)
				}
				blob, err := ch.FullData()
				if err != nil {
					t.Errorf("chunker %v FullData() returned error %v", ch, err)
				}
				gotBlobs[ue.Digest] = blob
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
	/*
		We want to test that the directory tree is built consistently and in lexical order according to it's path.
		Building directory with structure:

		dirA
		  -> dirC
		  -> dirF
		dirB
		  -> dirD
		  -> dirE

		We should get a Tree like
		{
			root: <root node>
			children: [dirA, dirC, dirF, dirB, dirD, dirE]
		}
	*/

	dirA := &repb.Directory{
		Directories: []*repb.DirectoryNode{
			{Name: "dirC", Digest: barDirDgPb},
			{Name: "dirF", Digest: fooDirDgPb},
		},
	}
	dirABlob := mustMarshal(dirA)
	dirADg := digest.NewFromBlob(dirABlob)

	dirB := &repb.Directory{
		Directories: []*repb.DirectoryNode{
			{Name: "dirD", Digest: bazDirDgPb},
			{Name: "dirE", Digest: foobarDirDgPb},
		},
	}
	dirBBlob := mustMarshal(dirB)
	dirBDg := digest.NewFromBlob(dirBBlob)

	tests := []struct {
		desc           string
		input          []*inputPath
		nodeProperties map[string]*cpb.NodeProperties
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
			nodeProperties: map[string]*cpb.NodeProperties{"foo": fooProperties},
			wantBlobs:      [][]byte{fooBlob, barBlob},
			wantTreeRoot:   foobarDir,
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
			nodeProperties: map[string]*cpb.NodeProperties{"foo": fooProperties},
			wantBlobs:      [][]byte{fooBlob, fooBlob},
			wantTreeRoot: &repb.Directory{Files: []*repb.FileNode{
				{Name: "bar", Digest: fooDgPb, IsExecutable: true},
				{Name: "foo", Digest: fooDgPb, IsExecutable: true, NodeProperties: command.NodePropertiesToAPI(fooProperties)},
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
			nodeProperties: map[string]*cpb.NodeProperties{"dir1/foo": fooProperties, "dir2/foo": fooProperties},
			wantBlobs:      [][]byte{fooBlob, fooDirBlob},
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
		{
			desc: "Directory tree preserves lexicographical order",
			input: []*inputPath{
				{path: "a/b/fooDir/dirA/dirC/bar", fileContents: barBlob},
				{path: "a/b/fooDir/dirA/dirF/foo", fileContents: fooBlob, isExecutable: true},
				{path: "a/b/fooDir/dirB/dirD/baz", fileContents: bazBlob},
				{path: "a/b/fooDir/dirB/dirE/foo", fileContents: fooBlob, isExecutable: true},
				{path: "a/b/fooDir/dirB/dirE/bar", fileContents: barBlob},
			},
			nodeProperties: map[string]*cpb.NodeProperties{"dirA/dirF/foo": fooProperties, "dirB/dirE/foo": fooProperties},
			wantBlobs:      [][]byte{fooBlob, barBlob, fooDirBlob, barDirBlob, dirABlob, dirBBlob, bazDirBlob, bazBlob, foobarDirBlob},
			wantTreeRoot: &repb.Directory{Directories: []*repb.DirectoryNode{
				{Name: "dirA", Digest: dirADg.ToProto()},
				{Name: "dirB", Digest: dirBDg.ToProto()},
			}},
			wantTreeChildren: []*repb.Directory{dirA, barDir, fooDir, dirB, bazDir, foobarDir},
			wantCacheCalls: map[string]int{
				"a/b/fooDir":               2,
				"a/b/fooDir/dirA":          1,
				"a/b/fooDir/dirA/dirC":     1,
				"a/b/fooDir/dirA/dirC/bar": 1,
				"a/b/fooDir/dirA/dirF":     1,
				"a/b/fooDir/dirA/dirF/foo": 1,
				"a/b/fooDir/dirB":          1,
				"a/b/fooDir/dirB/dirD":     1,
				"a/b/fooDir/dirB/dirD/baz": 1,
				"a/b/fooDir/dirB/dirE":     1,
				"a/b/fooDir/dirB/dirE/foo": 1,
				"a/b/fooDir/dirB/dirE/bar": 1,
			},
		},
	}

	for _, tc := range tests {
		root := t.TempDir()
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
			e, cleanup := fakes.NewTestEnv(t)
			defer cleanup()

			inputs, gotResult, err := e.Client.GrpcClient.ComputeOutputsToUpload(root, "", []string{"a/b/fooDir"}, cache, command.UnspecifiedSymlinkBehavior, tc.nodeProperties)
			if err != nil {
				t.Fatalf("ComputeOutputsToUpload(...) = gave error %v, want success", err)
			}
			for _, ue := range inputs {
				ch, err := chunker.New(ue, false, int(e.Client.GrpcClient.ChunkMaxSize))
				if err != nil {
					t.Fatalf("chunker.New(ue): failed to create chunker from UploadEntry: %v", err)
				}
				blob, err := ch.FullData()
				if err != nil {
					t.Errorf("chunker %v FullData() returned error %v", ch, err)
				}
				gotBlobs[ue.Digest] = blob
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

			digests := make(map[string]bool)
			digests[gotResult.OutputDirectories[0].TreeDigest.Hash] = true

			for i := 0; i < 5; i++ {
				_, gotResult, err = e.Client.GrpcClient.ComputeOutputsToUpload(root, "", []string{"a/b/fooDir"}, cache, command.UnspecifiedSymlinkBehavior, tc.nodeProperties)
				if err != nil {
					t.Fatalf("ComputeOutputsToUpload(...) = gave error %v, want success", err)
				}
				digests[gotResult.OutputDirectories[0].TreeDigest.Hash] = true
			}
			if len(digests) != 1 {
				dgList := []string{}
				for d := range digests {
					dgList = append(dgList, d)
				}
				t.Fatalf("ComputeOutputsToUpload(...) directory digests are not consistent got:%v", dgList)
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

func randomBytes(randGen *rand.Rand, n int) []byte {
	b := make([]byte, n)
	randGen.Read(b)
	return b
}

func BenchmarkComputeMerkleTree(b *testing.B) {
	e, cleanup := fakes.NewTestEnv(b)
	defer cleanup()

	randGen := rand.New(rand.NewSource(0))
	construct(e.ExecRoot, []*inputPath{
		{path: "a", fileContents: randomBytes(randGen, 2048)},
		{path: "b", fileContents: randomBytes(randGen, 9999)},
		{path: "c", fileContents: randomBytes(randGen, 1024)},
		{path: "d/a", fileContents: randomBytes(randGen, 4444)},
		{path: "d/b", fileContents: randomBytes(randGen, 7491)},
		{path: "d/c", emptyDir: true},
		{path: "d/d/a", fileContents: randomBytes(randGen, 5912)},
		{path: "d/d/b", fileContents: randomBytes(randGen, 9157)},
		{path: "d/d/c", isSymlink: true, relSymlinkTarget: "../../b"},
		{path: "d/d/d", fileContents: randomBytes(randGen, 5381)},
	})

	inputSpec := &command.InputSpec{
		Inputs: []string{"a", "b", "c", "d/a", "d/b", "d/c", "d/d/a", "d/d/b", "d/d/c", "d/d/d"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fmc := filemetadata.NewSingleFlightCache()
		_, _, _, err := e.Client.GrpcClient.ComputeMerkleTree(context.Background(), e.ExecRoot, "", "", inputSpec, fmc)
		if err != nil {
			b.Errorf("Failed to compute merkle tree: %v", err)
		}
	}
}
