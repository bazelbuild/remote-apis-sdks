package client_test

import (
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/client"
	"github.com/bazelbuild/remote-apis-sdks/go/digest"
	"github.com/golang/protobuf/proto"
	"github.com/google/go-cmp/cmp"
	"github.com/kylelemons/godebug/pretty"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

func TestBuildTree(t *testing.T) {
	t.Parallel()
	foo, bar, baz := []byte("f"), []byte("b"), []byte("z")
	tests := []struct {
		desc  string
		input map[string][]byte
		want  *client.FileTree
	}{
		{
			desc:  "empty tree",
			input: nil,
			want:  &client.FileTree{},
		},
		{
			desc: "flat dir",
			input: map[string][]byte{
				"foo": foo,
				"bar": bar,
				"baz": baz,
			},
			want: &client.FileTree{Files: map[string][]byte{
				"foo": foo,
				"bar": bar,
				"baz": baz,
			}},
		},
		{
			desc: "all different levels",
			input: map[string][]byte{
				"foo":     foo,
				"a/bar":   bar,
				"a/b/baz": baz,
			},
			want: &client.FileTree{
				Files: map[string][]byte{"foo": foo},
				Dirs: map[string]*client.FileTree{"a": {
					Files: map[string][]byte{"bar": bar},
					Dirs: map[string]*client.FileTree{"b": {
						Files: map[string][]byte{"baz": baz},
					}},
				}},
			},
		},
		{
			desc: "different dirs",
			input: map[string][]byte{
				"a/foo": foo,
				"b/bar": bar,
				"c/baz": baz,
			},
			want: &client.FileTree{
				Dirs: map[string]*client.FileTree{
					"a": &client.FileTree{Files: map[string][]byte{"foo": foo}},
					"b": &client.FileTree{Files: map[string][]byte{"bar": bar}},
					"c": &client.FileTree{Files: map[string][]byte{"baz": baz}},
				},
			},
		},
		{
			desc: "empty filenames",
			input: map[string][]byte{
				"":       foo,
				"a/":     bar,
				"b//baz": baz,
			},
			want: &client.FileTree{
				Files: map[string][]byte{"": foo},
				Dirs: map[string]*client.FileTree{
					"a": &client.FileTree{Files: map[string][]byte{"": bar}},
					"b": &client.FileTree{Dirs: map[string]*client.FileTree{"": {Files: map[string][]byte{"baz": baz}}}},
				},
			},
		},
		{
			desc: "duplicate file/dir",
			input: map[string][]byte{
				"foo":     foo,
				"foo/bar": bar,
				"foo/baz": baz,
			},
			want: &client.FileTree{
				Files: map[string][]byte{"foo": foo},
				Dirs: map[string]*client.FileTree{"foo": {Files: map[string][]byte{
					"bar": bar,
					"baz": baz,
				}}},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			got := client.BuildTree(tc.input)
			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("client.BuildTree(%+v) gave diff (-want +got):\n%s", tc.input, diff)
			}
		})
	}
}

func MustMarshal(t *testing.T, p proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(p)
	if err != nil {
		t.Fatalf("error marshalling proto during test setup: %s", err)
	}
	return b
}

func TestPackageTree(t *testing.T) {
	fooBlob, barBlob := []byte("foo"), []byte("bar")
	fooDg, barDg := digest.FromBlob(fooBlob), digest.FromBlob(barBlob)

	fooDir := &repb.Directory{Files: []*repb.FileNode{{Name: "foo", Digest: fooDg, IsExecutable: true}}}
	barDir := &repb.Directory{Files: []*repb.FileNode{{Name: "bar", Digest: barDg, IsExecutable: true}}}
	foobarDir := &repb.Directory{Files: []*repb.FileNode{
		{Name: "bar", Digest: barDg, IsExecutable: true},
		{Name: "foo", Digest: fooDg, IsExecutable: true},
	}}

	fooDirBlob, barDirBlob, foobarDirBlob := MustMarshal(t, fooDir), MustMarshal(t, barDir), MustMarshal(t, foobarDir)
	fooDirDg, barDirDg, foobarDirDg := digest.FromBlob(fooDirBlob), digest.FromBlob(barDirBlob), digest.FromBlob(foobarDirBlob)

	tests := []struct {
		desc  string
		input *client.FileTree
		// The expected results are calculated by marshalling rootDir, then expecting the result to be
		// the digest of rootDir plus a map containing rootDir's marshalled blob and all the additional
		// blobs.
		rootDir         *repb.Directory
		additionalBlobs [][]byte
	}{
		{
			desc:            "Empty directory",
			input:           &client.FileTree{},
			rootDir:         &repb.Directory{},
			additionalBlobs: nil,
		},
		{
			desc:            "Files at root",
			input:           &client.FileTree{Files: map[string][]byte{"foo": fooBlob, "bar": barBlob}},
			rootDir:         foobarDir,
			additionalBlobs: [][]byte{fooBlob, barBlob},
		},
		{
			desc: "File below root",
			input: &client.FileTree{Dirs: map[string]*client.FileTree{
				"fooDir": {Files: map[string][]byte{"foo": fooBlob}},
				"barDir": {Files: map[string][]byte{"bar": barBlob}},
			}},
			rootDir: &repb.Directory{Directories: []*repb.DirectoryNode{
				{Name: "barDir", Digest: barDirDg},
				{Name: "fooDir", Digest: fooDirDg},
			}},
			additionalBlobs: [][]byte{fooBlob, barBlob, fooDirBlob, barDirBlob},
		},
		{
			desc: "De-duplicating files",
			input: &client.FileTree{Dirs: map[string]*client.FileTree{
				"fooDir":    {Files: map[string][]byte{"foo": fooBlob}},
				"foobarDir": {Files: map[string][]byte{"foo": fooBlob, "bar": barBlob}},
			}},
			rootDir: &repb.Directory{Directories: []*repb.DirectoryNode{
				{Name: "fooDir", Digest: fooDirDg},
				{Name: "foobarDir", Digest: foobarDirDg},
			}},
			additionalBlobs: [][]byte{fooBlob, barBlob, fooDirBlob, foobarDirBlob},
		},
		{
			desc: "De-duplicating directories",
			input: &client.FileTree{Dirs: map[string]*client.FileTree{
				"fooDir1": {Files: map[string][]byte{"foo": fooBlob}},
				"fooDir2": {Files: map[string][]byte{"foo": fooBlob}},
			}},
			rootDir: &repb.Directory{Directories: []*repb.DirectoryNode{
				{Name: "fooDir1", Digest: fooDirDg},
				{Name: "fooDir2", Digest: fooDirDg},
			}},
			additionalBlobs: [][]byte{fooBlob, fooDirBlob},
		},
		{
			desc: "De-duplicating files with directories",
			input: &client.FileTree{
				Files: map[string][]byte{"fooDirBlob": fooDirBlob},
				Dirs:  map[string]*client.FileTree{"fooDir": {Files: map[string][]byte{"foo": fooBlob}}},
			},
			rootDir: &repb.Directory{
				Directories: []*repb.DirectoryNode{{Name: "fooDir", Digest: fooDirDg}},
				Files:       []*repb.FileNode{{Name: "fooDirBlob", Digest: fooDirDg, IsExecutable: true}},
			},
			additionalBlobs: [][]byte{fooBlob, fooDirBlob},
		},
		{
			// NOTE: The use of maps in client.FileTree means that the traversal order is unstable. The
			// outputs are required to be in lexicographic order, so if client.PackageTree is not sorting
			// correctly, this test will fail (except in the rare occasion the traversal order is
			// coincidentally correct).
			desc: "Correct sorting",
			input: &client.FileTree{
				Files: map[string][]byte{
					"a": fooBlob,
					"b": fooBlob,
					"c": fooBlob,
					"d": fooBlob,
					"e": fooBlob,
					"f": fooBlob,
					"g": fooBlob,
					"h": fooBlob,
					"i": fooBlob,
					"j": fooBlob,
				},
				Dirs: map[string]*client.FileTree{
					"k": {Files: map[string][]byte{"foo": fooBlob}},
					"l": {Files: map[string][]byte{"foo": fooBlob}},
					"m": {Files: map[string][]byte{"foo": fooBlob}},
					"n": {Files: map[string][]byte{"foo": fooBlob}},
					"o": {Files: map[string][]byte{"foo": fooBlob}},
					"p": {Files: map[string][]byte{"foo": fooBlob}},
					"q": {Files: map[string][]byte{"foo": fooBlob}},
					"r": {Files: map[string][]byte{"foo": fooBlob}},
					"s": {Files: map[string][]byte{"foo": fooBlob}},
					"t": {Files: map[string][]byte{"foo": fooBlob}},
				},
			},
			rootDir: &repb.Directory{
				Files: []*repb.FileNode{
					{Name: "a", Digest: fooDg, IsExecutable: true},
					{Name: "b", Digest: fooDg, IsExecutable: true},
					{Name: "c", Digest: fooDg, IsExecutable: true},
					{Name: "d", Digest: fooDg, IsExecutable: true},
					{Name: "e", Digest: fooDg, IsExecutable: true},
					{Name: "f", Digest: fooDg, IsExecutable: true},
					{Name: "g", Digest: fooDg, IsExecutable: true},
					{Name: "h", Digest: fooDg, IsExecutable: true},
					{Name: "i", Digest: fooDg, IsExecutable: true},
					{Name: "j", Digest: fooDg, IsExecutable: true},
				},
				Directories: []*repb.DirectoryNode{
					{Name: "k", Digest: fooDirDg},
					{Name: "l", Digest: fooDirDg},
					{Name: "m", Digest: fooDirDg},
					{Name: "n", Digest: fooDirDg},
					{Name: "o", Digest: fooDirDg},
					{Name: "p", Digest: fooDirDg},
					{Name: "q", Digest: fooDirDg},
					{Name: "r", Digest: fooDirDg},
					{Name: "s", Digest: fooDirDg},
					{Name: "t", Digest: fooDirDg},
				},
			},
			additionalBlobs: [][]byte{fooBlob, fooDirBlob},
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			wantBlobs := make(map[digest.Key][]byte)
			rootBlob := MustMarshal(t, tc.rootDir)
			rootDg := digest.FromBlob(rootBlob)
			wantBlobs[digest.ToKey(rootDg)] = rootBlob
			for _, b := range tc.additionalBlobs {
				wantBlobs[digest.ToKey(digest.FromBlob(b))] = b
			}

			gotRootDg, gotBlobs, err := client.PackageTree(tc.input)
			if err != nil {
				t.Fatalf("client.PackageTree(...) = gave error %s, want success", err)
			}
			if diff := cmp.Diff(rootDg, gotRootDg); diff != "" {
				t.Errorf("client.PackageTree(...) gave diff (want -> got) on root:\n%s", diff)
				if gotRootBlob, ok := gotBlobs[digest.ToKey(gotRootDg)]; ok {
					gotRoot := new(repb.Directory)
					if err := proto.Unmarshal(gotRootBlob, gotRoot); err != nil {
						t.Errorf("  When unpacking root blob, got error: %s", err)
					} else {
						diff := cmp.Diff(tc.rootDir, gotRoot)
						t.Errorf("  Diff between unpacked roots (want -> got):\n%s", diff)
					}
				} else {
					t.Logf("  Root digest gotten not present in blobs map")
				}
			}
			if diff := cmp.Diff(wantBlobs, gotBlobs); diff != "" {
				t.Errorf("client.PackageTree(...) gave diff (want -> got) on blobs:\n%s", diff)
			}
		})
	}
}

func TestPackageTreeErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		desc  string
		input *client.FileTree
	}{
		{
			desc:  "nil root",
			input: nil,
		},
		{
			desc:  "empty filename at root",
			input: &client.FileTree{Files: map[string][]byte{"": nil}},
		},
		{
			desc:  "empty directory name at root",
			input: &client.FileTree{Dirs: map[string]*client.FileTree{"": {}}},
		},
		{
			desc: "name collision at root",
			input: &client.FileTree{
				Files: map[string][]byte{"owch": nil},
				Dirs:  map[string]*client.FileTree{"owch": {}},
			},
		},
		{
			desc:  "nil child",
			input: &client.FileTree{Dirs: map[string]*client.FileTree{"foo": nil}},
		},
		{
			desc:  "empty filename in child",
			input: &client.FileTree{Dirs: map[string]*client.FileTree{"foo": {Files: map[string][]byte{"": nil}}}},
		},
		{
			desc:  "empty directory name in child",
			input: &client.FileTree{Dirs: map[string]*client.FileTree{"foo": {Dirs: map[string]*client.FileTree{"": {}}}}},
		},
		{
			desc: "collision in child",
			input: &client.FileTree{Dirs: map[string]*client.FileTree{"foo": {
				Files: map[string][]byte{"owch": nil},
				Dirs:  map[string]*client.FileTree{"owch": {}},
			}}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			_, _, err := client.PackageTree(tc.input)
			if err == nil {
				t.Errorf("client.PackageTree(%s) succeeded, want error", pretty.Sprint(tc.input))
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
	cDigest := digest.TestFromProto(dirC)
	dirB := &repb.Directory{
		Files: []*repb.FileNode{
			{Name: "foo", Digest: fooDigest, IsExecutable: false},
			{Name: "bar", Digest: barDigest, IsExecutable: true},
		},
		Directories: []*repb.DirectoryNode{
			{Name: "c", Digest: cDigest},
		},
	}
	bDigest := digest.TestFromProto(dirB)
	dirA := &repb.Directory{
		Directories: []*repb.DirectoryNode{
			{Name: "b", Digest: bDigest},
		}}
	aDigest := digest.TestFromProto(dirA)
	root := &repb.Directory{
		Directories: []*repb.DirectoryNode{
			{Name: "a", Digest: aDigest},
			{Name: "b", Digest: bDigest},
			{Name: "c", Digest: cDigest},
		},
		Files: []*repb.FileNode{
			{Name: "baz", Digest: bazDigest},
		},
	}
	tree := &repb.Tree{
		Root:     root,
		Children: []*repb.Directory{dirA, dirB, dirC},
	}
	outputs, err := client.FlattenTree(tree, "x")
	if err != nil {
		t.Errorf("FlattenTree gave error %v", err)
	}
	wantOutputs := map[string]*client.Output{
		"x/baz":     &client.Output{Digest: digest.ToKey(bazDigest)},
		"x/a/b/foo": &client.Output{Digest: digest.ToKey(fooDigest)},
		"x/a/b/bar": &client.Output{Digest: digest.ToKey(barDigest), IsExecutable: true},
		"x/b/foo":   &client.Output{Digest: digest.ToKey(fooDigest)},
		"x/b/bar":   &client.Output{Digest: digest.ToKey(barDigest), IsExecutable: true},
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
