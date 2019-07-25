package client_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/client"
	"github.com/bazelbuild/remote-apis-sdks/go/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/command"
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

func TestBuildTreeFromInputs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		desc  string
		input []*inputPath
		spec  *command.InputSpec
		want  *client.FileTree
	}{
		{
			desc:  "empty",
			input: nil,
			spec:  &command.InputSpec{},
			want:  &client.FileTree{},
		},
		{
			desc: "nested",
			input: []*inputPath{
				{path: "a/foo.txt", fileContents: []byte("foo")},
				{path: "a/b/bar.txt", fileContents: []byte("bar")},
				{path: "a/b/baz.txt", fileContents: []byte("baz")},
				{path: "b/bla.txt", fileContents: []byte("bla")},
				{path: "bla.txt", fileContents: []byte("bla")},
			},
			spec: &command.InputSpec{
				Inputs: []string{"a", "b", "bla.txt"},
			},
			want: &client.FileTree{
				Files: map[string][]byte{"bla.txt": []byte("bla")},
				Dirs: map[string]*client.FileTree{
					"a": &client.FileTree{
						Files: map[string][]byte{"foo.txt": []byte("foo")},
						Dirs: map[string]*client.FileTree{
							"b": &client.FileTree{
								Files: map[string][]byte{
									"bar.txt": []byte("bar"),
									"baz.txt": []byte("baz"),
								},
							},
						},
					},
					"b": &client.FileTree{Files: map[string][]byte{"bla.txt": []byte("bla")}},
				},
			},
		},
		{
			desc: "file_absolute_symlink",
			input: []*inputPath{
				{path: "a/foo.txt", fileContents: []byte("foo")},
				// The absolute symlink target will actually point to the absolute path of a/foo.txt.
				{path: "bla.txt", isSymlink: true, isAbsolute: true, symlinkTarget: "a/foo.txt"},
			},
			spec: &command.InputSpec{
				Inputs: []string{"a/foo.txt", "bla.txt"},
			},
			want: &client.FileTree{
				Files: map[string][]byte{"bla.txt": []byte("foo")},
				Dirs: map[string]*client.FileTree{
					"a": &client.FileTree{
						Files: map[string][]byte{"foo.txt": []byte("foo")},
					},
				},
			},
		},
		{
			desc: "file_relative_symlink",
			input: []*inputPath{
				{path: "a/foo.txt", fileContents: []byte("foo")},
				{path: "bla.txt", isSymlink: true, symlinkTarget: "a/foo.txt"},
			},
			spec: &command.InputSpec{
				Inputs: []string{"a/foo.txt", "bla.txt"},
			},
			want: &client.FileTree{
				Files: map[string][]byte{"bla.txt": []byte("foo")},
				Dirs: map[string]*client.FileTree{
					"a": &client.FileTree{
						Files: map[string][]byte{"foo.txt": []byte("foo")},
					},
				},
			},
		},
		{
			desc: "dir_relative_symlink",
			input: []*inputPath{
				{path: "a/foo.txt", fileContents: []byte("foo")},
				{path: "b", isSymlink: true, symlinkTarget: "a"},
			},
			spec: &command.InputSpec{
				Inputs: []string{"a/foo.txt", "b"},
			},
			want: &client.FileTree{
				Dirs: map[string]*client.FileTree{
					"a": &client.FileTree{
						Files: map[string][]byte{"foo.txt": []byte("foo")},
					},
					"b": &client.FileTree{
						Files: map[string][]byte{"foo.txt": []byte("foo")},
					},
				},
			},
		},
		{
			desc: "dir_absolute_symlink",
			input: []*inputPath{
				{path: "a/foo.txt", fileContents: []byte("foo")},
				// The absolute symlink target will actually point to the absolute path of a.
				{path: "b", isSymlink: true, isAbsolute: true, symlinkTarget: "a"},
			},
			spec: &command.InputSpec{
				Inputs: []string{"a/foo.txt", "b"},
			},
			want: &client.FileTree{
				Dirs: map[string]*client.FileTree{
					"a": &client.FileTree{
						Files: map[string][]byte{"foo.txt": []byte("foo")},
					},
					"b": &client.FileTree{
						Files: map[string][]byte{"foo.txt": []byte("foo")},
					},
				},
			},
		},
		{
			desc: "file_exclusions",
			input: []*inputPath{
				{path: "a/foo", fileContents: []byte("foo")},
				{path: "a/b/bar.txt", fileContents: []byte("bar")},
				{path: "txt/a", fileContents: []byte("a")},
				{path: "a/b/baz.txt", fileContents: []byte("baz")},
				{path: "b/bla", fileContents: []byte("bla")},
				{path: "bla.txt", fileContents: []byte("bla")},
			},
			spec: &command.InputSpec{
				Inputs: []string{"a", "b", "txt", "bla.txt"},
				InputExclusions: []*command.InputExclusion{
					&command.InputExclusion{Regex: `txt$`, Type: command.FileInputType},
				},
			},
			want: &client.FileTree{
				Dirs: map[string]*client.FileTree{
					"a":   &client.FileTree{Files: map[string][]byte{"foo": []byte("foo")}},
					"b":   &client.FileTree{Files: map[string][]byte{"bla": []byte("bla")}},
					"txt": &client.FileTree{Files: map[string][]byte{"a": []byte("a")}},
				},
			},
		},
		{
			desc: "dir_exclusions",
			input: []*inputPath{
				{path: "b/a", fileContents: []byte("ba")},
				{path: "a/x", fileContents: []byte("x")},
				{path: "c/d/aa/x", fileContents: []byte("x")},
				{path: "bla", fileContents: []byte("bla")},
			},
			spec: &command.InputSpec{
				Inputs: []string{"a", "b", "bla"},
				InputExclusions: []*command.InputExclusion{
					&command.InputExclusion{Regex: "a$", Type: command.DirectoryInputType},
				},
			},
			want: &client.FileTree{
				Files: map[string][]byte{"bla": []byte("bla")},
				Dirs: map[string]*client.FileTree{
					"b": &client.FileTree{Files: map[string][]byte{"a": []byte("ba")}},
				},
			},
		},
		{
			desc: "all_type_exclusions",
			input: []*inputPath{
				{path: "b/a", fileContents: []byte("ba")},
				{path: "a/x", fileContents: []byte("x")},
				{path: "c/d/aa/x", fileContents: []byte("x")},
				{path: "bla", fileContents: []byte("bla")},
			},
			spec: &command.InputSpec{
				Inputs: []string{"a", "b", "bla"},
				InputExclusions: []*command.InputExclusion{
					&command.InputExclusion{Regex: "a$", Type: command.UnspecifiedInputType},
				},
			},
			want: &client.FileTree{},
		},
		{
			desc:  "virtual inputs",
			input: nil,
			spec: &command.InputSpec{
				VirtualInputs: []*command.VirtualInput{
					&command.VirtualInput{Path: "a/foo.txt", Contents: []byte("foo")},
					&command.VirtualInput{Path: "bar.txt", Contents: []byte("bar")},
				},
			},
			want: &client.FileTree{
				Files: map[string][]byte{"bar.txt": []byte("bar")},
				Dirs: map[string]*client.FileTree{
					"a": &client.FileTree{
						Files: map[string][]byte{"foo.txt": []byte("foo")},
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			root, err := ioutil.TempDir("", tc.desc)
			if err != nil {
				t.Fatalf("failed to make temp dir: %v", err)
			}
			defer os.RemoveAll(root)
			if err := construct(root, tc.input); err != nil {
				t.Fatalf("failed to construct input dir structure: %v", err)
			}
			got, err := client.BuildTreeFromInputs(root, tc.spec)
			if err != nil {
				t.Errorf("BuildTreeFromInputs(%s, %v) = _, %v want _, nil", root, tc.spec, err)
			}
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
	fooDg, barDg := digest.NewFromBlob(fooBlob), digest.NewFromBlob(barBlob)
	fooDgPb, barDgPb := fooDg.ToProto(), barDg.ToProto()

	fooDir := &repb.Directory{Files: []*repb.FileNode{{Name: "foo", Digest: fooDgPb, IsExecutable: true}}}
	barDir := &repb.Directory{Files: []*repb.FileNode{{Name: "bar", Digest: barDgPb, IsExecutable: true}}}
	foobarDir := &repb.Directory{Files: []*repb.FileNode{
		{Name: "bar", Digest: barDgPb, IsExecutable: true},
		{Name: "foo", Digest: fooDgPb, IsExecutable: true},
	}}

	fooDirBlob, barDirBlob, foobarDirBlob := MustMarshal(t, fooDir), MustMarshal(t, barDir), MustMarshal(t, foobarDir)
	fooDirDg, barDirDg, foobarDirDg := digest.NewFromBlob(fooDirBlob), digest.NewFromBlob(barDirBlob), digest.NewFromBlob(foobarDirBlob)
	fooDirDgPb, barDirDgPb := fooDirDg.ToProto(), barDirDg.ToProto()

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
				{Name: "barDir", Digest: barDirDgPb},
				{Name: "fooDir", Digest: fooDirDgPb},
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
				{Name: "fooDir", Digest: fooDirDgPb},
				{Name: "foobarDir", Digest: foobarDirDg.ToProto()},
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
				{Name: "fooDir1", Digest: fooDirDgPb},
				{Name: "fooDir2", Digest: fooDirDgPb},
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
				Directories: []*repb.DirectoryNode{{Name: "fooDir", Digest: fooDirDgPb}},
				Files:       []*repb.FileNode{{Name: "fooDirBlob", Digest: fooDirDgPb, IsExecutable: true}},
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
					{Name: "a", Digest: fooDgPb, IsExecutable: true},
					{Name: "b", Digest: fooDgPb, IsExecutable: true},
					{Name: "c", Digest: fooDgPb, IsExecutable: true},
					{Name: "d", Digest: fooDgPb, IsExecutable: true},
					{Name: "e", Digest: fooDgPb, IsExecutable: true},
					{Name: "f", Digest: fooDgPb, IsExecutable: true},
					{Name: "g", Digest: fooDgPb, IsExecutable: true},
					{Name: "h", Digest: fooDgPb, IsExecutable: true},
					{Name: "i", Digest: fooDgPb, IsExecutable: true},
					{Name: "j", Digest: fooDgPb, IsExecutable: true},
				},
				Directories: []*repb.DirectoryNode{
					{Name: "k", Digest: fooDirDgPb},
					{Name: "l", Digest: fooDirDgPb},
					{Name: "m", Digest: fooDirDgPb},
					{Name: "n", Digest: fooDirDgPb},
					{Name: "o", Digest: fooDirDgPb},
					{Name: "p", Digest: fooDirDgPb},
					{Name: "q", Digest: fooDirDgPb},
					{Name: "r", Digest: fooDirDgPb},
					{Name: "s", Digest: fooDirDgPb},
					{Name: "t", Digest: fooDirDgPb},
				},
			},
			additionalBlobs: [][]byte{fooBlob, fooDirBlob},
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			wantBlobs := make(map[digest.Digest][]byte)
			rootBlob := MustMarshal(t, tc.rootDir)
			rootDg := digest.NewFromBlob(rootBlob)
			wantBlobs[rootDg] = rootBlob
			for _, b := range tc.additionalBlobs {
				wantBlobs[digest.NewFromBlob(b)] = b
			}

			gotRootDg, gotBlobs, err := client.PackageTree(tc.input)
			if err != nil {
				t.Fatalf("client.PackageTree(...) = gave error %s, want success", err)
			}
			if diff := cmp.Diff(rootDg, gotRootDg); diff != "" {
				t.Errorf("client.PackageTree(...) gave diff (want -> got) on root:\n%s", diff)
				if gotRootBlob, ok := gotBlobs[gotRootDg]; ok {
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
	outputs, err := client.FlattenTree(tree, "x")
	if err != nil {
		t.Errorf("FlattenTree gave error %v", err)
	}
	wantOutputs := map[string]*client.Output{
		"x/baz":     &client.Output{Digest: bazDigest},
		"x/a/b/foo": &client.Output{Digest: fooDigest},
		"x/a/b/bar": &client.Output{Digest: barDigest, IsExecutable: true},
		"x/b/foo":   &client.Output{Digest: fooDigest},
		"x/b/bar":   &client.Output{Digest: barDigest, IsExecutable: true},
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
