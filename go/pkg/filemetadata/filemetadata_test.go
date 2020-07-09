package filemetadata

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/google/go-cmp/cmp"
)

type testFileParams struct {
	contents   string
	executable bool
}

func TestComputeFiles(t *testing.T) {
	tests := []struct {
		name string
		*testFileParams
	}{
		{
			name: "empty",
			testFileParams: &testFileParams{
				contents: "",
			},
		},
		{
			name: "non-executable",
			testFileParams: &testFileParams{
				contents: "bla",
			},
		},
		{
			name: "executable",
			testFileParams: &testFileParams{
				contents:   "foo",
				executable: true,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			filename, err := createFile(t, tc.testFileParams)
			if err != nil {
				t.Fatalf("Failed to create tmp file for testing digests: %v", err)
			}
			defer os.RemoveAll(filename)
			got := Compute(filename)
			if got.Err != nil {
				t.Errorf("Compute(%v) failed. Got error: %v", filename, got.Err)
			}
			want := &Metadata{
				Digest:       digest.NewFromBlob([]byte(tc.contents)),
				IsExecutable: tc.executable,
			}
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("Compute(%v) returned diff. (-want +got)\n%s", filename, diff)
			}
		})
	}
}

func TestComputeDirectory(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	defer os.RemoveAll(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create temp directory")
	}
	got := Compute(tmpDir)
	if fe, ok := got.Err.(*FileError); !ok || !fe.IsDirectory {
		t.Errorf("Compute(%v).Err = %v, want FileError{IsDirectory:true}", tmpDir, got.Err)
	}
	if got.Digest != digest.Empty {
		t.Errorf("Compute(%v).Digest = %v, want %v", tmpDir, got.Digest, digest.Empty)
	}
}

func TestComputeSymlinksToFile(t *testing.T) {
	tests := []struct {
		name   string
		target *testFileParams
	}{
		{
			name: "valid",
			target: &testFileParams{
				contents: "bla",
			},
		},
		{
			name: "valid-executable",
			target: &testFileParams{
				contents:   "executable",
				executable: true,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fileParams := tc.target
			symlinkPath := filepath.Join(os.TempDir(), tc.name)
			defer os.RemoveAll(symlinkPath)
			targetPath, err := createSymlinkTofile(t, symlinkPath, fileParams)
			if err != nil {
				t.Fatalf("Failed to create tmp symlink for testing digests: %v", err)
			}
			got := Compute(symlinkPath)

			if got.Err != nil {
				t.Errorf("Compute(%v) failed. Got error: %v", symlinkPath, got.Err)
			}
			want := &Metadata{
				Symlink: &SymlinkMetadata{
					Target:     targetPath,
					IsDangling: false,
				},
				Digest:       digest.NewFromBlob([]byte(fileParams.contents)),
				IsExecutable: fileParams.executable,
			}

			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("Compute(%v) returned diff. (-want +got)\n%s", symlinkPath, diff)
			}
		})
	}
}

func TestComputeDanglingSymlinks(t *testing.T) {
	// Create a temporary fake target so that os.Symlink() can work.
	symlinkPath := filepath.Join(os.TempDir(), "dangling")
	defer os.RemoveAll(symlinkPath)
	targetPath, err := createSymlinkTofile(t, symlinkPath, &testFileParams{
		contents: "transient",
	})
	if err != nil {
		t.Fatalf("Failed to create tmp symlink for testing digests: %v", err)
	}
	// Make the symlink dangling
	os.RemoveAll(targetPath)

	got := Compute(symlinkPath)
	if got.Err == nil || !got.Symlink.IsDangling {
		t.Errorf("Compute(%v) should fail because the symlink is dangling", symlinkPath)
	}
	if got.Symlink.Target != targetPath {
		t.Errorf("Compute(%v) should still set Target for the dangling symlink, want=%v got=%v", symlinkPath, targetPath, got.Symlink.Target)
	}
}

func TestComputeSymlinksToDirectory(t *testing.T) {
	symlinkPath := filepath.Join(os.TempDir(), "dir-symlink")
	defer os.RemoveAll(symlinkPath)
	targetPath, err := ioutil.TempDir(os.TempDir(), "")
	if err != nil {
		t.Fatalf("Failed to create tmp directory: %v", err)
	}
	err = createSymlinkToTarget(t, symlinkPath, targetPath)

	if err != nil {
		t.Fatalf("Failed to create tmp symlink for testing digests: %v", err)
	}

	got := Compute(symlinkPath)
	if fe, ok := got.Err.(*FileError); !ok || !fe.IsDirectory {
		t.Errorf("Compute(%v).Err = %v, want FileError{IsDirectory:true}", symlinkPath, got.Err)
	}
}

func createFile(t *testing.T, fp *testFileParams) (string, error) {
	t.Helper()
	perm := os.FileMode(0666)
	if fp.executable {
		perm = os.FileMode(0766)
	}
	tmpFile, err := ioutil.TempFile(os.TempDir(), "")
	if err != nil {
		return "", err
	}
	if err := tmpFile.Chmod(perm); err != nil {
		return "", err
	}
	if err := tmpFile.Close(); err != nil {
		return "", err
	}
	filename := tmpFile.Name()
	if err = ioutil.WriteFile(filename, []byte(fp.contents), os.ModeTemporary); err != nil {
		return "", err
	}
	return filename, nil
}

func createSymlinkTofile(t *testing.T, symlinkPath string, target *testFileParams) (string, error) {
	t.Helper()
	targetPath, err := createFile(t, target)
	if err != nil {
		return "", err
	}
	if err := createSymlinkToTarget(t, symlinkPath, targetPath); err != nil {
		return "", err
	}
	return targetPath, nil
}

func createSymlinkToTarget(t *testing.T, symlinkPath string, targetPath string) error {
	t.Helper()
	return os.Symlink(targetPath, symlinkPath)
}
