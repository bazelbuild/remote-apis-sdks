package filemetadata

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/test_util"
	"github.com/google/go-cmp/cmp"
)

func TestComputeFiles(t *testing.T) {
	tests := []struct {
		name       string
		contents   string
		executable bool
	}{
		{
			name:     "empty",
			contents: "",
		},
		{
			name:     "non-executable",
			contents: "bla",
		},
		{
			name:       "executable",
			contents:   "foo",
			executable: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			filename, err := test_util.CreateFile(t, tc.executable, tc.contents)
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
	if got.Err != nil {
		t.Errorf("Compute(%v).Err = %v, expected nil", tmpDir, got.Err)
	}
	if !got.IsDirectory {
		t.Errorf("Compute(%v).IsDirectory = false, want true", tmpDir)
	}
	if got.Digest != digest.Empty {
		t.Errorf("Compute(%v).Digest = %v, want %v", tmpDir, got.Digest, digest.Empty)
	}
}

func TestComputeSymlinksToFile(t *testing.T) {
	tests := []struct {
		name       string
		contents   string
		executable bool
	}{
		{
			name:     "valid",
			contents: "bla",
		},
		{
			name:       "valid-executable",
			contents:   "executable",
			executable: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			symlinkPath := filepath.Join(os.TempDir(), tc.name)
			defer os.RemoveAll(symlinkPath)
			targetPath, err := createSymlinkToFile(t, symlinkPath, tc.executable, tc.contents)
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
				Digest:       digest.NewFromBlob([]byte(tc.contents)),
				IsExecutable: tc.executable,
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
	targetPath, err := createSymlinkToFile(t, symlinkPath, false, "transient")
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
	if got.Err != nil {
		t.Errorf("Compute(%v).Err = %v, expected nil", symlinkPath, got.Err)
	}
	if !got.IsDirectory {
		t.Errorf("Compute(%v).IsDirectory = false, want true", symlinkPath)
	}
}

func createSymlinkToFile(t *testing.T, symlinkPath string, executable bool, contents string) (string, error) {
	t.Helper()
	targetPath, err := test_util.CreateFile(t, executable, contents)
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
