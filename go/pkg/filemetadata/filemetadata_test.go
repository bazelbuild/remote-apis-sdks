package filemetadata

import (
	"crypto/rand"
	"encoding/hex"
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

type testSymlinkCreationResult struct {
	symlink string
	target  string
}

func (sc *testSymlinkCreationResult) cleanup() {
	os.RemoveAll(sc.symlink)
	os.RemoveAll(sc.target)
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
			symlinkResult, err := createSymlinkTofile(t, fileParams)
			if err != nil {
				t.Fatalf("Failed to create tmp symlink for testing digests: %v", err)
			}
			defer symlinkResult.cleanup()

			symlinkPath := symlinkResult.symlink
			got := Compute(symlinkPath)

			if got.Err != nil {
				t.Errorf("Compute(%v) failed. Got error: %v", symlinkPath, got.Err)
			}
			want := &Metadata{
				Symlink: &SymlinkMetadata{
					Target:     symlinkResult.target,
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
	symlinkResult, err := createSymlinkTofile(t, &testFileParams{
		contents: "transient",
	})
	if err != nil {
		t.Fatalf("Failed to create tmp symlink for testing digests: %v", err)
	}
	// Make the symlink dangling
	os.RemoveAll(symlinkResult.target)
	defer symlinkResult.cleanup()

	symlinkPath := symlinkResult.symlink
	got := Compute(symlinkPath)
	if got.Err == nil || !got.Symlink.IsDangling {
		t.Errorf("Compute(%v) should fail because the symlink is dangling", symlinkPath)
	}
	if got.Symlink.Target != symlinkResult.target {
		t.Errorf("Compute(%v) should still set Target for the dangling symlink, want=%v got=%v", symlinkPath, symlinkResult.target, got.Symlink.Target)
	}
}

func TestComputeSymlinksToDirectory(t *testing.T) {
	targetPath, err := ioutil.TempDir(os.TempDir(), "")
	if err != nil {
		t.Fatalf("Failed to create tmp directory: %v", err)
	}
	symlinkResult, err := createSymlinkToTarget(t, targetPath)

	if err != nil {
		t.Fatalf("Failed to create tmp symlink for testing digests: %v", err)
	}
	defer symlinkResult.cleanup()

	symlinkPath := symlinkResult.symlink
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

func createSymlinkTofile(t *testing.T, target *testFileParams) (*testSymlinkCreationResult, error) {
	targetPath, err := createFile(t, target)
	if err != nil {
		return nil, err
	}
	return createSymlinkToTarget(t, targetPath)
}

func createSymlinkToTarget(t *testing.T, targetPath string) (*testSymlinkCreationResult, error) {
	t.Helper()

	randBytes := make([]byte, 16)
	rand.Read(randBytes)
	symlinkPath := filepath.Join(os.TempDir(), hex.EncodeToString(randBytes))
	if err := os.Symlink(targetPath, symlinkPath); err != nil {
		return nil, err
	}

	return &testSymlinkCreationResult{
		symlink: symlinkPath,
		target:  targetPath,
	}, nil
}
