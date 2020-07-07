package filemetadata

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
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

// Used as a type enum
type testDirParams struct {
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

func TestComputeSymlinks(t *testing.T) {
	tests := []struct {
		name   string
		target interface{} // If unspecified, this is an invalid symlink
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
		{
			name: "invalid-file",
		},
		{
			name:   "symlink-dir",
			target: &testDirParams{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			symlinkResult, err := createSymlink(t, tc.target)
			if err != nil {
				t.Fatalf("Failed to create tmp symlink for testing digests: %v", err)
			}
			defer symlinkResult.cleanup()

			symlinkPath := symlinkResult.symlink
			got := Compute(symlinkPath)

			if tc.target == nil {
				if got.Err == nil || !got.Symlink.IsInvalid {
					t.Errorf("Compute(%v) should fail because the symlink is invalid", symlinkPath)
				}
				if got.Symlink.Target != "" {
					t.Errorf("Compute(%v) should fail because the symlink is invalid, got target: %s", symlinkPath, got.Symlink.Target)
				}
				return
			}

			if _, ok := tc.target.(*testDirParams); ok {
				if fe, ok := got.Err.(*FileError); !ok || !fe.IsDirectory {
					t.Errorf("Compute(%v).Err = %v, want FileError{IsDirectory:true}", symlinkPath, got.Err)
				}
				return
			}

			if got.Err != nil {
				t.Errorf("Compute(%v) failed. Got error: %v", symlinkPath, got.Err)
			}

			fileParams := tc.target.(*testFileParams)
			want := &Metadata{
				Symlink: &SymlinkMetadata{
					Target:    symlinkResult.target,
					IsInvalid: false,
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

func createSymlink(t *testing.T, target interface{}) (*testSymlinkCreationResult, error) {
	t.Helper()

	invalidTarget := target == nil
	if invalidTarget {
		// Create a temporary fake target so that os.Symlink() can work.
		target = &testFileParams{
			contents: "transient",
		}
	}
	targetPath, err := func() (string, error) {
		switch tgt := target.(type) {
		case *testFileParams:
			return createFile(t, tgt)
		case *testDirParams:
			return ioutil.TempDir(os.TempDir(), "")
		}
		return "", errors.New("Unknown target type")
	}()
	if err != nil {
		return nil, err
	}

	randBytes := make([]byte, 16)
	rand.Read(randBytes)
	symlinkPath := filepath.Join(os.TempDir(), hex.EncodeToString(randBytes))
	if err := os.Symlink(targetPath, symlinkPath); err != nil {
		return nil, err
	}

	result := &testSymlinkCreationResult{
		symlink: symlinkPath,
	}
	if invalidTarget {
		os.RemoveAll(targetPath)
	}
	result.target = targetPath

	return result, nil
}
