package filemetadata

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/testutil"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

var (
	ignoreMtime = cmpopts.IgnoreFields(Metadata{}, "MTime")
)

func TestComputeFilesNoXattr(t *testing.T) {
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
			before := time.Now().Truncate(time.Second)
			time.Sleep(5 * time.Second)
			filename, err := testutil.CreateFile(t, tc.executable, tc.contents)
			if err != nil {
				t.Fatalf("Failed to create tmp file for testing digests: %v", err)
			}
			after := time.Now().Truncate(time.Second).Add(time.Second)
			defer os.RemoveAll(filename)
			got := Compute(filename)
			if got.Err != nil {
				t.Errorf("Compute(%v) failed. Got error: %v", filename, got.Err)
			}
			want := &Metadata{
				Digest:       digest.NewFromBlob([]byte(tc.contents)),
				IsExecutable: tc.executable,
			}
			if diff := cmp.Diff(want, got, ignoreMtime); diff != "" {
				t.Errorf("Compute(%v) returned diff. (-want +got)\n%s", filename, diff)
			}
			if got.MTime.Before(before) || got.MTime.After(after) {
				t.Errorf("Compute(%v) returned MTime %v, want time in (%v, %v).", filename, got.MTime, before, after)
			}
		})
	}
}

func TestComputeFilesWithXattr(t *testing.T) {
	overwriteXattrGlobals(t, "google.digest.sha256", xattributeAccessorMock{})
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
			getXAttrMock = func(_ string, _ string) ([]byte, error) {
				return []byte(tc.name), nil
			}

			before := time.Now().Truncate(time.Second)
			filename, err := testutil.CreateFile(t, tc.executable, tc.contents)
			if err != nil {
				t.Fatalf("Failed to create tmp file for testing digests: %v", err)
			}
			after := time.Now().Truncate(time.Second).Add(time.Second)
			defer os.RemoveAll(filename)
			got := Compute(filename)
			if got.Err != nil {
				t.Errorf("Compute(%v) failed. Got error: %v", filename, got.Err)
			}
			wantDigest, _ := digest.NewFromString(fmt.Sprintf("%s/%d", tc.name, len(tc.contents)))
			want := &Metadata{
				Digest:       wantDigest,
				IsExecutable: tc.executable,
			}
			if diff := cmp.Diff(want, got, ignoreMtime); diff != "" {
				t.Errorf("Compute(%v) returned diff. (-want +got)\n%s", filename, diff)
			}
			if got.MTime.Before(before) || got.MTime.After(after) {
				t.Errorf("Compute(%v) returned MTime %v, want time in (%v, %v).", filename, got.MTime, before, after)
			}
		})
	}
}

func TestComputeDirectory(t *testing.T) {
	tmpDir := t.TempDir()
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

			if diff := cmp.Diff(want, got, ignoreMtime); diff != "" {
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
	targetPath := t.TempDir()
	if err := createSymlinkToTarget(t, symlinkPath, targetPath); err != nil {
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
	targetPath, err := testutil.CreateFile(t, executable, contents)
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

func overwriteXattrGlobals(t *testing.T, newXattrDigestName string, newXattrAccess xattributeAccessorInterface) {
	t.Helper()
	oldXattrDigestName := XattrDigestName
	oldXattrAccess := XattrAccess
	XattrDigestName = newXattrDigestName
	XattrAccess = newXattrAccess
	t.Cleanup(func() {
		XattrDigestName = oldXattrDigestName
		XattrAccess = oldXattrAccess
	})
}

// Mocking of the xattr package for testing.
var getXAttrMock func(path string, name string) ([]byte, error)

type xattributeAccessorMock struct{}

func (x xattributeAccessorMock) getXAttr(path string, name string) ([]byte, error) {
	return getXAttrMock(path, name)
}

func (x xattributeAccessorMock) isSupported() bool {
	return true
}
