package filemetadata

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pkg/xattr"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/testutil"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

const (
	mockedHash = "000000000000000000000000000000000000000000000000000000000000000a"
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
			t.Cleanup(func() { os.RemoveAll(filename) })
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
				return []byte(mockedHash), nil
			}

			before := time.Now().Truncate(time.Second)
			filename, err := testutil.CreateFile(t, tc.executable, tc.contents)
			if err != nil {
				t.Fatalf("Failed to create tmp file for testing digests: %v", err)
			}
			after := time.Now().Truncate(time.Second).Add(time.Second)
			t.Cleanup(func() { os.RemoveAll(filename) })
			got := Compute(filename)
			if got.Err != nil {
				t.Errorf("Compute(%v) failed. Got error: %v", filename, got.Err)
			}
			wantDigest, _ := digest.NewFromString(fmt.Sprintf("%s/%d", mockedHash, len(tc.contents)))
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

func TestComputeFileDigestWithXattr(t *testing.T) {
	xattrDgName := "user.myhash"
	overwriteXattrDgName(t, xattrDgName)
	tests := []struct {
		name       string
		contents   string
		xattrDgStr string
		wantDgStr  string
		wantErr    bool
	}{
		{
			name:      "no-xattr",
			contents:  "123456",
			wantDgStr: "8d969eef6ecad3c29a3a629280e686cf0c3f5d5a86aff3ca12020c923adc6c92/6",
		},
		{
			name:       "only-digest-hash",
			contents:   "123456",
			xattrDgStr: "1111111111111111111111111111111111111111111111111111111111111111",
			wantDgStr:  "1111111111111111111111111111111111111111111111111111111111111111/6",
		},
		{
			name:       "full-digest-(hash+size)",
			contents:   "",
			xattrDgStr: "1111111111111111111111111111111111111111111111111111111111111111/666",
			wantDgStr:  "1111111111111111111111111111111111111111111111111111111111111111/666",
		},
		{
			name:       "invalid-digest-hash",
			contents:   "123456",
			xattrDgStr: "abc",
			wantDgStr:  "abc/6",
			wantErr:    true,
		},
		{
			name:       "invalid-full-digest",
			contents:   "123456",
			xattrDgStr: "666/666",
			wantDgStr:  "666/666",
			wantErr:    true,
		},
		{
			name:       "invalid-full-digest-(extra-slash)",
			contents:   "123456",
			xattrDgStr: "///666",
			wantDgStr:  "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/0",
			wantErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			filePath := tc.name
			//In the Github pipeline, set xatrr for files created under /tmp folder (by running t.tempDir()) will raise an error for operation not supported
			//But local run of the unit test will pass. So we have to create these test files under the PWD.
			err := os.WriteFile(filePath, []byte(tc.contents), 0666)
			if err != nil {
				t.Fatalf("Failed to write to file: %v\n", err)
			}
			t.Cleanup(func() { os.RemoveAll(filePath) })
			if tc.xattrDgStr != "" {
				if err = xattr.Set(filePath, xattrDgName, []byte(tc.xattrDgStr)); err != nil {
					t.Fatalf("Failed to set xattr to file: %v\n", err)
				}
			}
			md := Compute(filePath)
			if tc.wantErr && md.Err == nil {
				t.Errorf("No error while computing digest %v, but error was expected", filePath)
			}
			if !tc.wantErr && md.Err != nil {
				t.Errorf("Returned error while computing digest %v, err: %v", filePath, md.Err)
			}
			got := md.Digest.String()
			want := tc.wantDgStr
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("Compute Digest for (%v) returned diff. (-want +got)\n%s", filePath, diff)
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
			t.Cleanup(func() { os.RemoveAll(symlinkPath) })
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
	t.Cleanup(func() { os.RemoveAll(symlinkPath) })
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
	t.Cleanup(func() { os.RemoveAll(symlinkPath) })
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

func overwriteXattrDgName(t *testing.T, newXattrDigestName string) {
	t.Helper()
	oldXattrDigestName := XattrDigestName
	XattrDigestName = newXattrDigestName
	t.Cleanup(func() {
		XattrDigestName = oldXattrDigestName
	})
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
