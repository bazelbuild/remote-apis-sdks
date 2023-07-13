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
	targetFile = "test.txt"
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

			// Compare unix seconds rather than instants because Truncate operates on durations
			// which means the returned instant is not always as expected.
			before := time.Now().Unix()
			filename, err := testutil.CreateFile(t, tc.executable, tc.contents)
			if err != nil {
				t.Fatalf("Failed to create tmp file for testing digests: %v", err)
			}
			after := time.Now().Add(time.Second).Unix()
			t.Cleanup(func() { os.RemoveAll(filename) })
			got := Compute(filename)
			if got.Err != nil {
				t.Errorf("Compute(%v) failed. Got error: %v", filename, got.Err)
			}
			wantDigest, err := digest.NewFromString(fmt.Sprintf("%s/%d", mockedHash, len(tc.contents)))
			if err != nil {
				t.Fatalf("Failed to create wantDigest: %v", err)
			}
			want := &Metadata{
				Digest:       wantDigest,
				IsExecutable: tc.executable,
			}
			if diff := cmp.Diff(want, got, ignoreMtime); diff != "" {
				t.Errorf("Compute(%v) returned diff. (-want +got)\n%s", filename, diff)
			}
			gotMT := got.MTime.Unix()
			if gotMT < before || gotMT > after {
				t.Errorf("Compute(%v) returned MTime %v, want time between (%v, %v).", filename, gotMT, before, after)
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
			name:       "only digest hash",
			contents:   "123456",
			xattrDgStr: "1111111111111111111111111111111111111111111111111111111111111111",
			wantDgStr:  "1111111111111111111111111111111111111111111111111111111111111111/6",
		},
		{
			name:       "full digest (hash+size)",
			contents:   "",
			xattrDgStr: "1111111111111111111111111111111111111111111111111111111111111111/666",
			wantDgStr:  "1111111111111111111111111111111111111111111111111111111111111111/666",
		},
		{
			name:       "invalid digest hash",
			contents:   "123456",
			xattrDgStr: "abc",
			wantDgStr:  digest.Empty.String(),
			wantErr:    true,
		},
		{
			name:       "invalid full digest",
			contents:   "123456",
			xattrDgStr: "666/666",
			wantDgStr:  digest.Empty.String(),
			wantErr:    true,
		},
		{
			name:       "invalid full digest (extra-slash)",
			contents:   "123456",
			xattrDgStr: "///666",
			wantDgStr:  digest.Empty.String(),
			wantErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			testName := tc.name
			targetFilePath := createFileWithXattr(t, tc.contents, xattrDgName, tc.xattrDgStr)
			t.Cleanup(func() { os.RemoveAll(targetFilePath) })
			md := Compute(targetFilePath)
			if tc.wantErr && md.Err == nil {
				t.Errorf("No error while computing digest for test %v, but error was expected", testName)
			}
			if !tc.wantErr && md.Err != nil {
				t.Errorf("Returned error while computing digest for test %v, err: %v", testName, md.Err)
			}
			got := md.Digest.String()
			want := tc.wantDgStr
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("Compute Digest for test %v returned diff. (-want +got)\n%s", testName, diff)
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

func createFileWithXattr(t *testing.T, fileContent, xattrName, xattrValue string) string {
	t.Helper()
	filePath := filepath.Join(t.TempDir(), targetFile)
	err := os.WriteFile(filePath, []byte(fileContent), 0666)
	if err != nil {
		t.Fatalf("Failed to write to a file: %v\n", err)
	}
	if xattrValue == "" {
		return filePath
	}
	if err = xattr.Set(filePath, xattrName, []byte(xattrValue)); err == nil {
		// setting xattr for a file in a TempDir succeeded
		return filePath
	}
	// Setting xattr for a file in a TempDir might've failed because on some linux systems
	// temp dir is mounted on tmpfs which does not support user extended attributes
	// (https://man7.org/linux/man-pages/man5/tmpfs.5.html.
	// In this case, try to create a file in a a working directory instead
	t.Logf("Setting xattr for a file in %v failed. Using a working directory instead. err: %v",
		t.TempDir(), err)
	filePath = targetFile
	if err = os.WriteFile(filePath, []byte(fileContent), 0666); err != nil {
		t.Fatalf("Failed to write to a file: %v\n", err)
	}
	if err = xattr.Set(filePath, xattrName, []byte(xattrValue)); err == nil {
		return filePath
	}
	os.RemoveAll(filePath)
	// It's possible that the working directory is read only, skipping the test
	// because it's not possible to set a user xattr neither in temp dir nor in working dir
	// on a test environment
	t.Logf("Setting xattr for a file in a working directory failed. Skipping the test. err: %v", err)
	t.Skip("Cannot set a user xattr for a file in neither temp nor working directory")
	return ""
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
