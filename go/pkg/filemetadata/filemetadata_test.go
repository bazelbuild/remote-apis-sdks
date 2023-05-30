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

func TestComputeFileDigestWithXattr(t *testing.T) {
	xattrDgName := "user.myhash"
	overwriteXattrDgName(t, xattrDgName)
	tests := []struct {
		filename     string
		contents     string
		xattrDgStr   string
		expectDgStr  string
		expectErrStr string
	}{
		{
			filename:    "provide_no_xattr_will_generate_real_digest(sha256sum+real_size)",
			contents:    "123456",
			expectDgStr: "8d969eef6ecad3c29a3a629280e686cf0c3f5d5a86aff3ca12020c923adc6c92/6",
		},
		{
			filename:    "provide_only_digest_hash_will_generate_digest_with_real_size",
			contents:    "123456",
			xattrDgStr:  "1111111111111111111111111111111111111111111111111111111111111111",
			expectDgStr: "1111111111111111111111111111111111111111111111111111111111111111/6",
		},
		{
			filename:    "valid_full_digest_(hash+size)_in_xatrr_will_be_used_directly",
			contents:    "",
			xattrDgStr:  "1111111111111111111111111111111111111111111111111111111111111111/666",
			expectDgStr: "1111111111111111111111111111111111111111111111111111111111111111/666",
		},
		{
			filename:     "provide_invalid_digest_(hash_missing_digits)_will_set_md.Err",
			contents:     "123456",
			xattrDgStr:   "abc",
			expectErrStr: "valid hash length is 64, got length 3 (abc)",
		},
		{
			filename:     "provide_invalid_full_digest_(hash_missing_digits)_will_set_md.Err",
			contents:     "123456",
			xattrDgStr:   "666/666",
			expectErrStr: "valid hash length is 64, got length 3 (666)",
		},
		{
			filename:     "provide_invalid_full_digest_(extra_slash)_will_set_md.Err",
			contents:     "123456",
			xattrDgStr:   "///666",
			expectErrStr: "expected digest in the form hash/size, got ///666",
		},
	}
	for _, tc := range tests {
		t.Run(tc.filename, func(t *testing.T) {
			file, err := os.Create(tc.filename)
			if err != nil {
				t.Fatalf("Failed to create file: %s", err)
			}
			path := file.Name()
			defer os.Remove(path)
			defer file.Close()
			_, err = file.WriteString(tc.contents)
			if err != nil {
				t.Fatalf("Failed to write to file: %v\n", err)
				return
			}
			if tc.xattrDgStr != "" {
				xattr.Set(path, xattrDgName, []byte(tc.xattrDgStr))
			}
			md := Compute(path)
			if md.Err != nil {
				got := md.Err.Error()
				want := tc.expectErrStr
				if diff := cmp.Diff(want, got); diff != "" {
					t.Errorf("Compute Digest for (%v) returned diff Error Msg. (-want +got)\n%s", tc.filename, diff)
				}
			} else {
				got := md.Digest.String()
				want := tc.expectDgStr
				if diff := cmp.Diff(want, got); diff != "" {
					t.Errorf("Compute Digest for (%v) returned diff. (-want +got)\n%s", tc.filename, diff)
				}
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
