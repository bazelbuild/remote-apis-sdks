package filemetadata

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/digest"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestCompute(t *testing.T) {
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
			margin := time.Second
			before := time.Now().Truncate(margin)
			filename, err := createFile(t, tc.executable, tc.contents)
			if err != nil {
				t.Fatalf("Failed to create tmp file for testing digests: %v", err)
			}
			defer os.RemoveAll(filename)
			after := time.Now().Truncate(margin).Add(margin)
			got := Compute(filename)
			if got.Err != nil {
				t.Errorf("Compute(%v) failed. Got error: %v", filename, got.Err)
			}
			want := &Metadata{
				Digest:       digest.NewFromBlob([]byte(tc.contents)),
				IsExecutable: tc.executable,
			}
			if diff := cmp.Diff(want, got, cmpopts.IgnoreFields(Metadata{}, "MTime")); diff != "" {
				t.Errorf("Compute(%v) returned diff. (-want +got)\n%s", filename, diff)
			}
			if got.MTime.Before(before) || got.MTime.After(after) {
				t.Errorf("Compute(%v) returned MTime %v, want time in (%v, %v).", filename, got.MTime, before, after)
			}
		})
	}
}

func TestComputeDirectory(t *testing.T) {
	margin := time.Second
	before := time.Now().Truncate(margin)
	tmpDir, err := ioutil.TempDir("", "")
	defer os.RemoveAll(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create temp directory")
	}
	after := time.Now().Truncate(margin).Add(margin)
	got := Compute(tmpDir)
	if fe, ok := got.Err.(*FileError); !ok || !fe.IsDirectory {
		t.Errorf("Compute(%v).Err = %v, want FileError{IsDirectory:true}", tmpDir, got.Err)
	}
	if got.Digest != digest.Empty {
		t.Errorf("Compute(%v).Digest = %v, want %v", tmpDir, got.Digest, digest.Empty)
	}
	if got.MTime.Before(before) || got.MTime.After(after) {
		t.Errorf("Compute(%v) returned MTime %v, want time in (%v, %v).", tmpDir, got.MTime, before, after)
	}
}

func createFile(t *testing.T, executable bool, contents string) (string, error) {
	t.Helper()
	perm := os.FileMode(0666)
	if executable {
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
	if err = ioutil.WriteFile(filename, []byte(contents), os.ModeTemporary); err != nil {
		return "", err
	}
	return filename, nil
}
