//go:build !windows
// +build !windows

package filemetadata

import (
	"os"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/testutil"
	"github.com/google/go-cmp/cmp"
)

func TestExecutableCacheLoad(t *testing.T) {
	c := NewSingleFlightCache()
	filename, err := testutil.CreateFile(t, true, "")
	if err != nil {
		t.Fatalf("Failed to create tmp file for testing digests: %v", err)
	}
	if err = os.WriteFile(filename, contents, os.ModeTemporary); err != nil {
		t.Fatalf("Failed to write to tmp file for testing digests: %v", err)
	}
	got := c.Get(filename)
	if got.Err != nil {
		t.Errorf("Get(%v) failed. Got error: %v", filename, got.Err)
	}
	want := &Metadata{
		Digest:       wantDg,
		IsExecutable: true,
	}
	if diff := cmp.Diff(want, got, ignoreMtime); diff != "" {
		t.Errorf("Get(%v) returned diff. (-want +got)\n%s", filename, diff)
	}
}
