package filemetadata

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/google/go-cmp/cmp"
)

var (
	contents = []byte("example")
	wantDg   = digest.NewFromBlob(contents)
)

func TestSimpleCacheLoad(t *testing.T) {
	c := NewSingleFlightCache()
	filename, err := createFile(t, false, "")
	if err != nil {
		t.Fatalf("Failed to create tmp file for testing digests: %v", err)
	}
	if err = ioutil.WriteFile(filename, contents, os.ModeTemporary); err != nil {
		t.Fatalf("Failed to write to tmp file for testing digests: %v", err)
	}
	got := c.Get(filename)
	if got.Err != nil {
		t.Errorf("Get(%v) failed. Got error: %v", filename, got.Err)
	}
	want := &Metadata{
		Digest:       wantDg,
		IsExecutable: false,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Get(%v) returned diff. (-want +got)\n%s", filename, diff)
	}
	if c.GetCacheHits() != 0 {
		t.Errorf("Cache has wrong num of CacheHits, want 0, got %v", c.GetCacheHits())
	}

	if c.GetCacheMisses() != 1 {
		t.Errorf("Cache has wrong num of CacheMisses, want 1, got %v", c.GetCacheMisses())
	}
}

func TestCacheOnceLoadMultiple(t *testing.T) {
	c := NewSingleFlightCache()
	filename, err := createFile(t, false, "")
	if err != nil {
		t.Fatalf("Failed to create tmp file for testing digests: %v", err)
	}
	if err = ioutil.WriteFile(filename, contents, os.ModeTemporary); err != nil {
		t.Fatalf("Failed to write to tmp file for testing digests: %v", err)
	}
	want := &Metadata{
		Digest:       wantDg,
		IsExecutable: false,
	}
	for i := 0; i < 2; i++ {
		got := c.Get(filename)
		if got.Err != nil {
			t.Errorf("Get(%v) failed. Got error: %v", filename, got.Err)
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("Get(%v) returned diff. (-want +got)\n%s", filename, diff)
		}
	}
	if c.GetCacheHits() != 1 {
		t.Errorf("Cache has wrong num of CacheHits, want 1, got %v", c.GetCacheHits())
	}
	if c.GetCacheMisses() != 1 {
		t.Errorf("Cache has wrong num of CacheMisses, want 1, got %v", c.GetCacheMisses())
	}
}

func TestLoadAfterChangeWithoutValidation(t *testing.T) {
	c := NewSingleFlightCache()
	filename, err := createFile(t, false, "")
	if err != nil {
		t.Fatalf("Failed to create tmp file for testing digests: %v", err)
	}
	if err = ioutil.WriteFile(filename, contents, os.ModeTemporary); err != nil {
		t.Fatalf("Failed to write to tmp file for testing digests: %v", err)
	}
	got := c.Get(filename)
	if got.Err != nil {
		t.Fatalf("Get(%v) failed. Got error: %v", filename, got.Err)
	}
	want := &Metadata{
		Digest:       wantDg,
		IsExecutable: false,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("Get(%v) returned diff. (-want +got)\n%s", filename, diff)
	}

	change := []byte("change")
	if err = ioutil.WriteFile(filename, change, os.ModeTemporary); err != nil {
		t.Fatalf("Failed to write to tmp file for testing digests: %v", err)
	}
	got = c.Get(filename)
	if got.Err != nil {
		t.Errorf("Get(%v) failed. Got error: %v", filename, got.Err)
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Get(%v) returned diff. (-want +got)\n%s", filename, diff)
	}
	if c.GetCacheHits() != 1 {
		t.Errorf("Cache has wrong num of CacheHits, want 1, got %v", c.GetCacheHits())
	}
	if c.GetCacheMisses() != 1 {
		t.Errorf("Cache has wrong num of CacheMisses, want 1, got %v", c.GetCacheMisses())
	}
}
