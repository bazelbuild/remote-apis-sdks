package diskcache

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/testutil"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

func TestStoreLoadCasPerm(t *testing.T) {
	tests := []struct {
		name       string
		executable bool
	}{
		{
			name:       "+X",
			executable: true,
		},
		{
			name:       "-X",
			executable: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			d, err := New(context.Background(), filepath.Join(root, "cache"), 20)
			if err != nil {
				t.Errorf("New: %v", err)
			}
			fname, _ := testutil.CreateFile(t, tc.executable, "12345")
			srcInfo, err := os.Stat(fname)
			if err != nil {
				t.Fatalf("os.Stat() failed: %v", err)
			}
			dg, err := digest.NewFromFile(fname)
			if err != nil {
				t.Fatalf("digest.NewFromFile failed: %v", err)
			}
			if err := d.StoreCas(dg, fname); err != nil {
				t.Errorf("StoreCas(%s, %s) failed: %v", dg, fname, err)
			}
			newName := filepath.Join(root, "new")
			if !d.LoadCas(dg, newName) {
				t.Errorf("expected to load %s from the cache to %s", dg, newName)
			}
			fileInfo, err := os.Stat(newName)
			if err != nil {
				t.Fatalf("os.Stat(%s) failed: %v", newName, err)
			}
			if fileInfo.Mode() != srcInfo.Mode() {
				t.Errorf("expected %s to have %v permissions, got: %v", newName, srcInfo.Mode(), fileInfo.Mode())
			}
			contents, err := os.ReadFile(newName)
			if err != nil {
				t.Errorf("error reading from %s: %v", newName, err)
			}
			if string(contents) != "12345" {
				t.Errorf("Cached result did not match: want %q, got %q", "12345", string(contents))
			}
			d.Shutdown()
			stats := d.GetStats()
			if stats.TotalNumFiles != 1 {
				t.Errorf("expected TotalNumFiles to be 1, got %d", stats.TotalNumFiles)
			}
			if stats.NumFilesStored != 1 {
				t.Errorf("expected NumFilesStored to be 1, got %d", stats.NumFilesStored)
			}
			if stats.TotalStoredBytes != 5 {
				t.Errorf("expected TotalStoredBytes to be 5, got %d", stats.TotalStoredBytes)
			}
			if stats.NumCacheHits != 1 {
				t.Errorf("expected NumCacheHits to be 1, got %d", stats.NumCacheHits)
			}
			if stats.TotalCacheHitSizeBytes != 5 {
				t.Errorf("expected TotalCacheHitSizeBytes to be 5, got %d", stats.TotalCacheHitSizeBytes)
			}
			if stats.NumCacheMisses != 0 {
				t.Errorf("expected NumCacheMisses to be 0, got %d", stats.NumCacheMisses)
			}
			if stats.TotalSizeBytes != 5 {
				t.Errorf("expected TotalSizeBytes to be 5, got %d", stats.TotalSizeBytes)
			}
			if stats.NumFilesGCed != 0 {
				t.Errorf("expected NumFilesGCed to be 0, got %d", stats.NumFilesGCed)
			}
			if stats.TotalGCedSizeBytes != 0 {
				t.Errorf("expected TotalGCedSizeBytes to be 0, got %d", stats.TotalGCedSizeBytes)
			}
			if stats.InitTime == 0 {
				t.Errorf("expected InitTime to be > 0")
			}
			if stats.TotalGCTime != 0 {
				t.Errorf("expected TotalGCTime to be 0")
			}
		})
	}
}

func TestLoadCasNotFound(t *testing.T) {
	root := t.TempDir()
	d, err := New(context.Background(), filepath.Join(root, "cache"), 20)
	if err != nil {
		t.Errorf("New: %v", err)
	}
	newName := filepath.Join(root, "new")
	dg := digest.NewFromBlob([]byte("bla"))
	if d.LoadCas(dg, newName) {
		t.Errorf("expected to not load %s from the cache to %s", dg, newName)
	}
	d.Shutdown()
	stats := d.GetStats()
	if stats.TotalNumFiles != 0 {
		t.Errorf("expected TotalNumFiles to be 0, got %d", stats.TotalNumFiles)
	}
	if stats.TotalSizeBytes != 0 {
		t.Errorf("expected TotalSizeBytes to be 0, got %d", stats.TotalSizeBytes)
	}
	if stats.NumFilesStored != 0 {
		t.Errorf("expected NumFilesStored to be 0, got %d", stats.NumFilesStored)
	}
	if stats.TotalStoredBytes != 0 {
		t.Errorf("expected TotalStoredBytes to be 0, got %d", stats.TotalStoredBytes)
	}
	if stats.NumCacheHits != 0 {
		t.Errorf("expected NumCacheHits to be 0, got %d", stats.NumCacheHits)
	}
	if stats.TotalCacheHitSizeBytes != 0 {
		t.Errorf("expected TotalCacheHitSizeBytes to be 0, got %d", stats.TotalCacheHitSizeBytes)
	}
	if stats.NumCacheMisses != 1 {
		t.Errorf("expected NumCacheMisses to be 1, got %d", stats.NumCacheMisses)
	}
	if stats.NumFilesGCed != 0 {
		t.Errorf("expected NumFilesGCed to be 0, got %d", stats.NumFilesGCed)
	}
	if stats.TotalGCedSizeBytes != 0 {
		t.Errorf("expected TotalGCedSizeBytes to be 0, got %d", stats.TotalGCedSizeBytes)
	}
	if stats.InitTime == 0 {
		t.Errorf("expected InitTime to be > 0")
	}
	if stats.TotalGCTime != 0 {
		t.Errorf("expected TotalGCTime to be 0")
	}
}

func TestStoreLoadActionCache(t *testing.T) {
	root := t.TempDir()
	d, err := New(context.Background(), filepath.Join(root, "cache"), 100)
	if err != nil {
		t.Errorf("New: %v", err)
	}
	ar := &repb.ActionResult{
		OutputFiles: []*repb.OutputFile{
			{Path: "bla", Digest: digest.Empty.ToProto()},
		},
	}
	dg := digest.NewFromBlob([]byte("foo"))
	if err := d.StoreActionCache(dg, ar); err != nil {
		t.Errorf("StoreActionCache(%s) failed: %v", dg, err)
	}
	got, loaded := d.LoadActionCache(dg)
	if !loaded {
		t.Errorf("expected to load %s from the cache", dg)
	}
	if diff := cmp.Diff(ar, got, cmp.Comparer(proto.Equal)); diff != "" {
		t.Errorf("LoadActionCache(...) gave diff on action result (-want +got):\n%s", diff)
	}
	d.Shutdown()
	stats := d.GetStats()
	if stats.TotalNumFiles != 1 {
		t.Errorf("expected TotalNumFiles to be 1, got %d", stats.TotalNumFiles)
	}
	bytes, err := proto.Marshal(ar)
	if err != nil {
		t.Fatalf("error marshalling proto: %v", err)
	}
	size := int64(len(bytes))
	if stats.TotalSizeBytes != size {
		t.Errorf("expected TotalSizeBytes to be %d, got %d", size, stats.TotalSizeBytes)
	}
	if stats.NumFilesStored != 1 {
		t.Errorf("expected NumFilesStored to be 1, got %d", stats.NumFilesStored)
	}
	if stats.TotalStoredBytes != size {
		t.Errorf("expected TotalStoredBytes to be %d, got %d", size, stats.TotalStoredBytes)
	}
	if stats.NumCacheHits != 1 {
		t.Errorf("expected NumCacheHits to be 1, got %d", stats.NumCacheHits)
	}
	if stats.TotalCacheHitSizeBytes != size {
		t.Errorf("expected TotalCacheHitSizeBytes to be %d, got %d", size, stats.TotalCacheHitSizeBytes)
	}
	if stats.NumCacheMisses != 0 {
		t.Errorf("expected NumCacheMisses to be 0, got %d", stats.NumCacheMisses)
	}
	if stats.NumFilesGCed != 0 {
		t.Errorf("expected NumFilesGCed to be 0, got %d", stats.NumFilesGCed)
	}
	if stats.TotalGCedSizeBytes != 0 {
		t.Errorf("expected TotalGCedSizeBytes to be 0, got %d", stats.TotalGCedSizeBytes)
	}
	if stats.InitTime == 0 {
		t.Errorf("expected InitTime to be > 0")
	}
	if stats.TotalGCTime != 0 {
		t.Errorf("expected TotalGCTime to be 0")
	}
}

func TestGcOldestCas(t *testing.T) {
	root := t.TempDir()
	d, err := New(context.Background(), filepath.Join(root, "cache"), 20)
	if err != nil {
		t.Errorf("New: %v", err)
	}
	for i := 0; i < 5; i++ {
		fname, _ := testutil.CreateFile(t, false, fmt.Sprintf("aaa %d", i))
		dg, err := digest.NewFromFile(fname)
		if err != nil {
			t.Fatalf("digest.NewFromFile failed: %v", err)
		}
		if err := d.StoreCas(dg, fname); err != nil {
			t.Errorf("StoreCas(%s, %s) failed: %v", dg, fname, err)
		}
	}
	d.Shutdown()
	newName := filepath.Join(root, "new")
	for i := 0; i < 5; i++ {
		dg := digest.NewFromBlob([]byte(fmt.Sprintf("aaa %d", i)))
		if d.LoadCas(dg, newName) != (i > 0) {
			t.Errorf("expected loaded to be %v for %s from the cache to %s", i > 0, dg, newName)
		}
	}
	stats := d.GetStats()
	if stats.TotalNumFiles != 4 {
		t.Errorf("expected TotalNumFiles to be 4, got %d", stats.TotalNumFiles)
	}
	if stats.NumFilesStored != 5 {
		t.Errorf("expected NumFilesStored to be 5, got %d", stats.NumFilesStored)
	}
	if stats.TotalStoredBytes != 25 {
		t.Errorf("expected TotalStoredBytes to be 25, got %d", stats.TotalStoredBytes)
	}
	if stats.NumCacheHits != 4 {
		t.Errorf("expected NumCacheHits to be 4, got %d", stats.NumCacheHits)
	}
	if stats.TotalCacheHitSizeBytes != 20 {
		t.Errorf("expected TotalCacheHitSizeBytes to be 20, got %d", stats.TotalCacheHitSizeBytes)
	}
	if stats.NumCacheMisses != 1 {
		t.Errorf("expected NumCacheMisses to be 1, got %d", stats.NumCacheMisses)
	}
	if stats.NumFilesGCed != 1 {
		t.Errorf("expected NumFilesGCed to be 1, got %d", stats.NumFilesGCed)
	}
	if stats.TotalGCedSizeBytes != 5 {
		t.Errorf("expected TotalGCedSizeBytes to be 5, got %d", stats.TotalGCedSizeBytes)
	}
	if uint64(stats.TotalSizeBytes) != d.maxCapacityBytes {
		t.Errorf("expected total size bytes to be %d, got %d", d.maxCapacityBytes, stats.TotalSizeBytes)
	}
	if stats.InitTime <= 0 {
		t.Errorf("expected InitTime to be > 0")
	}
	if stats.TotalGCTime <= 0 {
		t.Errorf("expected TotalGCTime to be > 0")
	}
}

func TestGcOldestActionCache(t *testing.T) {
	ar := &repb.ActionResult{
		OutputFiles: []*repb.OutputFile{
			{Path: "12345", Digest: digest.Empty.ToProto()},
		},
	}
	bytes, err := proto.Marshal(ar)
	if err != nil {
		t.Fatalf("error marshalling proto: %v", err)
	}
	size := len(bytes)
	root := t.TempDir()
	d, err := New(context.Background(), filepath.Join(root, "cache"), uint64(size)*4)
	if err != nil {
		t.Errorf("New: %v", err)
	}
	for i := 0; i < 5; i++ {
		si := fmt.Sprintf("aaa %d", i)
		dg := digest.NewFromBlob([]byte(si))
		ar.OutputFiles[0].Path = si
		if err := d.StoreActionCache(dg, ar); err != nil {
			t.Errorf("StoreActionCache(%s) failed: %v", dg, err)
		}
	}
	d.Shutdown()
	for i := 0; i < 5; i++ {
		si := fmt.Sprintf("aaa %d", i)
		dg := digest.NewFromBlob([]byte(si))
		got, loaded := d.LoadActionCache(dg)
		if loaded {
			ar.OutputFiles[0].Path = si
			if diff := cmp.Diff(ar, got, cmp.Comparer(proto.Equal)); diff != "" {
				t.Errorf("LoadActionCache(...) gave diff on action result (-want +got):\n%s", diff)
			}
		}
		if loaded != (i > 0) {
			t.Errorf("expected loaded to be %v for %s from the cache", i > 0, dg)
		}
	}
	stats := d.GetStats()
	if stats.TotalNumFiles != 4 {
		t.Errorf("expected TotalNumFiles to be 4, got %d", stats.TotalNumFiles)
	}
	if stats.NumFilesStored != 5 {
		t.Errorf("expected NumFilesStored to be 5, got %d", stats.NumFilesStored)
	}
	if stats.TotalStoredBytes != int64(size)*5 {
		t.Errorf("expected TotalStoredBytes to be %d, got %d", size*5, stats.TotalStoredBytes)
	}
	if stats.NumCacheHits != 4 {
		t.Errorf("expected NumCacheHits to be 4, got %d", stats.NumCacheHits)
	}
	if stats.TotalCacheHitSizeBytes != int64(size)*4 {
		t.Errorf("expected TotalCacheHitSizeBytes to be %d, got %d", size*4, stats.TotalCacheHitSizeBytes)
	}
	if stats.NumCacheMisses != 1 {
		t.Errorf("expected NumCacheMisses to be 1, got %d", stats.NumCacheMisses)
	}
	if stats.NumFilesGCed != 1 {
		t.Errorf("expected NumFilesGCed to be 1, got %d", stats.NumFilesGCed)
	}
	if stats.TotalGCedSizeBytes != int64(size) {
		t.Errorf("expected TotalGCedSizeBytes to be %d, got %d", size, stats.TotalGCedSizeBytes)
	}
	if uint64(stats.TotalSizeBytes) != d.maxCapacityBytes {
		t.Errorf("expected total size bytes to be %d, got %d", d.maxCapacityBytes, stats.TotalSizeBytes)
	}
	if stats.InitTime <= 0 {
		t.Errorf("expected InitTime to be > 0")
	}
	if stats.TotalGCTime <= 0 {
		t.Errorf("expected TotalGCTime to be > 0")
	}
}

func getLastAccessTime(path string) (time.Time, error) {
	info, err := os.Stat(path)
	if err != nil {
		return time.Time{}, err
	}
	return fileInfoToAccessTime(info), nil
}

// We say that Last Access Time is behaving accurately on a system if reading from the file
// bumps the LAT time forward. From experience, Mac and Linux Debian are accurate. Ubuntu -- not.
// From experience, even when the LAT gets modified on access on Ubuntu, it can be imprecise to
// an order of seconds (!).
func isSystemLastAccessTimeAccurate(t *testing.T) bool {
	t.Helper()
	fname, _ := testutil.CreateFile(t, false, "foo")
	lat, _ := getLastAccessTime(fname)
	if _, err := os.ReadFile(fname); err != nil {
		t.Fatalf("%v", err)
	}
	newLat, _ := getLastAccessTime(fname)
	return lat.Before(newLat)
}

func TestInitFromExistingCas(t *testing.T) {
	if !isSystemLastAccessTimeAccurate(t) {
		// This effectively skips the test on Ubuntu, because to make the test work there,
		// we would need to inject too many / too long time.Sleep statements to beat the system's
		// inaccuracy.
		t.Logf("Skipping TestInitFromExisting, because system Last Access Time is unreliable.")
		return
	}
	root := t.TempDir()
	d, err := New(context.Background(), filepath.Join(root, "cache"), 20)
	if err != nil {
		t.Errorf("New: %v", err)
	}
	for i := 0; i < 4; i++ {
		fname, _ := testutil.CreateFile(t, false, fmt.Sprintf("aaa %d", i))
		dg, err := digest.NewFromFile(fname)
		if err != nil {
			t.Fatalf("digest.NewFromFile failed: %v", err)
		}
		if err := d.StoreCas(dg, fname); err != nil {
			t.Errorf("StoreCas(%s, %s) failed: %v", dg, fname, err)
		}
	}
	newName := filepath.Join(root, "new")
	dg := digest.NewFromBlob([]byte("aaa 0"))
	if !d.LoadCas(dg, newName) { // Now 0 has been accessed, 1 is the oldest file.
		t.Errorf("expected %s to be cached", dg)
	}
	d.Shutdown()

	// Re-initialize from existing files.
	d, err = New(context.Background(), filepath.Join(root, "cache"), 20)
	if err != nil {
		t.Errorf("New: %v", err)
	}

	// Check old files are cached:
	dg = digest.NewFromBlob([]byte("aaa 1"))
	if !d.LoadCas(dg, newName) { // Now 1 has been accessed, 2 is the oldest file.
		t.Errorf("expected %s to be cached", dg)
	}
	fname, _ := testutil.CreateFile(t, false, "aaa 4")
	dg, err = digest.NewFromFile(fname)
	if err != nil {
		t.Fatalf("digest.NewFromFile failed: %v", err)
	}
	// Trigger a GC by adding a new file.
	if err := d.StoreCas(dg, fname); err != nil {
		t.Errorf("StoreCas(%s, %s) failed: %v", dg, fname, err)
	}
	d.Shutdown()
	dg = digest.NewFromBlob([]byte("aaa 2"))
	if d.LoadCas(dg, newName) {
		t.Errorf("expected to not load %s from the cache to %s", dg, newName)
	}
	stats := d.GetStats()
	if stats.TotalNumFiles != 4 {
		t.Errorf("expected TotalNumFiles to be 4, got %d", stats.TotalNumFiles)
	}
	if stats.NumFilesStored != 1 {
		t.Errorf("expected NumFilesStored to be 1, got %d", stats.NumFilesStored)
	}
	if stats.TotalStoredBytes != 5 {
		t.Errorf("expected TotalStoredBytes to be 5, got %d", stats.TotalStoredBytes)
	}
	if stats.NumCacheHits != 1 {
		t.Errorf("expected NumCacheHits to be 1, got %d", stats.NumCacheHits)
	}
	if stats.TotalCacheHitSizeBytes != 5 {
		t.Errorf("expected TotalCacheHitSizeBytes to be 5, got %d", stats.TotalCacheHitSizeBytes)
	}
	if stats.NumCacheMisses != 1 {
		t.Errorf("expected NumCacheMisses to be 1, got %d", stats.NumCacheMisses)
	}
	if stats.NumFilesGCed != 1 {
		t.Errorf("expected NumFilesGCed to be 1, got %d", stats.NumFilesGCed)
	}
	if stats.TotalGCedSizeBytes != 5 {
		t.Errorf("expected TotalGCedSizeBytes to be 5, got %d", stats.TotalGCedSizeBytes)
	}
	if uint64(stats.TotalSizeBytes) != d.maxCapacityBytes {
		t.Errorf("expected total size bytes to be %d, got %d", d.maxCapacityBytes, stats.TotalSizeBytes)
	}
	if stats.InitTime <= 0 {
		t.Errorf("expected InitTime to be > 0")
	}
	if stats.TotalGCTime <= 0 {
		t.Errorf("expected TotalGCTime to be > 0")
	}
}

func TestThreadSafetyCas(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "orig"), os.ModePerm); err != nil {
		t.Fatalf("%v", err)
	}
	if err := os.MkdirAll(filepath.Join(root, "new"), os.ModePerm); err != nil {
		t.Fatalf("%v", err)
	}
	nFiles := 10
	attempts := 5000
	// All blobs are size 5 exactly. We will have half the byte capacity we need.
	d, err := New(context.Background(), filepath.Join(root, "cache"), uint64(nFiles*5)/2)
	if err != nil {
		t.Errorf("New: %v", err)
	}
	var files []string
	var dgs []digest.Digest
	for i := 0; i < nFiles; i++ {
		fname := filepath.Join(root, "orig", fmt.Sprintf("%d", i))
		if err := os.WriteFile(fname, []byte(fmt.Sprintf("aa %02d", i)), 0644); err != nil {
			t.Fatalf("os.WriteFile: %v", err)
		}
		files = append(files, fname)
		dg, err := digest.NewFromFile(fname)
		if err != nil {
			t.Fatalf("digest.NewFromFile failed: %v", err)
		}
		dgs = append(dgs, dg)
		if err := d.StoreCas(dg, fname); err != nil {
			t.Errorf("StoreCas(%s, %s) failed: %v", dg, fname, err)
		}
	}
	// Randomly access and store files from different threads.
	eg := errgroup.Group{}
	var hits uint64
	var runs []int
	for k := 0; k < attempts; k++ {
		eg.Go(func() error {
			i := rand.Intn(nFiles)
			runs = append(runs, i)
			newName := filepath.Join(root, "new", uuid.New().String())
			if d.LoadCas(dgs[i], newName) {
				atomic.AddUint64(&hits, 1)
				contents, err := os.ReadFile(newName)
				if err != nil {
					return fmt.Errorf("os.ReadFile: %v", err)
				}
				want := fmt.Sprintf("aa %02d", i)
				if string(contents) != want {
					return fmt.Errorf("Cached result did not match: want %q, got %q for digest %v", want, string(contents), dgs[i])
				}
			} else if err := d.StoreCas(dgs[i], files[i]); err != nil {
				return fmt.Errorf("StoreCas: %v", err)
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		t.Error(err)
	}
	d.Shutdown()
	if int(hits) < attempts/2 {
		t.Errorf("Unexpectedly low cache hits %d out of %d attempts", hits, attempts)
	}
	stats := d.GetStats()
	if stats.TotalNumFiles != 5 {
		t.Errorf("expected TotalNumFiles to be 5, got %d", stats.TotalNumFiles)
	}
	if uint64(stats.NumCacheHits) != hits {
		t.Errorf("expected NumCacheHits to be %d, got %d", hits, stats.NumCacheHits)
	}
	if uint64(stats.TotalCacheHitSizeBytes) != hits*5 {
		t.Errorf("expected TotalCacheHitSizeBytes to be %d, got %d", hits*5, stats.TotalCacheHitSizeBytes)
	}
	if stats.NumCacheMisses+stats.NumCacheHits != int64(attempts) {
		t.Errorf("expected NumCacheHits+NumCacheMisses to be %d, got %d", attempts, stats.NumCacheMisses+stats.NumCacheHits)
	}
	// This is less or equal because of multiple concurrent Stores.
	if stats.NumFilesStored > int64(nFiles)+stats.NumCacheMisses {
		t.Errorf("expected NumFilesStored to be <= %d, got %d", int64(nFiles)+stats.NumCacheMisses, stats.NumFilesStored)
	}
	if stats.TotalStoredBytes != 5*stats.NumFilesStored {
		t.Errorf("expected TotalStoredBytes to be %d, got %d", 5*stats.NumFilesStored, stats.TotalStoredBytes)
	}
	if stats.NumFilesGCed <= 0 {
		t.Errorf("expected NumFilesGCed to be > 0")
	}
	if stats.TotalGCedSizeBytes <= 0 {
		t.Errorf("expected TotalGCedSizeBytes to be > 0")
	}
	if uint64(stats.TotalSizeBytes) != d.maxCapacityBytes {
		t.Errorf("expected total size bytes to be %d, got %d", d.maxCapacityBytes, stats.TotalSizeBytes)
	}
	if stats.InitTime <= 0 {
		t.Errorf("expected InitTime to be > 0")
	}
	if stats.TotalGCTime <= 0 {
		t.Errorf("expected TotalGCTime to be > 0")
	}
}
