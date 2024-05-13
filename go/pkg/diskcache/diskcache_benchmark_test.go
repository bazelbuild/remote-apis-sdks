package diskcache

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"golang.org/x/sync/errgroup"
)

type BenchmarkParams struct {
	Name             string
	MaxConcurrency   int           // Number of concurrent threads for requests.
	CacheMissCost    time.Duration // Similates a remote execution / fetch.
	CacheSizeBytes   uint64
	FileSizeBytes    int // All files have this size.
	NumRequests      int // Total number of cache load/store requests.
	TotalNumFiles    int // NumRequests will repeat over this set.
	NumExistingFiles int // Affects initialization time.
}

var kBenchmarks = []BenchmarkParams{
	{
		Name:             "AllGC_Small",
		MaxConcurrency:   100,
		CacheMissCost:    100 * time.Millisecond,
		CacheSizeBytes:   20000,
		FileSizeBytes:    100,
		NumRequests:      10000,
		TotalNumFiles:    500,
		NumExistingFiles: 0,
	},
	{
		Name:             "AllGC_Medium",
		MaxConcurrency:   100,
		CacheMissCost:    100 * time.Millisecond,
		CacheSizeBytes:   2000000,
		FileSizeBytes:    10000,
		NumRequests:      10000,
		TotalNumFiles:    500,
		NumExistingFiles: 0,
	},
	{
		Name:             "AllGC_Large",
		MaxConcurrency:   100,
		CacheMissCost:    100 * time.Millisecond,
		CacheSizeBytes:   2000000000,
		FileSizeBytes:    10000000,
		NumRequests:      5000,
		TotalNumFiles:    500,
		NumExistingFiles: 0,
	},
	{
		Name:             "AllCacheHits_Small",
		MaxConcurrency:   100,
		CacheMissCost:    100 * time.Millisecond,
		CacheSizeBytes:   50000,
		FileSizeBytes:    100,
		NumRequests:      20000,
		TotalNumFiles:    500,
		NumExistingFiles: 500,
	},
	{
		Name:             "AllCacheHits_Medium",
		MaxConcurrency:   100,
		CacheMissCost:    100 * time.Millisecond,
		CacheSizeBytes:   50000000,
		FileSizeBytes:    100000,
		NumRequests:      20000,
		TotalNumFiles:    500,
		NumExistingFiles: 500,
	},
	{
		Name:             "AllCacheHits_Large",
		MaxConcurrency:   100,
		CacheMissCost:    100 * time.Millisecond,
		CacheSizeBytes:   5000000000,
		FileSizeBytes:    10000000,
		NumRequests:      5000,
		TotalNumFiles:    500,
		NumExistingFiles: 500,
	},
}

func getFilename(i int) string {
	return fmt.Sprintf("f_%05d", i)
}

func TestRunAllBenchmarks(t *testing.T) {
	for _, b := range kBenchmarks {
		t.Run(b.Name, func(t *testing.T) {
			root := t.TempDir()
			fmt.Printf("Initializing source files for benchmark %s...\n", b.Name)
			start := time.Now()
			source := filepath.Join(root, "source")
			if err := os.MkdirAll(source, 0777); err != nil {
				t.Fatalf("%v", err)
			}
			dgs := make([]digest.Digest, b.TotalNumFiles)
			for i := 0; i < b.TotalNumFiles; i++ {
				filename := filepath.Join(source, getFilename(i))
				var s strings.Builder
				fmt.Fprintf(&s, "%d\n", i)
				for k := s.Len(); k < b.FileSizeBytes; k++ {
					s.WriteByte(0)
				}
				blob := []byte(s.String())
				dgs[i] = digest.NewFromBlob(blob)
				if err := os.WriteFile(filename, blob, 00666); err != nil {
					t.Fatalf("os.WriteFile(%s): %v", filename, err)
				}
			}

			cacheDir := filepath.Join(root, "cache")
			d, err := New(context.Background(), cacheDir, b.CacheSizeBytes)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			// Pre-populate the cache if requested:
			if b.NumExistingFiles > 0 {
				fmt.Printf("Pre-warming cache for benchmark %s...\n", b.Name)
				for i := 0; i < b.NumExistingFiles; i++ {
					fname := filepath.Join(source, getFilename(i))
					if err := d.StoreCas(dgs[i], fname); err != nil {
						t.Fatalf("StoreCas(%s, %s) failed: %v", dgs[i], fname, err)
					}
				}
				d.Shutdown()
				d, err = New(context.Background(), cacheDir, b.CacheSizeBytes)
				if err != nil {
					t.Fatalf("New: %v", err)
				}
			}
			// Run the simulation: store on every cache miss.
			new := filepath.Join(root, "new")
			if err := os.MkdirAll(new, 0777); err != nil {
				t.Fatalf("%v", err)
			}
			fmt.Printf("Finished initialization for benchmark %s, duration %v\n", b.Name, time.Since(start))
			eg := errgroup.Group{}
			eg.SetLimit(b.MaxConcurrency)
			fmt.Printf("Starting benchmark %s...\n", b.Name)
			start = time.Now()
			for k := 0; k < b.NumRequests; k++ {
				k := k
				eg.Go(func() error {
					i := k % b.TotalNumFiles
					newName := filepath.Join(new, getFilename(k))
					if d.LoadCas(dgs[i], newName) {
						if dg, err := digest.NewFromFile(newName); dg != dgs[i] || err != nil {
							return fmt.Errorf("%d: err %v or digest mismatch %v vs %v", k, err, dg, dgs[i])
						}
					} else {
						time.Sleep(b.CacheMissCost)
						if err := d.StoreCas(dgs[i], filepath.Join(source, getFilename(i))); err != nil {
							return fmt.Errorf("StoreCas: %v", err)
						}
					}
					return nil
				})
			}
			if err := eg.Wait(); err != nil {
				t.Fatalf("%v", err)
			}
			d.Shutdown()
			fmt.Printf("Finished benchmark %s, total duration %v, stats:\n%+v\n", b.Name, time.Since(start), d.GetStats())
		})
	}
}
