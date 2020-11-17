package chunker

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

var tests = []struct {
	name       string
	blob       []byte
	wantChunks []*Chunk
	chunkSize  int
}{
	{
		name:       "empty",
		wantChunks: []*Chunk{&Chunk{}},
		chunkSize:  3,
	},
	{
		name:       "one",
		blob:       []byte("12"),
		wantChunks: []*Chunk{&Chunk{Data: []byte("12")}},
		chunkSize:  3,
	},
	{
		name:       "one-even",
		blob:       []byte("123"),
		wantChunks: []*Chunk{&Chunk{Data: []byte("123")}},
		chunkSize:  3,
	},
	{
		name: "two",
		blob: []byte("12345"),
		wantChunks: []*Chunk{
			&Chunk{Data: []byte("123")},
			&Chunk{Data: []byte("45"), Offset: 3},
		},
		chunkSize: 3,
	},
	{
		name: "three-even",
		blob: []byte("123456789"),
		wantChunks: []*Chunk{
			&Chunk{Data: []byte("123")},
			&Chunk{Data: []byte("456"), Offset: 3},
			&Chunk{Data: []byte("789"), Offset: 6},
		},
		chunkSize: 3,
	},
	{
		name: "three",
		blob: []byte("123456789"),
		wantChunks: []*Chunk{
			&Chunk{Data: []byte("1234")},
			&Chunk{Data: []byte("5678"), Offset: 4},
			&Chunk{Data: []byte("9"), Offset: 8},
		},
		chunkSize: 4,
	},
	{
		name: "many",
		blob: []byte("1234567890abcdefghijklmnopqrstuvwxyz!"),
		wantChunks: []*Chunk{
			&Chunk{Data: []byte("1234")},
			&Chunk{Data: []byte("5678"), Offset: 4},
			&Chunk{Data: []byte("90ab"), Offset: 8},
			&Chunk{Data: []byte("cdef"), Offset: 12},
			&Chunk{Data: []byte("ghij"), Offset: 16},
			&Chunk{Data: []byte("klmn"), Offset: 20},
			&Chunk{Data: []byte("opqr"), Offset: 24},
			&Chunk{Data: []byte("stuv"), Offset: 28},
			&Chunk{Data: []byte("wxyz"), Offset: 32},
			&Chunk{Data: []byte("!"), Offset: 36},
		},
		chunkSize: 4,
	},
}

var bufferSizes = []int{3, 4, 8, 100}

func TestChunkerFromBlob(t *testing.T) {
	t.Parallel()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ue := EntryFromBlob(tc.blob)
			c, err := New(ue, false, tc.chunkSize)
			if err != nil {
				t.Fatalf("Could not make chunker from UEntry: %v", err)
			}
			var gotChunks []*Chunk
			for _, wantChunk := range tc.wantChunks {
				if !c.HasNext() {
					t.Errorf("%s: c.HasNext() was false on blob %q , expecting next chunk %q", tc.name, tc.blob, string(wantChunk.Data))
				}
				got, err := c.Next()
				if err != nil {
					t.Errorf("%s: c.Next() gave error %v on blob %q , expecting next chunk %q", tc.name, err, tc.blob, string(wantChunk.Data))
				}
				gotChunks = append(gotChunks, got)
			}
			if diff := cmp.Diff(tc.wantChunks, gotChunks); diff != "" {
				t.Errorf("%s: Chunker gave result diff (-want +got):\n%s", tc.name, diff)
			}
		})
	}
}

func TestChunkerFromFile(t *testing.T) {
	execRoot, err := ioutil.TempDir("", t.Name())
	if err != nil {
		t.Fatalf("failed to make temp dir: %v", err)
	}
	defer os.RemoveAll(execRoot)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(execRoot, tc.name)
			if err := ioutil.WriteFile(path, tc.blob, 0777); err != nil {
				t.Fatalf("failed to write temp file: %v", err)
			}
			for _, bufSize := range bufferSizes {
				if bufSize < tc.chunkSize {
					continue
				}
				dg := digest.NewFromBlob(tc.blob)
				IOBufferSize = bufSize
				ue := EntryFromFile(dg, path)
				c, err := New(ue, false, tc.chunkSize)
				if err != nil {
					t.Fatalf("Could not make chunker from UEntry: %v", err)
				}
				var gotChunks []*Chunk
				for _, wantChunk := range tc.wantChunks {
					if !c.HasNext() {
						t.Errorf("%s: c.HasNext() was false on blob %q buffer size %d, expecting next chunk %q", tc.name, tc.blob, bufSize, string(wantChunk.Data))
					}
					got, err := c.Next()
					if err != nil {
						t.Errorf("%s: c.Next() gave error %v on blob %q buffer size %d, expecting next chunk %q", tc.name, err, tc.blob, bufSize, string(wantChunk.Data))
					}
					gotChunks = append(gotChunks, got)
				}
				if diff := cmp.Diff(tc.wantChunks, gotChunks); diff != "" {
					t.Errorf("%s: Chunker buffer size %d gave result diff (-want +got):\n%s", tc.name, bufSize, diff)
				}
			}
		})
	}
}

func TestChunkerFullData(t *testing.T) {
	t.Parallel()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ue := EntryFromBlob(tc.blob)
			c, err := New(ue, false, tc.chunkSize)
			if err != nil {
				t.Fatalf("Could not make chunker from UEntry: %v", err)
			}
			gotBlob, err := c.FullData()
			if err != nil {
				t.Errorf("c.FullData() gave error %v on blob %q", err, tc.blob)
			}
			if diff := cmp.Diff(tc.blob, gotBlob, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("FullData gave result diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestChunkerFromBlob_Reset(t *testing.T) {
	t.Parallel()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for reset := 1; reset < len(tc.wantChunks); reset++ {
				ue := EntryFromBlob(tc.blob)
				c, err := New(ue, false, tc.chunkSize)
				if err != nil {
					t.Fatalf("Could not make chunker from UEntry: %v", err)
				}
				var gotChunks []*Chunk
				for i, wantChunk := range tc.wantChunks {
					if !c.HasNext() {
						t.Errorf("%s: c.HasNext() was false on blob %q , expecting next chunk %q", tc.name, tc.blob, string(wantChunk.Data))
					}
					got, err := c.Next()
					if err != nil {
						t.Errorf("%s: c.Next() gave error %v on blob %q , expecting next chunk %q", tc.name, err, tc.blob, string(wantChunk.Data))
					}
					gotChunks = append(gotChunks, got)
					if i == reset {
						c.Reset()
						break
					}
				}
				if diff := cmp.Diff(tc.wantChunks[:len(gotChunks)], gotChunks); diff != "" {
					t.Errorf("%s: Chunker gave result diff (-want +got):\n%s", tc.name, diff)
				}
				gotChunks = nil
				if reset >= len(tc.wantChunks) {
					continue
				}
				for _, wantChunk := range tc.wantChunks {
					if !c.HasNext() {
						t.Errorf("%s: c.HasNext() was false on blob %q , expecting next chunk %q", tc.name, tc.blob, string(wantChunk.Data))
					}
					got, err := c.Next()
					if err != nil {
						t.Errorf("%s: c.Next() gave error %v on blob %q , expecting next chunk %q", tc.name, err, tc.blob, string(wantChunk.Data))
					}
					gotChunks = append(gotChunks, got)
				}
				if diff := cmp.Diff(tc.wantChunks, gotChunks); diff != "" {
					t.Errorf("%s: Chunker gave result diff (-want +got):\n%s", tc.name, diff)
				}
			}
		})
	}
}

func TestChunkerFromFile_Reset(t *testing.T) {
	execRoot, err := ioutil.TempDir("", t.Name())
	if err != nil {
		t.Fatalf("failed to make temp dir: %v", err)
	}
	defer os.RemoveAll(execRoot)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(execRoot, tc.name)
			if err := ioutil.WriteFile(path, tc.blob, 0777); err != nil {
				t.Fatalf("failed to write temp file: %v", err)
			}
			for _, bufSize := range bufferSizes {
				if bufSize < tc.chunkSize {
					continue
				}
				dg := digest.NewFromBlob(tc.blob)
				IOBufferSize = bufSize
				for reset := 1; reset < len(tc.wantChunks); reset++ {
					ue := EntryFromFile(dg, path)
					c, err := New(ue, false, tc.chunkSize)
					if err != nil {
						t.Fatalf("Could not make chunker from UEntry: %v", err)
					}
					var gotChunks []*Chunk
					for i, wantChunk := range tc.wantChunks {
						if !c.HasNext() {
							t.Errorf("%s: c.HasNext() was false on blob %q buffer size %d, expecting next chunk %q", tc.name, tc.blob, bufSize, string(wantChunk.Data))
						}
						got, err := c.Next()
						if err != nil {
							t.Errorf("%s: c.Next() gave error %v on blob %q buffer size %d, expecting next chunk %q", tc.name, err, tc.blob, bufSize, string(wantChunk.Data))
						}
						gotChunks = append(gotChunks, got)
						if i == reset {
							c.Reset()
							break
						}
					}
					if diff := cmp.Diff(tc.wantChunks[:len(gotChunks)], gotChunks); diff != "" {
						t.Errorf("%s: Chunker buffer size %d gave result diff (-want +got):\n%s", tc.name, bufSize, diff)
					}
					gotChunks = nil
					if reset >= len(tc.wantChunks) {
						continue
					}
					for _, wantChunk := range tc.wantChunks {
						if !c.HasNext() {
							t.Errorf("%s: c.HasNext() was false on blob %q buffer size %d, expecting next chunk %q", tc.name, tc.blob, bufSize, string(wantChunk.Data))
						}
						got, err := c.Next()
						if err != nil {
							t.Errorf("%s: c.Next() gave error %v on blob %q buffer size %d, expecting next chunk %q", tc.name, err, tc.blob, bufSize, string(wantChunk.Data))
						}
						gotChunks = append(gotChunks, got)
					}
					if diff := cmp.Diff(tc.wantChunks, gotChunks); diff != "" {
						t.Errorf("%s: Chunker buffer size %d gave result diff (-want +got):\n%s", tc.name, bufSize, diff)
					}
				}
			}
		})
	}
}

func TestChunkerErrors_ErrEOF(t *testing.T) {
	ue := EntryFromBlob([]byte("12"))
	c, err := New(ue, false, 2)
	if err != nil {
		t.Fatalf("Could not make chunker from UEntry: %v", err)
	}
	if err != nil {
	}
	_, err = c.Next()
	if err != nil {
		t.Errorf("c.Next() gave error %v, expecting next chunk \"12\"", err)
	}
	got, err := c.Next()
	if err == nil {
		t.Errorf("c.Next() gave %v, %v, expecting _, error", got, err)
	}
}

func TestChunkerResetOptimization_SmallFile(t *testing.T) {
	// Files smaller than IOBufferSize are loaded into memory once and not re-read on Reset.
	execRoot, err := ioutil.TempDir("", t.Name())
	if err != nil {
		t.Fatalf("failed to make temp dir: %v", err)
	}
	defer os.RemoveAll(execRoot)

	blob := []byte("123")
	path := filepath.Join(execRoot, "file")
	if err := ioutil.WriteFile(path, blob, 0777); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	dg := digest.NewFromBlob(blob)
	IOBufferSize = 10
	ue := EntryFromFile(dg, path)
	c, err := New(ue, false, 4)
	if err != nil {
		t.Fatalf("Could not make chunker from UEntry: %v", err)
	}
	got, err := c.Next()
	if err != nil {
		t.Errorf("c.Next() gave error %v", err)
	}
	wantChunk := &Chunk{Data: blob}
	if diff := cmp.Diff(wantChunk, got); diff != "" {
		t.Errorf("c.Next() gave result diff (-want +got):\n%s", diff)
	}
	c.Reset()
	// Change the file contents.
	if err := ioutil.WriteFile(path, []byte("321"), 0777); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	got, err = c.Next()
	if err != nil {
		t.Errorf("c.Next() gave error %v", err)
	}
	if diff := cmp.Diff(wantChunk, got); diff != "" {
		t.Errorf("c.Next() gave result diff (-want +got):\n%s", diff)
	}
}

func TestChunkerResetOptimization_FullData(t *testing.T) {
	// After FullData is called once, the file contents will remain loaded into memory and not
	// re-read on Reset, even if the file is larger than IOBufferSize.
	execRoot, err := ioutil.TempDir("", t.Name())
	if err != nil {
		t.Fatalf("failed to make temp dir: %v", err)
	}
	defer os.RemoveAll(execRoot)

	blob := []byte("12345678")
	path := filepath.Join(execRoot, "file")
	if err := ioutil.WriteFile(path, blob, 0777); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	dg := digest.NewFromBlob(blob)
	IOBufferSize = 5
	ue := EntryFromFile(dg, path)
	c, err := New(ue, false, 3)
	if err != nil {
		t.Fatalf("Could not make chunker from UEntry: %v", err)
	}
	got, err := c.FullData()
	if err != nil {
		t.Errorf("c.FullData() gave error %v", err)
	}
	if !bytes.Equal(got, blob) {
		t.Errorf("c.FullData() gave result diff, want %q, got %q", string(blob), string(got))
	}
	c.Reset()
	// Change the file contents.
	if err := ioutil.WriteFile(path, []byte("987654321"), 0777); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	got, err = c.FullData()
	if err != nil {
		t.Errorf("c.FullData() gave error %v", err)
	}
	if !bytes.Equal(got, blob) {
		t.Errorf("c.FullData() gave result diff, want %q, got %q", string(blob), string(got))
	}
}
