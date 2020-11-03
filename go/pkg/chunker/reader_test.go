package chunker

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func setTmpFile(t *testing.T, content []byte) (string, func()) {
	t.Helper()
	execRoot, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("failed to make temp dir: %v", err)
	}

	path := filepath.Join(execRoot, "file")
	if err := ioutil.WriteFile(path, content, 0777); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}

	clean := func() {
		os.RemoveAll(execRoot)
	}

	return path, clean
}

func TestFileReaderSeeks(t *testing.T) {
	tests := []struct {
		name         string
		IOBuffSize   int
		dataBuffSize int
		blob         []byte
		seekOffset   int64
		wantResetErr error
	}{
		{
			name:         "Smaller data buffer",
			IOBuffSize:   10,
			dataBuffSize: 3,
			blob:         []byte("1234567"),
			seekOffset:   2,
			wantResetErr: nil,
		},
		{
			name:         "Smaller io buffer",
			IOBuffSize:   1,
			dataBuffSize: 3,
			blob:         []byte("1234567"),
			seekOffset:   2,
			wantResetErr: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path, clean := setTmpFile(t, tc.blob)
			defer clean()

			data := make([]byte, tc.dataBuffSize)

			r := NewFileReadSeeker(path, tc.IOBuffSize)
			if _, err := r.Read(data); err == nil {
				t.Errorf("Read() = should have err'd on unitialized reader")
			}
			if err := r.Initialize(); err != nil {
				t.Fatalf("Failed to initialize reader: %v", err)
			}

			n, err := io.ReadFull(r, data)
			if n != tc.dataBuffSize {
				t.Errorf("Read() = %d bytes, expected %d", n, tc.dataBuffSize)
			}
			if err != nil {
				t.Errorf("Read() = %v err, expected nil", err)
			}
			if diff := cmp.Diff(data, tc.blob[:tc.dataBuffSize]); diff != "" {
				t.Errorf("Read() = incorrect result, diff(-wang, +got): %v", diff)
			}

			r.Seek(tc.seekOffset)
			if _, err := r.Read(data); err == nil {
				t.Errorf("Read() = should have err'd on unitialized reader")
			}
			if err := r.Initialize(); err != nil {
				t.Fatalf("Failed to initialize reader: %v", err)
			}

			n, err = io.ReadFull(r, data)
			if n != tc.dataBuffSize {
				t.Errorf("Read() = %d bytes, expected %d", n, tc.dataBuffSize)
			}
			if err != nil {
				t.Errorf("Read() = %v err, expected nil", err)
			}
			endRead := int(tc.seekOffset) + tc.dataBuffSize
			if endRead > len(tc.blob) {
				endRead = len(tc.blob)
			}
			if diff := cmp.Diff(data, tc.blob[tc.seekOffset:endRead]); diff != "" {
				t.Errorf("Read() = incorrect result, diff(-want, +got): %v", diff)
			}
		})
	}

}

func TestFileReaderSeeksPastOffset(t *testing.T) {
	path, clean := setTmpFile(t, []byte("12345"))
	defer clean()

	r := NewFileReadSeeker(path, 10)
	// Past Offset
	r.Seek(10)
	if err := r.Initialize(); err != nil {
		t.Fatalf("Failed to initialize reader: %v", err)
	}

	data := make([]byte, 1)
	if _, err := r.Read(data); err == nil {
		t.Errorf("Expected err, got nil")
	}
}
