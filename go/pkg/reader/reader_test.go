package reader

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/klauspost/compress/zstd"
)

func setTmpFile(t *testing.T, content []byte) (string, func()) {
	t.Helper()
	execRoot, err := ioutil.TempDir("", strings.Replace(t.Name(), "/", "_", -1))
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
	t.Parallel()
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
			t.Parallel()
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

			r.SeekOffset(tc.seekOffset)
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
	t.Parallel()
	path, clean := setTmpFile(t, []byte("12345"))
	defer clean()

	r := NewFileReadSeeker(path, 10)
	// Past Offset
	r.SeekOffset(10)
	if err := r.Initialize(); err != nil {
		t.Fatalf("Failed to initialize reader: %v", err)
	}

	data := make([]byte, 1)
	if _, err := r.Read(data); err == nil {
		t.Errorf("Expected err, got nil")
	}
}

func TestCompressedReader(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		blob []byte
	}{
		{
			name: "basic",
			blob: []byte("12345"),
		},
		{
			name: "looong",
			blob: []byte("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."),
		},
		{
			name: "empty blob",
			blob: []byte(""),
		},
	}

	for _, tc := range tests {
		name := tc.name
		blob := tc.blob
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			path, clean := setTmpFile(t, blob)
			defer clean()

			buf := bytes.NewBuffer(nil)
			encd, err := zstd.NewWriter(buf)
			if err != nil {
				t.Fatalf("Failed to initialize compressor: %v", err)
			}
			if _, err = encd.Write(blob); err != nil {
				t.Fatalf("Failed to compress data: %v", err)
			}
			if err = encd.Close(); err != nil {
				t.Fatalf("Failed to finish compressing data: %v", err)
			}
			compressedData := buf.Bytes()

			r, err := NewCompressedFileSeeker(path, 10)
			if err != nil {
				t.Fatalf("Failed to initialize compressor reader: %v", err)
			}
			if err := r.Initialize(); err != nil {
				t.Fatalf("Failed to initialize reader: %v", err)
			}

			// It is theoretically possible for the compressed data to be
			// larger than the original
			data := make([]byte, len(blob)+100)
			var n, m int
			for err = nil; err == nil; m, err = r.Read(data[n:]) {
				n += m
			}
			if err != io.EOF {
				t.Errorf("Expected err, got nil")
			}

			if diff := cmp.Diff(compressedData[:n], data[:n]); diff != "" {
				t.Errorf("Read() = incorrect result, diff(-want, +got): %v", diff)
			}
		})
	}
}
