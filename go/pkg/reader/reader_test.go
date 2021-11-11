package reader

import (
	"bytes"
	"io"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/testutil"
	"github.com/google/go-cmp/cmp"
	"github.com/klauspost/compress/zstd"
)

func TestFileReaderSeeks(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		IOBuffSize   int
		dataBuffSize int
		blob         string
		seekOffset   int64
		wantResetErr error
	}{
		{
			name:         "Smaller data buffer",
			IOBuffSize:   10,
			dataBuffSize: 3,
			blob:         "1234567",
			seekOffset:   2,
			wantResetErr: nil,
		},
		{
			name:         "Smaller io buffer",
			IOBuffSize:   1,
			dataBuffSize: 3,
			blob:         "1234567",
			seekOffset:   2,
			wantResetErr: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path, err := testutil.CreateFile(t, false, tc.blob)
			if err != nil {
				t.Fatalf("Failed to make temp file: %v", err)
			}

			data := make([]byte, tc.dataBuffSize)

			r := NewFileReadSeeker(path, tc.IOBuffSize)
			defer r.Close()
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
			if diff := cmp.Diff(string(data), tc.blob[:tc.dataBuffSize]); diff != "" {
				t.Errorf("Read() = incorrect result, diff(-want, +got): %v", diff)
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
			if diff := cmp.Diff(string(data), tc.blob[tc.seekOffset:endRead]); diff != "" {
				t.Errorf("Read() = incorrect result, diff(-want, +got): %v", diff)
			}
		})
	}

}

func TestFileReaderSeeksPastOffset(t *testing.T) {
	t.Parallel()
	path, err := testutil.CreateFile(t, false, "12345")
	if err != nil {
		t.Fatalf("Failed to make temp file: %v", err)
	}

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
		blob string
	}{
		{
			name: "basic",
			blob: "12345",
		},
		{
			name: "looong",
			blob: "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
		},
		{
			name: "empty blob",
			blob: "",
		},
		{
			name: "1MB zero blob",
			blob: string(make([]byte, 1*1024*1024)),
		},
	}

	for _, tc := range tests {
		name := tc.name
		blob := tc.blob
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			path, err := testutil.CreateFile(t, false, blob)
			if err != nil {
				t.Fatalf("Failed to initialize temp file: %v", err)
			}

			buf := bytes.NewBuffer([]byte{})
			encd, err := zstd.NewWriter(buf)
			if err != nil {
				t.Fatalf("Failed to initialize compressor: %v", err)
			}
			if _, err = encd.Write([]byte(blob)); err != nil {
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
			// larger than the original - even more for these small blobs
			// that are barelly compressible and will need metadata.
			fullData := make([]byte, len(blob)+100)
			dataBuf := make([]byte, 5)
			var n, m int
			for err = nil; err == nil; {
				m, err = r.Read(dataBuf)
				copy(fullData[n:], dataBuf)
				n += m
			}
			if err != io.EOF {
				t.Errorf("Expected EOF, got nil")
			}

			if diff := cmp.Diff(compressedData[:buf.Len()], fullData[:buf.Len()]); diff != "" {
				t.Errorf("Read() = incorrect result, diff(-want, +got): %v", diff)
			}
		})
	}
}
