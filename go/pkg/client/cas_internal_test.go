package client

import (
	"bytes"
	"compress/gzip"
	"io"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/uploadinfo"
)

func TestCapToLimit(t *testing.T) {
	type testCase struct {
		name  string
		limit int
		input *ContextMetadata
		want  *ContextMetadata
	}
	tests := []testCase{
		{
			name:  "under limit",
			limit: 24,
			input: &ContextMetadata{
				ToolName:     "toolName",
				ActionID:     "actionID",
				InvocationID: "invocID*",
			},
			want: &ContextMetadata{
				ToolName:     "toolName",
				ActionID:     "actionID",
				InvocationID: "invocID*",
			},
		},
		{
			name:  "actionID over limit",
			limit: 24,
			input: &ContextMetadata{
				ToolName:     "toolName",
				ActionID:     "actionID-12345678",
				InvocationID: "invocID*",
			},
			want: &ContextMetadata{
				ToolName:     "toolName",
				ActionID:     "actionID",
				InvocationID: "invocID*",
			},
		},
		{
			name:  "invocationID over limit",
			limit: 29,
			input: &ContextMetadata{
				ToolName:     "toolName",
				ToolVersion:  "1.2.3",
				ActionID:     "actionID",
				InvocationID: "invocID*-12345678",
			},
			want: &ContextMetadata{
				ToolName:     "toolName",
				ToolVersion:  "1.2.3",
				ActionID:     "actionID",
				InvocationID: "invocID*",
			},
		},
		{
			name:  "both equally over limit",
			limit: 24,
			input: &ContextMetadata{
				ToolName:     "toolName",
				ActionID:     "actionID-12345678",
				InvocationID: "invocID*-12345678",
			},
			want: &ContextMetadata{
				ToolName:     "toolName",
				ActionID:     "actionID",
				InvocationID: "invocID*",
			},
		},
		{
			name:  "both over limit but actionID is bigger",
			limit: 24,
			input: &ContextMetadata{
				ToolName:     "toolName",
				ActionID:     "actionID-123456789012345678",
				InvocationID: "invocID*-12345678",
			},
			want: &ContextMetadata{
				ToolName:     "toolName",
				ActionID:     "actionID",
				InvocationID: "invocID*",
			},
		},
		{
			name:  "both over limit but invocationID is bigger",
			limit: 24,
			input: &ContextMetadata{
				ToolName:     "toolName",
				ActionID:     "actionID-12345678",
				InvocationID: "invocID*-123456789012345678",
			},
			want: &ContextMetadata{
				ToolName:     "toolName",
				ActionID:     "actionID",
				InvocationID: "invocID*",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			capToLimit(tc.input, tc.limit)
			if *tc.input != *tc.want {
				t.Errorf("Got %+v, want %+v", tc.input, tc.want)
			}
		})
	}
}

func TestCompressionClassifier(t *testing.T) {
	c := &Client{}
	threshold := CompressedBytestreamThreshold(1024)
	threshold.Apply(c)
	classifier := UploadCompressionClassifier(DetectArchiveUploads)
	classifier.Apply(c)

	// gz returns a byte slice of the given size
	gz := func(size int) []byte {
		var buf bytes.Buffer
		out := make([]byte, size)
		w := gzip.NewWriter(&buf)
		io.Copy(w, bytes.NewReader(bytes.Repeat([]byte{0}, size)))
		w.Close()
		io.Copy(&buf, bytes.NewReader(bytes.Repeat([]byte{0}, size)))
		return out[:size]
	}

	for _, tc := range []struct {
		name string
		ue uploadinfo.Entry
		expected bool
	}{
		{
			name: "small file",
			ue: uploadinfo.Entry{
				Contents: []byte("testy mctestface"),
				Digest: digest.Digest{Size: 16},
			},
			expected: false,
		},
		{
			name: "large file",
			ue: uploadinfo.Entry{
				Contents: bytes.Repeat([]byte{42}, 3000),
				Digest: digest.Digest{Size: 3000},
			},
			expected: true,
		},
		{
			name: "compressed lerge file",
			ue: uploadinfo.Entry{
				Contents: gz(3000),
				Digest: digest.Digest{Size: 3000},
			},
			expected: true,
		},
		{
			name: "small file name",
			ue: uploadinfo.Entry{
				Digest: digest.Digest{Size: 5},
				Path: "test.txt",
			},
			expected: false,
		},
		{
			name: "compressible file name",
			ue: uploadinfo.Entry{
				Digest: digest.Digest{Size: 3000},
				Path: "test.txt",
			},
			expected: true,
		},
		{
			name: "incompressible file name",
			ue: uploadinfo.Entry{
				Digest: digest.Digest{Size: 3000},
				Path: "test.jpeg",
			},
			expected: false,
		},
	}{
		t.Run(tc.name, func (t *testing.T) {
			if actual := c.shouldCompressUpload(&tc.ue); actual != tc.expected {
				t.Errorf("Got %v, want %v", actual, tc.expected)
			}
		})
	}
}
