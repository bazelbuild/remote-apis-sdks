package client

import (
	"testing"
)

func TestCapToLimit(t *testing.T) {
	type testCase struct {
		name  string
		limit int
		input *requestMetadata
		want  *requestMetadata
	}
	tests := []testCase{
		{
			name:  "under limit",
			limit: 24,
			input: &requestMetadata{
				toolName:     "toolName",
				actionID:     "actionID",
				invocationID: "invocID*",
			},
			want: &requestMetadata{
				toolName:     "toolName",
				actionID:     "actionID",
				invocationID: "invocID*",
			},
		},
		{
			name:  "actionID over limit",
			limit: 24,
			input: &requestMetadata{
				toolName:     "toolName",
				actionID:     "actionID-12345678",
				invocationID: "invocID*",
			},
			want: &requestMetadata{
				toolName:     "toolName",
				actionID:     "actionID",
				invocationID: "invocID*",
			},
		},
		{
			name:  "invocationID over limit",
			limit: 24,
			input: &requestMetadata{
				toolName:     "toolName",
				actionID:     "actionID",
				invocationID: "invocID*-12345678",
			},
			want: &requestMetadata{
				toolName:     "toolName",
				actionID:     "actionID",
				invocationID: "invocID*",
			},
		},
		{
			name:  "both equally over limit",
			limit: 24,
			input: &requestMetadata{
				toolName:     "toolName",
				actionID:     "actionID-12345678",
				invocationID: "invocID*-12345678",
			},
			want: &requestMetadata{
				toolName:     "toolName",
				actionID:     "actionID",
				invocationID: "invocID*",
			},
		},
		{
			name:  "both over limit but actionID is bigger",
			limit: 24,
			input: &requestMetadata{
				toolName:     "toolName",
				actionID:     "actionID-123456789012345678",
				invocationID: "invocID*-12345678",
			},
			want: &requestMetadata{
				toolName:     "toolName",
				actionID:     "actionID",
				invocationID: "invocID*",
			},
		},
		{
			name:  "both over limit but invocationID is bigger",
			limit: 24,
			input: &requestMetadata{
				toolName:     "toolName",
				actionID:     "actionID-12345678",
				invocationID: "invocID*-123456789012345678",
			},
			want: &requestMetadata{
				toolName:     "toolName",
				actionID:     "actionID",
				invocationID: "invocID*",
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
