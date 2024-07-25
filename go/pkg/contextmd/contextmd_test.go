package contextmd

import (
	"testing"
)

func TestCapToLimit(t *testing.T) {
	type testCase struct {
		name  string
		limit int
		input *Metadata
		want  *Metadata
	}
	tests := []testCase{
		{
			name:  "under limit",
			limit: 32,
			input: &Metadata{
				ToolName:                "toolName",
				ActionID:                "actionID",
				InvocationID:            "invocID*",
				CorrelatedInvocationsID: "12345678",
			},
			want: &Metadata{
				ToolName:                "toolName",
				ActionID:                "actionID",
				InvocationID:            "invocID*",
				CorrelatedInvocationsID: "12345678",
			},
		},
		{
			name:  "actionID over limit",
			limit: 32,
			input: &Metadata{
				ToolName:                "toolName",
				ActionID:                "actionID-12345678",
				InvocationID:            "invocID*",
				CorrelatedInvocationsID: "12345678",
			},
			want: &Metadata{
				ToolName:                "toolName",
				ActionID:                "actionID",
				InvocationID:            "invocID*",
				CorrelatedInvocationsID: "12345678",
			},
		},
		{
			name:  "invocationID over limit",
			limit: 37,
			input: &Metadata{
				ToolName:                "toolName",
				ToolVersion:             "1.2.3",
				ActionID:                "actionID",
				InvocationID:            "invocID*-12345678",
				CorrelatedInvocationsID: "12345678",
			},
			want: &Metadata{
				ToolName:                "toolName",
				ToolVersion:             "1.2.3",
				ActionID:                "actionID",
				InvocationID:            "invocID*",
				CorrelatedInvocationsID: "12345678",
			},
		},
		{
			name:  "ActionID and InvocationID both equally over limit",
			limit: 32,
			input: &Metadata{
				ToolName:                "toolName",
				ActionID:                "actionID-12345678",
				InvocationID:            "invocID*-12345678",
				CorrelatedInvocationsID: "12345678",
			},
			want: &Metadata{
				ToolName:                "toolName",
				ActionID:                "actionID",
				InvocationID:            "invocID*",
				CorrelatedInvocationsID: "12345678",
			},
		},
		{
			name:  "ActionID and InvocationID both over limit but actionID is bigger",
			limit: 32,
			input: &Metadata{
				ToolName:                "toolName",
				ActionID:                "actionID-123456789012345678",
				InvocationID:            "invocID*-12345678",
				CorrelatedInvocationsID: "12345678",
			},
			want: &Metadata{
				ToolName:                "toolName",
				ActionID:                "actionID",
				InvocationID:            "invocID*",
				CorrelatedInvocationsID: "12345678",
			},
		},
		{
			name:  "CorrelatedInvocationsID and InvocationID both over limit but CorrelatedInvocationsID is bigger",
			limit: 32,
			input: &Metadata{
				ToolName:                "toolName",
				ActionID:                "actionID",
				InvocationID:            "invocID*-12345678",
				CorrelatedInvocationsID: "1234567890987654321",
			},
			want: &Metadata{
				ToolName:                "toolName",
				ActionID:                "actionID",
				InvocationID:            "invocID*",
				CorrelatedInvocationsID: "12345678",
			},
		},
		{
			name:  "ActionID and InvocationID both over limit but invocationID is bigger",
			limit: 32,
			input: &Metadata{
				ToolName:                "toolName",
				ActionID:                "actionID-12345678",
				InvocationID:            "invocID*-123456789012345678",
				CorrelatedInvocationsID: "12345678",
			},
			want: &Metadata{
				ToolName:                "toolName",
				ActionID:                "actionID",
				InvocationID:            "invocID*",
				CorrelatedInvocationsID: "12345678",
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
