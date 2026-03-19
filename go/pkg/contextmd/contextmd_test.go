package contextmd

import (
	"context"
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
			limit: 24,
			input: &Metadata{
				ToolName:     "toolName",
				ActionID:     "actionID",
				InvocationID: "invocID*",
			},
			want: &Metadata{
				ToolName:     "toolName",
				ActionID:     "actionID",
				InvocationID: "invocID*",
			},
		},
		{
			name:  "new fields dropped before ActionID and InvocationID",
			limit: 24,
			input: &Metadata{
				ToolName:       "toolName",
				ActionID:       "actionID",
				InvocationID:   "invocID*",
				ActionMnemonic: "12345678",
			},
			want: &Metadata{
				ToolName:       "toolName",
				ActionID:       "actionID",
				InvocationID:   "invocID*",
				ActionMnemonic: "",
			},
		},
		{
			name:  "actionID over limit",
			limit: 24,
			input: &Metadata{
				ToolName:     "toolName",
				ActionID:     "actionID-12345678",
				InvocationID: "invocID*",
			},
			want: &Metadata{
				ToolName:     "toolName",
				ActionID:     "actionID",
				InvocationID: "invocID*",
			},
		},
		{
			name:  "invocationID over limit",
			limit: 29,
			input: &Metadata{
				ToolName:     "toolName",
				ToolVersion:  "1.2.3",
				ActionID:     "actionID",
				InvocationID: "invocID*-12345678",
			},
			want: &Metadata{
				ToolName:     "toolName",
				ToolVersion:  "1.2.3",
				ActionID:     "actionID",
				InvocationID: "invocID*",
			},
		},
		{
			name:  "both equally over limit",
			limit: 24,
			input: &Metadata{
				ToolName:     "toolName",
				ActionID:     "actionID-12345678",
				InvocationID: "invocID*-12345678",
			},
			want: &Metadata{
				ToolName:     "toolName",
				ActionID:     "actionID",
				InvocationID: "invocID*",
			},
		},
		{
			name:  "both over limit but actionID is bigger",
			limit: 24,
			input: &Metadata{
				ToolName:     "toolName",
				ActionID:     "actionID-123456789012345678",
				InvocationID: "invocID*-12345678",
			},
			want: &Metadata{
				ToolName:     "toolName",
				ActionID:     "actionID",
				InvocationID: "invocID*",
			},
		},
		{
			name:  "both over limit but invocationID is bigger",
			limit: 24,
			input: &Metadata{
				ToolName:     "toolName",
				ActionID:     "actionID-12345678",
				InvocationID: "invocID*-123456789012345678",
			},
			want: &Metadata{
				ToolName:     "toolName",
				ActionID:     "actionID",
				InvocationID: "invocID*",
			},
		},
		{
			name:  "new fields dropped before trimming ActionID and InvocationID",
			limit: 40,
			input: &Metadata{
				ToolName:        "toolName",
				ActionID:        "actionID",
				InvocationID:    "invocID*",
				ActionMnemonic:  "CppCompile",
				TargetID:        "//pkg:target",
				ConfigurationID: "config-abc",
			},
			want: &Metadata{
				ToolName:        "toolName",
				ActionID:        "actionID",
				InvocationID:    "invocID*",
				ActionMnemonic:  "CppCompile",
				TargetID:        "//pkg:",
				ConfigurationID: "",
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

func TestWithExtractMetadataRoundTrip(t *testing.T) {
	input := &Metadata{
		ToolName:                "myTool",
		ToolVersion:             "1.0",
		ActionID:                "action-1",
		InvocationID:            "invoc-1",
		CorrelatedInvocationsID: "corr-1",
		ActionMnemonic:          "CppCompile",
		TargetID:                "//pkg:target",
		ConfigurationID:         "config-abc",
	}

	ctx, err := WithMetadata(context.Background(), input)
	if err != nil {
		t.Fatalf("WithMetadata: %v", err)
	}

	got, err := ExtractMetadata(ctx)
	if err != nil {
		t.Fatalf("ExtractMetadata: %v", err)
	}

	if got, want := got.ToolName, input.ToolName; got != want {
		t.Errorf("ToolName: got %q, want %q", got, want)
	}
	if got, want := got.ToolVersion, input.ToolVersion; got != want {
		t.Errorf("ToolVersion: got %q, want %q", got, want)
	}
	if got, want := got.ActionID, input.ActionID; got != want {
		t.Errorf("ActionID: got %q, want %q", got, want)
	}
	if got, want := got.InvocationID, input.InvocationID; got != want {
		t.Errorf("InvocationID: got %q, want %q", got, want)
	}
	if got, want := got.CorrelatedInvocationsID, input.CorrelatedInvocationsID; got != want {
		t.Errorf("CorrelatedInvocationsID: got %q, want %q", got, want)
	}
	if got, want := got.ActionMnemonic, input.ActionMnemonic; got != want {
		t.Errorf("ActionMnemonic: got %q, want %q", got, want)
	}
	if got, want := got.TargetID, input.TargetID; got != want {
		t.Errorf("TargetID: got %q, want %q", got, want)
	}
	if got, want := got.ConfigurationID, input.ConfigurationID; got != want {
		t.Errorf("ConfigurationID: got %q, want %q", got, want)
	}
}

func TestMergeMetadata(t *testing.T) {
	m1 := &Metadata{
		ToolName:                "myTool",
		ToolVersion:             "1.0",
		ActionID:                "action-1",
		InvocationID:            "invoc-1",
		CorrelatedInvocationsID: "corr-1",
		ActionMnemonic:          "CppCompile",
		TargetID:                "//pkg:target",
		ConfigurationID:         "config-abc",
	}
	m2 := &Metadata{
		ToolName:                "otherTool",
		ToolVersion:             "2.0",
		ActionID:                "action-2",
		InvocationID:            "invoc-2",
		CorrelatedInvocationsID: "corr-2",
		ActionMnemonic:          "GoLink",
		TargetID:                "//other:target",
		ConfigurationID:         "config-xyz",
	}

	got := MergeMetadata(m1, m2)

	// ActionID and InvocationID are merged as sorted, comma-separated sets.
	if got, want := got.ActionID, "action-1,action-2"; got != want {
		t.Errorf("ActionID: got %q, want %q", got, want)
	}
	if got, want := got.InvocationID, "invoc-1,invoc-2"; got != want {
		t.Errorf("InvocationID: got %q, want %q", got, want)
	}
	// All other fields are taken from the first argument.
	if got, want := got.ToolName, "myTool"; got != want {
		t.Errorf("ToolName: got %q, want %q", got, want)
	}
	if got, want := got.ToolVersion, "1.0"; got != want {
		t.Errorf("ToolVersion: got %q, want %q", got, want)
	}
	if got, want := got.CorrelatedInvocationsID, "corr-1"; got != want {
		t.Errorf("CorrelatedInvocationsID: got %q, want %q", got, want)
	}
	if got, want := got.ActionMnemonic, "CppCompile"; got != want {
		t.Errorf("ActionMnemonic: got %q, want %q", got, want)
	}
	if got, want := got.TargetID, "//pkg:target"; got != want {
		t.Errorf("TargetID: got %q, want %q", got, want)
	}
	if got, want := got.ConfigurationID, "config-abc"; got != want {
		t.Errorf("ConfigurationID: got %q, want %q", got, want)
	}
}
