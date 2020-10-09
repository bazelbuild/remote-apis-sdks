package command

import (
	"errors"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

func TestStableId_SameCommands(t *testing.T) {
	t.Parallel()
	testcases := []struct {
		label string
		A, B  *Command
	}{
		{
			label: "platform",
			A: &Command{
				Platform: map[string]string{"a": "1", "b": "2", "c": "3"},
			},
			B: &Command{
				Platform: map[string]string{"c": "3", "b": "2", "a": "1"},
			},
		},
		{
			label: "inputs",
			A: &Command{
				InputSpec: &InputSpec{
					Inputs: []string{"a", "b", "c"},
				},
			},
			B: &Command{
				InputSpec: &InputSpec{
					Inputs: []string{"c", "b", "a"},
				},
			},
		},
		{
			label: "output files",
			A: &Command{
				OutputFiles: []string{"a", "b", "c"},
			},
			B: &Command{
				OutputFiles: []string{"c", "b", "a"},
			},
		},
		{
			label: "output directories",
			A: &Command{
				OutputDirs: []string{"a", "b", "c"},
			},
			B: &Command{
				OutputDirs: []string{"c", "b", "a"},
			},
		},
		{
			label: "environment",
			A: &Command{
				InputSpec: &InputSpec{
					EnvironmentVariables: map[string]string{"a": "1", "b": "2", "c": "3"},
				},
			},
			B: &Command{
				InputSpec: &InputSpec{
					EnvironmentVariables: map[string]string{"c": "3", "b": "2", "a": "1"},
				},
			},
		},
		{
			label: "exclusions",
			A: &Command{
				InputSpec: &InputSpec{
					InputExclusions: []*InputExclusion{
						{Regex: "a", Type: FileInputType},
						{Regex: "b", Type: DirectoryInputType},
						{Regex: "c", Type: UnspecifiedInputType},
					},
				},
			},
			B: &Command{
				InputSpec: &InputSpec{
					InputExclusions: []*InputExclusion{
						{Regex: "b", Type: DirectoryInputType},
						{Regex: "c", Type: UnspecifiedInputType},
						{Regex: "a", Type: FileInputType},
					},
				},
			},
		},
	}
	for _, tc := range testcases {
		aID := tc.A.stableID()
		bID := tc.B.stableID()
		if aID != bID {
			t.Errorf("%s: stableID of %v = %s different from %v = %s", tc.label, tc.A, aID, tc.B, bID)
		}
	}
}

func TestStableId_DifferentCommands(t *testing.T) {
	t.Parallel()
	testcases := []struct {
		label string
		A, B  *Command
	}{
		{
			label: "args",
			A:     &Command{Args: []string{"a", "b"}},
			B:     &Command{Args: []string{"b", "a"}},
		},
		{
			label: "exec root",
			A:     &Command{ExecRoot: "a"},
			B:     &Command{ExecRoot: "b"},
		},
		{
			label: "working dir",
			A:     &Command{WorkingDir: "a"},
			B:     &Command{WorkingDir: "b"},
		},
		{
			label: "output files",
			A:     &Command{OutputFiles: []string{"a", "b", "c"}},
			B:     &Command{OutputFiles: []string{"c", "b", "c"}},
		},
		{
			label: "output dirs",
			A:     &Command{OutputDirs: []string{"a", "b", "c"}},
			B:     &Command{OutputDirs: []string{"c", "b", "c"}},
		},
		{
			label: "platform",
			A:     &Command{Platform: map[string]string{"a": "1", "b": "2", "c": "3"}},
			B:     &Command{Platform: map[string]string{"c": "3", "b": "2", "a": "10"}},
		},
		{
			label: "inputs",
			A: &Command{
				InputSpec: &InputSpec{
					Inputs: []string{"a", "b", "c"},
				},
			},
			B: &Command{
				InputSpec: &InputSpec{
					Inputs: []string{"c", "b", "a1"},
				},
			},
		},
		{
			label: "environment",
			A: &Command{
				InputSpec: &InputSpec{
					EnvironmentVariables: map[string]string{"a": "1", "b": "2", "c": "3"},
				},
			},
			B: &Command{
				InputSpec: &InputSpec{
					EnvironmentVariables: map[string]string{"c": "3", "b": "2", "a": "10"},
				},
			},
		},
		{
			label: "exclusions",
			A: &Command{
				InputSpec: &InputSpec{
					InputExclusions: []*InputExclusion{
						{Regex: "a", Type: FileInputType},
						{Regex: "b", Type: DirectoryInputType},
						{Regex: "c", Type: UnspecifiedInputType},
					},
				},
			},
			B: &Command{
				InputSpec: &InputSpec{
					InputExclusions: []*InputExclusion{
						{Regex: "b", Type: UnspecifiedInputType},
						{Regex: "c", Type: UnspecifiedInputType},
						{Regex: "a", Type: FileInputType},
					},
				},
			},
		},
	}
	for _, tc := range testcases {
		aID := tc.A.stableID()
		bID := tc.B.stableID()
		if aID == bID {
			t.Errorf("%s: stableID of %v = %s is same as %v", tc.label, tc.A, aID, tc.B)
		}
	}
}

func TestFillDefaultFieldValues_Empty(t *testing.T) {
	t.Parallel()
	c := &Command{}
	c.FillDefaultFieldValues()
	if c.Identifiers == nil {
		t.Fatal("{}.Identifiers = nil, expected filled")
	}

	if c.Identifiers.CommandID == "" {
		t.Errorf("did not fill command id for empty command")
	}
	if c.Identifiers.ToolName == "" {
		t.Errorf("did not fill tool name for empty command, expected \"remote-client\"")
	}
	if c.Identifiers.InvocationID == "" {
		t.Errorf("did not generate invocation id for empty command")
	}
	if c.InputSpec == nil {
		t.Errorf("did not generate input spec for empty command")
	}
}

func TestFillDefaultFieldValues_PreserveExisting(t *testing.T) {
	t.Parallel()
	ids := &Identifiers{
		CommandID:    "bla",
		ToolName:     "foo",
		InvocationID: "bar",
	}
	inputSpec := &InputSpec{}
	c := &Command{InputSpec: inputSpec, Identifiers: ids}
	c.FillDefaultFieldValues()
	if c.Identifiers != ids {
		t.Fatal("command.Identifiers address not preserved")
	}

	if c.Identifiers.CommandID != "bla" {
		t.Errorf("did not preserve CommandID: got %s, expected bla", c.Identifiers.CommandID)
	}
	if c.Identifiers.ToolName != "foo" {
		t.Errorf("did not preserve CommandID: got %s, expected foo", c.Identifiers.ToolName)
	}
	if c.Identifiers.InvocationID != "bar" {
		t.Errorf("did not preserve CommandID: got %s, expected bar", c.Identifiers.InvocationID)
	}
	if c.InputSpec != inputSpec {
		t.Fatal("command.InputSpec address not preserved")
	}
}

func TestValidate_Errors(t *testing.T) {
	t.Parallel()
	testcases := []struct {
		label   string
		Command *Command
	}{
		{
			label: "missing args",
			Command: &Command{
				Identifiers: &Identifiers{},
				ExecRoot:    "a",
				InputSpec:   &InputSpec{},
			},
		},
		{
			label: "missing input spec",
			Command: &Command{
				Identifiers: &Identifiers{},
				Args:        []string{"a"},
				ExecRoot:    "a",
			},
		},
		{
			label: "missing exec root",
			Command: &Command{
				Identifiers: &Identifiers{},
				Args:        []string{"a"},
				InputSpec:   &InputSpec{},
			},
		},
		{
			label: "missing identifiers",
			Command: &Command{
				Args:      []string{"a"},
				InputSpec: &InputSpec{},
				ExecRoot:  "a",
			},
		},
	}
	for _, tc := range testcases {
		if err := tc.Command.Validate(); err == nil {
			t.Errorf("%s: expected Validate of %v to error, got nil", tc.label, tc.Command)
		}
	}
}

func TestValidate_NilSuccess(t *testing.T) {
	t.Parallel()
	var c *Command
	if err := c.Validate(); err != nil {
		t.Errorf("expected Validate of nil = nil, got %v", err)
	}
}

func TestValidate_Success(t *testing.T) {
	t.Parallel()
	c := &Command{
		Identifiers: &Identifiers{},
		Args:        []string{"a"},
		ExecRoot:    "a",
		InputSpec:   &InputSpec{},
	}
	if err := c.Validate(); err != nil {
		t.Errorf("expected Validate of %v = nil, got %v", c, err)
	}
}

func TestToREProto(t *testing.T) {
	tests := []struct {
		name    string
		cmd     *Command
		wantCmd *repb.Command
	}{
		{
			name:    "pass args",
			cmd:     &Command{Args: []string{"foo", "bar"}},
			wantCmd: &repb.Command{Arguments: []string{"foo", "bar"}},
		},
		{
			name:    "pass working directory",
			cmd:     &Command{WorkingDir: "a/b"},
			wantCmd: &repb.Command{WorkingDirectory: "a/b"},
		},
		{
			name:    "sort output files",
			cmd:     &Command{OutputFiles: []string{"foo", "bar", "abc"}},
			wantCmd: &repb.Command{OutputFiles: []string{"abc", "bar", "foo"}},
		},
		{
			name:    "sort output directories",
			cmd:     &Command{OutputDirs: []string{"foo", "bar", "abc"}},
			wantCmd: &repb.Command{OutputDirectories: []string{"abc", "bar", "foo"}},
		},
		{
			name: "sort environment variables",
			cmd: &Command{
				InputSpec: &InputSpec{
					EnvironmentVariables: map[string]string{"b": "3", "a": "2", "c": "1"},
				},
			},
			wantCmd: &repb.Command{
				EnvironmentVariables: []*repb.Command_EnvironmentVariable{
					&repb.Command_EnvironmentVariable{Name: "a", Value: "2"},
					&repb.Command_EnvironmentVariable{Name: "b", Value: "3"},
					&repb.Command_EnvironmentVariable{Name: "c", Value: "1"},
				},
			},
		},
		{
			name: "sort platform",
			cmd:  &Command{Platform: map[string]string{"b": "3", "a": "2", "c": "1"}},
			wantCmd: &repb.Command{
				Platform: &repb.Platform{
					Properties: []*repb.Platform_Property{
						&repb.Platform_Property{Name: "a", Value: "2"},
						&repb.Platform_Property{Name: "b", Value: "3"},
						&repb.Platform_Property{Name: "c", Value: "1"},
					},
				},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.cmd.FillDefaultFieldValues()
			gotCmd := tc.cmd.ToREProto()
			if diff := cmp.Diff(tc.wantCmd, gotCmd, cmpopts.EquateEmpty(), cmp.Comparer(proto.Equal)); diff != "" {
				t.Errorf("%s: buildCommand gave result diff (-want +got):\n%s", tc.name, diff)
			}
		})
	}
}

func TestToFromProto(t *testing.T) {
	cmd := &Command{
		Identifiers: &Identifiers{
			CommandID:    "a",
			InvocationID: "b",
			ToolName:     "c",
		},
		Args:     []string{"a", "b", "c"},
		ExecRoot: "/exec/root",
		InputSpec: &InputSpec{
			Inputs: []string{"foo.h", "bar.h"},
			VirtualInputs: []*VirtualInput{
				&VirtualInput{
					Path:         "empty_file",
					IsExecutable: true,
				},
				&VirtualInput{
					Path:             "foo/empty_dir",
					IsEmptyDirectory: true,
				},
				&VirtualInput{
					Path:     "foo/bar",
					Contents: []byte("bar-contents"),
				},
			},
			InputExclusions: []*InputExclusion{
				&InputExclusion{
					Regex: "*.bla",
					Type:  DirectoryInputType,
				},
				&InputExclusion{
					Regex: "*.blo",
					Type:  FileInputType,
				},
			},
			EnvironmentVariables: map[string]string{
				"k":  "v",
				"k1": "v1",
			},
		},
		OutputFiles: []string{"a/b/out"},
	}
	gotCmd := FromProto(ToProto(cmd))
	if diff := cmp.Diff(cmd, gotCmd, cmpopts.EquateEmpty()); diff != "" {
		t.Errorf("FromProto(ToProto()) returned diff in result: (-want +got)\n%s", diff)
	}
}

func TestResultToFromProto(t *testing.T) {
	res := &Result{
		Status:   CacheHitResultStatus,
		ExitCode: 42,
		Err:      errors.New("message"),
	}
	gotRes := ResultFromProto(ResultToProto(res))
	if diff := cmp.Diff(res, gotRes, cmpopts.IgnoreFields(Result{}, "Err")); diff != "" {
		t.Errorf("ResultFromProto(ResultToProto()) returned diff in result: (-want +got)\n%s", diff)
	}
	if res.Err.Error() != gotRes.Err.Error() {
		t.Errorf("ResultFromProto(ResultToProto()) returned diff in error: want %v, got %v", res.Err, gotRes.Err)
	}
}

func TestTimeIntervalToFromProto(t *testing.T) {
	ti := &TimeInterval{
		From: time.Now(),
		To:   time.Now(),
	}
	gotTi := TimeIntervalFromProto(TimeIntervalToProto(ti))
	if diff := cmp.Diff(ti, gotTi); diff != "" {
		t.Errorf("TimeIntervalFromProto(TimeIntervalToProto()) returned diff in result: (-want +got)\n%s", diff)
	}
	ti = &TimeInterval{From: time.Now()}
	gotTi = TimeIntervalFromProto(TimeIntervalToProto(ti))
	if diff := cmp.Diff(ti, gotTi); diff != "" {
		t.Errorf("TimeIntervalFromProto(TimeIntervalToProto()) returned diff in result: (-want +got)\n%s", diff)
	}
	ti = &TimeInterval{To: time.Now()}
	gotTi = TimeIntervalFromProto(TimeIntervalToProto(ti))
	if diff := cmp.Diff(ti, gotTi); diff != "" {
		t.Errorf("TimeIntervalFromProto(TimeIntervalToProto()) returned diff in result: (-want +got)\n%s", diff)
	}
	ti = &TimeInterval{}
	gotTi = TimeIntervalFromProto(TimeIntervalToProto(ti))
	if diff := cmp.Diff(ti, gotTi); diff != "" {
		t.Errorf("TimeIntervalFromProto(TimeIntervalToProto()) returned diff in result: (-want +got)\n%s", diff)
	}
	gotTi = TimeIntervalFromProto(TimeIntervalToProto(nil))
	if gotTi != nil {
		t.Errorf("TimeIntervalFromProto(TimeIntervalToProto()) returned %v, wanted nil", gotTi)
	}
}
