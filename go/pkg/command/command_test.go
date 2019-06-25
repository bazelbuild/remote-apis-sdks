package command

import "testing"

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
					Inputs:	[]string{"a", "b", "c"},
				},
			},
			B: &Command{
				InputSpec: &InputSpec{
					Inputs:	[]string{"c", "b", "a"},
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
					EnvironmentVariables:	map[string]string{"a": "1", "b": "2", "c": "3"},
				},
			},
			B: &Command{
				InputSpec: &InputSpec{
					EnvironmentVariables:	map[string]string{"c": "3", "b": "2", "a": "1"},
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
		aId := tc.A.stableId()
		bId := tc.B.stableId()
		if aId != bId {
			t.Errorf("%s: stableId of %v = %s different from %v = %s", tc.label, tc.A, aId, tc.B, bId)
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
			A: &Command{ Args: []string{"a", "b"}},
			B: &Command{ Args: []string{"b", "a"}},
		},
		{
			label: "exec root",
			A: &Command{ ExecRoot: "a"},
			B: &Command{ ExecRoot: "b"},
		},
		{
			label: "working dir",
			A: &Command{ WorkingDir: "a"},
			B: &Command{ WorkingDir: "b"},
		},
		{
			label: "output files",
			A: &Command{ OutputFiles: []string{"a", "b", "c"}},
			B: &Command{ OutputFiles: []string{"c", "b", "c"}},
		},
		{
			label: "output dirs",
			A: &Command{ OutputDirs: []string{"a", "b", "c"}},
			B: &Command{ OutputDirs: []string{"c", "b", "c"}},
		},
		{
			label: "platform",
			A: &Command{ Platform: map[string]string{"a": "1", "b": "2", "c": "3"}},
			B: &Command{ Platform: map[string]string{"c": "3", "b": "2", "a": "10"}},
		},
		{
			label: "inputs",
			A: &Command{
				InputSpec: &InputSpec{
					Inputs:	[]string{"a", "b", "c"},
				},
			},
			B: &Command{
				InputSpec: &InputSpec{
					Inputs:	[]string{"c", "b", "a1"},
				},
			},
		},
		{
			label: "environment",
			A: &Command{
				InputSpec: &InputSpec{
					EnvironmentVariables:	map[string]string{"a": "1", "b": "2", "c": "3"},
				},
			},
			B: &Command{
				InputSpec: &InputSpec{
					EnvironmentVariables:	map[string]string{"c": "3", "b": "2", "a": "10"},
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
		aId := tc.A.stableId()
		bId := tc.B.stableId()
		if aId == bId {
			t.Errorf("%s: stableId of %v = %s is same as %v", tc.label, tc.A, aId, tc.B)
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

	if c.Identifiers.CommandId == "" {
		t.Errorf("did not fill command id for empty command")
	}
	if c.Identifiers.ToolName == "" {
		t.Errorf("did not fill tool name for empty command, expected \"remote-client\"")
	}
	if c.Identifiers.InvocationId == "" {
		t.Errorf("did not generate invocation id for empty command")
	}
	if c.InputSpec == nil {
		t.Errorf("did not generate input spec for empty command")
	}
}

func TestFillDefaultFieldValues_PreserveExisting(t *testing.T) {
	t.Parallel()
	ids := &Identifiers {
		CommandId: "bla",
		ToolName: "foo",
		InvocationId: "bar",
	}
	inputSpec := &InputSpec{}
	c := &Command{InputSpec: inputSpec, Identifiers: ids}
	c.FillDefaultFieldValues()
	if c.Identifiers != ids {
		t.Fatal("command.Identifiers address not preserved")
	}

	if c.Identifiers.CommandId != "bla" {
		t.Errorf("did not preserve CommandId: got %s, expected bla", c.Identifiers.CommandId)
	}
	if c.Identifiers.ToolName != "foo" {
		t.Errorf("did not preserve CommandId: got %s, expected foo", c.Identifiers.ToolName)
	}
	if c.Identifiers.InvocationId != "bar" {
		t.Errorf("did not preserve CommandId: got %s, expected bar", c.Identifiers.InvocationId)
	}
	if c.InputSpec != inputSpec {
		t.Fatal("command.InputSpec address not preserved")
	}
}

func TestValidate_Errors(t *testing.T) {
	t.Parallel()
	testcases := []struct {
		label string
		Command  *Command
	}{
		{
			label: "missing args",
			Command: &Command{
				Identifiers: &Identifiers{},
				ExecRoot: "a",
				InputSpec: &InputSpec{},
			},
		},
		{
			label: "missing input spec",
			Command: &Command{
				Identifiers: &Identifiers{},
				Args: []string{"a"},
				ExecRoot: "a",
			},
		},
		{
			label: "missing exec root",
			Command: &Command{
				Identifiers: &Identifiers{},
				Args: []string{"a"},
				InputSpec: &InputSpec{},
			},
		},
		{
			label: "missing identifiers",
			Command: &Command{
				Args: []string{"a"},
				InputSpec: &InputSpec{},
				ExecRoot: "a",
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
		Args: []string{"a"},
		ExecRoot: "a",
		InputSpec: &InputSpec{},
	}
	if err := c.Validate(); err != nil {
		t.Errorf("expected Validate of %v = nil, got %v", c, err)
	}
}
