package tool

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/command"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/fakes"
)

// TestTool_DownloadActionResult_RejectsNonLocalWorkingDir verifies that a
// server-supplied WorkingDirectory containing `..` traversal causes
// DownloadActionResult to fail before downloading any outputs, instead of
// allowing the download to be redirected outside of pathPrefix.
func TestTool_DownloadActionResult_RejectsNonLocalWorkingDir(t *testing.T) {
	cases := []struct {
		name string
		wd   string
	}{
		{"parent-traversal", "../escape"},
		{"deep-parent-traversal", "../../escape"},
		{"absolute-path", "/etc"},
		{"trailing-traversal", "ok/../../escape"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e, cleanup := fakes.NewTestEnv(t)
			defer cleanup()
			cmd := &command.Command{
				Args:        []string{"tool"},
				ExecRoot:    e.ExecRoot,
				WorkingDir:  tc.wd,
				InputSpec:   &command.InputSpec{},
				OutputFiles: []string{"a/b/out"},
			}
			opt := command.DefaultExecutionOptions()
			_, acDg, _, _ := e.Set(cmd, opt,
				&command.Result{Status: command.CacheHitResultStatus},
				&fakes.OutputFile{Path: "a/b/out", Contents: "output"},
				fakes.StdOut("stdout"), fakes.StdErr("stderr"))

			toolClient := &Client{GrpcClient: e.Client.GrpcClient}
			tmpDir := t.TempDir()

			// Pre-create a sibling directory that would be the target of a
			// successful traversal; we'll verify nothing was written to it.
			sibling := filepath.Join(filepath.Dir(tmpDir), "escape")
			if err := os.MkdirAll(sibling, 0o755); err != nil {
				t.Fatalf("setup mkdir: %v", err)
			}
			defer os.RemoveAll(sibling)

			err := toolClient.DownloadActionResult(context.Background(), acDg.String(), tmpDir)
			if err == nil {
				t.Fatalf("DownloadActionResult should have rejected non-local WorkingDirectory %q, got nil error", tc.wd)
			}
			if !strings.Contains(err.Error(), "WorkingDirectory") {
				t.Errorf("expected error to mention WorkingDirectory, got: %v", err)
			}

			// Sanity: nothing was written to the sibling directory.
			entries, err := os.ReadDir(sibling)
			if err != nil {
				t.Fatalf("read sibling: %v", err)
			}
			if len(entries) != 0 {
				names := []string{}
				for _, e := range entries {
					names = append(names, e.Name())
				}
				t.Errorf("traversal still wrote into sibling directory %q: %v", sibling, names)
			}
		})
	}
}

// TestTool_DownloadActionResult_AcceptsLocalWorkingDir is a regression test
// confirming that a benign relative WorkingDirectory (the legitimate use case)
// still works after the validation is added.
func TestTool_DownloadActionResult_AcceptsLocalWorkingDir(t *testing.T) {
	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()
	cmd := &command.Command{
		Args:        []string{"tool"},
		ExecRoot:    e.ExecRoot,
		WorkingDir:  "subdir",
		InputSpec:   &command.InputSpec{},
		OutputFiles: []string{"a/b/out"},
	}
	opt := command.DefaultExecutionOptions()
	_, acDg, _, _ := e.Set(cmd, opt,
		&command.Result{Status: command.CacheHitResultStatus},
		&fakes.OutputFile{Path: "a/b/out", Contents: "output"},
		fakes.StdOut("stdout"), fakes.StdErr("stderr"))

	toolClient := &Client{GrpcClient: e.Client.GrpcClient}
	tmpDir := t.TempDir()
	if err := toolClient.DownloadActionResult(context.Background(), acDg.String(), tmpDir); err != nil {
		t.Fatalf("DownloadActionResult with local WorkingDirectory failed: %v", err)
	}
	// File should be written under tmpDir/subdir/a/b/out.
	want := filepath.Join(tmpDir, "subdir", "a/b/out")
	got, err := os.ReadFile(want)
	if err != nil {
		t.Fatalf("expected output at %v: %v", want, err)
	}
	if string(got) != "output" {
		t.Errorf("output content = %q, want %q", string(got), "output")
	}
}
