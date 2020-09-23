package tool

import (
	"context"
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/command"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/fakes"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/outerr"
	"github.com/google/go-cmp/cmp"
)

func TestTool_DownloadActionResult(t *testing.T) {
	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()
	cmd := &command.Command{
		Args:        []string{"tool"},
		ExecRoot:    e.ExecRoot,
		InputSpec:   &command.InputSpec{},
		OutputFiles: []string{"a/b/out"},
	}
	opt := command.DefaultExecutionOptions()
	output := "output"
	_, acDg := e.Set(cmd, opt, &command.Result{Status: command.CacheHitResultStatus}, &fakes.OutputFile{Path: "a/b/out", Contents: output},
		fakes.StdOut("stdout"), fakes.StdErr("stderr"))

	toolClient := &Client{GrpcClient: e.Client.GrpcClient}
	tmpDir := t.TempDir()
	if err := toolClient.DownloadActionResult(context.Background(), acDg.String(), tmpDir); err != nil {
		t.Fatalf("DownloadActionResult(%v,%v) failed: %v", acDg.String(), tmpDir, err)
	}
	verifyData := map[string]string{
		filepath.Join(tmpDir, "a/b/out"): "output",
		filepath.Join(tmpDir, "stdout"):  "stdout",
		filepath.Join(tmpDir, "stderr"):  "stderr",
	}
	for fp, want := range verifyData {
		c, err := ioutil.ReadFile(fp)
		if err != nil {
			t.Fatalf("Unable to read downloaded output file %v: %v", fp, err)
		}
		got := string(c)
		if got != want {
			t.Fatalf("Incorrect content in downloaded file %v, want %v, got %v", fp, want, got)
		}
	}
}

func TestTool_ShowAction(t *testing.T) {
	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()
	cmd := &command.Command{
		Args:     []string{"tool"},
		ExecRoot: e.ExecRoot,
		InputSpec: &command.InputSpec{
			Inputs: []string{
				"a/b/input.txt",
			},
		},
		OutputFiles: []string{"a/b/out"},
	}

	opt := command.DefaultExecutionOptions()
	_, acDg := e.Set(cmd, opt, &command.Result{Status: command.CacheHitResultStatus}, &fakes.OutputFile{Path: "a/b/out", Contents: "output"},
		fakes.StdOut("stdout"), fakes.StdErr("stderr"), &fakes.InputFile{Path: "a/b/input.txt", Contents: "input"})

	toolClient := &Client{GrpcClient: e.Client.GrpcClient}
	got, err := toolClient.ShowAction(context.Background(), acDg.String())
	if err != nil {
		t.Fatalf("ShowAction(%v) failed: %v", acDg.String(), err)
	}
	want := `Command
========
Command Digest: 76a608e419da9ed3673f59b8b903f21dbf7cc3178281029151a090cac02d9e4d/15
	tool

Inputs
======
[Root directory digest: e23e10be0d14b5b2b1b7af32de78dea554a74df5bb22b31ae6c49583c1a8aa0e/75]
a/b/input.txt: [File digest: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/0]

Output Files:
=============
a/b/out, digest: e0ee8bb50685e05fa0f47ed04203ae953fdfd055f5bd2892ea186504254f8c3a/6

Output Files From Directories:
=================
`
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("ShowAction(%v) returned diff (-want +got): %v\n\ngot: %v\n\nwant: %v\n", acDg.String(), diff, got, want)
	}
}

func TestTool_ReexecuteAction(t *testing.T) {
	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()
	cmd := &command.Command{
		Args:        []string{"foo bar baz"},
		ExecRoot:    e.ExecRoot,
		InputSpec:   &command.InputSpec{Inputs: []string{"i1", "i2"}},
		OutputFiles: []string{"a/b/out"},
	}
	if err := ioutil.WriteFile(filepath.Join(e.ExecRoot, "i1"), []byte("i1"), 0644); err != nil {
		t.Fatalf("failed creating input file: %v", err)
	}
	if err := ioutil.WriteFile(filepath.Join(e.ExecRoot, "i2"), []byte("i2"), 0644); err != nil {
		t.Fatalf("failed creating input file: %v", err)
	}
	out := "output"
	opt := &command.ExecutionOptions{AcceptCached: false, DownloadOutputs: true}
	_, acDg := e.Set(cmd, opt, &command.Result{Status: command.SuccessResultStatus}, &fakes.OutputFile{Path: "a/b/out", Contents: out},
		fakes.StdOut("stdout"), fakes.StdErr("stderr"))

	client := &Client{GrpcClient: e.Client.GrpcClient}
	oe := outerr.NewRecordingOutErr()
	if err := client.ReexecuteAction(context.Background(), acDg.String(), "", oe); err != nil {
		t.Errorf("error ReexecuteAction: %v", err)
	}
	if string(oe.Stderr()) != "stderr" {
		t.Errorf("Incorrect stderr %v, expected \"stderr\"", oe.Stderr())
	}
	if string(oe.Stdout()) != "stdout" {
		t.Errorf("Incorrect stdout %v, expected \"stdout\"", oe.Stdout())
	}
	// Now execute again with changed inputs.
	tmpDir := t.TempDir()
	if err := ioutil.WriteFile(filepath.Join(tmpDir, "i1"), []byte("i11"), 0644); err != nil {
		t.Fatalf("failed creating input file: %v", err)
	}
	if err := ioutil.WriteFile(filepath.Join(tmpDir, "i2"), []byte("i22"), 0644); err != nil {
		t.Fatalf("failed creating input file: %v", err)
	}
	cmd.ExecRoot = tmpDir
	_, acDg2 := e.Set(cmd, opt, &command.Result{Status: command.SuccessResultStatus}, &fakes.OutputFile{Path: "a/b/out", Contents: out},
		fakes.StdOut("stdout2"), fakes.StdErr("stderr2"))
	oe = outerr.NewRecordingOutErr()
	if err := client.ReexecuteAction(context.Background(), acDg2.String(), tmpDir, oe); err != nil {
		t.Errorf("error ReexecuteAction: %v", err)
	}

	fp := filepath.Join(tmpDir, "a/b/out")
	c, err := ioutil.ReadFile(fp)
	if err != nil {
		t.Fatalf("Unable to read downloaded output %v: %v", fp, err)
	}
	if string(c) != out {
		t.Fatalf("Incorrect content in downloaded file %v, want %s, got %s", fp, out, c)
	}
	if string(oe.Stderr()) != "stderr2" {
		t.Errorf("Incorrect stderr %v, expected \"stderr\"", oe.Stderr())
	}
	if string(oe.Stdout()) != "stdout2" {
		t.Errorf("Incorrect stdout %v, expected \"stdout\"", oe.Stdout())
	}
}

func TestTool_DownloadBlob(t *testing.T) {
	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()
	cas := e.Server.CAS
	dg := cas.Put([]byte("hello"))

	toolClient := &Client{GrpcClient: e.Client.GrpcClient}
	got, err := toolClient.DownloadBlob(context.Background(), dg.String(), "")
	if err != nil {
		t.Fatalf("DownloadBlob(%v) failed: %v", dg.String(), err)
	}
	want := "hello"
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("DownloadBlob(%v) returned diff (-want +got): %v\n\ngot: %v\n\nwant: %v\n", dg.String(), diff, got, want)
	}
	// Now download into a specified location.
	tmpFile, err := ioutil.TempFile(t.TempDir(), "")
	if err != nil {
		t.Fatalf("TempFile failed: %v", err)
	}
	if err := tmpFile.Close(); err != nil {
		t.Fatalf("TempFile Close failed: %v", err)
	}
	fp := tmpFile.Name()
	got, err = toolClient.DownloadBlob(context.Background(), dg.String(), fp)
	if err != nil {
		t.Fatalf("DownloadBlob(%v) failed: %v", dg.String(), err)
	}
	if got != "" {
		t.Fatalf("DownloadBlob(%v) returned %v, expected empty: ", dg.String(), got)
	}
	c, err := ioutil.ReadFile(fp)
	if err != nil {
		t.Fatalf("Unable to read downloaded output file %v: %v", fp, err)
	}
	got = string(c)
	if got != want {
		t.Fatalf("Incorrect content in downloaded file %v, want %v, got %v", fp, want, got)
	}
}
