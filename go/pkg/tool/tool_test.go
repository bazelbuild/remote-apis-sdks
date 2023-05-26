package tool

import (
	"context"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/command"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
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
	_, acDg, _, _ := e.Set(cmd, opt, &command.Result{Status: command.CacheHitResultStatus}, &fakes.OutputFile{Path: "a/b/out", Contents: output},
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
		c, err := os.ReadFile(fp)
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
	_, acDg, _, _ := e.Set(cmd, opt, &command.Result{Status: command.CacheHitResultStatus}, &fakes.OutputFile{Path: "a/b/out", Contents: "output"},
		fakes.StdOut("stdout"), fakes.StdErr("stderr"), &fakes.InputFile{Path: "a/b/input.txt", Contents: "input"})

	toolClient := &Client{GrpcClient: e.Client.GrpcClient}
	got, err := toolClient.ShowAction(context.Background(), acDg.String())
	if err != nil {
		t.Fatalf("ShowAction(%v) failed: %v", acDg.String(), err)
	}
	want := `Command
=======
Command Digest: 76a608e419da9ed3673f59b8b903f21dbf7cc3178281029151a090cac02d9e4d/15
	tool

Platform
========

Inputs
======
[Root directory digest: e23e10be0d14b5b2b1b7af32de78dea554a74df5bb22b31ae6c49583c1a8aa0e/75]
a/b/input.txt: [File digest: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855/0]

------------------------------------------------------------------------
Action Result

Exit code: 0
stdout digest: 63d42d26156fcc761e57da4128e9881d5bdf3bf933f0f6e9c93d6e26b9b90ae7/6
stderr digest: 7e6b710b765404cccbad9eedcff7615fc37b269d6db12cd81a58be541d93083c/6

Output Files
============
a/b/out, digest: e0ee8bb50685e05fa0f47ed04203ae953fdfd055f5bd2892ea186504254f8c3a/6

Output Files From Directories
=============================
`
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("ShowAction(%v) returned diff (-want +got): %v\n\ngot: %v\n\nwant: %v\n", acDg.String(), diff, got, want)
	}
}

func TestTool_CheckDeterminism(t *testing.T) {
	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()
	cmd := &command.Command{
		Args:        []string{"foo bar baz"},
		ExecRoot:    e.ExecRoot,
		InputSpec:   &command.InputSpec{Inputs: []string{"i1", "i2"}},
		OutputFiles: []string{"a/b/out"},
	}
	if err := os.WriteFile(filepath.Join(e.ExecRoot, "i1"), []byte("i1"), 0644); err != nil {
		t.Fatalf("failed creating input file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(e.ExecRoot, "i2"), []byte("i2"), 0644); err != nil {
		t.Fatalf("failed creating input file: %v", err)
	}
	out := "output"
	opt := &command.ExecutionOptions{AcceptCached: false, DownloadOutputs: true, DownloadOutErr: true}
	_, acDg, _, _ := e.Set(cmd, opt, &command.Result{Status: command.SuccessResultStatus}, &fakes.OutputFile{Path: "a/b/out", Contents: out})

	client := &Client{GrpcClient: e.Client.GrpcClient}
	if err := client.CheckDeterminism(context.Background(), acDg.String(), "", 2); err != nil {
		t.Errorf("CheckDeterminism returned an error: %v", err)
	}
	// Now execute again with changed inputs.
	testOnlyStartDeterminismExec = func() {
		out = "output2"
		e.Set(cmd, opt, &command.Result{Status: command.SuccessResultStatus}, &fakes.OutputFile{Path: "a/b/out", Contents: out})
	}
	defer func() { testOnlyStartDeterminismExec = func() {} }()
	if err := client.CheckDeterminism(context.Background(), acDg.String(), "", 2); err == nil {
		t.Errorf("CheckDeterminism returned nil, want error")
	}
}

func TestTool_ExecuteAction(t *testing.T) {
	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()
	cmd := &command.Command{
		Args:        []string{"foo bar baz"},
		ExecRoot:    e.ExecRoot,
		InputSpec:   &command.InputSpec{Inputs: []string{"i1", "i2"}},
		OutputFiles: []string{"a/b/out"},
	}
	if err := os.WriteFile(filepath.Join(e.ExecRoot, "i1"), []byte("i1"), 0644); err != nil {
		t.Fatalf("failed creating input file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(e.ExecRoot, "i2"), []byte("i2"), 0644); err != nil {
		t.Fatalf("failed creating input file: %v", err)
	}
	out := "output"
	opt := &command.ExecutionOptions{AcceptCached: false, DownloadOutputs: true, DownloadOutErr: true}
	_, acDg, _, _ := e.Set(cmd, opt, &command.Result{Status: command.SuccessResultStatus}, &fakes.OutputFile{Path: "a/b/out", Contents: out},
		fakes.StdOut("stdout"), fakes.StdErr("stderr"))

	client := &Client{GrpcClient: e.Client.GrpcClient}
	oe := outerr.NewRecordingOutErr()
	if _, err := client.ExecuteAction(context.Background(), acDg.String(), "", "", oe); err != nil {
		t.Errorf("error executeAction: %v", err)
	}
	if string(oe.Stderr()) != "stderr" {
		t.Errorf("Incorrect stderr %v, expected \"stderr\"", oe.Stderr())
	}
	if string(oe.Stdout()) != "stdout" {
		t.Errorf("Incorrect stdout %v, expected \"stdout\"", oe.Stdout())
	}
	// Now execute again with changed inputs.
	tmpDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(tmpDir, "i1"), []byte("i11"), 0644); err != nil {
		t.Fatalf("failed creating input file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "i2"), []byte("i22"), 0644); err != nil {
		t.Fatalf("failed creating input file: %v", err)
	}
	cmd.ExecRoot = tmpDir
	_, acDg2, _, _ := e.Set(cmd, opt, &command.Result{Status: command.SuccessResultStatus}, &fakes.OutputFile{Path: "a/b/out", Contents: out},
		fakes.StdOut("stdout2"), fakes.StdErr("stderr2"))
	oe = outerr.NewRecordingOutErr()
	if _, err := client.ExecuteAction(context.Background(), acDg2.String(), "", tmpDir, oe); err != nil {
		t.Errorf("error executeAction: %v", err)
	}

	fp := filepath.Join(tmpDir, "a/b/out")
	c, err := os.ReadFile(fp)
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

func TestTool_ExecuteActionFromRoot(t *testing.T) {
	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()
	cmd := &command.Command{
		Args:        []string{"foo bar baz"},
		ExecRoot:    e.ExecRoot,
		InputSpec:   &command.InputSpec{Inputs: []string{"i1", "i2"}},
		OutputFiles: []string{"a/b/out"},
	}
	// Create files necessary for the fake
	if err := os.WriteFile(filepath.Join(e.ExecRoot, "i1"), []byte("i1"), 0644); err != nil {
		t.Fatalf("failed creating input file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(e.ExecRoot, "i2"), []byte("i2"), 0644); err != nil {
		t.Fatalf("failed creating input file: %v", err)
	}
	out := "output"
	opt := &command.ExecutionOptions{AcceptCached: false, DownloadOutputs: false, DownloadOutErr: true}
	e.Set(cmd, opt, &command.Result{Status: command.SuccessResultStatus}, &fakes.OutputFile{Path: "a/b/out", Contents: out},
		fakes.StdOut("stdout"), fakes.StdErr("stderr"))

	client := &Client{GrpcClient: e.Client.GrpcClient}
	oe := outerr.NewRecordingOutErr()
	// Construct the action root
	os.Mkdir(filepath.Join(e.ExecRoot, "input"), os.ModePerm)
	if err := os.WriteFile(filepath.Join(e.ExecRoot, "input", "i1"), []byte("i1"), 0644); err != nil {
		t.Fatalf("failed creating input file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(e.ExecRoot, "input", "i2"), []byte("i2"), 0644); err != nil {
		t.Fatalf("failed creating input file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(e.ExecRoot, "cmd.textproto"), []byte(`arguments: "foo bar baz"
	output_files: "a/b/out"`), 0644); err != nil {
		t.Fatalf("failed creating command file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(e.ExecRoot, "ac.textproto"), []byte(""), 0644); err != nil {
		t.Fatalf("failed creating command file: %v", err)
	}
	if _, err := client.ExecuteAction(context.Background(), "", e.ExecRoot, "", oe); err != nil {
		t.Errorf("error executeAction: %v", err)
	}
	if string(oe.Stderr()) != "stderr" {
		t.Errorf("Incorrect stderr %v, expected \"stderr\"", string(oe.Stderr()))
	}
	if string(oe.Stdout()) != "stdout" {
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
	tmpFile, err := os.CreateTemp(t.TempDir(), "")
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
	c, err := os.ReadFile(fp)
	if err != nil {
		t.Fatalf("Unable to read downloaded output file %v: %v", fp, err)
	}
	got = string(c)
	if got != want {
		t.Fatalf("Incorrect content in downloaded file %v, want %v, got %v", fp, want, got)
	}
}

func TestTool_UploadBlob(t *testing.T) {
	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()
	cas := e.Server.CAS

	tmpFile := path.Join(t.TempDir(), "blob")
	if err := os.WriteFile(tmpFile, []byte("Hello, World!"), 0777); err != nil {
		t.Fatalf("Could not create temp blob: %v", err)
	}

	dg, err := digest.NewFromFile(tmpFile)
	if err != nil {
		t.Fatalf("digest.NewFromFile('%v') failed: %v", tmpFile, err)
	}

	toolClient := &Client{GrpcClient: e.Client.GrpcClient}
	if err := toolClient.UploadBlob(context.Background(), tmpFile); err != nil {
		t.Fatalf("UploadBlob('%v', '%v') failed: %v", dg.String(), tmpFile, err)
	}

	// First request should upload the blob.
	if cas.BlobWrites(dg) != 1 {
		t.Fatalf("Expected 1 write for blob '%v', got %v", dg.String(), cas.BlobWrites(dg))
	}

	// Retries should check whether the blob already exists and skip uploading if it does.
	if err := toolClient.UploadBlob(context.Background(), tmpFile); err != nil {
		t.Fatalf("UploadBlob('%v', '%v') failed: %v", dg.String(), tmpFile, err)
	}
	if cas.BlobWrites(dg) != 1 {
		t.Fatalf("Expected 1 write for blob '%v', got %v", dg.String(), cas.BlobWrites(dg))
	}
}
