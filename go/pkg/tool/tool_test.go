package tool

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/command"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/fakes"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/outerr"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/encoding/prototext"

	cpb "github.com/bazelbuild/remote-apis-sdks/go/api/command"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

var fooProperties = &cpb.NodeProperties{Properties: []*cpb.NodeProperty{{Name: "fooName", Value: "fooValue"}}}

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
				"a/b/input2.txt",
			},
			InputNodeProperties: map[string]*cpb.NodeProperties{"a/b/input2.txt": fooProperties},
		},
		OutputFiles: []string{"a/b/out"},
	}

	opt := command.DefaultExecutionOptions()
	_, acDg, _, _ := e.Set(cmd, opt, &command.Result{Status: command.CacheHitResultStatus}, &fakes.OutputFile{Path: "a/b/out", Contents: "output"},
		fakes.StdOut("stdout"), fakes.StdErr("stderr"), &fakes.InputFile{Path: "a/b/input.txt", Contents: "input"}, &fakes.InputFile{Path: "a/b/input2.txt", Contents: "input2"})

	toolClient := &Client{GrpcClient: e.Client.GrpcClient}
	got, err := toolClient.ShowAction(context.Background(), acDg.String())
	if err != nil {
		t.Fatalf("ShowAction(%v) failed: %v", acDg.String(), err)
	}
	want := fmt.Sprintf(`Command
=======
Command Digest: 76a608e419da9ed3673f59b8b903f21dbf7cc3178281029151a090cac02d9e4d/15
	tool

Platform
========

Inputs
======
[Root directory digest: 456e94a43b31b158fa7b3fe8d3a8cd6f0b66ef8a6a05ab8350e03df83b9740b6/75]
a/b/input.txt: [File digest: c96c6d5be8d08a12e7b5cdc1b207fa6b2430974c86803d8891675e76fd992c20/5]
a/b/input2.txt: [File digest: 124d8541ff3d7a18b95432bdfbecd86816b86c8265bff44ef629765afb25f06b/6] [Node properties: %s]

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
`, prototext.MarshalOptions{Multiline: false}.Format(fooProperties))
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("ShowAction(%v) returned diff (-want +got): %v\n\ngot: %v\n\nwant: %v\n", acDg.String(), diff, got, want)
	}
}

func TestTool_CheckDeterminism(t *testing.T) {
	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()
	cmd := &command.Command{
		Args:        []string{"foo", "bar", "baz"},
		ExecRoot:    e.ExecRoot,
		InputSpec:   &command.InputSpec{Inputs: []string{"i1", "i2"}},
		OutputFiles: []string{"a/b/out"},
	}
	_, acDg, _, _ := e.Set(cmd, command.DefaultExecutionOptions(), &command.Result{Status: command.SuccessResultStatus}, &fakes.InputFile{Path: "i1", Contents: "i1"}, &fakes.InputFile{Path: "i2", Contents: "i2"}, &fakes.OutputFile{Path: "a/b/out", Contents: "out"})

	client := &Client{GrpcClient: e.Client.GrpcClient}
	if err := client.CheckDeterminism(context.Background(), acDg.String(), "", 2); err != nil {
		t.Errorf("CheckDeterminism returned an error: %v", err)
	}
	// Now execute again and return a different output.
	testOnlyStartDeterminismExec = func() {
		e.Set(cmd, command.DefaultExecutionOptions(), &command.Result{Status: command.SuccessResultStatus}, &fakes.InputFile{Path: "i1", Contents: "i1"}, &fakes.InputFile{Path: "i2", Contents: "i2"}, &fakes.OutputFile{Path: "a/b/out", Contents: "out2"})
	}
	defer func() { testOnlyStartDeterminismExec = func() {} }()
	if err := client.CheckDeterminism(context.Background(), acDg.String(), "", 2); err == nil {
		t.Errorf("CheckDeterminism returned nil, want error")
	}
}

func TestTool_DownloadAction(t *testing.T) {
	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()
	cmd := &command.Command{
		Args:        []string{"foo", "bar", "baz"},
		ExecRoot:    e.ExecRoot,
		InputSpec:   &command.InputSpec{Inputs: []string{"i1", "a/b/i2"}, InputNodeProperties: map[string]*cpb.NodeProperties{"i1": fooProperties}},
		OutputFiles: []string{"a/b/out"},
	}
	_, acDg, _, _ := e.Set(cmd, command.DefaultExecutionOptions(), &command.Result{Status: command.SuccessResultStatus}, &fakes.InputFile{Path: "i1", Contents: "i1"}, &fakes.InputFile{Path: "a/b/i2", Contents: "i2"})

	client := &Client{GrpcClient: e.Client.GrpcClient}
	tmpDir := filepath.Join(t.TempDir(), "action_root")
	os.MkdirAll(tmpDir, os.ModePerm)
	if err := client.DownloadAction(context.Background(), acDg.String(), tmpDir, true); err != nil {
		t.Errorf("error DownloadAction: %v", err)
	}

	reCmdPb := &repb.Command{
		Arguments:   []string{"foo", "bar", "baz"},
		OutputFiles: []string{"a/b/out"},
	}
	acPb := &repb.Action{
		CommandDigest:   digest.TestNewFromMessage(reCmdPb).ToProto(),
		InputRootDigest: &repb.Digest{Hash: "01dc5b6fa6d16d30bf80a7d88ff58f13fe801c4060b9782253b94e793444df05", SizeBytes: 176},
	}
	ipPb := &cpb.InputSpec{InputNodeProperties: cmd.InputSpec.InputNodeProperties}
	expectedContents := []struct {
		path     string
		contents string
	}{
		{
			path:     "ac.textproto",
			contents: prototext.Format(acPb),
		},
		{
			path:     "cmd.textproto",
			contents: prototext.Format(reCmdPb),
		},
		{
			path:     "input_node_properties.textproto",
			contents: prototext.Format(ipPb),
		},
		{
			path:     "input/i1",
			contents: "i1",
		},
		{
			path:     "input/a/b/i2",
			contents: "i2",
		},
	}
	for _, ec := range expectedContents {
		fp := filepath.Join(tmpDir, ec.path)
		got, err := os.ReadFile(fp)
		if err != nil {
			t.Fatalf("Unable to read downloaded action file %v: %v", fp, err)
		}
		if diff := cmp.Diff(ec.contents, string(got)); diff != "" {
			t.Errorf("Incorrect content in downloaded file %v: diff (-want +got): %v\n\ngot: %s\n\nwant: %v\n", fp, diff, got, ec.contents)
		}
	}
}

func TestTool_ExecuteAction(t *testing.T) {
	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()
	cmd := &command.Command{
		Args:        []string{"foo", "bar", "baz"},
		ExecRoot:    e.ExecRoot,
		InputSpec:   &command.InputSpec{Inputs: []string{"i1", "i2"}, InputNodeProperties: map[string]*cpb.NodeProperties{"i1": fooProperties}},
		OutputFiles: []string{"a/b/out"},
	}
	opt := &command.ExecutionOptions{AcceptCached: false, DownloadOutputs: true, DownloadOutErr: true}
	_, acDg, _, _ := e.Set(cmd, opt, &command.Result{Status: command.SuccessResultStatus}, &fakes.OutputFile{Path: "a/b/out", Contents: "out"},
		&fakes.InputFile{Path: "i1", Contents: "i1"}, &fakes.InputFile{Path: "i2", Contents: "i2"}, fakes.StdOut("stdout"), fakes.StdErr("stderr"))

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
	tmpDir := filepath.Join(t.TempDir(), "action_root")
	os.MkdirAll(tmpDir, os.ModePerm)
	inputRoot := filepath.Join(tmpDir, "input")
	if err := client.DownloadAction(context.Background(), acDg.String(), tmpDir, true); err != nil {
		t.Errorf("error DownloadAction: %v", err)
	}
	if err := os.WriteFile(filepath.Join(inputRoot, "i1"), []byte("i11"), 0644); err != nil {
		t.Fatalf("failed overriding input file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(inputRoot, "i2"), []byte("i22"), 0644); err != nil {
		t.Fatalf("failed overriding input file: %v", err)
	}
	cmd.ExecRoot = inputRoot
	_, acDg2, _, _ := e.Set(cmd, opt, &command.Result{Status: command.SuccessResultStatus}, &fakes.OutputFile{Path: "a/b/out", Contents: "out2"},
		fakes.StdOut("stdout2"), fakes.StdErr("stderr2"))
	if diff := cmp.Diff(acDg, acDg2); diff == "" {
		t.Fatalf("expected action digest to change after input change, got %v\n", acDg)
	}
	oe = outerr.NewRecordingOutErr()
	if _, err := client.ExecuteAction(context.Background(), acDg2.String(), "", tmpDir, oe); err != nil {
		t.Errorf("error executeAction: %v", err)
	}

	fp := filepath.Join(tmpDir, "a/b/out")
	c, err := os.ReadFile(fp)
	if err != nil {
		t.Fatalf("Unable to read downloaded output %v: %v", fp, err)
	}
	if string(c) != "out2" {
		t.Fatalf("Incorrect content in downloaded file %v, want \"out2\", got %s", fp, c)
	}
	if string(oe.Stderr()) != "stderr2" {
		t.Errorf("Incorrect stderr %v, expected \"stderr2\"", oe.Stderr())
	}
	if string(oe.Stdout()) != "stdout2" {
		t.Errorf("Incorrect stdout %v, expected \"stdout2\"", oe.Stdout())
	}
	// Now execute again without node properties.
	fp = filepath.Join(tmpDir, "input_node_properties.textproto")
	if err := os.Remove(fp); err != nil {
		t.Fatalf("Unable to remove %v: %v", fp, err)
	}
	cmd.InputSpec.InputNodeProperties = nil
	_, acDg3, _, _ := e.Set(cmd, opt, &command.Result{Status: command.SuccessResultStatus}, &fakes.OutputFile{Path: "a/b/out", Contents: "out3"},
		fakes.StdOut("stdout3"), fakes.StdErr("stderr3"))
	if diff := cmp.Diff(acDg2, acDg3); diff == "" {
		t.Errorf("Expected action digests to be different when input node properties change, got: %v\n", acDg2)
	}
	oe = outerr.NewRecordingOutErr()
	if _, err := client.ExecuteAction(context.Background(), acDg3.String(), "", tmpDir, oe); err != nil {
		t.Errorf("error executeAction: %v", err)
	}

	fp = filepath.Join(tmpDir, "a/b/out")
	c, err = os.ReadFile(fp)
	if err != nil {
		t.Fatalf("Unable to read downloaded output %v: %v", fp, err)
	}
	if string(c) != "out3" {
		t.Fatalf("Incorrect content in downloaded file %v, want \"out3\", got %s", fp, c)
	}
	if string(oe.Stderr()) != "stderr3" {
		t.Errorf("Incorrect stderr %v, expected \"stderr3\"", oe.Stderr())
	}
	if string(oe.Stdout()) != "stdout3" {
		t.Errorf("Incorrect stdout %v, expected \"stdout3\"", oe.Stdout())
	}
}

func TestTool_ExecuteActionFromRoot(t *testing.T) {
	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()
	cmd := &command.Command{
		Args:        []string{"foo", "bar", "baz"},
		ExecRoot:    e.ExecRoot,
		InputSpec:   &command.InputSpec{Inputs: []string{"i1", "i2"}, InputNodeProperties: map[string]*cpb.NodeProperties{"i1": fooProperties}},
		OutputFiles: []string{"a/b/out"},
	}
	// Create files necessary for the fake
	if err := os.WriteFile(filepath.Join(e.ExecRoot, "i1"), []byte("i1"), 0644); err != nil {
		t.Fatalf("failed creating input file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(e.ExecRoot, "i2"), []byte("i2"), 0644); err != nil {
		t.Fatalf("failed creating input file: %v", err)
	}
	opt := &command.ExecutionOptions{AcceptCached: false, DownloadOutputs: false, DownloadOutErr: true}
	e.Set(cmd, opt, &command.Result{Status: command.SuccessResultStatus}, &fakes.OutputFile{Path: "a/b/out", Contents: "out"},
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
	ipPb := &cpb.InputSpec{
		InputNodeProperties: map[string]*cpb.NodeProperties{"i1": fooProperties},
	}
	if err := os.WriteFile(filepath.Join(e.ExecRoot, "input_node_properties.textproto"), []byte(prototext.Format(ipPb)), 0644); err != nil {
		t.Fatalf("failed creating input node properties file: %v", err)
	}
	reCmdPb := &repb.Command{
		Arguments:   []string{"foo", "bar", "baz"},
		OutputFiles: []string{"a/b/out"},
	}
	if err := os.WriteFile(filepath.Join(e.ExecRoot, "cmd.textproto"), []byte(prototext.Format(reCmdPb)), 0644); err != nil {
		t.Fatalf("failed creating command file: %v", err)
	}
	// The tool will embed the Command proto digest into the Action proto, so the `command_digest` field is effectively ignored:
	if err := os.WriteFile(filepath.Join(e.ExecRoot, "ac.textproto"), []byte(`command_digest: {hash: "whatever"}`), 0644); err != nil {
		t.Fatalf("failed creating action file: %v", err)
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
