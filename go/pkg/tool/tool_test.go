package tool

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/command"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/fakes"
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
	tmpDir := os.TempDir()
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
