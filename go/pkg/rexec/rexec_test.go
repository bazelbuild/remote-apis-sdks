// Package rexec_test contains tests for rexec package. It is a different package to avoid an
// import cycle.
package rexec_test

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/command"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/fakes"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/outerr"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

func TestExecCacheHit(t *testing.T) {
	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()
	fooPath := filepath.Join(e.ExecRoot, "foo")
	fooBlob := []byte("hello")
	if err := ioutil.WriteFile(fooPath, []byte("hello"), 0777); err != nil {
		t.Fatalf("failed to write input file %s", fooBlob)
	}
	cmd := &command.Command{
		Args:        []string{"tool"},
		ExecRoot:    e.ExecRoot,
		InputSpec:   &command.InputSpec{Inputs: []string{"foo"}},
		OutputFiles: []string{"a/b/out"},
	}
	opt := command.DefaultExecutionOptions()
	wantRes := &command.Result{Status: command.CacheHitResultStatus}
	cmdDg, acDg := e.Set(cmd, opt, wantRes, &fakes.OutputFile{"a/b/out", "output"}, fakes.StdOut("stdout"), fakes.StdErrRaw("stderr"))
	oe := outerr.NewRecordingOutErr()

	res, meta := e.Client.Run(context.Background(), cmd, opt, oe)

	fooDg := digest.NewFromBlob(fooBlob)
	fooDir := &repb.Directory{Files: []*repb.FileNode{{Name: "foo", Digest: fooDg.ToProto(), IsExecutable: true}}}
	fooDirDg, err := digest.NewFromMessage(fooDir)
	if err != nil {
		t.Fatalf("failed digesting message %v: %v", fooDir, err)
	}
	wantMeta := &command.Metadata{
		CommandDigest:    cmdDg,
		ActionDigest:     acDg,
		InputDirectories: 1,
		InputFiles:       1,
		TotalInputBytes:  fooDirDg.Size + cmdDg.Size + acDg.Size + fooDg.Size,
	}
	if diff := cmp.Diff(wantRes, res); diff != "" {
		t.Errorf("Run() gave result diff (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(wantMeta, meta); diff != "" {
		t.Errorf("Run() gave result diff (-want +got):\n%s", diff)
	}
	if !bytes.Equal(oe.Stdout(), []byte("stdout")) {
		t.Errorf("Run() gave stdout diff: want \"stdout\", got: %v", oe.Stdout())
	}
	if !bytes.Equal(oe.Stderr(), []byte("stderr")) {
		t.Errorf("Run() gave stderr diff: want \"stderr\", got: %v", oe.Stderr())
	}
	path := filepath.Join(e.ExecRoot, "a/b/out")
	contents, err := ioutil.ReadFile(path)
	if err != nil {
		t.Errorf("error reading from %s: %v", path, err)
	}
	if !bytes.Equal(contents, []byte("output")) {
		t.Errorf("expected %s to contain \"output\", got %v", path, contents)
	}
}

// TestExecNotAcceptCached should skip both client-side and server side action cache lookups.
func TestExecNotAcceptCached(t *testing.T) {
	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()
	cmd := &command.Command{Args: []string{"tool"}, ExecRoot: e.ExecRoot}
	opt := &command.ExecutionOptions{AcceptCached: false}
	wantRes := &command.Result{Status: command.SuccessResultStatus}
	_, acDg := e.Set(cmd, opt, wantRes, fakes.StdOutRaw("not cached"))
	e.Server.ActionCache.Put(acDg, &repb.ActionResult{StdoutRaw: []byte("cached")})

	oe := outerr.NewRecordingOutErr()

	res, _ := e.Client.Run(context.Background(), cmd, opt, oe)

	if diff := cmp.Diff(wantRes, res); diff != "" {
		t.Errorf("Run() gave result diff (-want +got):\n%s", diff)
	}
	if !bytes.Equal(oe.Stdout(), []byte("not cached")) {
		t.Errorf("Run() gave stdout diff: want \"not cached\", got: %v", oe.Stdout())
	}
	// We did specify DoNotCache=false, so the new result should now be cached:
	if diff := cmp.Diff(e.Server.Exec.ActionResult, e.Server.ActionCache.Get(acDg)); diff != "" {
		t.Errorf("Run() did not cache executed result  (-want +got):\n%s", diff)
	}
}

func TestExecManualCacheMiss(t *testing.T) {
	tests := []struct {
		name   string
		cached bool
		want   command.ResultStatus
	}{
		{
			name:   "remote hit",
			cached: true,
			want:   command.CacheHitResultStatus,
		},
		{
			name:   "remote miss",
			cached: false,
			want:   command.SuccessResultStatus,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e, cleanup := fakes.NewTestEnv(t)
			defer cleanup()
			cmd := &command.Command{Args: []string{"tool"}, ExecRoot: e.ExecRoot}
			opt := &command.ExecutionOptions{AcceptCached: true, DownloadOutputs: true}
			wantRes := &command.Result{Status: tc.want}
			e.Set(cmd, opt, wantRes, fakes.StdErr("stderr"), fakes.ExecutionCacheHit(tc.cached))
			oe := outerr.NewRecordingOutErr()

			res, _ := e.Client.Run(context.Background(), cmd, opt, oe)

			if diff := cmp.Diff(wantRes, res); diff != "" {
				t.Errorf("Run() gave result diff (-want +got):\n%s", diff)
			}
			if !bytes.Equal(oe.Stderr(), []byte("stderr")) {
				t.Errorf("Run() gave stderr diff: want \"stderr\", got: %v", oe.Stderr())
			}
		})
	}
}

func TestExecDoNotCache_NotAcceptCached(t *testing.T) {
	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()
	cmd := &command.Command{Args: []string{"tool"}, ExecRoot: e.ExecRoot}
	// DoNotCache true implies in particular that we also skip action cache lookups, local or remote.
	opt := &command.ExecutionOptions{DoNotCache: true}
	wantRes := &command.Result{Status: command.SuccessResultStatus}
	_, acDg := e.Set(cmd, opt, wantRes, fakes.StdOutRaw("not cached"))
	e.Server.ActionCache.Put(acDg, &repb.ActionResult{StdoutRaw: []byte("cached")})
	oe := outerr.NewRecordingOutErr()

	res, _ := e.Client.Run(context.Background(), cmd, opt, oe)

	if diff := cmp.Diff(wantRes, res); diff != "" {
		t.Errorf("Run() gave result diff (-want +got):\n%s", diff)
	}
	if !bytes.Equal(oe.Stdout(), []byte("not cached")) {
		t.Errorf("Run() gave stdout diff: want \"not cached\", got: %v", oe.Stdout())
	}
	// The action cache should still contain the same result, because we specified DoNotCache.
	if !bytes.Equal(e.Server.ActionCache.Get(acDg).StdoutRaw, []byte("cached")) {
		t.Error("Run() cached result for do_not_cache=true")
	}
}

func TestExecRemoteFailureDownloadsPartialResults(t *testing.T) {
	tests := []struct {
		name    string
		wantRes *command.Result
	}{
		{
			name:    "non zero exit",
			wantRes: &command.Result{ExitCode: 52, Status: command.NonZeroExitResultStatus},
		},
		{
			name:    "remote error",
			wantRes: command.NewRemoteErrorResult(status.New(codes.Internal, "problem").Err()),
		},
		{
			name:    "timeout",
			wantRes: command.NewTimeoutResult(),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e, cleanup := fakes.NewTestEnv(t)
			defer cleanup()
			cmd := &command.Command{
				Args:        []string{"tool"},
				OutputFiles: []string{"a/b/out"},
				ExecRoot:    e.ExecRoot,
			}
			opt := command.DefaultExecutionOptions()
			e.Set(cmd, opt, tc.wantRes, fakes.StdErr("stderr"), &fakes.OutputFile{"a/b/out", "output"})
			oe := outerr.NewRecordingOutErr()

			res, _ := e.Client.Run(context.Background(), cmd, opt, oe)

			if diff := cmp.Diff(tc.wantRes, res); diff != "" {
				t.Errorf("Run() gave result diff (-want +got):\n%s", diff)
			}
			if len(oe.Stdout()) != 0 {
				t.Errorf("Run() gave unexpected stdout: %v", oe.Stdout())
			}
			if !bytes.Equal(oe.Stderr(), []byte("stderr")) {
				t.Errorf("Run() gave stderr diff: want \"stderr\", got: %v", oe.Stderr())
			}
			path := filepath.Join(e.ExecRoot, "a/b/out")
			contents, err := ioutil.ReadFile(path)
			if err != nil {
				t.Errorf("error reading from %s: %v", path, err)
			}
			if !bytes.Equal(contents, []byte("output")) {
				t.Errorf("expected %s to contain \"output\", got %v", path, contents)
			}
		})
	}
}

func TestDoNotDownloadOutputs(t *testing.T) {
	tests := []struct {
		name     string
		cached   bool
		status   *status.Status
		exitCode int32
		wantRes  *command.Result
	}{
		{
			name:    "success",
			wantRes: &command.Result{Status: command.SuccessResultStatus},
		},
		{
			name:    "remote exec cache hit",
			cached:  true,
			wantRes: &command.Result{Status: command.CacheHitResultStatus},
		},
		{
			name:    "action cache hit",
			wantRes: &command.Result{Status: command.CacheHitResultStatus},
		},
		{
			name:     "non zero exit",
			exitCode: 11,
			wantRes:  &command.Result{ExitCode: 11, Status: command.NonZeroExitResultStatus},
		},
		{
			name:    "timeout",
			status:  status.New(codes.DeadlineExceeded, "timeout"),
			wantRes: command.NewTimeoutResult(),
		},
		{
			name:    "remote failure",
			status:  status.New(codes.Internal, "problem"),
			wantRes: command.NewRemoteErrorResult(status.New(codes.Internal, "problem").Err()),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e, cleanup := fakes.NewTestEnv(t)
			defer cleanup()
			cmd := &command.Command{
				Args:        []string{"tool"},
				OutputFiles: []string{"a/b/out"},
				ExecRoot:    e.ExecRoot,
			}
			opt := &command.ExecutionOptions{AcceptCached: true, DownloadOutputs: false}
			e.Set(cmd, opt, tc.wantRes, fakes.StdErr("stderr"), &fakes.OutputFile{"a/b/out", "output"}, fakes.ExecutionCacheHit(tc.cached))
			oe := outerr.NewRecordingOutErr()

			res, _ := e.Client.Run(context.Background(), cmd, opt, oe)

			if diff := cmp.Diff(tc.wantRes, res); diff != "" {
				t.Errorf("Run() gave result diff (-want +got):\n%s", diff)
			}
			if len(oe.Stdout()) != 0 {
				t.Errorf("Run() gave unexpected stdout: %v", oe.Stdout())
			}
			if !bytes.Equal(oe.Stderr(), []byte("stderr")) {
				t.Errorf("Run() gave stderr diff: want \"stderr\", got: %v", oe.Stderr())
			}
			path := filepath.Join(e.ExecRoot, "a/b/out")
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Errorf("expected output file %s to not be downloaded, but it was", path)
			}
		})
	}
}
