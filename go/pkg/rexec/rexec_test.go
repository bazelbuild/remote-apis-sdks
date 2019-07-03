package rexec

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/internal/test/fakes"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/command"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/outerr"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

// newTestEnv initializes the fake server, a client connected to it, and a temporary directory.
func newTestEnv(t *testing.T) (*Client, *fakes.Server, string, func()) {
	// Set up temp directory.
	execRoot, err := ioutil.TempDir("", strings.ReplaceAll(t.Name(), string(filepath.Separator), "_"))
	if err != nil {
		t.Fatalf("failed to make temp dir: %v", err)
	}
	// Set up the fake.
	s, err := fakes.NewServer()
	if err != nil {
		t.Fatalf("Error starting fake server: %v", err)
	}
	grpcClient, err := s.NewTestClient(context.Background())
	if err != nil {
		t.Fatalf("Error connecting to server: %v", err)
	}
	return New(grpcClient, nil), s, execRoot, func() {
		grpcClient.Close()
		s.Stop()
		os.RemoveAll(execRoot)
	}
}

func TestExecCacheHit(t *testing.T) {
	c, s, execRoot, cleanup := newTestEnv(t)
	defer cleanup()
	cmdPb := &repb.Command{
		Arguments:   []string{"tool"},
		OutputFiles: []string{"a/b/out"},
	}
	cmdDg := digest.TestNewFromMessage(cmdPb)
	ac := &repb.Action{
		CommandDigest:   cmdDg.ToProto(),
		InputRootDigest: digest.Empty.ToProto(),
	}
	acDg := digest.TestNewFromMessage(ac)
	outDg := digest.NewFromBlob([]byte("output"))
	errDg := digest.NewFromBlob([]byte("stderr"))
	ar := &repb.ActionResult{
		ExitCode:     0,
		OutputFiles:  []*repb.OutputFile{&repb.OutputFile{Path: "a/b/out", Digest: outDg.ToProto()}},
		StdoutRaw:    []byte("stdout"),
		StderrDigest: errDg.ToProto(),
	}
	s.ActionCache.PutAction(ac, ar)
	// Populate action outputs in CAS manually, because Execute will never be called.
	s.CAS.Put([]byte("output"))
	s.CAS.Put([]byte("stderr"))

	cmd := &command.Command{
		Args:        []string{"tool"},
		ExecRoot:    execRoot,
		OutputFiles: []string{"a/b/out"},
	}
	oe := outerr.NewRecordingOutErr()

	res, meta := c.Run(context.Background(), cmd, command.DefaultExecutionOptions(), oe)

	wantRes := &command.Result{Status: command.CacheHitResultStatus}
	wantMeta := &command.Metadata{
		CommandDigest: cmdDg,
		ActionDigest:  acDg,
	}
	if diff := cmp.Diff(wantRes, res); diff != "" {
		t.Errorf("Run() gave result diff (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(wantMeta, meta); diff != "" {
		t.Errorf("Run() gave result diff (-want +got):\n%s", diff)
	}
	if !bytes.Equal(oe.GetStdout(), []byte("stdout")) {
		t.Errorf("Run() gave stdout diff: want \"stdout\", got: %v", oe.GetStdout())
	}
	if !bytes.Equal(oe.GetStderr(), []byte("stderr")) {
		t.Errorf("Run() gave stderr diff: want \"stderr\", got: %v", oe.GetStderr())
	}
	path := filepath.Join(execRoot, "a/b/out")
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
	c, s, execRoot, cleanup := newTestEnv(t)
	defer cleanup()
	cmdPb := &repb.Command{
		Arguments: []string{"tool"},
	}
	cmdDg := digest.TestNewFromMessage(cmdPb)
	ac := &repb.Action{
		CommandDigest:   cmdDg.ToProto(),
		InputRootDigest: digest.Empty.ToProto(),
	}
	acDg := digest.TestNewFromMessage(ac)
	s.ActionCache.PutAction(ac, &repb.ActionResult{StdoutRaw: []byte("cached")})
	s.Exec.ActionResult = &repb.ActionResult{StdoutRaw: []byte("not cached")}

	cmd := &command.Command{
		Args:     []string{"tool"},
		ExecRoot: execRoot,
	}
	opt := &command.ExecutionOptions{AcceptCached: false}
	oe := outerr.NewRecordingOutErr()

	res, _ := c.Run(context.Background(), cmd, opt, oe)

	wantRes := &command.Result{Status: command.SuccessResultStatus}
	if diff := cmp.Diff(wantRes, res); diff != "" {
		t.Errorf("Run() gave result diff (-want +got):\n%s", diff)
	}
	if !bytes.Equal(oe.GetStdout(), []byte("not cached")) {
		t.Errorf("Run() gave stdout diff: want \"not cached\", got: %v", oe.GetStdout())
	}
	// We did specify DoNotCache=false, so the new result should now be cached:
	if diff := cmp.Diff(s.Exec.ActionResult, s.ActionCache.Get(acDg)); diff != "" {
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
			c, s, execRoot, cleanup := newTestEnv(t)
			defer cleanup()
			errDg := digest.NewFromBlob([]byte("stderr"))
			ar := &repb.ActionResult{
				ExitCode:     0,
				StderrDigest: errDg.ToProto(),
			}
			s.Exec.Cached = tc.cached
			s.Exec.ActionResult = ar
			s.Exec.OutputBlobs = [][]byte{[]byte("stderr")}

			cmd := &command.Command{
				Args:     []string{"tool"},
				ExecRoot: execRoot,
			}
			opt := &command.ExecutionOptions{
				AcceptCached:    true,
				DownloadOutputs: true,
			}
			oe := outerr.NewRecordingOutErr()
			res, _ := c.Run(context.Background(), cmd, opt, oe)
			wantRes := &command.Result{Status: tc.want}
			if diff := cmp.Diff(wantRes, res); diff != "" {
				t.Errorf("Run() gave result diff (-want +got):\n%s", diff)
			}
			if !bytes.Equal(oe.GetStderr(), []byte("stderr")) {
				t.Errorf("Run() gave stderr diff: want \"stderr\", got: %v", oe.GetStderr())
			}
		})
	}
}

func TestExecDoNotCache_NotAcceptCached(t *testing.T) {
	c, s, execRoot, cleanup := newTestEnv(t)
	defer cleanup()
	cmdPb := &repb.Command{
		Arguments: []string{"tool"},
	}
	cmdDg := digest.TestNewFromMessage(cmdPb)
	ac := &repb.Action{
		CommandDigest:   cmdDg.ToProto(),
		InputRootDigest: digest.Empty.ToProto(),
	}
	acDg := digest.TestNewFromMessage(ac)
	s.ActionCache.PutAction(ac, &repb.ActionResult{StdoutRaw: []byte("cached")})
	s.Exec.ActionResult = &repb.ActionResult{StdoutRaw: []byte("not cached")}

	cmd := &command.Command{
		Args:     []string{"tool"},
		ExecRoot: execRoot,
	}
	// DoNotCache true implies in particular that we also skip action cache lookups, local or remote.
	opt := &command.ExecutionOptions{DoNotCache: true}
	oe := outerr.NewRecordingOutErr()

	res, _ := c.Run(context.Background(), cmd, opt, oe)

	wantRes := &command.Result{Status: command.SuccessResultStatus}
	if diff := cmp.Diff(wantRes, res); diff != "" {
		t.Errorf("Run() gave result diff (-want +got):\n%s", diff)
	}
	if !bytes.Equal(oe.GetStdout(), []byte("not cached")) {
		t.Errorf("Run() gave stdout diff: want \"not cached\", got: %v", oe.GetStdout())
	}
	// The action cache should still contain the same result, because we specified DoNotCache.
	if !bytes.Equal(s.ActionCache.Get(acDg).StdoutRaw, []byte("cached")) {
		t.Error("Run() cached result for do_not_cache=true")
	}
}

func TestExecNonZeroExit(t *testing.T) {
	c, s, execRoot, cleanup := newTestEnv(t)
	defer cleanup()
	s.Exec.ActionResult = &repb.ActionResult{ExitCode: 52, StderrRaw: []byte("error")}

	cmd := &command.Command{
		Args:     []string{"tool"},
		ExecRoot: execRoot,
	}
	oe := outerr.NewRecordingOutErr()

	res, _ := c.Run(context.Background(), cmd, command.DefaultExecutionOptions(), oe)

	wantRes := &command.Result{ExitCode: 52, Status: command.NonZeroExitResultStatus}
	if diff := cmp.Diff(wantRes, res); diff != "" {
		t.Errorf("Run() gave result diff (-want +got):\n%s", diff)
	}
	if !bytes.Equal(oe.GetStderr(), []byte("error")) {
		t.Errorf("Run() gave stderr diff: want \"error\", got: %v", oe.GetStderr())
	}
}

func TestExecRemoteFailure(t *testing.T) {
	c, s, execRoot, cleanup := newTestEnv(t)
	defer cleanup()
	s.Exec.Status = status.New(codes.Internal, "problem")

	cmd := &command.Command{
		Args:     []string{"tool"},
		ExecRoot: execRoot,
	}
	oe := outerr.NewRecordingOutErr()

	res, _ := c.Run(context.Background(), cmd, command.DefaultExecutionOptions(), oe)

	wantRes := command.NewRemoteErrorResult(s.Exec.Status.Err())
	if diff := cmp.Diff(wantRes, res); diff != "" {
		t.Errorf("Run() gave result diff (-want +got):\n%s", diff)
	}
	if len(oe.GetStdout()) != 0 {
		t.Errorf("Run() gave unexpected stdout: %v", oe.GetStdout())
	}
	if len(oe.GetStderr()) != 0 {
		t.Errorf("Run() gave unexpected stderr: %v", oe.GetStderr())
	}
}

func TestExecRemoteFailureDownloadsPartialResults(t *testing.T) {
	c, s, execRoot, cleanup := newTestEnv(t)
	defer cleanup()
	outDg := digest.NewFromBlob([]byte("output"))
	errDg := digest.NewFromBlob([]byte("stderr"))
	s.Exec.ActionResult = &repb.ActionResult{
		ExitCode:     53,
		StderrDigest: errDg.ToProto(),
		OutputFiles:  []*repb.OutputFile{&repb.OutputFile{Path: "a/b/out", Digest: outDg.ToProto()}},
	}
	s.Exec.OutputBlobs = [][]byte{[]byte("stderr"), []byte("output")}
	s.Exec.Status = status.New(codes.Internal, "problem")

	cmd := &command.Command{
		Args:        []string{"tool"},
		OutputFiles: []string{"a/b/out"},
		ExecRoot:    execRoot,
	}
	oe := outerr.NewRecordingOutErr()

	res, _ := c.Run(context.Background(), cmd, command.DefaultExecutionOptions(), oe)

	wantRes := command.NewRemoteErrorResult(s.Exec.Status.Err())
	if diff := cmp.Diff(wantRes, res); diff != "" {
		t.Errorf("Run() gave result diff (-want +got):\n%s", diff)
	}
	if len(oe.GetStdout()) != 0 {
		t.Errorf("Run() gave unexpected stdout: %v", oe.GetStdout())
	}
	if !bytes.Equal(oe.GetStderr(), []byte("stderr")) {
		t.Errorf("Run() gave stderr diff: want \"stderr\", got: %v", oe.GetStderr())
	}
	path := filepath.Join(execRoot, "a/b/out")
	contents, err := ioutil.ReadFile(path)
	if err != nil {
		t.Errorf("error reading from %s: %v", path, err)
	}
	if !bytes.Equal(contents, []byte("output")) {
		t.Errorf("expected %s to contain \"output\", got %v", path, contents)
	}
}

func TestExecTimeoutDownloadsPartialResults(t *testing.T) {
	c, s, execRoot, cleanup := newTestEnv(t)
	defer cleanup()
	outDg := digest.NewFromBlob([]byte("output"))
	errDg := digest.NewFromBlob([]byte("stderr"))
	s.Exec.ActionResult = &repb.ActionResult{
		ExitCode:     53,
		StderrDigest: errDg.ToProto(),
		OutputFiles:  []*repb.OutputFile{&repb.OutputFile{Path: "a/b/out", Digest: outDg.ToProto()}},
	}
	s.Exec.OutputBlobs = [][]byte{[]byte("stderr"), []byte("output")}
	s.Exec.Status = status.New(codes.DeadlineExceeded, "timeout")

	cmd := &command.Command{
		Args:        []string{"tool"},
		OutputFiles: []string{"a/b/out"},
		ExecRoot:    execRoot,
	}
	oe := outerr.NewRecordingOutErr()

	res, _ := c.Run(context.Background(), cmd, command.DefaultExecutionOptions(), oe)

	wantRes := command.NewTimeoutResult()
	if diff := cmp.Diff(wantRes, res); diff != "" {
		t.Errorf("Run() gave result diff (-want +got):\n%s", diff)
	}
	if len(oe.GetStdout()) != 0 {
		t.Errorf("Run() gave unexpected stdout: %v", oe.GetStdout())
	}
	if !bytes.Equal(oe.GetStderr(), []byte("stderr")) {
		t.Errorf("Run() gave stderr diff: want \"stderr\", got: %v", oe.GetStderr())
	}
	path := filepath.Join(execRoot, "a/b/out")
	contents, err := ioutil.ReadFile(path)
	if err != nil {
		t.Errorf("error reading from %s: %v", path, err)
	}
	if !bytes.Equal(contents, []byte("output")) {
		t.Errorf("expected %s to contain \"output\", got %v", path, contents)
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
			name:    "remote cached",
			cached:  true,
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
			c, s, execRoot, cleanup := newTestEnv(t)
			defer cleanup()
			outDg := digest.NewFromBlob([]byte("output"))
			errDg := digest.NewFromBlob([]byte("stderr"))
			s.Exec.ActionResult = &repb.ActionResult{
				ExitCode:     tc.exitCode,
				StderrDigest: errDg.ToProto(),
				OutputFiles:  []*repb.OutputFile{&repb.OutputFile{Path: "a/b/out", Digest: outDg.ToProto()}},
			}
			s.Exec.Cached = tc.cached
			s.Exec.OutputBlobs = [][]byte{[]byte("stderr"), []byte("output")}
			s.Exec.Status = tc.status

			cmd := &command.Command{
				Args:        []string{"tool"},
				OutputFiles: []string{"a/b/out"},
				ExecRoot:    execRoot,
			}
			oe := outerr.NewRecordingOutErr()

			opt := &command.ExecutionOptions{AcceptCached: true, DownloadOutputs: false}
			res, _ := c.Run(context.Background(), cmd, opt, oe)

			if diff := cmp.Diff(tc.wantRes, res); diff != "" {
				t.Errorf("Run() gave result diff (-want +got):\n%s", diff)
			}
			if len(oe.GetStdout()) != 0 {
				t.Errorf("Run() gave unexpected stdout: %v", oe.GetStdout())
			}
			if !bytes.Equal(oe.GetStderr(), []byte("stderr")) {
				t.Errorf("Run() gave stderr diff: want \"stderr\", got: %v", oe.GetStderr())
			}
			path := filepath.Join(execRoot, "a/b/out")
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Errorf("expected output file %s to not be downloaded, but it was", path)
			}
		})
	}
}

func TestDoNotDownloadOutputs_cached(t *testing.T) {
	c, s, execRoot, cleanup := newTestEnv(t)
	defer cleanup()
	cmdPb := &repb.Command{
		Arguments:   []string{"tool"},
		OutputFiles: []string{"a/b/out"},
	}
	cmdDg := digest.TestNewFromMessage(cmdPb)
	ac := &repb.Action{
		CommandDigest:   cmdDg.ToProto(),
		InputRootDigest: digest.Empty.ToProto(),
	}
	outDg := digest.NewFromBlob([]byte("output"))
	errDg := digest.NewFromBlob([]byte("stderr"))
	ar := &repb.ActionResult{
		ExitCode:     0,
		OutputFiles:  []*repb.OutputFile{&repb.OutputFile{Path: "a/b/out", Digest: outDg.ToProto()}},
		StderrDigest: errDg.ToProto(),
	}
	s.ActionCache.PutAction(ac, ar)
	// Populate action outputs in CAS manually, because Execute will never be called.
	s.CAS.Put([]byte("output"))
	s.CAS.Put([]byte("stderr"))

	opt := &command.ExecutionOptions{
		AcceptCached:    true,
		DownloadOutputs: false,
	}
	cmd := &command.Command{
		Args:        []string{"tool"},
		ExecRoot:    execRoot,
		OutputFiles: []string{"a/b/out"},
	}
	oe := outerr.NewRecordingOutErr()

	res, _ := c.Run(context.Background(), cmd, opt, oe)

	wantRes := &command.Result{Status: command.CacheHitResultStatus}
	if diff := cmp.Diff(wantRes, res); diff != "" {
		t.Errorf("Run() gave result diff (-want +got):\n%s", diff)
	}
	if len(oe.GetStdout()) != 0 {
		t.Errorf("Run() gave unexpected stdout: %v", oe.GetStdout())
	}
	if !bytes.Equal(oe.GetStderr(), []byte("stderr")) {
		t.Errorf("Run() gave stderr diff: want \"stderr\", got: %v", oe.GetStderr())
	}
	path := filepath.Join(execRoot, "a/b/out")
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("expected output file %s to not be downloaded, but it was", path)
	}
}

func TestBuildCommand(t *testing.T) {
	tests := []struct {
		name    string
		cmd     *command.Command
		wantCmd *repb.Command
	}{
		{
			name:    "pass args",
			cmd:     &command.Command{Args: []string{"foo", "bar"}},
			wantCmd: &repb.Command{Arguments: []string{"foo", "bar"}},
		},
		{
			name:    "pass working directory",
			cmd:     &command.Command{WorkingDir: "a/b"},
			wantCmd: &repb.Command{WorkingDirectory: "a/b"},
		},
		{
			name:    "sort output files",
			cmd:     &command.Command{OutputFiles: []string{"foo", "bar", "abc"}},
			wantCmd: &repb.Command{OutputFiles: []string{"abc", "bar", "foo"}},
		},
		{
			name:    "sort output directories",
			cmd:     &command.Command{OutputDirs: []string{"foo", "bar", "abc"}},
			wantCmd: &repb.Command{OutputDirectories: []string{"abc", "bar", "foo"}},
		},
		{
			name: "sort environment variables",
			cmd: &command.Command{
				InputSpec: &command.InputSpec{
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
			cmd:  &command.Command{Platform: map[string]string{"b": "3", "a": "2", "c": "1"}},
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
			gotCmd := buildCommand(tc.cmd)
			if diff := cmp.Diff(tc.wantCmd, gotCmd, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("%s: buildCommand gave result diff (-want +got):\n%s", tc.name, diff)
			}
		})
	}
}
