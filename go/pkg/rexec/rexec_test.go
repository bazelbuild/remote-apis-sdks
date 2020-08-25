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

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/command"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/fakes"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/outerr"
	"github.com/golang/protobuf/proto"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

func TestExecCacheHit(t *testing.T) {
	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()
	fooPath := filepath.Join(e.ExecRoot, "foo")
	fooBlob := []byte("hello")
	if err := ioutil.WriteFile(fooPath, fooBlob, 0777); err != nil {
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
	cmdDg, acDg := e.Set(cmd, opt, wantRes, &fakes.OutputFile{Path: "a/b/out", Contents: "output"},
		fakes.StdOut("stdout"), fakes.StdErrRaw("stderr"))
	oe := outerr.NewRecordingOutErr()

	for i := 0; i < 2; i++ {
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
			OutputFiles:      1,
			TotalOutputBytes: 18, // "output" + "stdout" + "stderr"
			OutputDigests:    map[string]digest.Digest{"a/b/out": digest.NewFromBlob([]byte("output"))},
		}
		if diff := cmp.Diff(wantRes, res); diff != "" {
			t.Errorf("Run() gave result diff (-want +got):\n%s", diff)
		}
		if diff := cmp.Diff(wantMeta, meta, cmpopts.IgnoreFields(command.Metadata{}, "EventTimes")); diff != "" {
			t.Errorf("Run() gave result diff (-want +got):\n%s", diff)
		}
		var eventNames []string
		for name, interval := range meta.EventTimes {
			eventNames = append(eventNames, name)
			if interval == nil || interval.To.Before(interval.From) {
				t.Errorf("Run() gave bad timing stats for event %v: %v", name, interval)
			}
		}
		wantNames := []string{
			command.EventComputeMerkleTree,
			command.EventCheckActionCache,
			command.EventDownloadResults,
		}
		if diff := cmp.Diff(wantNames, eventNames, cmpopts.SortSlices(func(a, b string) bool { return a < b })); diff != "" {
			t.Errorf("Run gave different events: want %v, got %v", wantNames, eventNames)
		}
		if i == 0 {
			if !bytes.Equal(oe.Stdout(), []byte("stdout")) {
				t.Errorf("Run() gave stdout diff: want \"stdout\", got: %v", oe.Stdout())
			}
			if !bytes.Equal(oe.Stderr(), []byte("stderr")) {
				t.Errorf("Run() gave stderr diff: want \"stderr\", got: %v", oe.Stderr())
			}
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

	res, meta := e.Client.Run(context.Background(), cmd, opt, oe)
	wantMeta := &command.Metadata{
		ActionDigest:     acDg,
		InputDirectories: 1,
		TotalOutputBytes: 10,
	}
	if diff := cmp.Diff(wantRes, res); diff != "" {
		t.Errorf("Run() gave result diff (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(wantMeta, meta, cmpopts.EquateEmpty(), cmpopts.IgnoreFields(command.Metadata{}, "CommandDigest", "TotalInputBytes", "EventTimes", "MissingDigests")); diff != "" {
		t.Errorf("Run() gave result diff (-want +got):\n%s", diff)
	}
	var eventNames []string
	for name, interval := range meta.EventTimes {
		eventNames = append(eventNames, name)
		if interval == nil || interval.To.Before(interval.From) {
			t.Errorf("Run() gave bad timing stats for event %v: %v", name, interval)
		}
	}
	wantNames := []string{
		command.EventComputeMerkleTree,
		command.EventUploadInputs,
		command.EventExecuteRemotely,
		command.EventServerQueued,
		command.EventServerWorker,
		command.EventServerWorkerInputFetch,
		command.EventServerWorkerExecution,
		command.EventServerWorkerOutputUpload,
		command.EventDownloadResults,
	}
	if diff := cmp.Diff(wantNames, eventNames, cmpopts.SortSlices(func(a, b string) bool { return a < b })); diff != "" {
		t.Errorf("Run gave different events: want %v, got %v", wantNames, eventNames)
	}

	if diff := cmp.Diff(wantRes, res); diff != "" {
		t.Errorf("Run() gave result diff (-want +got):\n%s", diff)
	}
	if !bytes.Equal(oe.Stdout(), []byte("not cached")) {
		t.Errorf("Run() gave stdout diff: want \"not cached\", got: %v", oe.Stdout())
	}
	// We did specify DoNotCache=false, so the new result should now be cached:
	if diff := cmp.Diff(e.Server.Exec.ActionResult, e.Server.ActionCache.Get(acDg), cmp.Comparer(proto.Equal)); diff != "" {
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
			e.Client.GrpcClient.Retrier = nil // Disable retries
			cmd := &command.Command{
				Args:        []string{"tool"},
				OutputFiles: []string{"a/b/out"},
				ExecRoot:    e.ExecRoot,
			}
			opt := command.DefaultExecutionOptions()
			e.Set(cmd, opt, tc.wantRes, fakes.StdErr("stderr"), &fakes.OutputFile{Path: "a/b/out", Contents: "output"})
			oe := outerr.NewRecordingOutErr()

			res, _ := e.Client.Run(context.Background(), cmd, opt, oe)

			if diff := cmp.Diff(tc.wantRes, res, cmp.Comparer(proto.Equal), cmp.Comparer(equalError)); diff != "" {
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

func equalError(x, y error) bool {
	return x == y || (x != nil && y != nil && x.Error() == y.Error())
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
			e.Client.GrpcClient.Retrier = nil // Disable retries
			cmd := &command.Command{
				Args:        []string{"tool"},
				OutputFiles: []string{"a/b/out"},
				ExecRoot:    e.ExecRoot,
			}
			opt := &command.ExecutionOptions{AcceptCached: true, DownloadOutputs: false}
			e.Set(cmd, opt, tc.wantRes, fakes.StdErr("stderr"), &fakes.OutputFile{Path: "a/b/out", Contents: "output"}, fakes.ExecutionCacheHit(tc.cached))
			oe := outerr.NewRecordingOutErr()

			res, _ := e.Client.Run(context.Background(), cmd, opt, oe)

			if diff := cmp.Diff(tc.wantRes, res, cmp.Comparer(proto.Equal), cmp.Comparer(equalError)); diff != "" {
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

func TestUpdateRemoteCache(t *testing.T) {
	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()
	fooPath := filepath.Join(e.ExecRoot, "foo")
	fooBlob := []byte("hello")
	if err := ioutil.WriteFile(fooPath, fooBlob, 0777); err != nil {
		t.Fatalf("failed to write input file %s", fooBlob)
	}
	cmd := &command.Command{
		Args:        []string{"tool"},
		ExecRoot:    e.ExecRoot,
		InputSpec:   &command.InputSpec{Inputs: []string{"foo"}},
		OutputFiles: []string{"a/b/out"},
	}
	opt := command.DefaultExecutionOptions()
	oe := outerr.NewRecordingOutErr()

	ec, err := e.Client.NewContext(context.Background(), cmd, opt, oe)
	if err != nil {
		t.Fatalf("failed creating execution context: %v", err)
	}
	// Simulating local execution.
	outPath := filepath.Join(e.ExecRoot, "a/b/out")
	if err := os.MkdirAll(filepath.Dir(outPath), os.FileMode(0777)); err != nil {
		t.Fatalf("failed to create output file parents %s: %v", outPath, err)
	}
	outBlob := []byte("out!")
	if err := ioutil.WriteFile(outPath, outBlob, 0777); err != nil {
		t.Fatalf("failed to write output file %s: %v", outPath, err)
	}
	ec.UpdateCachedResult()
	if diff := cmp.Diff(&command.Result{Status: command.SuccessResultStatus}, ec.Result); diff != "" {
		t.Errorf("UpdateCachedResult() gave result diff (-want +got):\n%s", diff)
	}
	if _, ok := e.Server.CAS.Get(ec.Metadata.ActionDigest); !ok {
		t.Error("UpdateCachedResult() failed to upload Action proto")
	}
	if _, ok := e.Server.CAS.Get(ec.Metadata.CommandDigest); !ok {
		t.Error("UpdateCachedResult() failed to upload Command proto")
	}
	// Now delete the local result and check that we get a remote cache hit and download it.
	if err := os.Remove(outPath); err != nil {
		t.Fatalf("failed to remove output file %s", outPath)
	}
	ec.GetCachedResult()
	if diff := cmp.Diff(&command.Result{Status: command.CacheHitResultStatus}, ec.Result); diff != "" {
		t.Errorf("GetCachedResult() gave result diff (-want +got):\n%s", diff)
	}
	contents, err := ioutil.ReadFile(outPath)
	if err != nil {
		t.Errorf("error reading from %s: %v", outPath, err)
	}
	if !bytes.Equal(contents, outBlob) {
		t.Errorf("expected %s to contain %q, got %v", outPath, string(outBlob), contents)
	}
	file, err := os.Stat(outPath)
	if err != nil {
		t.Errorf("error reading from %s: %v", outPath, err)
	} else if (file.Mode() & 0100) == 0 {
		t.Errorf("expected %s to have executable permission", outPath)
	}
	if len(oe.Stdout()) != 0 {
		t.Errorf("GetCachedResult() gave unexpected stdout: %v", oe.Stdout())
	}
	if len(oe.Stderr()) != 0 {
		t.Errorf("GetCachedResult() gave unexpected stdout: %v", oe.Stderr())
	}
}

func TestDownloadResults(t *testing.T) {
	e, cleanup := fakes.NewTestEnv(t)
	defer cleanup()
	fooPath := filepath.Join(e.ExecRoot, "foo")
	fooBlob := []byte("hello")
	if err := ioutil.WriteFile(fooPath, fooBlob, 0777); err != nil {
		t.Fatalf("failed to write input file %s", fooBlob)
	}
	cmd := &command.Command{
		Args:        []string{"tool"},
		ExecRoot:    e.ExecRoot,
		InputSpec:   &command.InputSpec{Inputs: []string{"foo"}},
		OutputFiles: []string{"a/b/out"},
	}
	opt := &command.ExecutionOptions{AcceptCached: true, DownloadOutputs: false}
	oe := outerr.NewRecordingOutErr()
	ec, err := e.Client.NewContext(context.Background(), cmd, opt, oe)
	if err != nil {
		t.Fatalf("failed creating execution context: %v", err)
	}
	outPath := filepath.Join(e.ExecRoot, "a/b/out")
	outBlob := []byte("out!")
	wantRes := &command.Result{Status: command.CacheHitResultStatus}
	e.Set(cmd, opt, wantRes, &fakes.OutputFile{Path: "a/b/out", Contents: string(outBlob)},
		fakes.StdOut("stdout"), fakes.StdErrRaw("stderr"))
	ec.GetCachedResult()
	if diff := cmp.Diff(wantRes, ec.Result); diff != "" {
		t.Errorf("GetCachedResult() gave result diff (-want +got):\n%s", diff)
	}
	if _, err := os.Stat(outPath); !os.IsNotExist(err) {
		t.Errorf("expected output file %s to not be downloaded, but it was", outPath)
	}
	if len(oe.Stdout()) == 0 {
		t.Errorf("GetCachedResult() gave unexpected stdout: %v", oe.Stdout())
	}
	if len(oe.Stderr()) == 0 {
		t.Errorf("GetCachedResult() gave unexpected stderr: %v", oe.Stderr())
	}
	ec.DownloadResults(e.ExecRoot)
	contents, err := ioutil.ReadFile(outPath)
	if err != nil {
		t.Errorf("error reading from %s: %v", outPath, err)
	}
	if !bytes.Equal(contents, outBlob) {
		t.Errorf("expected %s to contain %q, got %v", outPath, string(outBlob), contents)
	}
}
