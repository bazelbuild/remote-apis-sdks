package fakes

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/command"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/filemetadata"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/rexec"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/tree"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	rc "github.com/bazelbuild/remote-apis-sdks/go/pkg/client"
	regrpc "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	log "github.com/golang/glog"
	tspb "github.com/golang/protobuf/ptypes/timestamp"
	bsgrpc "google.golang.org/genproto/googleapis/bytestream"
)

// Server is a configurable fake in-process RBE server for use in integration tests.
type Server struct {
	Exec        *Exec
	CAS         *CAS
	ActionCache *ActionCache
	listener    net.Listener
	srv         *grpc.Server
}

// NewServer creates a server that is ready to accept requests.
func NewServer(t *testing.T) (s *Server, err error) {
	cas := NewCAS()
	ac := NewActionCache()
	s = &Server{Exec: NewExec(t, ac, cas), CAS: cas, ActionCache: ac}
	s.listener, err = net.Listen("tcp", ":0")
	if err != nil {
		return nil, err
	}
	s.srv = grpc.NewServer()
	bsgrpc.RegisterByteStreamServer(s.srv, s.CAS)
	regrpc.RegisterContentAddressableStorageServer(s.srv, s.CAS)
	regrpc.RegisterActionCacheServer(s.srv, s.ActionCache)
	regrpc.RegisterExecutionServer(s.srv, s.Exec)
	go s.srv.Serve(s.listener)
	return s, nil
}

// Clear clears the fake results.
func (s *Server) Clear() {
	s.CAS.Clear()
	s.ActionCache.Clear()
	s.Exec.Clear()
}

// Stop shuts down the in process server.
func (s *Server) Stop() {
	s.listener.Close()
	s.srv.Stop()
}

// NewTestClient returns a new in-process Client connected to this server.
func (s *Server) NewTestClient(ctx context.Context) (*rc.Client, error) {
	return rc.NewClient(ctx, "instance", rc.DialParams{
		Service:    s.listener.Addr().String(),
		NoSecurity: true,
	})
}

// TestEnv is a wrapper for convenient integration tests of remote execution.
type TestEnv struct {
	Client   *rexec.Client
	Server   *Server
	ExecRoot string
	t        *testing.T
}

// NewTestEnv initializes a TestEnv containing a fake server, a client connected to it,
// and a temporary directory used as execution root for inputs and outputs.
// It returns the new env and a cleanup function that should be called in the end of the test.
func NewTestEnv(t *testing.T) (*TestEnv, func()) {
	t.Helper()
	// Set up temp directory.
	execRoot, err := ioutil.TempDir("", strings.ReplaceAll(t.Name(), string(filepath.Separator), "_"))
	if err != nil {
		t.Fatalf("failed to make temp dir: %v", err)
	}
	// Set up the fake.
	s, err := NewServer(t)
	if err != nil {
		t.Fatalf("Error starting fake server: %v", err)
	}
	grpcClient, err := s.NewTestClient(context.Background())
	if err != nil {
		t.Fatalf("Error connecting to server: %v", err)
	}
	return &TestEnv{
			Client: &rexec.Client{
				FileMetadataCache: filemetadata.NewNoopCache(),
				GrpcClient:        grpcClient,
			},
			Server:   s,
			ExecRoot: execRoot,
			t:        t,
		}, func() {
			grpcClient.Close()
			s.Stop()
			os.RemoveAll(execRoot)
		}
}

func timeToProto(t time.Time) *tspb.Timestamp {
	if t.IsZero() {
		return nil
	}
	ts, err := ptypes.TimestampProto(t)
	if err != nil {
		log.Warningf("Unable to convert time to Timestamp: %v", err.Error())
		return nil
	}
	return ts
}

// Set sets up the fake to return the given result on the given command execution.
// It is not possible to make the fake result in a LocalErrorResultStatus or an InterruptedResultStatus.
func (e *TestEnv) Set(cmd *command.Command, opt *command.ExecutionOptions, res *command.Result, opts ...option) (cmdDg, acDg digest.Digest) {
	e.t.Helper()
	cmd.FillDefaultFieldValues()

	t, _ := time.Parse(time.RFC3339, "2006-01-02T15:04:05Z")
	ar := &repb.ActionResult{
		ExitCode: int32(res.ExitCode),
		ExecutionMetadata: &repb.ExecutedActionMetadata{
			QueuedTimestamp:                timeToProto(t.Add(time.Millisecond)),
			WorkerStartTimestamp:           timeToProto(t.Add(2 * time.Millisecond)),
			WorkerCompletedTimestamp:       timeToProto(t.Add(3 * time.Millisecond)),
			InputFetchStartTimestamp:       timeToProto(t.Add(4 * time.Millisecond)),
			InputFetchCompletedTimestamp:   timeToProto(t.Add(5 * time.Millisecond)),
			ExecutionStartTimestamp:        timeToProto(t.Add(6 * time.Millisecond)),
			ExecutionCompletedTimestamp:    timeToProto(t.Add(7 * time.Millisecond)),
			OutputUploadStartTimestamp:     timeToProto(t.Add(8 * time.Millisecond)),
			OutputUploadCompletedTimestamp: timeToProto(t.Add(9 * time.Millisecond)),
		},
	}
	for _, o := range opts {
		if err := o.Apply(ar, e.Server, e.ExecRoot); err != nil {
			e.t.Fatalf("error applying option %+v: %v", o, err)
		}
	}

	root, inputs, _, err := tree.ComputeMerkleTree(cmd.ExecRoot, cmd.InputSpec, int(e.Client.GrpcClient.ChunkMaxSize), e.Client.FileMetadataCache)
	if err != nil {
		e.t.Fatalf("error building input tree in fake setup: %v", err)
		return digest.Empty, digest.Empty
	}
	for _, inp := range inputs {
		bytes, err := inp.FullData()
		if err != nil {
			e.t.Fatalf("error getting data from input chunker: %v", err)
		}
		e.Server.CAS.Put(bytes)
	}

	cmdPb := cmd.ToREProto()
	bytes, err := proto.Marshal(cmdPb)
	if err != nil {
		e.t.Fatalf("error inserting command digest blob into CAS %v", err)
	}
	e.Server.CAS.Put(bytes)

	cmdDg = digest.TestNewFromMessage(cmdPb)
	ac := &repb.Action{
		CommandDigest:   cmdDg.ToProto(),
		InputRootDigest: root.ToProto(),
		DoNotCache:      opt.DoNotCache,
	}
	if cmd.Timeout > 0 {
		ac.Timeout = ptypes.DurationProto(cmd.Timeout)
	}
	acDg = digest.TestNewFromMessage(ac)

	bytes, err = proto.Marshal(ac)
	if err != nil {
		e.t.Fatalf("error inserting action digest blob into CAS %v", err)
	}
	e.Server.CAS.Put(bytes)

	e.Server.Exec.adg = acDg
	e.Server.Exec.ActionResult = ar
	switch res.Status {
	case command.TimeoutResultStatus:
		e.Server.Exec.Status = status.New(codes.DeadlineExceeded, "timeout")
	case command.RemoteErrorResultStatus:
		st, ok := status.FromError(res.Err)
		if !ok {
			st = status.New(codes.Internal, "remote error")
		}
		e.Server.Exec.Status = st
	case command.CacheHitResultStatus:
		if !e.Server.Exec.Cached { // Assume the user means in this case the actual ActionCache should miss.
			e.Server.ActionCache.Put(acDg, ar)
		}
	}
	return cmdDg, acDg
}

type option interface {
	Apply(*repb.ActionResult, *Server, string) error
}

// InputFile to be made available to the fake action.
type InputFile struct {
	Path     string
	Contents string
}

// Apply creates a file in the execroot with the given content
// and also inserts the file blob into CAS.
func (f *InputFile) Apply(ac *repb.ActionResult, s *Server, execRoot string) error {
	bytes := []byte(f.Contents)
	if err := os.MkdirAll(filepath.Join(execRoot, filepath.Dir(f.Path)), os.ModePerm); err != nil {
		return fmt.Errorf("failed to create input dir %v: %v", filepath.Dir(f.Path), err)
	}
	err := ioutil.WriteFile(filepath.Join(execRoot, f.Path), nil, 0755)
	if err != nil {
		return fmt.Errorf("failed to setup file %v under temp exec root %v: %v", f.Path, execRoot, err)
	}
	s.CAS.Put(bytes)
	return nil
}

// OutputFile is to be added as an output of the fake action.
type OutputFile struct {
	Path     string
	Contents string
}

// Apply puts the file in the fake CAS and the given ActionResult.
func (f *OutputFile) Apply(ac *repb.ActionResult, s *Server, execRoot string) error {
	bytes := []byte(f.Contents)
	s.Exec.OutputBlobs = append(s.Exec.OutputBlobs, bytes)
	dg := s.CAS.Put(bytes)
	ac.OutputFiles = append(ac.OutputFiles, &repb.OutputFile{Path: f.Path, Digest: dg.ToProto()})
	return nil
}

// StdOut is to be added as an output of the fake action.
type StdOut string

// Apply puts the action stdout in the fake CAS and the given ActionResult.
func (o StdOut) Apply(ac *repb.ActionResult, s *Server, execRoot string) error {
	bytes := []byte(o)
	s.Exec.OutputBlobs = append(s.Exec.OutputBlobs, bytes)
	dg := s.CAS.Put(bytes)
	ac.StdoutDigest = dg.ToProto()
	return nil
}

// StdOutRaw is to be added as a raw output of the fake action.
type StdOutRaw string

// Apply puts the action stdout as raw bytes in the given ActionResult.
func (o StdOutRaw) Apply(ac *repb.ActionResult, s *Server, execRoot string) error {
	ac.StdoutRaw = []byte(o)
	return nil
}

// StdErr is to be added as an output of the fake action.
type StdErr string

// Apply puts the action stderr in the fake CAS and the given ActionResult.
func (o StdErr) Apply(ac *repb.ActionResult, s *Server, execRoot string) error {
	bytes := []byte(o)
	s.Exec.OutputBlobs = append(s.Exec.OutputBlobs, bytes)
	dg := s.CAS.Put(bytes)
	ac.StderrDigest = dg.ToProto()
	return nil
}

// StdErrRaw is to be added as a raw output of the fake action.
type StdErrRaw string

// Apply puts the action stderr as raw bytes in the given ActionResult.
func (o StdErrRaw) Apply(ac *repb.ActionResult, s *Server, execRoot string) error {
	ac.StderrRaw = []byte(o)
	return nil
}

// ExecutionCacheHit of true will cause the ActionResult to be returned as a cache hit during
// fake execution.
type ExecutionCacheHit bool

// Apply on true will cause the ActionResult to be returned as a cache hit during fake execution.
func (c ExecutionCacheHit) Apply(ac *repb.ActionResult, s *Server, execRoot string) error {
	s.Exec.Cached = bool(c)
	return nil
}
