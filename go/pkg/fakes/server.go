package fakes

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/api/fakes"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/chunker"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/client"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/command"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/filemetadata"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/rexec"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	// Redundant imports are required for the google3 mirror. Aliases should not be changed.
	rc "github.com/bazelbuild/remote-apis-sdks/go/pkg/client"
	regrpc "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	bsgrpc "google.golang.org/genproto/googleapis/bytestream"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/protobuf/types/known/anypb"
	dpb "google.golang.org/protobuf/types/known/durationpb"
	tspb "google.golang.org/protobuf/types/known/timestamppb"
)

// Server is a configurable fake in-process RBE server for use in integration tests.
type Server struct {
	Exec        *Exec
	CAS         *CAS
	LogStreams  *LogStreams
	ActionCache *ActionCache
	listener    net.Listener
	srv         *grpc.Server
}

// NewServer creates a server that is ready to accept requests.
func NewServer(t testing.TB) (s *Server, err error) {
	cas := NewCAS()
	ls := NewLogStreams()
	ac := NewActionCache()
	s = &Server{Exec: NewExec(t, ac, cas), CAS: cas, LogStreams: ls, ActionCache: ac}
	s.listener, err = net.Listen("tcp", ":0")
	if err != nil {
		return nil, err
	}
	s.srv = grpc.NewServer()
	bsgrpc.RegisterByteStreamServer(s.srv, s)
	regrpc.RegisterContentAddressableStorageServer(s.srv, s.CAS)
	regrpc.RegisterActionCacheServer(s.srv, s.ActionCache)
	regrpc.RegisterCapabilitiesServer(s.srv, s.Exec)
	regrpc.RegisterExecutionServer(s.srv, s.Exec)
	go s.srv.Serve(s.listener)
	return s, nil
}

// Clear clears the fake results.
func (s *Server) Clear() {
	s.CAS.Clear()
	s.LogStreams.Clear()
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
	return rc.NewClient(ctx, "instance", s.dialParams())
}

// NewClientConn returns a gRPC client connction to the server.
func (s *Server) NewClientConn(ctx context.Context) (*grpc.ClientConn, error) {
	p := s.dialParams()
	conn, _, err := client.Dial(ctx, p.Service, p)
	return conn, err
}

func (s *Server) dialParams() rc.DialParams {
	return rc.DialParams{
		Service:    s.listener.Addr().String(),
		NoSecurity: true,
	}
}

// Read will serve both logstream and CAS resources, depending on the resource type indicated in the
// request.
func (s *Server) Read(req *bspb.ReadRequest, stream bsgrpc.ByteStream_ReadServer) error {
	path := strings.Split(req.ResourceName, "/")
	if len(path) < 2 || path[0] != "instance" {
		return status.Errorf(codes.InvalidArgument, "test fake expected resource name of the form \"instance/<type>/...\", got %q", req.ResourceName)
	}
	if path[1] == "logstreams" {
		return s.LogStreams.Read(req, stream)
	} else if path[1] == "blobs" || path[1] == "compressed-blobs" {
		return s.CAS.Read(req, stream)
	}
	return status.Errorf(codes.InvalidArgument, "invalid resource type: %q", path[1])
}

// Write writes a blob to CAS.
func (s *Server) Write(stream bsgrpc.ByteStream_WriteServer) error {
	return s.CAS.Write(stream)
}

// QueryWriteStatus implements the corresponding RE API function.
func (*Server) QueryWriteStatus(context.Context, *bspb.QueryWriteStatusRequest) (*bspb.QueryWriteStatusResponse, error) {
	return nil, status.Error(codes.Unimplemented, "test fake does not implement method")
}

// TestEnv is a wrapper for convenient integration tests of remote execution.
type TestEnv struct {
	Client   *rexec.Client
	Server   *Server
	ExecRoot string
	t        testing.TB
}

// NewTestEnv initializes a TestEnv containing a fake server, a client connected to it,
// and a temporary directory used as execution root for inputs and outputs.
// It returns the new env and a cleanup function that should be called in the end of the test.
func NewTestEnv(t testing.TB) (*TestEnv, func()) {
	t.Helper()
	// Set up temp directory.
	execRoot := t.TempDir()
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
	return tspb.New(t)
}

// Set sets up the fake to return the given result on the given command execution.
// It is not possible to make the fake result in a LocalErrorResultStatus or an InterruptedResultStatus.
func (e *TestEnv) Set(cmd *command.Command, opt *command.ExecutionOptions, res *command.Result, opts ...Option) (cmdDg, acDg, stderrDg, stdoutDg digest.Digest) {
	e.t.Helper()
	cmd.FillDefaultFieldValues()
	if err := cmd.Validate(); err != nil {
		e.t.Fatalf("command validation failed: %v", err)
	}

	auxMeta := &fakes.AuxiliaryMetadata{FakeMemoryPercentagePeak: 50.0}
	anyAuxMeta, err := anypb.New(auxMeta)
	if err != nil {
		e.t.Fatalf("unable to create fake auxiliary anypb metadata: %v", err)
	}
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
			AuxiliaryMetadata:              []*anypb.Any{anyAuxMeta},
		},
	}
	for _, o := range opts {
		if err := o.apply(ar, e.Server, e.ExecRoot); err != nil {
			e.t.Fatalf("error applying option %+v: %v", o, err)
		}
	}

	execRoot, workingDir, remoteWorkingDir := cmd.ExecRoot, cmd.WorkingDir, cmd.RemoteWorkingDir
	root, inputs, _, err := e.Client.GrpcClient.ComputeMerkleTree(context.Background(), execRoot, workingDir, remoteWorkingDir, cmd.InputSpec, e.Client.FileMetadataCache)
	if err != nil {
		e.t.Fatalf("error building input tree in fake setup: %v", err)
		return digest.Empty, digest.Empty, digest.Empty, digest.Empty
	}
	for _, inp := range inputs {
		ch, err := chunker.New(inp, false, int(e.Client.GrpcClient.ChunkMaxSize))
		if err != nil {
			e.t.Fatalf("error getting data from input entry: %v", err)
		}
		bytes, err := ch.FullData()
		if err != nil {
			e.t.Fatalf("error getting data from input chunker: %v", err)
		}
		e.Server.CAS.Put(bytes)
	}

	cmdPb := cmd.ToREProto(false)
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
		ac.Timeout = dpb.New(cmd.Timeout)
	}
	acDg = digest.TestNewFromMessage(ac)
	if ar.StderrDigest != nil {
		stderrDg = digest.NewFromProtoUnvalidated(ar.StderrDigest)
	}
	if ar.StdoutDigest != nil {
		stdoutDg = digest.NewFromProtoUnvalidated(ar.StdoutDigest)
	}

	bytes, err = proto.Marshal(ac)
	if err != nil {
		e.t.Fatalf("error inserting action digest blob into CAS %v", err)
	}
	e.Server.CAS.Put(bytes)

	e.Server.Exec.adg = acDg
	e.Server.Exec.ActionResult = ar
	switch res.Status {
	case command.TimeoutResultStatus:
		if res.Err == nil {
			e.Server.Exec.Status = status.New(codes.DeadlineExceeded, "timeout")
		} else {
			e.Server.Exec.Status = status.New(codes.DeadlineExceeded, res.Err.Error())
		}
	case command.RemoteErrorResultStatus:
		st, ok := status.FromError(res.Err)
		if !ok {
			if res.Err == nil {
				st = status.New(codes.Internal, "remote error")
			} else {
				st = status.New(codes.Internal, res.Err.Error())
			}
		}
		e.Server.Exec.Status = st
	case command.CacheHitResultStatus:
		if !e.Server.Exec.Cached { // Assume the user means in this case the actual ActionCache should miss.
			e.Server.ActionCache.Put(acDg, ar)
		}
	}
	return cmdDg, acDg, stderrDg, stdoutDg
}

// Option provides extra configuration for the test environment.
type Option interface {
	apply(*repb.ActionResult, *Server, string) error
}

// InputFile to be made available to the fake action.
type InputFile struct {
	Path     string
	Contents string
}

// Apply creates a file in the execroot with the given content
// and also inserts the file blob into CAS.
func (f *InputFile) apply(ac *repb.ActionResult, s *Server, execRoot string) error {
	bytes := []byte(f.Contents)
	if err := os.MkdirAll(filepath.Join(execRoot, filepath.Dir(f.Path)), os.ModePerm); err != nil {
		return fmt.Errorf("failed to create input dir %v: %v", filepath.Dir(f.Path), err)
	}
	err := os.WriteFile(filepath.Join(execRoot, f.Path), bytes, 0755)
	if err != nil {
		return fmt.Errorf("failed to setup file %v under temp exec root %v: %v", f.Path, execRoot, err)
	}
	s.CAS.Put(bytes)
	return nil
}

// InputSymlink to be made available to the fake action.
type InputSymlink struct {
	Path    string // newname
	Content string
	Target  string // oldname
}

// Apply puts the target file in the fake CAS and create a symlink in OS.
func (ins *InputSymlink) apply(ac *repb.ActionResult, s *Server, execRoot string) error {
	inf := InputFile{Path: ins.Target, Contents: ins.Content}
	inf.apply(ac, s, execRoot)
	// create a symlink from oldname to newname
	return os.Symlink(filepath.Join(execRoot, ins.Target), filepath.Join(execRoot, ins.Path))
}

// OutputFile is to be added as an output of the fake action.
type OutputFile struct {
	Path     string
	Contents string
}

// Apply puts the file in the fake CAS and the given ActionResult.
func (f *OutputFile) apply(ac *repb.ActionResult, s *Server, execRoot string) error {
	bytes := []byte(f.Contents)
	s.Exec.OutputBlobs = append(s.Exec.OutputBlobs, bytes)
	dg := s.CAS.Put(bytes)
	ac.OutputFiles = append(ac.OutputFiles, &repb.OutputFile{Path: f.Path, Digest: dg.ToProto()})
	return nil
}

// OutputDir is to be added as an output of the fake action.
type OutputDir struct {
	Path string
}

// Apply puts the file in the fake CAS and the given ActionResult.
func (d *OutputDir) apply(ac *repb.ActionResult, s *Server, execRoot string) error {
	root, ch, err := BuildDir(d.Path, s, execRoot)
	if err != nil {
		return fmt.Errorf("failed to build directory tree: %v", err)
	}
	tr := &repb.Tree{
		Root:     root,
		Children: ch,
	}
	treeBlob, err := proto.Marshal(tr)
	if err != nil {
		return fmt.Errorf("failed to marshal tree: %v", err)
	}
	treeDigest := s.CAS.Put(treeBlob)
	ac.OutputDirectories = append(ac.OutputDirectories, &repb.OutputDirectory{Path: d.Path, TreeDigest: treeDigest.ToProto()})
	return nil
}

// OutputSymlink is to be added as an output of the fake action.
type OutputSymlink struct {
	Path   string
	Target string
}

// Apply puts the file in the fake CAS and the given ActionResult.
func (l *OutputSymlink) apply(ac *repb.ActionResult, s *Server, execRoot string) error {
	ac.OutputFileSymlinks = append(ac.OutputFileSymlinks, &repb.OutputSymlink{Path: l.Path, Target: l.Target})
	return nil
}

// BuildDir builds the directory tree by recursively iterating through the directory.
// This is similar to tree.go ComputeMerkleTree.
func BuildDir(path string, s *Server, execRoot string) (root *repb.Directory, childDir []*repb.Directory, err error) {
	res := &repb.Directory{}
	ch := []*repb.Directory{}

	files, err := os.ReadDir(filepath.Join(execRoot, path))
	if err != nil {
		return nil, nil, fmt.Errorf("failed read directory: %v", err)
	}

	for _, file := range files {
		fn := file.Name()
		fp := filepath.Join(execRoot, path, fn)
		if file.IsDir() {
			root, _, err := BuildDir(fp, s, execRoot)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to build directory tree: %v", err)
			}
			res.Directories = append(res.Directories, &repb.DirectoryNode{Name: fn, Digest: digest.TestNewFromMessage(root).ToProto()})
			ch = append(ch, root)
		} else {
			content, err := os.ReadFile(fp)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to read file: %v", err)
			}
			dg := s.CAS.Put(content)
			res.Files = append(res.Files, &repb.FileNode{Name: fn, Digest: dg.ToProto()})
		}
	}
	return res, ch, nil
}

// LogStream adds a new logstream that may be served from the bytestream API.
type LogStream struct {
	// Name is the name of the stream. The stream may be downloaded by fetching the resource named
	// instance/logstreams/<name> from bytestream.
	Name string
	// Chunks is a list of the chunks that will be sent by bytestream.
	Chunks []string
}

func (l *LogStream) apply(ac *repb.ActionResult, s *Server, execRoot string) error {
	return s.LogStreams.Put(l.Name, l.Chunks...)
}

// StdOutStream causes the fake action to indicate this as the name of the stdout logstream.
type StdOutStream string

func (o StdOutStream) apply(ac *repb.ActionResult, s *Server, execRoot string) error {
	s.Exec.StdOutStreamName = string(o)
	return nil
}

// StdErrStream causes the fake action to indicate this as the name of the stderr logstream.
type StdErrStream string

func (e StdErrStream) apply(ac *repb.ActionResult, s *Server, execRoot string) error {
	s.Exec.StdErrStreamName = string(e)
	return nil
}

// StdOut is to be added as an output of the fake action.
type StdOut string

// Apply puts the action stdout in the fake CAS and the given ActionResult.
func (o StdOut) apply(ac *repb.ActionResult, s *Server, execRoot string) error {
	bytes := []byte(o)
	s.Exec.OutputBlobs = append(s.Exec.OutputBlobs, bytes)
	dg := s.CAS.Put(bytes)
	ac.StdoutDigest = dg.ToProto()
	return nil
}

// StdOutRaw is to be added as a raw output of the fake action.
type StdOutRaw string

// Apply puts the action stdout as raw bytes in the given ActionResult.
func (o StdOutRaw) apply(ac *repb.ActionResult, s *Server, execRoot string) error {
	ac.StdoutRaw = []byte(o)
	return nil
}

// StdErr is to be added as an output of the fake action.
type StdErr string

// Apply puts the action stderr in the fake CAS and the given ActionResult.
func (o StdErr) apply(ac *repb.ActionResult, s *Server, execRoot string) error {
	bytes := []byte(o)
	s.Exec.OutputBlobs = append(s.Exec.OutputBlobs, bytes)
	dg := s.CAS.Put(bytes)
	ac.StderrDigest = dg.ToProto()
	return nil
}

// StdErrRaw is to be added as a raw output of the fake action.
type StdErrRaw string

// Apply puts the action stderr as raw bytes in the given ActionResult.
func (o StdErrRaw) apply(ac *repb.ActionResult, s *Server, execRoot string) error {
	ac.StderrRaw = []byte(o)
	return nil
}

// ExecutionCacheHit of true will cause the ActionResult to be returned as a cache hit during
// fake execution.
type ExecutionCacheHit bool

// Apply on true will cause the ActionResult to be returned as a cache hit during fake execution.
func (c ExecutionCacheHit) apply(ac *repb.ActionResult, s *Server, execRoot string) error {
	s.Exec.Cached = bool(c)
	return nil
}
