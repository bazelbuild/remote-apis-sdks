package fakes

import (
	"context"
	"google.golang.org/grpc"
	"net"

	rc "github.com/bazelbuild/remote-apis-sdks/go/client"
	regrpc "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	bsgrpc "google.golang.org/genproto/googleapis/bytestream"
)

// Server is a configurable fake in-process RBE server for use in integration tests.
type Server struct {
	Exec        *Exec
	Cas         *CAS
	ActionCache *ActionCache
	listener    net.Listener
	srv         *grpc.Server
}

// NewServer creates a server that is ready to accept requests.
func NewServer() (s *Server, err error) {
	cas := NewCas()
	ac := NewActionCache()
	s = &Server{Exec: NewExec(ac, cas), Cas: cas, ActionCache: ac}
	s.listener, err = net.Listen("tcp", ":0")
	if err != nil {
		return nil, err
	}
	s.srv = grpc.NewServer()
	bsgrpc.RegisterByteStreamServer(s.srv, s.Cas)
	regrpc.RegisterContentAddressableStorageServer(s.srv, s.Cas)
	regrpc.RegisterActionCacheServer(s.srv, s.ActionCache)
	regrpc.RegisterExecutionServer(s.srv, s.Exec)
	go s.srv.Serve(s.listener)
	return s, nil
}

// Clear clears the fake results.
func (s *Server) Clear() {
	s.Cas.Clear()
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
	return rc.Dial(ctx, "instance", rc.DialParams{
		Service:    s.listener.Addr().String(),
		NoSecurity: true,
	})
}
