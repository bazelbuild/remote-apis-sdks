package client

import (
	"context"
	"fmt"
	"net"
	"testing"

	"google.golang.org/grpc"

	bsgrpc "google.golang.org/genproto/googleapis/bytestream"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var (
	logStreams map[string]*logStream
)

type logStream struct {
	logStreamID   string
	logicalOffset int64
	finalized     bool
}

func TestWriteChunkedWithOffset_LogStream(t *testing.T) {
	tests := []struct {
		description string
		ls          *logStream
		data        []byte
		numIterate  int
		offset      int64
		notFound    bool
		wantBytes   int64
		wantErr     bool
	}{
		{
			description: "valid data with offset 0",
			ls:          &logStream{logStreamID: "logstream1", logicalOffset: 0},
			data:        []byte("Hello World! This is large data to send."),
			numIterate:  3,
			offset:      0,
			wantErr:     false,
		},
		{
			description: "valid data with non-zero offset",
			ls:          &logStream{logStreamID: "logstream2", logicalOffset: 4},
			data:        []byte("Hello World! This is large data to send."),
			numIterate:  3,
			offset:      4,
			wantErr:     false,
		},
		{
			description: "one big chunk",
			ls:          &logStream{logStreamID: "logstream3", logicalOffset: 0},
			data:        []byte("Hello World! This is large data to send."),
			numIterate:  1,
			offset:      0,
			wantErr:     false,
		},
		{
			description: "empty data",
			ls:          &logStream{logStreamID: "logstream4", logicalOffset: 0},
			data:        []byte{},

			numIterate: 1,
			offset:     0,
			wantBytes:  0,
			wantErr:    false,
		},
		{
			description: "invalid write to finalized logstream",
			ls:          &logStream{logStreamID: "logstream5", logicalOffset: 5, finalized: true},
			data:        []byte("Hello World! This is large data to send."),
			numIterate:  1,
			offset:      5,
			wantBytes:   0,
			wantErr:     true,
		},
		{
			description: "not found",
			ls:          &logStream{logStreamID: "notFound", logicalOffset: 0},
			data:        []byte("Hello World! This is large data to send."),
			numIterate:  1,
			offset:      0,
			notFound:    true,
			wantBytes:   0,
			wantErr:     true,
		},
		{
			description: "invlid offset",
			ls:          &logStream{logStreamID: "logstream6", logicalOffset: 8},
			data:        []byte("Hello World! This is large data to send."),
			numIterate:  1,
			offset:      5,
			notFound:    false,
			wantBytes:   0,
			wantErr:     true,
		},
	}

	b := setup(t)
	defer b.shutDown()

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			size := len(test.data)/test.numIterate + 1
			start := int(test.offset)
			end := size
			test.wantBytes = int64(len(test.data))
			lsID := test.ls.logStreamID
			opts := &ByteStreamWriteOpts{LastLogicalOffset: test.offset}
			if !test.notFound {
				logStreams[lsID] = test.ls
			}
			ChunkMaxSize(size).Apply(b.client)

			for i := 0; i < test.numIterate; i++ {
				if end > len(test.data) {
					end = len(test.data)
				}
				if i == test.numIterate-1 {
					opts.FinishWrite = true
				}
				writtenBytes, err := b.client.WriteBytesWithOffset(b.ctx, lsID, test.data[start:end], opts)

				if test.wantErr && err == nil {
					t.Fatal("WriteBytesWithOffset() got nil error, want non-nil error")
				}
				if !test.wantErr && err != nil {
					t.Fatalf("WriteBytesWithOffset() failed unexpectedly: %v", err)
				}
				opts.LastLogicalOffset += writtenBytes
				start = end
				end += size
			}

			if !test.wantErr && logStreams[lsID].logicalOffset != test.wantBytes {
				t.Errorf("WriteBytesWithOffset() = %d, want %d", logStreams[lsID].logicalOffset, test.wantBytes)
			}
			if !test.wantErr && logStreams[lsID].finalized == false {
				t.Error("WriteBytesWithOffset() didn't correctly finalize logstream")
			}
		})
	}
}

type Bytestream struct {
}

type Server struct {
	client   *Client
	listener net.Listener
	server   *grpc.Server
	fake     *Bytestream
	ctx      context.Context
}

func setup(t *testing.T) *Server {
	s := &Server{ctx: context.Background()}
	var err error
	s.listener, err = net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	s.server = grpc.NewServer()
	s.fake = &Bytestream{}
	bsgrpc.RegisterByteStreamServer(s.server, s.fake)

	go s.server.Serve(s.listener)
	s.client, err = NewClient(s.ctx, instance, DialParams{
		Service:    s.listener.Addr().String(),
		NoSecurity: true,
	}, StartupCapabilities(false), ChunkMaxSize(2))
	if err != nil {
		t.Fatalf("Error connecting to server: %v", err)
	}
	logStreams = make(map[string]*logStream, 10)
	return s
}

func (s *Server) shutDown() {
	s.client.Close()
	s.listener.Close()
	s.server.Stop()
}

func (b *Bytestream) QueryWriteStatus(context.Context, *bspb.QueryWriteStatusRequest) (*bspb.QueryWriteStatusResponse, error) {
	return &bspb.QueryWriteStatusResponse{}, nil
}

func (b *Bytestream) Read(req *bspb.ReadRequest, stream bsgrpc.ByteStream_ReadServer) error {
	return nil
}

// Write implements the write operation for LogStream Write API.
func (b *Bytestream) Write(stream bsgrpc.ByteStream_WriteServer) error {
	defer stream.SendAndClose(&bspb.WriteResponse{})
	req, err := stream.Recv()
	if err != nil {
		return err
	}

	size := int64(len(req.GetData()))
	ls, ok := logStreams[req.GetResourceName()]
	if !ok {
		return fmt.Errorf("unable to find LogStream")
	}
	if ls.finalized {
		return fmt.Errorf("unable to extend finalized LogStream")
	}
	if ls.logicalOffset != req.GetWriteOffset() {
		return fmt.Errorf("incorrect LogStream offset")
	}
	ls.finalized = req.GetFinishWrite()
	ls.logicalOffset += size

	return nil
}
