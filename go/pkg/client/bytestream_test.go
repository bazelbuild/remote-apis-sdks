package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	// Redundant imports are required for the google3 mirror. Aliases should not be changed.
	bsgrpc "google.golang.org/genproto/googleapis/bytestream"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

var logStreamData = []byte("Hello World! This is large data to send.")

type logStream struct {
	logStreamID   string
	logicalOffset int64
	finalized     bool
}

func TestReadTimeout(t *testing.T) {
	s := newServer(t)
	defer s.shutDown()

	s.client.Retrier = nil
	s.client.rpcTimeouts["Read"] = 100 * time.Millisecond
	s.fake.read = func(req *bspb.ReadRequest, stream bsgrpc.ByteStream_ReadServer) error {
		time.Sleep(1 * time.Second)
		return stream.Send(&bspb.ReadResponse{})
	}

	_, err := s.client.ReadBytes(context.Background(), "test")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected error %v, but got %v", context.DeadlineExceeded, err)
	}
}

func TestWriteTimeout(t *testing.T) {
	s := newServer(t)
	defer s.shutDown()

	s.client.Retrier = nil
	s.client.rpcTimeouts["Write"] = 100 * time.Millisecond
	s.fake.write = func(stream bsgrpc.ByteStream_WriteServer) error {
		time.Sleep(1 * time.Second)
		return fmt.Errorf("write should have timed out")
	}

	err := s.client.WriteBytes(context.Background(), "test", []byte("hello"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected error %v, but got %v", context.DeadlineExceeded, err)
	}
}

func TestWriteBytesAtRemoteOffsetSuccess_LogStream(t *testing.T) {
	tests := []struct {
		description   string
		ls            *logStream
		data          []byte
		dataPartsLen  int
		doNotFinalize bool
		initialOffset int64
		wantBytesLen  int64
	}{
		{
			description:   "valid data with offset 0",
			ls:            &logStream{logicalOffset: 0},
			data:          logStreamData,
			doNotFinalize: true,
			initialOffset: 0,
			dataPartsLen:  3,
			wantBytesLen:  int64(len(logStreamData)),
		},
		{
			description:   "valid data with non-zero offset",
			ls:            &logStream{logicalOffset: 4},
			data:          logStreamData,
			doNotFinalize: true,
			initialOffset: 4,
			dataPartsLen:  3,
			wantBytesLen:  int64(len(logStreamData)),
		},
		{
			description:   "one big chunk",
			ls:            &logStream{logicalOffset: 0},
			data:          logStreamData,
			doNotFinalize: false,
			initialOffset: 0,
			dataPartsLen:  1,
			wantBytesLen:  int64(len(logStreamData)),
		},
		{
			description:   "empty data",
			ls:            &logStream{logicalOffset: 0},
			data:          []byte{},
			doNotFinalize: false,
			initialOffset: 0,
			dataPartsLen:  1,
			wantBytesLen:  0,
		},
	}

	b := newServer(t)
	defer b.shutDown()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			size := len(test.data)/test.dataPartsLen + 1
			start := int(test.initialOffset)
			end := size
			lsID := test.description
			test.ls.logStreamID = lsID
			b.fake.logStreams[lsID] = test.ls
			ChunkMaxSize(size).Apply(b.client)

			for i := 0; i < test.dataPartsLen; i++ {
				if end > len(test.data) {
					end = len(test.data)
				}
				if i == test.dataPartsLen-1 {
					test.doNotFinalize = false
				}

				writtenBytes, err := b.client.WriteBytesAtRemoteOffset(b.ctx, lsID, test.data[start:end], test.doNotFinalize, test.initialOffset)
				if err != nil {
					t.Errorf("WriteBytesAtRemoteOffset() failed unexpectedly: %v", err)
				}
				if b.fake.logStreams[lsID].logicalOffset != int64(end) {
					t.Errorf("WriteBytesAtRemoteOffset() = %d, want %d", b.fake.logStreams[lsID].logicalOffset, end)
				}
				// LogStream shouldn't be finalized when we set ByteStreamOptFinishWrite false.
				if i != test.dataPartsLen-1 && b.fake.logStreams[lsID].finalized {
					t.Error("WriteBytesAtRemoteOffset() incorrectly finalized LogStream")
				}

				test.initialOffset += writtenBytes
				start = end
				end += size
			}

			if b.fake.logStreams[lsID].logicalOffset != test.wantBytesLen {
				t.Errorf("WriteBytesAtRemoteOffset() = %d, want %d", b.fake.logStreams[lsID].logicalOffset, test.wantBytesLen)
			}
			if !b.fake.logStreams[lsID].finalized {
				t.Error("WriteBytesAtRemoteOffset() didn't correctly finalize LogStream")
			}
		})
	}
}

func TestWriteBytesAtRemoteOffsetErrors_LogStream(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping slow test because short is set")
	}
	tests := []struct {
		description   string
		ls            *logStream
		data          []byte
		initialOffset int64
	}{
		{
			description:   "invalid write to finalized logstream",
			ls:            &logStream{logicalOffset: 0, finalized: true},
			data:          logStreamData,
			initialOffset: 0,
		},
		{
			description:   "not found",
			ls:            nil,
			data:          logStreamData,
			initialOffset: 0,
		},
		{
			description:   "invalid smaller offset",
			ls:            &logStream{logicalOffset: 4},
			data:          logStreamData,
			initialOffset: 1,
		},
		{
			description:   "invalid larger offset",
			ls:            &logStream{logicalOffset: 2},
			data:          logStreamData,
			initialOffset: 4,
		},
		{
			description:   "invalid negative offset",
			ls:            &logStream{logicalOffset: 0},
			data:          logStreamData,
			initialOffset: -1,
		},
	}

	b := newServer(t)
	defer b.shutDown()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			lsID := test.description
			if test.ls != nil {
				test.ls.logStreamID = lsID
				b.fake.logStreams[lsID] = test.ls
			}
			data := []byte("Hello World!")
			ChunkMaxSize(len(data)).Apply(b.client)

			writtenBytes, err := b.client.WriteBytesAtRemoteOffset(b.ctx, lsID, data, false, test.initialOffset)
			if err == nil {
				t.Errorf("WriteBytesAtRemoteOffset(ctx, %s, %s, false, %d) got nil error, want non-nil error", lsID, string(data), test.initialOffset)
			}
			if writtenBytes != 0 {
				t.Errorf("WriteBytesAtRemoteOffset(ctx, %s, %s, false, %d) got %d byte(s), want 0 byte", lsID, string(data), test.initialOffset, writtenBytes)
			}
		})
	}
}

type ByteStream struct {
	logStreams map[string]*logStream
	read       func(req *bspb.ReadRequest, stream bsgrpc.ByteStream_ReadServer) error
	write      func(stream bsgrpc.ByteStream_WriteServer) error
}

type Server struct {
	client   *Client
	listener net.Listener
	server   *grpc.Server
	fake     *ByteStream
	ctx      context.Context
}

func newServer(t *testing.T) *Server {
	s := &Server{ctx: context.Background()}
	var err error
	s.listener, err = net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	s.server = grpc.NewServer()
	s.fake = &ByteStream{logStreams: make(map[string]*logStream)}
	bsgrpc.RegisterByteStreamServer(s.server, s.fake)

	go s.server.Serve(s.listener)
	s.client, err = NewClient(s.ctx, "test", DialParams{
		Service:    s.listener.Addr().String(),
		NoSecurity: true,
	}, StartupCapabilities(false), ChunkMaxSize(2))
	if err != nil {
		t.Fatalf("Error connecting to server: %v", err)
	}
	return s
}

func (s *Server) shutDown() {
	s.client.Close()
	s.listener.Close()
	s.server.Stop()
}

func (b *ByteStream) QueryWriteStatus(context.Context, *bspb.QueryWriteStatusRequest) (*bspb.QueryWriteStatusResponse, error) {
	return &bspb.QueryWriteStatusResponse{}, nil
}

func (b *ByteStream) Read(req *bspb.ReadRequest, stream bsgrpc.ByteStream_ReadServer) error {
	if b.read != nil {
		return b.read(req, stream)
	}
	return stream.Send(&bspb.ReadResponse{Data: logStreamData})
}

// Write implements the write operation for LogStream Write API.
func (b *ByteStream) Write(stream bsgrpc.ByteStream_WriteServer) error {
	if b.write != nil {
		return b.write(stream)
	}

	defer stream.SendAndClose(&bspb.WriteResponse{})
	req, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("failed to write: %w", err)
	}

	ls, ok := b.logStreams[req.GetResourceName()]
	if !ok {
		return fmt.Errorf("unable to find LogStream")
	}
	if ls.finalized {
		return fmt.Errorf("unable to extend finalized LogStream")
	}
	if ls.logicalOffset != req.GetWriteOffset() || ls.logicalOffset < 0 {
		return fmt.Errorf("incorrect LogStream offset")
	}
	ls.finalized = req.GetFinishWrite()
	ls.logicalOffset += int64(len(req.GetData()))

	return nil
}
