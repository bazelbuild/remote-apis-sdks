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
	logStreams    map[string]*logStream
	logStreamData = []byte("Hello World! This is large data to send.")
)

type logStream struct {
	logStreamID   string
	logicalOffset int64
	finalized     bool
}

func TestWriteBytesWithOffsetSuccess_LogStream(t *testing.T) {
	tests := []struct {
		description string
		ls          *logStream
		data        []byte
		numIterate  int
		opts        []ByteStreamWriteOption
		wantBytes   int64
	}{
		{
			description: "valid data with offset 0",
			ls:          &logStream{logStreamID: "logstream1", logicalOffset: 0},
			data:        logStreamData,
			opts:        []ByteStreamWriteOption{ByteStreamOptOffset(0), ByteSteramOptFinishWrite(false)},
			numIterate:  3,
			wantBytes:   int64(len(logStreamData)),
		},
		{
			description: "valid data with non-zero offset",
			ls:          &logStream{logStreamID: "logstream2", logicalOffset: 4},
			data:        logStreamData,
			opts:        []ByteStreamWriteOption{ByteStreamOptOffset(4), ByteSteramOptFinishWrite(false)},
			numIterate:  3,
			wantBytes:   int64(len(logStreamData)),
		},
		{
			description: "one big chunk",
			ls:          &logStream{logStreamID: "logstream3", logicalOffset: 0},
			data:        logStreamData,
			opts:        []ByteStreamWriteOption{ByteStreamOptOffset(0), ByteSteramOptFinishWrite(true)},
			numIterate:  1,
			wantBytes:   int64(len(logStreamData)),
		},
		{
			description: "empty data",
			ls:          &logStream{logStreamID: "logstream4", logicalOffset: 0},
			data:        []byte{},
			opts:        []ByteStreamWriteOption{ByteStreamOptOffset(0), ByteSteramOptFinishWrite(false)},
			numIterate:  1,
			wantBytes:   0,
		},
	}

	b := setup(t)
	defer b.shutDown()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			size := len(test.data)/test.numIterate + 1
			var start int
			start = int(test.opts[0].(ByteStreamOptOffset))
			end := size
			lsID := test.ls.logStreamID
			logStreams[lsID] = test.ls
			ChunkMaxSize(size).Apply(b.client)

			for i := 0; i < test.numIterate; i++ {
				if end > len(test.data) {
					end = len(test.data)
				}
				if i == test.numIterate-1 {
					test.opts[1] = ByteSteramOptFinishWrite(true)
				}

				writtenBytes, err := b.client.WriteBytesWithOptions(b.ctx, lsID, test.data[start:end], test.opts...)
				if err != nil {
					t.Errorf("WriteBytesWithOffset() failed unexpectedly: %v", err)
				}
				test.opts[0] = test.opts[0].(ByteStreamOptOffset) + ByteStreamOptOffset(writtenBytes)
				start = end
				end += size
			}

			if logStreams[lsID].logicalOffset != test.wantBytes {
				t.Errorf("WriteBytesWithOffset() = %d, want %d", logStreams[lsID].logicalOffset, test.wantBytes)
			}
			if logStreams[lsID].finalized == false {
				t.Error("WriteBytesWithOffset() didn't correctly finalize logstream")
			}
		})
	}
}

func TestWriteBytesWithOffsetErrors_LogStream(t *testing.T) {
	tests := []struct {
		description string
		ls          *logStream
		data        []byte
		notFound    bool
		opts        []ByteStreamWriteOption
	}{
		{
			description: "invalid write to finalized logstream",
			ls:          &logStream{logStreamID: "logstream1", logicalOffset: 0, finalized: true},
			data:        logStreamData,
			opts:        []ByteStreamWriteOption{ByteStreamOptOffset(0), ByteSteramOptFinishWrite(false)},
			notFound:    false,
		},
		{
			description: "not found",
			ls:          &logStream{logStreamID: "notFound", logicalOffset: 0},
			data:        logStreamData,
			opts:        []ByteStreamWriteOption{ByteStreamOptOffset(0), ByteSteramOptFinishWrite(false)},
			notFound:    true,
		},
		{
			description: "invlid smaller offset",
			ls:          &logStream{logStreamID: "logstream2", logicalOffset: 4},
			data:        logStreamData,
			opts:        []ByteStreamWriteOption{ByteStreamOptOffset(1), ByteSteramOptFinishWrite(false)},
			notFound:    false,
		},
		{
			description: "invlid larger offset",
			ls:          &logStream{logStreamID: "logstream3", logicalOffset: 2},
			data:        logStreamData,
			opts:        []ByteStreamWriteOption{ByteStreamOptOffset(4), ByteSteramOptFinishWrite(false)},
			notFound:    false,
		},
		{
			description: "invlid negative offset",
			ls:          &logStream{logStreamID: "logstream4", logicalOffset: 0},
			data:        logStreamData,
			opts:        []ByteStreamWriteOption{ByteStreamOptOffset(-1), ByteSteramOptFinishWrite(false)},
			notFound:    false,
		},
	}

	b := setup(t)
	defer b.shutDown()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			lsID := test.ls.logStreamID
			if !test.notFound {
				logStreams[lsID] = test.ls
			}
			data := []byte("Hello World!")
			ChunkMaxSize(len(data)).Apply(b.client)
			writtenBytes, err := b.client.WriteBytesWithOptions(b.ctx, lsID, data, test.opts...)
			if err == nil {
				t.Error("WriteBytesWithOffset() got nil error, want non-nil error")
			}
			if writtenBytes != 0 {
				t.Errorf("WriteBytesWithOffset() got %d byte(s), want 0 byte", writtenBytes)
			}
		})
	}
}

func TestWirteBytesWithOptions_FinishWrite(t *testing.T) {
	b := setup(t)
	defer b.shutDown()
	dataSize := len(logStreamData)
	for i, flag := range []ByteStreamWriteOption{ByteSteramOptFinishWrite(true), ByteSteramOptFinishWrite(false)} {
		t.Run(fmt.Sprintf("ByteSteramOptFinishWrite=%t", flag), func(t *testing.T) {
			lsID := fmt.Sprintf("logstream%d", i+1)
			logStreams[lsID] = &logStream{logStreamID: lsID, logicalOffset: 0}
			ChunkMaxSize(dataSize).Apply(b.client)

			_, err := b.client.WriteBytesWithOptions(b.ctx, lsID, logStreamData, flag)
			if err != nil {
				t.Errorf("WriteBytesWithOffset() failed unexpectedly: %v", err)
			}

			if logStreams[lsID].logicalOffset != int64(dataSize) {
				t.Errorf("WriteBytesWithOffset() = %d, want %d", logStreams[lsID].logicalOffset, int64(dataSize))
			}
			if logStreams[lsID].finalized != bool(flag.(ByteSteramOptFinishWrite)) {
				t.Errorf("WriteBytesWithOffset() got %t, want %t state", logStreams[lsID].finalized, bool(flag.(ByteSteramOptFinishWrite)))
			}
		})

	}
}

func TestWirteBytesWithOptions_Offset(t *testing.T) {
	b := setup(t)
	defer b.shutDown()
	dataSize := len(logStreamData)
	for _, offset := range []ByteStreamWriteOption{ByteStreamOptOffset(0), ByteStreamOptOffset(6)} {
		t.Run(fmt.Sprintf("ByteSteramOptFinishWrite=%t", offset), func(t *testing.T) {
			lsID := fmt.Sprintf("logstream%d", offset)
			logStreams[lsID] = &logStream{logStreamID: lsID, logicalOffset: int64(offset.(ByteStreamOptOffset))}
			ChunkMaxSize(dataSize).Apply(b.client)

			_, err := b.client.WriteBytesWithOptions(b.ctx, lsID, logStreamData[int(offset.(ByteStreamOptOffset)):], offset)
			if err != nil {
				t.Errorf("WriteBytesWithOffset() failed unexpectedly: %v", err)
			}
			if logStreams[lsID].logicalOffset != int64(dataSize) {
				t.Errorf("WriteBytesWithOffset() = %d, want %d", logStreams[lsID].logicalOffset, int64(dataSize))
			}
			if logStreams[lsID].finalized != true {
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

	ls, ok := logStreams[req.GetResourceName()]
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
