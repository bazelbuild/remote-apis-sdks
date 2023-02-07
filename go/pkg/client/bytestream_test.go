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

var logStreamData = []byte("Hello World! This is large data to send.")

type logStream struct {
	logStreamID   string
	logicalOffset int64
	finalized     bool
}

func TestWriteBytesWithOffsetSuccess_LogStream(t *testing.T) {
	tests := []struct {
		description  string
		ls           *logStream
		data         []byte
		dataPartsLen int
		opts         []ByteStreamWriteOption
		wantBytesLen int64
	}{
		{
			description:  "valid data with offset 0",
			ls:           &logStream{logicalOffset: 0},
			data:         logStreamData,
			opts:         []ByteStreamWriteOption{ByteStreamOptOffset(0), ByteSteramOptFinishWrite(false)},
			dataPartsLen: 3,
			wantBytesLen: int64(len(logStreamData)),
		},
		{
			description:  "valid data with non-zero offset",
			ls:           &logStream{logicalOffset: 4},
			data:         logStreamData,
			opts:         []ByteStreamWriteOption{ByteStreamOptOffset(4), ByteSteramOptFinishWrite(false)},
			dataPartsLen: 3,
			wantBytesLen: int64(len(logStreamData)),
		},
		{
			description:  "one big chunk",
			ls:           &logStream{logicalOffset: 0},
			data:         logStreamData,
			opts:         []ByteStreamWriteOption{ByteStreamOptOffset(0), ByteSteramOptFinishWrite(true)},
			dataPartsLen: 1,
			wantBytesLen: int64(len(logStreamData)),
		},
		{
			description:  "empty data",
			ls:           &logStream{logicalOffset: 0},
			data:         []byte{},
			opts:         []ByteStreamWriteOption{ByteStreamOptOffset(0), ByteSteramOptFinishWrite(false)},
			dataPartsLen: 1,
			wantBytesLen: 0,
		},
	}

	b := newServer(t)
	defer b.shutDown()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			size := len(test.data)/test.dataPartsLen + 1
			start := int(test.opts[0].(ByteStreamOptOffset))
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
					test.opts[1] = ByteSteramOptFinishWrite(true)
				}

				writtenBytes, err := b.client.WriteBytesWithOptions(b.ctx, lsID, test.data[start:end], test.opts...)
				if err != nil {
					t.Errorf("WriteBytesWithOffset() failed unexpectedly: %v", err)
				}
				if b.fake.logStreams[lsID].logicalOffset != int64(end) {
					t.Errorf("WriteBytesWithOffset() = %d, want %d", b.fake.logStreams[lsID].logicalOffset, end)
				}
				// LogStream shouldn't be finalized when we set ByteSteramOptFinishWrite false.
				if i != test.dataPartsLen-1 && b.fake.logStreams[lsID].finalized == true {
					t.Error("WriteBytesWithOffset() incorrectly finalized LogStream")
				}

				test.opts[0] = test.opts[0].(ByteStreamOptOffset) + ByteStreamOptOffset(writtenBytes)
				start = end
				end += size
			}

			if b.fake.logStreams[lsID].logicalOffset != test.wantBytesLen {
				t.Errorf("WriteBytesWithOffset() = %d, want %d", b.fake.logStreams[lsID].logicalOffset, test.wantBytesLen)
			}
			if b.fake.logStreams[lsID].finalized == false {
				t.Error("WriteBytesWithOffset() didn't correctly finalize LogStream")
			}
		})
	}
}

func TestWriteBytesWithOffsetErrors_LogStream(t *testing.T) {
	tests := []struct {
		description string
		ls          *logStream
		data        []byte
		opts        []ByteStreamWriteOption
	}{
		{
			description: "invalid write to finalized logstream",
			ls:          &logStream{logicalOffset: 0, finalized: true},
			data:        logStreamData,
			opts:        []ByteStreamWriteOption{ByteStreamOptOffset(0), ByteSteramOptFinishWrite(false)},
		},
		{
			description: "not found",
			ls:          nil,
			data:        logStreamData,
			opts:        []ByteStreamWriteOption{ByteStreamOptOffset(0), ByteSteramOptFinishWrite(false)},
		},
		{
			description: "invlid smaller offset",
			ls:          &logStream{logicalOffset: 4},
			data:        logStreamData,
			opts:        []ByteStreamWriteOption{ByteStreamOptOffset(1), ByteSteramOptFinishWrite(false)},
		},
		{
			description: "invlid larger offset",
			ls:          &logStream{logicalOffset: 2},
			data:        logStreamData,
			opts:        []ByteStreamWriteOption{ByteStreamOptOffset(4), ByteSteramOptFinishWrite(false)},
		},
		{
			description: "invlid negative offset",
			ls:          &logStream{logicalOffset: 0},
			data:        logStreamData,
			opts:        []ByteStreamWriteOption{ByteStreamOptOffset(-1), ByteSteramOptFinishWrite(false)},
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

func TestWirteBytesWithOptions_LogStream_FinishWrite(t *testing.T) {
	b := newServer(t)
	defer b.shutDown()
	dataSize := len(logStreamData)
	for i, flag := range []ByteStreamWriteOption{ByteSteramOptFinishWrite(true), ByteSteramOptFinishWrite(false)} {
		t.Run(fmt.Sprintf("ByteSteramOptFinishWrite=%t", flag), func(t *testing.T) {
			lsID := fmt.Sprintf("logstream%d", i+1)
			b.fake.logStreams[lsID] = &logStream{logStreamID: lsID, logicalOffset: 0}
			ChunkMaxSize(dataSize).Apply(b.client)

			_, err := b.client.WriteBytesWithOptions(b.ctx, lsID, logStreamData, flag)
			if err != nil {
				t.Errorf("WriteBytesWithOffset() failed unexpectedly: %v", err)
			}

			if b.fake.logStreams[lsID].logicalOffset != int64(dataSize) {
				t.Errorf("WriteBytesWithOffset() = %d, want %d", b.fake.logStreams[lsID].logicalOffset, int64(dataSize))
			}
			if b.fake.logStreams[lsID].finalized != bool(flag.(ByteSteramOptFinishWrite)) {
				t.Errorf("WriteBytesWithOffset() got %t, want %t state", b.fake.logStreams[lsID].finalized, bool(flag.(ByteSteramOptFinishWrite)))
			}
		})

	}
}

func TestWirteBytesWithOptions_LogStream_Offset(t *testing.T) {
	b := newServer(t)
	defer b.shutDown()
	dataSize := len(logStreamData)
	for _, offset := range []ByteStreamWriteOption{ByteStreamOptOffset(0), ByteStreamOptOffset(6)} {
		t.Run(fmt.Sprintf("ByteSteramOptFinishWrite=%t", offset), func(t *testing.T) {
			lsID := fmt.Sprintf("logstream%d", offset)
			b.fake.logStreams[lsID] = &logStream{logStreamID: lsID, logicalOffset: int64(offset.(ByteStreamOptOffset))}
			ChunkMaxSize(dataSize).Apply(b.client)

			_, err := b.client.WriteBytesWithOptions(b.ctx, lsID, logStreamData[int(offset.(ByteStreamOptOffset)):], offset)
			if err != nil {
				t.Errorf("WriteBytesWithOffset() failed unexpectedly: %v", err)
			}
			if b.fake.logStreams[lsID].logicalOffset != int64(dataSize) {
				t.Errorf("WriteBytesWithOffset() = %d, want %d", b.fake.logStreams[lsID].logicalOffset, int64(dataSize))
			}
			if !b.fake.logStreams[lsID].finalized {
				t.Error("WriteBytesWithOffset() didn't correctly finalize LogStream")
			}
		})
	}
}

type Bytestream struct {
	logStreams map[string]*logStream
}

type Server struct {
	client   *Client
	listener net.Listener
	server   *grpc.Server
	fake     *Bytestream
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
	s.fake.logStreams = make(map[string]*logStream)
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
