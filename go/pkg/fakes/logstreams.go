package fakes

import (
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	bsgrpc "google.golang.org/genproto/googleapis/bytestream"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

// LogStreams is a fake logstream implementation that implements the bytestream Read command.
type LogStreams struct {
	// streams is a map containing the logstreams. Each logstream consists of a list of chunks. When
	// read, the Read() method will send each chunk one a time.
	streams map[string][][]byte
}

// NewLogStreams returns a new empty fake logstream implementation.
func NewLogStreams() *LogStreams {
	return &LogStreams{streams: make(map[string][][]byte)}
}

// Clear removes all logstreams.
func (l *LogStreams) Clear() {
	l.streams = make(map[string][][]byte)
}

// Put stores a new logstream.
func (l *LogStreams) Put(name string, chunks ...string) error {
	if _, ok := l.streams[name]; ok {
		return fmt.Errorf("stream with name %q already exists", name)
	}
	var bs [][]byte
	for _, chunk := range chunks {
		bs = append(bs, []byte(chunk))
	}
	l.streams[name] = bs
	return nil
}

// Read implements the Bytestream Read command. The chunks of the requested logstream are sent one
// at a time.
func (l *LogStreams) Read(req *bspb.ReadRequest, stream bsgrpc.ByteStream_ReadServer) error {
	path := strings.Split(req.ResourceName, "/")
	if len(path) != 3 || path[0] != "instance" || path[1] != "logstreams" {
		return status.Error(codes.InvalidArgument, "test fake expected resource name of the form \"instance/logstreams/<name>\"")
	}

	chunks, ok := l.streams[path[2]]
	if !ok {
		return status.Error(codes.NotFound, "logstream not found")
	}

	for _, chunk := range chunks {
		if err := stream.Send(&bspb.ReadResponse{Data: chunk}); err != nil {
			return err
		}
	}
	return nil
}
