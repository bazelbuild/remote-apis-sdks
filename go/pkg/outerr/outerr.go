// Package outerr contains types to record/pass system out-err streams.
package outerr

import (
	"bytes"
	"io"
	"os"

	log "github.com/golang/glog"
)

// OutErr is a general consumer of stdout and stderr.
type OutErr interface {
	WriteOut([]byte)
	WriteErr([]byte)
}

// StreamOutErr passes the stdout and stderr to two provided writers.
type StreamOutErr struct {
	OutWriter, ErrWriter io.Writer
}

// NewStreamOutErr creates an OutErr from two Writers.
func NewStreamOutErr(out, err io.Writer) *StreamOutErr {
	return &StreamOutErr{out, err}
}

// SystemOutErr is a constant wrapping the standard stdout/stderr streams.
var SystemOutErr = NewStreamOutErr(os.Stdout, os.Stderr)

// WriteOut writes the given bytes to stdout.
func (s *StreamOutErr) WriteOut(buf []byte) {
	if _, e := s.OutWriter.Write(buf); e != nil {
		log.Errorf("error writing to stdout stream: %v", e)
	}
}

// WriteErr writes the given bytes to stderr.
func (s *StreamOutErr) WriteErr(buf []byte) {
	if _, e := s.ErrWriter.Write(buf); e != nil {
		log.Errorf("error writing to stdout stream: %v", e)
	}
}

// RecordingOutErr is an OutErr capable of recording and returning its contents.
type RecordingOutErr struct {
	StreamOutErr
	out, err bytes.Buffer
}

// NewRecordingOutErr initializes a new RecordingOutErr.
func NewRecordingOutErr() *RecordingOutErr {
	res := &RecordingOutErr{}
	res.StreamOutErr = *NewStreamOutErr(&res.out, &res.err)
	return res
}

// GetStdout returns the full recorded stdout contents.
func (s *RecordingOutErr) GetStdout() []byte {
	return s.out.Bytes()
}

// GetStderr returns the full recorded stderr contents.
func (s *RecordingOutErr) GetStderr() []byte {
	return s.err.Bytes()
}
