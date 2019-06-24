// Package outerr contains types to record/pass system out-err streams.
package outerr

import (
	"bytes"
	"io"
	"os"

	log "github.com/golang/glog"
)

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

// A constant wrapping the standard stdout/stderr streams.
var SystemOutErr = NewStreamOutErr(os.Stdout, os.Stderr)

func (s *StreamOutErr) WriteOut(buf []byte) {
	if _, e := s.OutWriter.Write(buf); e != nil {
		log.Errorf("error writing to stdout stream: %v", e)
	}
}

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

func (s *RecordingOutErr) GetStdout() []byte {
	return s.out.Bytes()
}

func (s *RecordingOutErr) GetStderr() []byte {
	return s.err.Bytes()
}
