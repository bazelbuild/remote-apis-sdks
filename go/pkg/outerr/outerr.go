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
		log.Errorf("Error writing to stdout stream: %v", e)
	}
}

// WriteErr writes the given bytes to stderr.
func (s *StreamOutErr) WriteErr(buf []byte) {
	if _, e := s.ErrWriter.Write(buf); e != nil {
		log.Errorf("Error writing to stderr stream: %v", e)
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

// Stdout returns the full recorded stdout contents.
func (s *RecordingOutErr) Stdout() []byte {
	return s.out.Bytes()
}

// Stderr returns the full recorded stderr contents.
func (s *RecordingOutErr) Stderr() []byte {
	return s.err.Bytes()
}

// outWriter is a Writer that writes to the out stream of an OutErr.
type outWriter struct {
	OutErr
}

// NewOutWriter provides an io.Writer implementation for writing to the out
// stream of an OutErr.
func NewOutWriter(oe OutErr) io.Writer {
	return &outWriter{OutErr: oe}
}

// Write writes to the out stream of the OutErr. This method always returns len(p), nil.
func (o *outWriter) Write(p []byte) (int, error) {
	o.WriteOut(p)
	return len(p), nil
}

// errWriter is a Writer that writes to the err stream of an OutErr.
type errWriter struct {
	OutErr
}

// NewErrWriter provides an io.Writer implementation for writing to the err
// stream of an OutErr.
func NewErrWriter(oe OutErr) io.Writer {
	return &errWriter{OutErr: oe}
}

// Write writes to the stderr stream of the OutErr. This method always returns len(p), nil.
func (e *errWriter) Write(p []byte) (int, error) {
	e.WriteErr(p)
	return len(p), nil
}
