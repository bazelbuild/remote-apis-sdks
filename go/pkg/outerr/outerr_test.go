package outerr

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"testing"
)

func TestRecordingOutErr(t *testing.T) {
	t.Parallel()
	o := NewRecordingOutErr()
	o.WriteOut([]byte("hello"))
	o.WriteErr([]byte("world"))
	gotOut := o.Stdout()
	if !bytes.Equal(gotOut, []byte("hello")) {
		t.Errorf("expected o.Stdout() to return hello, got %v", gotOut)
	}
	gotErr := o.Stderr()
	if !bytes.Equal(gotErr, []byte("world")) {
		t.Errorf("expected o.Stderr() to return hello, got %v", gotErr)
	}
}

func TestSystemOutErr(t *testing.T) {
	// Capture the actual system stdout/stderr.
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("error calling os.Pipe() = %v", err)
	}

	stdout := os.Stdout
	fmt.Printf("capturing stdout to %v\n", w)
	os.Stdout = w
	defer func() {
		fmt.Printf("resetting stdout to %v", stdout)
		os.Stdout = stdout
	}()

	stderr := os.Stderr
	os.Stderr = w
	defer func() {
		os.Stderr = stderr
	}()

	// We have to replicate the construction in the test instead of using SystemOutErr directly.
	systemOutErr := NewStreamOutErr(os.Stdout, os.Stderr)
	systemOutErr.WriteOut([]byte("hello "))
	systemOutErr.WriteErr([]byte("world"))

	w.Close()

	var buf bytes.Buffer
	io.Copy(&buf, r)

	if buf.String() != "hello world" {
		t.Errorf("expected stdout+stderr to equal \"hello world\", got %v", buf.String())
	}
}

func TestWriters(t *testing.T) {
	t.Parallel()
	oe := NewRecordingOutErr()
	o, e := NewOutWriter(oe), NewErrWriter(oe)
	if n, err := o.Write([]byte("hello")); n != 5 || err != nil {
		t.Errorf("expected o.Write(\"hello\") to return 5, nil; got %d, %v", n, err)
	}
	if n, err := e.Write([]byte("world")); n != 5 || err != nil {
		t.Errorf("expected e.Write(\"world\") to return 5, nil; got %d, %v", n, err)
	}
	if got := oe.Stdout(); !bytes.Equal(got, []byte("hello")) {
		t.Errorf("expected oe.Stdout() to return hello, got %v", got)
	}
	if got := oe.Stderr(); !bytes.Equal(got, []byte("world")) {
		t.Errorf("expected oe.Stderr() to return world, got %v", got)
	}
}
