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
	gotOut := o.GetStdout()
	if !bytes.Equal(gotOut, []byte("hello")) {
		t.Errorf("expected o.GetStdout() to return hello, got %v", gotOut)
	}
	gotErr := o.GetStderr()
	if !bytes.Equal(gotErr, []byte("world")) {
		t.Errorf("expected o.GetStderr() to return hello, got %v", gotErr)
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
