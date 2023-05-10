package symlinkopts_test

import (
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/symlinkopts"
)

func TestSymlinkOpts(t *testing.T) {
	opts := symlinkopts.ResolveAlways()
	if opts.Preserve() {
		t.Errorf("do not want Preserve option")
	}
	if !opts.NoDangling() {
		t.Errorf("missing NoDangling option")
	}
	if !opts.IncludeTarget() {
		t.Errorf("missing IncludeTarget option")
	}
	if !opts.Resolve() {
		t.Errorf("missing Resolve option")
	}
	if !opts.ResolveExternal() {
		t.Errorf("missing ResolveExternal option")
	}

	opts = symlinkopts.ResolveExternalOnly()
	if !opts.Preserve() {
		t.Errorf("missing Preserve option")
	}
	if !opts.NoDangling() {
		t.Errorf("missing NoDangling option")
	}
	if opts.IncludeTarget() {
		t.Errorf("do not want IncludeTarget option")
	}
	if opts.Resolve() {
		t.Errorf("do not want Resolve option")
	}
	if !opts.ResolveExternal() {
		t.Errorf("missing ResolveExternal option")
	}
}
