package client_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/client"
)

// Regression test: DownloadOutputs validates TreeOutput.Path (the location a symlink is
// created at) via getAbsPath, confirmed by the existing TestEscapeDownloadOutputs test.
// It previously did not validate TreeOutput.SymlinkTarget (what the symlink points to),
// which comes directly from server-controlled proto fields (ActionResult.
// OutputFileSymlinks/OutputDirectorySymlinks, or a Directory's Symlinks) with no
// containment check, unlike the upload-side ComputeMerkleTree path which enforces this
// via getTargetRelPath/TreeSymlinkOpts. A malicious or compromised remote cache/RBE
// server could make the client create a symlink, safely placed inside outDir, that
// pointed anywhere on the local filesystem outside outDir.
func TestDownloadOutputs_RejectsSymlinkTargetEscapingOutDir(t *testing.T) {
	t.Parallel()
	env := createEscapedPathTestEnv(t)

	const linkName = "innocuous_output_link"
	const maliciousTarget = "/etc/passwd" // stand-in for any sensitive file outside outDir

	outputs := map[string]*client.TreeOutput{
		linkName: {
			Path:          linkName,
			SymlinkTarget: maliciousTarget,
		},
	}

	if _, err := env.c.DownloadOutputs(env.ctx, outputs, env.outDir, env.cache); err == nil {
		t.Fatalf("DownloadOutputs succeeded with a symlink target escaping outDir, want an error")
	}

	if _, err := os.Lstat(filepath.Join(env.outDir, linkName)); err == nil {
		t.Fatalf("symlink %v should not have been created", filepath.Join(env.outDir, linkName))
	}
}

// A relative symlink target that resolves within outDir must still be allowed.
func TestDownloadOutputs_AllowsSymlinkTargetWithinOutDir(t *testing.T) {
	t.Parallel()
	env := createEscapedPathTestEnv(t)

	const linkName = "link_to_safe_file"
	const relativeTarget = "safe_file.txt"

	outputs := map[string]*client.TreeOutput{
		env.safePath: {
			Digest: env.safeDigest,
			Path:   env.safePath,
		},
		linkName: {
			Path:          linkName,
			SymlinkTarget: relativeTarget,
		},
	}

	if _, err := env.c.DownloadOutputs(env.ctx, outputs, env.outDir, env.cache); err != nil {
		t.Fatalf("DownloadOutputs failed for a symlink target within outDir: %v", err)
	}

	linkPath := filepath.Join(env.outDir, linkName)
	target, err := os.Readlink(linkPath)
	if err != nil {
		t.Fatalf("expected a symlink to have been created at %v: %v", linkPath, err)
	}
	if target != relativeTarget {
		t.Fatalf("symlink target = %q, want %q", target, relativeTarget)
	}
}
