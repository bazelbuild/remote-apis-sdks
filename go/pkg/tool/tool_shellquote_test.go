package tool

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

func runBashScript(t *testing.T, path string) {
	t.Helper()
	cmd := exec.Command("/bin/bash", path)
	// Discard output; we care about side effects (sentinel files), not stdout.
	cmd.Stdout = nil
	cmd.Stderr = nil
	_ = cmd.Run()
}

// stripSingleQuotedRegions returns line with every single-quoted region
// (including the standard `'\”` escape sequence for an embedded single quote)
// removed. The resulting string contains only the parts of the line that bash
// would parse as unquoted; if any shell metacharacter appears in it, the line
// is unsafe.
func stripSingleQuotedRegions(line string) string {
	var out strings.Builder
	inQuote := false
	i := 0
	for i < len(line) {
		if !inQuote {
			if line[i] == '\'' {
				inQuote = true
				i++
				continue
			}
			out.WriteByte(line[i])
			i++
			continue
		}
		// Inside a single-quoted region. The only way out is a closing `'`.
		// The 4-character escape sequence `'\''` represents an embedded `'`:
		// it ends the current quote, then has a literal `\'`, then re-opens.
		// Treat it as continuing the quoted region.
		if line[i] == '\'' && i+3 < len(line) && line[i+1] == '\\' && line[i+2] == '\'' && line[i+3] == '\'' {
			i += 4
			continue
		}
		if line[i] == '\'' {
			inQuote = false
			i++
			continue
		}
		i++
	}
	return out.String()
}

// TestShellQuote verifies the POSIX single-quote escaping primitive used by
// writeExecScript covers every case that previously allowed shell injection
// through server-controlled Command fields.
func TestShellQuote(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", "''"},
		{"plain", "hello", "'hello'"},
		{"space", "hello world", "'hello world'"},
		{"single-quote", "it's", `'it'\''s'`},
		{"command-substitution", "$(touch /tmp/pwned)", "'$(touch /tmp/pwned)'"},
		{"backtick", "`id`", "'`id`'"},
		{"semicolon", "x;rm -rf /tmp", "'x;rm -rf /tmp'"},
		{"newline", "a\nb", "'a\nb'"},
		{"escape-attempt", `a' ; touch /tmp/pwned ; '`, `'a'\'' ; touch /tmp/pwned ; '\'''`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := shellQuote(tc.in)
			if got != tc.want {
				t.Fatalf("shellQuote(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestWriteExecScript_NoShellInjection drives writeExecScript with malicious
// values in every server-controlled Command field and asserts that none of the
// shell metacharacters reach the generated script unquoted.
func TestWriteExecScript_NoShellInjection(t *testing.T) {
	tmp := t.TempDir()
	filename := filepath.Join(tmp, "run_locally.sh")

	cmd := &repb.Command{
		Arguments: []string{
			"echo",
			"normal-arg",
			"$(touch /tmp/should-not-fire-arg)",
			"with space",
			"--cfg=$(touch /tmp/should-not-fire-cfg)",
			`with'quote`,
		},
		WorkingDirectory: "$(touch /tmp/should-not-fire-wd)",
		OutputDirectories: []string{
			"out/$(touch /tmp/should-not-fire-od)",
		},
		OutputFiles: []string{
			"out/$(touch /tmp/should-not-fire-of)/file",
		},
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			{Name: "GOOD_NAME", Value: "$(touch /tmp/should-not-fire-env)"},
			// Invalid name must be dropped, not emitted.
			{Name: "BAD;NAME", Value: "x"},
			{Name: "1STARTS_WITH_DIGIT", Value: "x"},
			{Name: "", Value: "x"},
		},
		Platform: &repb.Platform{
			Properties: []*repb.Platform_Property{
				{Name: "container-image", Value: "docker://busybox"},
			},
		},
	}

	c := &Client{}
	if err := c.writeExecScript(context.Background(), cmd, filename); err != nil {
		t.Fatalf("writeExecScript failed: %v", err)
	}

	scriptBytes, err := os.ReadFile(filepath.Join(tmp, "run_command.sh"))
	if err != nil {
		t.Fatalf("read run_command.sh: %v", err)
	}
	script := string(scriptBytes)

	// 1. The hard invariant: when bash parses this script, no command
	//    substitution may execute on any line containing a payload. Verify by
	//    stripping single-quoted regions (including the `'\''` escape sequence
	//    for an embedded single quote) and asserting no shell metacharacters
	//    remain.
	for _, line := range strings.Split(script, "\n") {
		if strings.HasPrefix(line, "#") || line == "" || line == "bash" {
			continue
		}
		stripped := stripSingleQuotedRegions(line)
		for _, meta := range []string{"$(", "`", "$VAR", "${"} {
			if strings.Contains(stripped, meta) {
				t.Errorf("shell metacharacter %q found outside single-quoted region\nline: %q\nstripped: %q", meta, line, stripped)
			}
		}
	}

	// Sanity: each payload must appear *somewhere* in the script (otherwise
	// the test isn't actually exercising the quoting path).
	for _, payload := range []string{
		"should-not-fire-arg",
		"should-not-fire-cfg",
		"should-not-fire-wd",
		"should-not-fire-od",
		"should-not-fire-of",
		"should-not-fire-env",
	} {
		if !strings.Contains(script, payload) {
			t.Errorf("payload sentinel %q missing from script — test not exercising the path", payload)
		}
	}

	// 2. Embedded single quote in an arg must be escaped, not break out.
	//    `with'quote` must appear as `'with'\''quote'` in the script.
	if !strings.Contains(script, `'with'\''quote'`) {
		t.Errorf("embedded single quote in arg not properly escaped:\n%s", script)
	}

	// 3. Invalid env var names must NOT produce export lines.
	for _, badName := range []string{"BAD;NAME", "1STARTS_WITH_DIGIT"} {
		if strings.Contains(script, "export "+badName) {
			t.Errorf("invalid env var name %q was emitted:\n%s", badName, script)
		}
	}
	// And specifically, the metacharacter version must not be emitted as a name.
	if strings.Contains(script, "export BAD;NAME") {
		t.Errorf("env var name with semicolon was emitted unescaped:\n%s", script)
	}

	// 4. Valid env var must appear with quoted value.
	if !strings.Contains(script, "export GOOD_NAME='$(touch /tmp/should-not-fire-env)'") {
		t.Errorf("valid env var value not properly quoted:\n%s", script)
	}
}

// TestWriteExecScript_RunGeneratedScript_NoSideEffects optionally executes the
// generated script under bash and verifies that none of the injection sentinels
// fire. Skipped on systems without bash to keep CI portable.
func TestWriteExecScript_RunGeneratedScript_NoSideEffects(t *testing.T) {
	if _, err := os.Stat("/bin/bash"); err != nil {
		t.Skip("bash not available")
	}
	tmp := t.TempDir()
	scriptDir := filepath.Join(tmp, "scripts")
	if err := os.MkdirAll(scriptDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	filename := filepath.Join(scriptDir, "run_locally.sh")

	// Sentinel files inside tmp; if the shell ever interprets the payloads,
	// these will be created and the test fails.
	sentinel := func(tag string) string { return filepath.Join(tmp, "sentinel-"+tag) }

	cmd := &repb.Command{
		// Use `true` as the action so the script exits cleanly.
		Arguments: []string{
			"true",
			"$(touch " + sentinel("arg") + ")",
			"x;touch " + sentinel("argsemi") + ";true",
		},
		// cd into a real directory so `cd` doesn't fail.
		WorkingDirectory: tmp + "/$(touch " + sentinel("wd") + ")fakedir",
		OutputDirectories: []string{
			"od;touch " + sentinel("od"),
		},
		OutputFiles: []string{
			"of/$(touch " + sentinel("of") + ")/x",
		},
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			{Name: "EVIL", Value: "$(touch " + sentinel("env") + ")"},
		},
		Platform: &repb.Platform{
			Properties: []*repb.Platform_Property{
				{Name: "container-image", Value: "docker://busybox"},
			},
		},
	}

	c := &Client{}
	if err := c.writeExecScript(context.Background(), cmd, filename); err != nil {
		t.Fatalf("writeExecScript: %v", err)
	}

	// Run only the line that doesn't depend on `cd` succeeding: extract the
	// script and feed everything except the `cd` line to bash, so we verify
	// the rest of the script's quoting in isolation. (cd to a non-existent
	// dir would `exit` regardless of injection.)
	scriptBytes, err := os.ReadFile(filepath.Join(scriptDir, "run_command.sh"))
	if err != nil {
		t.Fatalf("read run_command.sh: %v", err)
	}
	var filtered []string
	for _, line := range strings.Split(string(scriptBytes), "\n") {
		if strings.HasPrefix(line, "cd ") || strings.HasPrefix(line, "mkdir ") || line == "bash" {
			continue
		}
		filtered = append(filtered, line)
	}
	stripped := filepath.Join(scriptDir, "stripped.sh")
	if err := os.WriteFile(stripped, []byte(strings.Join(filtered, "\n")), 0o755); err != nil {
		t.Fatalf("write stripped: %v", err)
	}
	// Run it. Ignore exit code (the `true` arg call may still produce stderr
	// from the quoted args, but no sentinel must fire).
	runBashScript(t, stripped)

	for _, tag := range []string{"arg", "argsemi", "wd", "od", "of", "env"} {
		if _, err := os.Stat(sentinel(tag)); err == nil {
			t.Errorf("sentinel %q fired — shell injection still possible", tag)
		}
	}
}
