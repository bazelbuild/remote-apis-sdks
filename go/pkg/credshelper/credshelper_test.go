package credshelper

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"golang.org/x/oauth2"
	grpcOauth "google.golang.org/grpc/credentials/oauth"
)

func TestCredentialsHelperCache(t *testing.T) {
	dir, err := os.MkdirTemp("", "test")
	if err != nil {
		t.Errorf("failed to create the temp directory: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	cf := filepath.Join(dir, "reproxy.creds")
	err = os.MkdirAll(filepath.Dir(cf), 0755)
	if err != nil {
		t.Errorf("failed to create dir for credentials file %q: %v", cf, err)
	}
	credsHelperCmd := newReusableCmd("echo", []string{`{"token":"testToken", "expiry":"", "refresh_expiry":""}`})
	ts := &grpcOauth.TokenSource{
		TokenSource: oauth2.ReuseTokenSourceWithExpiry(
			&oauth2.Token{},
			&externalTokenSource{credsHelperCmd: credsHelperCmd},
			5*time.Minute,
		),
	}
	creds := Credentials{
		refreshExp:     time.Time{},
		tokenSource:    ts,
		credsFile:      cf,
		credsHelperCmd: credsHelperCmd,
	}
	creds.SaveToDisk()
	_, err = loadCredsFromDisk(cf, credsHelperCmd)
	if err != nil {
		t.Errorf("LoadCredsFromDisk failed: %v", err)
	}
}

func TestExternalToken(t *testing.T) {
	expiry := time.Now().Truncate(time.Second)
	exp := expiry.Format(time.UnixDate)
	tk := "testToken"
	var (
		credshelper     string
		credshelperArgs []string
	)
	if runtime.GOOS == "windows" {
		tf, err := os.CreateTemp("", "testexternaltoken.json")
		if err != nil {
			t.Fatalf("Unable to create temporary file: %v", err)
		}
		chJSON := fmt.Sprintf(`{"token":"%v","expiry":"%s","refresh_expiry":""}`, tk, exp)
		if _, err := tf.Write([]byte(chJSON)); err != nil {
			t.Fatalf("Unable to write to file %v: %v", tf.Name(), err)
		}
		credshelper = "cmd"
		credshelperArgs = []string{
			"/c",
			"cat",
			tf.Name(),
		}
	} else {
		credshelper = "echo"
		credshelperArgs = []string{fmt.Sprintf(`{"token":"%v","expiry":"%s","refresh_expiry":""}`, tk, exp)}
	}

	credsHelperCmd := newReusableCmd(credshelper, credshelperArgs)
	ts := &externalTokenSource{
		credsHelperCmd: credsHelperCmd,
	}
	oauth2tk, err := ts.Token()
	if err != nil {
		t.Errorf("externalTokenSource.Token() returned an error: %v", err)
	}
	if oauth2tk.AccessToken != tk {
		t.Errorf("externalTokenSource.Token() returned token=%s, want=%s", oauth2tk.AccessToken, tk)
	}
	if !oauth2tk.Expiry.Equal(expiry) {
		t.Errorf("externalTokenSource.Token() returned expiry=%s, want=%s", oauth2tk.Expiry, exp)
	}
}

func TestExternalTokenRefresh(t *testing.T) {
	tmp := t.TempDir()
	tokenFile := filepath.Join(tmp, "reproxy.creds")
	var (
		credshelper     string
		credshelperArgs []string
	)
	if runtime.GOOS == "windows" {
		credshelper = "cmd"
		credshelperArgs = []string{
			"/c",
			"cat",
			tokenFile,
		}
	} else {
		credshelper = "cat"
		credshelperArgs = []string{
			tokenFile,
		}
	}
	credsHelperCmd := newReusableCmd(credshelper, credshelperArgs)
	ts := &externalTokenSource{
		credsHelperCmd: credsHelperCmd,
	}
	for _, token := range []string{"testToken", "testTokenRefresh"} {
		expiry := time.Now().Truncate(time.Second)
		writeTokenFile(t, tokenFile, token, expiry)

		oauth2tk, err := ts.Token()
		if err != nil {
			t.Errorf("externalTokenSource.Token() returned an error: %v", err)
		}
		if oauth2tk.AccessToken != token {
			t.Errorf("externalTokenSource.Token() returned token=%s, want=%s", oauth2tk.AccessToken, token)
		}
		if !oauth2tk.Expiry.Equal(expiry) {
			t.Errorf("externalTokenSource.Token() returned expiry=%s, want=%s", oauth2tk.Expiry, expiry)
		}
	}
}

func writeTokenFile(t *testing.T, path, token string, expiry time.Time) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("Unable to open file %v: %v", path, err)
	}
	defer f.Close()
	chJSON := fmt.Sprintf(`{"token":"%v","expiry":"%s","refresh_expiry":""}`, token, expiry.Format(time.UnixDate))
	if _, err := f.Write([]byte(chJSON)); err != nil {
		t.Fatalf("Unable to write to file %v: %v", f.Name(), err)
	}
}

func TestNewExternalCredentials(t *testing.T) {
	testToken := "token"
	exp := time.Now().Add(time.Hour).Truncate(time.Second)
	expStr := exp.String()
	unixExp := exp.Format(time.UnixDate)
	tests := []struct {
		name           string
		wantErr        bool
		checkExp       bool
		credshelperOut string
	}{{
		name:           "No Token",
		wantErr:        true,
		credshelperOut: `{"token":"","expiry":"","refresh_expiry":""}`,
	}, {
		name:           "Credshelper Command Passed - No Expiry",
		credshelperOut: fmt.Sprintf(`{"token":"%v","expiry":"","refresh_expiry":""}`, testToken),
	}, {
		name:           "Credshelper Command Passed - Expiry",
		checkExp:       true,
		credshelperOut: fmt.Sprintf(`{"token":"%v","expiry":"%v","refresh_expiry":""}`, testToken, unixExp),
	}, {
		name:           "Credshelper Command Passed - Refresh Expiry",
		checkExp:       true,
		credshelperOut: fmt.Sprintf(`{"token":"%v","expiry":"%v","refresh_expiry":"%v"}`, testToken, unixExp, unixExp),
	}, {
		name:           "Wrong Expiry Format",
		wantErr:        true,
		credshelperOut: fmt.Sprintf(`{"token":"%v","expiry":"%v","refresh_expiry":"%v"}`, testToken, expStr, expStr),
	}}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var (
				credshelper     string
				credshelperArgs []string
			)
			if runtime.GOOS == "windows" {
				tf, err := os.CreateTemp("", "testnewexternalcreds.json")
				if err != nil {
					t.Fatalf("Unable to create temporary file: %v", err)
				}
				if _, err := tf.Write([]byte(test.credshelperOut)); err != nil {
					t.Fatalf("Unable to write to file %v: %v", tf.Name(), err)
				}
				credshelper = "cmd"
				credshelperArgs = []string{
					"/c",
					"cat",
					tf.Name(),
				}
			} else {
				credshelper = "echo"
				credshelperArgs = []string{test.credshelperOut}
			}

			creds, err := NewExternalCredentials(credshelper, credshelperArgs, "")
			if test.wantErr && err == nil {
				t.Fatalf("NewExternalCredentials did not return an error.")
			}
			if !test.wantErr {
				if err != nil {
					t.Fatalf("NewExternalCredentials returned an error: %v", err)
				}
				if creds.tokenSource == nil {
					t.Fatalf("NewExternalCredentials returned credentials with a nil tokensource.")
				}
				tk, err := creds.tokenSource.Token()
				if err != nil {
					t.Fatalf("tokensource.Token() call failed: %v", err)
				}
				if tk.AccessToken != testToken {
					t.Fatalf("tokensource.Token() gave token=%s, want=%s",
						tk.AccessToken, testToken)
				}
				if test.checkExp && !exp.Equal(tk.Expiry) {
					t.Fatalf("tokensource.Token() gave expiry=%v, want=%v",
						tk.Expiry, exp)
				}
			}
		})
	}
}

func TestReusableCmd(t *testing.T) {
	binary := "echo"
	args := []string{"hello"}
	cmd := newReusableCmd(binary, args)

	output, err := cmd.Cmd().CombinedOutput()
	if err != nil {
		t.Errorf("Command failed: %v", err)
	} else if string(output) != "hello\n" {
		t.Errorf("Command returned unexpected output: %s", output)
	}

	output, err = cmd.Cmd().CombinedOutput()
	if err != nil {
		t.Errorf("Command failed second time: %v", err)
	} else if string(output) != "hello\n" {
		t.Errorf("Command returned unexpected output second time: %s", output)
	}
}

func TestReusableCmdDigest(t *testing.T) {
	cmd1 := newReusableCmd("echo", []string{"Hello"})
	cmd2 := newReusableCmd("echo", []string{"Bye"})
	if cmd1.Digest() == cmd2.Digest() {
		t.Errorf("`%s` and `%s` have the same digest", cmd1, cmd2)
	}
}
