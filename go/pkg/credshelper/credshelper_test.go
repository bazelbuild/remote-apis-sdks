package credshelper

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
)

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
		chJSON := fmt.Sprintf(`{"headers":{"hdr":"val"},"token":"%v","expiry":"%s"}`, tk, exp)
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
		credshelperArgs = []string{fmt.Sprintf(`{"headers":{"hdr":"val"},"token":"%v","expiry":"%s"}`, tk, exp)}
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
	chJSON := fmt.Sprintf(`{"headers":{"hdr":"val"},"token":"%v","expiry":"%s"}`, token, expiry.Format(time.UnixDate))
	if _, err := f.Write([]byte(chJSON)); err != nil {
		t.Fatalf("Unable to write to file %v: %v", f.Name(), err)
	}
}

func TestNewExternalCredentials(t *testing.T) {
	testToken := "token"
	exp := time.Now().Add(time.Hour).Truncate(time.Second)
	expStr := exp.String()
	unixExp := exp.Format(time.UnixDate)
	rfc3339Exp := exp.Format(time.RFC3339)
	tests := []struct {
		name           string
		wantErr        bool
		checkExp       bool
		credshelperOut string
	}{{
		name:           "No Headers",
		credshelperOut: fmt.Sprintf(`{"token":"%v","expiry":""}`, testToken),
	}, {
		name:           "No Token",
		credshelperOut: `{"headers":{"hdr":"val"},"token":"","expiry":""}`,
	}, {
		name:           "Credshelper Command Passed - No Expiry",
		credshelperOut: fmt.Sprintf(`{"headers":{"hdr":"val"},"token":"%v","expiry":""}`, testToken),
	}, {
		name:           "Credshelper Command Passed - Expiry",
		checkExp:       true,
		credshelperOut: fmt.Sprintf(`{"headers":{"hdr":"val"},"token":"%v","expiry":"%v"}`, testToken, unixExp),
	}, {
		name:           "Wrong Expiry Format",
		wantErr:        true,
		credshelperOut: fmt.Sprintf(`{"headers":{"hdr":"val"},"token":"%v","expiry":"%v"}`, testToken, expStr),
	}, {
		name:           "bazelCompatAndReclientLegacy",
		credshelperOut: fmt.Sprintf(`{"headers":{"hdr":["val"]},"token":%q,"expiry":%q, "expires":%q}`, testToken, unixExp, rfc3339Exp),
	}, {
		name:           "bazelCompat",
		credshelperOut: fmt.Sprintf(`{"headers":{"Authorization":["Bearer %s"]},"expires":%q}`, testToken, rfc3339Exp),
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

			creds, err := NewExternalCredentials(credshelper, credshelperArgs)
			if test.wantErr && err == nil {
				t.Fatalf("NewExternalCredentials did not return an error.")
			}
			if !test.wantErr && test.name != "No Token" {
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

func TestGetRequestMetadata(t *testing.T) {
	testToken := "token"
	expiredHdrs := map[string]string{"expired": "true"}
	testHdrs := map[string]string{"expired": "false"}
	expiredExp := time.Now().Add(-time.Hour).Truncate(time.Second)
	aboutToExpire := time.Now().Add(time.Minute).Truncate(time.Second)
	exp := time.Now().Add(time.Hour).Truncate(time.Second)
	unixExp := exp.Format(time.UnixDate)
	tests := []struct {
		name           string
		tsExp          time.Time
		tsHeaders      map[string]string
		wantErr        bool
		wantExpired    bool
		credshelperOut string
	}{{
		name:      "Creds Not Expired",
		tsExp:     exp,
		tsHeaders: testHdrs,
	}, {
		name:           "Creds Expired: Credshelper Successful",
		tsExp:          expiredExp,
		tsHeaders:      expiredHdrs,
		wantExpired:    true,
		credshelperOut: fmt.Sprintf(`{"headers":{"expired":"false"},"token":"%v","expiry":"%v"}`, testToken, unixExp),
	}, {
		name:           "Creds About to Expire: Credshelper Successful",
		tsExp:          aboutToExpire,
		tsHeaders:      expiredHdrs,
		wantExpired:    true,
		credshelperOut: fmt.Sprintf(`{"headers":{"expired":"false"},"token":"%v","expiry":"%v"}`, testToken, unixExp),
	}, {
		name:           "Creds Expired: Credshelper Failed",
		wantErr:        true,
		wantExpired:    true,
		credshelperOut: fmt.Sprintf(`{"headers":"","token":"%v","expiry":""`, testToken),
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
			credsHelperCmd := newReusableCmd(credshelper, credshelperArgs)
			p := perRPCCredentials{
				credsHelperCmd: credsHelperCmd,
				expiry:         test.tsExp,
				headers:        test.tsHeaders,
			}
			hdrs, err := p.GetRequestMetadata(context.Background(), "uri")
			if test.wantErr && err == nil {
				t.Fatalf("GetRequestMetadata did not return an error.")
			}
			if !test.wantErr {
				if err != nil {
					t.Fatalf("GetRequestMetadata returned an error: %v", err)
				}
				if !reflect.DeepEqual(hdrs, p.headers) {
					t.Errorf("GetRequestMetadata did not update headers in the tokensource: returned hdrs: %v, tokensource headers: %v", hdrs, p.headers)
				}
				if !exp.Equal(p.expiry) {
					t.Errorf("GetRequestMetadata did not update expiry in the tokensource")
				}
				if !test.wantExpired && !reflect.DeepEqual(hdrs, testHdrs) {
					t.Errorf("GetRequestMetadata returned headers: %v, but want headers: %v", hdrs, testHdrs)
				}
				if test.wantExpired && reflect.DeepEqual(hdrs, expiredHdrs) {
					t.Errorf("GetRequestMetadata returned expired headers")
				}
			}
		})
	}
}

func TestGetRequestMetadata_Concurrent(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip()
	}
	testToken := "token"
	testHdrs := map[string]string{"expired": "false"}
	aboutToExpire := time.Now().Add(time.Minute).Truncate(time.Second)
	exp := time.Now().Add(time.Hour).Truncate(time.Second)
	unixExp := exp.Format(time.UnixDate)
	callsFile := filepath.Join(t.TempDir(), "calls.txt")
	credsHelperScript := fmt.Sprintf(`
	echo 1 > %v
	sleep 5
	echo '{"headers":{"expired":"false"},"token":"%v","expiry":"%v"}'
`, callsFile, testToken, unixExp)
	scriptFile := filepath.Join(t.TempDir(), "script.sh")
	if err := os.WriteFile(scriptFile, []byte(credsHelperScript), 0755); err != nil {
		t.Fatalf("Unable to write to file %v: %v", scriptFile, err)
	}
	credshelper := "bash"
	credshelperArgs := []string{"-c", scriptFile}

	credsHelperCmd := newReusableCmd(credshelper, credshelperArgs)
	p := perRPCCredentials{
		credsHelperCmd: credsHelperCmd,
		expiry:         aboutToExpire,
		headers:        testHdrs,
	}
	eg, eCtx := errgroup.WithContext(context.Background())
	for i := 0; i < 10; i++ {
		eg.Go(func() error {
			_, err := p.GetRequestMetadata(eCtx, "uri")
			return err
		})
	}
	if err := eg.Wait(); err != nil {
		t.Errorf("GetRequestMetadata returned an error: %v", err)
	}
	callsBlob, err := os.ReadFile(callsFile)
	if err != nil {
		t.Fatal("Failed to read calls file")
	}
	wantCalls := "1\n"
	if string(callsBlob) != wantCalls {
		t.Errorf("Calls file had unexpected content, got: %q, want: %q", callsBlob, wantCalls)
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
