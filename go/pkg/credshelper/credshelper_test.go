package credshelper

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"
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
		chJSON := fmt.Sprintf(`{"headers":{"hdr":"val"},"token":"%v","expiry":"%s","refresh_expiry":""}`, tk, exp)
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
		credshelperArgs = []string{fmt.Sprintf(`{"headers":{"hdr":"val"},"token":"%v","expiry":"%s","refresh_expiry":""}`, tk, exp)}
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
	chJSON := fmt.Sprintf(`{"headers":{"hdr":"val"},"token":"%v","expiry":"%s","refresh_expiry":""}`, token, expiry.Format(time.UnixDate))
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
		name:           "No Headers",
		credshelperOut: fmt.Sprintf(`{"token":"%v","expiry":"","refresh_expiry":""}`, testToken),
	}, {
		name:           "No Token",
		wantErr:        true,
		credshelperOut: `{"headers":{"hdr":"val"},"token":"","expiry":"","refresh_expiry":""}`,
	}, {
		name:           "Credshelper Command Passed - No Expiry",
		credshelperOut: fmt.Sprintf(`{"headers":{"hdr":"val"},"token":"%v","expiry":"","refresh_expiry":""}`, testToken),
	}, {
		name:           "Credshelper Command Passed - Expiry",
		checkExp:       true,
		credshelperOut: fmt.Sprintf(`{"headers":{"hdr":"val"},"token":"%v","expiry":"%v","refresh_expiry":""}`, testToken, unixExp),
	}, {
		name:           "Credshelper Command Passed - Refresh Expiry",
		checkExp:       true,
		credshelperOut: fmt.Sprintf(`{"headers":{"hdr":"val"},"token":"%v","expiry":"%v","refresh_expiry":"%v"}`, testToken, unixExp, unixExp),
	}, {
		name:           "Wrong Expiry Format",
		wantErr:        true,
		credshelperOut: fmt.Sprintf(`{"headers":{"hdr":"val"},"token":"%v","expiry":"%v", "refresh_expiry":"%v"}`, testToken, expStr, expStr),
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

func TestGetRequestMetadata(t *testing.T) {
	testToken := "token"
	expiredHdrs := map[string]string{"expired": "true"}
	testHdrs := map[string]string{"expired": "false"}
	expiredExp := time.Now().Add(-time.Hour).Truncate(time.Second)
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
			exTs := externalTokenSource{
				credsHelperCmd: credsHelperCmd,
				expiry:         test.tsExp,
				headers:        test.tsHeaders,
				headersLock:    sync.RWMutex{},
			}
			hdrs, err := exTs.GetRequestMetadata(context.Background(), "uri")
			if test.wantErr && err == nil {
				t.Fatalf("GetRequestMetadata did not return an error.")
			}
			if !test.wantErr {
				if err != nil {
					t.Fatalf("GetRequestMetadata returned an error: %v", err)
				}
				if !reflect.DeepEqual(hdrs, exTs.headers) {
					t.Errorf("GetRequestMetadata did not update headers in the tokensource: returned hdrs: %v, tokensource headers: %v", hdrs, exTs.headers)
				}
				if !exp.Equal(exTs.expiry) {
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

func TestRefreshStatus(t *testing.T) {
	c := Credentials{refreshExp: time.Time{}}
	if err := c.RefreshStatus(); err != nil {
		t.Errorf("RefreshStatus returned an error when refreshExpiry is zero")
	}
	c.refreshExp = time.Now().Add(time.Hour)
	if err := c.RefreshStatus(); err != nil {
		t.Errorf("RefreshStatus returned an error when refreshExpiry has not passed")
	}
	c.refreshExp = time.Now().Add(-time.Hour)
	if err := c.RefreshStatus(); err == nil {
		t.Errorf("RefreshStatus did not return an error when refreshExpiry when it has passed")
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
