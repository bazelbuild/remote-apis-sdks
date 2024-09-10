// Package credshelper implements functionality to authenticate using an external credentials helper.
package credshelper

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"

	log "github.com/golang/glog"
	"golang.org/x/oauth2"
	grpcOauth "google.golang.org/grpc/credentials/oauth"
)

const (
	// CredentialsHelper is using an externally provided binary to get credentials.
	CredentialsHelper = "CredentialsHelper"
	// CredshelperPathFlag is the path to the credentials helper binary.
	CredshelperPathFlag = "credentials_helper"
	// CredshelperArgsFlag is the flag used to pass in the arguments to the credentials helper binary.
	CredshelperArgsFlag = "credentials_helper_args"

	expiryBuffer = 5 * time.Minute
)

var nowFn = time.Now

// Error is an error occured during authenticating or initializing credentials.
type Error struct {
	error
	// ExitCode is the exit code for the error.
	ExitCode int
}

type reusableCmd struct {
	path       string
	args       []string
	digestOnce sync.Once
	digest     digest.Digest
}

func newReusableCmd(binary string, args []string) *reusableCmd {
	cmd := exec.Command(binary, args...)
	return &reusableCmd{
		path: cmd.Path,
		args: args,
	}
}

func (r *reusableCmd) String() string {
	return fmt.Sprintf("%s %v", r.path, strings.Join(r.args, " "))
}

func (r *reusableCmd) Cmd() *exec.Cmd {
	return exec.Command(r.path, r.args...)
}

func (r *reusableCmd) Digest() digest.Digest {
	r.digestOnce.Do(func() {
		chCmd := append(r.args, r.path)
		sort.Strings(chCmd)
		cmdStr := strings.Join(chCmd, ",")
		r.digest = digest.NewFromBlob([]byte(cmdStr))
	})
	return r.digest
}

// Credentials provides auth functionalities using an external credentials helper
type Credentials struct {
	tokenSource    *grpcOauth.TokenSource
	credsHelperCmd *reusableCmd
}

// externaltokenSource uses a credentialsHelper to obtain gcp oauth tokens.
// This should be wrapped in a "golang.org/x/oauth2".ReuseTokenSource
// to avoid obtaining new tokens each time. It implements both the
// oauth2.TokenSource and credentials.PerRPCCredentials interfaces.
type externalTokenSource struct {
	credsHelperCmd *reusableCmd
	headers        map[string]string
	expiry         time.Time
	headersLock    sync.RWMutex
}

// TokenSource returns a token source for this credentials instance.
func (c *Credentials) TokenSource() *grpcOauth.TokenSource {
	if c == nil {
		return nil
	}
	return c.tokenSource
}

// Token retrieves an oauth2 token from the external tokensource.
func (ts *externalTokenSource) Token() (*oauth2.Token, error) {
	if ts == nil {
		return nil, fmt.Errorf("empty tokensource")
	}
	credsOut, err := runCredsHelperCmd(ts.credsHelperCmd)
	if err != nil {
		return nil, err
	}
	log.Infof("'%s' credentials refreshed at %v, expires at %v", ts.credsHelperCmd, time.Now(), credsOut.tk.Expiry)
	return credsOut.tk, err
}

// GetRequestMetadata gets the current request metadata, refreshing tokens if required.
func (ts *externalTokenSource) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	ts.headersLock.RLock()
	defer ts.headersLock.RUnlock()
	if ts.expiry.Before(nowFn().Add(-expiryBuffer)) {
		credsOut, err := runCredsHelperCmd(ts.credsHelperCmd)
		if err != nil {
			return nil, err
		}
		ts.expiry = credsOut.tk.Expiry
		ts.headers = credsOut.hdrs
	}
	return ts.headers, nil
}

// RequireTransportSecurity indicates whether the credentials require transport security.
func (ts *externalTokenSource) RequireTransportSecurity() bool {
	return true
}

// NewExternalCredentials creates credentials obtained from a credshelper.
func NewExternalCredentials(credshelper string, credshelperArgs []string) (*Credentials, error) {
	if credshelper == "execrel://" {
		credshelperPath, err := binaryRelToAbs("credshelper")
		if err != nil {
			log.Fatalf("Specified %s=execrel:// but `credshelper` was not found in the same directory as `bootstrap` or `reproxy`: %v", CredshelperPathFlag, err)
		}
		credshelper = credshelperPath
	}
	credsHelperCmd := newReusableCmd(credshelper, credshelperArgs)
	credsOut, err := runCredsHelperCmd(credsHelperCmd)
	if err != nil {
		return nil, err
	}
	c := &Credentials{
		credsHelperCmd: credsHelperCmd,
	}
	baseTS := &externalTokenSource{
		credsHelperCmd: credsHelperCmd,
	}
	c.tokenSource = &grpcOauth.TokenSource{
		// Wrap the base token source with a ReuseTokenSource so that we only
		// generate new credentials when the current one is about to expire.
		// This is needed because retrieving the token is expensive and some
		// token providers have per hour rate limits.
		TokenSource: oauth2.ReuseTokenSourceWithExpiry(
			credsOut.tk,
			baseTS,
			// Refresh tokens a bit early to be safe
			expiryBuffer,
		),
	}
	return c, nil
}

type credshelperOutput struct {
	hdrs map[string]string
	tk   *oauth2.Token
	rexp time.Time
}

func runCredsHelperCmd(credsHelperCmd *reusableCmd) (*credshelperOutput, error) {
	log.V(2).Infof("Running %v", credsHelperCmd)
	var stdout, stderr bytes.Buffer
	cmd := credsHelperCmd.Cmd()
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	out := stdout.String()
	if stderr.String() != "" {
		log.Errorf("Credentials helper warnings and errors: %v", stderr.String())
	}
	if err != nil {
		return nil, err
	}
	return parseTokenExpiryFromOutput(out)
}

// JSONOut is the struct to record the json output from the credshelper.
type JSONOut struct {
	Token   string            `json:"token"`
	Headers map[string]string `json:"headers"`
	Expiry  string            `json:"expiry"`
}

func parseTokenExpiryFromOutput(out string) (*credshelperOutput, error) {
	credsOut := &credshelperOutput{}
	var jsonOut JSONOut
	if err := json.Unmarshal([]byte(out), &jsonOut); err != nil {
		return nil, fmt.Errorf("error while decoding credshelper output:%v", err)
	}
	if jsonOut.Token == "" {
		return nil, fmt.Errorf("no token was printed by the credentials helper")
	}
	credsOut.tk = &oauth2.Token{AccessToken: jsonOut.Token}
	credsOut.hdrs = jsonOut.Headers
	if jsonOut.Expiry != "" {
		expiry, err := time.Parse(time.UnixDate, jsonOut.Expiry)
		if err != nil {
			return nil, fmt.Errorf("invalid expiry format: %v (Expected time.UnixDate format)", jsonOut.Expiry)
		}
		credsOut.tk.Expiry = expiry
	}
	return credsOut, nil
}

// binaryRelToAbs converts a path that is relative to the current executable
// to absolyte path. If the executable is a symlink then the symlink is
// resolved before generating the path.
func binaryRelToAbs(relPath string) (string, error) {
	executable, err := os.Executable()
	if err != nil {
		return "", err
	}
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		return "", err
	}
	binary := filepath.Join(filepath.Dir(executable), relPath)
	return binary, nil
}
