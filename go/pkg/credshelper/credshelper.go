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
	"google.golang.org/grpc/credentials"
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
	perRPCCreds    *perRPCCredentials
	credsHelperCmd *reusableCmd
}

// perRPCCredentials fullfills the grpc.Credentials.PerRPCCredentials interface
// to provde auth functionalities with headers
type perRPCCredentials struct {
	headers        map[string]string
	expiry         time.Time
	mu             sync.Mutex
	credsHelperCmd *reusableCmd
}

// externaltokenSource uses a credentialsHelper to obtain gcp oauth tokens.
// This should be wrapped in a "golang.org/x/oauth2".ReuseTokenSource
// to avoid obtaining new tokens each time. It implements both the
// oauth2.TokenSource and credentials.PerRPCCredentials interfaces.
type externalTokenSource struct {
	credsHelperCmd *reusableCmd
}

// TokenSource returns a token source for this credentials instance.
func (c *Credentials) TokenSource() *grpcOauth.TokenSource {
	if c == nil {
		return nil
	}
	return c.tokenSource
}

// PerRPCCreds returns a perRPCCredentials for this credentials instance.
func (c *Credentials) PerRPCCreds() credentials.PerRPCCredentials {
	if c == nil {
		return nil
	}
	// If no perRPCCreds exist for this Credentials object, then
	// grpcOauth.TokenSource will do since it implements the same interface
	// and some credentials helpers may only provide a token without headers
	if c.perRPCCreds == nil {
		return c.TokenSource()
	}
	return c.perRPCCreds
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
	if credsOut.tk.AccessToken == "" {
		return nil, fmt.Errorf("no token was printed by the credentials helper")
	}
	log.Infof("'%s' credentials refreshed at %v, expires at %v", ts.credsHelperCmd, time.Now(), credsOut.tk.Expiry)
	return credsOut.tk, err
}

// GetRequestMetadata gets the current request metadata, refreshing tokens if required.
func (p *perRPCCredentials) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.expiry.Before(nowFn().Add(expiryBuffer)) {
		credsOut, err := runCredsHelperCmd(p.credsHelperCmd)
		if err != nil {
			return nil, err
		}
		p.expiry = credsOut.tk.Expiry
		p.headers = credsOut.hdrs
	}
	return p.headers, nil
}

// RequireTransportSecurity indicates whether the credentials require transport security.
func (p *perRPCCredentials) RequireTransportSecurity() bool {
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
	if len(credsOut.hdrs) != 0 {
		c.perRPCCreds = &perRPCCredentials{
			headers:        credsOut.hdrs,
			expiry:         credsOut.tk.Expiry,
			credsHelperCmd: credsHelperCmd,
		}
	}
	if credsOut.tk.AccessToken != "" {
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
	}
	return c, nil
}

type credshelperOutput struct {
	hdrs map[string]string
	tk   *oauth2.Token
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
	if jsonOut.Token == "" && len(jsonOut.Headers) == 0 {
		return nil, fmt.Errorf("both token and headers are empty, invalid credentials")
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
