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
	// ExperimentalCredsCachePathFlag is the flag used to pass in the path of the file where credentials should be cached.
	ExperimentalCredsCachePathFlag = "experimental_creds_cache_file"

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
	credsFile      string
	refreshExp     time.Time
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

func buildExternalCredentials(baseCreds cachedCredentials, credsFile string, credsHelperCmd *reusableCmd) *Credentials {
	c := &Credentials{
		credsHelperCmd: credsHelperCmd,
		credsFile:      credsFile,
		refreshExp:     baseCreds.refreshExp,
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
			baseCreds.token,
			baseTS,
			// Refresh tokens a bit early to be safe
			expiryBuffer,
		),
	}
	return c
}

func loadCredsFromDisk(credsFile string, credsHelperCmd *reusableCmd) (*Credentials, error) {
	cc, err := loadFromDisk(credsFile)
	if err != nil {
		return nil, err
	}
	cmdDigest := credsHelperCmd.Digest()
	if cc.credsHelperCmdDigest != cmdDigest.String() {
		return nil, fmt.Errorf("cached credshelper command digest: %s is not the same as requested credshelper command digest: %s",
			cc.credsHelperCmdDigest, cmdDigest.String())
	}
	isExpired := cc.token != nil && cc.token.Expiry.Before(nowFn())
	if isExpired {
		return nil, fmt.Errorf("cached token is expired at %v", cc.token.Expiry)
	}
	return buildExternalCredentials(cc, credsFile, credsHelperCmd), nil
}

// SaveToDisk saves credentials to disk.
func (c *Credentials) SaveToDisk() {
	if c == nil {
		return
	}
	cc := cachedCredentials{authSource: CredentialsHelper, refreshExp: c.refreshExp}
	// Since c.tokenSource is always wrapped in a oauth2.ReuseTokenSourceWithExpiry
	// this will return a cached credential if one exists.
	t, err := c.tokenSource.Token()
	if err != nil {
		log.Errorf("Failed to get token to persist to disk: %v", err)
		return
	}
	cc.token = t
	if c.credsHelperCmd != nil {
		cc.credsHelperCmdDigest = c.credsHelperCmd.Digest().String()
	}
	if err := saveToDisk(cc, c.credsFile); err != nil {
		log.Errorf("Failed to save credentials to disk: %v", err)
	}
}

// RemoveFromDisk deletes the credentials cache on disk.
func (c *Credentials) RemoveFromDisk() {
	if c == nil {
		return
	}
	if err := os.Remove(c.credsFile); err != nil {
		log.Errorf("Failed to remove credentials from disk: %v", err)
	}
}

// RefreshStatus checks refresh expiry of credentials in case a manual refresh is required.
func (c *Credentials) RefreshStatus() error {
	if !c.refreshExp.IsZero() && c.refreshExp.Before(nowFn()) {
		return fmt.Errorf("credentials cannot be refreshed automatically, manual re-authentication required")
	}
	return nil
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
func NewExternalCredentials(credshelper string, credshelperArgs []string, credsFile string) (*Credentials, error) {
	if credshelper == "execrel://" {
		credshelperPath, err := binaryRelToAbs("credshelper")
		if err != nil {
			log.Fatalf("Specified %s=execrel:// but `credshelper` was not found in the same directory as `bootstrap` or `reproxy`: %v", CredshelperPathFlag, err)
		}
		credshelper = credshelperPath
	}
	credsHelperCmd := newReusableCmd(credshelper, credshelperArgs)
	if credsFile != "" {
		creds, err := loadCredsFromDisk(credsFile, credsHelperCmd)
		if err == nil {
			return creds, nil
		}
		log.Warningf("Failed to use cached credentials: %v", err)
	}
	credsOut, err := runCredsHelperCmd(credsHelperCmd)
	if err != nil {
		return nil, err
	}
	return buildExternalCredentials(cachedCredentials{token: credsOut.tk, refreshExp: credsOut.rexp}, credsFile, credsHelperCmd), nil
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
	Token         string            `json:"token"`
	Headers       map[string]string `json:"headers"`
	Expiry        string            `json:"expiry"`
	RefreshExpiry string            `json:"refresh_expiry"`
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
	if len(jsonOut.Headers) == 0 {
		return nil, fmt.Errorf("no headers were printed by the credentials helper")
	}
	credsOut.hdrs = jsonOut.Headers
	if jsonOut.Expiry != "" {
		expiry, err := time.Parse(time.UnixDate, jsonOut.Expiry)
		if err != nil {
			return nil, fmt.Errorf("invalid expiry format: %v (Expected time.UnixDate format)", jsonOut.Expiry)
		}
		credsOut.tk.Expiry = expiry
	}
	if jsonOut.RefreshExpiry != "" {
		rexpiry, err := time.Parse(time.UnixDate, jsonOut.RefreshExpiry)
		if err != nil {
			return nil, fmt.Errorf("invalid refresh expiry format: %v (Expected time.UnixDate format)", jsonOut.RefreshExpiry)
		}
		credsOut.rexp = rexpiry
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
