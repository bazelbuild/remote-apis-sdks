// Package credshelper implements functionality to authenticate using an external credentials helper.
package credshelper

import (
	"bytes"
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
	// CredsFileFlag is the flag used to pass in the path of the file where credentials should be cached.
	CredsFileFlag = "creds_file"
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
// to avoid obtaining new tokens each time.
type externalTokenSource struct {
	credsHelperCmd *reusableCmd
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
			// Refresh tokens 5 mins early to be safe
			5*time.Minute,
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

// Token retrieves an oauth2 token from the external tokensource.
func (ts *externalTokenSource) Token() (*oauth2.Token, error) {
	if ts == nil {
		return nil, fmt.Errorf("empty tokensource")
	}
	tk, _, err := runCredsHelperCmd(ts.credsHelperCmd)
	if err == nil {
		log.Infof("'%s' credentials refreshed at %v, expires at %v", ts.credsHelperCmd, time.Now(), tk.Expiry)
	}
	return tk, err
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
	tk, rexp, err := runCredsHelperCmd(credsHelperCmd)
	if err != nil {
		return nil, err
	}
	return buildExternalCredentials(cachedCredentials{token: tk, refreshExp: rexp}, credsFile, credsHelperCmd), nil
}

func runCredsHelperCmd(credsHelperCmd *reusableCmd) (*oauth2.Token, time.Time, error) {
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
		return nil, time.Time{}, err
	}
	token, expiry, refreshExpiry, err := parseTokenExpiryFromOutput(out)
	return &oauth2.Token{
		AccessToken: token,
		Expiry:      expiry,
	}, refreshExpiry, err
}

// CredsHelperOut is the struct to record the json output from the credshelper.
type CredsHelperOut struct {
	Token         string `json:"token"`
	Expiry        string `json:"expiry"`
	RefreshExpiry string `json:"refresh_expiry"`
}

func parseTokenExpiryFromOutput(out string) (string, time.Time, time.Time, error) {
	var (
		tk        string
		exp, rexp time.Time
		chOut     CredsHelperOut
	)
	if err := json.Unmarshal([]byte(out), &chOut); err != nil {
		return tk, exp, rexp,
			fmt.Errorf("error while decoding credshelper output:%v", err)
	}
	tk = chOut.Token
	if tk == "" {
		return tk, exp, rexp,
			fmt.Errorf("no token was printed by the credentials helper")
	}
	if chOut.Expiry != "" {
		expiry, err := time.Parse(time.UnixDate, chOut.Expiry)
		if err != nil {
			return tk, exp, rexp, fmt.Errorf("invalid expiry format: %v (Expected time.UnixDate format)", chOut.Expiry)
		}
		exp = expiry
		rexp = expiry
	}
	if chOut.RefreshExpiry != "" {
		rexpiry, err := time.Parse(time.UnixDate, chOut.RefreshExpiry)
		if err != nil {
			return tk, exp, rexp, fmt.Errorf("invalid refresh expiry format: %v (Expected time.UnixDate format)", chOut.RefreshExpiry)
		}
		rexp = rexpiry
	}
	return tk, exp, rexp, nil
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
