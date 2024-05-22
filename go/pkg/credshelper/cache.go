package credshelper

import (
	"fmt"
	"os"
	"time"

	cpb "github.com/bazelbuild/remote-apis-sdks/go/api/credshelper"

	log "github.com/golang/glog"
	"github.com/hectane/go-acl"
	"golang.org/x/oauth2"
	"google.golang.org/protobuf/encoding/prototext"
	tspb "google.golang.org/protobuf/types/known/timestamppb"
)

// CachedCredentials are the credentials cached to disk.
type cachedCredentials struct {
	authSource           string
	refreshExp           time.Time
	token                *oauth2.Token
	credsHelperCmdDigest string
}

func loadFromDisk(tf string) (cachedCredentials, error) {
	if tf == "" {
		return cachedCredentials{}, fmt.Errorf("creds_file path not provided")
	}
	blob, err := os.ReadFile(tf)
	if err != nil {
		return cachedCredentials{}, fmt.Errorf("could not read creds_file (%s): %v", tf, err)
	}
	cPb := &cpb.Credentials{}
	if err := prototext.Unmarshal(blob, cPb); err != nil {
		return cachedCredentials{}, fmt.Errorf("could not unmarshal file to proto: %v", err)
	}
	accessToken := cPb.GetToken()
	exp := TimeFromProto(cPb.GetExpiry())
	var token *oauth2.Token
	if accessToken != "" && !exp.IsZero() {
		token = &oauth2.Token{
			AccessToken: accessToken,
			Expiry:      exp,
		}
	}
	c := cachedCredentials{
		authSource:           cPb.GetAuthSource(),
		token:                token,
		refreshExp:           TimeFromProto(cPb.GetRefreshExpiry()),
		credsHelperCmdDigest: cPb.GetCredshelperCmdDigest(),
	}
	log.Infof("Loaded cached credentials of type %v, expires at %v", c.authSource, exp)
	return c, nil
}

func saveToDisk(c cachedCredentials, tf string) error {
	if tf == "" {
		return nil
	}
	cPb := &cpb.Credentials{}
	cPb.AuthSource = c.authSource
	if c.token != nil {
		cPb.Token = c.token.AccessToken
		cPb.Expiry = TimeToProto(c.token.Expiry)
		cPb.CredshelperCmdDigest = c.credsHelperCmdDigest
	}
	if !c.refreshExp.IsZero() {
		cPb.RefreshExpiry = TimeToProto(c.refreshExp)
	}
	f, err := os.Create(tf)
	if err != nil {
		return fmt.Errorf("creds_file: %s could not be created: %v", tf, err)
	}
	// Only owner can read/write the credential cache.
	// This is consistent with gcloud's credentials.db.
	// os.OpenFile(..., 0600) is not used because it does not properly set ACLs on windows.
	if err := acl.Chmod(tf, 0600); err != nil {
		return fmt.Errorf("could not set ACLs on creds_file: %v", err)
	}
	defer f.Close()
	_, err = f.WriteString(prototext.Format(cPb))
	if err != nil {
		return fmt.Errorf("could not write creds to file: %v", err)
	}
	log.Infof("Saved cached credentials of type %v, expires at %v to %v", c.authSource, cPb.Expiry, tf)
	return nil
}

// TimeToProto converts a valid time.Time into a proto Timestamp.
func TimeToProto(t time.Time) *tspb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return tspb.New(t)
}

// TimeFromProto converts a valid Timestamp proto into a time.Time.
func TimeFromProto(tPb *tspb.Timestamp) time.Time {
	if tPb == nil {
		return time.Time{}
	}
	return tPb.AsTime()
}
