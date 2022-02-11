// Package actas provides a TokenSource that returns access tokens that impersonate
// a different service account other than the default app credentials.
package actas

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/oauth2"
	"google.golang.org/grpc/credentials"
)

const (
	// See https://cloud.google.com/iam/reference/rest/v1/projects.serviceAccounts/signJwt
	// for details on signJWT call.
	// See https://cloud.google.com/endpoints/docs/openapi/service-account-authentication
	// for details on authenticating as a service account, including the grantType for
	// authenticating using a signed JWT.
	signJWTURLTemplate = "https://iam.googleapis.com/v1/projects/-/serviceAccounts/%s:signJwt"
	audience           = "https://www.googleapis.com/oauth2/v4/token"
	grantType          = "urn:ietf:params:oauth:grant-type:jwt-bearer"

	// expiryWiggleRoom is the number of seconds to subtract from the expiry time to give a bit of
	// wiggle room for token refreshing.
	expiryWiggleRoom = 120 * time.Second
)

// signJwtURL is the url string for the signJwt call.  The expected payload is the JWT to sign.
var newSignJWTURL = func(account string) string {
	return fmt.Sprintf(signJWTURLTemplate, account)
}

var audienceURL = audience

// TokenSource is an oauth2.TokenSource implementation that provides impersonated credentials.
// The current implementation uses the application default credentials to
// sign for the impersonated credentials.  This means whatever service account
// is running the code needs to be a member in the ServiceAccountTokenCreator
// role for the impersonated service account.
type TokenSource struct {
	// ctx is the context used for obtaining tokens.
	ctx context.Context

	// actAsAccount is the account that tokens are obtained for.
	actAsAccount string

	// cred is the credentials used.
	cred credentials.PerRPCCredentials

	// scopes is the list of scopes for the tokens.
	scopes []string

	// httpClient is the http client used to obtain tokens.
	httpClient *http.Client
}

// NewTokenSource returns a impersonated credentials token source.
func NewTokenSource(ctx context.Context, cred credentials.PerRPCCredentials, client *http.Client, actAsAccount string, scopes []string) *TokenSource {
	return &TokenSource{
		ctx:          ctx,
		actAsAccount: actAsAccount,
		cred:         cred,
		scopes:       scopes,
		httpClient:   client,
	}
}

// Token returns an authorization token for the impersonated service account.
func (s *TokenSource) Token() (*oauth2.Token, error) {
	log.Infof("Generating new act-as token.")

	authHeaders, err := s.cred.GetRequestMetadata(s.ctx)
	if err != nil {
		return nil, err
	}

	log.V(1).Infof("Obtained credentials request metadata: %+v", authHeaders)

	signature, err := s.getSignedJWT(authHeaders)
	if err != nil {
		return nil, err
	}

	// Next do the access token request, using the JWT signed as the impersonated SA
	token, err := s.getToken(authHeaders, signature)
	if err != nil {
		return nil, err
	}

	return token, nil
}

// claims contains the set of JWT claims needed to for the signJWT call.
type claims struct {
	Scope string `json:"scope"`
	Iss   string `json:"iss"`
	Aud   string `json:"aud"`
	Iat   int64  `json:"iat"`
}

// newClaims constructs and encode the jwt claims.
func (s *TokenSource) newClaims() (string, error) {
	c := claims{
		Scope: strings.Join(s.scopes, " "),
		Aud:   audienceURL,
		Iss:   s.actAsAccount,
		Iat:   time.Now().Unix(),
	}
	j, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	claimsEncoded := strings.Replace(string(j), "\"", "\\\"", -1)
	claimsPayload := "{\"payload\": \"" + claimsEncoded + "\"}"
	return claimsPayload, nil
}

// signaturePayload is the structure of the returned payload of a successful signJWT call.
type signaturePayload struct {
	KeyID     string `json:"keyId"`
	SignedJwt string `json:"signedJwt"`
}

// signatureError is the structure of the returned payload of a failed signJWT call.
type signatureError struct {
	Error struct {
		Code    int64  `json:"code"`
		Message string `json:"message"`
		Status  string `json:"status"`
	} `json:"error"`
}

func (s *TokenSource) getSignedJWT(headers map[string]string) (*signaturePayload, error) {
	claims, err := s.newClaims()
	if err != nil {
		return nil, err
	}

	// Construct the signJWT request and send it.
	req, err := http.NewRequest("POST", newSignJWTURL(s.actAsAccount), strings.NewReader(claims))
	if err != nil {
		return nil, err
	}
	// Copy the authHeaders from the default credentials into the request headers.
	for k, v := range headers {
		req.Header.Add(k, v)
	}

	log.V(1).Infof("HTTP request to signJWT: %+v", req)

	// Execute the call to signJWT
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	log.V(1).Infof("HTTP response from signJWT: %+v", resp)

	// Extract the signedJWT from the response body.
	signatureBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if !isOK(resp.StatusCode) {
		var errResp signatureError
		err = json.Unmarshal(signatureBody, &errResp)
		if err != nil {
			return nil, fmt.Errorf("signJWT call failed with http code %v, unable to parse error payload", resp.StatusCode)
		}
		return nil, fmt.Errorf("signJWT call failed with http code %v, error status %v, message %v", resp.StatusCode, errResp.Error.Status, errResp.Error.Message)
	}

	payload := &signaturePayload{}
	if err := json.Unmarshal(signatureBody, payload); err != nil {
		return nil, fmt.Errorf("failed to parse sign jwt payload %q: %v", string(signatureBody), err)
	}

	log.V(1).Infof("Payload: %+v", payload)

	return payload, nil
}

// tokenPayload is the structure of the returned payload of the access token call when it
// returns successfully.
type tokenPayload struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int64  `json:"expires_in"`
}

// tokenError is the structure of the returned payload of the access token call when there is
// an error.
type tokenError struct {
	Error            string `json:"error"`
	ErrorDescription string `json:"error_description"`
}

func (s TokenSource) getToken(headers map[string]string, signature *signaturePayload) (*oauth2.Token, error) {
	accessForm := url.Values{}
	accessForm.Add("grant_type", grantType)
	accessForm.Add("assertion", signature.SignedJwt)
	req, err := http.NewRequest("POST", audienceURL, strings.NewReader(accessForm.Encode()))
	if err != nil {
		return nil, err
	}
	// Copy the authHeaders into the new request.
	for k, v := range headers {
		req.Header.Add(k, v)
	}
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	log.V(1).Infof("HTTP request: %+v", req)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	log.V(1).Infof("HTTP response: %+v", resp)

	tokenBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if !isOK(resp.StatusCode) {
		var errResp tokenError
		err = json.Unmarshal(tokenBody, &errResp)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal access token error response: %v", err)
		}
		return nil, fmt.Errorf("access token call failed with status code %d and error %s, %s", resp.StatusCode, errResp.Error, errResp.ErrorDescription)
	}

	payload := &tokenPayload{}
	err = json.Unmarshal(tokenBody, payload)
	if err != nil {
		return nil, fmt.Errorf("failed to parse access token payload %q: %v", string(tokenBody), err)
	}

	log.V(1).Infof("Payload: %+v", payload)

	token := &oauth2.Token{
		AccessToken: payload.AccessToken,
		TokenType:   payload.TokenType,
		Expiry:      time.Now().Add(time.Duration(payload.ExpiresIn)*time.Second - expiryWiggleRoom),
	}
	return token, nil
}

func isOK(httpStatusCode int) bool {
	return 200 <= httpStatusCode && httpStatusCode < 300
}
