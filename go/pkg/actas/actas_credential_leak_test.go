package actas

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Regression test: previously, on a 200-status response from the access-token
// endpoint whose body failed to unmarshal into tokenPayload (e.g. because
// expires_in is returned as a JSON string instead of a number, a real-world
// OAuth2 server quirk), getToken() embedded the full raw response body --
// including the live access_token -- verbatim into the returned error. The fix
// drops the raw body from the error message.
func TestTokenSource_Token_MalformedButSuccessfulResponseDoesNotLeakAccessToken(t *testing.T) {
	ctx := context.Background()

	h, cleanup := newFakeHTTP()
	defer cleanup()

	newSignJWTURL = func(string) string {
		return h.Server.URL + "/sign"
	}
	audienceURL = h.Server.URL + "/token"

	const liveSecretToken = "ya29.REAL-LOOKING-LIVE-ACCESS-TOKEN-abcXYZ123"

	h.Handler = func(req *http.Request) (interface{}, error) {
		if req.URL.Path == "/sign" {
			return &signaturePayload{
				KeyID:     "fake-key-id",
				SignedJwt: "fake-signed-jwt",
			}, nil
		}
		if req.URL.Path == "/token" {
			// Real endpoint, HTTP 200, but expires_in is a STRING instead of a number --
			// a real-world quirk seen from some OAuth2-compatible token servers. This makes
			// json.Unmarshal into tokenPayload{ExpiresIn int64} fail even though the body
			// legitimately contains a live access token.
			return map[string]any{
				"access_token": liveSecretToken,
				"token_type":   "Bearer",
				"expires_in":   "3600",
			}, nil
		}
		return nil, nil
	}

	d := &stubDefaultCredentials{}
	s := NewTokenSource(ctx, d, h.Client, account, []string{scope})

	_, err := s.Token()
	if err == nil {
		t.Fatalf("Token() unexpectedly succeeded, expected the malformed-body parse error")
	}

	if strings.Contains(err.Error(), liveSecretToken) {
		t.Fatalf("access token leaked into error text: %v", err)
	}
}

// Regression test: previously, the sibling getSignedJWT() had the identical bug
// shape for the signed-JWT bearer credential. A 200 response with a body that is
// genuine JSON truncated mid-stream (a realistic network/proxy truncation
// scenario) still failed json.Unmarshal, and the raw (truncated but still
// credential-bearing) body was embedded verbatim in the returned error. The fix
// drops the raw body from the error message.
func TestTokenSource_Token_TruncatedResponseDoesNotLeakSignedJWT(t *testing.T) {
	ctx := context.Background()

	const liveSignedJwt = "eyJhbGciOiJSUzI1NiJ9.REAL-LOOKING-LIVE-SIGNED-JWT-PAYLOAD.sigSIG"

	// Full well-formed body would be:
	//   {"keyId":"fake-key-id","signedJwt":"eyJhbGciOiJSUzI1NiJ9.REAL-LOOKING-LIVE-SIGNED-JWT-PAYLOAD.sigSIG"}
	// Simulate a network/proxy truncation that cuts it off right after the value,
	// before the closing brace -- the signedJwt bytes are still fully present in the
	// truncated body, but the JSON itself is now invalid.
	truncatedBody := `{"keyId":"fake-key-id","signedJwt":"` + liveSignedJwt + `"`

	signServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(truncatedBody))
	}))
	defer signServer.Close()

	newSignJWTURL = func(string) string { return signServer.URL }

	d := &stubDefaultCredentials{}
	s := NewTokenSource(ctx, d, signServer.Client(), account, []string{scope})

	_, err := s.Token()
	if err == nil {
		t.Fatalf("Token() unexpectedly succeeded, expected the truncated-body parse error")
	}

	if strings.Contains(err.Error(), liveSignedJwt) {
		t.Fatalf("signed JWT leaked into error text: %v", err)
	}
}
