package actas

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	log "github.com/golang/glog"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"golang.org/x/oauth2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

const (
	account = "fake-account@fake-consumer.iam.gserviceaccount.com"
	scope   = "https://www.googleapis.com/auth/cloud-platform"
)

// defaultHandler is a default HTTP request handler that returns a nil body.
func defaultHandler(req *http.Request) (interface{}, error) {
	return nil, nil
}

// fakeHTTP provides an HTTP server routing calls to its handler and a client connected to it.
type fakeHTTP struct {
	// Handler is handler for HTTP requests.
	Handler func(req *http.Request) (body interface{}, err error)
	// Server is the HTTP server. Uses handler for handling requests.
	Server *httptest.Server
	// Client is the HTTP client connected to the HTTP server.
	Client *http.Client
}

// newFakeHTTP creates a new HTTP server and client.
func newFakeHTTP() (*fakeHTTP, func() error) {
	f := &fakeHTTP{}
	f.Handler = defaultHandler
	h := func(w http.ResponseWriter, req *http.Request) {
		log.Infof("HTTP Request: %+v", req)
		body, err := f.Handler(req)
		if err != nil {
			// This is not strictly the correct HTTP status (different gRPC codes should lead to different
			// HTTP statuses), but for the purposes of this test it doesn't matter.
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if err := json.NewEncoder(w).Encode(body); err != nil {
			log.Errorf("json.NewEncoder(%v).Encode(%v) failed: %v", w, body, err)
			http.Error(w, "encoding the response failed", http.StatusInternalServerError)
			return
		}
	}

	f.Server = httptest.NewServer(http.HandlerFunc(h))
	f.Client = f.Server.Client()
	cleanup := func() error {
		f.Server.Close()
		return nil
	}
	return f, cleanup
}

type stubDefaultCredentials struct {
	credentials.PerRPCCredentials
	err error
}

func (s *stubDefaultCredentials) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{"key": "value"}, s.err
}

func TestTokenSource_Token(t *testing.T) {
	ctx := context.Background()

	h, cleanup := newFakeHTTP()
	defer cleanup()

	// Override URLs for test.
	newSignJWTURL = func(string) string {
		return h.Server.URL + "/sign"
	}
	audienceURL = h.Server.URL + "/token"
	// duration of token
	duration := 10 * time.Minute
	token := 0
	// Populate the response in the http request handler based on the request URL.
	h.Handler = func(req *http.Request) (body interface{}, err error) {
		log.Infof("HTTP Request: %+v", req)
		if req.URL.Path == "/sign" {
			return &signaturePayload{
				KeyID:     "fake-key-id",
				SignedJwt: "fake-signed-jwt",
			}, nil
		}
		if req.URL.Path == "/token" {
			// Each time a call is made for obtaining a token we return a new one.
			// This is to check that calls are made only when needed.
			token++
			return &tokenPayload{
				AccessToken: fmt.Sprintf("fake-access-token-%v", token),
				TokenType:   "fake-token-type",
				ExpiresIn:   int64(duration.Seconds()),
			}, nil
		}
		return nil, nil
	}
	d := &stubDefaultCredentials{}

	before := time.Now()
	s := NewTokenSource(ctx, d, h.Client, account, []string{scope})

	// First token.
	got, err := s.Token()
	if err != nil {
		t.Fatalf("Token() failed: %v", err)
	}
	want := &oauth2.Token{
		AccessToken: "fake-access-token-1",
		TokenType:   "fake-token-type",
	}
	opts := cmp.Options{cmpopts.IgnoreUnexported(oauth2.Token{}), cmpopts.IgnoreFields(oauth2.Token{}, "Expiry")}
	if diff := cmp.Diff(want, got, opts); diff != "" {
		t.Errorf("Token() returned diff:\n%s\n", diff)
	}

	// Check if the expiry is in expected range (want-2,want+2)
	w := 2 * time.Minute
	wantExp := before.Add(duration)
	if got.Expiry.Before(wantExp.Add(-w)) || got.Expiry.After(wantExp.Add(w)) {
		t.Errorf("Token().Expiry = %+v, want in [%v,%v])", got.Expiry, wantExp.Add(-w), wantExp.Add(w))
	}

	// Second token.
	// Expiry is after now, but actas.TokenSource should not do its own caching, so we expect a new
	// token.
	want.AccessToken = "fake-access-token-2"
	got, err = s.Token()
	if err != nil {
		t.Fatalf("Token() failed: %v", err)
	}
	if diff := cmp.Diff(want, got, opts); diff != "" {
		t.Errorf("Token() returned diff:\n%s\n", diff)
	}
}

func TestNewTokenSource_CredGetRequestMetadataFails(t *testing.T) {
	ctx := context.Background()

	h, cleanup := newFakeHTTP()
	defer cleanup()

	// Override URLs for test.
	newSignJWTURL = func(string) string {
		return h.Server.URL + "/sign"
	}
	audienceURL = h.Server.URL + "/token"

	d := &stubDefaultCredentials{err: status.Error(codes.Unknown, "some error")}

	s := NewTokenSource(ctx, d, h.Client, account, []string{scope})
	if _, err := s.Token(); err == nil {
		t.Fatal("Token() should fail when GetRequestMetadata fails.")
	}
}

func TestTokenSource_Token_GettingSignatureFails(t *testing.T) {
	ctx := context.Background()

	h, cleanup := newFakeHTTP()
	defer cleanup()

	// Override URLs for test.
	newSignJWTURL = func(string) string {
		return h.Server.URL + "/sign"
	}
	audienceURL = h.Server.URL + "/token"
	// duration of token
	duration := 10 * time.Minute
	token := 0
	// Populate the response in the http request handler based on the request URL.
	h.Handler = func(req *http.Request) (body interface{}, err error) {
		log.Infof("HTTP Request: %+v", req)
		if req.URL.Path == "/sign" {
			return nil, status.Error(codes.Unknown, "some error")
		}
		if req.URL.Path == "/token" {
			token++
			return &tokenPayload{
				AccessToken: fmt.Sprintf("fake-access-token-%v", token),
				TokenType:   "fake-token-type",
				ExpiresIn:   int64(duration.Nanoseconds()),
			}, nil
		}
		return nil, nil
	}
	d := &stubDefaultCredentials{}

	s := NewTokenSource(ctx, d, h.Client, account, []string{scope})
	if _, err := s.Token(); err == nil {
		t.Fatalf("Token() should fail when cannot get signature.")
	}
}

func TestTokenSource_Token_GettingTokenFails(t *testing.T) {
	ctx := context.Background()

	h, cleanup := newFakeHTTP()
	defer cleanup()

	// Override URLs for test.
	newSignJWTURL = func(string) string {
		return h.Server.URL + "/sign"
	}
	audienceURL = h.Server.URL + "/token"

	// Populate the response in the http request handler based on the request URL.
	h.Handler = func(req *http.Request) (body interface{}, err error) {
		log.Infof("HTTP Request: %+v", req)
		if req.URL.Path == "/sign" {
			return &signaturePayload{
				KeyID:     "fake-key-id",
				SignedJwt: "fake-signed-jwt",
			}, nil
		}
		if req.URL.Path == "/token" {
			return nil, status.Error(codes.Unknown, "some error")
		}
		return nil, nil
	}
	d := &stubDefaultCredentials{}

	s := NewTokenSource(ctx, d, h.Client, account, []string{scope})
	if _, err := s.Token(); err == nil {
		t.Fatalf("Token() should fail when cannot get token.")
	}
}

func Test_isOK(t *testing.T) {
	tests := []struct {
		code int
		want bool
	}{
		{
			code: 199,
			want: false,
		},
		{
			code: 200,
			want: true,
		},
		{
			code: 299,
			want: true,
		},
		{
			code: 300,
			want: false,
		},
	}
	for _, tc := range tests {
		if got := isOK(tc.code); got != tc.want {
			t.Errorf("isOK(%v) = %v, want %v", tc.code, got, tc.want)
		}
	}
}
