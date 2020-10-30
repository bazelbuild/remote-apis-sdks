// Package client contains a high-level remote execution client library.
package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/user"
	"strings"
	"sync"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/actas"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/balancer"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/chunker"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"
	"golang.org/x/oauth2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/oauth"
	"google.golang.org/grpc/status"

	configpb "github.com/bazelbuild/remote-apis-sdks/go/pkg/balancer/proto"
	regrpc "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	log "github.com/golang/glog"
	emptypb "github.com/golang/protobuf/ptypes/empty"
	bsgrpc "google.golang.org/genproto/googleapis/bytestream"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	opgrpc "google.golang.org/genproto/googleapis/longrunning"
	oppb "google.golang.org/genproto/googleapis/longrunning"
)

const (
	scopes      = "https://www.googleapis.com/auth/cloud-platform"
	authority   = "test-server"
	localPrefix = "localhost"

	// HomeDirMacro is replaced by the current user's home dir in the CredFile dial parameter.
	HomeDirMacro = "${HOME}"
)

// Client is a client to several services, including remote execution and services used in
// conjunction with remote execution. A Client must be constructed by calling Dial() or NewClient()
// rather than attempting to assemble it directly.
//
// Unless specified otherwise, and provided the fields are not modified, a Client is safe for
// concurrent use.
type Client struct {
	// InstanceName is the instance name for the targeted remote execution instance; e.g. for Google
	// RBE: "projects/<foo>/instances/default_instance".
	InstanceName string
	actionCache  regrpc.ActionCacheClient
	byteStream   bsgrpc.ByteStreamClient
	cas          regrpc.ContentAddressableStorageClient
	execution    regrpc.ExecutionClient
	operations   opgrpc.OperationsClient
	// Retrier is the Retrier that is used for RPCs made by this client.
	//
	// These fields are logically "protected" and are intended for use by extensions of Client.
	Retrier       *Retrier
	Connection    *grpc.ClientConn
	CASConnection *grpc.ClientConn // Can be different from Connection a separate CAS endpoint is provided.
	// StartupCapabilities denotes whether to load ServerCapabilities on startup.
	StartupCapabilities StartupCapabilities
	// ChunkMaxSize is maximum chunk size to use for CAS uploads/downloads.
	ChunkMaxSize ChunkMaxSize
	// MaxBatchDigests is maximum amount of digests to batch in batched operations.
	MaxBatchDigests MaxBatchDigests
	// MaxBatchSize is maximum size in bytes of a batch request for batch operations.
	MaxBatchSize MaxBatchSize
	// DirMode is mode used to create directories.
	DirMode os.FileMode
	// ExecutableMode is mode used to create executable files.
	ExecutableMode os.FileMode
	// RegularMode is mode used to create non-executable files.
	RegularMode os.FileMode
	// UtilizeLocality is to specify whether client downloads files utilizing disk access locality.
	UtilizeLocality UtilizeLocality
	serverCaps      *repb.ServerCapabilities
	useBatchOps     UseBatchOps
	casUploaders    chan bool
	casDownloaders  chan bool
	casUploadLocks  sync.Map
	casUploadErrors sync.Map
	rpcTimeouts     RPCTimeouts
	creds           credentials.PerRPCCredentials
}

const (
	// DefaultMaxBatchSize is the maximum size of a batch to upload with BatchWriteBlobs. We set it to slightly
	// below 4 MB, because that is the limit of a message size in gRPC
	DefaultMaxBatchSize = 4*1024*1024 - 1024

	// DefaultMaxBatchDigests is a suggested approximate limit based on current RBE implementation.
	// Above that BatchUpdateBlobs calls start to exceed a typical minute timeout.
	DefaultMaxBatchDigests = 4000

	// DefaultDirMode is mode used to create directories.
	DefaultDirMode = 0777

	// DefaultExecutableMode is mode used to create executable files.
	DefaultExecutableMode = 0777

	// DefaultRegularMode is mode used to create non-executable files.
	DefaultRegularMode = 0644
)

// Close closes the underlying gRPC connection(s).
func (c *Client) Close() error {
	err := c.Connection.Close()
	if err != nil {
		return err
	}
	if c.CASConnection != c.Connection {
		return c.CASConnection.Close()
	}
	return nil
}

// Opt is an option that can be passed to Dial in order to configure the behaviour of the client.
type Opt interface {
	Apply(*Client)
}

// ChunkMaxSize is maximum chunk size to use in Bytestream wrappers.
type ChunkMaxSize int

// Apply sets the client's maximal chunk size s.
func (s ChunkMaxSize) Apply(c *Client) {
	c.ChunkMaxSize = s
}

// UtilizeLocality is to specify whether client downloads files utilizing disk access locality.
type UtilizeLocality bool

func (s UtilizeLocality) Apply(c *Client) {
	c.UtilizeLocality = s
}

// MaxBatchDigests is maximum amount of digests to batch in batched operations.
type MaxBatchDigests int

// Apply sets the client's maximal batch digests to s.
func (s MaxBatchDigests) Apply(c *Client) {
	c.MaxBatchDigests = s
}

// MaxBatchSize is maximum size in bytes of a batch request for batch operations.
type MaxBatchSize int64

// Apply sets the client's maximum batch size to s.
func (s MaxBatchSize) Apply(c *Client) {
	c.MaxBatchSize = s
}

// UseBatchOps can be set to true to use batch CAS operations when uploading multiple blobs, or
// false to always use individual ByteStream requests.
type UseBatchOps bool

// Apply sets the UseBatchOps flag on a client.
func (u UseBatchOps) Apply(c *Client) {
	c.useBatchOps = u
}

// CASConcurrency is the number of simultaneous requests that will be issued for CAS upload and
// download operations.
type CASConcurrency int

// DefaultCASConcurrency is the default maximum number of concurrent upload and download operations.
const DefaultCASConcurrency = 500

// DefaultMaxConcurrentRequests specifies the default maximum number of concurrent requests on a single connection
// that the GRPC balancer can perform.
const DefaultMaxConcurrentRequests = 25

// DefaultMaxConcurrentStreams specifies the default threshold value at which the GRPC balancer should create
// new sub-connections.
const DefaultMaxConcurrentStreams = 25

// Apply sets the CASConcurrency flag on a client.
func (cy CASConcurrency) Apply(c *Client) {
	c.casUploaders = make(chan bool, cy)
	c.casDownloaders = make(chan bool, cy)
}

// StartupCapabilities controls whether the client should attempt to fetch the remote
// server capabilities on New. If set to true, some configuration such as MaxBatchSize
// is set according to the remote server capabilities instead of using the provided values.
type StartupCapabilities bool

// Apply sets the StartupCapabilities flag on a client.
func (s StartupCapabilities) Apply(c *Client) {
	c.StartupCapabilities = s
}

// PerRPCCreds sets per-call options that will be set on all RPCs to the underlying connection.
type PerRPCCreds struct {
	Creds credentials.PerRPCCredentials
}

// Apply saves the per-RPC creds in the Client.
func (p *PerRPCCreds) Apply(c *Client) {
	c.creds = p.Creds
}

func getImpersonatedRPCCreds(ctx context.Context, actAs string, cred credentials.PerRPCCredentials) credentials.PerRPCCredentials {
	// Wrap in a ReuseTokenSource to cache valid tokens in memory (i.e., non-nil, with a non-expired
	// access token).
	ts := oauth2.ReuseTokenSource(
		nil, actas.NewTokenSource(ctx, cred, http.DefaultClient, actAs, []string{scopes}))
	return oauth.TokenSource{
		TokenSource: ts,
	}
}

func getRPCCreds(ctx context.Context, credFile string, useApplicationDefault bool, useComputeEngine bool) (credentials.PerRPCCredentials, error) {
	if useApplicationDefault {
		return oauth.NewApplicationDefault(ctx, scopes)
	}
	if useComputeEngine {
		return oauth.NewComputeEngine(), nil
	}
	rpcCreds, err := oauth.NewServiceAccountFromFile(credFile, scopes)
	if err != nil {
		return nil, fmt.Errorf("couldn't create RPC creds from %s: %v", credFile, err)
	}
	return rpcCreds, nil
}

// DialParams contains all the parameters that Dial needs.
type DialParams struct {
	// Service contains the address of remote execution service.
	Service string

	// CASService contains the address of the CAS service, if it is separate from
	// the remote execution service.
	CASService string

	// UseApplicationDefault indicates that the default credentials should be used.
	UseApplicationDefault bool

	// UseComputeEngine indicates that the default CE credentials should be used.
	UseComputeEngine bool

	// CredFile is the JSON file that contains the credentials for RPCs.
	CredFile string

	// ActAsAccount is the service account to act as when making RPC calls.
	ActAsAccount string

	// NoSecurity is true if there is no security: no credentials are configured and
	// grpc.WithInsecure() is passed in. Should only be used in test code.
	NoSecurity bool

	// TransportCredsOnly is true if it's the caller's responsibility to set per-RPC credentials
	// on individual calls. This overrides ActAsAccount, UseApplicationDefault, and UseComputeEngine.
	// This is not the same as NoSecurity, as transport credentials will still be set.
	TransportCredsOnly bool

	// TLSCACertFile is the PEM file that contains TLS root certificates.
	TLSCACertFile string

	// TLSServerName overrides the server name sent in TLS, if set to a non-empty string.
	TLSServerName string

	// DialOpts defines the set of gRPC DialOptions to apply, in addition to any used internally.
	DialOpts []grpc.DialOption

	// MaxConcurrentRequests specifies the maximum number of concurrent RPCs on a single connection.
	MaxConcurrentRequests uint32

	// MaxConcurrentStreams specifies the maximum number of concurrent stream RPCs on a single connection.
	MaxConcurrentStreams uint32

	// TLSClientAuthCert specifies the public key in PEM format for using mTLS auth to connect to the RBE service.
	//
	// If this is specified, TLSClientAuthKey must also be specified.
	TLSClientAuthCert string

	// TLSClientAuthKey specifies the private key for using mTLS auth to connect to the RBE service.
	//
	// If this is specified, TLSClientAuthCert must also be specified.
	TLSClientAuthKey string
}

func createGRPCInterceptor(p DialParams) *balancer.GCPInterceptor {
	apiConfig := &configpb.ApiConfig{
		ChannelPool: &configpb.ChannelPoolConfig{
			MaxSize:                          p.MaxConcurrentRequests,
			MaxConcurrentStreamsLowWatermark: p.MaxConcurrentStreams,
		},
		Method: []*configpb.MethodConfig{
			&configpb.MethodConfig{
				Name: []string{".*"},
				Affinity: &configpb.AffinityConfig{
					Command:     configpb.AffinityConfig_BIND,
					AffinityKey: "bind-affinity",
				},
			},
		},
	}
	return balancer.NewGCPInterceptor(apiConfig)
}

func createTLSConfig(params DialParams) (*tls.Config, error) {
	var certPool *x509.CertPool
	if params.TLSCACertFile != "" {
		certPool = x509.NewCertPool()
		ca, err := ioutil.ReadFile(params.TLSCACertFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read %s: %w", params.TLSCACertFile, err)
		}
		if ok := certPool.AppendCertsFromPEM(ca); !ok {
			return nil, fmt.Errorf("failed to load TLS CA certificates from %s", params.TLSCACertFile)
		}
	}

	var mTLSCredentials []tls.Certificate
	if params.TLSClientAuthCert != "" || params.TLSClientAuthKey != "" {
		if params.TLSClientAuthCert == "" || params.TLSClientAuthKey == "" {
			return nil, fmt.Errorf("TLSClientAuthCert and TLSClientAuthKey must both be empty or both be set, got TLSClientAuthCert='%v' and TLSClientAuthKey='%v'", params.TLSClientAuthCert, params.TLSClientAuthKey)
		}

		cert, err := tls.LoadX509KeyPair(params.TLSClientAuthCert, params.TLSClientAuthKey)
		if err != nil {
			return nil, fmt.Errorf("failed to read mTLS cert pair ('%v', '%v'): %v", params.TLSClientAuthCert, params.TLSClientAuthKey, err)
		}
		mTLSCredentials = append(mTLSCredentials, cert)
	}

	c := &tls.Config{
		ServerName:   params.TLSServerName,
		RootCAs:      certPool,
		Certificates: mTLSCredentials,
	}
	return c, nil
}

// Dial dials a given endpoint and returns the grpc connection that is established.
func Dial(ctx context.Context, endpoint string, params DialParams) (*grpc.ClientConn, error) {
	var opts []grpc.DialOption
	opts = append(opts, params.DialOpts...)

	if params.MaxConcurrentRequests == 0 {
		params.MaxConcurrentRequests = DefaultMaxConcurrentRequests
	}
	if params.MaxConcurrentStreams == 0 {
		params.MaxConcurrentStreams = DefaultMaxConcurrentStreams
	}
	if params.NoSecurity {
		opts = append(opts, grpc.WithInsecure())
	} else {
		credFile := params.CredFile
		if strings.Contains(credFile, HomeDirMacro) {
			usr, err := user.Current()
			if err != nil {
				return nil, fmt.Errorf("could not fetch home directory because of error determining current user: %v", err)
			}
			credFile = strings.Replace(credFile, HomeDirMacro, usr.HomeDir, -1 /* no limit */)
		}

		if !params.TransportCredsOnly {
			rpcCreds, err := getRPCCreds(ctx, credFile, params.UseApplicationDefault, params.UseComputeEngine)
			if err != nil {
				return nil, fmt.Errorf("couldn't create RPC creds for %s: %v", scopes, err)
			}

			if params.ActAsAccount != "" {
				rpcCreds = getImpersonatedRPCCreds(ctx, params.ActAsAccount, rpcCreds)
			}

			opts = append(opts, grpc.WithPerRPCCredentials(rpcCreds))
		}

		tlsConfig, err := createTLSConfig(params)
		if err != nil {
			return nil, fmt.Errorf("Could not create TLS config: %v", err)
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	}
	grpcInt := createGRPCInterceptor(params)
	opts = append(opts, grpc.WithBalancerName(balancer.Name))
	opts = append(opts, grpc.WithUnaryInterceptor(grpcInt.GCPUnaryClientInterceptor))
	opts = append(opts, grpc.WithStreamInterceptor(grpcInt.GCPStreamClientInterceptor))

	conn, err := grpc.Dial(endpoint, opts...)
	if err != nil {
		return nil, fmt.Errorf("couldn't dial gRPC %q: %v", endpoint, err)
	}
	return conn, nil
}

// DialRaw dials a remote execution service and returns the grpc connection that is established.
// TODO(olaola): remove this overload when all clients use Dial.
func DialRaw(ctx context.Context, params DialParams) (*grpc.ClientConn, error) {
	if params.Service == "" {
		return nil, fmt.Errorf("service needs to be specified")
	}
	log.Infof("Connecting to remote execution service %s", params.Service)
	return Dial(ctx, params.Service, params)
}

// NewClient connects to a remote execution service and returns a client suitable for higher-level
// functionality.
func NewClient(ctx context.Context, instanceName string, params DialParams, opts ...Opt) (*Client, error) {
	if instanceName == "" {
		log.Warning("Instance name was not specified.")
	}
	if params.Service == "" {
		return nil, fmt.Errorf("service needs to be specified")
	}
	log.Infof("Connecting to remote execution instance %s", instanceName)
	log.Infof("Connecting to remote execution service %s", params.Service)
	conn, err := Dial(ctx, params.Service, params)
	casConn := conn
	if params.CASService != "" && params.CASService != params.Service {
		log.Infof("Connecting to CAS service %s", params.Service)
		casConn, err = Dial(ctx, params.CASService, params)
	}
	if err != nil {
		return nil, err
	}
	client := &Client{
		InstanceName:        instanceName,
		actionCache:         regrpc.NewActionCacheClient(casConn),
		byteStream:          bsgrpc.NewByteStreamClient(casConn),
		cas:                 regrpc.NewContentAddressableStorageClient(casConn),
		execution:           regrpc.NewExecutionClient(conn),
		operations:          opgrpc.NewOperationsClient(conn),
		rpcTimeouts:         DefaultRPCTimeouts,
		Connection:          conn,
		CASConnection:       casConn,
		ChunkMaxSize:        chunker.DefaultChunkSize,
		MaxBatchDigests:     DefaultMaxBatchDigests,
		MaxBatchSize:        DefaultMaxBatchSize,
		DirMode:             DefaultDirMode,
		ExecutableMode:      DefaultExecutableMode,
		RegularMode:         DefaultRegularMode,
		useBatchOps:         true,
		StartupCapabilities: true,
		casUploaders:        make(chan bool, DefaultCASConcurrency),
		casDownloaders:      make(chan bool, DefaultCASConcurrency),
		Retrier:             RetryTransient(),
	}
	for _, o := range opts {
		o.Apply(client)
	}
	if client.StartupCapabilities {
		if err := client.CheckCapabilities(ctx); err != nil {
			return nil, err
		}
	}
	return client, nil
}

// RPCTimeouts is a Opt that sets the per-RPC deadline.
// The keys are RPC names. The "default" key, if present, is the default
// timeout. 0 values are valid and indicate no timeout.
type RPCTimeouts map[string]time.Duration

// Apply applies the timeouts to a Client. It overrides the provided values,
// but doesn't remove/alter any other present values.
func (d RPCTimeouts) Apply(c *Client) {
	c.rpcTimeouts = map[string]time.Duration(d)
}

var DefaultRPCTimeouts = map[string]time.Duration{
	"default":          20 * time.Second,
	"GetCapabilities":  5 * time.Second,
	"Read":             time.Minute,
	"Write":            time.Minute,
	"BatchUpdateBlobs": time.Minute,
	"BatchReadBlobs":   time.Minute,
	"GetTree":          time.Minute,
	// Note: due to an implementation detail, WaitExecution will use the same
	// per-RPC timeout as Execute. It is extremely ill-advised to set the Execute
	// timeout at above 0; most users should use the Action Timeout instead.
	"Execute":       0,
	"WaitExecution": 0,
}

// RPCOpts returns the default RPC options that should be used for calls made with this client.
//
// This method is logically "protected" and is intended for use by extensions of Client.
func (c *Client) RPCOpts() []grpc.CallOption {
	// Set a high limit on receiving large messages from the server.
	opts := []grpc.CallOption{grpc.MaxCallRecvMsgSize(100 * 1024 * 1024)}
	if c.creds == nil {
		return opts
	}
	return append(opts, grpc.PerRPCCredentials(c.creds))
}

// CallWithTimeout executes the given function f with a context that times out after an RPC timeout.
//
// This method is logically "protected" and is intended for use by extensions of Client.
func (c *Client) CallWithTimeout(ctx context.Context, rpcName string, f func(ctx context.Context) error) error {
	timeout, ok := c.rpcTimeouts[rpcName]
	if !ok {
		if timeout, ok = c.rpcTimeouts["default"]; !ok {
			timeout = 0
		}
	}
	if timeout == 0 {
		return f(ctx)
	}
	childCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	e := f(childCtx)
	if childCtx.Err() != nil {
		return childCtx.Err()
	}
	return e
}

// Retrier applied to all client requests.
type Retrier struct {
	Backoff     retry.BackoffPolicy
	ShouldRetry retry.ShouldRetry
}

// Apply sets the client's retrier function to r.
func (r *Retrier) Apply(c *Client) {
	c.Retrier = r
}

// Do executes f() with retries.
// It can be called with a nil receiver; in that case no retries are done (just a passthrough call
// to f()).
func (r *Retrier) Do(ctx context.Context, f func() error) error {
	if r == nil {
		return f()
	}
	return retry.WithPolicy(ctx, r.ShouldRetry, r.Backoff, f)
}

// RetryTransient is a default retry policy for transient status codes.
func RetryTransient() *Retrier {
	return &Retrier{
		Backoff: retry.ExponentialBackoff(225*time.Millisecond, 2*time.Second, retry.Attempts(6)),
		ShouldRetry: func(err error) bool {
			// Retry RPC timeouts. Note that we do *not* retry context cancellations (context.Cancelled);
			// if the user wants to back out of the call we should let them.
			if err == context.DeadlineExceeded {
				return true
			}
			s, ok := status.FromError(err)
			if !ok {
				return false
			}
			switch s.Code() {
			case codes.Canceled, codes.Unknown, codes.DeadlineExceeded, codes.Aborted,
				codes.Internal, codes.Unavailable, codes.Unauthenticated, codes.ResourceExhausted:
				return true
			default:
				return false
			}
		},
	}
}

// GetActionResult wraps the underlying call with specific client options.
func (c *Client) GetActionResult(ctx context.Context, req *repb.GetActionResultRequest) (res *repb.ActionResult, err error) {
	opts := c.RPCOpts()
	err = c.Retrier.Do(ctx, func() (e error) {
		return c.CallWithTimeout(ctx, "GetActionResult", func(ctx context.Context) (e error) {
			res, e = c.actionCache.GetActionResult(ctx, req, opts...)
			return e
		})
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// UpdateActionResult wraps the underlying call with specific client options.
func (c *Client) UpdateActionResult(ctx context.Context, req *repb.UpdateActionResultRequest) (res *repb.ActionResult, err error) {
	opts := c.RPCOpts()
	err = c.Retrier.Do(ctx, func() (e error) {
		return c.CallWithTimeout(ctx, "UpdateActionResult", func(ctx context.Context) (e error) {
			res, e = c.actionCache.UpdateActionResult(ctx, req, opts...)
			return e
		})
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Read wraps the underlying call with specific client options.
// The wrapper is here for completeness to provide access to the low-level
// RPCs. Prefer using higher-level functions such as ReadBlob(ToFile) instead,
// as they include retries/timeouts handling.
func (c *Client) Read(ctx context.Context, req *bspb.ReadRequest) (res bsgrpc.ByteStream_ReadClient, err error) {
	return c.byteStream.Read(ctx, req, c.RPCOpts()...)
}

// Write wraps the underlying call with specific client options.
// The wrapper is here for completeness to provide access to the low-level
// RPCs. Prefer using higher-level functions such as WriteBlob(s) instead,
// as they include retries/timeouts handling.
func (c *Client) Write(ctx context.Context) (res bsgrpc.ByteStream_WriteClient, err error) {
	return c.byteStream.Write(ctx, c.RPCOpts()...)
}

// QueryWriteStatus wraps the underlying call with specific client options.
func (c *Client) QueryWriteStatus(ctx context.Context, req *bspb.QueryWriteStatusRequest) (res *bspb.QueryWriteStatusResponse, err error) {
	opts := c.RPCOpts()
	err = c.Retrier.Do(ctx, func() (e error) {
		return c.CallWithTimeout(ctx, "QueryWriteStatus", func(ctx context.Context) (e error) {
			res, e = c.byteStream.QueryWriteStatus(ctx, req, opts...)
			return e
		})
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// FindMissingBlobs wraps the underlying call with specific client options.
func (c *Client) FindMissingBlobs(ctx context.Context, req *repb.FindMissingBlobsRequest) (res *repb.FindMissingBlobsResponse, err error) {
	opts := c.RPCOpts()
	err = c.Retrier.Do(ctx, func() (e error) {
		return c.CallWithTimeout(ctx, "FindMissingBlobs", func(ctx context.Context) (e error) {
			res, e = c.cas.FindMissingBlobs(ctx, req, opts...)
			return e
		})
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// BatchUpdateBlobs wraps the underlying call with specific client options.
// NOTE that its retry logic ignores the per-blob errors embedded in the response; you probably want
// to use BatchWriteBlobs() instead.
func (c *Client) BatchUpdateBlobs(ctx context.Context, req *repb.BatchUpdateBlobsRequest) (res *repb.BatchUpdateBlobsResponse, err error) {
	opts := c.RPCOpts()
	err = c.Retrier.Do(ctx, func() (e error) {
		return c.CallWithTimeout(ctx, "BatchUpdateBlobs", func(ctx context.Context) (e error) {
			res, e = c.cas.BatchUpdateBlobs(ctx, req, opts...)
			return e
		})
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// BatchReadBlobs wraps the underlying call with specific client options.
// NOTE that its retry logic ignores the per-blob errors embedded in the response.
func (c *Client) BatchReadBlobs(ctx context.Context, req *repb.BatchReadBlobsRequest) (res *repb.BatchReadBlobsResponse, err error) {
	opts := c.RPCOpts()
	err = c.Retrier.Do(ctx, func() (e error) {
		return c.CallWithTimeout(ctx, "BatchReadBlobs", func(ctx context.Context) (e error) {
			res, e = c.cas.BatchReadBlobs(ctx, req, opts...)
			return e
		})
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// GetTree wraps the underlying call with specific client options.
// The wrapper is here for completeness to provide access to the low-level
// RPCs. Prefer using higher-level GetDirectoryTree instead,
// as it includes retries/timeouts handling.
func (c *Client) GetTree(ctx context.Context, req *repb.GetTreeRequest) (res regrpc.ContentAddressableStorage_GetTreeClient, err error) {
	return c.cas.GetTree(ctx, req, c.RPCOpts()...)
}

// Execute wraps the underlying call with specific client options.
// The wrapper is here for completeness to provide access to the low-level
// RPCs. Prefer using higher-level ExecuteAndWait instead,
// as it includes retries/timeouts handling.
func (c *Client) Execute(ctx context.Context, req *repb.ExecuteRequest) (res regrpc.Execution_ExecuteClient, err error) {
	return c.execution.Execute(ctx, req, c.RPCOpts()...)
}

// WaitExecution wraps the underlying call with specific client options.
// The wrapper is here for completeness to provide access to the low-level
// RPCs. Prefer using higher-level ExecuteAndWait instead,
// as it includes retries/timeouts handling.
func (c *Client) WaitExecution(ctx context.Context, req *repb.WaitExecutionRequest) (res regrpc.Execution_ExecuteClient, err error) {
	return c.execution.WaitExecution(ctx, req, c.RPCOpts()...)
}

// GetBackendCapabilities returns the capabilities for a specific server connection
// (either the main connection or the CAS connection).
func (c *Client) GetBackendCapabilities(ctx context.Context, conn *grpc.ClientConn, req *repb.GetCapabilitiesRequest) (res *repb.ServerCapabilities, err error) {
	opts := c.RPCOpts()
	err = c.Retrier.Do(ctx, func() (e error) {
		return c.CallWithTimeout(ctx, "GetCapabilities", func(ctx context.Context) (e error) {
			res, e = regrpc.NewCapabilitiesClient(conn).GetCapabilities(ctx, req, opts...)
			return e
		})
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// GetOperation wraps the underlying call with specific client options.
func (c *Client) GetOperation(ctx context.Context, req *oppb.GetOperationRequest) (res *oppb.Operation, err error) {
	opts := c.RPCOpts()
	err = c.Retrier.Do(ctx, func() (e error) {
		return c.CallWithTimeout(ctx, "GetOperation", func(ctx context.Context) (e error) {
			res, e = c.operations.GetOperation(ctx, req, opts...)
			return e
		})
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// ListOperations wraps the underlying call with specific client options.
func (c *Client) ListOperations(ctx context.Context, req *oppb.ListOperationsRequest) (res *oppb.ListOperationsResponse, err error) {
	opts := c.RPCOpts()
	err = c.Retrier.Do(ctx, func() (e error) {
		return c.CallWithTimeout(ctx, "ListOperations", func(ctx context.Context) (e error) {
			res, e = c.operations.ListOperations(ctx, req, opts...)
			return e
		})
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// CancelOperation wraps the underlying call with specific client options.
func (c *Client) CancelOperation(ctx context.Context, req *oppb.CancelOperationRequest) (res *emptypb.Empty, err error) {
	opts := c.RPCOpts()
	err = c.Retrier.Do(ctx, func() (e error) {
		return c.CallWithTimeout(ctx, "CancelOperation", func(ctx context.Context) (e error) {
			res, e = c.operations.CancelOperation(ctx, req, opts...)
			return e
		})
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// DeleteOperation wraps the underlying call with specific client options.
func (c *Client) DeleteOperation(ctx context.Context, req *oppb.DeleteOperationRequest) (res *emptypb.Empty, err error) {
	opts := c.RPCOpts()
	err = c.Retrier.Do(ctx, func() (e error) {
		return c.CallWithTimeout(ctx, "DeleteOperation", func(ctx context.Context) (e error) {
			res, e = c.operations.DeleteOperation(ctx, req, opts...)
			return e
		})
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}
