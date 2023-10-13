// Package client contains a high-level remote execution client library.
package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"math"
	"net/http"
	"os"
	"os/user"
	"strings"
	"sync"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/actas"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/balancer"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/casng"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/chunker"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/uploadinfo"
	"github.com/pkg/errors"
	"golang.org/x/oauth2"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/oauth"
	"google.golang.org/grpc/status"

	// Redundant imports are required for the google3 mirror. Aliases should not be changed.
	configpb "github.com/bazelbuild/remote-apis-sdks/go/pkg/balancer/proto"
	regrpc "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	log "github.com/golang/glog"
	bsgrpc "google.golang.org/genproto/googleapis/bytestream"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	opgrpc "google.golang.org/genproto/googleapis/longrunning"
	oppb "google.golang.org/genproto/googleapis/longrunning"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

const (
	scopes = "https://www.googleapis.com/auth/cloud-platform"

	// HomeDirMacro is replaced by the current user's home dir in the CredFile dial parameter.
	HomeDirMacro = "${HOME}"
)

// ErrEmptySegment indicates an attempt to construct a resource name with an empty segment.
var ErrEmptySegment = errors.New("empty segment in resoure name")

// AuthType indicates the type of authentication being used.
type AuthType int

const (
	// UnknownAuth refers to unknown authentication type.
	UnknownAuth AuthType = iota

	// NoAuth refers to no authentication when connecting to the RBE service.
	NoAuth

	// ExternalTokenAuth is used to connect to the RBE service.
	ExternalTokenAuth

	// CredsFileAuth refers to a JSON credentials file used to connect to the RBE service.
	CredsFileAuth

	// ApplicationDefaultCredsAuth refers to Google Application default credentials that is
	// used to connect to the RBE service.
	ApplicationDefaultCredsAuth

	// GCECredsAuth refers to GCE machine credentials that is
	// used to connect to the RBE service.
	GCECredsAuth
)

// String returns a human readable form of authentication used to connect to RBE.
func (a AuthType) String() string {
	switch a {
	case NoAuth:
		return "no authentication"
	case ExternalTokenAuth:
		return "external authentication token (gcert?)"
	case CredsFileAuth:
		return "credentials file"
	case ApplicationDefaultCredsAuth:
		return "application default credentials"
	case GCECredsAuth:
		return "gce credentials"
	}
	return "unknown authentication type"
}

// InitError is used to wrap the error returned when initializing a new
// client to also indicate the type of authentication used.
type InitError struct {
	// Err refers to the underlying client initialization error.
	Err error

	// AuthUsed stores the type of authentication used to connect to RBE.
	AuthUsed AuthType
}

// Error returns a string error that includes information about the
// type of auth used to connect to RBE.
func (ce *InitError) Error() string {
	return fmt.Sprintf("%v, authentication type (identity) used=%q", ce.Err.Error(), ce.AuthUsed)
}

// Client is a client to several services, including remote execution and services used in
// conjunction with remote execution. A Client must be constructed by calling Dial() or NewClient()
// rather than attempting to assemble it directly.
//
// Unless specified otherwise, and provided the fields are not modified, a Client is safe for
// concurrent use.
type Client struct {
	// InstanceName is the instance name for the targeted remote execution instance; e.g. for Google
	// RBE: "projects/<foo>/instances/default_instance".
	// It should NOT be used to construct resource names, but rather only for reusing the instance name as is.
	// Use the ResourceName method to create correctly formatted resource names.
	InstanceName  string
	actionCache   regrpc.ActionCacheClient
	byteStream    bsgrpc.ByteStreamClient
	cas           regrpc.ContentAddressableStorageClient
	useCasNg      bool
	ngCasUploader *casng.BatchingUploader
	execution     regrpc.ExecutionClient
	operations    opgrpc.OperationsClient
	// Retrier is the Retrier that is used for RPCs made by this client.
	//
	// These fields are logically "protected" and are intended for use by extensions of Client.
	Retrier       *Retrier
	Connection    *grpc.ClientConn
	CASConnection *grpc.ClientConn // Can be different from Connection a separate CAS endpoint is provided.
	// StartupCapabilities denotes whether to load ServerCapabilities on startup.
	StartupCapabilities StartupCapabilities
	// LegacyExecRootRelativeOutputs denotes whether outputs are relative to the exec root.
	LegacyExecRootRelativeOutputs LegacyExecRootRelativeOutputs
	// ChunkMaxSize is maximum chunk size to use for CAS uploads/downloads.
	ChunkMaxSize ChunkMaxSize
	// CompressedBytestreamThreshold is the threshold in bytes for which blobs are read and written
	// compressed. Use 0 for all writes being compressed, and a negative number for all operations being
	// uncompressed.
	CompressedBytestreamThreshold CompressedBytestreamThreshold
	// UploadCompressionPredicate is a function called to decide whether a blob should be compressed for upload.
	UploadCompressionPredicate UploadCompressionPredicate
	// MaxBatchDigests is maximum amount of digests to batch in upload and download operations.
	MaxBatchDigests MaxBatchDigests
	// MaxQueryBatchDigests is maximum amount of digests to batch in CAS query operations.
	MaxQueryBatchDigests MaxQueryBatchDigests
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
	// UnifiedUploads specifies whether the client uploads files in the background.
	UnifiedUploads UnifiedUploads
	// UnifiedUploadBufferSize specifies when the unified upload daemon flushes the pending requests.
	UnifiedUploadBufferSize UnifiedUploadBufferSize
	// UnifiedUploadTickDuration specifies how often the unified upload daemon flushes the pending requests.
	UnifiedUploadTickDuration UnifiedUploadTickDuration
	// UnifiedDownloads specifies whether the client downloads files in the background.
	UnifiedDownloads UnifiedDownloads
	// UnifiedDownloadBufferSize specifies when the unified download daemon flushes the pending requests.
	UnifiedDownloadBufferSize UnifiedDownloadBufferSize
	// UnifiedDownloadTickDuration specifies how often the unified download daemon flushes the pending requests.
	UnifiedDownloadTickDuration UnifiedDownloadTickDuration
	// TreeSymlinkOpts controls how symlinks are handled when constructing a tree.
	TreeSymlinkOpts *TreeSymlinkOpts

	serverCaps          *repb.ServerCapabilities
	useBatchOps         UseBatchOps
	casConcurrency      int64
	casUploaders        *semaphore.Weighted
	casUploadRequests   chan *uploadRequest
	casUploads          map[digest.Digest]*uploadState
	casDownloaders      *semaphore.Weighted
	casDownloadRequests chan *downloadRequest
	rpcTimeouts         RPCTimeouts
	creds               credentials.PerRPCCredentials
	uploadOnce          sync.Once
	downloadOnce        sync.Once
	useBatchCompression UseBatchCompression
}

const (
	// DefaultMaxBatchSize is the maximum size of a batch to upload with BatchWriteBlobs. We set it to slightly
	// below 4 MB, because that is the limit of a message size in gRPC
	DefaultMaxBatchSize = 4*1024*1024 - 1024

	// DefaultMaxBatchDigests is a suggested approximate limit based on current RBE implementation.
	// Above that BatchUpdateBlobs calls start to exceed a typical minute timeout.
	DefaultMaxBatchDigests = 4000

	// DefaultMaxQueryBatchDigests is a suggested limit for the number of items for in batch for a missing blobs query.
	DefaultMaxQueryBatchDigests = 10_000

	// DefaultDirMode is mode used to create directories.
	DefaultDirMode = 0777

	// DefaultExecutableMode is mode used to create executable files.
	DefaultExecutableMode = 0777

	// DefaultRegularMode is mode used to create non-executable files.
	DefaultRegularMode = 0644
)

// Close closes the underlying gRPC connection(s).
func (c *Client) Close() error {
	// Close the channels & stop background operations.
	UnifiedUploads(false).Apply(c)
	UnifiedDownloads(false).Apply(c)
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

// CompressedBytestreamThreshold is the threshold for compressing blobs when writing/reading.
// See comment in related field on the Client struct.
type CompressedBytestreamThreshold int64

// Apply sets the client's maximal chunk size s.
func (s CompressedBytestreamThreshold) Apply(c *Client) {
	c.CompressedBytestreamThreshold = s
}

// An UploadCompressionPredicate determines whether to compress a blob on upload.
// Note that the CompressedBytestreamThreshold takes priority over this (i.e. if the blob to be uploaded
// is smaller than the threshold, this will not be called).
type UploadCompressionPredicate func(*uploadinfo.Entry) bool

// Apply sets the client's compression predicate.
func (cc UploadCompressionPredicate) Apply(c *Client) {
	c.UploadCompressionPredicate = cc
}

// UtilizeLocality is to specify whether client downloads files utilizing disk access locality.
type UtilizeLocality bool

// Apply sets the client's UtilizeLocality.
func (s UtilizeLocality) Apply(c *Client) {
	c.UtilizeLocality = s
}

// UnifiedUploads is to specify whether client uploads files in the background, unifying operations between different actions.
type UnifiedUploads bool

// Apply sets the client's UnifiedUploads.
func (s UnifiedUploads) Apply(c *Client) {
	c.UnifiedUploads = s
}

// UnifiedUploadBufferSize is to tune when the daemon for UnifiedUploads flushes the pending requests.
type UnifiedUploadBufferSize int

// DefaultUnifiedUploadBufferSize is the default UnifiedUploadBufferSize.
const DefaultUnifiedUploadBufferSize = 10000

// Apply sets the client's UnifiedDownloadBufferSize.
func (s UnifiedUploadBufferSize) Apply(c *Client) {
	c.UnifiedUploadBufferSize = s
}

// UnifiedUploadTickDuration is to tune how often the daemon for UnifiedUploads flushes the pending requests.
type UnifiedUploadTickDuration time.Duration

// DefaultUnifiedUploadTickDuration is the default UnifiedUploadTickDuration.
const DefaultUnifiedUploadTickDuration = UnifiedUploadTickDuration(50 * time.Millisecond)

// Apply sets the client's UnifiedUploadTickDuration.
func (s UnifiedUploadTickDuration) Apply(c *Client) {
	c.UnifiedUploadTickDuration = s
}

// UnifiedDownloads is to specify whether client uploads files in the background, unifying operations between different actions.
type UnifiedDownloads bool

// Apply sets the client's UnifiedDownloads.
// Note: it is unsafe to change this property when connections are ongoing.
func (s UnifiedDownloads) Apply(c *Client) {
	c.UnifiedDownloads = s
}

// UnifiedDownloadBufferSize is to tune when the daemon for UnifiedDownloads flushes the pending requests.
type UnifiedDownloadBufferSize int

// DefaultUnifiedDownloadBufferSize is the default UnifiedDownloadBufferSize.
const DefaultUnifiedDownloadBufferSize = 10000

// Apply sets the client's UnifiedDownloadBufferSize.
func (s UnifiedDownloadBufferSize) Apply(c *Client) {
	c.UnifiedDownloadBufferSize = s
}

// UnifiedDownloadTickDuration is to tune how often the daemon for UnifiedDownloads flushes the pending requests.
type UnifiedDownloadTickDuration time.Duration

// DefaultUnifiedDownloadTickDuration is the default UnifiedDownloadTickDuration.
const DefaultUnifiedDownloadTickDuration = UnifiedDownloadTickDuration(50 * time.Millisecond)

// Apply sets the client's UnifiedDownloadTickDuration.
func (s UnifiedDownloadTickDuration) Apply(c *Client) {
	c.UnifiedDownloadTickDuration = s
}

// Apply sets the client's TreeSymlinkOpts.
func (o *TreeSymlinkOpts) Apply(c *Client) {
	c.TreeSymlinkOpts = o
}

// MaxBatchDigests is maximum amount of digests to batch in upload and download operations.
type MaxBatchDigests int

// Apply sets the client's maximal batch digests to s.
func (s MaxBatchDigests) Apply(c *Client) {
	c.MaxBatchDigests = s
}

// MaxQueryBatchDigests is maximum amount of digests to batch in query operations.
type MaxQueryBatchDigests int

// Apply sets the client's maximal batch digests to s.
func (s MaxQueryBatchDigests) Apply(c *Client) {
	c.MaxQueryBatchDigests = s
}

// MaxBatchSize is maximum size in bytes of a batch request for batch operations.
type MaxBatchSize int64

// Apply sets the client's maximum batch size to s.
func (s MaxBatchSize) Apply(c *Client) {
	c.MaxBatchSize = s
}

// DirMode is mode used to create directories.
type DirMode os.FileMode

// Apply sets the client's DirMode to m.
func (m DirMode) Apply(c *Client) {
	c.DirMode = os.FileMode(m)
}

// ExecutableMode is mode used to create executable files.
type ExecutableMode os.FileMode

// Apply sets the client's ExecutableMode to m.
func (m ExecutableMode) Apply(c *Client) {
	c.ExecutableMode = os.FileMode(m)
}

// RegularMode is mode used to create non-executable files.
type RegularMode os.FileMode

// Apply sets the client's RegularMode to m.
func (m RegularMode) Apply(c *Client) {
	c.RegularMode = os.FileMode(m)
}

// UseBatchOps can be set to true to use batch CAS operations when uploading multiple blobs, or
// false to always use individual ByteStream requests.
type UseBatchOps bool

// Apply sets the UseBatchOps flag on a client.
func (u UseBatchOps) Apply(c *Client) {
	c.useBatchOps = u
}

// UseBatchCompression is currently set to true when the server has
// SupportedBatchUpdateCompressors capability and supports ZSTD compression.
type UseBatchCompression bool

// Apply sets the batchCompression flag on a client.
func (u UseBatchCompression) Apply(c *Client) {
	c.useBatchCompression = u
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
	c.casConcurrency = int64(cy)
	c.casUploaders = semaphore.NewWeighted(c.casConcurrency)
	c.casDownloaders = semaphore.NewWeighted(c.casConcurrency)
}

// StartupCapabilities controls whether the client should attempt to fetch the remote
// server capabilities on New. If set to true, some configuration such as MaxBatchSize
// is set according to the remote server capabilities instead of using the provided values.
type StartupCapabilities bool

// Apply sets the StartupCapabilities flag on a client.
func (s StartupCapabilities) Apply(c *Client) {
	c.StartupCapabilities = s
}

// LegacyExecRootRelativeOutputs controls whether the client uses legacy behavior of
// treating output paths as relative to the exec root instead of the working directory.
type LegacyExecRootRelativeOutputs bool

// Apply sets the LegacyExecRootRelativeOutputs flag on a client.
func (l LegacyExecRootRelativeOutputs) Apply(c *Client) {
	c.LegacyExecRootRelativeOutputs = l
}

// PerRPCCreds sets per-call options that will be set on all RPCs to the underlying connection.
type PerRPCCreds struct {
	Creds credentials.PerRPCCredentials
}

// Apply saves the per-RPC creds in the Client.
func (p *PerRPCCreds) Apply(c *Client) {
	c.creds = p.Creds
}

// UseCASNG is a feature flag for the casng package.
type UseCASNG bool

// Apply sets the feature flag value in the Client.
func (o UseCASNG) Apply(c *Client) {
	c.useCasNg = bool(o)
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

func getRPCCreds(ctx context.Context, credFile string, useApplicationDefault bool, useComputeEngine bool) (credentials.PerRPCCredentials, AuthType, error) {
	if useApplicationDefault {
		c, err := oauth.NewApplicationDefault(ctx, scopes)
		return c, ApplicationDefaultCredsAuth, err
	}
	if useComputeEngine {
		return oauth.NewComputeEngine(), GCECredsAuth, nil
	}
	rpcCreds, err := oauth.NewServiceAccountFromFile(credFile, scopes)
	if err != nil {
		return nil, CredsFileAuth, fmt.Errorf("couldn't create RPC creds from %s: %v", credFile, err)
	}
	return rpcCreds, CredsFileAuth, nil
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

	// UseExternalAuthToken indicates whether an externally specified auth token should be used.
	// If set to true, ExternalPerRPCCreds should also be non-nil.
	UseExternalAuthToken bool

	// ExternalPerRPCCreds refers to the per RPC credentials that should be used for each RPC.
	ExternalPerRPCCreds *PerRPCCreds

	// CredFile is the JSON file that contains the credentials for RPCs.
	CredFile string

	// ActAsAccount is the service account to act as when making RPC calls.
	ActAsAccount string

	// NoSecurity is true if there is no security: no credentials are configured
	// (NoAuth is implied) and grpc.WithInsecure() is passed in. Should only be
	// used in test code.
	NoSecurity bool

	// NoAuth is true if TLS is enabled (NoSecurity is false) but the client does
	// not need to authenticate with the server.
	NoAuth bool

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
			{
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
		ca, err := os.ReadFile(params.TLSCACertFile)
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
func Dial(ctx context.Context, endpoint string, params DialParams) (*grpc.ClientConn, AuthType, error) {
	var authUsed AuthType

	var opts []grpc.DialOption
	opts = append(opts, params.DialOpts...)

	if params.MaxConcurrentRequests == 0 {
		params.MaxConcurrentRequests = DefaultMaxConcurrentRequests
	}
	if params.MaxConcurrentStreams == 0 {
		params.MaxConcurrentStreams = DefaultMaxConcurrentStreams
	}
	if params.NoSecurity {
		authUsed = NoAuth
		opts = append(opts, grpc.WithInsecure())
	} else if params.NoAuth {
		authUsed = NoAuth
		// Set the ServerName and RootCAs fields, if needed.
		tlsConfig, err := createTLSConfig(params)
		if err != nil {
			return nil, authUsed, fmt.Errorf("could not create TLS config: %v", err)
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	} else if params.UseExternalAuthToken {
		authUsed = ExternalTokenAuth
		if params.ExternalPerRPCCreds == nil {
			return nil, authUsed, fmt.Errorf("ExternalPerRPCCreds unspecified when using external auth token mechanism")
		}
		opts = append(opts, grpc.WithPerRPCCredentials(params.ExternalPerRPCCreds.Creds))
		// Set the ServerName and RootCAs fields, if needed.
		tlsConfig, err := createTLSConfig(params)
		if err != nil {
			return nil, authUsed, fmt.Errorf("could not create TLS config: %v", err)
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	} else {
		credFile := params.CredFile
		if strings.Contains(credFile, HomeDirMacro) {
			authUsed = CredsFileAuth
			usr, err := user.Current()
			if err != nil {
				return nil, authUsed, fmt.Errorf("could not fetch home directory because of error determining current user: %v", err)
			}
			credFile = strings.Replace(credFile, HomeDirMacro, usr.HomeDir, -1 /* no limit */)
		}

		if !params.TransportCredsOnly {
			var (
				rpcCreds credentials.PerRPCCredentials
				err      error
			)
			rpcCreds, authUsed, err = getRPCCreds(ctx, credFile, params.UseApplicationDefault, params.UseComputeEngine)
			if err != nil {
				return nil, authUsed, fmt.Errorf("couldn't create RPC creds for %s: %v", scopes, err)
			}

			if params.ActAsAccount != "" {
				rpcCreds = getImpersonatedRPCCreds(ctx, params.ActAsAccount, rpcCreds)
			}

			opts = append(opts, grpc.WithPerRPCCredentials(rpcCreds))
		}
		tlsConfig, err := createTLSConfig(params)
		if err != nil {
			return nil, authUsed, fmt.Errorf("could not create TLS config: %v", err)
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	}
	grpcInt := createGRPCInterceptor(params)
	opts = append(opts, grpc.WithDisableServiceConfig())
	opts = append(opts, grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"loadBalancingConfig": [{"%s":{}}]}`, balancer.Name)))
	opts = append(opts, grpc.WithUnaryInterceptor(grpcInt.GCPUnaryClientInterceptor))
	opts = append(opts, grpc.WithStreamInterceptor(grpcInt.GCPStreamClientInterceptor))

	conn, err := grpc.Dial(endpoint, opts...)
	if err != nil {
		return nil, authUsed, fmt.Errorf("couldn't dial gRPC %q: %v", endpoint, err)
	}
	return conn, authUsed, nil
}

// DialRaw dials a remote execution service and returns the grpc connection that is established.
// TODO(olaola): remove this overload when all clients use Dial.
func DialRaw(ctx context.Context, params DialParams) (*grpc.ClientConn, AuthType, error) {
	if params.Service == "" {
		return nil, UnknownAuth, fmt.Errorf("service needs to be specified")
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
		return nil, &InitError{Err: fmt.Errorf("service needs to be specified")}
	}
	log.Infof("Connecting to remote execution instance %s", instanceName)
	log.Infof("Connecting to remote execution service %s", params.Service)
	conn, authUsed, err := Dial(ctx, params.Service, params)
	casConn := conn
	if params.CASService != "" && params.CASService != params.Service {
		log.Infof("Connecting to CAS service %s", params.CASService)
		casConn, authUsed, err = Dial(ctx, params.CASService, params)
	}
	if err != nil {
		return nil, &InitError{Err: statusWrap(err), AuthUsed: authUsed}
	}
	client, err := NewClientFromConnection(ctx, instanceName, conn, casConn, opts...)
	if err != nil {
		return nil, &InitError{Err: err, AuthUsed: authUsed}
	}
	return client, nil
}

// NewClientFromConnection creates a client from gRPC connections to a remote execution service and a cas service.
func NewClientFromConnection(ctx context.Context, instanceName string, conn, casConn *grpc.ClientConn, opts ...Opt) (*Client, error) {
	if conn == nil {
		return nil, fmt.Errorf("connection to remote execution service may not be nil")
	}
	if casConn == nil {
		return nil, fmt.Errorf("connection to CAS service may not be nil")
	}
	client := &Client{
		InstanceName:                  instanceName,
		actionCache:                   regrpc.NewActionCacheClient(casConn),
		byteStream:                    bsgrpc.NewByteStreamClient(casConn),
		cas:                           regrpc.NewContentAddressableStorageClient(casConn),
		execution:                     regrpc.NewExecutionClient(conn),
		operations:                    opgrpc.NewOperationsClient(conn),
		rpcTimeouts:                   DefaultRPCTimeouts,
		Connection:                    conn,
		CASConnection:                 casConn,
		CompressedBytestreamThreshold: DefaultCompressedBytestreamThreshold,
		ChunkMaxSize:                  chunker.DefaultChunkSize,
		MaxBatchDigests:               DefaultMaxBatchDigests,
		MaxQueryBatchDigests:          DefaultMaxQueryBatchDigests,
		MaxBatchSize:                  DefaultMaxBatchSize,
		DirMode:                       DefaultDirMode,
		ExecutableMode:                DefaultExecutableMode,
		RegularMode:                   DefaultRegularMode,
		useBatchOps:                   true,
		StartupCapabilities:           true,
		LegacyExecRootRelativeOutputs: false,
		casConcurrency:                DefaultCASConcurrency,
		casUploaders:                  semaphore.NewWeighted(DefaultCASConcurrency),
		casDownloaders:                semaphore.NewWeighted(DefaultCASConcurrency),
		casUploads:                    make(map[digest.Digest]*uploadState),
		UnifiedUploadTickDuration:     DefaultUnifiedUploadTickDuration,
		UnifiedUploadBufferSize:       DefaultUnifiedUploadBufferSize,
		UnifiedDownloadTickDuration:   DefaultUnifiedDownloadTickDuration,
		UnifiedDownloadBufferSize:     DefaultUnifiedDownloadBufferSize,
		Retrier:                       RetryTransient(),
		useCasNg:                      false,
	}
	for _, o := range opts {
		o.Apply(client)
	}
	if client.StartupCapabilities {
		if err := client.CheckCapabilities(ctx); err != nil {
			return nil, statusWrap(err)
		}
	}
	if client.casConcurrency < 1 {
		return nil, fmt.Errorf("CASConcurrency should be at least 1")
	}
	if client.useCasNg {
		queryCfg := casng.GRPCConfig{
			ConcurrentCallsLimit: int(client.casConcurrency),
			BytesLimit:           int(client.MaxBatchSize),
			ItemsLimit:           int(client.MaxQueryBatchDigests),
			BundleTimeout:        10 * time.Millisecond, // Low value to fast track queries.
			// Timeout:              DefaultRPCTimeouts["FindMissingBlobs"],
			Timeout:        DefaultRPCTimeouts["default"],
			RetryPolicy:    client.Retrier.Backoff,
			RetryPredicate: client.Retrier.ShouldRetry,
		}
		batchCfg := casng.GRPCConfig{
			ConcurrentCallsLimit: int(client.casConcurrency),
			BytesLimit:           int(client.MaxBatchSize),
			ItemsLimit:           int(client.UnifiedUploadBufferSize),
			BundleTimeout:        time.Duration(client.UnifiedUploadTickDuration), // Low value to fast track queries.
			Timeout:              DefaultRPCTimeouts["BatchUpdateBlobs"],
			RetryPolicy:          client.Retrier.Backoff,
			RetryPredicate:       client.Retrier.ShouldRetry,
		}
		streamCfg := casng.GRPCConfig{
			ConcurrentCallsLimit: int(client.casConcurrency),
			BytesLimit:           1,                // Unused.
			ItemsLimit:           1,                // Unused.
			BundleTimeout:        time.Millisecond, // Unused.
			Timeout:              DefaultRPCTimeouts["default"],
			RetryPolicy:          client.Retrier.Backoff,
			RetryPredicate:       client.Retrier.ShouldRetry,
		}
		ioCfg := casng.IOConfig{
			ConcurrentWalksLimit:     int(client.casConcurrency),
			OpenFilesLimit:           casng.DefaultOpenFilesLimit,
			OpenLargeFilesLimit:      casng.DefaultOpenLargeFilesLimit,
			SmallFileSizeThreshold:   casng.DefaultSmallFileSizeThreshold,
			LargeFileSizeThreshold:   casng.DefaultLargeFileSizeThreshold,
			CompressionSizeThreshold: int64(client.CompressedBytestreamThreshold),
			BufferSize:               int(client.ChunkMaxSize),
		}
		if client.CompressedBytestreamThreshold < 0 {
			ioCfg.CompressionSizeThreshold = math.MaxInt64
		}
		var err error
		client.ngCasUploader, err = casng.NewBatchingUploader(ctx, client.cas, client.byteStream, instanceName, queryCfg, batchCfg, streamCfg, ioCfg)
		if err != nil {
			return nil, fmt.Errorf("error initializing CASNG: %w", err)
		}
	}
	client.RunBackgroundTasks(ctx)
	return client, nil
}

// RunBackgroundTasks starts background goroutines for the client.
func (c *Client) RunBackgroundTasks(ctx context.Context) {
	if c.UnifiedUploads {
		c.uploadOnce.Do(func() {
			c.casUploadRequests = make(chan *uploadRequest, c.UnifiedUploadBufferSize)
			go c.uploadProcessor(ctx)
		})
	}
	if c.UnifiedDownloads {
		c.downloadOnce.Do(func() {
			c.casDownloadRequests = make(chan *downloadRequest, c.UnifiedDownloadBufferSize)
			go c.downloadProcessor(ctx)
		})
	}
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

// DefaultRPCTimeouts contains the default timeout of various RPC calls to RBE.
var DefaultRPCTimeouts = map[string]time.Duration{
	"default":          20 * time.Second,
	"GetCapabilities":  5 * time.Second,
	"BatchUpdateBlobs": time.Minute,
	"BatchReadBlobs":   time.Minute,
	"GetTree":          time.Minute,
	// Note: due to an implementation detail, WaitExecution will use the same
	// per-RPC timeout as Execute. It is extremely ill-advised to set the Execute
	// timeout at above 0; most users should use the Action Timeout instead.
	"Execute":       0,
	"WaitExecution": 0,
}

// ResourceName constructs a correctly formatted resource name as defined in the spec.
// No keyword validation is performed since the semantics of the path are defined by the server.
// See: https://github.com/bazelbuild/remote-apis/blob/cb8058798964f0adf6dbab2f4c2176ae2d653447/build/bazel/remote/execution/v2/remote_execution.proto#L223
func (c *Client) ResourceName(segments ...string) (string, error) {
	segs := make([]string, 0, len(segments)+1)
	if c.InstanceName != "" {
		segs = append(segs, c.InstanceName)
	}
	for _, s := range segments {
		if s == "" {
			return "", ErrEmptySegment
		}
		segs = append(segs, s)
	}
	return strings.Join(segs, "/"), nil
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
		Backoff:     retry.ExponentialBackoff(225*time.Millisecond, 2*time.Second, retry.Attempts(6)),
		ShouldRetry: retry.TransientOnly,
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
		return nil, statusWrap(err)
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
		return nil, statusWrap(err)
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
		return nil, statusWrap(err)
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
		return nil, statusWrap(err)
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
		return nil, statusWrap(err)
	}
	return res, nil
}

// BatchReadBlobs wraps the underlying call with specific client options.
// NOTE that its retry logic ignores the per-blob errors embedded in the response.
// It is recommended to use BatchDownloadBlobs instead.
func (c *Client) BatchReadBlobs(ctx context.Context, req *repb.BatchReadBlobsRequest) (res *repb.BatchReadBlobsResponse, err error) {
	opts := c.RPCOpts()
	err = c.Retrier.Do(ctx, func() (e error) {
		return c.CallWithTimeout(ctx, "BatchReadBlobs", func(ctx context.Context) (e error) {
			res, e = c.cas.BatchReadBlobs(ctx, req, opts...)
			return e
		})
	})
	if err != nil {
		return nil, statusWrap(err)
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
		return nil, statusWrap(err)
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
		return nil, statusWrap(err)
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
		return nil, statusWrap(err)
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
		return nil, statusWrap(err)
	}
	return res, nil
}

// gRPC errors are incompatible with simple wraps. See
// https://github.com/grpc/grpc-go/issues/3115
func statusWrap(err error) error {
	if st, ok := status.FromError(err); ok {
		return status.Errorf(st.Code(), errors.WithStack(err).Error())
	}
	return errors.WithStack(err)
}
