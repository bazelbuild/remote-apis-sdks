// Package client contains a high-level remote execution client library.
package client

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os/user"
	"strings"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/actas"
	"github.com/bazelbuild/remote-apis-sdks/go/retry"
	log "github.com/golang/glog"

	"golang.org/x/oauth2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/oauth"
	"google.golang.org/grpc/status"

	regrpc "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	emptypb "github.com/golang/protobuf/ptypes/empty"
	bsgrpc "google.golang.org/genproto/googleapis/bytestream"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	opgrpc "google.golang.org/genproto/googleapis/longrunning"
	oppb "google.golang.org/genproto/googleapis/longrunning"
)

const (
	// DefaultMaxWriteChunkSize is the default max chunk size for ByteStream.Write RPCs.
	DefaultMaxWriteChunkSize = 1024 * 1024

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
	InstanceName   string
	actionCache    regrpc.ActionCacheClient
	byteStream     bsgrpc.ByteStreamClient
	cas            regrpc.ContentAddressableStorageClient
	execution      regrpc.ExecutionClient
	capabilities   regrpc.CapabilitiesClient
	operations     opgrpc.OperationsClient
	retrier        *Retrier
	chunkMaxSize   ChunkMaxSize
	useBatchOps    UseBatchOps
	casConcurrency CASConcurrency
	rpcTimeout     time.Duration
	creds          credentials.PerRPCCredentials
	// Used to close the underlying connection.
	io.Closer
}

// Opt is an option that can be passed to Dial in order to configure the behaviour of the client.
type Opt interface {
	Apply(*Client)
}

// ChunkMaxSize is maximum chunk size to use in Bytestream wrappers.
type ChunkMaxSize int

// Apply sets the client's maximal chunk size s.
func (s ChunkMaxSize) Apply(c *Client) {
	c.chunkMaxSize = s
}

// UseBatchOps can be set to true to use batch CAS operations when uploading multiple blobs, or
// false to always use individual ByteStream requests.
type UseBatchOps bool

// Apply sets the UseBatchOps flag on a client.
func (u UseBatchOps) Apply(c *Client) {
	c.useBatchOps = u
}

// CASConcurrency is the number of simultaneous requests that will be issued for batch CAS upload an
// download operations. It is a per-operation limit, not a global one, so N separate calls to CAS
// methods can result in N * CASConcurrency requests in flight at once.
type CASConcurrency int

// Apply sets the CASConcurrency flag on a client.
func (cy CASConcurrency) Apply(c *Client) {
	c.casConcurrency = cy
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
}

// DialRaw dials a remote execution service and returns the grpc connection that is established.
func DialRaw(ctx context.Context, params DialParams) (*grpc.ClientConn, error) {
	var opts []grpc.DialOption = []grpc.DialOption{grpc.WithWaitForHandshake()}

	if params.Service == "" {
		return nil, fmt.Errorf("service needs to be specified")
	}
	log.Infof("Connecting to remote execution service %s", params.Service)
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

		tlsCreds := credentials.NewClientTLSFromCert(nil, "")
		opts = append(opts, grpc.WithTransportCredentials(tlsCreds))
	}

	conn, err := grpc.Dial(params.Service, opts...)
	if err != nil {
		return nil, fmt.Errorf("couldn't dial gRPC %q: %v", params.Service, err)
	}
	return conn, nil
}

// Dial dials a remote execution service and returns a client suitable for higher-level
// functionality.
func Dial(ctx context.Context, instanceName string, params DialParams, opts ...Opt) (*Client, error) {
	conn, err := DialRaw(ctx, params)
	if err != nil {
		return nil, err
	}
	return NewClient(conn, instanceName, opts...)
}

// NewClient creates a client from an existing gRPC connection.
func NewClient(conn *grpc.ClientConn, instanceName string, opts ...Opt) (*Client, error) {
	if instanceName == "" {
		return nil, fmt.Errorf("instance needs to be specified")
	}
	log.Infof("Connecting to remote execution instance %s", instanceName)
	client := &Client{
		InstanceName:   instanceName,
		actionCache:    regrpc.NewActionCacheClient(conn),
		byteStream:     bsgrpc.NewByteStreamClient(conn),
		cas:            regrpc.NewContentAddressableStorageClient(conn),
		execution:      regrpc.NewExecutionClient(conn),
		capabilities:   regrpc.NewCapabilitiesClient(conn),
		operations:     opgrpc.NewOperationsClient(conn),
		rpcTimeout:     time.Minute,
		Closer:         conn,
		chunkMaxSize:   DefaultMaxWriteChunkSize,
		useBatchOps:    true,
		casConcurrency: 10,
	}
	for _, o := range opts {
		o.Apply(client)
	}
	return client, nil
}

// RPCTimeout is a Opt that sets the per-RPC deadline.
// NOTE that the deadline is only applied to non-streaming calls.
// The default timeout value is 1 minute.
type RPCTimeout time.Duration

// Apply applies the timeout to a Client.
func (d RPCTimeout) Apply(c *Client) {
	c.rpcTimeout = time.Duration(d)
}

func (c *Client) rpcOpts() []grpc.CallOption {
	if c.creds == nil {
		return nil
	}
	return []grpc.CallOption{grpc.PerRPCCredentials(c.creds)}
}

func (c *Client) callWithTimeout(ctx context.Context, f func(ctx context.Context) error) error {
	childCtx, cancel := context.WithTimeout(ctx, c.rpcTimeout)
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
	c.retrier = r
}

// do executes f() with retries.
// It can be called with a nil receiver; in that case no retries are done (just a passthrough call
// to f()).
func (r *Retrier) do(ctx context.Context, f func() error) error {
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
	opts := c.rpcOpts()
	err = c.retrier.do(ctx, func() (e error) {
		return c.callWithTimeout(ctx, func(ctx context.Context) (e error) {
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
	opts := c.rpcOpts()
	err = c.retrier.do(ctx, func() (e error) {
		return c.callWithTimeout(ctx, func(ctx context.Context) (e error) {
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
func (c *Client) Read(ctx context.Context, req *bspb.ReadRequest) (res bsgrpc.ByteStream_ReadClient, err error) {
	opts := c.rpcOpts()
	err = c.retrier.do(ctx, func() (e error) {
		res, e = c.byteStream.Read(ctx, req, opts...)
		return e
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Write wraps the underlying call with specific client options.
func (c *Client) Write(ctx context.Context) (res bsgrpc.ByteStream_WriteClient, err error) {
	opts := c.rpcOpts()
	err = c.retrier.do(ctx, func() (e error) {
		res, e = c.byteStream.Write(ctx, opts...)
		return e
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// QueryWriteStatus wraps the underlying call with specific client options.
func (c *Client) QueryWriteStatus(ctx context.Context, req *bspb.QueryWriteStatusRequest) (res *bspb.QueryWriteStatusResponse, err error) {
	opts := c.rpcOpts()
	err = c.retrier.do(ctx, func() (e error) {
		return c.callWithTimeout(ctx, func(ctx context.Context) (e error) {
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
	opts := c.rpcOpts()
	err = c.retrier.do(ctx, func() (e error) {
		return c.callWithTimeout(ctx, func(ctx context.Context) (e error) {
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
	opts := c.rpcOpts()
	err = c.retrier.do(ctx, func() (e error) {
		return c.callWithTimeout(ctx, func(ctx context.Context) (e error) {
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
	opts := c.rpcOpts()
	err = c.retrier.do(ctx, func() (e error) {
		return c.callWithTimeout(ctx, func(ctx context.Context) (e error) {
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
func (c *Client) GetTree(ctx context.Context, req *repb.GetTreeRequest) (res regrpc.ContentAddressableStorage_GetTreeClient, err error) {
	opts := c.rpcOpts()
	err = c.retrier.do(ctx, func() (e error) {
		res, e = c.cas.GetTree(ctx, req, opts...)
		return e
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Execute wraps the underlying call with specific client options.
func (c *Client) Execute(ctx context.Context, req *repb.ExecuteRequest) (res regrpc.Execution_ExecuteClient, err error) {
	opts := c.rpcOpts()
	err = c.retrier.do(ctx, func() (e error) {
		res, e = c.execution.Execute(ctx, req, opts...)
		return e
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// WaitExecution wraps the underlying call with specific client options.
func (c *Client) WaitExecution(ctx context.Context, req *repb.WaitExecutionRequest) (res regrpc.Execution_ExecuteClient, err error) {
	opts := c.rpcOpts()
	err = c.retrier.do(ctx, func() (e error) {
		res, e = c.execution.WaitExecution(ctx, req, opts...)
		return e
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// GetCapabilities wraps the underlying call with specific client options.
func (c *Client) GetCapabilities(ctx context.Context, req *repb.GetCapabilitiesRequest) (res *repb.ServerCapabilities, err error) {
	opts := c.rpcOpts()
	err = c.retrier.do(ctx, func() (e error) {
		return c.callWithTimeout(ctx, func(ctx context.Context) (e error) {
			res, e = c.capabilities.GetCapabilities(ctx, req, opts...)
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
	opts := c.rpcOpts()
	err = c.retrier.do(ctx, func() (e error) {
		return c.callWithTimeout(ctx, func(ctx context.Context) (e error) {
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
	opts := c.rpcOpts()
	err = c.retrier.do(ctx, func() (e error) {
		return c.callWithTimeout(ctx, func(ctx context.Context) (e error) {
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
	opts := c.rpcOpts()
	err = c.retrier.do(ctx, func() (e error) {
		return c.callWithTimeout(ctx, func(ctx context.Context) (e error) {
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
	opts := c.rpcOpts()
	err = c.retrier.do(ctx, func() (e error) {
		return c.callWithTimeout(ctx, func(ctx context.Context) (e error) {
			res, e = c.operations.DeleteOperation(ctx, req, opts...)
			return e
		})
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}
