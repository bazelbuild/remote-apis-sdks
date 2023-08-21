// Package flags provides a convenient way to initialize the remote client from flags.
package flags

import (
	"context"
	"flag"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/balancer"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/client"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/moreflag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	log "github.com/golang/glog"
)

var (
	// The flags credential_file, use_application_default_credentials, and use_gce_credentials
	// determine the client identity that is used to authenticate with remote execution.
	// One of the following must be true for client authentication to work, and they are used in
	// this order of preference:
	// - the use_application_default_credentials flag must be set to true, or
	// - the use_gce_credentials must be set to true, or
	// - the credential_file flag must be set to point to a valid credential file

	// CredFile is the name of a file that contains service account credentials to use when calling
	// remote execution. Used only if --use_application_default_credentials and --use_gce_credentials
	// are false.
	CredFile = flag.String("credential_file", "", "The name of a file that contains service account credentials to use when calling remote execution. Used only if --use_application_default_credentials and --use_gce_credentials are false.")
	// UseApplicationDefaultCreds is whether to use application default credentials to connect to
	// remote execution. See
	// https://cloud.google.com/sdk/gcloud/reference/auth/application-default/login
	UseApplicationDefaultCreds = flag.Bool("use_application_default_credentials", false, "If true, use application default credentials to connect to remote execution. See https://cloud.google.com/sdk/gcloud/reference/auth/application-default/login")
	// UseGCECredentials is whether to use the default GCE credentials to authenticate with remote
	// execution. --use_application_default_credentials must be false.
	UseGCECredentials = flag.Bool("use_gce_credentials", false, "If true (and --use_application_default_credentials is false), use the default GCE credentials to authenticate with remote execution.")
	// UseRPCCredentials can be set to false to disable all per-RPC credentials.
	UseRPCCredentials = flag.Bool("use_rpc_credentials", true, "If false, no per-RPC credentials will be used (disables --credential_file, --use_application_default_credentials, and --use_gce_credentials.")
	// UseExternalAuthToken specifies whether to use an externally provided auth token, given via PerRPCCreds dial option, should be used.
	UseExternalAuthToken = flag.Bool("use_external_auth_token", false, "If true, se an externally provided auth token, given via PerRPCCreds when the SDK is initialized.")
	// Service represents the host (and, if applicable, port) of the remote execution service.
	Service = flag.String("service", "", "The remote execution service to dial when calling via gRPC, including port, such as 'localhost:8790' or 'remotebuildexecution.googleapis.com:443'")
	// ServiceNoSecurity can be set to connect to the gRPC service without TLS and without authentication (enables --service_no_auth).
	ServiceNoSecurity = flag.Bool("service_no_security", false, "If true, do not use TLS or authentication when connecting to the gRPC service.")
	// ServiceNoAuth can be set to disable authentication while still using TLS.
	ServiceNoAuth = flag.Bool("service_no_auth", false, "If true, do not authenticate with the service (implied by --service_no_security).")
	// CASService represents the host (and, if applicable, port) of the CAS service, if different from the remote execution service.
	CASService = flag.String("cas_service", "", "The CAS service to dial when calling via gRPC, including port, such as 'localhost:8790' or 'remotebuildexecution.googleapis.com:443'")
	// Instance gives the instance of remote execution to test (in
	// projects/[PROJECT_ID]/instances/[INSTANCE_NAME] format for Google RBE).
	Instance = flag.String("instance", "", "The instance ID to target when calling remote execution via gRPC (e.g., projects/$PROJECT/instances/default_instance for Google RBE).")
	// CASConcurrency specifies the maximum number of concurrent upload & download RPCs that can be in flight.
	CASConcurrency = flag.Int("cas_concurrency", client.DefaultCASConcurrency, "Num concurrent upload / download RPCs that the SDK is allowed to do.")
	// MaxConcurrentRequests denotes the maximum number of concurrent RPCs on a single gRPC connection.
	MaxConcurrentRequests = flag.Uint("max_concurrent_requests_per_conn", client.DefaultMaxConcurrentRequests, "Maximum number of concurrent RPCs on a single gRPC connection.")
	// MaxConcurrentStreams denotes the maximum number of concurrent stream RPCs on a single gRPC connection.
	MaxConcurrentStreams = flag.Uint("max_concurrent_streams_per_conn", client.DefaultMaxConcurrentStreams, "Maximum number of concurrent stream RPCs on a single gRPC connection.")
	// TLSServerName overrides the server name sent in the TLS session.
	TLSServerName = flag.String("tls_server_name", "", "Override the TLS server name")
	// TLSCACert loads CA certificates from a file
	TLSCACert = flag.String("tls_ca_cert", "", "Load TLS CA certificates from this file")
	// TLSClientAuthCert sets the public key in PEM format for using mTLS auth to connect to the RBE service.
	TLSClientAuthCert = flag.String("tls_client_auth_cert", "", "Certificate to use when using mTLS to connect to the RBE service.")
	// TLSClientAuthKey sets the private key for using mTLS auth to connect to the RBE service.
	TLSClientAuthKey = flag.String("tls_client_auth_key", "", "Key to use when using mTLS to connect to the RBE service.")
	// StartupCapabilities specifies whether to self-configure based on remote server capabilities on startup.
	StartupCapabilities = flag.Bool("startup_capabilities", true, "Whether to self-configure based on remote server capabilities on startup.")
	// RPCTimeouts stores the per-RPC timeout values.
	RPCTimeouts map[string]string
	// KeepAliveTime specifies gRPCs keepalive time parameter.
	KeepAliveTime = flag.Duration("grpc_keepalive_time", 0*time.Second, "After a duration of this time if the client doesn't see any activity it pings the server to see if the transport is still alive. If zero or not set, the mechanism is off.")
	// KeepAliveTimeout specifies gRPCs keepalive timeout parameter.
	KeepAliveTimeout = flag.Duration("grpc_keepalive_timeout", 20*time.Second, "After having pinged for keepalive check, the client waits for a duration of Timeout and if no activity is seen even after that the connection is closed. Default is 20s.")
	// KeepAlivePermitWithoutStream specifies gRPCs keepalive permitWithoutStream parameter.
	KeepAlivePermitWithoutStream = flag.Bool("grpc_keepalive_permit_without_stream", false, "If true, client sends keepalive pings even with no active RPCs; otherwise, doesn't send pings even if time and timeout are set. Default is false.")
)

func init() {
	// MinConnections denotes the minimum number of gRPC sub-connections the gRPC balancer should create during SDK initialization.
	flag.IntVar(&balancer.MinConnections, "min_grpc_connections", balancer.DefaultMinConnections, "Minimum number of gRPC sub-connections the gRPC balancer should create during SDK initialization.")
	// RPCTimeouts stores the per-RPC timeout values. The flag allows users to override the defaults
	// set in client.DefaultRPCTimeouts. This is in order to not force the users to familiarize
	// themselves with every RPC, otherwise it is easy to accidentally enforce a timeout on
	// WaitExecution, for example.
	flag.Var((*moreflag.StringMapValue)(&RPCTimeouts), "rpc_timeouts", "Comma-separated key value pairs in the form rpc_name=timeout. The key for default RPC is named default. 0 indicates no timeout. Example: GetActionResult=500ms,Execute=0,default=10s.")
}

// NewClientFromFlags connects to a remote execution service and returns a client suitable for higher-level
// functionality. It uses the flags from above to configure the connection to remote execution.
func NewClientFromFlags(ctx context.Context, opts ...client.Opt) (*client.Client, error) {
	opts = append(opts, []client.Opt{client.CASConcurrency(*CASConcurrency), client.StartupCapabilities(*StartupCapabilities)}...)
	if len(RPCTimeouts) > 0 {
		timeouts := make(map[string]time.Duration)
		for rpc, d := range client.DefaultRPCTimeouts {
			timeouts[rpc] = d
		}
		// Override the defaults with flags, but do not replace.
		for rpc, s := range RPCTimeouts {
			d, err := time.ParseDuration(s)
			if err != nil {
				return nil, err
			}
			timeouts[rpc] = d
		}
		opts = append(opts, client.RPCTimeouts(timeouts))
	}
	var perRPCCreds *client.PerRPCCreds
	for _, opt := range opts {
		switch opt.(type) {
		case *client.PerRPCCreds:
			perRPCCreds = (opt).(*(client.PerRPCCreds))
		}
	}

	dialOpts := make([]grpc.DialOption, 0)
	if *KeepAliveTime > 0*time.Second {
		params := keepalive.ClientParameters{
			Time:                *KeepAliveTime,
			Timeout:             *KeepAliveTimeout,
			PermitWithoutStream: *KeepAlivePermitWithoutStream,
		}
		log.V(1).Infof("KeepAlive params = %v", params)
		dialOpts = append(dialOpts, grpc.WithKeepaliveParams(params))
	}
	return client.NewClient(ctx, *Instance, client.DialParams{
		Service:               *Service,
		NoSecurity:            *ServiceNoSecurity,
		NoAuth:                *ServiceNoAuth,
		CASService:            *CASService,
		CredFile:              *CredFile,
		DialOpts:              dialOpts,
		UseApplicationDefault: *UseApplicationDefaultCreds,
		UseComputeEngine:      *UseGCECredentials,
		UseExternalAuthToken:  *UseExternalAuthToken,
		ExternalPerRPCCreds:   perRPCCreds,
		TransportCredsOnly:    !*UseRPCCredentials,
		TLSServerName:         *TLSServerName,
		TLSCACertFile:         *TLSCACert,
		TLSClientAuthCert:     *TLSClientAuthCert,
		TLSClientAuthKey:      *TLSClientAuthKey,
		MaxConcurrentRequests: uint32(*MaxConcurrentRequests),
		MaxConcurrentStreams:  uint32(*MaxConcurrentStreams),
	}, opts...)
}
