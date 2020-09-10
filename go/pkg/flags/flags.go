// Package flags provides a convenient way to initialize the remote client from flags.
package flags

import (
	"context"
	"flag"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/balancer"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/client"
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
	// Service represents the host (and, if applicable, port) of the remote execution service.
	Service = flag.String("service", "", "The remote execution service to dial when calling via gRPC, including port, such as 'localhost:8790' or 'remotebuildexecution.googleapis.com:443'")
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
)

func init() {
	// MinConnections denotes the minimum number of gRPC sub-connections the gRPC balancer should create during SDK initialization.
	flag.IntVar(&balancer.MinConnections, "min_grpc_connections", balancer.DefaultMinConnections, "Minimum number of gRPC sub-connections the gRPC balancer should create during SDK initialization.")
}

// NewClientFromFlags connects to a remote execution service and returns a client suitable for higher-level
// functionality. It uses the flags from above to configure the connection to remote execution.
func NewClientFromFlags(ctx context.Context, opts ...client.Opt) (*client.Client, error) {
	opts = append(opts, client.CASConcurrency(*CASConcurrency))
	return client.NewClient(ctx, *Instance, client.DialParams{
		Service:               *Service,
		CASService:            *CASService,
		CredFile:              *CredFile,
		UseApplicationDefault: *UseApplicationDefaultCreds,
		UseComputeEngine:      *UseGCECredentials,
		MaxConcurrentRequests: uint32(*MaxConcurrentRequests),
		MaxConcurrentStreams:  uint32(*MaxConcurrentStreams),
	}, opts...)
}
