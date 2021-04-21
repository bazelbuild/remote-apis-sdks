// Package cas implements an efficient client for Content Addressable Storage.
package cas

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

// Client is a client for Content Addressable Storage.
// Create one using NewClient.
//
// Goroutine-safe.
//
// All fields are considered immutable, and should not be changed.
type Client struct {
	conn *grpc.ClientConn
	// The full name of the RBE instance.
	InstanceName string

	// The configuration that the client was created with.
	ClientConfig

	byteStream bspb.ByteStreamClient
	cas        repb.ContentAddressableStorageClient

	// Mockable functions.

	testScheduleCheck func(ctx context.Context, item *uploadItem) error
}

// ClientConfig is a config for Client.
// See DefaultClientConfig() for the default values.
type ClientConfig struct {
	// Maximum number of concurrent file system operations.
	// TODO(nodir): ensure this does not hurt streaming performance
	FSConcurrency int

	// If a file is smaller than or equal to this threshold, then it is buffered
	// entirely (read only once).
	SmallFileThreshold int64

	// If a file is larger than or equal to this threshold, then it is considered
	// large. Large file IO concurrency limits are much tighter than other files,
	// and locality is prioritized: the file is read for the first and second
	// times with minimal delay between the two.
	LargeFileThreshold int64

	// The size of file reads.
	FileIOSize int64

	// TODO(nodir): add per-RPC timeouts.
	// TODO(nodir): add retries.
}

// DefaultClientConfig returns the default config.
//
// To override a specific value:
//   cfg := DefaultClientConfig()
//   ... mutate cfg ...
//   client, err := NewClientWithConfig(ctx, cfg)
func DefaultClientConfig() ClientConfig {
	return ClientConfig{
		// GCE docs recommend at least 32 concurrent IOs.
		// https://cloud.google.com/compute/docs/disks/optimizing-pd-performance#io-queue-depth
		FSConcurrency: 64,

		SmallFileThreshold: 1024 * 1024,
		LargeFileThreshold: 256 * 1024 * 1024,

		// GCE docs recommend 4MB IO size for large files.
		// https://cloud.google.com/compute/docs/disks/optimizing-pd-performance#io-size
		FileIOSize: 4 * 1024 * 1024,
	}
}

// Validate returns a non-nil error if the config is invalid.
func (c *ClientConfig) Validate() error {
	switch {
	case c.FSConcurrency <= 0:
		return fmt.Errorf("FSConcurrency must be positive")

	case c.SmallFileThreshold <= 0:
		return fmt.Errorf("SmallFileThreshold must not be negative")
	case c.LargeFileThreshold < 0:
		return fmt.Errorf("LargeFileThreshold must be positive")
	case c.SmallFileThreshold >= c.LargeFileThreshold:
		return fmt.Errorf("SmallFileThreshold must be smaller than LargeFileThreshold")

	case c.FileIOSize <= 0:
		return fmt.Errorf("FileIOSize must be positive")

	default:
		return nil
	}
}

// NewClient creates a new client with the default configuration.
// Use client.Dial to create a connection.
func NewClient(ctx context.Context, conn *grpc.ClientConn, instanceName string) (*Client, error) {
	return NewClientWithConfig(ctx, conn, instanceName, DefaultClientConfig())
}

// NewClientWithConfig creates a new client and accepts a configuration.
func NewClientWithConfig(ctx context.Context, conn *grpc.ClientConn, instanceName string, config ClientConfig) (*Client, error) {
	switch err := config.Validate(); {
	case err != nil:
		return nil, errors.Wrap(err, "invalid config")
	case conn == nil:
		return nil, fmt.Errorf("conn is unspecified")
	case instanceName == "":
		return nil, fmt.Errorf("instance name is unspecified")
	}

	client := &Client{
		InstanceName: instanceName,
		ClientConfig: config,
		conn:         conn,
		byteStream:   bspb.NewByteStreamClient(conn),
		cas:          repb.NewContentAddressableStorageClient(conn),
	}

	// TODO(nodir): Check capabilities.
	return client, nil
}
