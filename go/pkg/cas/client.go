// Package cas implements an efficient client for Content Addressable Storage.
package cas

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/semaphore"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"
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
	// InstanceName is the full name of the RBE instance.
	InstanceName string

	// ClientConfig is the configuration that the client was created with.
	ClientConfig

	byteStream          bspb.ByteStreamClient
	cas                 repb.ContentAddressableStorageClient
	semBatchUpdateBlobs *semaphore.Weighted

	// TODO(nodir): ensure it does not hurt streaming.
	semFileIO *semaphore.Weighted
	// muLargeFile ensures only one large file is read/written at a time.
	// TODO(nodir): ensure this doesn't hurt performance on SSDs.
	muLargeFile sync.Mutex
	// Pools of []byte slices with the length of ClientConfig.FileIOSize.
	fileIOBufs sync.Pool

	// Mockable functions.

	testScheduleCheck func(ctx context.Context, item *uploadItem) error
}

// ClientConfig is a config for Client.
// See DefaultClientConfig() for the default values.
type ClientConfig struct {
	// FSConcurrency is the maximum number of concurrent file system operations.
	// TODO(nodir): ensure this does not hurt streaming performance
	FSConcurrency int

	// SmallFileThreshold is a size threshold to categorize a file as small.
	// Such files are buffered entirely (read only once).
	SmallFileThreshold int64

	// LargeFileThreshold is a size threshold to categorize a file as large. For
	// such files, IO concurrency limits are much tighter and locality is
	// prioritized: the file is read for the first and second times with minimal
	// delay between the two.
	LargeFileThreshold int64

	// FileIOSize is the size of file reads.
	FileIOSize int64

	// FindMissingBlobs is configuration for FindMissingBlobs RPCs.
	// FindMissingBlobs.MaxSizeBytes is ignored.
	FindMissingBlobs RPCConfig

	// BatchUpdateBlobs is configuration for BatchUpdateBlobs RPCs.
	BatchUpdateBlobs RPCConfig

	// RetryPolicy specifies how to retry requests on transient errors.
	RetryPolicy retry.BackoffPolicy

	// IgnoreCapabilities specifies whether to ignore server-provided capabilities.
	// Capabilities are consulted by default.
	IgnoreCapabilities bool
}

// RPCConfig is configuration for a particular CAS RPC.
// Some of the fields might not apply to certain RPCs.
type RPCConfig struct {
	// Concurrency is the maximum number of RPCs at a time.
	Concurrency int

	// MaxSizeBytes is the maximum size of the request/response, in bytes.
	MaxSizeBytes int

	// MaxItems is the maximum number of blobs/digests per RPC.
	// Applies to batch RPCs.
	MaxItems int

	// Timeout is the maximum duration of the RPC.
	Timeout time.Duration
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
		// TODO(nodir): tune this number.
		FSConcurrency: 32,

		SmallFileThreshold: 1024 * 1024,       // 1MiB
		LargeFileThreshold: 256 * 1024 * 1024, // 256MiB

		// GCE docs recommend 4MB IO size for large files.
		// https://cloud.google.com/compute/docs/disks/optimizing-pd-performance#io-size
		FileIOSize: 4 * 1024 * 1024, // 4MiB

		FindMissingBlobs: RPCConfig{
			Concurrency: 64,
			MaxItems:    1000,
			Timeout:     time.Second,
		},
		BatchUpdateBlobs: RPCConfig{
			Concurrency: 256,

			// This is a suggested approximate limit based on current RBE implementation for writes.
			// Above that BatchUpdateBlobs calls start to exceed a typical minute timeout.
			// This default might not be best for reads though.
			MaxItems: 4000,
			// 4MiB is the default gRPC request size limit.
			MaxSizeBytes: 4 * 1024 * 1024,
			Timeout:      time.Second,
		},

		RetryPolicy: retry.ExponentialBackoff(225*time.Millisecond, 2*time.Second, retry.Attempts(6)),
	}
}

// Validate returns a non-nil error if the config is invalid.
func (c *ClientConfig) Validate() error {
	switch {
	case c.FSConcurrency <= 0:
		return fmt.Errorf("FSConcurrency must be positive")

	case c.SmallFileThreshold < 0:
		return fmt.Errorf("SmallFileThreshold must be non-negative")
	case c.LargeFileThreshold <= 0:
		return fmt.Errorf("LargeFileThreshold must be positive")
	case c.SmallFileThreshold >= c.LargeFileThreshold:
		return fmt.Errorf("SmallFileThreshold must be smaller than LargeFileThreshold")

	case c.FileIOSize <= 0:
		return fmt.Errorf("FileIOSize must be positive")

	// Do not allow more than 100K, otherwise we might run into the request size limits.
	case c.FindMissingBlobs.MaxItems > 10000:
		return fmt.Errorf("FindMissingBlobs.MaxItems must <= 10000")
	}

	if err := c.FindMissingBlobs.validate(false); err != nil {
		return errors.Wrap(err, "FindMissingBlobs")
	}
	if err := c.BatchUpdateBlobs.validate(true); err != nil {
		return errors.Wrap(err, "BatchUpdateBlobs")
	}
	return nil
}

// validate returns an error if the config is invalid.
func (c *RPCConfig) validate(validateMaxSizeBytes bool) error {
	switch {
	case c.Concurrency <= 0:
		return fmt.Errorf("Concurrency must be positive")
	case validateMaxSizeBytes && c.MaxSizeBytes <= 0:
		return fmt.Errorf("MaxSizeBytes must be positive")
	case c.MaxItems <= 0:
		return fmt.Errorf("MaxItems must be positive")
	case c.Timeout <= 0:
		return fmt.Errorf("Timeout must be positive")
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
	if !client.IgnoreCapabilities {
		if err := client.checkCapabilities(ctx); err != nil {
			return nil, errors.Wrapf(err, "checking capabilities")
		}
	}

	client.init()

	return client, nil
}

// init is a part of NewClientWithConfig that can be done in tests without
// creating a real gRPC connection. This function exists purely to aid testing,
// and is tightly coupled with NewClientWithConfig.
func (c *Client) init() {
	c.semBatchUpdateBlobs = semaphore.NewWeighted(int64(c.BatchUpdateBlobs.Concurrency))
	c.semFileIO = semaphore.NewWeighted(int64(c.FSConcurrency))
	c.fileIOBufs.New = func() interface{} {
		return make([]byte, c.FileIOSize)
	}
}

// unaryRPC calls f with retries, and with per-RPC timeouts.
// Does not limit concurrency.
// It is useful when f calls an unary RPC.
func (c *Client) unaryRPC(ctx context.Context, cfg *RPCConfig, f func(context.Context) error) error {
	return retry.WithPolicy(ctx, retry.TransientOnly, c.RetryPolicy, func() error {
		ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
		defer cancel()
		return f(ctx)
	})
}

// checkCapabilities consults with server-side capabilities and potentially
// mutates c.ClientConfig.
func (c *Client) checkCapabilities(ctx context.Context) error {
	caps, err := repb.NewCapabilitiesClient(c.conn).GetCapabilities(ctx, &repb.GetCapabilitiesRequest{InstanceName: c.InstanceName})
	if err != nil {
		return errors.Wrapf(err, "GetCapabilities RPC")
	}

	if err := digest.CheckCapabilities(caps); err != nil {
		return errors.Wrapf(err, "digest function mismatch")
	}

	if c.BatchUpdateBlobs.MaxSizeBytes > int(caps.CacheCapabilities.MaxBatchTotalSizeBytes) {
		c.BatchUpdateBlobs.MaxSizeBytes = int(caps.CacheCapabilities.MaxBatchTotalSizeBytes)
	}
	return nil
}
