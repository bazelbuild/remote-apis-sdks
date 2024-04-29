// Package cas implements an efficient client for Content Addressable Storage.
package cas

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	// Redundant imports are required for the google3 mirror. Aliases should not be changed.
	"github.com/pkg/errors"
	"golang.org/x/sync/semaphore"
	bsgrpc "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"
	regrpc "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
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

	// Config is the configuration that the client was created with.
	Config ClientConfig

	byteStream bsgrpc.ByteStreamClient
	cas        regrpc.ContentAddressableStorageClient

	// per-RPC semaphores

	semFindMissingBlobs *semaphore.Weighted
	semBatchUpdateBlobs *semaphore.Weighted
	semByteStreamWrite  *semaphore.Weighted

	// TODO(nodir): ensure it does not hurt streaming.
	semFileIO *semaphore.Weighted
	// semLargeFile ensures only few large files are read/written at a time.
	// TODO(nodir): ensure this doesn't hurt performance on SSDs.
	semLargeFile *semaphore.Weighted

	// fileBufReaders is a pool of reusable *bufio.Readers
	// with buffer size = ClientConfig.FileIOSize, so e.g. 4MiB.
	// Use fileBufReaders.Get(), then reset the reader with bufio.Reader.Reset,
	// and put back to the when done.
	fileBufReaders sync.Pool

	// streamBufs is a pool of []byte slices used for ByteStream read/write RPCs.
	streamBufs sync.Pool

	// Mockable functions.

	testScheduleCheck func(ctx context.Context, item *uploadItem) error
}

// ClientConfig is a config for Client.
// See DefaultClientConfig() for the default values.
type ClientConfig struct {
	// FSConcurrency is the maximum number of concurrent file system operations.
	// TODO(nodir): ensure this does not hurt streaming performance
	FSConcurrency int

	// FSLargeConcurrency is the maximum number of concurrent large file read operation.
	FSLargeConcurrency int

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

	// CompressedBytestreamThreshold is the minimum blob size to enable compression
	// in ByteStream RPCs.
	// Use 0 for all writes being compressed, and a negative number for all operations being
	// uncompressed.
	// DefaultClientConfig() disables compression by default.
	CompressedBytestreamThreshold int64

	// FindMissingBlobs is configuration for ContentAddressableStorage.FindMissingBlobs RPCs.
	// FindMissingBlobs.MaxSizeBytes is ignored.
	FindMissingBlobs RPCConfig

	// BatchUpdateBlobs is configuration for ContentAddressableStorage.BatchUpdateBlobs RPCs.
	BatchUpdateBlobs RPCConfig

	// ByteStreamWrite is configuration for ByteStream.Write RPCs.
	// ByteStreamWrite.MaxItems is ignored.
	ByteStreamWrite RPCConfig

	// RetryPolicy specifies how to retry requests on transient errors.
	RetryPolicy retry.BackoffPolicy

	// IgnoreCapabilities specifies whether to ignore server-provided capabilities.
	// Capabilities are consulted by default.
	IgnoreCapabilities bool
}

// RPCConfig is configuration for a particular CAS RPC.
// Some of the fields might not apply to certain RPCs.
//
// For streaming RPCs, the values apply to individual requests/responses in a
// stream, not the entire stream.
type RPCConfig struct {
	// Concurrency is the maximum number of RPCs in flight.
	Concurrency int

	// MaxSizeBytes is the maximum size of the request/response, in bytes.
	MaxSizeBytes int

	// MaxItems is the maximum number of blobs/digests per RPC.
	// Applies only to batch RPCs, such as FindMissingBlobs.
	MaxItems int

	// Timeout is the maximum duration of the RPC.
	Timeout time.Duration
}

// DefaultClientConfig returns the default config.
//
// To override a specific value:
//
//	cfg := DefaultClientConfig()
//	... mutate cfg ...
//	client, err := NewClientWithConfig(ctx, cfg)
func DefaultClientConfig() ClientConfig {
	return ClientConfig{
		// GCE docs recommend at least 32 concurrent IOs.
		// https://cloud.google.com/compute/docs/disks/optimizing-pd-performance#io-queue-depth
		// TODO(nodir): tune this number.
		FSConcurrency: 32,

		FSLargeConcurrency: 2,

		SmallFileThreshold: 1024 * 1024,       // 1MiB
		LargeFileThreshold: 256 * 1024 * 1024, // 256MiB

		// GCE docs recommend 4MB IO size for large files.
		// https://cloud.google.com/compute/docs/disks/optimizing-pd-performance#io-size
		FileIOSize: 4 * 1024 * 1024, // 4MiB

		FindMissingBlobs: RPCConfig{
			Concurrency: 256, // Should be >= BatchUpdateBlobs.Concurrency.
			MaxItems:    1000,
			Timeout:     time.Minute,
		},
		BatchUpdateBlobs: RPCConfig{
			Concurrency: 256,

			// This is a suggested approximate limit based on current RBE implementation for writes.
			// Above that BatchUpdateBlobs calls start to exceed a typical minute timeout.
			// This default might not be best for reads though.
			MaxItems: 4000,
			// 4MiB is the default gRPC request size limit.
			MaxSizeBytes: 4 * 1024 * 1024,
			Timeout:      time.Minute,
		},
		ByteStreamWrite: RPCConfig{
			Concurrency: 256,
			// 4MiB is the default gRPC request size limit.
			MaxSizeBytes: 4 * 1024 * 1024,
			Timeout:      time.Minute,
		},

		// Disable compression by default.
		CompressedBytestreamThreshold: -1,

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

	// Checking more than 100K blobs may run into the request size limits.
	// It does not really make sense to check even >10K blobs, so limit to 10k.
	case c.FindMissingBlobs.MaxItems > 10000:
		return fmt.Errorf("FindMissingBlobs.MaxItems must <= 10000")
	}

	if err := c.FindMissingBlobs.validate(); err != nil {
		return errors.Wrap(err, "FindMissingBlobs")
	}
	if err := c.BatchUpdateBlobs.validate(); err != nil {
		return errors.Wrap(err, "BatchUpdateBlobs")
	}
	if err := c.ByteStreamWrite.validate(); err != nil {
		return errors.Wrap(err, "BatchUpdateBlobs")
	}
	return nil
}

// validate returns an error if the config is invalid.
func (c *RPCConfig) validate() error {
	switch {
	case c.Concurrency <= 0:
		return fmt.Errorf("Concurrency must be positive")
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
		Config:       config,
		conn:         conn,
		byteStream:   bsgrpc.NewByteStreamClient(conn),
		cas:          regrpc.NewContentAddressableStorageClient(conn),
	}
	if !client.Config.IgnoreCapabilities {
		if err := client.checkCapabilities(ctx); err != nil {
			return nil, errors.Wrapf(err, "checking capabilities")
		}
	}

	client.init()

	return client, nil
}

var emptyReader = bytes.NewReader(nil)

// init is a part of NewClientWithConfig that can be done in tests without
// creating a real gRPC connection. This function exists purely to aid testing,
// and is tightly coupled with NewClientWithConfig.
func (c *Client) init() {
	c.semFindMissingBlobs = semaphore.NewWeighted(int64(c.Config.FindMissingBlobs.Concurrency))
	c.semBatchUpdateBlobs = semaphore.NewWeighted(int64(c.Config.BatchUpdateBlobs.Concurrency))
	c.semByteStreamWrite = semaphore.NewWeighted(int64(c.Config.ByteStreamWrite.Concurrency))

	c.semFileIO = semaphore.NewWeighted(int64(c.Config.FSConcurrency))
	c.semLargeFile = semaphore.NewWeighted(int64(c.Config.FSLargeConcurrency))
	c.fileBufReaders.New = func() interface{} {
		return bufio.NewReaderSize(emptyReader, int(c.Config.FileIOSize))
	}

	streamBufSize := 32 * 1024 // by default, send 32KiB chunks.
	if streamBufSize < c.Config.ByteStreamWrite.MaxSizeBytes {
		streamBufSize = int(c.Config.ByteStreamWrite.MaxSizeBytes)
	}
	c.streamBufs.New = func() interface{} {
		buf := make([]byte, streamBufSize)
		return &buf
	}
}

// unaryRPC calls f with retries, and with per-RPC timeouts.
// Does not limit concurrency.
// It is useful when f calls an unary RPC.
func (c *Client) unaryRPC(ctx context.Context, cfg *RPCConfig, f func(context.Context) error) error {
	return c.withRetries(ctx, func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
		defer cancel()
		return f(ctx)
	})
}

func (c *Client) withRetries(ctx context.Context, f func(context.Context) error) error {
	return retry.WithPolicy(ctx, retry.TransientOnly, c.Config.RetryPolicy, func() error {
		return f(ctx)
	})
}

// checkCapabilities consults with server-side capabilities and potentially
// mutates c.ClientConfig.
func (c *Client) checkCapabilities(ctx context.Context) error {
	caps, err := regrpc.NewCapabilitiesClient(c.conn).GetCapabilities(ctx, &repb.GetCapabilitiesRequest{InstanceName: c.InstanceName})
	if err != nil {
		return errors.Wrapf(err, "GetCapabilities RPC")
	}

	if err := digest.CheckCapabilities(caps); err != nil {
		return errors.Wrapf(err, "digest function mismatch")
	}

	if c.Config.BatchUpdateBlobs.MaxSizeBytes > int(caps.CacheCapabilities.MaxBatchTotalSizeBytes) {
		c.Config.BatchUpdateBlobs.MaxSizeBytes = int(caps.CacheCapabilities.MaxBatchTotalSizeBytes)
	}

	// TODO(nodir): check compression capabilities.

	return nil
}

// withPerCallTimeout returns a function wrapper that cancels the context if
// fn does not return within the timeout.
func withPerCallTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc, func(fn func())) {
	ctx, cancel := context.WithCancel(ctx)
	return ctx, cancel, func(fn func()) {
		stop := make(chan struct{})
		defer close(stop)
		go func() {
			select {
			case <-time.After(timeout):
				cancel()
			case <-stop:
			}
		}()
		fn()
	}
}
