package cas

import (
	"errors"
	"math"
	"sync"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/retry"
)

var (
	// ErrNegativeLimit indicates an invalid value that is < 0.
	ErrNegativeLimit = errors.New("cas: limit value must be >= 0")

	// ErrZeroOrNegativeLimit indicates an invalid value that is <= 0.
	ErrZeroOrNegativeLimit = errors.New("cas: limit value must be > 0")
)

const (
	// MegaByte is 1_048_576 bytes.
	MegaByte = 1024 * 1024

	// DefaultGRPCConcurrentCallsLimit is set arbitrarily to a power of 2.
	DefaultGRPCConcurrentCallsLimit = 256

	// DefaultGRPCBytesLimit is the same as the default gRPC request size limit.
	// See: https://pkg.go.dev/google.golang.org/grpc#MaxCallRecvMsgSize
	DefaultGRPCBytesLimit = 4 * MegaByte

	// DefaultGRPCItemsLimit is a 10th of the max.
	DefaultGRPCItemsLimit = 1000

	// MaxGRPCItems is heuristcally (with Google's RBE) set to 10k.
	MaxGRPCItems = 10_000

	// DefaultRPCTimeout is arbitrarily set to what is reasonable for a large action.
	DefaultRPCTimeout = time.Minute

	// DefaultOpenFilesLimit is based on GCS recommendations.
	// See: https://cloud.google.com/compute/docs/disks/optimizing-pd-performance#io-queue-depth
	DefaultOpenFilesLimit = 32

	// DefaultOpenLargeFilesLimit is arbitrarily set.
	DefaultOpenLargeFilesLimit = 2

	// DefaultCompressionSizeThreshold is disabled by default.
	DefaultCompressionSizeThreshold = math.MaxInt64

	// BufferSize is based on GCS recommendations.
	// See: https://cloud.google.com/compute/docs/disks/optimizing-pd-performance#io-size
	BufferSize = 4 * MegaByte
)

// GRPCConfig specifies the configuration for a gRPC endpoint.
type GRPCConfig struct {
	// ConcurrentCallsLimit sets the upper bound of concurrent calls.
	// Must be > 0.
	ConcurrentCallsLimit int

	// BytesLimit sets the upper bound for the size of each request.
	// Comparisons against this value may not be exact due to padding and other serialization naunces.
	// Clients should choose a value that is sufficiently lower than the max size limit for corresponding gRPC connection.
	// Must be > 0.
	// This is defined as int rather than int64 because gRPC uses int for its limit.
	BytesLimit int

	// ItemsLimit sets the upper bound for the number of items per request.
	// Must be > 0.
	ItemsLimit int

	// BundleTimeout sets the maximum duration a call is delayed while bundling.
	// Bundling is used to ammortize the cost of a gRPC call over time. Instead of sending
	// many requests with few items, bunlding attempt to maximize the number of items sent in a single request.
	// This includes waiting for a bit to see if more items are requested.
	BundleTimeout time.Duration

	// Timeout sets the upper bound of the total time spent processing a request.
	// For streaming calls, this applies to each Send/Recv call individually, not the whole streaming session.
	// This does not take into account the time it takes to abort the request upon timeout.
	Timeout time.Duration

	// RetryPolicy sets the retry policy for calls using this config.
	RetryPolicy retry.BackoffPolicy
}

// IOConfig specifies the configuration for IO operations.
type IOConfig struct {
	// ConcurrentWalksLimit sets the upper bound of concurrent filesystem tree traversals.
	// This affects the number of concurrent upload requests for the uploader since each one requires a walk.
	// Must be > 0.
	ConcurrentWalksLimit int

	// ConcurrentWalkerVisits sets the upper bound of concurrent visits per walk.
	// Must b > 0.
	ConcurrentWalkerVisits int

	// OpenFilesLimit sets the upper bound for the number of files being simultanuously processed.
	// Must be > 0.
	OpenFilesLimit int

	// OpenLargeFilesLimit sets the upper bound for the number of large files being simultanuously processed.
	//
	// This value counts towards open files. I.e. the following inequality is always effectively true:
	// OpenFilesLimit >= OpenLargeFilesLimit
	// Must be > 0.
	OpenLargeFilesLimit int

	// SmallFileSizeThreshold sets the upper bound (inclusive) for the file size to be considered a small file.
	//
	// Files that are larger than this value (medium and large files) are uploaded via the streaming API.
	//
	// Small files are buffered entirely in memory and uploaded via the batching API.
	// However, it is still possible for a file to be small in size, but still results in a request that is larger than the gRPC size limit.
	// In that case, the file is uploaded via the streaming API instead.
	//
	// The amount of memory used to buffer files is affected by this value and OpenFilesLimit as well as bundling limits for gRPC.
	// The uploader will stop buffering once the OpenFilesLimit is reached, before which the number of buffered files is bound by
	// the number of blobs buffered for uploading (and whatever the GC hasn't freed yet).
	// In the extreme case, the number of buffered bytes for small files (not including streaming buffers) equals
	// the concurrency limit for the upload gRPC call, times the bytes limit per call, times this value.
	// Note that the amount of memory used to buffer bytes of a generated proto messages is not included in this estimate.
	//
	// Must be >= 0.
	SmallFileSizeThreshold int64

	// LargeFileSizeThreshold sets the lower bound (inclusive) for the file size to be considered a large file.
	// Such files are uploaded in chunks using the file streaming API.
	// Must be >= 0.
	LargeFileSizeThreshold int64

	// CompressionSizeThreshold sets the lower bound for the chunk size before it is subject to compression.
	// A value of 0 enables compression for any chunk size. To disable compression, use math.MaxInt64.
	// Must >= 0.
	CompressionSizeThreshold int64

	// BufferSize sets the buffer size for IO read/write operations.
	// Must be > 0.
	BufferSize int

	// OptimizeForDiskLocality enables sorting files by path before they are written to disk to optimize for disk locality.
	// Assuming files under the same directory are located close to each other on disk, then such files are batched together.
	OptimizeForDiskLocality bool

	// Cache is a read/write cache for digested files.
	// The key is the file path and the associated exclusion filter.
	// The value is a a proto message that represents one of repb.SymlinkNode, repb.DirectoryNode, repb.FileNode.
	// Providing a cache here allows for reusing entries between clients.
	// Cache entries are never evicted which implies the assumption that the files are never edited during the lifetime of the cache entry.
	Cache sync.Map
}

// Stats represents potential metrics reported by various methods.
// Not all fields are populated by every method.
type Stats struct {
	// BytesRequested is the total number of bytes in a request.
	// It does not necessarily equal the total number of bytes uploaded/downloaded.
	BytesRequested int64

	// LogicalBytesMoved is the amount of BytesRequested that was processed.
	// It cannot be larger than BytesRequested, but may be smaller in case of a partial response.
	LogicalBytesMoved int64

	// TotalBytesMoved is the total number of bytes moved over the wire.
	// This may not be accurate since a gRPC call may be interrupted in which case this number may be higher than the real one.
	// It may be larger than (retries) or smaller than BytesRequested (compression, cache hits or partial response).
	TotalBytesMoved int64

	// EffectiveBytesMoved is the total number of bytes moved over the wire, excluding retries.
	// This may not be accurate since a gRPC call may be interrupted in which case this number may be higher than the real one.
	// For failures, this is reported as 0.
	// It may be higher than BytesRequested (compression headers), but never higher than BytesMoved.
	EffectiveBytesMoved int64

	// LogicalBytesCached is the total number of bytes not moved over the wire due to caching (either remotely or locally).
	// For failures, this is reported as 0.
	LogicalBytesCached int64

	// LogicalBytesStreamed is the total number of logical bytes moved by the streaming API.
	// It may be larger than (retries) or smaller than (cache hits or partial response) than the requested size.
	// For failures, this is reported as 0.
	LogicalBytesStreamed int64

	// LogicalBytesBatched is the total number of logical bytes moved by the batching API.
	// It may be larger than (retries) or smaller than (cache hits or partial response) the requested size.
	// For failures, this is reported as 0.
	LogicalBytesBatched int64

	// InputFileCount is the number of processed regular files.
	InputFileCount int64

	// InputDirCount is the number of processed directories.
	InputDirCount int64

	// InputSymlinkCount is the number of processed symlinks (not the number of symlinks in the uploaded merkle tree which may be lower).
	InputSymlinkCount int64

	// CacheHitCount is the number of cache hits.
	CacheHitCount int64

	// CacheMissCount is the number of cache misses.
	CacheMissCount int64

	// DigestCount is the number of processed digests.
	DigestCount int64

	// BatchedCount is the number of batched files.
	BatchedCount int64

	// StreamedCount is the number of streamed files.
	// For methods that accept bytes, the value is 1 upon success, 0 otherwise.
	StreamedCount int64
}

// Add mutates the stats by adding all the corresponding fields of the specified instance.
func (s *Stats) Add(other Stats) {
	s.BytesRequested += other.BytesRequested
	s.LogicalBytesMoved += other.LogicalBytesMoved
	s.TotalBytesMoved += other.TotalBytesMoved
	s.EffectiveBytesMoved += other.EffectiveBytesMoved
	s.LogicalBytesCached += other.LogicalBytesCached
	s.LogicalBytesStreamed += other.LogicalBytesStreamed
	s.LogicalBytesBatched += other.LogicalBytesBatched
	s.InputFileCount += other.InputFileCount
	s.InputDirCount += other.InputDirCount
	s.InputSymlinkCount += other.InputSymlinkCount
	s.CacheHitCount += other.CacheHitCount
	s.CacheMissCount += other.CacheMissCount
	s.DigestCount += other.DigestCount
	s.BatchedCount += other.BatchedCount
	s.StreamedCount += other.StreamedCount
}

func validateGrpcConfig(cfg *GRPCConfig) error {
	if cfg.ConcurrentCallsLimit < 1 || cfg.ItemsLimit < 1 || cfg.BytesLimit < 1 {
		return ErrZeroOrNegativeLimit
	}
	return nil
}

func validateIOConfig(cfg *IOConfig) error {
	if cfg.ConcurrentWalksLimit < 1 || cfg.ConcurrentWalkerVisits < 1 || cfg.OpenFilesLimit < 1 || cfg.OpenLargeFilesLimit < 1 || cfg.BufferSize < 1 {
		return ErrZeroOrNegativeLimit
	}
	if cfg.SmallFileSizeThreshold < 0 || cfg.LargeFileSizeThreshold < 0 || cfg.CompressionSizeThreshold < 0 {
		return ErrNegativeLimit
	}
	return nil
}
