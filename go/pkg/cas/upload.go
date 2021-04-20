package cas

import "context"

// UploadInput is one of inputs to Client.Upload function.
//
// It can be either a reference to a file/dir (see Path) or it can be an
// in-memory blob (see Content).
type UploadInput struct {
	// Path to the file or a directory to upload.
	// If empty, the Content is uploaded instead.
	//
	// Must be absolute or relative to CWD.
	Path string

	// Contents to upload.
	// Ignored if Path is not empty.
	Content []byte

	// TODO(nodir): add Predicate.
	// TODO(nodir): add AllowDanglingSymlinks
	// TODO(nodir): add PreserveSymlinks.
}

// TransferStats is upload/download statistics.
type TransferStats struct {
	CacheHits   DigestStat
	CacheMisses DigestStat

	Streamed DigestStat // streamed transfers
	Batched  DigestStat // batched transfers
}

// DigestStat is aggregated statistics over a set of digests.
type DigestStat struct {
	Digests int64 // number of unique digests
	Bytes   int64 // total sum of of digest sizes
}

// Upload uploads all inputs. It exits when inputC is closed or ctx is canceled.
func (c *Client) Upload(ctx context.Context, inputC <-chan *UploadInput) (stats *TransferStats, err error) {
	// TODO(nodir): implement.
	panic("not implemented")
}
