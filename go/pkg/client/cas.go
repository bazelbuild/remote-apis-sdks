package client

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/contextmd"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/uploadinfo"
	"google.golang.org/protobuf/encoding/protowire"

	log "github.com/golang/glog"
)

// DefaultCompressedBytestreamThreshold is the default threshold, in bytes, for
// transferring blobs compressed on ByteStream.Write RPCs.
const DefaultCompressedBytestreamThreshold = -1

const logInterval = 25

// MovedBytesMetadata represents the bytes moved in CAS related requests.
type MovedBytesMetadata struct {
	// Requested is the sum of the sizes in bytes for all the uncompressed
	// blobs needed by the execution. It includes bytes that might have
	// been deduped and thus not passed through the wire.
	Requested int64
	// LogicalMoved is the sum of the sizes in bytes of the uncompressed
	// versions of the blobs passed through the wire. It does not included
	// bytes for blobs that were de-duped.
	LogicalMoved int64
	// RealMoved is the sum of sizes in bytes for all blobs passed
	// through the wire in the format they were passed through (eg
	// compressed).
	RealMoved int64
	// Cached is amount of logical bytes that we did not have to move
	// through the wire because they were de-duped.
	Cached int64
}

func (mbm *MovedBytesMetadata) addFrom(other *MovedBytesMetadata) *MovedBytesMetadata {
	if other == nil {
		return mbm
	}
	mbm.Requested += other.Requested
	mbm.LogicalMoved += other.LogicalMoved
	mbm.RealMoved += other.RealMoved
	mbm.Cached += other.Cached
	return mbm
}

func (c *Client) shouldCompress(sizeBytes int64) bool {
	return int64(c.CompressedBytestreamThreshold) >= 0 && int64(c.CompressedBytestreamThreshold) <= sizeBytes
}

func (c *Client) shouldCompressEntry(ue *uploadinfo.Entry) bool {
	if !c.shouldCompress(ue.Digest.Size) {
		return false
	} else if c.UploadCompressionPredicate == nil {
		return true
	}
	return c.UploadCompressionPredicate(ue)
}

// makeBatches splits a list of digests into batches of size no more than the maximum.
//
// First, we sort all the blobs, then we make each batch by taking the largest available blob and
// then filling in with as many small blobs as we can fit. This is a naive approach to the knapsack
// problem, and may have suboptimal results in some cases, but it results in deterministic batches,
// runs in O(n log n) time, and avoids most of the pathological cases that result from scanning from
// one end of the list only.
//
// The input list is sorted in-place; additionally, any blob bigger than the maximum will be put in
// a batch of its own and the caller will need to ensure that it is uploaded with Write, not batch
// operations.
func (c *Client) makeBatches(ctx context.Context, dgs []digest.Digest, optimizeSize bool) [][]digest.Digest {
	var batches [][]digest.Digest
	contextmd.Infof(ctx, log.Level(2), "Batching %d digests", len(dgs))
	if optimizeSize {
		sort.Slice(dgs, func(i, j int) bool {
			return dgs[i].Size < dgs[j].Size
		})
	}
	for len(dgs) > 0 {
		var batch []digest.Digest
		if optimizeSize {
			batch = []digest.Digest{dgs[len(dgs)-1]}
			dgs = dgs[:len(dgs)-1]
		} else {
			batch = []digest.Digest{dgs[0]}
			dgs = dgs[1:]
		}
		requestOverhead := marshalledFieldSize(int64(len(c.InstanceName)))
		sz := requestOverhead + marshalledRequestSize(batch[0])
		var nextSize int64
		if len(dgs) > 0 {
			nextSize = marshalledRequestSize(dgs[0])
		}
		for len(dgs) > 0 && len(batch) < int(c.MaxBatchDigests) && nextSize <= int64(c.MaxBatchSize)-sz { // nextSize+sz possibly overflows so subtract instead.
			sz += nextSize
			batch = append(batch, dgs[0])
			dgs = dgs[1:]
			if len(dgs) > 0 {
				nextSize = marshalledRequestSize(dgs[0])
			}
		}
		contextmd.Infof(ctx, log.Level(3), "Created batch of %d blobs with total size %d", len(batch), sz)
		batches = append(batches, batch)
	}
	contextmd.Infof(ctx, log.Level(2), "%d batches created", len(batches))
	return batches
}

func (c *Client) makeQueryBatches(ctx context.Context, digests []digest.Digest) [][]digest.Digest {
	var batches [][]digest.Digest
	for len(digests) > 0 {
		batchSize := int(c.MaxQueryBatchDigests)
		if len(digests) < int(c.MaxQueryBatchDigests) {
			batchSize = len(digests)
		}
		batch := make([]digest.Digest, 0, batchSize)
		for i := 0; i < batchSize; i++ {
			batch = append(batch, digests[i])
		}
		digests = digests[batchSize:]
		contextmd.Infof(ctx, log.Level(3), "Created query batch of %d blobs", len(batch))
		batches = append(batches, batch)
	}
	return batches
}

func marshalledFieldSize(size int64) int64 {
	return 1 + int64(protowire.SizeVarint(uint64(size))) + size
}

func marshalledRequestSize(d digest.Digest) int64 {
	// An additional BatchUpdateBlobsRequest_Request includes the Digest and data fields,
	// as well as the message itself. Every field has a 1-byte size tag, followed by
	// the varint field size for variable-sized fields (digest hash and data).
	// Note that the BatchReadBlobsResponse_Response field is similar, but includes
	// and additional Status proto which can theoretically be unlimited in size.
	// We do not account for it here, relying on the Client setting a large (100MB)
	// limit for incoming messages.
	digestSize := marshalledFieldSize(int64(len(d.Hash)))
	if d.Size > 0 {
		digestSize += 1 + int64(protowire.SizeVarint(uint64(d.Size)))
	}
	reqSize := marshalledFieldSize(digestSize)
	if d.Size > 0 {
		reqSize += marshalledFieldSize(int64(d.Size))
	}
	return marshalledFieldSize(reqSize)
}

func copyFile(srcOutDir, dstOutDir, from, to string, mode os.FileMode) error {
	src := filepath.Join(srcOutDir, from)
	s, err := os.Open(src)
	if err != nil {
		return err
	}
	defer s.Close()

	dst := filepath.Join(dstOutDir, to)
	t, err := os.OpenFile(dst, os.O_RDWR|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	defer t.Close()
	_, err = io.Copy(t, s)
	return err
}
