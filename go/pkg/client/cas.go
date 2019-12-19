package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/chunker"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/tree"
	"github.com/golang/protobuf/proto"
	"github.com/pborman/uuid"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	log "github.com/golang/glog"
)

// UploadIfMissing stores a number of uploadable items.
// It first queries the CAS to see which items are missing and only uploads those that are.
func (c *Client) UploadIfMissing(ctx context.Context, data ...*chunker.Chunker) error {
	if cap(c.casUploaders) <= 0 {
		return fmt.Errorf("CASConcurrency should be at least 1")
	}
	const (
		logInterval = 25
	)

	var dgs []digest.Digest
	chunkers := make(map[digest.Digest]*chunker.Chunker)
	for _, c := range data {
		dg := c.Digest()
		if _, ok := chunkers[dg]; !ok {
			dgs = append(dgs, dg)
			chunkers[dg] = c
		}
	}

	missing, err := c.MissingBlobs(ctx, dgs)
	if err != nil {
		return err
	}
	log.V(2).Infof("%d items to store", len(missing))
	var batches [][]digest.Digest
	if c.useBatchOps {
		batches = makeBatches(missing)
	} else {
		log.V(2).Info("Uploading them individually")
		for i := range missing {
			log.V(3).Infof("Creating single batch of blob %s", missing[i])
			batches = append(batches, missing[i:i+1])
		}
	}

	eg, eCtx := errgroup.WithContext(ctx)
	for i, batch := range batches {
		i, batch := i, batch   // https://golang.org/doc/faq#closures_and_goroutines
		c.casUploaders <- true // Reserve an uploader thread.
		eg.Go(func() error {
			defer func() { <-c.casUploaders }()
			if i%logInterval == 0 {
				log.V(2).Infof("%d batches left to store", len(batches)-i)
			}
			if len(batch) > 1 {
				log.V(3).Infof("Uploading batch of %d blobs", len(batch))
				bchMap := make(map[digest.Digest][]byte)
				for _, dg := range batch {
					data, err := chunkers[dg].FullData()
					if err != nil {
						return err
					}
					bchMap[dg] = data
				}
				if err := c.BatchWriteBlobs(eCtx, bchMap); err != nil {
					return err
				}
			} else {
				log.V(3).Infof("Uploading single blob with digest %s", batch[0])
				ch := chunkers[batch[0]]
				dg := ch.Digest()
				if err := c.WriteChunked(eCtx, c.ResourceNameWrite(dg.Hash, dg.Size), ch); err != nil {
					return err
				}
			}
			if eCtx.Err() != nil {
				return eCtx.Err()
			}
			return nil
		})
	}

	log.V(2).Info("Waiting for remaining jobs")
	err = eg.Wait()
	log.V(2).Info("Done")
	return err
}

// WriteBlobs stores a large number of blobs from a digest-to-blob map. It's intended for use on the
// result of PackageTree. Unlike with the single-item functions, it first queries the CAS to
// see which blobs are missing and only uploads those that are.
// TODO(olaola): rethink the API of this layer:
// * Do we want to allow []byte uploads, or require the user to construct Chunkers?
// * How to consistently distinguish in the API between should we use GetMissing or not?
// * Should BatchWrite be a public method at all?
func (c *Client) WriteBlobs(ctx context.Context, blobs map[digest.Digest][]byte) error {
	var chunkers []*chunker.Chunker
	for _, blob := range blobs {
		chunkers = append(chunkers, chunker.NewFromBlob(blob, int(c.ChunkMaxSize)))
	}
	return c.UploadIfMissing(ctx, chunkers...)
}

// WriteProto marshals and writes a proto.
func (c *Client) WriteProto(ctx context.Context, msg proto.Message) (digest.Digest, error) {
	bytes, err := proto.Marshal(msg)
	if err != nil {
		return digest.Empty, err
	}
	return c.WriteBlob(ctx, bytes)
}

// WriteBlob uploads a blob to the CAS.
func (c *Client) WriteBlob(ctx context.Context, blob []byte) (digest.Digest, error) {
	ch := chunker.NewFromBlob(blob, int(c.ChunkMaxSize))
	dg := ch.Digest()
	return dg, c.WriteChunked(ctx, c.ResourceNameWrite(dg.Hash, dg.Size), ch)
}

const (
	// MaxBatchSz is the maximum size of a batch to upload with BatchWriteBlobs. We set it to slightly
	// below 4 MB, because that is the limit of a message size in gRPC
	MaxBatchSz = 4*1024*1024 - 1024

	// MaxBatchDigests is a suggested approximate limit based on current RBE implementation.
	// Above that BatchUpdateBlobs calls start to exceed a typical minute timeout.
	MaxBatchDigests = 4000
)

// BatchWriteBlobs uploads a number of blobs to the CAS. They must collectively be below the
// maximum total size for a batch upload, which is about 4 MB (see MaxBatchSz). Digests must be
// computed in advance by the caller. In case multiple errors occur during the blob upload, the
// last error will be returned.
func (c *Client) BatchWriteBlobs(ctx context.Context, blobs map[digest.Digest][]byte) error {
	var reqs []*repb.BatchUpdateBlobsRequest_Request
	var sz int64
	for k, b := range blobs {
		sz += k.Size
		reqs = append(reqs, &repb.BatchUpdateBlobsRequest_Request{
			Digest: k.ToProto(),
			Data:   b,
		})
	}
	if sz > MaxBatchSz {
		return fmt.Errorf("batch update of %d total bytes exceeds maximum of %d", sz, MaxBatchSz)
	}
	if len(blobs) > MaxBatchDigests {
		return fmt.Errorf("batch update of %d total blobs exceeds maximum of %d", len(blobs), MaxBatchDigests)
	}
	closure := func() error {
		var resp *repb.BatchUpdateBlobsResponse
		err := c.CallWithTimeout(ctx, func(ctx context.Context) (e error) {
			resp, e = c.cas.BatchUpdateBlobs(ctx, &repb.BatchUpdateBlobsRequest{
				InstanceName: c.InstanceName,
				Requests:     reqs,
			})
			return e
		})
		if err != nil {
			return err
		}

		numErrs, errDg, errMsg := 0, new(repb.Digest), ""
		var failedReqs []*repb.BatchUpdateBlobsRequest_Request
		var retriableError error
		allRetriable := true
		for _, r := range resp.Responses {
			st := status.FromProto(r.Status)
			if st.Code() != codes.OK {
				e := st.Err()
				if c.Retrier.ShouldRetry(e) {
					failedReqs = append(failedReqs, &repb.BatchUpdateBlobsRequest_Request{
						Digest: r.Digest,
						Data:   blobs[digest.NewFromProtoUnvalidated(r.Digest)],
					})
					retriableError = e
				} else {
					allRetriable = false
				}
				numErrs++
				errDg = r.Digest
				errMsg = r.Status.Message
			}
		}
		reqs = failedReqs
		if numErrs > 0 {
			if allRetriable {
				return retriableError // Retriable errors only, retry the failed requests.
			}
			return fmt.Errorf("uploading blobs as part of a batch resulted in %d failures, including blob %s: %s", numErrs, errDg, errMsg)
		}
		return nil
	}
	return c.Retrier.Do(ctx, closure)
}

// BatchDownloadBlobs downloads a number of blobs from the CAS to memory. They must collectively be below the
// maximum total size for a batch read, which is about 4 MB (see MaxBatchSz). Digests must be
// computed in advance by the caller. In case multiple errors occur during the blob read, the
// last error will be returned.
func (c *Client) BatchDownloadBlobs(ctx context.Context, dgs []digest.Digest) (map[digest.Digest][]byte, error) {
	if len(dgs) > MaxBatchDigests {
		return nil, fmt.Errorf("batch read of %d total blobs exceeds maximum of %d", len(dgs), MaxBatchDigests)
	}
	req := &repb.BatchReadBlobsRequest{InstanceName: c.InstanceName}
	var sz int64
	foundEmpty := false
	for _, dg := range dgs {
		if dg.Size == 0 {
			foundEmpty = true
			continue
		}
		sz += dg.Size
		req.Digests = append(req.Digests, dg.ToProto())
	}
	if sz > MaxBatchSz {
		return nil, fmt.Errorf("batch read of %d total bytes exceeds maximum of %d", sz, MaxBatchSz)
	}
	res := make(map[digest.Digest][]byte)
	if foundEmpty {
		res[digest.Empty] = nil
	}
	closure := func() error {
		var resp *repb.BatchReadBlobsResponse
		err := c.CallWithTimeout(ctx, func(ctx context.Context) (e error) {
			resp, e = c.cas.BatchReadBlobs(ctx, req)
			return e
		})
		if err != nil {
			return err
		}

		numErrs, errDg, errMsg := 0, &repb.Digest{}, ""
		var failedDgs []*repb.Digest
		var retriableError error
		allRetriable := true
		for _, r := range resp.Responses {
			st := status.FromProto(r.Status)
			if st.Code() != codes.OK {
				e := st.Err()
				if c.Retrier.ShouldRetry(e) {
					failedDgs = append(failedDgs, r.Digest)
					retriableError = e
				} else {
					allRetriable = false
				}
				numErrs++
				errDg = r.Digest
				errMsg = r.Status.Message
			} else {
				res[digest.NewFromProtoUnvalidated(r.Digest)] = r.Data
			}
		}
		req.Digests = failedDgs
		if numErrs > 0 {
			if allRetriable {
				return retriableError // Retriable errors only, retry the failed digests.
			}
			return fmt.Errorf("downloading blobs as part of a batch resulted in %d failures, including blob %s: %s", numErrs, errDg, errMsg)
		}
		return nil
	}
	return res, c.Retrier.Do(ctx, closure)
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
func makeBatches(dgs []digest.Digest) [][]digest.Digest {
	var batches [][]digest.Digest
	log.V(2).Infof("Batching %d digests", len(dgs))
	sort.Slice(dgs, func(i, j int) bool {
		return dgs[i].Size < dgs[j].Size
	})
	for len(dgs) > 0 {
		batch := []digest.Digest{dgs[len(dgs)-1]}
		dgs = dgs[:len(dgs)-1]
		sz := batch[0].Size
		for len(dgs) > 0 && len(batch) < MaxBatchDigests && dgs[0].Size <= MaxBatchSz-sz { // dg.Size+sz possibly overflows so subtract instead.
			sz += dgs[0].Size
			batch = append(batch, dgs[0])
			dgs = dgs[1:]
		}
		log.V(3).Infof("Created batch of %d blobs with total size %d", len(batch), sz)
		batches = append(batches, batch)
	}
	log.V(2).Infof("%d batches created", len(batches))
	return batches
}

// ReadBlob fetches a blob from the CAS into a byte slice.
func (c *Client) ReadBlob(ctx context.Context, d digest.Digest) ([]byte, error) {
	return c.readBlob(ctx, d.Hash, d.Size, 0, 0)
}

// ReadBlobRange fetches a partial blob from the CAS into a byte slice, starting from offset bytes
// and including at most limit bytes (or no limit if limit==0). The offset must be non-negative and
// no greater than the size of the entire blob. The limit must not be negative, but offset+limit may
// be greater than the size of the entire blob.
func (c *Client) ReadBlobRange(ctx context.Context, d digest.Digest, offset, limit int64) ([]byte, error) {
	return c.readBlob(ctx, d.Hash, d.Size, offset, limit)
}

func (c *Client) readBlob(ctx context.Context, hash string, sizeBytes, offset, limit int64) ([]byte, error) {
	// int might be 32-bit, in which case we could have a blob whose size is representable in int64
	// but not int32, and thus can't fit in a slice. We can check for this by casting and seeing if
	// the result is negative, since 32 bits is big enough wrap all out-of-range values of int64 to
	// negative numbers. If int is 64-bits, the cast is a no-op and so the condition will always fail.
	if int(sizeBytes) < 0 {
		return nil, fmt.Errorf("digest size %d is too big to fit in a byte slice", sizeBytes)
	}
	if offset > sizeBytes {
		return nil, fmt.Errorf("offset %d out of range for a blob of size %d", offset, sizeBytes)
	}
	if offset < 0 {
		return nil, fmt.Errorf("offset %d may not be negative", offset)
	}
	if limit < 0 {
		return nil, fmt.Errorf("limit %d may not be negative", limit)
	}
	sz := sizeBytes - offset
	if limit > 0 && limit < sz {
		sz = limit
	}
	sz += bytes.MinRead // Pad size so bytes.Buffer does not reallocate.
	buf := bytes.NewBuffer(make([]byte, 0, sz))
	_, err := c.readBlobStreamed(ctx, hash, sizeBytes, offset, limit, buf)
	return buf.Bytes(), err
}

// ReadBlobToFile fetches a blob with a provided digest name from the CAS, saving it into a file.
// It returns the number of bytes read.
func (c *Client) ReadBlobToFile(ctx context.Context, d digest.Digest, fpath string) (int64, error) {
	return c.readBlobToFile(ctx, d.Hash, d.Size, fpath)
}

func (c *Client) readBlobToFile(ctx context.Context, hash string, sizeBytes int64, fpath string) (int64, error) {
	n, err := c.readToFile(ctx, c.resourceNameRead(hash, sizeBytes), fpath)
	if err != nil {
		return n, err
	}
	if n != sizeBytes {
		return n, fmt.Errorf("CAS fetch read %d bytes but %d were expected", n, sizeBytes)
	}
	return n, nil
}

// ReadBlobStreamed fetches a blob with a provided digest from the CAS.
// It streams into an io.Writer, and returns the number of bytes read.
func (c *Client) ReadBlobStreamed(ctx context.Context, d digest.Digest, w io.Writer) (int64, error) {
	return c.readBlobStreamed(ctx, d.Hash, d.Size, 0, 0, w)
}

func (c *Client) readBlobStreamed(ctx context.Context, hash string, sizeBytes, offset, limit int64, w io.Writer) (int64, error) {
	if sizeBytes == 0 {
		// Do not download empty blobs.
		return 0, nil
	}
	n, err := c.readStreamed(ctx, c.resourceNameRead(hash, sizeBytes), offset, limit, w)
	if err != nil {
		return n, err
	}
	sz := sizeBytes - offset
	if limit > 0 && limit < sz {
		sz = limit
	}
	if n != sz {
		return n, fmt.Errorf("CAS fetch read %d bytes but %d were expected", n, sz)
	}
	return n, nil
}

// MissingBlobs queries the CAS to determine if it has the listed blobs. It returns a list of the
// missing blobs.
func (c *Client) MissingBlobs(ctx context.Context, ds []digest.Digest) ([]digest.Digest, error) {
	if cap(c.casUploaders) <= 0 {
		return nil, fmt.Errorf("CASConcurrency should be at least 1")
	}
	var batches [][]digest.Digest
	var missing []digest.Digest
	var resultMutex sync.Mutex
	const (
		logInterval   = 25
		maxQueryLimit = 10000
	)
	for len(ds) > 0 {
		batchSize := maxQueryLimit
		if len(ds) < maxQueryLimit {
			batchSize = len(ds)
		}
		var batch []digest.Digest
		for i := 0; i < batchSize; i++ {
			batch = append(batch, ds[i])
		}
		ds = ds[batchSize:]
		log.V(3).Infof("Created query batch of %d blobs", len(batch))
		batches = append(batches, batch)
	}
	log.V(3).Infof("%d query batches created", len(batches))

	eg, eCtx := errgroup.WithContext(ctx)
	for i, batch := range batches {
		i, batch := i, batch   // https://golang.org/doc/faq#closures_and_goroutines
		c.casUploaders <- true // Reserve an uploader thread.
		eg.Go(func() error {
			defer func() { <-c.casUploaders }()
			if i%logInterval == 0 {
				log.V(3).Infof("%d missing batches left to query", len(batches)-i)
			}
			var batchPb []*repb.Digest
			for _, dg := range batch {
				batchPb = append(batchPb, dg.ToProto())
			}
			req := &repb.FindMissingBlobsRequest{
				InstanceName: c.InstanceName,
				BlobDigests:  batchPb,
			}
			resp, err := c.FindMissingBlobs(eCtx, req)
			if err != nil {
				return err
			}
			resultMutex.Lock()
			for _, d := range resp.MissingBlobDigests {
				missing = append(missing, digest.NewFromProtoUnvalidated(d))
			}
			resultMutex.Unlock()
			if eCtx.Err() != nil {
				return eCtx.Err()
			}
			return nil
		})
	}
	log.V(3).Info("Waiting for remaining query jobs")
	err := eg.Wait()
	log.V(3).Info("Done")
	return missing, err
}

func (c *Client) resourceNameRead(hash string, sizeBytes int64) string {
	return fmt.Sprintf("%s/blobs/%s/%d", c.InstanceName, hash, sizeBytes)
}

// ResourceNameWrite generates a valid write resource name.
func (c *Client) ResourceNameWrite(hash string, sizeBytes int64) string {
	return fmt.Sprintf("%s/uploads/%s/blobs/%s/%d", c.InstanceName, uuid.New(), hash, sizeBytes)
}

// GetDirectoryTree returns the entire directory tree rooted at the given digest (which must target
// a Directory stored in the CAS).
func (c *Client) GetDirectoryTree(ctx context.Context, d *repb.Digest) (result []*repb.Directory, err error) {
	pageTok := ""
	result = []*repb.Directory{}
	closure := func() error {
		// Use the low-level GetTree method to avoid retrying twice.
		stream, err := c.cas.GetTree(ctx, &repb.GetTreeRequest{
			InstanceName: c.InstanceName,
			RootDigest:   d,
			PageToken:    pageTok,
		})
		if err != nil {
			return err
		}

		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			pageTok = resp.NextPageToken
			result = append(result, resp.Directories...)
		}
		return nil
	}
	if err := c.Retrier.Do(ctx, closure); err != nil {
		return nil, err
	}
	return result, nil
}

// FlattenActionOutputs collects and flattens all the outputs of an action.
// It downloads the output directory metadata, if required, but not the leaf file blobs.
func (c *Client) FlattenActionOutputs(ctx context.Context, ar *repb.ActionResult) (map[string]*tree.Output, error) {
	outs := make(map[string]*tree.Output)
	for _, file := range ar.OutputFiles {
		outs[file.Path] = &tree.Output{
			Path:         file.Path,
			Digest:       digest.NewFromProtoUnvalidated(file.Digest),
			IsExecutable: file.IsExecutable,
		}
	}
	for _, sm := range ar.OutputFileSymlinks {
		outs[sm.Path] = &tree.Output{
			Path:          sm.Path,
			SymlinkTarget: sm.Target,
		}
	}
	for _, sm := range ar.OutputDirectorySymlinks {
		outs[sm.Path] = &tree.Output{
			Path:          sm.Path,
			SymlinkTarget: sm.Target,
		}
	}
	for _, dir := range ar.OutputDirectories {
		if blob, err := c.ReadBlob(ctx, digest.NewFromProtoUnvalidated(dir.TreeDigest)); err == nil {
			t := &repb.Tree{}
			if err := proto.Unmarshal(blob, t); err != nil {
				return nil, err
			}
			dirouts, err := tree.FlattenTree(t, dir.Path)
			if err != nil {
				return nil, err
			}
			for _, out := range dirouts {
				outs[out.Path] = out
			}
		}
	}
	return outs, nil
}

// DownloadActionOutputs downloads the output files and directories in the given action result.
func (c *Client) DownloadActionOutputs(ctx context.Context, resPb *repb.ActionResult, execRoot string) error {
	outs, err := c.FlattenActionOutputs(ctx, resPb)
	if err != nil {
		return err
	}
	// Remove the existing output directories before downloading.
	for _, dir := range resPb.OutputDirectories {
		if err := os.RemoveAll(filepath.Join(execRoot, dir.Path)); err != nil {
			return err
		}
	}
	var symlinks, copies []*tree.Output
	downloads := make(map[digest.Digest]*tree.Output)
	for _, out := range outs {
		path := filepath.Join(execRoot, out.Path)
		// TODO(olaola): fix the (upstream) bug that this doesn't download empty directory trees.
		if err := os.MkdirAll(filepath.Dir(path), os.FileMode(0777)); err != nil {
			return err
		}
		// We create the symbolic links after all regular downloads are finished, because dangling
		// links will not work.
		if out.SymlinkTarget != "" {
			symlinks = append(symlinks, out)
			continue
		}
		if _, ok := downloads[out.Digest]; ok {
			copies = append(copies, out)
		} else {
			downloads[out.Digest] = out
		}
	}
	if err := c.downloadFiles(ctx, execRoot, downloads); err != nil {
		return err
	}
	for _, out := range copies {
		if err := copyFile(execRoot, downloads[out.Digest].Path, out.Path); err != nil {
			return err
		}
	}
	for _, out := range symlinks {
		if err := os.Symlink(out.SymlinkTarget, filepath.Join(execRoot, out.Path)); err != nil {
			return err
		}
	}
	return nil
}

func copyFile(execRoot, from, to string) error {
	src := filepath.Join(execRoot, from)
	st, err := os.Stat(src)
	if err != nil {
		return err
	}

	if !st.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", src)
	}

	s, err := os.Open(src)
	if err != nil {
		return err
	}
	defer s.Close()

	dst := filepath.Join(execRoot, to)
	t, err := os.OpenFile(dst, os.O_RDWR|os.O_CREATE, st.Mode())
	if err != nil {
		return err
	}
	defer t.Close()
	_, err = io.Copy(t, s)
	return err
}

func (c *Client) downloadFiles(ctx context.Context, execRoot string, outputs map[digest.Digest]*tree.Output) error {
	if cap(c.casDownloaders) <= 0 {
		return fmt.Errorf("CASConcurrency should be at least 1")
	}
	const (
		logInterval = 25
	)

	var dgs []digest.Digest
	for dg := range outputs {
		dgs = append(dgs, dg)
	}

	log.V(2).Infof("%d items to download", len(dgs))
	var batches [][]digest.Digest
	if c.useBatchOps {
		batches = makeBatches(dgs)
	} else {
		log.V(2).Info("Downloading them individually")
		for i := range dgs {
			log.V(3).Infof("Creating single batch of blob %s", dgs[i])
			batches = append(batches, dgs[i:i+1])
		}
	}

	eg, eCtx := errgroup.WithContext(ctx)
	for i, batch := range batches {
		i, batch := i, batch     // https://golang.org/doc/faq#closures_and_goroutines
		c.casDownloaders <- true // Reserve an downloader thread.
		eg.Go(func() error {
			defer func() { <-c.casDownloaders }()
			if i%logInterval == 0 {
				log.V(2).Infof("%d batches left to download", len(batches)-i)
			}
			if len(batch) > 1 {
				log.V(3).Infof("Downloading batch of %d files", len(batch))
				bchMap, err := c.BatchDownloadBlobs(eCtx, batch)
				if err != nil {
					return err
				}
				for dg, data := range bchMap {
					out := outputs[dg]
					perm := os.FileMode(0644)
					if out.IsExecutable {
						perm = os.FileMode(0777)
					}
					if err := ioutil.WriteFile(filepath.Join(execRoot, out.Path), data, perm); err != nil {
						return err
					}
				}
			} else {
				out := outputs[batch[0]]
				path := filepath.Join(execRoot, out.Path)
				log.V(3).Infof("Downloading single file with digest %s to %s", out.Digest, path)
				if _, err := c.ReadBlobToFile(ctx, out.Digest, path); err != nil {
					return err
				}
				if out.IsExecutable {
					if err := os.Chmod(path, os.FileMode(0777)); err != nil {
						return err
					}
				}
			}
			if eCtx.Err() != nil {
				return eCtx.Err()
			}
			return nil
		})
	}

	log.V(3).Info("Waiting for remaining jobs")
	err := eg.Wait()
	log.V(3).Info("Done")
	return err
}
