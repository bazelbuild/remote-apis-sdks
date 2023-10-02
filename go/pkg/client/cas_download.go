package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/contextmd"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/filemetadata"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	log "github.com/golang/glog"
	"github.com/klauspost/compress/zstd"
	syncpool "github.com/mostynb/zstdpool-syncpool"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// DownloadFiles downloads the output files under |outDir|.
// It returns the number of logical and real bytes downloaded, which may be different from sum
// of sizes of the files due to dedupping and compression.
func (c *Client) DownloadFiles(ctx context.Context, outDir string, outputs map[digest.Digest]*TreeOutput) (*MovedBytesMetadata, error) {
	stats := &MovedBytesMetadata{}

	if !c.UnifiedDownloads {
		return c.downloadNonUnified(ctx, outDir, outputs)
	}
	count := len(outputs)
	if count == 0 {
		return stats, nil
	}
	meta, err := contextmd.ExtractMetadata(ctx)
	if err != nil {
		return stats, err
	}
	wait := make(chan *downloadResponse, count)
	for dg, out := range outputs {
		r := &downloadRequest{
			digest:  dg,
			context: ctx,
			outDir:  outDir,
			output:  out,
			meta:    meta,
			wait:    wait,
		}
		select {
		case <-ctx.Done():
			contextmd.Infof(ctx, log.Level(2), "Download canceled")
			return stats, ctx.Err()
		case c.casDownloadRequests <- r:
			continue
		}
	}

	// Wait for all downloads to finish.
	for count > 0 {
		select {
		case <-ctx.Done():
			contextmd.Infof(ctx, log.Level(2), "Download canceled")
			return stats, ctx.Err()
		case resp := <-wait:
			if resp.err != nil {
				return stats, resp.err
			}
			stats.addFrom(resp.stats)
			count--
		}
	}
	return stats, nil
}

// DownloadOutputs downloads the specified outputs. It returns the amount of downloaded bytes.
// It returns the number of logical and real bytes downloaded, which may be different from sum
// of sizes of the files due to dedupping and compression.
func (c *Client) DownloadOutputs(ctx context.Context, outs map[string]*TreeOutput, outDir string, cache filemetadata.Cache) (*MovedBytesMetadata, error) {
	var symlinks, copies []*TreeOutput
	downloads := make(map[digest.Digest]*TreeOutput)
	fullStats := &MovedBytesMetadata{}
	for _, out := range outs {
		path := filepath.Join(outDir, out.Path)
		if out.IsEmptyDirectory {
			if err := os.MkdirAll(path, c.DirMode); err != nil {
				return fullStats, err
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(path), c.DirMode); err != nil {
			return fullStats, err
		}
		// We create the symbolic links after all regular downloads are finished, because dangling
		// links will not work.
		if out.SymlinkTarget != "" {
			symlinks = append(symlinks, out)
			continue
		}
		if _, ok := downloads[out.Digest]; ok {
			copies = append(copies, out)
			// All copies are effectivelly cached
			fullStats.Requested += out.Digest.Size
			fullStats.Cached += out.Digest.Size
		} else {
			downloads[out.Digest] = out
		}
	}
	stats, err := c.DownloadFiles(ctx, outDir, downloads)
	fullStats.addFrom(stats)
	if err != nil {
		return fullStats, err
	}

	for _, output := range downloads {
		path := output.Path
		md := &filemetadata.Metadata{
			Digest:       output.Digest,
			IsExecutable: output.IsExecutable,
		}
		absPath := path
		if !filepath.IsAbs(absPath) {
			absPath = filepath.Join(outDir, absPath)
		}
		if err := cache.Update(absPath, md); err != nil {
			return fullStats, err
		}
	}
	for _, out := range copies {
		perm := c.RegularMode
		if out.IsExecutable {
			perm = c.ExecutableMode
		}
		src := downloads[out.Digest]
		if src.IsEmptyDirectory {
			return fullStats, fmt.Errorf("unexpected empty directory: %s", src.Path)
		}
		if err := copyFile(outDir, outDir, src.Path, out.Path, perm); err != nil {
			return fullStats, err
		}
	}
	for _, out := range symlinks {
		if err := os.Symlink(out.SymlinkTarget, filepath.Join(outDir, out.Path)); err != nil {
			return fullStats, err
		}
	}
	return fullStats, nil
}

// DownloadDirectory downloads the entire directory of given digest.
// It returns the number of logical and real bytes downloaded, which may be different from sum
// of sizes of the files due to dedupping and compression.
func (c *Client) DownloadDirectory(ctx context.Context, d digest.Digest, outDir string, cache filemetadata.Cache) (map[string]*TreeOutput, *MovedBytesMetadata, error) {
	dir := &repb.Directory{}
	stats := &MovedBytesMetadata{}

	protoStats, err := c.ReadProto(ctx, d, dir)
	stats.addFrom(protoStats)
	if err != nil {
		return nil, stats, fmt.Errorf("digest %v cannot be mapped to a directory proto: %v", d, err)
	}

	dirs, err := c.GetDirectoryTree(ctx, d.ToProto())
	if err != nil {
		return nil, stats, err
	}

	outputs, err := c.FlattenTree(&repb.Tree{
		Root:     dir,
		Children: dirs,
	}, "")
	if err != nil {
		return nil, stats, err
	}

	outStats, err := c.DownloadOutputs(ctx, outputs, outDir, cache)
	stats.addFrom(outStats)
	return outputs, stats, err
}

// zstdDecoder is a shared instance that should only be used in stateless mode, i.e. only by calling DecodeAll()
var zstdDecoder, _ = zstd.NewReader(nil)

// CompressedBlobInfo is primarily used to store stats about compressed blob size
// in addition to the actual blob data.
type CompressedBlobInfo struct {
	CompressedSize int64
	Data           []byte
}

func (c *Client) BatchDownloadBlobsWithStats(ctx context.Context, dgs []digest.Digest) (map[digest.Digest]CompressedBlobInfo, error) {
	if len(dgs) > int(c.MaxBatchDigests) {
		return nil, fmt.Errorf("batch read of %d total blobs exceeds maximum of %d", len(dgs), c.MaxBatchDigests)
	}
	req := &repb.BatchReadBlobsRequest{InstanceName: c.InstanceName}
	if c.useBatchCompression {
		req.AcceptableCompressors = []repb.Compressor_Value{repb.Compressor_ZSTD}
	}
	var sz int64
	foundEmpty := false
	for _, dg := range dgs {
		if dg.Size == 0 {
			foundEmpty = true
			continue
		}
		sz += int64(dg.Size)
		req.Digests = append(req.Digests, dg.ToProto())
	}
	if sz > int64(c.MaxBatchSize) {
		return nil, fmt.Errorf("batch read of %d total bytes exceeds maximum of %d", sz, c.MaxBatchSize)
	}
	res := make(map[digest.Digest]CompressedBlobInfo)
	if foundEmpty {
		res[digest.Empty] = CompressedBlobInfo{}
	}
	opts := c.RPCOpts()
	closure := func() error {
		var resp *repb.BatchReadBlobsResponse
		err := c.CallWithTimeout(ctx, "BatchReadBlobs", func(ctx context.Context) (e error) {
			resp, e = c.cas.BatchReadBlobs(ctx, req, opts...)
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
				CompressedSize := len(r.Data)
				switch r.Compressor {
				case repb.Compressor_IDENTITY:
					// do nothing
				case repb.Compressor_ZSTD:
					CompressedSize = len(r.Data)
					b, err := zstdDecoder.DecodeAll(r.Data, nil)
					if err != nil {
						errDg = r.Digest
						errMsg = err.Error()
						continue
					}
					r.Data = b
				default:
					errDg = r.Digest
					errMsg = fmt.Sprintf("blob returned with unsupported compressor %s", r.Compressor)
					continue
				}
				bi := CompressedBlobInfo{
					CompressedSize: int64(CompressedSize),
					Data:           r.Data,
				}
				res[digest.NewFromProtoUnvalidated(r.Digest)] = bi
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

// BatchDownloadBlobs downloads a number of blobs from the CAS to memory. They must collectively be below the
// maximum total size for a batch read, which is about 4 MB (see MaxBatchSize). Digests must be
// computed in advance by the caller. In case multiple errors occur during the blob read, the
// last error will be returned.
func (c *Client) BatchDownloadBlobs(ctx context.Context, dgs []digest.Digest) (map[digest.Digest][]byte, error) {
	biRes, err := c.BatchDownloadBlobsWithStats(ctx, dgs)
	res := make(map[digest.Digest][]byte)
	for dg, bi := range biRes {
		res[dg] = bi.Data
	}
	return res, err
}

// ReadBlob fetches a blob from the CAS into a byte slice.
// Returns the size of the blob and the amount of bytes moved through the wire.
func (c *Client) ReadBlob(ctx context.Context, d digest.Digest) ([]byte, *MovedBytesMetadata, error) {
	return c.readBlob(ctx, d, 0, 0)
}

// ReadBlobRange fetches a partial blob from the CAS into a byte slice, starting from offset bytes
// and including at most limit bytes (or no limit if limit==0). The offset must be non-negative and
// no greater than the size of the entire blob. The limit must not be negative, but offset+limit may
// be greater than the size of the entire blob.
func (c *Client) ReadBlobRange(ctx context.Context, d digest.Digest, offset, limit int64) ([]byte, *MovedBytesMetadata, error) {
	return c.readBlob(ctx, d, offset, limit)
}

// ReadBlobToFile fetches a blob with a provided digest name from the CAS, saving it into a file.
// It returns the number of bytes read.
func (c *Client) ReadBlobToFile(ctx context.Context, d digest.Digest, fpath string) (*MovedBytesMetadata, error) {
	f, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, c.RegularMode)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return c.readBlobStreamed(ctx, d, 0, 0, f)
}

// ReadProto reads a blob from the CAS and unmarshals it into the given message.
// Returns the size of the proto and the amount of bytes moved through the wire.
func (c *Client) ReadProto(ctx context.Context, d digest.Digest, msg proto.Message) (*MovedBytesMetadata, error) {
	bytes, stats, err := c.ReadBlob(ctx, d)
	if err != nil {
		return stats, err
	}
	return stats, proto.Unmarshal(bytes, msg)
}

// Returns the size of the blob and the amount of bytes moved through the wire.
func (c *Client) readBlob(ctx context.Context, dg digest.Digest, offset, limit int64) ([]byte, *MovedBytesMetadata, error) {
	// int might be 32-bit, in which case we could have a blob whose size is representable in int64
	// but not int32, and thus can't fit in a slice. We can check for this by casting and seeing if
	// the result is negative, since 32 bits is big enough wrap all out-of-range values of int64 to
	// negative numbers. If int is 64-bits, the cast is a no-op and so the condition will always fail.
	if int(dg.Size) < 0 {
		return nil, nil, fmt.Errorf("digest size %d is too big to fit in a byte slice", dg.Size)
	}
	if offset > dg.Size {
		return nil, nil, fmt.Errorf("offset %d out of range for a blob of size %d", offset, dg.Size)
	}
	if offset < 0 {
		return nil, nil, fmt.Errorf("offset %d may not be negative", offset)
	}
	if limit < 0 {
		return nil, nil, fmt.Errorf("limit %d may not be negative", limit)
	}
	sz := dg.Size - offset
	if limit > 0 && limit < sz {
		sz = limit
	}
	// Pad size so bytes.Buffer does not reallocate.
	buf := bytes.NewBuffer(make([]byte, 0, sz+bytes.MinRead))
	stats, err := c.readBlobStreamed(ctx, dg, offset, limit, buf)
	return buf.Bytes(), stats, err
}

func (c *Client) readBlobStreamed(ctx context.Context, d digest.Digest, offset, limit int64, w io.Writer) (*MovedBytesMetadata, error) {
	stats := &MovedBytesMetadata{}
	stats.Requested = d.Size
	if d.Size == 0 {
		// Do not download empty blobs.
		return stats, nil
	}
	sz := d.Size - offset
	if limit > 0 && limit < sz {
		sz = limit
	}
	wt := newWriteTracker(w)
	defer func() { stats.LogicalMoved = wt.n }()
	closure := func() (err error) {
		name, wc, done, e := c.maybeCompressReadBlob(d, wt)
		if e != nil {
			return e
		}

		defer func() {
			errC := wc.Close()
			errD := <-done
			close(done)

			if err == nil && errC != nil {
				err = errC
			} else if errC != nil {
				log.Errorf("Failed to close writer: %v", errC)
			}
			if err == nil && errD != nil {
				err = errD
			} else if errD != nil {
				log.Errorf("Failed to finalize writing blob: %v", errD)
			}
		}()

		wireBytes, err := c.readStreamed(ctx, name, offset+wt.n, limit, wc)
		stats.RealMoved += wireBytes
		if err != nil {
			return err
		}
		return nil
	}
	// Only retry on transient backend issues.
	if err := c.Retrier.Do(ctx, closure); err != nil {
		return stats, err
	}
	if wt.n != sz {
		return stats, fmt.Errorf("partial read of digest %s returned %d bytes, expected %d bytes", d, wt.n, sz)
	}

	// Incomplete reads only, since we can't reliably calculate hash without the full blob
	if d.Size == sz {
		// Signal for writeTracker to take the digest of the data.
		if err := wt.Close(); err != nil {
			return stats, err
		}
		// Wait for the digest to be ready.
		if err := <-wt.ready; err != nil {
			return stats, err
		}
		close(wt.ready)
		if wt.dg != d {
			return stats, fmt.Errorf("calculated digest %s != expected digest %s", wt.dg, d)
		}
	}

	return stats, nil
}

// GetDirectoryTree returns the entire directory tree rooted at the given digest (which must target
// a Directory stored in the CAS).
func (c *Client) GetDirectoryTree(ctx context.Context, d *repb.Digest) (result []*repb.Directory, err error) {
	if digest.NewFromProtoUnvalidated(d).IsEmpty() {
		return []*repb.Directory{&repb.Directory{}}, nil
	}
	pageTok := ""
	result = []*repb.Directory{}
	closure := func(ctx context.Context) error {
		stream, err := c.GetTree(ctx, &repb.GetTreeRequest{
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
	if err := c.Retrier.Do(ctx, func() error { return c.CallWithTimeout(ctx, "GetTree", closure) }); err != nil {
		return nil, err
	}
	return result, nil
}

// FlattenActionOutputs collects and flattens all the outputs of an action.
// It downloads the output directory metadata, if required, but not the leaf file blobs.
func (c *Client) FlattenActionOutputs(ctx context.Context, ar *repb.ActionResult) (map[string]*TreeOutput, error) {
	outs := make(map[string]*TreeOutput)
	for _, file := range ar.OutputFiles {
		outs[file.Path] = &TreeOutput{
			Path:         file.Path,
			Digest:       digest.NewFromProtoUnvalidated(file.Digest),
			IsExecutable: file.IsExecutable,
		}
	}
	for _, sm := range ar.OutputFileSymlinks {
		outs[sm.Path] = &TreeOutput{
			Path:          sm.Path,
			SymlinkTarget: sm.Target,
		}
	}
	for _, sm := range ar.OutputDirectorySymlinks {
		outs[sm.Path] = &TreeOutput{
			Path:          sm.Path,
			SymlinkTarget: sm.Target,
		}
	}
	for _, dir := range ar.OutputDirectories {
		t := &repb.Tree{}
		if _, err := c.ReadProto(ctx, digest.NewFromProtoUnvalidated(dir.TreeDigest), t); err != nil {
			return nil, err
		}
		dirouts, err := c.FlattenTree(t, dir.Path)
		if err != nil {
			return nil, err
		}
		for _, out := range dirouts {
			outs[out.Path] = out
		}
	}
	return outs, nil
}

// DownloadActionOutputs downloads the output files and directories in the given action result. It returns the amount of downloaded bytes.
// It returns the number of logical and real bytes downloaded, which may be different from sum
// of sizes of the files due to dedupping and compression.
func (c *Client) DownloadActionOutputs(ctx context.Context, resPb *repb.ActionResult, outDir string, cache filemetadata.Cache) (*MovedBytesMetadata, error) {
	outs, err := c.FlattenActionOutputs(ctx, resPb)
	if err != nil {
		return nil, err
	}
	// Remove the existing output directories before downloading.
	for _, dir := range resPb.OutputDirectories {
		if err := os.RemoveAll(filepath.Join(outDir, dir.Path)); err != nil {
			return nil, err
		}
	}
	return c.DownloadOutputs(ctx, outs, outDir, cache)
}

var decoderInit sync.Once
var decoders *sync.Pool

// NewCompressedWriteBuffer creates wraps a io.Writer contained compressed contents to write
// decompressed contents.
func NewCompressedWriteBuffer(w io.Writer) (io.WriteCloser, chan error, error) {
	// Our Bytestream abstraction uses a Writer so that the bytestream interface can "write"
	// the data upstream. However, the zstd library only has an interface from a reader.
	// Instead of writing a different bytestream version that returns a reader, we're piping
	// the writer data.
	r, nw := io.Pipe()

	decoderInit.Do(func() {
		decoders = syncpool.NewDecoderPool(zstd.WithDecoderConcurrency(1))
	})

	decdIntf := decoders.Get()
	decoderW, ok := decdIntf.(*syncpool.DecoderWrapper)
	if !ok || decoderW == nil {
		return nil, nil, fmt.Errorf("failed creating new decoder")
	}

	if err := decoderW.Reset(r); err != nil {
		return nil, nil, err
	}

	done := make(chan error)
	go func() {
		// WriteTo will block until the reader is closed - or, in this
		// case, the pipe writer, so we have to launch our compressor in a
		// separate thread. As such, we also need a way to signal the main
		// thread that the decoding has finished - which will have some delay
		// from the last Write call.
		_, err := decoderW.WriteTo(w)
		if err != nil {
			// Because WriteTo returned early, the pipe writers still
			// have to go somewhere or they'll block execution.
			io.Copy(io.Discard, r)
		}
		// DecoderWrapper.Close moves the decoder back to the Pool.
		decoderW.Close()
		done <- err
	}()

	return nw, done, nil
}

// writerTracker is useful as an midware before writing to a Read caller's
// underlying data. Since cas.go should be responsible for sanity checking data,
// and potentially having to re-open files on disk to do a checking earlier
// on the call stack, we dup the writes through a digest creator and track
// how much data was written.
type writerTracker struct {
	w  io.Writer
	pw *io.PipeWriter
	dg digest.Digest
	// Tracked independently of the digest as we might want to retry
	// on partial reads.
	n     int64
	ready chan error
}

func newWriteTracker(w io.Writer) *writerTracker {
	pr, pw := io.Pipe()
	wt := &writerTracker{
		pw:    pw,
		w:     w,
		ready: make(chan error, 1),
		n:     0,
	}

	go func() {
		var err error
		wt.dg, err = digest.NewFromReader(pr)
		wt.ready <- err
	}()

	return wt
}

func (wt *writerTracker) Write(p []byte) (int, error) {
	// Any error on this write will be reflected on the
	// pipe reader end when trying to calculate the digest.
	// Additionally, if we are not downloading the entire
	// blob, we can't even verify the digest to begin with.
	// So we can ignore errors on this pipewriter.
	wt.pw.Write(p)
	n, err := wt.w.Write(p)
	wt.n += int64(n)
	return n, err
}

// Close closes the pipe - which triggers the end of the
// digest creation.
func (wt *writerTracker) Close() error {
	return wt.pw.Close()
}

type downloadRequest struct {
	digest digest.Digest
	outDir string
	// TODO(olaola): use channels for cancellations instead of embedding download context.
	context context.Context
	output  *TreeOutput
	meta    *contextmd.Metadata
	wait    chan<- *downloadResponse
}

type downloadResponse struct {
	stats *MovedBytesMetadata
	err   error
}

func (c *Client) downloadProcessor(ctx context.Context) {
	var buffer []*downloadRequest
	ticker := time.NewTicker(time.Duration(c.UnifiedDownloadTickDuration))
	for {
		select {
		case ch, ok := <-c.casDownloadRequests:
			if !ok {
				// Client is exiting. Notify remaining downloads to prevent deadlocks.
				ticker.Stop()
				if buffer != nil {
					for _, r := range buffer {
						r.wait <- &downloadResponse{err: context.Canceled}
					}
				}
				return
			}
			buffer = append(buffer, ch)
			if len(buffer) >= int(c.UnifiedDownloadBufferSize) {
				c.download(ctx, buffer)
				buffer = nil
			}
		case <-ticker.C:
			if buffer != nil {
				c.download(ctx, buffer)
				buffer = nil
			}
		}
	}
}

func (c *Client) download(ctx context.Context, data []*downloadRequest) {
	// It is possible to have multiple same files download to different locations.
	// This will download once and copy to the other locations.
	reqs := make(map[digest.Digest][]*downloadRequest)
	var metas []*contextmd.Metadata
	for _, r := range data {
		rs := reqs[r.digest]
		rs = append(rs, r)
		reqs[r.digest] = rs
		metas = append(metas, r.meta)
	}

	var dgs []digest.Digest

	if bool(c.useBatchOps) && bool(c.UtilizeLocality) {
		paths := make([]*TreeOutput, 0, len(data))
		for _, r := range data {
			paths = append(paths, r.output)
		}

		// This is to utilize locality in disk when writing files.
		sort.Slice(paths, func(i, j int) bool {
			return paths[i].Path < paths[j].Path
		})

		for _, path := range paths {
			dgs = append(dgs, path.Digest)
		}
	} else {
		for dg := range reqs {
			dgs = append(dgs, dg)
		}
	}

	unifiedMeta := contextmd.MergeMetadata(metas...)
	var err error
	if unifiedMeta.ActionID != "" {
		ctx, err = contextmd.WithMetadata(ctx, unifiedMeta)
	}
	if err != nil {
		afterDownload(dgs, reqs, map[digest.Digest]*MovedBytesMetadata{}, err)
		return
	}

	contextmd.Infof(ctx, log.Level(2), "%d digests to download (%d reqs)", len(dgs), len(reqs))
	var batches [][]digest.Digest
	if c.useBatchOps {
		batches = c.makeBatches(ctx, dgs, !bool(c.UtilizeLocality))
	} else {
		contextmd.Infof(ctx, log.Level(2), "Downloading them individually")
		for i := range dgs {
			contextmd.Infof(ctx, log.Level(3), "Creating single batch of blob %s", dgs[i])
			batches = append(batches, dgs[i:i+1])
		}
	}

	for i, batch := range batches {
		i, batch := i, batch // https://golang.org/doc/faq#closures_and_goroutines
		go func() {
			if c.casDownloaders.Acquire(ctx, 1) == nil {
				defer c.casDownloaders.Release(1)
			}
			if i%logInterval == 0 {
				contextmd.Infof(ctx, log.Level(2), "%d batches left to download", len(batches)-i)
			}
			if len(batch) > 1 {
				c.downloadBatch(ctx, batch, reqs)
			} else {
				rs := reqs[batch[0]]
				downloadCtx := ctx
				if len(rs) == 1 {
					// We have only one download request for this digest.
					// Download on same context as the issuing request, to support proper cancellation.
					downloadCtx = rs[0].context
				}
				c.downloadSingle(downloadCtx, batch[0], reqs)
			}
		}()
	}
}

func (c *Client) downloadBatch(ctx context.Context, batch []digest.Digest, reqs map[digest.Digest][]*downloadRequest) {
	contextmd.Infof(ctx, log.Level(3), "Downloading batch of %d files", len(batch))
	bchMap, err := c.BatchDownloadBlobsWithStats(ctx, batch)
	if err != nil {
		afterDownload(batch, reqs, map[digest.Digest]*MovedBytesMetadata{}, err)
		return
	}
	for _, dg := range batch {
		bi := bchMap[dg]
		stats := &MovedBytesMetadata{
			Requested:    dg.Size,
			LogicalMoved: dg.Size,
			// There's no compression for batch requests, and there's no such thing as "partial" data for
			// a blob since they're all inlined in the response.
			RealMoved: bi.CompressedSize,
		}
		for i, r := range reqs[dg] {
			perm := c.RegularMode
			if r.output.IsExecutable {
				perm = c.ExecutableMode
			}
			// bytesMoved will be zero for error cases.
			// We only report it to the first client to prevent double accounting.
			r.wait <- &downloadResponse{
				stats: stats,
				err:   os.WriteFile(filepath.Join(r.outDir, r.output.Path), bi.Data, perm),
			}
			if i == 0 {
				// Prevent races by not writing to the original stats.
				newStats := &MovedBytesMetadata{}
				newStats.Requested = stats.Requested
				newStats.Cached = stats.LogicalMoved
				newStats.RealMoved = 0
				newStats.LogicalMoved = 0

				stats = newStats
			}
		}
	}
}

func (c *Client) downloadSingle(ctx context.Context, dg digest.Digest, reqs map[digest.Digest][]*downloadRequest) (err error) {
	// The lock is released when all file copies are finished.
	// We cannot release the lock after each individual file copy, because
	// the caller might move the file, and we don't have the contents in memory.
	bytesMoved := map[digest.Digest]*MovedBytesMetadata{}
	defer func() { afterDownload([]digest.Digest{dg}, reqs, bytesMoved, err) }()
	rs := reqs[dg]
	if len(rs) < 1 {
		return fmt.Errorf("Failed precondition: cannot find %v in reqs map", dg)
	}
	r := rs[0]
	rs = rs[1:]
	path := filepath.Join(r.outDir, r.output.Path)
	contextmd.Infof(ctx, log.Level(3), "Downloading single file with digest %s to %s", r.output.Digest, path)
	stats, err := c.ReadBlobToFile(ctx, r.output.Digest, path)
	if err != nil {
		return err
	}
	bytesMoved[r.output.Digest] = stats
	if r.output.IsExecutable {
		if err := os.Chmod(path, c.ExecutableMode); err != nil {
			return err
		}
	}
	for _, cp := range rs {
		perm := c.RegularMode
		if cp.output.IsExecutable {
			perm = c.ExecutableMode
		}
		if err := copyFile(r.outDir, cp.outDir, r.output.Path, cp.output.Path, perm); err != nil {
			return err
		}
	}
	return err
}

// This is a legacy function used only when UnifiedDownloads=false.
// It will be removed when UnifiedDownloads=true is stable.
// Returns the number of logical and real bytes downloaded, which may be
// different from sum of sizes of the files due to compression.
func (c *Client) downloadNonUnified(ctx context.Context, outDir string, outputs map[digest.Digest]*TreeOutput) (*MovedBytesMetadata, error) {
	var dgs []digest.Digest
	// statsMu protects stats across threads.
	statsMu := sync.Mutex{}
	fullStats := &MovedBytesMetadata{}

	if bool(c.useBatchOps) && bool(c.UtilizeLocality) {
		paths := make([]*TreeOutput, 0, len(outputs))
		for _, output := range outputs {
			paths = append(paths, output)
		}

		// This is to utilize locality in disk when writing files.
		sort.Slice(paths, func(i, j int) bool {
			return paths[i].Path < paths[j].Path
		})

		for _, path := range paths {
			dgs = append(dgs, path.Digest)
			fullStats.Requested += path.Digest.Size
		}
	} else {
		for dg := range outputs {
			dgs = append(dgs, dg)
			fullStats.Requested += dg.Size
		}
	}

	contextmd.Infof(ctx, log.Level(2), "%d items to download", len(dgs))
	var batches [][]digest.Digest
	if c.useBatchOps {
		batches = c.makeBatches(ctx, dgs, !bool(c.UtilizeLocality))
	} else {
		contextmd.Infof(ctx, log.Level(2), "Downloading them individually")
		for i := range dgs {
			contextmd.Infof(ctx, log.Level(3), "Creating single batch of blob %s", dgs[i])
			batches = append(batches, dgs[i:i+1])
		}
	}

	eg, eCtx := errgroup.WithContext(ctx)
	for i, batch := range batches {
		i, batch := i, batch // https://golang.org/doc/faq#closures_and_goroutines
		eg.Go(func() error {
			if err := c.casDownloaders.Acquire(eCtx, 1); err != nil {
				return err
			}
			defer c.casDownloaders.Release(1)
			if i%logInterval == 0 {
				contextmd.Infof(ctx, log.Level(2), "%d batches left to download", len(batches)-i)
			}
			if len(batch) > 1 {
				contextmd.Infof(ctx, log.Level(3), "Downloading batch of %d files", len(batch))
				bchMap, err := c.BatchDownloadBlobsWithStats(eCtx, batch)
				for _, dg := range batch {
					bi := bchMap[dg]
					out := outputs[dg]
					perm := c.RegularMode
					if out.IsExecutable {
						perm = c.ExecutableMode
					}
					if err := os.WriteFile(filepath.Join(outDir, out.Path), bi.Data, perm); err != nil {
						return err
					}
					statsMu.Lock()
					fullStats.LogicalMoved += int64(len(bi.Data))
					fullStats.RealMoved += bi.CompressedSize
					statsMu.Unlock()
				}
				if err != nil {
					return err
				}
			} else {
				out := outputs[batch[0]]
				path := filepath.Join(outDir, out.Path)
				contextmd.Infof(ctx, log.Level(3), "Downloading single file with digest %s to %s", out.Digest, path)
				stats, err := c.ReadBlobToFile(ctx, out.Digest, path)
				if err != nil {
					return err
				}
				statsMu.Lock()
				fullStats.addFrom(stats)
				statsMu.Unlock()
				if out.IsExecutable {
					if err := os.Chmod(path, c.ExecutableMode); err != nil {
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

	contextmd.Infof(ctx, log.Level(3), "Waiting for remaining jobs")
	err := eg.Wait()
	contextmd.Infof(ctx, log.Level(3), "Done")
	return fullStats, err
}

func afterDownload(batch []digest.Digest, reqs map[digest.Digest][]*downloadRequest, bytesMoved map[digest.Digest]*MovedBytesMetadata, err error) {
	if err != nil {
		log.Errorf("Error downloading %v: %v", batch[0], err)
	}
	for _, dg := range batch {
		rs, ok := reqs[dg]
		if !ok {
			log.Errorf("Precondition failed: download request not found in input %v.", dg)
		}
		stats, ok := bytesMoved[dg]
		if !ok {
			log.Errorf("Internal tool error - matching map entry")
			continue
		}
		// If there's no real bytes moved it likely means there was an error moving these.
		for i, r := range rs {
			// bytesMoved will be zero for error cases.
			// We only report it to the first client to prevent double accounting.
			r.wait <- &downloadResponse{stats: stats, err: err}
			if i == 0 {
				// Prevent races by not writing to the original stats.
				newStats := &MovedBytesMetadata{}
				newStats.Requested = stats.Requested
				newStats.Cached = stats.LogicalMoved
				newStats.RealMoved = 0
				newStats.LogicalMoved = 0

				stats = newStats
			}
		}
	}
}

type writeDummyCloser struct {
	io.Writer
}

func (w *writeDummyCloser) Close() error { return nil }

func (c *Client) resourceNameRead(hash string, sizeBytes int64) string {
	rname, _ := c.ResourceName("blobs", hash, strconv.FormatInt(sizeBytes, 10))
	return rname
}

// TODO(rubensf): Converge compressor to proto in https://github.com/bazelbuild/remote-apis/pull/168 once
// that gets merged in.
func (c *Client) resourceNameCompressedRead(hash string, sizeBytes int64) string {
	rname, _ := c.ResourceName("compressed-blobs", "zstd", hash, strconv.FormatInt(sizeBytes, 10))
	return rname
}

// maybeCompressReadBlob will, depending on the client configuration, set the blobs to be
// read compressed. It returns the appropriate resource name.
func (c *Client) maybeCompressReadBlob(d digest.Digest, w io.Writer) (string, io.WriteCloser, chan error, error) {
	if !c.shouldCompress(d.Size) {
		// If we aren't compressing the data, theere's nothing to wait on.
		dummyDone := make(chan error, 1)
		dummyDone <- nil
		return c.resourceNameRead(d.Hash, d.Size), &writeDummyCloser{w}, dummyDone, nil
	}
	cw, done, err := NewCompressedWriteBuffer(w)
	if err != nil {
		return "", nil, nil, err
	}
	return c.resourceNameCompressedRead(d.Hash, d.Size), cw, done, nil
}
