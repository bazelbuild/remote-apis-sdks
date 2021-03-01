package tool

import (
	"context"
	"sync"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/uploadinfo"

	rc "github.com/bazelbuild/remote-apis-sdks/go/pkg/client"
	log "github.com/golang/glog"
)

type parallelUploader struct {
	queue   chan *uploadinfo.Entry
	closeFn func() error
}

// Enqueue adds a new file to the upload queue and returns any error occuring
// during enqueueing. It does NOT return error that happen during upload.
func (u *parallelUploader) Enqueue(path string) error {
	dg, err := digest.NewFromFile(path)
	if err != nil {
		return err
	}

	log.Infof("Enqueued blob of '%v' from '%v' for uploading", dg, path)
	u.queue <- uploadinfo.EntryFromFile(dg, path)

	return nil
}

// CloseAndWait closes the uploader and waits for all uploads to finish.
func (u *parallelUploader) CloseAndWait() error {
	return u.closeFn()
}

func newParallelUploader(ctx context.Context, grpcClient *rc.Client, concurrency uint64) *parallelUploader {
	queue := make(chan *uploadinfo.Entry, 10*concurrency)
	var wg sync.WaitGroup

	for i := uint64(0); i < concurrency; i++ {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Done()

			log.Infof("Starting uploader %v", id)
			defer log.Infof("Stopping uploader %v", id)

			for {
				select {
				case <-ctx.Done():
					return

				case ue, ok := <-queue:
					if !ok {
						return
					}

					if ue.IsFile() {
						log.Infof("Uploader %v: Uploading blob of '%v' from '%v'", id, ue.Digest, ue.Path)
					} else {
						log.Infof("Uploader %v: Uploading blob of '%v'", id, ue.Digest)
					}
					if _, _, err := grpcClient.UploadIfMissing(ctx, ue); err != nil {
						if ue.IsFile() {
							log.Errorf("Uploader %v: Error uploading blob of '%v' from '%v': %v", id, ue.Digest, ue.Path, err)
						} else {
							log.Errorf("Uploader %v: Error uploading blob of '%v': %v", id, ue.Digest, err)
						}
					}
				}
			}
		}(i)
	}

	return &parallelUploader{
		queue: queue,
		closeFn: func() error {
			close(queue)
			wg.Wait()
			return nil
		},
	}
}
