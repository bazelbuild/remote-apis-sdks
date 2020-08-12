// Package tool provides implementation of the debugging related operations
// supported by go/cmd/remotetool package.
package tool

import (
	"context"
	"fmt"
	"io/ioutil"
	"path/filepath"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/filemetadata"

	rc "github.com/bazelbuild/remote-apis-sdks/go/pkg/client"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	log "github.com/golang/glog"
)

const (
	stdoutFile = "stdout"
	stderrFile = "stderr"
)

// Client is a remote execution client.
type Client struct {
	GrpcClient *rc.Client
}

// DownloadActionResult downloads the action result of the given action digest
// if it exists in the RBE cache.
func (c *Client) DownloadActionResult(ctx context.Context, actionDigest, pathPrefix string) error {
	acDg, err := digest.NewFromString(actionDigest)
	if err != nil {
		return err
	}
	d := &repb.Digest{
		Hash:      acDg.Hash,
		SizeBytes: acDg.Size,
	}
	resPb, err := c.GrpcClient.CheckActionCache(ctx, d)
	if err != nil {
		return err
	}
	if resPb == nil {
		return fmt.Errorf("action digest %v not found in cache", d)
	}

	log.Infof("Downloading action results of %v to %v.", actionDigest, pathPrefix)
	// We don't really need an in-memory filemetadata cache for debugging operations.
	noopCache := filemetadata.NewNoopCache()
	if err := c.GrpcClient.DownloadActionOutputs(ctx, resPb, pathPrefix, noopCache); err != nil {
		log.Errorf("Failed downloading action outputs: %v.", err)
	}

	// We have not requested for stdout/stderr to be inlined in GetActionResult, so the server
	// should be returning the digest instead of sending raw data.
	outMsgs := map[string]*repb.Digest{
		filepath.Join(pathPrefix, stdoutFile): resPb.StdoutDigest,
		filepath.Join(pathPrefix, stderrFile): resPb.StderrDigest,
	}
	for path, reDg := range outMsgs {
		if reDg == nil {
			continue
		}
		dg := &digest.Digest{
			Hash: reDg.GetHash(),
			Size: reDg.GetSizeBytes(),
		}
		log.Infof("Downloading stdout/stderr to %v.", path)
		bytes, err := c.GrpcClient.ReadBlob(ctx, *dg)
		if err != nil {
			log.Errorf("Unable to read blob for %v with digest %v.", path, dg)
		}
		if err := ioutil.WriteFile(path, bytes, 0644); err != nil {
			log.Errorf("Unable to write output of digest %v to file %v.", dg, path)
		}
	}
	log.Infof("Successfully downloaded results of %v to %v.", actionDigest, pathPrefix)
	return nil
}
