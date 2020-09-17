// Main package for the remotetool binary.
//
// This tool supports common debugging operations concerning remotely executed
// actions:
// 1. Download a file or directory from remote cache by its digest.
// 2. Display details of a remotely executed action.
// 3. Download action results by the action digest.
//
// Example (download an action result from remote action cache):
// bazelisk run //go/cmd/remotetool -- \
//  --operation=download_action_result \
// 	--instance=$INSTANCE \
// 	--service remotebuildexecution.googleapis.com:443 \
// 	--alsologtostderr --v 1 \
// 	--credential_file $CRED_FILE \
// 	--digest=52a54724e6b3dff3bc44ef5dceb3aab5892f2fc7e37fce5aa6e16a7a266fbed6/147 \
// 	--path=`pwd`/tmp
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/tool"

	rflags "github.com/bazelbuild/remote-apis-sdks/go/pkg/flags"
	log "github.com/golang/glog"
)

// OpType denotes the type of operation to perform.
type OpType string

const (
	downloadActionResult OpType = "download_action_result"
	showAction           OpType = "show_action"
	downloadBlob         OpType = "download_blob"
	downloadDir          OpType = "download_dir"
)

var supportedOps = []OpType{
	downloadActionResult,
	showAction,
	downloadBlob,
	downloadDir,
}

var (
	operation  = flag.String("operation", "", fmt.Sprintf("Specifies the operation to perform. Supported values: %v", supportedOps))
	digest     = flag.String("digest", "", "Digest in <digest/size_bytes> format.")
	pathPrefix = flag.String("path", "", "Path to which outputs should be downloaded to.")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %v [-flags] -- --operation <op> arguments ...\n", path.Base(os.Args[0]))
		flag.PrintDefaults()
	}
	flag.Parse()
	if *operation == "" {
		log.Exitf("--operation must be specified.")
	}
	if *digest == "" {
		log.Exitf("--digest must be specified.")
	}

	ctx := context.Background()
	grpcClient, err := rflags.NewClientFromFlags(ctx)
	if err != nil {
		log.Exitf("error connecting to remote execution client: %v", err)
	}
	defer grpcClient.Close()
	c := &tool.Client{GrpcClient: grpcClient}

	switch OpType(*operation) {
	case downloadActionResult:
		if *pathPrefix == "" {
			log.Exitf("--path must be specified.")
		}
		if err := c.DownloadActionResult(ctx, *digest, *pathPrefix); err != nil {
			log.Exitf("error downloading action result for digest %v: %v", *digest, err)
		}

	case downloadBlob:
		res, err := c.DownloadBlob(ctx, *digest, *pathPrefix)
		if err != nil {
			log.Exitf("error downloading blob for digest %v: %v", *digest, err)
		}
		fmt.Fprintf(os.Stdout, res)

	case downloadDir:
		if *pathPrefix == "" {
			log.Exitf("--path must be specified.")
		}
		if err := c.DownloadDirectory(ctx, *digest, *pathPrefix); err != nil {
			log.Exitf("error downloading directory for digest %v: %v", *digest, err)
		}

	case showAction:
		res, err := c.ShowAction(ctx, *digest)
		if err != nil {
			log.Exitf("error fetching action %v: %v", *digest, err)
		}
		fmt.Fprintf(os.Stdout, res)

	default:
		log.Exitf("unsupported operation %v. Supported operations:\n%v", *operation, supportedOps)
	}
}
