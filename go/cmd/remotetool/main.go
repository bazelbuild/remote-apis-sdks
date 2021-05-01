// Main package for the remotetool binary.
//
// This tool supports common debugging operations concerning remotely executed
// actions:
// 1. Download a file or directory from remote cache by its digest.
// 2. Display details of a remotely executed action.
// 3. Download action results by the action digest.
// 4. Re-execute remote action (with optional inputs override).
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

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/outerr"
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
	reexecuteAction      OpType = "reexecute_action"
	checkDeterminism     OpType = "check_determinism"
	uploadBlob           OpType = "upload_blob"
	uploadTree           OpType = "upload_tree"
)

var supportedOps = []OpType{
	downloadActionResult,
	showAction,
	downloadBlob,
	downloadDir,
	reexecuteAction,
	checkDeterminism,
	uploadBlob,
	uploadTree,
}

var (
	operation         = flag.String("operation", "", fmt.Sprintf("Specifies the operation to perform. Supported values: %v", supportedOps))
	digest            = flag.String("digest", "", "Digest in <digest/size_bytes> format.")
	pathPrefix        = flag.String("path", "", "Path to which outputs should be downloaded to.")
	inputRoot         = flag.String("input_root", "", "For reexecute_action: if specified, override the action inputs with the specified input root.")
	execAttempts      = flag.Int("exec_attempts", 10, "For check_determinism: the number of times to remotely execute the action and check for mismatches.")
	uploadConcurrency = flag.Uint64("upload_concurrency", 1, "The number of concurrent uploads.")
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
	if *execAttempts <= 0 {
		log.Exitf("--exec_attempts must be >= 1.")
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
		if err := c.DownloadActionResult(ctx, getDigestFlag(), getPathFlag()); err != nil {
			log.Exitf("error downloading action result for digest %v: %v", getDigestFlag(), err)
		}

	case downloadBlob:
		res, err := c.DownloadBlob(ctx, getDigestFlag(), getPathFlag())
		if err != nil {
			log.Exitf("error downloading blob for digest %v: %v", getDigestFlag(), err)
		}
		os.Stdout.Write([]byte(res))

	case downloadDir:
		if err := c.DownloadDirectory(ctx, getDigestFlag(), getPathFlag()); err != nil {
			log.Exitf("error downloading directory for digest %v: %v", getDigestFlag(), err)
		}

	case showAction:
		res, err := c.ShowAction(ctx, getDigestFlag())
		if err != nil {
			log.Exitf("error fetching action %v: %v", getDigestFlag(), err)
		}
		os.Stdout.Write([]byte(res))

	case reexecuteAction:
		if err := c.ReexecuteAction(ctx, getDigestFlag(), *inputRoot, outerr.SystemOutErr); err != nil {
			log.Exitf("error reexecuting action %v: %v", getDigestFlag(), err)
		}

	case checkDeterminism:
		if err := c.CheckDeterminism(ctx, getDigestFlag(), *inputRoot, *execAttempts); err != nil {
			log.Exitf("error checking the determinism of %v: %v", getDigestFlag(), err)
		}

	case uploadBlob:
		if err := c.UploadBlob(ctx, getPathFlag()); err != nil {
			log.Exitf("error uploading blob from path '%v': %v", getPathFlag(), err)
		}

	case uploadTree:
		if err := c.UploadTree(ctx, *uploadConcurrency, getPathFlag()); err != nil {
			log.Exitf("error uploading tree from path '%v': %v", getPathFlag(), err)
		}

	default:
		log.Exitf("unsupported operation %v. Supported operations:\n%v", *operation, supportedOps)
	}
}

func getDigestFlag() string {
	if *digest == "" {
		log.Exitf("--digest must be specified.")
	}
	return *digest
}

func getPathFlag() string {
	if *pathPrefix == "" {
		log.Exitf("--path must be specified.")
	}
	return *pathPrefix
}
