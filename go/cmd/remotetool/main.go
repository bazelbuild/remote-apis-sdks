// Main package for the remotetool binary.
//
// Here's a sample invocation of this tool to download an action result:
// bazelisk run //go/cmd/remotetool -- \
// 	--instance=projects/rbe-android-ci/instances/default_instance \
// 	--service remotebuildexecution.googleapis.com:443 \
// 	--alsologtostderr --v 1 \
// 	--credential_file ~/.config/foundry/keys/dev-foundry.json \
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
)

var (
	operation  = flag.String("operation", "download_action_result", "Specifies the operation to perform. Currently only download_action_result is supported.")
	digest     = flag.String("digest", "", "Digest in <digest/size_bytes> format.")
	pathPrefix = flag.String("path", "/tmp", "Directory to which outputs should be downloaded to. Defaults to /tmp/.")
)

func validate() {
	if *digest == "" {
		log.Exitf("--digest must be specified.")
	}

	switch OpType(*operation) {
	case downloadActionResult, showAction:
		return

	default:
		log.Exitf("unsupported operation %q.", *operation)
	}
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %v [-flags] -- --operation <op> arguments ...\n", path.Base(os.Args[0]))
		flag.PrintDefaults()
	}
	flag.Parse()
	validate()

	ctx := context.Background()
	grpcClient, err := rflags.NewClientFromFlags(ctx)
	if err != nil {
		log.Exitf("error connecting to remote execution client: %v", err)
	}
	defer grpcClient.Close()
	c := &tool.Client{GrpcClient: grpcClient}

	switch OpType(*operation) {
	case downloadActionResult:
		if err := c.DownloadActionResult(ctx, *digest, *pathPrefix); err != nil {
			log.Exitf("error downloading action result for digest %v: %v", *digest, err)
		}

	case showAction:
		res, err := c.ShowAction(ctx, *digest)
		if err != nil {
			log.Exitf("error fetching action %v: %v", *digest, err)
		}
		fmt.Fprintf(os.Stdout, res)

	default:
		log.Exitf("unsupported operation %v.", *operation)
	}
}
