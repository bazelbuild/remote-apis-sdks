// Main package for the remotetool binary.
//
// This tool supports common debugging operations concerning remotely executed
// actions:
// 1. Download/upload a file or directory from/to remote cache by its digest.
// 2. Display details of a remotely executed action.
// 3. Download action results by the action digest.
// 4. Re-execute remote action (with optional inputs override).
//
// Example (download an action result from remote action cache):
//
//	bazelisk run //go/cmd/remotetool -- \
//	 --operation=download_action_result \
//		--instance=$INSTANCE \
//		--service remotebuildexecution.googleapis.com:443 \
//		--alsologtostderr --v 1 \
//		--credential_file $CRED_FILE \
//		--digest=52a54724e6b3dff3bc44ef5dceb3aab5892f2fc7e37fce5aa6e16a7a266fbed6/147 \
//		--path=`pwd`/tmp
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

var (
	operation = flag.String("operation", "", fmt.Sprintf("Specifies the operation to perform. Supported values: %v", tool.SupportedOps))
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %v [-flags] -- --operation <op> arguments ...\n", path.Base(os.Args[0]))
		flag.PrintDefaults()
	}
	tool.RegisterFlags()
	flag.Parse()
	if *operation == "" {
		log.Exitf("--operation must be specified.")
	}
	tool.ValidateFlags()

	ctx := context.Background()
	grpcClient, err := rflags.NewClientFromFlags(ctx)
	if err != nil {
		log.Exitf("error connecting to remote execution client: %v", err)
	}
	defer grpcClient.Close()
	c := &tool.Client{GrpcClient: grpcClient}

	fn, ok := tool.RemoteToolOperations[tool.OpType(*operation)]
	if !ok {
		log.Exitf("unsupported operation %v. Supported operations:\n%v", *operation, tool.SupportedOps)
	}
	fn(ctx, c)
}
