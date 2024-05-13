// Package tool in this file provides functions that can be embedded into a go binary
// such that the binary now supports the various operations the remotetool
// supports (e.g., show_action, upload_blob, etc).
// A canonical usage of this library is the remotetool binary.
package tool

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/outerr"

	log "github.com/golang/glog"
)

var (
	inputDigest  string
	pathPrefix   string
	overwrite    bool
	actionRoot   string
	execAttempts int
	jsonOutput   string
)

// RegisterFlags registers the flags necessary for the embedded tool to work.
func RegisterFlags() {
	flag.StringVar(&inputDigest, "digest", "", "Digest in <digest/size_bytes> format.")
	flag.StringVar(&pathPrefix, "path", "", "Path to which outputs should be downloaded to.")
	flag.BoolVar(&overwrite, "overwrite", false, "Overwrite the output path if it already exist.")
	flag.StringVar(&actionRoot, "action_root", "", "For execute_action: the root of the action spec, containing ac.textproto (Action proto), cmd.textproto (Command proto), and input/ (root of the input tree).")
	flag.IntVar(&execAttempts, "exec_attempts", 10, "For check_determinism: the number of times to remotely execute the action and check for mismatches.")
	flag.StringVar(&jsonOutput, "json", "", "Path to output operation result as JSON. Currently supported for \"upload_dir\", and includes various upload metadata (see UploadStats).")
}

// OpType denotes the type of operation to perform.
type OpType string

const (
	downloadActionResult OpType = "download_action_result"
	showAction           OpType = "show_action"
	downloadAction       OpType = "download_action"
	downloadBlob         OpType = "download_blob"
	downloadDir          OpType = "download_dir"
	executeAction        OpType = "execute_action"
	checkDeterminism     OpType = "check_determinism"
	uploadBlob           OpType = "upload_blob"
	uploadBlobV2         OpType = "upload_blob_v2"
	uploadDir            OpType = "upload_dir"
)

// SupportedOps denote the list of operations supported by this remotetool.
var SupportedOps = []OpType{
	downloadActionResult,
	showAction,
	downloadAction,
	downloadBlob,
	downloadDir,
	executeAction,
	checkDeterminism,
	uploadBlob,
	uploadDir,
}

// RemoteToolOperations maps each supported operation to a function that performs
// the operation.
var RemoteToolOperations = map[OpType]func(ctx context.Context, c *Client){
	downloadActionResult: func(ctx context.Context, c *Client) {
		if err := c.DownloadActionResult(ctx, getDigestFlag(), getPathFlag()); err != nil {
			log.Exitf("error downloading action result for digest %v: %v", getDigestFlag(), err)
		}
	},
	downloadBlob: func(ctx context.Context, c *Client) {
		res, err := c.DownloadBlob(ctx, getDigestFlag(), getPathFlag())
		if err != nil {
			log.Exitf("error downloading blob for digest %v: %v", getDigestFlag(), err)
		}
		os.Stdout.Write([]byte(res))
	},
	downloadDir: func(ctx context.Context, c *Client) {
		if err := c.DownloadDirectory(ctx, getDigestFlag(), getPathFlag()); err != nil {
			log.Exitf("error downloading directory for digest %v: %v", getDigestFlag(), err)
		}
	},
	showAction: func(ctx context.Context, c *Client) {
		res, err := c.ShowAction(ctx, getDigestFlag())
		if err != nil {
			log.Exitf("error fetching action %v: %v", getDigestFlag(), err)
		}
		os.Stdout.Write([]byte(res))
	},
	downloadAction: func(ctx context.Context, c *Client) {
		err := c.DownloadAction(ctx, getDigestFlag(), getPathFlag(), overwrite)
		if err != nil {
			log.Exitf("error fetching action %v: %v", getDigestFlag(), err)
		}
		fmt.Printf("Action downloaded to %v\n", getPathFlag())
	},
	executeAction: func(ctx context.Context, c *Client) {
		if _, err := c.ExecuteAction(ctx, getDigestFlag(), actionRoot, getPathFlag(), outerr.SystemOutErr); err != nil {
			log.Exitf("error executing action: %v", err)
		}
	},
	checkDeterminism: func(ctx context.Context, c *Client) {
		if err := c.CheckDeterminism(ctx, getDigestFlag(), actionRoot, execAttempts); err != nil {
			log.Exitf("error checking determinism: %v", err)
		}
	},
	uploadBlob: func(ctx context.Context, c *Client) {
		if err := c.UploadBlob(ctx, getPathFlag()); err != nil {
			log.Exitf("error uploading blob for digest %v: %v", getDigestFlag(), err)
		}
	},
	uploadBlobV2: func(ctx context.Context, c *Client) {
		if err := c.UploadBlobV2(ctx, getPathFlag()); err != nil {
			log.Exitf("error uploading blob for digest %v: %v", getDigestFlag(), err)
		}
	},
	uploadDir: func(ctx context.Context, c *Client) {
		us, err := c.UploadDirectory(ctx, getPathFlag())
		if jsonOutput != "" {
			js, _ := json.MarshalIndent(us, "", "  ")
			if jsonOutput == "-" {
				fmt.Printf("%s\n", js)
			} else {
				log.Infof("Outputting JSON results to %s", jsonOutput)
				if err := os.WriteFile(jsonOutput, []byte(js), 0o666); err != nil {
					log.Exitf("Error writing JSON output to file: %v", err)
				}
			}
		}
		if err != nil {
			log.Exitf("error uploading directory for path %s: %v", getPathFlag(), err)
		}
	},
}

// ValidateFlags validates the command line flags associated with this embedded remote tool.
func ValidateFlags() {
	if execAttempts <= 0 {
		log.Exitf("--exec_attempts must be >= 1.")
	}
}

func getDigestFlag() string {
	if inputDigest == "" {
		log.Exitf("--digest must be specified.")
	}
	return inputDigest
}

func getPathFlag() string {
	if pathPrefix == "" {
		log.Exitf("--path must be specified.")
	}
	return pathPrefix
}
