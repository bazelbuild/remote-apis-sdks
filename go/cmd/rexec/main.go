// Main package for the rexec binary.
//
// This tool executes a command remotely, downloading the command outputs and propagating its
// stdout and stderr.
//
// Example usage:
//
//	rexec --alsologtostderr --v 1 \
//	  --service remotebuildexecution.googleapis.com:443 \
//	  --instance $INSTANCE \
//	  --credential_file $CRED_FILE \
//	  --exec_root $HOME/example \
//	  --platform container-image $CONTAINER \
//	  --inputs a/hello,a/goodbye
//	  --working_directory foo/bar
//	  --output_files foo/bar/out
//	  -- /bin/bash -c 'cat hello goodbye > out'
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/command"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/filemetadata"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/moreflag"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/outerr"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/rexec"

	rflags "github.com/bazelbuild/remote-apis-sdks/go/pkg/flags"
	log "github.com/golang/glog"
)

func initFlags(cmd *command.Command, opt *command.ExecutionOptions) {
	flag.StringVar(&cmd.Identifiers.CommandID, "command_id", "", "An identifier for the command for debugging.")
	flag.StringVar(&cmd.Identifiers.InvocationID, "invocation_id", "", "An identifier for a group of commands for debugging.")
	flag.StringVar(&cmd.Identifiers.ToolName, "tool_name", "", "The name of the tool to associate with executed commands.")
	flag.StringVar(&cmd.ExecRoot, "exec_root", "", "The exec root of the command. The path from which all inputs and outputs are defined relatively.")
	flag.StringVar(&cmd.WorkingDir, "working_directory", "", "The working directory, relative to the exec root, for the command to run in. It must be a directory which exists in the input tree. If it is left empty, then the action is run in the exec root.")
	flag.Var((*moreflag.StringListValue)(&cmd.InputSpec.Inputs), "inputs", "Comma-separated command input paths, relative to exec root.")
	flag.Var((*moreflag.StringListValue)(&cmd.OutputFiles), "output_files", "Comma-separated command output file paths, relative to exec root.")
	flag.Var((*moreflag.StringListValue)(&cmd.OutputDirs), "output_directories", "Comma-separated command output directory paths, relative to exec root.")
	flag.DurationVar(&cmd.Timeout, "exec_timeout", 0, "Timeout for the command. Value of 0 means no timeout.")
	flag.Var((*moreflag.StringMapValue)(&cmd.Platform), "platform", "Comma-separated key value pairs in the form key=value. This is used to identify remote platform settings like the docker image to use to run the command.")
	flag.Var((*moreflag.StringMapValue)(&cmd.InputSpec.EnvironmentVariables), "environment_variables", "Environment variables to pass through to remote execution, as comma-separated key value pairs in the form key=value.")
	flag.BoolVar(&opt.AcceptCached, "accept_cached", true, "Boolean indicating whether to accept remote cache hits.")
	flag.BoolVar(&opt.DoNotCache, "do_not_cache", false, "Boolean indicating whether to skip caching the command result remotely.")
	flag.BoolVar(&opt.DownloadOutputs, "download_outputs", true, "Boolean indicating whether to download outputs after the command is executed.")
	flag.BoolVar(&opt.DownloadOutErr, "download_outerr", true, "Boolean indicating whether to download stdout and stderr after the command is executed.")
}

func main() {
	cmd := &command.Command{InputSpec: &command.InputSpec{}, Identifiers: &command.Identifiers{}}
	opt := &command.ExecutionOptions{}
	initFlags(cmd, opt)
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %v [-flags] -- command arguments ...\n", path.Base(os.Args[0]))
		flag.PrintDefaults()
	}
	flag.Parse()
	cmd.Args = flag.Args()
	if err := cmd.Validate(); err != nil {
		flag.Usage()
		log.Exitf("Invalid command provided: %v", err)
	}

	ctx := context.Background()
	grpcClient, err := rflags.NewClientFromFlags(ctx)
	if err != nil {
		log.Exitf("error connecting to remote execution client: %v", err)
	}
	defer grpcClient.Close()
	c := &rexec.Client{
		FileMetadataCache: filemetadata.NewNoopCache(),
		GrpcClient:        grpcClient,
	}
	res, _ := c.Run(ctx, cmd, opt, outerr.SystemOutErr)
	switch res.Status {
	case command.NonZeroExitResultStatus:
		fmt.Fprintf(os.Stderr, "Remote action FAILED with exit code %d.\n", res.ExitCode)
	case command.TimeoutResultStatus:
		fmt.Fprintf(os.Stderr, "Remote action TIMED OUT after %0f seconds.\n", cmd.Timeout.Seconds())
	case command.InterruptedResultStatus:
		fmt.Fprintf(os.Stderr, "Remote execution was interrupted.\n")
	case command.RemoteErrorResultStatus:
		fmt.Fprintf(os.Stderr, "Remote execution error: %v.\n", res.Err)
	case command.LocalErrorResultStatus:
		fmt.Fprintf(os.Stderr, "Local error: %v.\n", res.Err)
	}
}
