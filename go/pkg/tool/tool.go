// Package tool provides implementation of the debugging related operations
// supported by go/cmd/remotetool package.
package tool

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	log "github.com/golang/glog"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/cas"
	rc "github.com/bazelbuild/remote-apis-sdks/go/pkg/client"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/command"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/filemetadata"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/outerr"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/rexec"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/uploadinfo"

	cpb "github.com/bazelbuild/remote-apis-sdks/go/api/command"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

const (
	stdoutFile = "stdout"
	stderrFile = "stderr"
)

// testOnlyStartDeterminismExec
var testOnlyStartDeterminismExec = func() {}

// Client is a remote execution client.
type Client struct {
	GrpcClient *rc.Client
}

// CheckDeterminism executes the action the given number of times and compares
// output digests, reporting failure if a mismatch is detected.
func (c *Client) CheckDeterminism(ctx context.Context, actionDigest, actionRoot string, attempts int) error {
	oe := outerr.SystemOutErr
	firstMd, firstRes := c.ExecuteAction(ctx, actionDigest, actionRoot, "", oe)
	for i := 1; i < attempts; i++ {
		testOnlyStartDeterminismExec()
		md, res := c.ExecuteAction(ctx, actionDigest, actionRoot, "", oe)
		gotErr := false
		if (firstRes == nil) != (res == nil) {
			log.Errorf("action does not produce a consistent result, got %v and %v from consecutive executions", res, firstRes)
			gotErr = true
		}
		if len(md.OutputFileDigests) != len(firstMd.OutputFileDigests) {
			log.Errorf("action does not produce a consistent number of outputs, got %v and %v from consecutive executions", len(md.OutputFileDigests), len(firstMd.OutputFileDigests))
			gotErr = true
		}
		for p, d := range md.OutputFileDigests {
			firstD, ok := firstMd.OutputFileDigests[p]
			if !ok {
				log.Errorf("action does not produce %v consistently", p)
				gotErr = true
				continue
			}
			if d != firstD {
				log.Errorf("action does not produce a consistent digest for %v, got %v and %v", p, d, firstD)
				gotErr = true
				continue
			}
		}
		if gotErr {
			return fmt.Errorf("action is not deterministic, check error log for more details")
		}
	}
	return nil
}

func (c *Client) prepCommand(ctx context.Context, client *rexec.Client, actionDigest, actionRoot string) (*command.Command, error) {
	acDg, err := digest.NewFromString(actionDigest)
	if err != nil {
		return nil, err
	}
	actionProto := &repb.Action{}
	if _, err := c.GrpcClient.ReadProto(ctx, acDg, actionProto); err != nil {
		return nil, err
	}

	commandProto := &repb.Command{}
	cmdDg, err := digest.NewFromProto(actionProto.GetCommandDigest())
	if err != nil {
		return nil, err
	}

	log.Infof("Reading command from action digest..")
	if _, err := c.GrpcClient.ReadProto(ctx, cmdDg, commandProto); err != nil {
		return nil, err
	}
	fetchInputs := actionRoot == ""
	if fetchInputs {
		curTime := time.Now().Format(time.RFC3339)
		actionRoot = filepath.Join(os.TempDir(), acDg.Hash+"_"+curTime)
	}
	inputRoot := filepath.Join(actionRoot, "input")
	var nodeProperties map[string]*cpb.NodeProperties
	if fetchInputs {
		dg, err := digest.NewFromProto(actionProto.GetInputRootDigest())
		if err != nil {
			return nil, err
		}
		log.Infof("Fetching input tree from input root digest %s into %s", dg, inputRoot)
		ts, _, err := c.GrpcClient.DownloadDirectory(ctx, dg, inputRoot, client.FileMetadataCache)
		if err != nil {
			return nil, err
		}
		nodeProperties = make(map[string]*cpb.NodeProperties)
		for path, t := range ts {
			if t.NodeProperties != nil {
				nodeProperties[path] = command.NodePropertiesFromAPI(t.NodeProperties)
			}
		}
	} else if nodeProperties, err = readNodePropertiesFromFile(filepath.Join(actionRoot, "input_node_properties.textproto")); err != nil {
		return nil, err
	}
	contents, err := os.ReadDir(inputRoot)
	if err != nil {
		return nil, err
	}
	inputPaths := []string{}
	for _, f := range contents {
		inputPaths = append(inputPaths, f.Name())
	}
	// Construct Command object.
	cmd := command.FromREProto(commandProto)
	cmd.InputSpec.Inputs = inputPaths
	cmd.InputSpec.InputNodeProperties = nodeProperties
	cmd.ExecRoot = inputRoot
	if actionProto.Timeout != nil {
		cmd.Timeout = actionProto.Timeout.AsDuration()
	}
	return cmd, nil
}

func readNodePropertiesFromFile(path string) (nps map[string]*cpb.NodeProperties, err error) {
	if _, err = os.Stat(path); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("error accessing input node properties file: %v", err)
		}
		return nil, nil
	}
	inTxt, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("error reading input node properties from file: %v", err)
	}
	ipb := &cpb.InputSpec{}
	if err := prototext.Unmarshal(inTxt, ipb); err != nil {
		return nil, fmt.Errorf("error unmarshalling input node properties from file %s: %v", path, err)
	}
	return ipb.InputNodeProperties, nil
}

// DownloadActionResult downloads the action result of the given action digest
// if it exists in the remote cache.
func (c *Client) DownloadActionResult(ctx context.Context, actionDigest, pathPrefix string) error {
	acDg, err := digest.NewFromString(actionDigest)
	if err != nil {
		return err
	}
	actionProto := &repb.Action{}
	if _, err := c.GrpcClient.ReadProto(ctx, acDg, actionProto); err != nil {
		return err
	}
	commandProto := &repb.Command{}
	cmdDg, err := digest.NewFromProto(actionProto.GetCommandDigest())
	if err != nil {
		return err
	}
	log.Infof("Reading command from action digest..")
	if _, err := c.GrpcClient.ReadProto(ctx, cmdDg, commandProto); err != nil {
		return err
	}
	// Construct Command object.
	cmd := command.FromREProto(commandProto)

	resPb, err := c.getActionResult(ctx, actionDigest)
	if err != nil {
		return err
	}
	if resPb == nil {
		return fmt.Errorf("action digest %v not found in cache", actionDigest)
	}

	log.Infof("Cleaning contents of %v.", pathPrefix)
	os.RemoveAll(pathPrefix)
	os.Mkdir(pathPrefix, 0755)

	log.Infof("Downloading action results of %v to %v.", actionDigest, pathPrefix)
	// We don't really need an in-memory filemetadata cache for debugging operations.
	noopCache := filemetadata.NewNoopCache()
	if _, err := c.GrpcClient.DownloadActionOutputs(ctx, resPb, filepath.Join(pathPrefix, cmd.WorkingDir), noopCache); err != nil {
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
		bytes, _, err := c.GrpcClient.ReadBlob(ctx, *dg)
		if err != nil {
			log.Errorf("Unable to read blob for %v with digest %v.", path, dg)
		}
		if err := os.WriteFile(path, bytes, 0644); err != nil {
			log.Errorf("Unable to write output of digest %v to file %v.", dg, path)
		}
	}
	log.Infof("Successfully downloaded results of %v to %v.", actionDigest, pathPrefix)
	return nil
}

// DownloadBlob downloads a blob from the remote cache into the specified path.
// If the path is empty, it writes the contents to stdout instead.
func (c *Client) DownloadBlob(ctx context.Context, blobDigest, path string) (string, error) {
	outputToStdout := false
	if path == "" {
		outputToStdout = true
		// Create a temp file.
		tmpFile, err := os.CreateTemp(os.TempDir(), "")
		if err != nil {
			return "", err
		}
		if err := tmpFile.Close(); err != nil {
			return "", err
		}
		path = tmpFile.Name()
		defer os.Remove(path)
	}
	dg, err := digest.NewFromString(blobDigest)
	if err != nil {
		return "", err
	}
	log.Infof("Downloading blob of %v to %v.", dg, path)
	if _, err := c.GrpcClient.ReadBlobToFile(ctx, dg, path); err != nil {
		return "", err
	}
	if !outputToStdout {
		return "", nil
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(contents), nil
}

// UploadBlob uploads a blob from the specified path into the remote cache.
func (c *Client) UploadBlob(ctx context.Context, path string) error {
	dg, err := digest.NewFromFile(path)
	if err != nil {
		return err
	}

	log.Infof("Uploading blob of %v from %v.", dg, path)
	ue := uploadinfo.EntryFromFile(dg, path)
	if _, _, err := c.GrpcClient.UploadIfMissing(ctx, ue); err != nil {
		return err
	}
	return nil
}

// UploadBlobV2 uploads a blob from the specified path into the remote cache using newer cas implementation.
func (c *Client) UploadBlobV2(ctx context.Context, path string) error {
	casC, err := cas.NewClient(ctx, c.GrpcClient.Connection, c.GrpcClient.InstanceName)
	if err != nil {
		return errors.WithStack(err)
	}
	inputC := make(chan *cas.UploadInput)

	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		inputC <- &cas.UploadInput{
			Path: path,
		}
		close(inputC)
		return nil
	})

	eg.Go(func() error {
		_, err := casC.Upload(ctx, cas.UploadOptions{}, inputC)
		return errors.WithStack(err)
	})

	return errors.WithStack(eg.Wait())
}

// DownloadDirectory downloads a an input root from the remote cache into the specified path.
func (c *Client) DownloadDirectory(ctx context.Context, rootDigest, path string) error {
	log.Infof("Cleaning contents of %v.", path)
	os.RemoveAll(path)
	os.Mkdir(path, 0755)

	dg, err := digest.NewFromString(rootDigest)
	if err != nil {
		return err
	}
	log.Infof("Downloading input root %v to %v.", dg, path)
	_, _, err = c.GrpcClient.DownloadDirectory(ctx, dg, path, filemetadata.NewNoopCache())
	return err
}

// UploadStats contains various metadata of a directory upload.
type UploadStats struct {
	rc.TreeStats
	RootDigest       digest.Digest
	CountBlobs       int64
	CountCacheMisses int64
	BytesTransferred int64
	BytesCacheMisses int64
	Error            string
}

// UploadDirectory uploads a directory from the specified path as a Merkle-tree to the remote cache.
func (c *Client) UploadDirectory(ctx context.Context, path string) (*UploadStats, error) {
	log.Infof("Computing Merkle tree rooted at %s", path)
	root, blobs, stats, err := c.GrpcClient.ComputeMerkleTree(ctx, path, "", "", &command.InputSpec{Inputs: []string{"."}}, filemetadata.NewNoopCache())
	if err != nil {
		return &UploadStats{Error: err.Error()}, err
	}
	us := &UploadStats{
		TreeStats:  *stats,
		RootDigest: root,
		CountBlobs: int64(len(blobs)),
	}
	log.Infof("Directory root digest: %v", root)
	log.Infof("Directory stats: %d files, %d directories, %d symlinks, %d total bytes", stats.InputFiles, stats.InputDirectories, stats.InputSymlinks, stats.TotalInputBytes)
	log.Infof("Uploading directory %v rooted at %s to CAS.", root, path)
	missing, n, err := c.GrpcClient.UploadIfMissing(ctx, blobs...)
	if err != nil {
		us.Error = err.Error()
		return us, err
	}
	var sumMissingBytes int64
	for _, d := range missing {
		sumMissingBytes += d.Size
	}
	us.CountCacheMisses = int64(len(missing))
	us.BytesTransferred = n
	us.BytesCacheMisses = sumMissingBytes
	return us, nil
}

func (c *Client) writeProto(m proto.Message, baseName string) error {
	f, err := os.Create(baseName)
	if err != nil {
		return err
	}
	defer f.Close()
	f.WriteString(prototext.Format(m))
	return nil
}

// DownloadAction parses and downloads an action to the given directory.
// The output directory will have the following:
//  1. ac.textproto: the action proto file in text format.
//  2. cmd.textproto: the command proto file in text format.
//  3. input/: the input tree root directory with all files under it.
//  4. input_node_properties.txtproto: all the NodeProperties defined on the
//     input tree, as an InputSpec proto file in text format. Will be omitted
//     if no NodeProperties are defined.
func (c *Client) DownloadAction(ctx context.Context, actionDigest, outputPath string, overwrite bool) error {
	resPb, err := c.getActionResult(ctx, actionDigest)
	if err != nil {
		return err
	}
	acDg, err := digest.NewFromString(actionDigest)
	if err != nil {
		return err
	}
	actionProto := &repb.Action{}
	log.Infof("Reading action..")
	if _, err := c.GrpcClient.ReadProto(ctx, acDg, actionProto); err != nil {
		return err
	}

	// Directory already exists, ask the user for confirmation before overwrite it.
	if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
		fmt.Printf("Directory '%s' already exists. Do you want to overwrite it? (yes/no): \n", outputPath)
		if !overwrite {
			reader := bufio.NewReader(os.Stdin)
			input, err := reader.ReadString('\n')
			if err != nil {
				return fmt.Errorf("error reading user input: %v", err)
			}
			input = strings.TrimSpace(input)
			input = strings.ToLower(input)

			if !(input == "yes" || input == "y") {
				return errors.Errorf("operation aborted.")
			}
		}
		// If the user confirms, remove the existing directory and create a new one
		err = os.RemoveAll(outputPath)
		if err != nil {
			return fmt.Errorf("error removing existing directory: %v", err)
		}
	}
	// Directory doesn't exist, create it.
	err = os.MkdirAll(outputPath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("error creating the directory: %v", err)
	}
	log.Infof("Directory created: %v", outputPath)

	if err := c.writeProto(actionProto, filepath.Join(outputPath, "ac.textproto")); err != nil {
		return err
	}

	cmdDg, err := digest.NewFromProto(actionProto.GetCommandDigest())
	if err != nil {
		return err
	}
	log.Infof("Reading command from action..")
	commandProto := &repb.Command{}
	if _, err := c.GrpcClient.ReadProto(ctx, cmdDg, commandProto); err != nil {
		return err
	}
	if err := c.writeProto(commandProto, filepath.Join(outputPath, "cmd.textproto")); err != nil {
		return err
	}

	if err := c.writeExecScript(ctx, commandProto, filepath.Join(outputPath, "run_locally.sh")); err != nil {
		return err
	}

	log.Infof("Fetching input tree from input root digest.. %v", actionProto.GetInputRootDigest())
	rootPath := filepath.Join(outputPath, "input")
	os.RemoveAll(rootPath)
	os.Mkdir(rootPath, 0755)
	rDg, err := digest.NewFromProto(actionProto.GetInputRootDigest())
	if err != nil {
		return err
	}
	ts, _, err := c.GrpcClient.DownloadDirectory(ctx, rDg, rootPath, filemetadata.NewNoopCache())
	if err != nil {
		return fmt.Errorf("error fetching input tree: %v", err)
	}
	is := &cpb.InputSpec{InputNodeProperties: make(map[string]*cpb.NodeProperties)}
	for path, t := range ts {
		if t.NodeProperties != nil {
			is.InputNodeProperties[path] = command.NodePropertiesFromAPI(t.NodeProperties)
		}
	}
	if len(is.InputNodeProperties) != 0 {
		err = c.writeProto(is, filepath.Join(outputPath, "input_node_properties.textproto"))
		if err != nil {
			return err
		}
	}
	res, err := c.formatAction(ctx, actionProto, resPb, commandProto, cmdDg)
	if err != nil {
		return fmt.Errorf("error formatting action %v", err)
	}
	err = os.WriteFile(filepath.Join(outputPath, "action.txt"), []byte(res), 0644)
	if err != nil {
		return fmt.Errorf("error dumping to action.txt] %v: %v", outputPath, err)
	}
	return nil
}

// shellSprintf is intended to add args sanitization before using them.
func shellSprintf(format string, args ...any) string {
	// TODO: check args for flag injection
	return fmt.Sprintf(format, args...)
}

func (c *Client) writeExecScript(ctx context.Context, cmd *repb.Command, filename string) error {
	if cmd == nil {
		return fmt.Errorf("invalid comment (nil)")
	}

	var runActionScript bytes.Buffer
	runActionFilename := filepath.Join(filepath.Dir(filename), "run_command.sh")
	wd := cmd.WorkingDirectory
	execCmd := strings.Join(cmd.GetArguments(), " ")
	runActionScript.WriteString(shellSprintf("#!/bin/bash\n\n"))
	runActionScript.WriteString(shellSprintf("# This script is meant to be called by %v.\n", filename))
	if wd != "" {
		runActionScript.WriteString(shellSprintf("cd %v\n", wd))
	}
	for _, od := range cmd.GetOutputDirectories() {
		runActionScript.WriteString(shellSprintf("mkdir -p %v\n", od))
	}
	for _, of := range cmd.GetOutputFiles() {
		runActionScript.WriteString(shellSprintf("mkdir -p %v\n", filepath.Dir(of)))
	}
	for _, e := range cmd.GetEnvironmentVariables() {
		runActionScript.WriteString(shellSprintf("export %v=%v\n", e.GetName(), e.GetValue()))
	}
	runActionScript.WriteString(execCmd)
	runActionScript.WriteRune('\n')
	runActionScript.WriteString(shellSprintf("bash\n"))
	if err := os.WriteFile(runActionFilename, runActionScript.Bytes(), 0755); err != nil {
		return err
	}

	var container string
	for _, property := range cmd.Platform.GetProperties() {
		if property.Name == "container-image" {
			container = strings.TrimPrefix(property.Value, "docker://")
		}
	}
	if container == "" {
		return fmt.Errorf("container-image platform property missing from command proto: %v", cmd)
	}
	var execScript bytes.Buffer
	dockerCmd := shellSprintf("docker run -i -t -w /b/f/w -v `pwd`/input:/b/f/w -v `pwd`/run_command.sh:/b/f/w/run_command.sh %v ./run_command.sh\n", container)
	execScript.WriteString(shellSprintf("#!/bin/bash\n\n"))
	execScript.WriteString(shellSprintf("# This script can be used to run the action locally on\n"))
	execScript.WriteString(shellSprintf("# this machine.\n"))
	execScript.WriteString(shellSprintf("echo \"WARNING: The results from executing the action through this script may differ from results from RBE.\"\n"))
	execScript.WriteString(shellSprintf("set -x\n"))
	execScript.WriteString(dockerCmd)
	return os.WriteFile(filename, execScript.Bytes(), 0755)
}

func (c *Client) prepProtos(ctx context.Context, actionRoot string) (string, error) {
	cmdTxt, err := os.ReadFile(filepath.Join(actionRoot, "cmd.textproto"))
	if err != nil {
		return "", err
	}
	cmdPb := &repb.Command{}
	if err := prototext.Unmarshal(cmdTxt, cmdPb); err != nil {
		return "", err
	}
	ue, err := uploadinfo.EntryFromProto(cmdPb)
	if err != nil {
		return "", err
	}
	if _, _, err := c.GrpcClient.UploadIfMissing(ctx, ue); err != nil {
		return "", err
	}
	ac, err := os.ReadFile(filepath.Join(actionRoot, "ac.textproto"))
	if err != nil {
		return "", err
	}
	acPb := &repb.Action{}
	if err := prototext.Unmarshal(ac, acPb); err != nil {
		return "", err
	}
	dg, err := digest.NewFromMessage(cmdPb)
	if err != nil {
		return "", err
	}
	acPb.CommandDigest = dg.ToProto()
	ue, err = uploadinfo.EntryFromProto(acPb)
	if err != nil {
		return "", err
	}
	if _, _, err := c.GrpcClient.UploadIfMissing(ctx, ue); err != nil {
		return "", err
	}
	dg, err = digest.NewFromMessage(acPb)
	if err != nil {
		return "", err
	}
	return dg.String(), nil
}

// ExecuteAction executes an action in a canonical structure remotely.
// The structure is the same as that produced by DownloadAction.
// top level >
//
//	> ac.textproto (Action text proto)
//	> cmd.textproto (Command text proto)
//	> input_node_properties.textproto (InputSpec text proto, optional)
//	> input (Input root)
//	  > inputs...
func (c *Client) ExecuteAction(ctx context.Context, actionDigest, actionRoot, outDir string, oe outerr.OutErr) (*command.Metadata, error) {
	fmc := filemetadata.NewNoopCache()
	client := &rexec.Client{
		FileMetadataCache: fmc,
		GrpcClient:        c.GrpcClient,
	}
	if actionRoot != "" {
		var err error
		if actionDigest, err = c.prepProtos(ctx, actionRoot); err != nil {
			return nil, err
		}
	}
	cmd, err := c.prepCommand(ctx, client, actionDigest, actionRoot)
	if err != nil {
		return nil, err
	}
	opt := &command.ExecutionOptions{AcceptCached: false, DownloadOutputs: false, DownloadOutErr: true}
	ec, err := client.NewContext(ctx, cmd, opt, oe)
	if err != nil {
		return nil, err
	}
	ec.ExecuteRemotely()
	fmt.Printf("Action complete\n")
	fmt.Printf("---------------\n")
	fmt.Printf("Action digest: %v\n", ec.Metadata.ActionDigest.String())
	fmt.Printf("Command digest: %v\n", ec.Metadata.CommandDigest.String())
	fmt.Printf("Stdout digest: %v\n", ec.Metadata.StdoutDigest.String())
	fmt.Printf("Stderr digest: %v\n", ec.Metadata.StderrDigest.String())
	fmt.Printf("Number of Input Files: %v\n", ec.Metadata.InputFiles)
	fmt.Printf("Number of Input Dirs: %v\n", ec.Metadata.InputDirectories)
	if len(cmd.InputSpec.InputNodeProperties) != 0 {
		fmt.Printf("Number of Input Node Properties: %d\n", len(cmd.InputSpec.InputNodeProperties))
	}
	fmt.Printf("Number of Output Files: %v\n", ec.Metadata.OutputFiles)
	fmt.Printf("Number of Output Directories: %v\n", ec.Metadata.OutputDirectories)
	switch ec.Result.Status {
	case command.NonZeroExitResultStatus:
		oe.WriteErr([]byte(fmt.Sprintf("Remote action FAILED with exit code %d.\n", ec.Result.ExitCode)))
	case command.TimeoutResultStatus:
		oe.WriteErr([]byte(fmt.Sprintf("Remote action TIMED OUT after %0f seconds.\n", cmd.Timeout.Seconds())))
	case command.InterruptedResultStatus:
		oe.WriteErr([]byte(fmt.Sprintf("Remote execution was interrupted.\n")))
	case command.RemoteErrorResultStatus:
		oe.WriteErr([]byte(fmt.Sprintf("Remote execution error: %v.\n", ec.Result.Err)))
	case command.LocalErrorResultStatus:
		oe.WriteErr([]byte(fmt.Sprintf("Local error: %v.\n", ec.Result.Err)))
	}
	if ec.Result.Err == nil && outDir != "" {
		ec.DownloadOutputs(outDir)
		fmt.Printf("Output written to %v\n", outDir)
	}
	return ec.Metadata, ec.Result.Err
}

// ShowAction parses and displays an action with its corresponding command.
func (c *Client) ShowAction(ctx context.Context, actionDigest string) (string, error) {
	resPb, err := c.getActionResult(ctx, actionDigest)
	if err != nil {
		return "", err
	}

	acDg, err := digest.NewFromString(actionDigest)
	if err != nil {
		return "", err
	}
	actionProto := &repb.Action{}
	if _, err := c.GrpcClient.ReadProto(ctx, acDg, actionProto); err != nil {
		return "", err
	}
	commandProto := &repb.Command{}
	cmdDg, err := digest.NewFromProto(actionProto.GetCommandDigest())
	if err != nil {
		return "", err
	}
	log.Infof("Reading command from action digest..")
	if _, err := c.GrpcClient.ReadProto(ctx, cmdDg, commandProto); err != nil {
		return "", err
	}
	return c.formatAction(ctx, actionProto, resPb, commandProto, cmdDg)
}

func (c *Client) formatAction(ctx context.Context, actionProto *repb.Action, resPb *repb.ActionResult, commandProto *repb.Command, cmdDg digest.Digest) (string, error) {
	var showActionRes bytes.Buffer
	if actionProto.Timeout != nil {
		timeout := actionProto.Timeout.AsDuration()
		showActionRes.WriteString(fmt.Sprintf("Timeout: %s\n", timeout.String()))
	}
	showActionRes.WriteString("Command\n=======\n")
	showActionRes.WriteString(fmt.Sprintf("Command Digest: %v\n", cmdDg))
	for _, ev := range commandProto.GetEnvironmentVariables() {
		showActionRes.WriteString(fmt.Sprintf("\t%s=%s\n", ev.Name, ev.Value))
	}
	cmdStr := strings.Join(commandProto.GetArguments(), " ")
	showActionRes.WriteString(fmt.Sprintf("\t%v\n", cmdStr))
	showActionRes.WriteString("\nPlatform\n========\n")
	for _, property := range commandProto.GetPlatform().GetProperties() {
		showActionRes.WriteString(fmt.Sprintf("\t%s=%s\n", property.Name, property.Value))
	}
	showActionRes.WriteString("\nInputs\n======\n")
	log.Infof("Fetching input tree from input root digest..")
	inpTree, _, err := c.getInputTree(ctx, actionProto.GetInputRootDigest())
	if err != nil {
		showActionRes.WriteString("Failed to fetch input tree:\n")
		showActionRes.WriteString(err.Error())
		showActionRes.WriteString("\n")
	} else {
		showActionRes.WriteString(inpTree)
	}

	if resPb == nil {
		showActionRes.WriteString("\nNo action result in cache.\n")
	} else {
		log.Infof("Fetching output tree from action result..")
		outs, err := c.getOutputs(ctx, resPb)
		if err != nil {
			return "", err
		}
		showActionRes.WriteString("\n")
		showActionRes.WriteString(outs)
	}
	return showActionRes.String(), nil
}

func (c *Client) getOutputs(ctx context.Context, actionRes *repb.ActionResult) (string, error) {
	var res bytes.Buffer

	res.WriteString("------------------------------------------------------------------------\n")
	res.WriteString("Action Result\n\n")
	res.WriteString(fmt.Sprintf("Exit code: %d\n", actionRes.ExitCode))

	if actionRes.StdoutDigest != nil {
		dg, err := digest.NewFromProto(actionRes.StdoutDigest)
		if err != nil {
			return "", err
		}
		res.WriteString(fmt.Sprintf("stdout digest: %v\n", dg))
	}

	if actionRes.StderrDigest != nil {
		dg, err := digest.NewFromProto(actionRes.StderrDigest)
		if err != nil {
			return "", err
		}
		res.WriteString(fmt.Sprintf("stderr digest: %v\n", dg))
	}

	res.WriteString("\nOutput Files\n============\n")
	for _, of := range actionRes.GetOutputFiles() {
		dg, err := digest.NewFromProto(of.GetDigest())
		if err != nil {
			return "", err
		}
		res.WriteString(fmt.Sprintf("%v, digest: %v\n", of.GetPath(), dg))
	}

	res.WriteString("\nOutput Files From Directories\n=============================\n")
	for _, od := range actionRes.GetOutputDirectories() {
		treeDigest := od.GetTreeDigest()
		dg, err := digest.NewFromProto(treeDigest)
		if err != nil {
			return "", err
		}
		outDirTree := &repb.Tree{}
		if _, err := c.GrpcClient.ReadProto(ctx, dg, outDirTree); err != nil {
			return "", err
		}

		outputs, _, err := c.flattenTree(ctx, outDirTree)
		if err != nil {
			return "", err
		}
		res.WriteString("\n")
		res.WriteString(outputs)
	}

	return res.String(), nil
}

func (c *Client) getInputTree(ctx context.Context, root *repb.Digest) (string, []string, error) {
	var res bytes.Buffer

	dg, err := digest.NewFromProto(root)
	if err != nil {
		return "", nil, err
	}
	res.WriteString(fmt.Sprintf("[Root directory digest: %v]", dg))

	dirs, err := c.GrpcClient.GetDirectoryTree(ctx, root)
	if err != nil {
		return "", nil, err
	}
	if len(dirs) == 0 {
		return "", nil, fmt.Errorf("Empty directories returned by GetTree for %v", dg)
	}
	t := &repb.Tree{
		Root:     dirs[0],
		Children: dirs,
	}
	inputs, paths, err := c.flattenTree(ctx, t)
	if err != nil {
		return "", nil, err
	}
	res.WriteString("\n")
	res.WriteString(inputs)

	return res.String(), paths, nil
}

func (c *Client) flattenTree(ctx context.Context, t *repb.Tree) (string, []string, error) {
	var res bytes.Buffer
	outputs, err := c.GrpcClient.FlattenTree(t, "")
	if err != nil {
		return "", nil, err
	}
	// Sort the values by path.
	paths := make([]string, 0, len(outputs))
	for path := range outputs {
		if path == "" {
			path = "."
			outputs[path] = outputs[""]
		}
		if output := outputs[path]; output.SymlinkTarget != "" {
			path = path + "->" + output.SymlinkTarget
		}
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		path = strings.Split(path, "->")[0]
		output := outputs[path]
		var np string
		if output.NodeProperties != nil {
			np = fmt.Sprintf(" [Node properties: %v]", prototext.MarshalOptions{Multiline: false}.Format(output.NodeProperties))
		}
		if output.IsEmptyDirectory {
			res.WriteString(fmt.Sprintf("%v: [Directory digest: %v]%s\n", path, output.Digest, np))
		} else if output.SymlinkTarget != "" {
			res.WriteString(fmt.Sprintf("%v: [Symlink digest: %v, Symlink Target: %v]%s\n", path, output.Digest, output.SymlinkTarget, np))
		} else {
			res.WriteString(fmt.Sprintf("%v: [File digest: %v]%s\n", path, output.Digest, np))
		}
	}
	return res.String(), paths, nil
}

func (c *Client) getActionResult(ctx context.Context, actionDigest string) (*repb.ActionResult, error) {
	acDg, err := digest.NewFromString(actionDigest)
	if err != nil {
		return nil, err
	}
	d := &repb.Digest{
		Hash:      acDg.Hash,
		SizeBytes: acDg.Size,
	}
	resPb, err := c.GrpcClient.CheckActionCache(ctx, d)
	if err != nil {
		return nil, err
	}
	return resPb, nil
}

type IO struct {
	inputs  []string
	outputs []string
}

func (c *Client) GetIO(ctx context.Context, actionDigest string) (*IO, error) {
	acDg, err := digest.NewFromString(actionDigest)
	if err != nil {
		return nil, err
	}
	actionProto := &repb.Action{}
	if _, err := c.GrpcClient.ReadProto(ctx, acDg, actionProto); err != nil {
		return nil, err
	}
	cmdDg, err := digest.NewFromProto(actionProto.GetCommandDigest())
	if err != nil {
		return nil, err
	}
	commandProto := &repb.Command{}
	if _, err := c.GrpcClient.ReadProto(ctx, cmdDg, commandProto); err != nil {
		return nil, err
	}
	_, inputs, err := c.getInputTree(ctx, actionProto.GetInputRootDigest())
	if err != nil {
		return nil, err
	}

	resPb, err := c.getActionResult(ctx, actionDigest)
	if err != nil {
		return nil, err
	}
	var outputs []string
	symlinks := append(resPb.GetOutputFileSymlinks(), resPb.GetOutputDirectorySymlinks()...)
	for _, s := range symlinks {
		if s != nil {
			outputs = append(outputs, s.GetPath()+"->"+s.GetTarget())
		}
	}
	for _, f := range resPb.GetOutputFiles() {
		if f != nil {
			outputs = append(outputs, f.GetPath())
		}
	}
	for _, d := range resPb.GetOutputDirectories() {
		if d != nil {
			outputs = append(outputs, d.GetPath())
		}
	}
	sort.Strings(outputs)
	return &IO{
		inputs,
		outputs,
	}, nil
}
