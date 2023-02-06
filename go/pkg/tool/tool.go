// Package tool provides implementation of the debugging related operations
// supported by go/cmd/remotetool package.
package tool

import (
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

func (c *Client) prepCommand(ctx context.Context, client *rexec.Client, actionDigest, inputRoot string) (*command.Command, error) {
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
	if inputRoot == "" {
		curTime := time.Now().Format(time.RFC3339)
		inputRoot = filepath.Join(os.TempDir(), acDg.Hash+"_"+curTime)
		dg, err := digest.NewFromProto(actionProto.GetInputRootDigest())
		if err != nil {
			return nil, err
		}
		log.Infof("Fetching input tree from input root digest %s into %s", dg, inputRoot)
		_, _, err = c.GrpcClient.DownloadDirectory(ctx, dg, inputRoot, client.FileMetadataCache)
		if err != nil {
			return nil, err
		}
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
	cmd := commandFromREProto(commandProto)
	cmd.InputSpec.Inputs = inputPaths
	cmd.ExecRoot = inputRoot
	if actionProto.Timeout != nil {
		cmd.Timeout = actionProto.Timeout.AsDuration()
	}
	return cmd, nil
}

func commandFromREProto(cmdPb *repb.Command) *command.Command {
	cmd := &command.Command{
		InputSpec: &command.InputSpec{
			EnvironmentVariables: make(map[string]string),
		},
		Identifiers: &command.Identifiers{},
		WorkingDir:  cmdPb.WorkingDirectory,
		OutputFiles: cmdPb.OutputFiles,
		OutputDirs:  cmdPb.OutputDirectories,
		Platform:    make(map[string]string),
		Args:        cmdPb.Arguments,
	}

	for _, ev := range cmdPb.EnvironmentVariables {
		cmd.InputSpec.EnvironmentVariables[ev.Name] = ev.Value
	}
	for _, pt := range cmdPb.GetPlatform().GetProperties() {
		cmd.Platform[pt.Name] = pt.Value
	}
	return cmd
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
	cmd := commandFromREProto(commandProto)

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
func (c *Client) DownloadAction(ctx context.Context, actionDigest, outputPath string) error {
	acDg, err := digest.NewFromString(actionDigest)
	if err != nil {
		return err
	}
	actionProto := &repb.Action{}
	log.Infof("Reading action..")
	if _, err := c.GrpcClient.ReadProto(ctx, acDg, actionProto); err != nil {
		return err
	}
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

	log.Infof("Fetching input tree from input root digest.. %v", actionProto.GetInputRootDigest())
	rootPath := filepath.Join(outputPath, "input")
	os.RemoveAll(rootPath)
	os.Mkdir(rootPath, 0755)
	rDg, err := digest.NewFromProto(actionProto.GetInputRootDigest())
	if err != nil {
		return err
	}
	_, _, err = c.GrpcClient.DownloadDirectory(ctx, rDg, rootPath, filemetadata.NewNoopCache())
	return err
}

func (c *Client) prepProtos(ctx context.Context, actionRoot string) (string, error) {
	cmdTxt, err := os.ReadFile(filepath.Join(actionRoot, "cmd.textproto"))
	if err != nil {
		return "", err
	}
	cmdProto := &repb.Command{}
	if err := prototext.Unmarshal(cmdTxt, cmdProto); err != nil {
		return "", err
	}
	cmdPb, err := proto.Marshal(cmdProto)
	if err != nil {
		return "", err
	}
	ue := uploadinfo.EntryFromBlob(cmdPb)
	if _, _, err := c.GrpcClient.UploadIfMissing(ctx, ue); err != nil {
		return "", err
	}
	ac, err := os.ReadFile(filepath.Join(actionRoot, "ac.textproto"))
	if err != nil {
		return "", err
	}
	actionProto := &repb.Action{}
	if err := prototext.Unmarshal(ac, actionProto); err != nil {
		return "", err
	}
	actionProto.CommandDigest = digest.NewFromBlob(cmdPb).ToProto()
	acPb, err := proto.Marshal(actionProto)
	if err != nil {
		return "", err
	}
	ue = uploadinfo.EntryFromBlob(acPb)
	if _, _, err := c.GrpcClient.UploadIfMissing(ctx, ue); err != nil {
		return "", err
	}
	return digest.NewFromBlob(acPb).String(), nil
}

// ExecuteAction executes an action in a canonical structure remotely.
// The structure is the same as that produced by DownloadAction.
// top level >
//
//	> ac.textproto (Action text proto)
//	> cmd.textproto (Command text proto)
//	> input (Input root)
//	  > inputs...
func (c *Client) ExecuteAction(ctx context.Context, actionDigest, actionRoot, outDir string, oe outerr.OutErr) (*command.Metadata, error) {
	fmc := filemetadata.NewNoopCache()
	client := &rexec.Client{
		FileMetadataCache: fmc,
		GrpcClient:        c.GrpcClient,
	}
	inputRoot := ""
	if actionRoot != "" {
		var err error
		if actionDigest, err = c.prepProtos(ctx, actionRoot); err != nil {
			return nil, err
		}
		inputRoot = filepath.Join(actionRoot, "input")
	}
	cmd, err := c.prepCommand(ctx, client, actionDigest, inputRoot)
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
	fmt.Printf("Number of Input Files: %v\n", ec.Metadata.InputFiles)
	fmt.Printf("Number of Input Dirs: %v\n", ec.Metadata.InputDirectories)
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
	var showActionRes bytes.Buffer
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

	if actionProto.Timeout != nil {
		timeout := actionProto.Timeout.AsDuration()
		showActionRes.WriteString(fmt.Sprintf("Timeout: %s\n", timeout.String()))
	}

	commandProto := &repb.Command{}
	cmdDg, err := digest.NewFromProto(actionProto.GetCommandDigest())
	if err != nil {
		return "", err
	}
	showActionRes.WriteString("Command\n=======\n")
	showActionRes.WriteString(fmt.Sprintf("Command Digest: %v\n", cmdDg))

	log.Infof("Reading command from action digest..")
	if _, err := c.GrpcClient.ReadProto(ctx, cmdDg, commandProto); err != nil {
		return "", err
	}
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
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		output := outputs[path]
		if output.IsEmptyDirectory {
			res.WriteString(fmt.Sprintf("%v: [Directory digest: %v]\n", path, output.Digest))
		} else if output.SymlinkTarget != "" {
			res.WriteString(fmt.Sprintf("%v: [Symlink digest: %v, Symlink Target: %v]\n", path, output.Digest, output.SymlinkTarget))
		} else {
			res.WriteString(fmt.Sprintf("%v: [File digest: %v]\n", path, output.Digest))
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
