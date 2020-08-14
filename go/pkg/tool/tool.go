// Package tool provides implementation of the debugging related operations
// supported by go/cmd/remotetool package.
package tool

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/filemetadata"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/tree"

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
	resPb, err := c.getActionResult(ctx, actionDigest)
	if err != nil {
		return err
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
	if err := c.GrpcClient.ReadProto(ctx, acDg, actionProto); err != nil {
		return "", err
	}
	showActionRes.WriteString("Command\n========\n")
	showActionRes.WriteString(fmt.Sprintf("Command Digest: %v\n", actionProto.GetCommandDigest().Hash))

	log.Infof("Reading command from action digest..")
	commandProto := &repb.Command{}
	cmdDg, err := digest.NewFromProto(actionProto.GetCommandDigest())
	if err != nil {
		return "", err
	}
	if err := c.GrpcClient.ReadProto(ctx, cmdDg, commandProto); err != nil {
		return "", err
	}
	cmdStr := strings.Join(commandProto.GetArguments(), " ")
	showActionRes.WriteString(fmt.Sprintf("\t%v\n", cmdStr))

	log.Infof("Fetching input tree from input root digest..")
	inpTree, err := c.getInputTree(ctx, actionProto.GetInputRootDigest())
	if err != nil {
		return "", err
	}
	showActionRes.WriteString("\nInputs\n======\n")
	showActionRes.WriteString(inpTree)

	log.Infof("Fetching output tree from action result..")
	outs, err := c.getOutputs(ctx, resPb)
	if err != nil {
		return "", err
	}
	showActionRes.WriteString("\n\n")
	showActionRes.WriteString(outs)
	return showActionRes.String(), nil
}

func (c *Client) getOutputs(ctx context.Context, actionRes *repb.ActionResult) (string, error) {
	var res bytes.Buffer
	res.WriteString("Output Files:\n=============")
	for _, of := range actionRes.GetOutputFiles() {
		res.WriteString(fmt.Sprintf("\n%v, digest: %v", of.GetPath(), of.GetDigest().Hash))
	}

	res.WriteString("\n\nOutput Files From Directories:\n=================")
	for _, od := range actionRes.GetOutputDirectories() {
		treeDigest := od.GetTreeDigest()
		dg, err := digest.NewFromProto(treeDigest)
		if err != nil {
			return "", err
		}
		outDirTree := &repb.Tree{}
		if err := c.GrpcClient.ReadProto(ctx, dg, outDirTree); err != nil {
			return "", err
		}

		outputs, err := c.flattenTree(ctx, outDirTree)
		if err != nil {
			return "", err
		}
		res.WriteString("\n")
		res.WriteString(outputs)
	}
	return res.String(), nil
}

func (c *Client) getInputTree(ctx context.Context, root *repb.Digest) (string, error) {
	var res bytes.Buffer

	dg, err := digest.NewFromProto(root)
	if err != nil {
		return "", err
	}
	rootDir := &repb.Directory{}
	if err := c.GrpcClient.ReadProto(ctx, dg, rootDir); err != nil {
		return "", fmt.Errorf("error fetching root dir proto for digest %v: %v", dg, err)
	}

	if len(rootDir.GetDirectories()) < 1 {
		return "", nil
	}
	rootDirNode := rootDir.GetDirectories()[0]
	res.WriteString(fmt.Sprintf("%v: [Directory digest: %v]", rootDirNode.GetName(), rootDirNode.GetDigest().Hash))

	dirs, err := c.GrpcClient.GetDirectoryTree(ctx, rootDirNode.GetDigest())
	if err != nil {
		return "", err
	}
	t := &repb.Tree{
		Root:     rootDir,
		Children: dirs,
	}
	inputs, err := c.flattenTree(ctx, t)
	if err != nil {
		return "", err
	}
	res.WriteString("\n")
	res.WriteString(inputs)

	return res.String(), nil
}

func (c *Client) flattenTree(ctx context.Context, t *repb.Tree) (string, error) {
	var res bytes.Buffer
	outputs, err := tree.FlattenTree(t, "")
	if err != nil {
		return "", err
	}
	for path, output := range outputs {
		if output.IsEmptyDirectory {
			res.WriteString(fmt.Sprintf("%v: [Directory digest: %v]\n", path, output.Digest))
		} else if output.SymlinkTarget != "" {
			res.WriteString(fmt.Sprintf("%v: [Symlink digest: %v, Symlink Target: %v]\n", path, output.Digest, output.SymlinkTarget))
		} else {
			res.WriteString(fmt.Sprintf("%v: [File digest: %v]\n", path, output.Digest))
		}
	}
	return res.String(), nil
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
	if resPb == nil {
		return nil, fmt.Errorf("action digest %v not found in cache", d)
	}
	return resPb, nil
}
