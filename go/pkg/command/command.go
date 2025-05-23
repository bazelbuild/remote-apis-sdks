// Package command defines common types to be used with command execution.
package command

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	log "github.com/golang/glog"
	"github.com/google/uuid"

	cpb "github.com/bazelbuild/remote-apis-sdks/go/api/command"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	anypb "google.golang.org/protobuf/types/known/anypb"
	tspb "google.golang.org/protobuf/types/known/timestamppb"
)

// InputType can be specified to narrow down the matching for a given input path.
type InputType int

const (
	// UnspecifiedInputType means any input type will match.
	UnspecifiedInputType InputType = iota

	// DirectoryInputType means only directories match.
	DirectoryInputType

	// FileInputType means only files match.
	FileInputType

	// SymlinkInputType means only symlink match.
	SymlinkInputType
)

var inputTypes = [...]string{"UnspecifiedInputType", "DirectoryInputType", "FileInputType"}

func (s InputType) String() string {
	if UnspecifiedInputType <= s && s <= FileInputType {
		return inputTypes[s]
	}
	return fmt.Sprintf("InvalidInputType(%d)", s)
}

// SymlinkBehaviorType represents how symlinks are handled.
type SymlinkBehaviorType int

const (
	// UnspecifiedSymlinkBehavior means following clients.TreeSymlinkOpts
	// or DefaultTreeSymlinkOpts if clients.TreeSymlinkOpts is null.
	UnspecifiedSymlinkBehavior SymlinkBehaviorType = iota

	// ResolveSymlink means symlinks are resolved.
	ResolveSymlink

	// PreserveSymlink means symlinks are kept as-is.
	PreserveSymlink
)

var symlinkBehaviorType = [...]string{"UnspecifiedSymlinkBehavior", "ResolveSymlink", "PreserveSymlink"}

func (s SymlinkBehaviorType) String() string {
	if UnspecifiedSymlinkBehavior <= s && s <= PreserveSymlink {
		return symlinkBehaviorType[s-UnspecifiedSymlinkBehavior]
	}
	return fmt.Sprintf("InvalidSymlinkBehaviorType(%d)", s)
}

func SymlinkBehaviorFromString(s string) SymlinkBehaviorType {
	switch s {
	case "resolve", "ResolveSymlink":
		return ResolveSymlink
	case "preserve", "PreserveSymlink":
		return PreserveSymlink
	default:
		return UnspecifiedSymlinkBehavior
	}
}

// InputExclusion represents inputs to be excluded from being considered for command execution.
type InputExclusion struct {
	// Required: the path regular expression to match for exclusion.
	Regex string

	// The input type to match for exclusion.
	Type InputType
}

// VirtualInput represents an input that may exist on disk but shouldn't be accessed.
// We want to stage it on disk for the command execution.
type VirtualInput struct {
	// The path for the input to be staged at, relative to the ExecRoot.
	Path string

	// The byte contents of the file to be staged.
	Contents []byte

	// The digest of the virtual input that is expected to exist in the CAS.
	// Should not be used together with Contents.
	Digest string

	// Whether the file should be staged as executable.
	IsExecutable bool

	// Whether the file is actually an empty directory. This is used to provide
	// empty directory inputs. When this is set, Contents and IsExecutable are
	// ignored.
	IsEmptyDirectory bool

	// Mtime of the virtual input.
	Mtime time.Time

	// The virtual inputs' mode and permissions bits.
	FileMode os.FileMode
}

// InputSpec represents all the required inputs to a remote command.
type InputSpec struct {
	// Input paths (files or directories) that need to be present for the command execution.
	Inputs []string

	// Inputs not present on the local file system, but should be staged for command execution.
	VirtualInputs []*VirtualInput

	// Inputs matching these patterns will be excluded.
	InputExclusions []*InputExclusion

	// Environment variables the command relies on.
	EnvironmentVariables map[string]string

	// SymlinkBehavior represents the way symlinks will be handled.
	SymlinkBehavior SymlinkBehaviorType

	// Node properties of inputs.
	InputNodeProperties map[string]*cpb.NodeProperties
}

// String returns the string representation of the VirtualInput.
func (s *VirtualInput) String() string {
	return fmt.Sprintf("%+v", *s)
}

// String returns the string representation of the InputExclusion.
func (s *InputExclusion) String() string {
	return fmt.Sprintf("%+v", *s)
}

// Identifiers is a group of identifiers of a command.
type Identifiers struct {
	// CommandID is an optional id to use to identify a command.
	CommandID string

	// InvocationID is an optional id to use to identify an invocation spanning multiple commands.
	InvocationID string

	// CorrelatedInvocationsID is an optional id to use to identify a build spanning multiple invocations.
	CorrelatedInvocationsID string

	// ToolName is an optional tool name to pass to the remote server for logging.
	ToolName string

	// ToolVersion is an optional tool version to pass to the remote server for logging.
	ToolVersion string

	// ExecutionID is a UUID generated for a particular execution of this command.
	ExecutionID string
}

// Command encompasses the complete information required to execute a command remotely.
// To make sure to initialize a valid Command object, call FillDefaultFieldValues on the created
// struct.
type Command struct {
	// Identifiers used to identify this command to be passed to RE.
	Identifiers *Identifiers

	// Args (required): command line elements to execute.
	Args []string

	// ExecRoot is an absolute path to the execution root of the command. All the other paths are
	// specified relatively to this path.
	ExecRoot string

	// WorkingDir is the working directory, relative to the exec root, for the command to run
	// in. It must be a directory which exists in the input tree. If it is left empty, then the
	// action is run from the exec root.
	WorkingDir string

	// RemoteWorkingDir is the working directory when executing the command on RE server.
	// It's relative to exec root and, if provided, needs to have the same number of levels
	// as WorkingDir. If not provided, the remote command is run from the WorkingDir
	RemoteWorkingDir string

	// InputSpec: the command inputs.
	InputSpec *InputSpec

	// OutputFiles are the command output files.
	OutputFiles []string

	// OutputDirs are the command output directories.
	// The files and directories will likely be merged into a single Outputs field in the future.
	OutputDirs []string

	// Timeout is an optional duration to wait for command execution before timing out.
	Timeout time.Duration

	// Platform is the platform to use for the execution.
	Platform map[string]string
}

func marshallMap(m map[string]string, buf *[]byte) {
	var pkeys []string
	for k := range m {
		pkeys = append(pkeys, k)
	}
	sort.Strings(pkeys)
	for _, k := range pkeys {
		*buf = append(*buf, []byte(k)...)
		*buf = append(*buf, []byte(m[k])...)
	}
}

func marshallSlice(s []string, buf *[]byte) {
	for _, i := range s {
		*buf = append(*buf, []byte(i)...)
	}
}

func marshallSortedSlice(s []string, buf *[]byte) {
	ss := make([]string, len(s))
	copy(ss, s)
	sort.Strings(ss)
	marshallSlice(ss, buf)
}

// Validate checks whether all required command fields have been specified.
func (c *Command) Validate() error {
	if c == nil {
		return nil
	}
	if len(c.Args) == 0 {
		return errors.New("missing command arguments")
	}
	if c.ExecRoot == "" {
		return errors.New("missing command exec root")
	}
	if c.InputSpec == nil {
		return errors.New("missing command input spec")
	}
	if c.Identifiers == nil {
		return errors.New("missing command identifiers")
	}
	if c.RemoteWorkingDir != "" && levels(c.RemoteWorkingDir) != levels(c.WorkingDir) {
		return fmt.Errorf("invalid RemoteWorkingDir=%q[%v level(s)], it's expected to have the same depth as WorkingDir=%q[%v level(s)]",
			c.RemoteWorkingDir, levels(c.RemoteWorkingDir), c.WorkingDir, levels(c.WorkingDir))
	}
	// TODO(olaola): make Platform required?
	return nil
}

// Generates a stable id for the command.
func (c *Command) stableID() string {
	var buf []byte
	marshallSlice(c.Args, &buf)
	buf = append(buf, []byte(c.ExecRoot)...)
	buf = append(buf, []byte(c.WorkingDir)...)
	marshallSortedSlice(c.OutputFiles, &buf)
	marshallSortedSlice(c.OutputDirs, &buf)
	buf = append(buf, []byte(c.Timeout.String())...)
	marshallMap(c.Platform, &buf)
	if c.InputSpec != nil {
		marshallMap(c.InputSpec.EnvironmentVariables, &buf)
		marshallSortedSlice(c.InputSpec.Inputs, &buf)
		inputExclusions := make([]*InputExclusion, len(c.InputSpec.InputExclusions))
		copy(inputExclusions, c.InputSpec.InputExclusions)
		sort.Slice(inputExclusions, func(i, j int) bool {
			e1 := inputExclusions[i]
			e2 := inputExclusions[j]
			return e1.Regex > e2.Regex || e1.Regex == e2.Regex && e1.Type > e2.Type
		})
		for _, e := range inputExclusions {
			buf = append(buf, []byte(e.Regex)...)
			buf = append(buf, []byte(e.Type.String())...)
		}
	}
	sha256Arr := sha256.Sum256(buf)
	return hex.EncodeToString(sha256Arr[:])[:8]
}

// FillDefaultFieldValues initializes valid default values to inner Command fields.
// This function should be called on every new Command object before use.
func (c *Command) FillDefaultFieldValues() {
	if c == nil {
		return
	}
	if c.Identifiers == nil {
		c.Identifiers = &Identifiers{}
	}
	if c.Identifiers.CommandID == "" {
		c.Identifiers.CommandID = c.stableID()
	}
	if c.Identifiers.ToolName == "" {
		c.Identifiers.ToolName = "remote-client"
	}
	if c.Identifiers.InvocationID == "" {
		if id, err := uuid.NewRandom(); err == nil {
			c.Identifiers.InvocationID = id.String()
		} else {
			log.Warningf("Failed to generate InvocationID: %s", err)
		}
	}
	if c.Identifiers.ExecutionID == "" {
		if id, err := uuid.NewRandom(); err == nil {
			c.Identifiers.ExecutionID = id.String()
		} else {
			log.Warningf("Failed to generate ExecutionID: %s", err)
		}
	}
	if c.InputSpec == nil {
		c.InputSpec = &InputSpec{}
	}
}

func levels(path string) int {
	return len(strings.Split(path, string(os.PathSeparator)))
}

// ExecutionOptions specify how to execute a given Command.
type ExecutionOptions struct {
	// Whether to accept cached action results. Defaults to true.
	AcceptCached bool

	// When set, this execution results will not be cached.
	DoNotCache bool

	// Download command outputs after execution. Defaults to true.
	DownloadOutputs bool

	// Preserve mtimes for unchanged outputs when downloading. Defaults to false.
	PreserveUnchangedOutputMtime bool

	// Download command stdout and stderr. Defaults to true. If StreamOutErr is also set, this value
	// is ignored for uncached actions if the server provides log streams for both stdout and stderr.
	// For cached action results, or if the server does not provide log streams for stdout or stderr,
	// this value will determine whether stdout and stderr is downloaded.
	DownloadOutErr bool

	// Request that stdout and stderr be streamed back to the client while the action is running.
	// Defaults to false. If either stream is not provided by the server, the client will fall back to
	// downloading the corresponding streams after the action has completed, provided DownloadOutErr
	// is also set. The client may expect a delay in this scenario as the streams are downloaded after
	// the fact.
	StreamOutErr bool
}

// DefaultExecutionOptions returns the recommended ExecutionOptions.
func DefaultExecutionOptions() *ExecutionOptions {
	return &ExecutionOptions{
		AcceptCached:                 true,
		DoNotCache:                   false,
		DownloadOutputs:              true,
		PreserveUnchangedOutputMtime: false,
		DownloadOutErr:               true,
		StreamOutErr:                 false,
	}
}

// ResultStatus represents the options for a finished command execution.
type ResultStatus int

const (
	// UnspecifiedResultStatus is an invalid value, should not be used.
	UnspecifiedResultStatus ResultStatus = iota

	// SuccessResultStatus indicates that the command executed successfully.
	SuccessResultStatus

	// CacheHitResultStatus indicates that the command was a cache hit.
	CacheHitResultStatus

	// NonZeroExitResultStatus indicates that the command executed with a non zero exit code.
	NonZeroExitResultStatus

	// TimeoutResultStatus indicates that the command exceeded its specified deadline.
	TimeoutResultStatus

	// InterruptedResultStatus indicates that the command execution was interrupted.
	InterruptedResultStatus

	// RemoteErrorResultStatus indicates that an error occurred on the remote server.
	RemoteErrorResultStatus

	// LocalErrorResultStatus indicates that an error occurred locally.
	LocalErrorResultStatus
)

var resultStatuses = [...]string{
	"UnspecifiedResultStatus",
	"SuccessResultStatus",
	"CacheHitResultStatus",
	"NonZeroExitResultStatus",
	"TimeoutResultStatus",
	"InterruptedResultStatus",
	"RemoteErrorResultStatus",
	"LocalErrorResultStatus",
}

// IsOk returns whether the status indicates a successful action.
func (s ResultStatus) IsOk() bool {
	return s == SuccessResultStatus || s == CacheHitResultStatus
}

func (s ResultStatus) String() string {
	if UnspecifiedResultStatus <= s && s <= LocalErrorResultStatus {
		return resultStatuses[s]
	}
	return fmt.Sprintf("InvalidResultStatus(%d)", s)
}

// Result is the result of a finished command execution.
type Result struct {
	// Command exit code.
	ExitCode int
	// Status of the finished run.
	Status ResultStatus
	// Any error encountered.
	Err error
}

// IsOk returns whether the result was successful.
func (r *Result) IsOk() bool {
	return r.Status.IsOk()
}

// LocalErrorExitCode is an exit code corresponding to a local error.
const LocalErrorExitCode = 35

// TimeoutExitCode is an exit code corresponding to the command timing out remotely.
const TimeoutExitCode = /*SIGNAL_BASE=*/ 128 + /*SIGALRM=*/ 14

// RemoteErrorExitCode is an exit code corresponding to a remote server error.
const RemoteErrorExitCode = 45

// InterruptedExitCode is an exit code corresponding to an execution interruption by the user.
const InterruptedExitCode = 8

// NewLocalErrorResult constructs a Result from a local error.
func NewLocalErrorResult(err error) *Result {
	return &Result{
		ExitCode: LocalErrorExitCode,
		Status:   LocalErrorResultStatus,
		Err:      err,
	}
}

// NewRemoteErrorResult constructs a Result from a remote error.
func NewRemoteErrorResult(err error) *Result {
	return &Result{
		ExitCode: RemoteErrorExitCode,
		Status:   RemoteErrorResultStatus,
		Err:      err,
	}
}

// NewResultFromExitCode constructs a Result from a given command exit code.
func NewResultFromExitCode(exitCode int) *Result {
	st := SuccessResultStatus
	if exitCode != 0 {
		st = NonZeroExitResultStatus
	}
	return &Result{
		ExitCode: exitCode,
		Status:   st,
	}
}

// NewTimeoutResult constructs a new result for a timeout-exceeded command.
func NewTimeoutResult() *Result {
	return &Result{
		ExitCode: TimeoutExitCode,
		Status:   TimeoutResultStatus,
	}
}

// TimeInterval is a time window for an event.
type TimeInterval struct {
	From, To time.Time
}

// These are the events that we export time metrics on:
const (
	// EventServerQueued: Queued time on the remote server.
	EventServerQueued = "ServerQueued"

	// EventServerWorker: The total remote worker (bot) time.
	EventServerWorker = "ServerWorker"

	// EventServerWorkerInputFetch: Time to fetch inputs to the remote bot.
	EventServerWorkerInputFetch = "ServerWorkerInputFetch"

	// EventServerWorkerExecution: The actual execution on the remote bot.
	EventServerWorkerExecution = "ServerWorkerExecution"

	// EventServerWorkerOutputUpload: Uploading outputs to the CAS on the bot.
	EventServerWorkerOutputUpload = "ServerWorkerOutputUpload"

	// EventDownloadResults: Downloading action results from CAS.
	EventDownloadResults = "DownloadResults"

	// EventComputeMerkleTree: Computing the input Merkle tree.
	EventComputeMerkleTree = "ComputeMerkleTree"

	// EventCheckActionCache: Checking the action cache.
	EventCheckActionCache = "CheckActionCache"

	// EventUpdateCachedResult: Uploading local outputs to CAS and updating cached
	// action result.
	EventUpdateCachedResult = "UpdateCachedResult"

	// EventUploadInputs: Uploading action inputs to CAS for remote execution.
	EventUploadInputs = "UploadInputs"

	// EventExecuteRemotely: Total time to execute remotely.
	EventExecuteRemotely = "ExecuteRemotely"
)

// Metadata is general information associated with a Command execution.
type Metadata struct {
	// CommandDigest is a digest of the command being executed. It can be used
	// to detect changes in the command between builds.
	CommandDigest digest.Digest
	// ActionDigest is a digest of the action being executed. It can be used
	// to detect changes in the action between builds.
	ActionDigest digest.Digest
	// The total number of input files.
	InputFiles int
	// The total number of input directories.
	InputDirectories int
	// The overall number of bytes from all the inputs.
	TotalInputBytes int64
	// Event times for remote events, by event name.
	EventTimes map[string]*TimeInterval

	AuxiliaryMetadata []*anypb.Any
	// The total number of output files (incl symlinks).
	OutputFiles int
	// The total number of output directories (incl symlinks, but not recursive).
	OutputDirectories int
	// The overall number of bytes from all the output files (incl. stdout/stderr, but not symlinks).
	TotalOutputBytes int64
	// Output File digests.
	OutputFileDigests map[string]digest.Digest
	// Output File executable bit.
	OutputFileIsExecutable map[string]bool
	// Output Directory digests.
	OutputDirectoryDigests map[string]digest.Digest
	// Output Symlinks.
	OutputSymlinks map[string]string
	// Missing digests that are uploaded to CAS.
	MissingDigests []digest.Digest
	// LogicalBytesUploaded is the sum of sizes in bytes of the blobs that were uploaded. It should be
	// the same value as the sum of digest sizes in MissingDigests.
	LogicalBytesUploaded int64
	// RealBytesUploaded is the number of bytes that were put on the wire for upload (exclusing metadata).
	// It may differ from LogicalBytesUploaded due to compression.
	RealBytesUploaded int64
	// LogicalBytesDownloaded is the sum of sizes in bytes of the blobs that were downloaded. It should be
	// the same value as the sum of digest sizes in OutputDigests.
	LogicalBytesDownloaded int64
	// RealBytesDownloaded is the number of bytes that were put on the wire for download (exclusing metadata).
	// It may differ from LogicalBytesDownloaded due to compression.
	RealBytesDownloaded int64
	// StderrDigest is a digest of the standard error after being executed.
	StderrDigest digest.Digest
	// StdoutDigest is a digest of the standard output after being executed.
	StdoutDigest digest.Digest
	// TODO(olaola): Add a lot of other fields.
}

// ToREProto converts the Command to an RE API Command proto.
// `useOutputPathsField` selects what field/s to fill with the paths of outputs,
// which will depend on the RE API version.
func (c *Command) ToREProto(useOutputPathsField bool) *repb.Command {
	workingDir := c.RemoteWorkingDir
	if workingDir == "" {
		workingDir = c.WorkingDir
	}
	cmdPb := &repb.Command{
		Arguments:        c.Args,
		WorkingDirectory: workingDir,
	}

	// In v2.1 of the RE API the `output_{files, directories}` fields were
	// replaced by a single field: `output_paths`.
	if useOutputPathsField {
		cmdPb.OutputPaths = make([]string, 0, len(c.OutputFiles)+len(c.OutputDirs))
		cmdPb.OutputPaths = append(cmdPb.OutputPaths, c.OutputFiles...)
		cmdPb.OutputPaths = append(cmdPb.OutputPaths, c.OutputDirs...)
		sort.Strings(cmdPb.OutputPaths)
	} else {
		cmdPb.OutputFiles = make([]string, len(c.OutputFiles))
		copy(cmdPb.OutputFiles, c.OutputFiles)
		sort.Strings(cmdPb.OutputFiles)

		cmdPb.OutputDirectories = make([]string, len(c.OutputDirs))
		copy(cmdPb.OutputDirectories, c.OutputDirs)
		sort.Strings(cmdPb.OutputDirectories)
	}

	for name, val := range c.InputSpec.EnvironmentVariables {
		cmdPb.EnvironmentVariables = append(cmdPb.EnvironmentVariables, &repb.Command_EnvironmentVariable{Name: name, Value: val})
	}
	sort.Slice(cmdPb.EnvironmentVariables, func(i, j int) bool { return cmdPb.EnvironmentVariables[i].Name < cmdPb.EnvironmentVariables[j].Name })
	if len(c.Platform) > 0 {
		cmdPb.Platform = &repb.Platform{}
		for name, val := range c.Platform {
			cmdPb.Platform.Properties = append(cmdPb.Platform.Properties, &repb.Platform_Property{Name: name, Value: val})
		}
		sort.Slice(cmdPb.Platform.Properties, func(i, j int) bool { return cmdPb.Platform.Properties[i].Name < cmdPb.Platform.Properties[j].Name })
	}
	return cmdPb
}

func FromREProto(cmdPb *repb.Command) *Command {
	cmd := &Command{
		InputSpec: &InputSpec{
			EnvironmentVariables: make(map[string]string),
			InputNodeProperties:  make(map[string]*cpb.NodeProperties),
		},
		Identifiers: &Identifiers{},
		WorkingDir:  cmdPb.WorkingDirectory,
		OutputFiles: cmdPb.OutputFiles,
		OutputDirs:  cmdPb.OutputDirectories,
		Platform:    make(map[string]string),
		Args:        cmdPb.Arguments,
	}

	// In v2.1 of the RE API the `output_{files, directories}` fields were
	// replaced by a single field: `output_paths`.
	if len(cmdPb.OutputPaths) > 0 {
		cmd.OutputFiles = cmdPb.OutputPaths
		cmd.OutputDirs = nil
	}
	for _, ev := range cmdPb.EnvironmentVariables {
		cmd.InputSpec.EnvironmentVariables[ev.Name] = ev.Value
	}
	for _, pt := range cmdPb.GetPlatform().GetProperties() {
		cmd.Platform[pt.Name] = pt.Value
	}
	return cmd
}

// FromProto parses a Command struct from a proto message.
func FromProto(p *cpb.Command) *Command {
	ids := &Identifiers{
		CommandID:               p.GetIdentifiers().GetCommandId(),
		InvocationID:            p.GetIdentifiers().GetInvocationId(),
		CorrelatedInvocationsID: p.GetIdentifiers().GetCorrelatedInvocationsId(),
		ToolName:                p.GetIdentifiers().GetToolName(),
		ToolVersion:             p.GetIdentifiers().GetToolVersion(),
		ExecutionID:             p.GetIdentifiers().GetExecutionId(),
	}
	is := inputSpecFromProto(p.GetInput())
	return &Command{
		Identifiers:      ids,
		ExecRoot:         p.ExecRoot,
		Args:             p.Args,
		WorkingDir:       p.WorkingDirectory,
		RemoteWorkingDir: p.RemoteWorkingDirectory,
		InputSpec:        is,
		OutputFiles:      p.GetOutput().GetOutputFiles(),
		OutputDirs:       p.GetOutput().GetOutputDirectories(),
		Timeout:          time.Duration(p.ExecutionTimeout) * time.Second,
		Platform:         p.Platform,
	}
}

func inputSpecFromProto(is *cpb.InputSpec) *InputSpec {
	var excl []*InputExclusion
	for _, ex := range is.GetExcludeInputs() {
		excl = append(excl, &InputExclusion{
			Regex: ex.Regex,
			Type:  inputTypeFromProto(ex.Type),
		})
	}
	var vis []*VirtualInput
	for _, vi := range is.GetVirtualInputs() {
		contents := make([]byte, len(vi.Contents))
		copy(contents, vi.Contents)
		vis = append(vis, &VirtualInput{
			Path:             vi.Path,
			Contents:         contents,
			IsExecutable:     vi.IsExecutable,
			IsEmptyDirectory: vi.IsEmptyDirectory,
			Digest:           vi.Digest,
			Mtime:            vi.Mtime.AsTime(),
			FileMode:         os.FileMode(vi.Filemode),
		})
	}
	return &InputSpec{
		Inputs:               is.GetInputs(),
		VirtualInputs:        vis,
		InputExclusions:      excl,
		EnvironmentVariables: is.GetEnvironmentVariables(),
		SymlinkBehavior:      symlinkBehaviorFromProto(is.GetSymlinkBehavior()),
		InputNodeProperties:  is.GetInputNodeProperties(),
	}
}

func NodePropertiesToAPI(np *cpb.NodeProperties) *repb.NodeProperties {
	if np == nil {
		return nil
	}
	res := &repb.NodeProperties{
		Mtime:    np.GetMtime(),
		UnixMode: np.GetUnixMode(),
	}
	if np.Properties != nil {
		res.Properties = make([]*repb.NodeProperty, 0, len(np.Properties))
	}
	for _, p := range np.GetProperties() {
		res.Properties = append(res.Properties, &repb.NodeProperty{
			Name:  p.Name,
			Value: p.Value,
		})
	}
	return res
}

func NodePropertiesFromAPI(np *repb.NodeProperties) *cpb.NodeProperties {
	if np == nil {
		return nil
	}
	res := &cpb.NodeProperties{
		Mtime:    np.GetMtime(),
		UnixMode: np.GetUnixMode(),
	}
	if np.Properties != nil {
		res.Properties = make([]*cpb.NodeProperty, 0, len(np.Properties))
	}
	for _, p := range np.GetProperties() {
		res.Properties = append(res.Properties, &cpb.NodeProperty{
			Name:  p.Name,
			Value: p.Value,
		})
	}
	return res
}

func inputSpecToProto(is *InputSpec) *cpb.InputSpec {
	var excl []*cpb.ExcludeInput
	for _, ex := range is.InputExclusions {
		excl = append(excl, &cpb.ExcludeInput{
			Regex: ex.Regex,
			Type:  inputTypeToProto(ex.Type),
		})
	}
	var vis []*cpb.VirtualInput
	for _, vi := range is.VirtualInputs {
		contents := make([]byte, len(vi.Contents))
		copy(contents, vi.Contents)
		vis = append(vis, &cpb.VirtualInput{
			Path:             vi.Path,
			Contents:         contents,
			IsExecutable:     vi.IsExecutable,
			IsEmptyDirectory: vi.IsEmptyDirectory,
			Digest:           vi.Digest,
			Mtime:            tspb.New(vi.Mtime),
			Filemode:         uint32(vi.FileMode),
		})
	}
	return &cpb.InputSpec{
		Inputs:               is.Inputs,
		VirtualInputs:        vis,
		ExcludeInputs:        excl,
		EnvironmentVariables: is.EnvironmentVariables,
		SymlinkBehavior:      symlinkBehaviorToProto(is.SymlinkBehavior),
		InputNodeProperties:  is.InputNodeProperties,
	}
}

func inputTypeFromProto(t cpb.InputType_Value) InputType {
	switch t {
	case cpb.InputType_DIRECTORY:
		return DirectoryInputType
	case cpb.InputType_FILE:
		return FileInputType
	default:
		return UnspecifiedInputType
	}
}

func inputTypeToProto(t InputType) cpb.InputType_Value {
	switch t {
	case DirectoryInputType:
		return cpb.InputType_DIRECTORY
	case FileInputType:
		return cpb.InputType_FILE
	default:
		return cpb.InputType_UNSPECIFIED
	}
}

func symlinkBehaviorFromProto(t cpb.SymlinkBehaviorType_Value) SymlinkBehaviorType {
	switch t {
	case cpb.SymlinkBehaviorType_RESOLVE:
		return ResolveSymlink
	case cpb.SymlinkBehaviorType_PRESERVE:
		return PreserveSymlink
	default:
		return UnspecifiedSymlinkBehavior
	}
}

func symlinkBehaviorToProto(t SymlinkBehaviorType) cpb.SymlinkBehaviorType_Value {
	switch t {
	case ResolveSymlink:
		return cpb.SymlinkBehaviorType_RESOLVE
	case PreserveSymlink:
		return cpb.SymlinkBehaviorType_PRESERVE
	default:
		return cpb.SymlinkBehaviorType_UNSPECIFIED
	}
}

func protoStatusFromResultStatus(s ResultStatus) cpb.CommandResultStatus_Value {
	switch s {
	case SuccessResultStatus:
		return cpb.CommandResultStatus_SUCCESS
	case CacheHitResultStatus:
		return cpb.CommandResultStatus_CACHE_HIT
	case NonZeroExitResultStatus:
		return cpb.CommandResultStatus_NON_ZERO_EXIT
	case TimeoutResultStatus:
		return cpb.CommandResultStatus_TIMEOUT
	case InterruptedResultStatus:
		return cpb.CommandResultStatus_INTERRUPTED
	case RemoteErrorResultStatus:
		return cpb.CommandResultStatus_REMOTE_ERROR
	case LocalErrorResultStatus:
		return cpb.CommandResultStatus_LOCAL_ERROR
	default:
		return cpb.CommandResultStatus_UNKNOWN
	}
}

func protoStatusToResultStatus(s cpb.CommandResultStatus_Value) ResultStatus {
	switch s {
	case cpb.CommandResultStatus_SUCCESS:
		return SuccessResultStatus
	case cpb.CommandResultStatus_CACHE_HIT:
		return CacheHitResultStatus
	case cpb.CommandResultStatus_NON_ZERO_EXIT:
		return NonZeroExitResultStatus
	case cpb.CommandResultStatus_TIMEOUT:
		return TimeoutResultStatus
	case cpb.CommandResultStatus_INTERRUPTED:
		return InterruptedResultStatus
	case cpb.CommandResultStatus_REMOTE_ERROR:
		return RemoteErrorResultStatus
	case cpb.CommandResultStatus_LOCAL_ERROR:
		return LocalErrorResultStatus
	default:
		return UnspecifiedResultStatus
	}
}

// ToProto serializes a Command struct into a proto message.
func ToProto(cmd *Command) *cpb.Command {
	if cmd == nil {
		return nil
	}
	cPb := &cpb.Command{
		ExecRoot:               cmd.ExecRoot,
		Input:                  inputSpecToProto(cmd.InputSpec),
		Output:                 &cpb.OutputSpec{OutputFiles: cmd.OutputFiles, OutputDirectories: cmd.OutputDirs},
		Args:                   cmd.Args,
		ExecutionTimeout:       int32(cmd.Timeout.Seconds()),
		WorkingDirectory:       cmd.WorkingDir,
		RemoteWorkingDirectory: cmd.RemoteWorkingDir,
		Platform:               cmd.Platform,
	}
	if cmd.Identifiers != nil {
		cPb.Identifiers = &cpb.Identifiers{
			CommandId:               cmd.Identifiers.CommandID,
			InvocationId:            cmd.Identifiers.InvocationID,
			CorrelatedInvocationsId: cmd.Identifiers.CorrelatedInvocationsID,
			ToolName:                cmd.Identifiers.ToolName,
			ToolVersion:             cmd.Identifiers.ToolVersion,
			ExecutionId:             cmd.Identifiers.ExecutionID,
		}
	}
	return cPb
}

// ResultToProto serializes a command.Result struct into a proto message.
func ResultToProto(res *Result) *cpb.CommandResult {
	if res == nil {
		return nil
	}
	resPb := &cpb.CommandResult{
		Status:   protoStatusFromResultStatus(res.Status),
		ExitCode: int32(res.ExitCode),
	}
	if res.Err != nil {
		resPb.Msg = res.Err.Error()
	}
	return resPb
}

// ResultFromProto parses a command.Result struct from a proto message.
func ResultFromProto(res *cpb.CommandResult) *Result {
	if res == nil {
		return nil
	}
	var err error
	if res.Msg != "" {
		err = errors.New(res.Msg)
	}
	return &Result{
		Status:   protoStatusToResultStatus(res.Status),
		ExitCode: int(res.ExitCode),
		Err:      err,
	}
}

// TimeToProto converts a valid time.Time into a proto Timestamp.
func TimeToProto(t time.Time) *tspb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return tspb.New(t)
}

// TimeFromProto converts a valid Timestamp proto into a time.Time.
func TimeFromProto(tPb *tspb.Timestamp) time.Time {
	if tPb == nil {
		return time.Time{}
	}
	return tPb.AsTime()
}

// TimeIntervalToProto serializes the SDK TimeInterval into a proto.
func TimeIntervalToProto(t *TimeInterval) *cpb.TimeInterval {
	if t == nil {
		return nil
	}
	return &cpb.TimeInterval{
		From: TimeToProto(t.From),
		To:   TimeToProto(t.To),
	}
}

// TimeIntervalFromProto parses the SDK TimeInterval from a proto.
func TimeIntervalFromProto(t *cpb.TimeInterval) *TimeInterval {
	if t == nil {
		return nil
	}
	return &TimeInterval{
		From: TimeFromProto(t.From),
		To:   TimeFromProto(t.To),
	}
}
