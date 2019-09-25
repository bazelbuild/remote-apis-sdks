// Package command defines common types to be used with command execution.
package command

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/digest"
	"github.com/pborman/uuid"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
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
)

var inputTypes = [...]string{"UnspecifiedInputType", "DirectoryInputType", "FileInputType"}

func (s InputType) String() string {
	if UnspecifiedInputType <= s && s <= FileInputType {
		return inputTypes[s]
	}
	return fmt.Sprintf("InvalidInputType(%d)", s)
}

// InputExclusion represents inputs to be excluded from being considered for command execution.
type InputExclusion struct {
	// Required: the path regular expression to match for exclusion.
	Regex string

	// The input type to match for exclusion.
	Type InputType
}

// VirtualInput represents an input that does not actually exist as a file on disk, but we want
// to stage it as a file on disk for the command execution.
type VirtualInput struct {
	// The path for the input file to be staged at, relative to the ExecRoot.
	Path string

	// The byte contents of the file to be staged.
	Contents []byte

	// Whether the file should be staged as executable.
	IsExecutable bool
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
}

// Identifiers is a group of identifiers of a command.
type Identifiers struct {
	// CommandID is an optional id to use to identify a command.
	CommandID string

	// InvocationID is an optional id to use to identify an invocation spanning multiple commands.
	InvocationID string

	// CorrelatedInvocationID is an optional id to use to identify a build spanning multiple invocations.
	CorrelatedInvocationID string

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
		c.Identifiers.InvocationID = uuid.New()
	}
	if c.InputSpec == nil {
		c.InputSpec = &InputSpec{}
	}
}

// ExecutionOptions specify how to execute a given Command.
type ExecutionOptions struct {
	// Whether to accept cached action results. Defaults to true.
	AcceptCached bool

	// When set, this execution results will not be cached.
	DoNotCache bool

	// Download command outputs after execution. Defaults to true.
	DownloadOutputs bool
}

// DefaultExecutionOptions returns the recommended ExecutionOptions.
func DefaultExecutionOptions() *ExecutionOptions {
	return &ExecutionOptions{
		AcceptCached:    true,
		DoNotCache:      false,
		DownloadOutputs: true,
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
	// TODO(olaola): Add a lot of other fields.
}

// ToREProto converts the Command to an RE API Command proto.
func (c *Command) ToREProto() *repb.Command {
	cmdPb := &repb.Command{
		Arguments:         c.Args,
		OutputFiles:       make([]string, len(c.OutputFiles)),
		OutputDirectories: make([]string, len(c.OutputDirs)),
		WorkingDirectory:  c.WorkingDir,
	}
	copy(cmdPb.OutputFiles, c.OutputFiles)
	copy(cmdPb.OutputDirectories, c.OutputDirs)
	sort.Strings(cmdPb.OutputFiles)
	sort.Strings(cmdPb.OutputDirectories)
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
