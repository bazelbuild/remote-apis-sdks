// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.22.0
// 	protoc        v3.8.0
// source: go/api/command/command.proto

package cmd

import (
	proto "github.com/golang/protobuf/proto"
	timestamp "github.com/golang/protobuf/ptypes/timestamp"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// This is a compile-time assertion that a sufficiently up-to-date version
// of the legacy proto package is being used.
const _ = proto.ProtoPackageIsVersion4

type InputType_Value int32

const (
	InputType_UNSPECIFIED InputType_Value = 0
	InputType_DIRECTORY   InputType_Value = 1
	InputType_FILE        InputType_Value = 2
)

// Enum value maps for InputType_Value.
var (
	InputType_Value_name = map[int32]string{
		0: "UNSPECIFIED",
		1: "DIRECTORY",
		2: "FILE",
	}
	InputType_Value_value = map[string]int32{
		"UNSPECIFIED": 0,
		"DIRECTORY":   1,
		"FILE":        2,
	}
)

func (x InputType_Value) Enum() *InputType_Value {
	p := new(InputType_Value)
	*p = x
	return p
}

func (x InputType_Value) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (InputType_Value) Descriptor() protoreflect.EnumDescriptor {
	return file_go_api_command_command_proto_enumTypes[0].Descriptor()
}

func (InputType_Value) Type() protoreflect.EnumType {
	return &file_go_api_command_command_proto_enumTypes[0]
}

func (x InputType_Value) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use InputType_Value.Descriptor instead.
func (InputType_Value) EnumDescriptor() ([]byte, []int) {
	return file_go_api_command_command_proto_rawDescGZIP(), []int{2, 0}
}

type CommandResultStatus_Value int32

const (
	CommandResultStatus_UNKNOWN       CommandResultStatus_Value = 0
	CommandResultStatus_SUCCESS       CommandResultStatus_Value = 1
	CommandResultStatus_CACHE_HIT     CommandResultStatus_Value = 2
	CommandResultStatus_NON_ZERO_EXIT CommandResultStatus_Value = 3
	CommandResultStatus_TIMEOUT       CommandResultStatus_Value = 4
	CommandResultStatus_INTERRUPTED   CommandResultStatus_Value = 5
	CommandResultStatus_REMOTE_ERROR  CommandResultStatus_Value = 6
	CommandResultStatus_LOCAL_ERROR   CommandResultStatus_Value = 7
)

// Enum value maps for CommandResultStatus_Value.
var (
	CommandResultStatus_Value_name = map[int32]string{
		0: "UNKNOWN",
		1: "SUCCESS",
		2: "CACHE_HIT",
		3: "NON_ZERO_EXIT",
		4: "TIMEOUT",
		5: "INTERRUPTED",
		6: "REMOTE_ERROR",
		7: "LOCAL_ERROR",
	}
	CommandResultStatus_Value_value = map[string]int32{
		"UNKNOWN":       0,
		"SUCCESS":       1,
		"CACHE_HIT":     2,
		"NON_ZERO_EXIT": 3,
		"TIMEOUT":       4,
		"INTERRUPTED":   5,
		"REMOTE_ERROR":  6,
		"LOCAL_ERROR":   7,
	}
)

func (x CommandResultStatus_Value) Enum() *CommandResultStatus_Value {
	p := new(CommandResultStatus_Value)
	*p = x
	return p
}

func (x CommandResultStatus_Value) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (CommandResultStatus_Value) Descriptor() protoreflect.EnumDescriptor {
	return file_go_api_command_command_proto_enumTypes[1].Descriptor()
}

func (CommandResultStatus_Value) Type() protoreflect.EnumType {
	return &file_go_api_command_command_proto_enumTypes[1]
}

func (x CommandResultStatus_Value) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use CommandResultStatus_Value.Descriptor instead.
func (CommandResultStatus_Value) EnumDescriptor() ([]byte, []int) {
	return file_go_api_command_command_proto_rawDescGZIP(), []int{7, 0}
}

type Command struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Identifiers      *Identifiers      `protobuf:"bytes,1,opt,name=identifiers,proto3" json:"identifiers,omitempty"`
	ExecRoot         string            `protobuf:"bytes,2,opt,name=exec_root,json=execRoot,proto3" json:"exec_root,omitempty"`
	Input            *InputSpec        `protobuf:"bytes,3,opt,name=input,proto3" json:"input,omitempty"`
	Output           *OutputSpec       `protobuf:"bytes,4,opt,name=output,proto3" json:"output,omitempty"`
	Args             []string          `protobuf:"bytes,5,rep,name=args,proto3" json:"args,omitempty"`
	ExecutionTimeout int32             `protobuf:"varint,6,opt,name=execution_timeout,json=executionTimeout,proto3" json:"execution_timeout,omitempty"`
	WorkingDirectory string            `protobuf:"bytes,7,opt,name=working_directory,json=workingDirectory,proto3" json:"working_directory,omitempty"`
	Platform         map[string]string `protobuf:"bytes,8,rep,name=platform,proto3" json:"platform,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
}

func (x *Command) Reset() {
	*x = Command{}
	if protoimpl.UnsafeEnabled {
		mi := &file_go_api_command_command_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Command) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Command) ProtoMessage() {}

func (x *Command) ProtoReflect() protoreflect.Message {
	mi := &file_go_api_command_command_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Command.ProtoReflect.Descriptor instead.
func (*Command) Descriptor() ([]byte, []int) {
	return file_go_api_command_command_proto_rawDescGZIP(), []int{0}
}

func (x *Command) GetIdentifiers() *Identifiers {
	if x != nil {
		return x.Identifiers
	}
	return nil
}

func (x *Command) GetExecRoot() string {
	if x != nil {
		return x.ExecRoot
	}
	return ""
}

func (x *Command) GetInput() *InputSpec {
	if x != nil {
		return x.Input
	}
	return nil
}

func (x *Command) GetOutput() *OutputSpec {
	if x != nil {
		return x.Output
	}
	return nil
}

func (x *Command) GetArgs() []string {
	if x != nil {
		return x.Args
	}
	return nil
}

func (x *Command) GetExecutionTimeout() int32 {
	if x != nil {
		return x.ExecutionTimeout
	}
	return 0
}

func (x *Command) GetWorkingDirectory() string {
	if x != nil {
		return x.WorkingDirectory
	}
	return ""
}

func (x *Command) GetPlatform() map[string]string {
	if x != nil {
		return x.Platform
	}
	return nil
}

type Identifiers struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	CommandId               string `protobuf:"bytes,1,opt,name=command_id,json=commandId,proto3" json:"command_id,omitempty"`
	InvocationId            string `protobuf:"bytes,2,opt,name=invocation_id,json=invocationId,proto3" json:"invocation_id,omitempty"`
	CorrelatedInvocationsId string `protobuf:"bytes,3,opt,name=correlated_invocations_id,json=correlatedInvocationsId,proto3" json:"correlated_invocations_id,omitempty"`
	ToolName                string `protobuf:"bytes,4,opt,name=tool_name,json=toolName,proto3" json:"tool_name,omitempty"`
	ToolVersion             string `protobuf:"bytes,5,opt,name=tool_version,json=toolVersion,proto3" json:"tool_version,omitempty"`
	ExecutionId             string `protobuf:"bytes,6,opt,name=execution_id,json=executionId,proto3" json:"execution_id,omitempty"`
}

func (x *Identifiers) Reset() {
	*x = Identifiers{}
	if protoimpl.UnsafeEnabled {
		mi := &file_go_api_command_command_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Identifiers) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Identifiers) ProtoMessage() {}

func (x *Identifiers) ProtoReflect() protoreflect.Message {
	mi := &file_go_api_command_command_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Identifiers.ProtoReflect.Descriptor instead.
func (*Identifiers) Descriptor() ([]byte, []int) {
	return file_go_api_command_command_proto_rawDescGZIP(), []int{1}
}

func (x *Identifiers) GetCommandId() string {
	if x != nil {
		return x.CommandId
	}
	return ""
}

func (x *Identifiers) GetInvocationId() string {
	if x != nil {
		return x.InvocationId
	}
	return ""
}

func (x *Identifiers) GetCorrelatedInvocationsId() string {
	if x != nil {
		return x.CorrelatedInvocationsId
	}
	return ""
}

func (x *Identifiers) GetToolName() string {
	if x != nil {
		return x.ToolName
	}
	return ""
}

func (x *Identifiers) GetToolVersion() string {
	if x != nil {
		return x.ToolVersion
	}
	return ""
}

func (x *Identifiers) GetExecutionId() string {
	if x != nil {
		return x.ExecutionId
	}
	return ""
}

type InputType struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *InputType) Reset() {
	*x = InputType{}
	if protoimpl.UnsafeEnabled {
		mi := &file_go_api_command_command_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *InputType) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*InputType) ProtoMessage() {}

func (x *InputType) ProtoReflect() protoreflect.Message {
	mi := &file_go_api_command_command_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use InputType.ProtoReflect.Descriptor instead.
func (*InputType) Descriptor() ([]byte, []int) {
	return file_go_api_command_command_proto_rawDescGZIP(), []int{2}
}

type ExcludeInput struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Regex string          `protobuf:"bytes,1,opt,name=regex,proto3" json:"regex,omitempty"`
	Type  InputType_Value `protobuf:"varint,2,opt,name=type,proto3,enum=cmd.InputType_Value" json:"type,omitempty"`
}

func (x *ExcludeInput) Reset() {
	*x = ExcludeInput{}
	if protoimpl.UnsafeEnabled {
		mi := &file_go_api_command_command_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ExcludeInput) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ExcludeInput) ProtoMessage() {}

func (x *ExcludeInput) ProtoReflect() protoreflect.Message {
	mi := &file_go_api_command_command_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ExcludeInput.ProtoReflect.Descriptor instead.
func (*ExcludeInput) Descriptor() ([]byte, []int) {
	return file_go_api_command_command_proto_rawDescGZIP(), []int{3}
}

func (x *ExcludeInput) GetRegex() string {
	if x != nil {
		return x.Regex
	}
	return ""
}

func (x *ExcludeInput) GetType() InputType_Value {
	if x != nil {
		return x.Type
	}
	return InputType_UNSPECIFIED
}

type VirtualInput struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Path             string `protobuf:"bytes,1,opt,name=path,proto3" json:"path,omitempty"`
	Contents         []byte `protobuf:"bytes,2,opt,name=contents,proto3" json:"contents,omitempty"`
	IsExecutable     bool   `protobuf:"varint,3,opt,name=is_executable,json=isExecutable,proto3" json:"is_executable,omitempty"`
	IsEmptyDirectory bool   `protobuf:"varint,4,opt,name=is_empty_directory,json=isEmptyDirectory,proto3" json:"is_empty_directory,omitempty"`
}

func (x *VirtualInput) Reset() {
	*x = VirtualInput{}
	if protoimpl.UnsafeEnabled {
		mi := &file_go_api_command_command_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *VirtualInput) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*VirtualInput) ProtoMessage() {}

func (x *VirtualInput) ProtoReflect() protoreflect.Message {
	mi := &file_go_api_command_command_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use VirtualInput.ProtoReflect.Descriptor instead.
func (*VirtualInput) Descriptor() ([]byte, []int) {
	return file_go_api_command_command_proto_rawDescGZIP(), []int{4}
}

func (x *VirtualInput) GetPath() string {
	if x != nil {
		return x.Path
	}
	return ""
}

func (x *VirtualInput) GetContents() []byte {
	if x != nil {
		return x.Contents
	}
	return nil
}

func (x *VirtualInput) GetIsExecutable() bool {
	if x != nil {
		return x.IsExecutable
	}
	return false
}

func (x *VirtualInput) GetIsEmptyDirectory() bool {
	if x != nil {
		return x.IsEmptyDirectory
	}
	return false
}

type InputSpec struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Inputs               []string          `protobuf:"bytes,2,rep,name=inputs,proto3" json:"inputs,omitempty"`
	VirtualInputs        []*VirtualInput   `protobuf:"bytes,5,rep,name=virtual_inputs,json=virtualInputs,proto3" json:"virtual_inputs,omitempty"`
	ExcludeInputs        []*ExcludeInput   `protobuf:"bytes,3,rep,name=exclude_inputs,json=excludeInputs,proto3" json:"exclude_inputs,omitempty"`
	EnvironmentVariables map[string]string `protobuf:"bytes,4,rep,name=environment_variables,json=environmentVariables,proto3" json:"environment_variables,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
}

func (x *InputSpec) Reset() {
	*x = InputSpec{}
	if protoimpl.UnsafeEnabled {
		mi := &file_go_api_command_command_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *InputSpec) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*InputSpec) ProtoMessage() {}

func (x *InputSpec) ProtoReflect() protoreflect.Message {
	mi := &file_go_api_command_command_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use InputSpec.ProtoReflect.Descriptor instead.
func (*InputSpec) Descriptor() ([]byte, []int) {
	return file_go_api_command_command_proto_rawDescGZIP(), []int{5}
}

func (x *InputSpec) GetInputs() []string {
	if x != nil {
		return x.Inputs
	}
	return nil
}

func (x *InputSpec) GetVirtualInputs() []*VirtualInput {
	if x != nil {
		return x.VirtualInputs
	}
	return nil
}

func (x *InputSpec) GetExcludeInputs() []*ExcludeInput {
	if x != nil {
		return x.ExcludeInputs
	}
	return nil
}

func (x *InputSpec) GetEnvironmentVariables() map[string]string {
	if x != nil {
		return x.EnvironmentVariables
	}
	return nil
}

type OutputSpec struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	OutputFiles       []string `protobuf:"bytes,1,rep,name=output_files,json=outputFiles,proto3" json:"output_files,omitempty"`
	OutputDirectories []string `protobuf:"bytes,2,rep,name=output_directories,json=outputDirectories,proto3" json:"output_directories,omitempty"`
}

func (x *OutputSpec) Reset() {
	*x = OutputSpec{}
	if protoimpl.UnsafeEnabled {
		mi := &file_go_api_command_command_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *OutputSpec) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*OutputSpec) ProtoMessage() {}

func (x *OutputSpec) ProtoReflect() protoreflect.Message {
	mi := &file_go_api_command_command_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use OutputSpec.ProtoReflect.Descriptor instead.
func (*OutputSpec) Descriptor() ([]byte, []int) {
	return file_go_api_command_command_proto_rawDescGZIP(), []int{6}
}

func (x *OutputSpec) GetOutputFiles() []string {
	if x != nil {
		return x.OutputFiles
	}
	return nil
}

func (x *OutputSpec) GetOutputDirectories() []string {
	if x != nil {
		return x.OutputDirectories
	}
	return nil
}

type CommandResultStatus struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *CommandResultStatus) Reset() {
	*x = CommandResultStatus{}
	if protoimpl.UnsafeEnabled {
		mi := &file_go_api_command_command_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *CommandResultStatus) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CommandResultStatus) ProtoMessage() {}

func (x *CommandResultStatus) ProtoReflect() protoreflect.Message {
	mi := &file_go_api_command_command_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CommandResultStatus.ProtoReflect.Descriptor instead.
func (*CommandResultStatus) Descriptor() ([]byte, []int) {
	return file_go_api_command_command_proto_rawDescGZIP(), []int{7}
}

type CommandResult struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Status   CommandResultStatus_Value `protobuf:"varint,1,opt,name=status,proto3,enum=cmd.CommandResultStatus_Value" json:"status,omitempty"`
	ExitCode int32                     `protobuf:"varint,2,opt,name=exit_code,json=exitCode,proto3" json:"exit_code,omitempty"`
	Msg      string                    `protobuf:"bytes,3,opt,name=msg,proto3" json:"msg,omitempty"`
}

func (x *CommandResult) Reset() {
	*x = CommandResult{}
	if protoimpl.UnsafeEnabled {
		mi := &file_go_api_command_command_proto_msgTypes[8]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *CommandResult) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CommandResult) ProtoMessage() {}

func (x *CommandResult) ProtoReflect() protoreflect.Message {
	mi := &file_go_api_command_command_proto_msgTypes[8]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CommandResult.ProtoReflect.Descriptor instead.
func (*CommandResult) Descriptor() ([]byte, []int) {
	return file_go_api_command_command_proto_rawDescGZIP(), []int{8}
}

func (x *CommandResult) GetStatus() CommandResultStatus_Value {
	if x != nil {
		return x.Status
	}
	return CommandResultStatus_UNKNOWN
}

func (x *CommandResult) GetExitCode() int32 {
	if x != nil {
		return x.ExitCode
	}
	return 0
}

func (x *CommandResult) GetMsg() string {
	if x != nil {
		return x.Msg
	}
	return ""
}

type TimeInterval struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	From *timestamp.Timestamp `protobuf:"bytes,1,opt,name=from,proto3" json:"from,omitempty"`
	To   *timestamp.Timestamp `protobuf:"bytes,2,opt,name=to,proto3" json:"to,omitempty"`
}

func (x *TimeInterval) Reset() {
	*x = TimeInterval{}
	if protoimpl.UnsafeEnabled {
		mi := &file_go_api_command_command_proto_msgTypes[9]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TimeInterval) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TimeInterval) ProtoMessage() {}

func (x *TimeInterval) ProtoReflect() protoreflect.Message {
	mi := &file_go_api_command_command_proto_msgTypes[9]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TimeInterval.ProtoReflect.Descriptor instead.
func (*TimeInterval) Descriptor() ([]byte, []int) {
	return file_go_api_command_command_proto_rawDescGZIP(), []int{9}
}

func (x *TimeInterval) GetFrom() *timestamp.Timestamp {
	if x != nil {
		return x.From
	}
	return nil
}

func (x *TimeInterval) GetTo() *timestamp.Timestamp {
	if x != nil {
		return x.To
	}
	return nil
}

var File_go_api_command_command_proto protoreflect.FileDescriptor

var file_go_api_command_command_proto_rawDesc = []byte{
	0x0a, 0x1c, 0x67, 0x6f, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x63, 0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64,
	0x2f, 0x63, 0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x03,
	0x63, 0x6d, 0x64, 0x1a, 0x1f, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x62, 0x75, 0x66, 0x2f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x22, 0x8c, 0x03, 0x0a, 0x07, 0x43, 0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64,
	0x12, 0x32, 0x0a, 0x0b, 0x69, 0x64, 0x65, 0x6e, 0x74, 0x69, 0x66, 0x69, 0x65, 0x72, 0x73, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x10, 0x2e, 0x63, 0x6d, 0x64, 0x2e, 0x49, 0x64, 0x65, 0x6e,
	0x74, 0x69, 0x66, 0x69, 0x65, 0x72, 0x73, 0x52, 0x0b, 0x69, 0x64, 0x65, 0x6e, 0x74, 0x69, 0x66,
	0x69, 0x65, 0x72, 0x73, 0x12, 0x1b, 0x0a, 0x09, 0x65, 0x78, 0x65, 0x63, 0x5f, 0x72, 0x6f, 0x6f,
	0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x65, 0x78, 0x65, 0x63, 0x52, 0x6f, 0x6f,
	0x74, 0x12, 0x24, 0x0a, 0x05, 0x69, 0x6e, 0x70, 0x75, 0x74, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x0e, 0x2e, 0x63, 0x6d, 0x64, 0x2e, 0x49, 0x6e, 0x70, 0x75, 0x74, 0x53, 0x70, 0x65, 0x63,
	0x52, 0x05, 0x69, 0x6e, 0x70, 0x75, 0x74, 0x12, 0x27, 0x0a, 0x06, 0x6f, 0x75, 0x74, 0x70, 0x75,
	0x74, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0f, 0x2e, 0x63, 0x6d, 0x64, 0x2e, 0x4f, 0x75,
	0x74, 0x70, 0x75, 0x74, 0x53, 0x70, 0x65, 0x63, 0x52, 0x06, 0x6f, 0x75, 0x74, 0x70, 0x75, 0x74,
	0x12, 0x12, 0x0a, 0x04, 0x61, 0x72, 0x67, 0x73, 0x18, 0x05, 0x20, 0x03, 0x28, 0x09, 0x52, 0x04,
	0x61, 0x72, 0x67, 0x73, 0x12, 0x2b, 0x0a, 0x11, 0x65, 0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f,
	0x6e, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x6f, 0x75, 0x74, 0x18, 0x06, 0x20, 0x01, 0x28, 0x05, 0x52,
	0x10, 0x65, 0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x54, 0x69, 0x6d, 0x65, 0x6f, 0x75,
	0x74, 0x12, 0x2b, 0x0a, 0x11, 0x77, 0x6f, 0x72, 0x6b, 0x69, 0x6e, 0x67, 0x5f, 0x64, 0x69, 0x72,
	0x65, 0x63, 0x74, 0x6f, 0x72, 0x79, 0x18, 0x07, 0x20, 0x01, 0x28, 0x09, 0x52, 0x10, 0x77, 0x6f,
	0x72, 0x6b, 0x69, 0x6e, 0x67, 0x44, 0x69, 0x72, 0x65, 0x63, 0x74, 0x6f, 0x72, 0x79, 0x12, 0x36,
	0x0a, 0x08, 0x70, 0x6c, 0x61, 0x74, 0x66, 0x6f, 0x72, 0x6d, 0x18, 0x08, 0x20, 0x03, 0x28, 0x0b,
	0x32, 0x1a, 0x2e, 0x63, 0x6d, 0x64, 0x2e, 0x43, 0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x2e, 0x50,
	0x6c, 0x61, 0x74, 0x66, 0x6f, 0x72, 0x6d, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x08, 0x70, 0x6c,
	0x61, 0x74, 0x66, 0x6f, 0x72, 0x6d, 0x1a, 0x3b, 0x0a, 0x0d, 0x50, 0x6c, 0x61, 0x74, 0x66, 0x6f,
	0x72, 0x6d, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c,
	0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x3a,
	0x02, 0x38, 0x01, 0x22, 0xf0, 0x01, 0x0a, 0x0b, 0x49, 0x64, 0x65, 0x6e, 0x74, 0x69, 0x66, 0x69,
	0x65, 0x72, 0x73, 0x12, 0x1d, 0x0a, 0x0a, 0x63, 0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x5f, 0x69,
	0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x63, 0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64,
	0x49, 0x64, 0x12, 0x23, 0x0a, 0x0d, 0x69, 0x6e, 0x76, 0x6f, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e,
	0x5f, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0c, 0x69, 0x6e, 0x76, 0x6f, 0x63,
	0x61, 0x74, 0x69, 0x6f, 0x6e, 0x49, 0x64, 0x12, 0x3a, 0x0a, 0x19, 0x63, 0x6f, 0x72, 0x72, 0x65,
	0x6c, 0x61, 0x74, 0x65, 0x64, 0x5f, 0x69, 0x6e, 0x76, 0x6f, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e,
	0x73, 0x5f, 0x69, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x17, 0x63, 0x6f, 0x72, 0x72,
	0x65, 0x6c, 0x61, 0x74, 0x65, 0x64, 0x49, 0x6e, 0x76, 0x6f, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e,
	0x73, 0x49, 0x64, 0x12, 0x1b, 0x0a, 0x09, 0x74, 0x6f, 0x6f, 0x6c, 0x5f, 0x6e, 0x61, 0x6d, 0x65,
	0x18, 0x04, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x74, 0x6f, 0x6f, 0x6c, 0x4e, 0x61, 0x6d, 0x65,
	0x12, 0x21, 0x0a, 0x0c, 0x74, 0x6f, 0x6f, 0x6c, 0x5f, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e,
	0x18, 0x05, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x74, 0x6f, 0x6f, 0x6c, 0x56, 0x65, 0x72, 0x73,
	0x69, 0x6f, 0x6e, 0x12, 0x21, 0x0a, 0x0c, 0x65, 0x78, 0x65, 0x63, 0x75, 0x74, 0x69, 0x6f, 0x6e,
	0x5f, 0x69, 0x64, 0x18, 0x06, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0b, 0x65, 0x78, 0x65, 0x63, 0x75,
	0x74, 0x69, 0x6f, 0x6e, 0x49, 0x64, 0x22, 0x3e, 0x0a, 0x09, 0x49, 0x6e, 0x70, 0x75, 0x74, 0x54,
	0x79, 0x70, 0x65, 0x22, 0x31, 0x0a, 0x05, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x12, 0x0f, 0x0a, 0x0b,
	0x55, 0x4e, 0x53, 0x50, 0x45, 0x43, 0x49, 0x46, 0x49, 0x45, 0x44, 0x10, 0x00, 0x12, 0x0d, 0x0a,
	0x09, 0x44, 0x49, 0x52, 0x45, 0x43, 0x54, 0x4f, 0x52, 0x59, 0x10, 0x01, 0x12, 0x08, 0x0a, 0x04,
	0x46, 0x49, 0x4c, 0x45, 0x10, 0x02, 0x22, 0x4e, 0x0a, 0x0c, 0x45, 0x78, 0x63, 0x6c, 0x75, 0x64,
	0x65, 0x49, 0x6e, 0x70, 0x75, 0x74, 0x12, 0x14, 0x0a, 0x05, 0x72, 0x65, 0x67, 0x65, 0x78, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x72, 0x65, 0x67, 0x65, 0x78, 0x12, 0x28, 0x0a, 0x04,
	0x74, 0x79, 0x70, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x14, 0x2e, 0x63, 0x6d, 0x64,
	0x2e, 0x49, 0x6e, 0x70, 0x75, 0x74, 0x54, 0x79, 0x70, 0x65, 0x2e, 0x56, 0x61, 0x6c, 0x75, 0x65,
	0x52, 0x04, 0x74, 0x79, 0x70, 0x65, 0x22, 0x91, 0x01, 0x0a, 0x0c, 0x56, 0x69, 0x72, 0x74, 0x75,
	0x61, 0x6c, 0x49, 0x6e, 0x70, 0x75, 0x74, 0x12, 0x12, 0x0a, 0x04, 0x70, 0x61, 0x74, 0x68, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x70, 0x61, 0x74, 0x68, 0x12, 0x1a, 0x0a, 0x08, 0x63,
	0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74, 0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x08, 0x63,
	0x6f, 0x6e, 0x74, 0x65, 0x6e, 0x74, 0x73, 0x12, 0x23, 0x0a, 0x0d, 0x69, 0x73, 0x5f, 0x65, 0x78,
	0x65, 0x63, 0x75, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x08, 0x52, 0x0c,
	0x69, 0x73, 0x45, 0x78, 0x65, 0x63, 0x75, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x12, 0x2c, 0x0a, 0x12,
	0x69, 0x73, 0x5f, 0x65, 0x6d, 0x70, 0x74, 0x79, 0x5f, 0x64, 0x69, 0x72, 0x65, 0x63, 0x74, 0x6f,
	0x72, 0x79, 0x18, 0x04, 0x20, 0x01, 0x28, 0x08, 0x52, 0x10, 0x69, 0x73, 0x45, 0x6d, 0x70, 0x74,
	0x79, 0x44, 0x69, 0x72, 0x65, 0x63, 0x74, 0x6f, 0x72, 0x79, 0x22, 0xbf, 0x02, 0x0a, 0x09, 0x49,
	0x6e, 0x70, 0x75, 0x74, 0x53, 0x70, 0x65, 0x63, 0x12, 0x16, 0x0a, 0x06, 0x69, 0x6e, 0x70, 0x75,
	0x74, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x09, 0x52, 0x06, 0x69, 0x6e, 0x70, 0x75, 0x74, 0x73,
	0x12, 0x38, 0x0a, 0x0e, 0x76, 0x69, 0x72, 0x74, 0x75, 0x61, 0x6c, 0x5f, 0x69, 0x6e, 0x70, 0x75,
	0x74, 0x73, 0x18, 0x05, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x11, 0x2e, 0x63, 0x6d, 0x64, 0x2e, 0x56,
	0x69, 0x72, 0x74, 0x75, 0x61, 0x6c, 0x49, 0x6e, 0x70, 0x75, 0x74, 0x52, 0x0d, 0x76, 0x69, 0x72,
	0x74, 0x75, 0x61, 0x6c, 0x49, 0x6e, 0x70, 0x75, 0x74, 0x73, 0x12, 0x38, 0x0a, 0x0e, 0x65, 0x78,
	0x63, 0x6c, 0x75, 0x64, 0x65, 0x5f, 0x69, 0x6e, 0x70, 0x75, 0x74, 0x73, 0x18, 0x03, 0x20, 0x03,
	0x28, 0x0b, 0x32, 0x11, 0x2e, 0x63, 0x6d, 0x64, 0x2e, 0x45, 0x78, 0x63, 0x6c, 0x75, 0x64, 0x65,
	0x49, 0x6e, 0x70, 0x75, 0x74, 0x52, 0x0d, 0x65, 0x78, 0x63, 0x6c, 0x75, 0x64, 0x65, 0x49, 0x6e,
	0x70, 0x75, 0x74, 0x73, 0x12, 0x5d, 0x0a, 0x15, 0x65, 0x6e, 0x76, 0x69, 0x72, 0x6f, 0x6e, 0x6d,
	0x65, 0x6e, 0x74, 0x5f, 0x76, 0x61, 0x72, 0x69, 0x61, 0x62, 0x6c, 0x65, 0x73, 0x18, 0x04, 0x20,
	0x03, 0x28, 0x0b, 0x32, 0x28, 0x2e, 0x63, 0x6d, 0x64, 0x2e, 0x49, 0x6e, 0x70, 0x75, 0x74, 0x53,
	0x70, 0x65, 0x63, 0x2e, 0x45, 0x6e, 0x76, 0x69, 0x72, 0x6f, 0x6e, 0x6d, 0x65, 0x6e, 0x74, 0x56,
	0x61, 0x72, 0x69, 0x61, 0x62, 0x6c, 0x65, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x14, 0x65,
	0x6e, 0x76, 0x69, 0x72, 0x6f, 0x6e, 0x6d, 0x65, 0x6e, 0x74, 0x56, 0x61, 0x72, 0x69, 0x61, 0x62,
	0x6c, 0x65, 0x73, 0x1a, 0x47, 0x0a, 0x19, 0x45, 0x6e, 0x76, 0x69, 0x72, 0x6f, 0x6e, 0x6d, 0x65,
	0x6e, 0x74, 0x56, 0x61, 0x72, 0x69, 0x61, 0x62, 0x6c, 0x65, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79,
	0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b,
	0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01, 0x22, 0x5e, 0x0a, 0x0a,
	0x4f, 0x75, 0x74, 0x70, 0x75, 0x74, 0x53, 0x70, 0x65, 0x63, 0x12, 0x21, 0x0a, 0x0c, 0x6f, 0x75,
	0x74, 0x70, 0x75, 0x74, 0x5f, 0x66, 0x69, 0x6c, 0x65, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x09,
	0x52, 0x0b, 0x6f, 0x75, 0x74, 0x70, 0x75, 0x74, 0x46, 0x69, 0x6c, 0x65, 0x73, 0x12, 0x2d, 0x0a,
	0x12, 0x6f, 0x75, 0x74, 0x70, 0x75, 0x74, 0x5f, 0x64, 0x69, 0x72, 0x65, 0x63, 0x74, 0x6f, 0x72,
	0x69, 0x65, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x09, 0x52, 0x11, 0x6f, 0x75, 0x74, 0x70, 0x75,
	0x74, 0x44, 0x69, 0x72, 0x65, 0x63, 0x74, 0x6f, 0x72, 0x69, 0x65, 0x73, 0x22, 0x9c, 0x01, 0x0a,
	0x13, 0x43, 0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x52, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x53, 0x74,
	0x61, 0x74, 0x75, 0x73, 0x22, 0x84, 0x01, 0x0a, 0x05, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x12, 0x0b,
	0x0a, 0x07, 0x55, 0x4e, 0x4b, 0x4e, 0x4f, 0x57, 0x4e, 0x10, 0x00, 0x12, 0x0b, 0x0a, 0x07, 0x53,
	0x55, 0x43, 0x43, 0x45, 0x53, 0x53, 0x10, 0x01, 0x12, 0x0d, 0x0a, 0x09, 0x43, 0x41, 0x43, 0x48,
	0x45, 0x5f, 0x48, 0x49, 0x54, 0x10, 0x02, 0x12, 0x11, 0x0a, 0x0d, 0x4e, 0x4f, 0x4e, 0x5f, 0x5a,
	0x45, 0x52, 0x4f, 0x5f, 0x45, 0x58, 0x49, 0x54, 0x10, 0x03, 0x12, 0x0b, 0x0a, 0x07, 0x54, 0x49,
	0x4d, 0x45, 0x4f, 0x55, 0x54, 0x10, 0x04, 0x12, 0x0f, 0x0a, 0x0b, 0x49, 0x4e, 0x54, 0x45, 0x52,
	0x52, 0x55, 0x50, 0x54, 0x45, 0x44, 0x10, 0x05, 0x12, 0x10, 0x0a, 0x0c, 0x52, 0x45, 0x4d, 0x4f,
	0x54, 0x45, 0x5f, 0x45, 0x52, 0x52, 0x4f, 0x52, 0x10, 0x06, 0x12, 0x0f, 0x0a, 0x0b, 0x4c, 0x4f,
	0x43, 0x41, 0x4c, 0x5f, 0x45, 0x52, 0x52, 0x4f, 0x52, 0x10, 0x07, 0x22, 0x76, 0x0a, 0x0d, 0x43,
	0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x52, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x12, 0x36, 0x0a, 0x06,
	0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x1e, 0x2e, 0x63,
	0x6d, 0x64, 0x2e, 0x43, 0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x52, 0x65, 0x73, 0x75, 0x6c, 0x74,
	0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x2e, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x52, 0x06, 0x73, 0x74,
	0x61, 0x74, 0x75, 0x73, 0x12, 0x1b, 0x0a, 0x09, 0x65, 0x78, 0x69, 0x74, 0x5f, 0x63, 0x6f, 0x64,
	0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x05, 0x52, 0x08, 0x65, 0x78, 0x69, 0x74, 0x43, 0x6f, 0x64,
	0x65, 0x12, 0x10, 0x0a, 0x03, 0x6d, 0x73, 0x67, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03,
	0x6d, 0x73, 0x67, 0x22, 0x6a, 0x0a, 0x0c, 0x54, 0x69, 0x6d, 0x65, 0x49, 0x6e, 0x74, 0x65, 0x72,
	0x76, 0x61, 0x6c, 0x12, 0x2e, 0x0a, 0x04, 0x66, 0x72, 0x6f, 0x6d, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x04, 0x66,
	0x72, 0x6f, 0x6d, 0x12, 0x2a, 0x0a, 0x02, 0x74, 0x6f, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75,
	0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x02, 0x74, 0x6f, 0x62,
	0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_go_api_command_command_proto_rawDescOnce sync.Once
	file_go_api_command_command_proto_rawDescData = file_go_api_command_command_proto_rawDesc
)

func file_go_api_command_command_proto_rawDescGZIP() []byte {
	file_go_api_command_command_proto_rawDescOnce.Do(func() {
		file_go_api_command_command_proto_rawDescData = protoimpl.X.CompressGZIP(file_go_api_command_command_proto_rawDescData)
	})
	return file_go_api_command_command_proto_rawDescData
}

var file_go_api_command_command_proto_enumTypes = make([]protoimpl.EnumInfo, 2)
var file_go_api_command_command_proto_msgTypes = make([]protoimpl.MessageInfo, 12)
var file_go_api_command_command_proto_goTypes = []interface{}{
	(InputType_Value)(0),           // 0: cmd.InputType.Value
	(CommandResultStatus_Value)(0), // 1: cmd.CommandResultStatus.Value
	(*Command)(nil),                // 2: cmd.Command
	(*Identifiers)(nil),            // 3: cmd.Identifiers
	(*InputType)(nil),              // 4: cmd.InputType
	(*ExcludeInput)(nil),           // 5: cmd.ExcludeInput
	(*VirtualInput)(nil),           // 6: cmd.VirtualInput
	(*InputSpec)(nil),              // 7: cmd.InputSpec
	(*OutputSpec)(nil),             // 8: cmd.OutputSpec
	(*CommandResultStatus)(nil),    // 9: cmd.CommandResultStatus
	(*CommandResult)(nil),          // 10: cmd.CommandResult
	(*TimeInterval)(nil),           // 11: cmd.TimeInterval
	nil,                            // 12: cmd.Command.PlatformEntry
	nil,                            // 13: cmd.InputSpec.EnvironmentVariablesEntry
	(*timestamp.Timestamp)(nil),    // 14: google.protobuf.Timestamp
}
var file_go_api_command_command_proto_depIdxs = []int32{
	3,  // 0: cmd.Command.identifiers:type_name -> cmd.Identifiers
	7,  // 1: cmd.Command.input:type_name -> cmd.InputSpec
	8,  // 2: cmd.Command.output:type_name -> cmd.OutputSpec
	12, // 3: cmd.Command.platform:type_name -> cmd.Command.PlatformEntry
	0,  // 4: cmd.ExcludeInput.type:type_name -> cmd.InputType.Value
	6,  // 5: cmd.InputSpec.virtual_inputs:type_name -> cmd.VirtualInput
	5,  // 6: cmd.InputSpec.exclude_inputs:type_name -> cmd.ExcludeInput
	13, // 7: cmd.InputSpec.environment_variables:type_name -> cmd.InputSpec.EnvironmentVariablesEntry
	1,  // 8: cmd.CommandResult.status:type_name -> cmd.CommandResultStatus.Value
	14, // 9: cmd.TimeInterval.from:type_name -> google.protobuf.Timestamp
	14, // 10: cmd.TimeInterval.to:type_name -> google.protobuf.Timestamp
	11, // [11:11] is the sub-list for method output_type
	11, // [11:11] is the sub-list for method input_type
	11, // [11:11] is the sub-list for extension type_name
	11, // [11:11] is the sub-list for extension extendee
	0,  // [0:11] is the sub-list for field type_name
}

func init() { file_go_api_command_command_proto_init() }
func file_go_api_command_command_proto_init() {
	if File_go_api_command_command_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_go_api_command_command_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Command); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_go_api_command_command_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Identifiers); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_go_api_command_command_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*InputType); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_go_api_command_command_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ExcludeInput); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_go_api_command_command_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*VirtualInput); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_go_api_command_command_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*InputSpec); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_go_api_command_command_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*OutputSpec); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_go_api_command_command_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*CommandResultStatus); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_go_api_command_command_proto_msgTypes[8].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*CommandResult); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_go_api_command_command_proto_msgTypes[9].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TimeInterval); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_go_api_command_command_proto_rawDesc,
			NumEnums:      2,
			NumMessages:   12,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_go_api_command_command_proto_goTypes,
		DependencyIndexes: file_go_api_command_command_proto_depIdxs,
		EnumInfos:         file_go_api_command_command_proto_enumTypes,
		MessageInfos:      file_go_api_command_command_proto_msgTypes,
	}.Build()
	File_go_api_command_command_proto = out.File
	file_go_api_command_command_proto_rawDesc = nil
	file_go_api_command_command_proto_goTypes = nil
	file_go_api_command_command_proto_depIdxs = nil
}
