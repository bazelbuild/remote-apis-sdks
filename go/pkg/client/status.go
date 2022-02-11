package client

import (
	"fmt"
	"strings"

	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"

	_ "google.golang.org/genproto/googleapis/rpc/errdetails" // the proto needs to be compiled in for unmarshalling of status details.
)

// StatusError is the same as status.Error except it includes the error details in the error message.
type StatusError struct {
	st      *status.Status
	details string
}

// StatusDetailedError creates a StatusError from Status, which is the same as st.Err() except it includes the error details in the error message.
func StatusDetailedError(st *status.Status) *StatusError {
	var details []string
	for _, d := range st.Details() {
		s := fmt.Sprintf("%+v", d)
		if pb, ok := d.(proto.Message); ok {
			s = prototext.Format(pb)
		}
		details = append(details, s)
	}
	return &StatusError{st, strings.Join(details, "; ")}
}

func (e *StatusError) Error() string {
	msg := fmt.Sprintf("rpc error: code = %s desc = %s", e.st.Code(), e.st.Message())
	if e.details != "" {
		msg += " details = " + e.details
	}
	return msg
}

// GRPCStatus returns the Status represented by e.
func (e *StatusError) GRPCStatus() *status.Status {
	return e.st
}

// Is implements error.Is functionality.
// A StatusError is equivalent if the code and message are identical.
func (e *StatusError) Is(target error) bool {
	if tse, ok := target.(*StatusError); ok {
		return proto.Equal(e.st.Proto(), tse.st.Proto())
	}
	if tst, ok := status.FromError(target); ok {
		return proto.Equal(e.st.Proto(), tst.Proto())
	}
	return false
}
