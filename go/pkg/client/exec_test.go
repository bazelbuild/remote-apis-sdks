package client_test

import (
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/client"
	"github.com/golang/protobuf/ptypes"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	oppb "google.golang.org/genproto/googleapis/longrunning"
	spb "google.golang.org/genproto/googleapis/rpc/status"
)

func TestOperationStatus(t *testing.T) {
	respv2, err := ptypes.MarshalAny(&repb.ExecuteResponse{Status: &spb.Status{Code: 2}})
	if err != nil {
		t.Fatalf("Unable to marshal V2 proto: %s", err)
	}
	respOther, err := ptypes.MarshalAny(&spb.Status{Code: 3})
	if err != nil {
		t.Fatalf("Unable to marshal status proto: %s", err)
	}

	tests := []struct {
		name       string
		op         *oppb.Operation
		wantStatus int
		wantNil    bool
	}{
		{
			name:    "empty operation",
			op:      &oppb.Operation{},
			wantNil: true,
		},
		{
			name:    "no response in operation",
			op:      &oppb.Operation{Result: &oppb.Operation_Error{Error: &spb.Status{Code: 99}}},
			wantNil: true, // we ignore the status in the "Operation.Error" field
		},
		{
			name:       "correct proto present",
			op:         &oppb.Operation{Result: &oppb.Operation_Response{Response: respv2}},
			wantStatus: 2,
		},
		{
			name:    "wrong proto type in response",
			op:      &oppb.Operation{Result: &oppb.Operation_Response{Response: respOther}},
			wantNil: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			st := client.OperationStatus(test.op)
			if test.wantNil {
				if st != nil {
					t.Errorf("OperationStatus(%v) = %v; want <nil>", test.op, st)
				}
				return
			}
			if st == nil {
				t.Errorf("OperationStatus(%v) = <nil>, want status code %v", test.op, test.wantStatus)
				return
			}
			if int(st.Code()) != test.wantStatus {
				t.Errorf("OperationStatus(%v) = %v, want status code %v", test.op, st, test.wantStatus)
			}
		})
	}
}
