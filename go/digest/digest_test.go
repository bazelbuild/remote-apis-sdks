package digest

import (
	"crypto/sha256"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/google/go-cmp/cmp"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

var (
	dInvalid        = &repb.Digest{Hash: strings.Repeat("a", 25), SizeBytes: 321}
	dSHA256         = &repb.Digest{Hash: strings.Repeat("a", 64), SizeBytes: 321}
	dSHA256Sizeless = &repb.Digest{Hash: dSHA256.Hash, SizeBytes: -1}

	sGood     = fmt.Sprintf("%s/%d", dSHA256.Hash, dSHA256.SizeBytes)
	sSizeless = fmt.Sprintf("%s/%d", dSHA256Sizeless.Hash, dSHA256Sizeless.SizeBytes)
	sInvalid1 = fmt.Sprintf("%d/%s", dSHA256.SizeBytes, dSHA256.Hash)
	sInvalid2 = fmt.Sprintf("%s/test", sInvalid1)
	sInvalid3 = fmt.Sprintf("x%s", sGood[1:])
)

func TestValidateDigests(t *testing.T) {
	t.Parallel()
	testcases := []struct {
		desc   string
		digest *repb.Digest
	}{
		{"SHA256", dSHA256},
	}
	for _, tc := range testcases {
		if err := Validate(tc.digest); err != nil {
			t.Errorf("%s: Validate(%v) = %v, want nil", tc.desc, tc.digest, err)
		}
	}
}

func TestValidateDigests_Errors(t *testing.T) {
	t.Parallel()
	testcases := []struct {
		desc   string
		digest *repb.Digest
	}{
		{"too short", &repb.Digest{Hash: "000a"}},
		{"too long", &repb.Digest{Hash: "0000000000000000000000000000000000000000000000000000000000000000a"}},
		{"must be lowercase", &repb.Digest{Hash: "000000000000000000000000000000000000000000000000000000000000000A"}},
		{"can't be crazy", &repb.Digest{Hash: "000000000000000000000000000000000000000000000000000000000foobar!"}},
		{"can't be nil", nil},
		{"can't be negatively-sized", &repb.Digest{Hash: dSHA256.Hash, SizeBytes: -1}},
	}
	for _, tc := range testcases {
		err := Validate(tc.digest)
		if err == nil {
			t.Errorf("%s: got success, wanted error", tc.desc)
		}
		t.Logf("%s: correctly got an error (%s)", tc.desc, err)
	}
}

func Test_New(t *testing.T) {
	t.Parallel()
	testcases := []struct {
		label      string
		hash       string
		size       int64
		wantDigest *repb.Digest
		wantErr    bool
	}{
		{"SHA256", dSHA256.Hash, dSHA256.SizeBytes, dSHA256, false},
		{"Invalid", dInvalid.Hash, dInvalid.SizeBytes, nil, true},
	}
	for _, tc := range testcases {
		gotDigest, err := New(tc.hash, tc.size)
		if err == nil && tc.wantErr {
			t.Errorf("%s: New(%s, %d) = (_, nil), want (_, error)", tc.label, tc.hash, tc.size)
		}
		if err != nil && !tc.wantErr {
			t.Errorf("%s: New(%s, %d) = (_, %v), want (_, nil)", tc.label, tc.hash, tc.size, err)
		}
		if diff := cmp.Diff(tc.wantDigest, gotDigest); diff != "" {
			t.Errorf("%s: New(%s, %d) = (%v, _), want (%v, _), diff (-want +got):\n:%s", tc.label, tc.hash, tc.size, gotDigest, tc.wantDigest, diff)
		}
	}
}

func TestTestNewPads(t *testing.T) {
	t.Parallel()
	dShort := TestNew("a", 0)
	dFull := TestNew("000000000000000000000000000000000000000000000000000000000000000a", 0)
	want := "000000000000000000000000000000000000000000000000000000000000000a"
	if dShort.Hash != want {
		t.Errorf("dShort.Hash = %s, want %s", dShort.Hash, want)
	}
	if dFull.Hash != want {
		t.Errorf("dFull.Hash = %s, want %s", dFull.Hash, want)
	}
}

func TestTestNew(t *testing.T) {
	t.Parallel()
	dWant := "000000000000000000000000000000000000000000000000000000000000000a"
	if dGot := TestNew("a", 0); dGot.Hash != dWant {
		t.Errorf("TestNew('a', _) = %v, want %v", dGot.Hash, dWant)
	}
}

func TestFromBlob(t *testing.T) {
	t.Parallel()
	dWant, err := NewFromHash(sha256.New(), 0)
	if err != nil {
		t.Fatalf("NewFromHash(sha256.New(), 0) = (_, %v), want (_, nil)", err)
	}
	if dGot := FromBlob([]byte{}); !Equal(dGot, dWant) {
		t.Errorf("FromBlob([]byte{}) = %v, want %v", dGot, dWant)
	}
}

func Test_FromProto(t *testing.T) {
	t.Parallel()

	// Use a repb.Digest as a black-box proto.
	msg := FromBlob([]byte{1, 2, 3})
	b, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("proto.Marshal(%v) = (_, %v), want (_, nil)", msg, err)
	}
	dWant := FromBlob(b)

	dGot, err := FromProto(msg)
	if err != nil {
		t.Errorf("FromProto(%v) = (_, %v), want (_, nil)", msg, err)
	}
	if !Equal(dGot, dWant) {
		t.Errorf("FromProto(%v) = (%v, _), want (%v, _)", msg, dGot, dWant)
	}
}

func TestTestFromProto(t *testing.T) {
	t.Parallel()

	// Use a repb.Digest as a black-box proto.
	msg := FromBlob([]byte{1, 2, 3})
	b, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("proto.Marshal(%v) = (_, %v), want (_, nil)", msg, err)
	}
	dWant := FromBlob(b)

	dGot := TestFromProto(msg)
	if !Equal(dGot, dWant) {
		t.Errorf("TestFromProto(%v) = %v, want %v", msg, dGot, dWant)
	}
}

func TestToString(t *testing.T) {
	t.Parallel()
	if sGot := ToString(dSHA256); sGot != sGood {
		t.Errorf("ToString(%v) = '%s', want '%s'", dSHA256, sGot, sGood)
	}
	if sGot := ToString(dSHA256Sizeless); sGot != sSizeless {
		t.Errorf("ToString(%v) = '%s', want '%s'", dSHA256Sizeless, sGot, sSizeless)
	}
}

func TestFromString(t *testing.T) {
	t.Parallel()
	if dGot, err := FromString(sGood); err != nil || !Equal(dGot, dSHA256) {
		t.Errorf("FromString(%s) = (%v, %v), want (%v, nil)", sGood, dGot, err, dSHA256)
	}
	if _, err := FromString(sSizeless); err == nil {
		t.Errorf("FromString(%s) = (_, nil), want (_, error)", sSizeless)
	}
	if _, err := FromString(sInvalid1); err == nil {
		t.Errorf("FromString(%s) = (_, nil), want (_, error)", sInvalid1)
	}
	if _, err := FromString(sInvalid2); err == nil {
		t.Errorf("FromString(%s) = (_, nil), want (_, error)", sInvalid2)
	}
	if _, err := FromString(sInvalid3); err == nil {
		t.Errorf("FromString(%s) = (_, nil), want (_, error)", sInvalid3)
	}
}

func TestEqual(t *testing.T) {
	t.Parallel()
	if !Equal(dSHA256, dSHA256) {
		t.Errorf("Equal(dSHA256, dSHA256) = false, want true")
	}
	if !Equal(nil, nil) {
		t.Errorf("Equal(nil, nil) = false, want true")
	}
	if Equal(nil, dSHA256) {
		t.Errorf("Equal(nil, dSHA256) = true, want false")
	}
}

func TestMustNewShouldPanic(t *testing.T) {
	t.Parallel()
	defer func() {
		if err := recover(); err == nil {
			t.Error("mustNew(dInvalid) should have panicked")
		}
	}()
	mustNew(dInvalid.Hash, dInvalid.SizeBytes)
}

func TestNewFromHash(t *testing.T) {
	t.Parallel()
	// SHA256 is valid.
	if got, err := NewFromHash(sha256.New(), 0); err != nil {
		t.Errorf("NewFromHash(sha256.New(), 0) = (%v, %v), want (_, nil)", got, err)
	}
	// SHA224 is NOT valid.
	if _, err := NewFromHash(sha256.New224(), 0); err == nil {
		t.Errorf("NewFromHash(sha256.New224(), 0) = (%v, nil), want (_, non-nil)", err)
	}
}

func TestFilterDuplicateDigests(t *testing.T) {
	t.Parallel()
	d1 := TestNew("10", 0)
	d2 := TestNew("20", 0)
	d3 := TestNew("30", 0)
	d4 := TestNew("40", 0)
	actual := FilterDuplicates([]*repb.Digest{d1, d2, d1, d3, d1, d4, d4, d1})
	expected := []*repb.Digest{d1, d2, d3, d4}
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expected %v, but got %v", expected, actual)
	}
}

func TestToFromKey(t *testing.T) {
	t.Parallel()
	digests := []*repb.Digest{
		TestNew("1", 0),
		TestNew("2", 0),
		TestNew("1", 1),
		TestNew("2", 1),
		TestNew("a", 34),
	}
	seen := make(map[Key]*repb.Digest)
	for _, d := range digests {
		k := ToKey(d)
		if s, ok := seen[k]; ok {
			t.Errorf("ToKey(%+v) = ToKey(%+v) = %+v; want distinct results", d, s, k)
		}
		seen[k] = d

		rt := FromKey(k)
		if !reflect.DeepEqual(d, rt) {
			t.Errorf("FromKey(ToKey(%+v)) = %+v; want result equal to input", d, rt)
		}
	}
}
