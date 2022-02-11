package digest

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
)

var (
	dInvalid        = Digest{Hash: strings.Repeat("a", 25), Size: 321}
	dSHA256         = Digest{Hash: strings.Repeat("a", 64), Size: 321}
	dSHA256Sizeless = Digest{Hash: dSHA256.Hash, Size: -1}

	sGood     = fmt.Sprintf("%s/%d", dSHA256.Hash, dSHA256.Size)
	sSizeless = fmt.Sprintf("%s/%d", dSHA256Sizeless.Hash, dSHA256Sizeless.Size)
	sInvalid1 = fmt.Sprintf("%d/%s", dSHA256.Size, dSHA256.Hash)
	sInvalid2 = fmt.Sprintf("%s/test", sInvalid1)
	sInvalid3 = fmt.Sprintf("x%s", sGood[1:])
)

func TestProtoConversion(t *testing.T) {
	dPb := dSHA256.ToProto()
	d, err := NewFromProto(dPb)
	if err != nil {
		t.Errorf("NewFromProto(%v) = %v, want nil", dPb, err)
	}
	if dSHA256 != d {
		t.Errorf("NewFromProto(%v.ToProto()) = (%s, _), want (%s, _)", dSHA256, d, dSHA256)
	}
}

func TestValidateDigests_Pass(t *testing.T) {
	if err := dSHA256.Validate(); err != nil {
		t.Errorf("Validate(%v) = %v, want nil", dSHA256, err)
	}
}

func TestValidateDigests_Errors(t *testing.T) {
	t.Parallel()
	testcases := []struct {
		desc   string
		digest Digest
	}{
		{"too short", Digest{Hash: "000a"}},
		{"too long", Digest{Hash: "0000000000000000000000000000000000000000000000000000000000000000a"}},
		{"must be lowercase", Digest{Hash: "000000000000000000000000000000000000000000000000000000000000000A"}},
		{"can't be crazy", Digest{Hash: "000000000000000000000000000000000000000000000000000000000foobar!"}},
		{"can't be negatively-sized", Digest{Hash: dSHA256.Hash, Size: -1}},
	}
	for _, tc := range testcases {
		err := tc.digest.Validate()
		if err == nil {
			t.Errorf("%s: got success, wanted error", tc.desc)
		}
		t.Logf("%s: correctly got an error (%s)", tc.desc, err)
	}
}

func Test_New_Success(t *testing.T) {
	t.Parallel()
	gotDigest, err := New(dSHA256.Hash, dSHA256.Size)
	if err != nil {
		t.Errorf("New(%s, %d) = (_, %v), want (_, nil)", dSHA256.Hash, dSHA256.Size, err)
	}
	if dSHA256 != gotDigest {
		t.Errorf("New(%s, %d) = (%v, _), want (%v, _)", dSHA256.Hash, dSHA256.Size, gotDigest, dSHA256)
	}
}

func Test_New_Error(t *testing.T) {
	t.Parallel()
	gotDigest, err := New(dInvalid.Hash, dInvalid.Size)
	if err == nil {
		t.Errorf("New(%s, %d) = (%s, nil), want (_, error)", dInvalid.Hash, dInvalid.Size, gotDigest)
	}
}

func Test_NewFromProto_Success(t *testing.T) {
	t.Parallel()
	dPb := dSHA256.ToProto()
	gotDigest, err := NewFromProto(dPb)
	if err != nil {
		t.Errorf("NewFromProto(%v) = (_, %v), want (_, nil)", dPb, err)
	}
	if dSHA256 != gotDigest {
		t.Errorf("NewFromProto(%v) = (%v, _), want (%v, _)", dPb, gotDigest, dSHA256)
	}
}

func Test_NewFromProto_Error(t *testing.T) {
	t.Parallel()
	dPb := dInvalid.ToProto()
	gotDigest, err := NewFromProto(dPb)
	if err == nil {
		t.Errorf("NewFromProto(%v) = (%s, nil), want (_, error)", gotDigest, dPb)
	}
}

func TestNewFromProtoUnvalidated(t *testing.T) {
	t.Parallel()
	testcases := []struct {
		label  string
		digest Digest
	}{
		{"SHA256", dSHA256},
		{"Invalid", dInvalid},
	}
	for _, tc := range testcases {
		dPb := tc.digest.ToProto()
		gotDigest := NewFromProtoUnvalidated(dPb)
		if tc.digest != gotDigest {
			t.Errorf("%s: NewFromProto(%v) = (%v, _), want (%v, _)", tc.label, dPb, gotDigest, tc.digest)
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

func TestTestNew_Panic(t *testing.T) {
	t.Parallel()
	d := &Digest{Hash: "not good", Size: -1}
	defer func() {
		if err := recover(); err == nil {
			t.Errorf("TestNew(%v) should have panicked", d)
		}
	}()
	TestNew(d.Hash, d.Size)
}

func TestNewFromBlob(t *testing.T) {
	t.Parallel()
	if dGot := NewFromBlob([]byte{}); dGot != Empty {
		t.Errorf("NewFromBlob([]byte{}) = %v, want %v", dGot, Empty)
	}
	dWant := TestNew("039058c6f2c0cb492c533b0a4d14ef77cc0f78abccced5287d84a1a2011cfb81", 3)
	if dGot := NewFromBlob([]byte{1, 2, 3}); dGot != dWant {
		t.Errorf("NewFromBlob([]byte{}) = %v, want %v", dGot, dWant)
	}
}

func Test_NewFromMessage(t *testing.T) {
	t.Parallel()
	// Use a repb.Digest as a black-box proto.
	msg := NewFromBlob([]byte{1, 2, 3}).ToProto()
	b, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("proto.Marshal(%v) = (_, %v), want (_, nil)", msg, err)
	}
	dWant := NewFromBlob(b)

	dGot, err := NewFromMessage(msg)
	if err != nil {
		t.Errorf("NewFromMessage(%v) = (_, %v), want (_, nil)", msg, err)
	}
	if dGot != dWant {
		t.Errorf("NewFromMessage(%v) = (%v, _), want (%v, _)", msg, dGot, dWant)
	}
}

func TestTestNewFromMessage(t *testing.T) {
	t.Parallel()
	// Use a repb.Digest as a black-box proto.
	msg := NewFromBlob([]byte{1, 2, 3}).ToProto()
	b, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("proto.Marshal(%v) = (_, %v), want (_, nil)", msg, err)
	}
	dWant := NewFromBlob(b)

	dGot := TestNewFromMessage(msg)
	if dGot != dWant {
		t.Errorf("TestNewFromMessage(%v) = (%v, _), want (%v, _)", msg, dGot, dWant)
	}
}

func TestNewFromFile(t *testing.T) {
	t.Parallel()
	testDir := t.TempDir()
	path := filepath.Join(testDir, "input")
	if err := os.WriteFile(path, []byte{1, 2, 3}, os.FileMode(0666)); err != nil {
		t.Fatalf("os.WriteFile(%v, _, _) = %v, want nil", path, err)
	}
	dWant := TestNew("039058c6f2c0cb492c533b0a4d14ef77cc0f78abccced5287d84a1a2011cfb81", 3)
	dGot, err := NewFromFile(path)
	if err != nil {
		t.Errorf("NewFromFile(%v) = (_, %v), want (_, nil)", path, err)
	}
	if dGot != dWant {
		t.Errorf("NewFromFile(%v) = (%v, _), want (%v, _)", path, dGot, dWant)
	}
}

func TestString(t *testing.T) {
	t.Parallel()
	if sGot := dSHA256.String(); sGot != sGood {
		t.Errorf("%v.String() = '%s', want '%s'", dSHA256, sGot, sGood)
	}
	if sGot := dSHA256Sizeless.String(); sGot != sSizeless {
		t.Errorf("%v.String() = '%s', want '%s'", dSHA256Sizeless, sGot, sSizeless)
	}
}

func TestNewFromString(t *testing.T) {
	t.Parallel()
	if dGot, err := NewFromString(sGood); err != nil || dGot != dSHA256 {
		t.Errorf("FromString(%s) = (%v, %v), want (%v, nil)", sGood, dGot, err, dSHA256)
	}
	if _, err := NewFromString(sSizeless); err == nil {
		t.Errorf("FromString(%s) = (_, nil), want (_, error)", sSizeless)
	}
	if _, err := NewFromString(sInvalid1); err == nil {
		t.Errorf("FromString(%s) = (_, nil), want (_, error)", sInvalid1)
	}
	if _, err := NewFromString(sInvalid2); err == nil {
		t.Errorf("FromString(%s) = (_, nil), want (_, error)", sInvalid2)
	}
	if _, err := NewFromString(sInvalid3); err == nil {
		t.Errorf("FromString(%s) = (_, nil), want (_, error)", sInvalid3)
	}
}
