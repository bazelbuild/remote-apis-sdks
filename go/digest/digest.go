// Package digest contains functions to simplify handling content digests.
package digest

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"regexp"
	"strconv"
	"strings"

	"github.com/golang/protobuf/proto"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

var (
	// hexStringRegex doesn't contain the size because that's checked separately.
	hexStringRegex = regexp.MustCompile("^[a-f0-9]+$")

	// Empty is the SHA256 digest of the empty blob.
	Empty = FromBlob([]byte{})
)

// IsEmpty returns true iff digest is of an empty blob.
func IsEmpty(dg *repb.Digest) bool {
	if dg == nil {
		return false
	}
	return dg.SizeBytes == 0 && dg.Hash == Empty.Hash
}

func validateHashLength(hash string) (bool, error) {
	length := len(hash)
	if length == sha256.Size*2 {
		return true, nil
	}
	return false, fmt.Errorf("valid hash length is %d, got length %d (%s)", sha256.Size*2, length, hash)
}

// Validate returns nil if a digest appears to be valid, or a descriptive error
// if it is not. All functions accepting digests directly from clients should
// call this function, whether it's via an RPC call or by reading a serialized
// proto message that contains digests that was uploaded directly from the
// client.
func Validate(digest *repb.Digest) error {
	if digest == nil {
		return errors.New("nil digest")
	}
	if ok, err := validateHashLength(digest.Hash); !ok {
		return err
	}
	if !hexStringRegex.MatchString(digest.Hash) {
		return fmt.Errorf("hash is not a lowercase hex string (%s)", digest.Hash)
	}
	if digest.SizeBytes < 0 {
		return fmt.Errorf("expected non-negative size, got %d", digest.SizeBytes)
	}
	return nil
}

// New creates a new digest from a string and size. It does some basic
// validation, which makes it marginally superior to constructing a Digest
// yourself. It returns an error if there are any problems.
func New(hash string, size int64) (*repb.Digest, error) {
	digest := &repb.Digest{Hash: hash, SizeBytes: size}
	if err := Validate(digest); err != nil {
		return nil, err
	}
	return digest, nil
}

// NewFromHash is a variant of New that accepts a hash.Hash object instead of a
// string.
func NewFromHash(h hash.Hash, size int64) (*repb.Digest, error) {
	format := fmt.Sprintf("%%0%dx", h.Size()*2)
	return New(fmt.Sprintf(format, h.Sum(nil)), size)
}

// TestNew is like New but also pads your hash with zeros if it is shorter than the required length,
// and panics on error rather than returning the error.
// ONLY USE FOR TESTS.
func TestNew(hash string, size int64) *repb.Digest {
	return mustNew(padHashSHA256(hash), size)
}

// TestFromProto is only suitable for testing and panics on error.
// ONLY USE FOR TESTS.
func TestFromProto(msg proto.Message) *repb.Digest {
	blob, err := proto.Marshal(msg)
	if err != nil {
		panic(err)
	}
	return FromBlob(blob)
}

// FromBlob takes a blob (in the form of a byte array) and returns the
// Digest proto for that blob. Changing this function will lead to cache
// invalidations (execution cache and potentially others).
func FromBlob(blob []byte) *repb.Digest {
	sha256Arr := sha256.Sum256(blob)
	return mustNew(hex.EncodeToString(sha256Arr[:]), int64(len(blob)))
}

// FromProto calculates the digest of a protobuf in SHA-256 mode.
func FromProto(msg proto.Message) (*repb.Digest, error) {
	blob, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return FromBlob(blob), nil
}

// ToDebugString returns a verbose string suitable for displaying to users.
func ToDebugString(digest *repb.Digest) string {
	if digest != nil {
		return fmt.Sprintf("size: %d, hash: %q", digest.SizeBytes, digest.Hash)
	}
	return "nil"
}

// ToString returns a hash in a canonical form of hash/size.
func ToString(digest *repb.Digest) string {
	return fmt.Sprintf("%s/%d", digest.Hash, digest.SizeBytes)
}

// FromString returns a digest from a canonical string
func FromString(s string) (*repb.Digest, error) {
	pair := strings.Split(s, "/")
	if len(pair) != 2 {
		return nil, fmt.Errorf("expected digest in the form hash/size, got %s", s)
	}
	size, err := strconv.ParseInt(pair[1], 10, 64)
	if err != nil || size < 0 {
		return nil, fmt.Errorf("invalid size in digest %s: %s", s, err)
	}
	return New(pair[0], size)
}

// Equal compares two digests for equality
func Equal(d1, d2 *repb.Digest) bool {
	return proto.Equal(d1, d2)
}

// Key is a type to be used as keys of maps from digests.
type Key struct {
	hash string
	size int64
}

// ToKey converts a digest to a Key which can then be used as map key.
func ToKey(dg *repb.Digest) Key {
	return Key{hash: dg.Hash, size: dg.SizeBytes}
}

// FromKey converts a Key back into a digest. No validation is performed, so a key taken from an
// invalid digest will produce an invalid digest.
func FromKey(k Key) *repb.Digest {
	return &repb.Digest{Hash: k.hash, SizeBytes: k.size}
}

// FilterDuplicates filters out the duplicates in a list of digests
func FilterDuplicates(digests []*repb.Digest) []*repb.Digest {
	seen := make(map[Key]bool, len(digests))
	filtered := make([]*repb.Digest, 0, len(digests))
	for _, digest := range digests {
		digestStr := ToKey(digest)
		if seen[digestStr] {
			continue
		}
		seen[digestStr] = true
		filtered = append(filtered, digest)
	}
	return filtered
}

// mustNew panics on error; it should only be called either when
// all inputs have been validated already in this package or in
// a test context (ie NewTest)
func mustNew(hash string, size int64) *repb.Digest {
	digest, err := New(hash, size)
	if err != nil {
		panic(err.Error())
	}
	return digest
}

func padHashSHA256(hash string) string {
	hashLen := sha256.Size * 2
	if len(hash) < hashLen {
		return strings.Repeat("0", hashLen-len(hash)) + hash
	}
	return hash
}
