package client

import (
	"context"
	"errors"
	"net"
	"os"
	"path"
	"testing"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	svpb "github.com/bazelbuild/remote-apis/build/bazel/semver"
	"google.golang.org/grpc"
)

const (
	// This key pair for `foo.example.com` was generated using https://golang.org/src/crypto/tls/generate_cert.go

	tlsCert = `
-----BEGIN CERTIFICATE-----
MIIC/zCCAeegAwIBAgIQa6p4srafgw0S6iDvt7rlXzANBgkqhkiG9w0BAQsFADAS
MRAwDgYDVQQKEwdBY21lIENvMB4XDTIwMTAwODA5NTExM1oXDTIxMTAwODA5NTEx
M1owEjEQMA4GA1UEChMHQWNtZSBDbzCCASIwDQYJKoZIhvcNAQEBBQADggEPADCC
AQoCggEBAM1Mo2UOcQxkPHs2+oaclJsPZo80Cqb0Oze0zxA9uFXg6SiklqPjWUPc
o2TTBmbpw6t2Cv6Ywr6cTB7PJXCruOFo5x+6rZlzjrfqArYn3Btv64/6SwaarIEW
UcisWDvcEW+QArAmsJo39RL4ptsb+0Eoqkm1WeQ31XWh9i7W0F3qy43lC4k5QpXb
imeevejwk3TMFAnAc8dBXxNDg3gr9isDq2yH1ZhJ/4BNcle6WYNny0Uh9kYCLCWe
PK1ZYOeT1rRCtBJTg7w1/e0Qpln6V/MsJNtgt14vYPsHAsW+AMLycN1f9/8T7joz
6/D8GVMC5Vaz2z5CxeNb5ifCQkmEN8kCAwEAAaNRME8wDgYDVR0PAQH/BAQDAgWg
MBMGA1UdJQQMMAoGCCsGAQUFBwMBMAwGA1UdEwEB/wQCMAAwGgYDVR0RBBMwEYIP
Zm9vLmV4YW1wbGUuY29tMA0GCSqGSIb3DQEBCwUAA4IBAQB0d9y7XyLwWMBMxIRL
5rdgk9AvpVKX5FtYkJmxG7F9p67GlLhy314SyPDaUm/tl0yf/LM9m7BM7m9900mO
9qjfTi6wc0Co9dDMhi4rh0CPlEilWpiA7wAyM0eVQPJx+wv/u/8aDzVy/Cr8wLwi
UyXETaAVsbrHmoIMQgyy0boTDzBwwhx8e/heqaZfV6H6RsBpChYFIF0A7T92bu91
WmSA9kfUwzYeOvLyX/s+J77U/bnYo4Fl/LnBkBr1vkTPF9xGWYZFU0Ok/uk9TyrB
vI06wv3H6phi4fWDMbWuSiBHr3AefTqcxkBcaxYCzLgOdRJEnV1GWdw/Gt9PNyJ2
IJff
-----END CERTIFICATE-----
`

	tlsKey = `
-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDNTKNlDnEMZDx7
NvqGnJSbD2aPNAqm9Ds3tM8QPbhV4OkopJaj41lD3KNk0wZm6cOrdgr+mMK+nEwe
zyVwq7jhaOcfuq2Zc4636gK2J9wbb+uP+ksGmqyBFlHIrFg73BFvkAKwJrCaN/US
+KbbG/tBKKpJtVnkN9V1ofYu1tBd6suN5QuJOUKV24pnnr3o8JN0zBQJwHPHQV8T
Q4N4K/YrA6tsh9WYSf+ATXJXulmDZ8tFIfZGAiwlnjytWWDnk9a0QrQSU4O8Nf3t
EKZZ+lfzLCTbYLdeL2D7BwLFvgDC8nDdX/f/E+46M+vw/BlTAuVWs9s+QsXjW+Yn
wkJJhDfJAgMBAAECggEAOdsALQl92FUjKkIbJfZYdr5HU9IBsn0fdxpwRA5wtVr1
uitv/G2kiVhTf6Vsp3H3X4mbnZYlQ5w+Y2xTC8aJCpUKlUmBhL4pGTFMZFMlV2Ia
C3Ao1oqeVJ6am6feEbh5/WujJksw97UPTrJeK19eVkWEf9MoqwUFFep3u0l0XRK9
zR5ZhhZe+3C1DS3r7P89/7ZlnqLuba94fIqwn11D7jzmiZQBuBLa6X16grGs03Gb
LoBoiTj7g0a6nSEcDen5jI8oPrZuMWAlTXLYb68eCgvU2FXe0A2PFZzh8XQ71Y4J
Y6L0oGGiTThgphsH/bqQi03mF/Hd0BcIlBe+8mPSFQKBgQDuUI5ENq650eOf6PPh
abLl+cmBIxNdfFYZHB5IapDfqKBK3imD3QYT4cBdbcQ0PWjbFj/aCgkPYCmMxBSX
WGVohhH4sXHNIS0BL6dGalQhE+C3tWkzg79AE84tTrvijVuOom3RiR0SGzBQ4YBl
boPPKinY6Hz6FEhlfWdlwR0GcwKBgQDciN2m1wNG6gkDg/2OEn+g79boKPdI0lC4
VTfQ6xDKwGs2N8HIOoeXHhTStgkPCeNmxuOtEtqfKKRpje2ZRFvjZWOh6t+CvX0w
ZE+GhJtBC0ODYQ9+Hai1HgBbLYrMR44cRWRx5uMD4/v4Vrg7jdtpUICeonPd4mMD
ppgV0Ze90wKBgBWu+LLWMuGvakGjhYUuA9wO9TmtwlZQONlCCLNYFuRyyFrniel4
k9P9R254zVBfggnlJVwma5qdikpHkQQQQ/HVuQ7ivlMGwLyJ5HNwu0TjNSAh/nd0
dlNTOgA/WLMNX5ZDyzA0hJOgK65ARll0S8Puty4IQU7Tx56MYXsNriFnAoGBAI1g
+Kfio2ks1AZ68WvJFCT6XM2Mpar7mV/y0cuPRr6USKcDa6mPTClm0Xab2IbZkfzr
eD4WHi12gqBgqnddSYMoFo1Q42UPUVhalljoXhd+vxNUf/gbz3H8/8F0Gs0G+mXJ
XrFekR6HY5s7DPSw5n5Ha41HG/ydC1jlzg3+xcadAoGBAIZ/ZJpnC2Tv8poGHbmD
B3hWlPIQSU2dEGQCEhiGhzoi7muXmYqx5t8YnMGLGmsltLxD95KhACrTZZmBGiUS
HavFSkKMI1EuncwH2ooshIF9ZQqYNpIH7YGxnzDgNXnmeb26FI+b0uuxW74PWZrL
h4A58eQ+JGSLao6JSmi2T0tZ
-----END PRIVATE KEY-----
`

	instance = "instance"
)

func TestCreateTLSConfig(t *testing.T) {
	t.Run("EmptyDialParams", func(t *testing.T) {
		_, err := createTLSConfig(DialParams{})
		if err != nil {
			t.Errorf("Could not create TLS config: %v", err)
		}
	})

	t.Run("OverrideServerName", func(t *testing.T) {
		tlsConfig, err := createTLSConfig(DialParams{
			TLSServerName: "foo.example.com",
		})
		if err != nil {
			t.Errorf("Could not create TLS config: %v", err)
		}
		if tlsConfig.ServerName != "foo.example.com" {
			t.Errorf("Expected ServerName to be 'foo.example.com', got '%v'", tlsConfig.ServerName)
		}
	})

	t.Run("UseClientCertificate", func(t *testing.T) {
		t.Run("OnlyTLSClientAuthCert", func(t *testing.T) {
			_, err := createTLSConfig(DialParams{
				TLSClientAuthCert: "/foo/bar",
			})
			if err == nil {
				t.Error("Expected error creating mTLS config without auth-key, got nil")
			}
		})

		t.Run("OnlyTLSClientAuthKey", func(t *testing.T) {
			_, err := createTLSConfig(DialParams{
				TLSClientAuthKey: "/foo/bar",
			})
			if err == nil {
				t.Error("Expected error creating mTLS config without auth-cert, got nil")
			}
		})

		t.Run("OnlyTLSClientAuthCert", func(t *testing.T) {
			certPath := path.Join(t.TempDir(), "cert.pem")
			if err := os.WriteFile(certPath, []byte(tlsCert), 0644); err != nil {
				t.Fatalf("Could not write '%v': %v", certPath, err)
			}
			keyPath := path.Join(t.TempDir(), "key.pem")
			if err := os.WriteFile(keyPath, []byte(tlsKey), 0644); err != nil {
				t.Fatalf("Could not write '%v': %v", keyPath, err)
			}

			tlsConfig, err := createTLSConfig(DialParams{
				TLSClientAuthCert: certPath,
				TLSClientAuthKey:  keyPath,
			})
			if err != nil {
				t.Errorf("Could not create TLS config: %v", err)
			}
			if len(tlsConfig.Certificates) != 1 {
				t.Errorf("Expected exactly 1 certificate, got: %v", tlsConfig.Certificates)
			}
		})
	})
}

func TestVersionComparison(t *testing.T) {
	latestSupportedVersion := &svpb.SemVer{Major: 2, Minor: 1}
	serverCapabilities := &repb.ServerCapabilities{
		HighApiVersion: latestSupportedVersion,
	}

	if !highAPIVersionNewerThanOrEqualTo(serverCapabilities, 1, 9) {
		t.Errorf("Got: !(2.1 >= 1.9")
	}

	if !highAPIVersionNewerThanOrEqualTo(serverCapabilities, 2, 0) {
		t.Errorf("Got: !(2.1 >= 2.0")
	}

	if highAPIVersionNewerThanOrEqualTo(serverCapabilities, 2, 2) {
		t.Errorf("Got: 2.1 >= 2.2")
	}

	if highAPIVersionNewerThanOrEqualTo(&repb.ServerCapabilities{}, 0, 1) {
		t.Errorf("Got: empty >= 0.1")
	}

	if highAPIVersionNewerThanOrEqualTo(nil, 0, 1) {
		t.Errorf("Got: nil >= 0.1")
	}
}

func TestActionSupportsPlatformProperties(t *testing.T) {
	if supportsActionPlatformProperties(&repb.ServerCapabilities{HighApiVersion: &svpb.SemVer{Major: 2, Minor: 1}}) {
		t.Errorf("Got platform properties field suppported for v2.1 (expected only for v >= 2.2)")
	}

	if !supportsActionPlatformProperties(&repb.ServerCapabilities{HighApiVersion: &svpb.SemVer{Major: 2, Minor: 2}}) {
		t.Errorf("Got platform properties field not suppported for v2.2 (expected for v >= 2.2)")
	}

	if !supportsActionPlatformProperties(&repb.ServerCapabilities{HighApiVersion: &svpb.SemVer{Major: 2, Minor: 3}}) {
		t.Errorf("Got platform properties field not suppported for v2.3 (expected for v >= 2.2)")
	}
}

func TestCommandUsesOutputPaths(t *testing.T) {
	if supportsCommandOutputPaths(&repb.ServerCapabilities{HighApiVersion: &svpb.SemVer{Major: 2, Minor: 0}}) {
		t.Errorf("Got `output_paths` field suppported for v2.0 (expected only for v >= 2.1)")
	}

	if !supportsCommandOutputPaths(&repb.ServerCapabilities{HighApiVersion: &svpb.SemVer{Major: 2, Minor: 1}}) {
		t.Errorf("Got `output_paths` field not suppported for v2.1 (expected for v >= 2.1)")
	}

	if !supportsCommandOutputPaths(&repb.ServerCapabilities{HighApiVersion: &svpb.SemVer{Major: 2, Minor: 2}}) {
		t.Errorf("Got `output_paths` field not suppported for v2.2 (expected for v >= 2.1)")
	}
}

func TestNewClient(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	c, err := NewClient(ctx, instance, DialParams{
		Service:    "server",
		NoSecurity: true,
	}, StartupCapabilities(false))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer c.Close()
}

func TestNewClientFromConnection(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}
	conn, err := grpc.Dial(l.Addr().String(), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Cannot establish gRPC connection: %v", err)
	}

	c, err := NewClientFromConnection(ctx, instance, conn, conn, StartupCapabilities(false))
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}
	defer c.Close()

	_, err = NewClientFromConnection(ctx, instance, nil, conn, StartupCapabilities(false))
	if err == nil {
		t.Fatalf("Expected error got nil")
	}

	_, err = NewClientFromConnection(ctx, instance, conn, nil, StartupCapabilities(false))
	if err == nil {
		t.Fatalf("Expected error got nil")
	}
}

func TestResourceName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		instanceName string
		segments     []string
		wantName     string
		wantErr      error
	}{
		{
			name:         "valid",
			segments:     []string{"uploads", "uuid", "blobs", "abc", "1", "meta"},
			instanceName: "the/instance",
			wantName:     "the/instance/uploads/uuid/blobs/abc/1/meta",
		},
		{
			name:         "empty_instance_name",
			segments:     []string{"uploads", "uuid", "blobs", "abc", "1", "meta"},
			instanceName: "",
			wantName:     "uploads/uuid/blobs/abc/1/meta",
		},
		{
			name:         "empty_instance_name_no_segments",
			segments:     []string{},
			instanceName: "",
			wantName:     "",
		},
		{
			name:         "empty_segment",
			segments:     []string{"uploads", "", "uuid", "blobs", "abc", "1", "meta"},
			instanceName: "the/instance",
			wantName:     "",
			wantErr:      ErrEmptySegment,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := Client{InstanceName: test.instanceName}
			name, err := c.ResourceName(test.segments...)
			if !errors.Is(test.wantErr, err) {
				t.Errorf("unexpected error; want %v, got %v", test.wantErr, err)
			}
			if name != test.wantName {
				t.Errorf("name mismatch; want %q, got %q", test.wantName, name)
			}
		})
	}
}
