module github.com/bazelbuild/remote-apis-sdks

// When you update the go version here, you have to also update the
// go version in the .github/workflows/golangci-lint.yml file.
go 1.25.7

require (
	github.com/bazelbuild/remote-apis v0.0.0-20230411132548-35aee1c4a425
	github.com/golang/glog v1.2.5
	github.com/google/go-cmp v0.7.0
	github.com/google/uuid v1.6.0
	github.com/klauspost/compress v1.18.3
	github.com/pkg/xattr v0.4.12
	golang.org/x/oauth2 v0.34.0
	golang.org/x/sync v0.19.0
	google.golang.org/api v0.265.0
	google.golang.org/genproto v0.0.0-20260203192932-546029d2fa20
	google.golang.org/genproto/googleapis/bytestream v0.0.0-20260203192932-546029d2fa20
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260203192932-546029d2fa20
	google.golang.org/grpc v1.78.0
	google.golang.org/protobuf v1.36.11
)

require (
	cloud.google.com/go/compute/metadata v0.9.0 // indirect
	cloud.google.com/go/longrunning v0.8.0 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	golang.org/x/net v0.49.0 // indirect
	golang.org/x/sys v0.40.0 // indirect
	golang.org/x/text v0.33.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260203192932-546029d2fa20 // indirect
)
