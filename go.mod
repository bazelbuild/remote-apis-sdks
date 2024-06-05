module github.com/bazelbuild/remote-apis-sdks

go 1.20

require (
	cloud.google.com/go/longrunning v0.5.7
	github.com/bazelbuild/remote-apis v0.0.0-20240409135018-1f36c310b28d
	github.com/golang/glog v1.2.0
	github.com/google/go-cmp v0.6.0
	github.com/google/uuid v1.6.0
	github.com/hectane/go-acl v0.0.0-20230122075934-ca0b05cb1adb
	github.com/klauspost/compress v1.17.8
	github.com/pkg/xattr v0.4.9
	golang.org/x/oauth2 v0.21.0
	golang.org/x/sync v0.7.0
	google.golang.org/api v0.183.0
	google.golang.org/genproto/googleapis/bytestream v0.0.0-20240528184218-531527333157
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240528184218-531527333157
	google.golang.org/grpc v1.64.0
	google.golang.org/protobuf v1.34.1
)

replace github.com/bazelbuild/remote-apis v0.0.0-20240409135018-1f36c310b28d => github.com/bentekkie/remote-apis v0.0.0-20240605144600-b36ef32b88ff

require (
	cloud.google.com/go/compute/metadata v0.3.0 // indirect
	golang.org/x/net v0.25.0 // indirect
	golang.org/x/sys v0.20.0 // indirect
	golang.org/x/text v0.15.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20240521202816-d264139d666e // indirect
)
