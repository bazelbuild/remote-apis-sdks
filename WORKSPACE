workspace(name = "bazel_remote_apis_sdks")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "io_bazel_rules_go",
    sha256 = "6b65cb7917b4d1709f9410ffe00ecf3e160edf674b78c54a894471320862184f",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_go/releases/download/v0.39.0/rules_go-v0.39.0.zip",
        "https://github.com/bazelbuild/rules_go/releases/download/v0.39.0/rules_go-v0.39.0.zip",
    ],
)

http_archive(
    name = "bazel_gazelle",
    sha256 = "727f3e4edd96ea20c29e8c2ca9e8d2af724d8c7778e7923a854b2c80952bc405",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-gazelle/releases/download/v0.30.0/bazel-gazelle-v0.30.0.tar.gz",
        "https://github.com/bazelbuild/bazel-gazelle/releases/download/v0.30.0/bazel-gazelle-v0.30.0.tar.gz",
    ],
)

load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies", "go_repository")
load("@io_bazel_rules_go//go:deps.bzl", "go_register_toolchains", "go_rules_dependencies")

go_rules_dependencies()

go_register_toolchains(version = "1.20.7")

# Need "build_file_proto_mode" argument.
go_repository(
    name = "org_golang_google_grpc",
    build_file_proto_mode = "disable",
    importpath = "google.golang.org/grpc",
    commit = "7aceafcc52f95f31da11dabb4eb4b1803364a9bb"
)

# Need "build_file_proto_mode" argument.
go_repository(
    name = "org_golang_google_api",
    build_file_proto_mode = "disable",
    importpath = "google.golang.org/api",
    sum = "h1:zDobeejm3E7pEG1mNHvdxvjs5XJoCMzyNH+CmwL94Es=",
    version = "v0.122.0",
)

# Insert go_repostiry rules before this one to override specific deps.
gazelle_dependencies()

load("//:go_deps.bzl", "remote_apis_sdks_go_deps")

# gazelle:repository_macro go_deps.bzl%remote_apis_sdks_go_deps
remote_apis_sdks_go_deps()

# protobuf.
http_archive(
    name = "rules_proto",
    sha256 = "66bfdf8782796239d3875d37e7de19b1d94301e8972b3cbd2446b332429b4df1",
    strip_prefix = "rules_proto-4.0.0",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_proto/archive/refs/tags/4.0.0.tar.gz",
        "https://github.com/bazelbuild/rules_proto/archive/refs/tags/4.0.0.tar.gz",
    ],
)

load("@rules_proto//proto:repositories.bzl", "rules_proto_dependencies", "rules_proto_toolchains")

rules_proto_dependencies()

rules_proto_toolchains()

# gRPC.
http_archive(
    name = "com_github_grpc_grpc",
    sha256 = "12a4a6f8c06b96e38f8576ded76d0b79bce13efd7560ed22134c2f433bc496ad",
    strip_prefix = "grpc-1.41.1",
    urls = ["https://github.com/grpc/grpc/archive/v1.41.1.tar.gz"],
)

# Pull in all gRPC dependencies.
load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", "grpc_deps")

grpc_deps()

# Needed for the googleapis protos used by com_github_bazelbuild_remote_apis below.
http_archive(
    name = "googleapis",
    build_file = "BUILD.googleapis",
    sha256 = "7b6ea252f0b8fb5cd722f45feb83e115b689909bbb6a393a873b6cbad4ceae1d",
    strip_prefix = "googleapis-143084a2624b6591ee1f9d23e7f5241856642f4d",
    urls = ["https://github.com/googleapis/googleapis/archive/143084a2624b6591ee1f9d23e7f5241856642f4d.zip"],
)

go_repository(
    name = "com_github_bazelbuild_remote_apis",
    importpath = "github.com/bazelbuild/remote-apis",
    sum = "h1:Lj8uXWW95oXyYguUSdQDvzywQb4f0jbJWsoLPQWAKTY=",
    version = "v0.0.0-20230411132548-35aee1c4a425",
)

load("@com_github_bazelbuild_remote_apis//:remote_apis_deps.bzl", "remote_apis_go_deps")

remote_apis_go_deps()
