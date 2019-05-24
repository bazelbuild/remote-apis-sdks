workspace(name = "bazel_remote_apis_sdks")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

# Go rules.
http_archive(
    name = "io_bazel_rules_go",
    sha256 = "86ae934bd4c43b99893fc64be9d9fc684b81461581df7ea8fc291c816f5ee8c5",
    urls = ["https://github.com/bazelbuild/rules_go/releases/download/0.18.3/rules_go-0.18.3.tar.gz"],
)

# Gazelle.
http_archive(
    name = "bazel_gazelle",
    sha256 = "3c681998538231a2d24d0c07ed5a7658cb72bfb5fd4bf9911157c0e9ac6a2687",
    urls = ["https://github.com/bazelbuild/bazel-gazelle/releases/download/0.17.0/bazel-gazelle-0.17.0.tar.gz"],
)

load("@io_bazel_rules_go//go:deps.bzl", "go_rules_dependencies", "go_register_toolchains")
load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies", "go_repository")

go_rules_dependencies()

go_register_toolchains()

gazelle_dependencies()

# gRPC.
http_archive(
    name = "com_github_grpc_grpc",
    sha256 = "8da7f32cc8978010d2060d740362748441b81a34e5425e108596d3fcd63a97f2",
    strip_prefix = "grpc-1.21.0",
    urls = [
        "https://github.com/grpc/grpc/archive/v1.21.0.tar.gz",
        "https://mirror.bazel.build/github.com/grpc/grpc/archive/v1.21.0.tar.gz",
    ],
)

# Pull in all gRPC dependencies.
load("@com_github_grpc_grpc//bazel:grpc_deps.bzl", "grpc_deps")

grpc_deps()

# Go dependencies.
#
# Add or update repos using Gazelle:
# https://github.com/bazelbuild/bazel-gazelle#update-repos

go_repository(
    name = "com_github_pkg_errors",
    commit = "27936f6d90f9c8e1145f11ed52ffffbfdb9e0af7",
    importpath = "github.com/pkg/errors",
)

go_repository(
    name = "com_github_golang_protobuf",
    commit = "e91709a02e0e8ff8b86b7aa913fdc9ae9498e825",
    importpath = "github.com/golang/protobuf",
)

go_repository(
    name = "com_github_google_go_cmp",
    commit = "6f77996f0c42f7b84e5a2b252227263f93432e9b",
    importpath = "github.com/google/go-cmp",
)

go_repository(
    name = "org_golang_google_grpc",
    commit = "a8b5bd3c39ac82177c7bad36e1dd695096cd0ef5",
    importpath = "google.golang.org/grpc",
)

go_repository(
    name = "org_golang_x_oauth2",
    commit = "9f3314589c9a9136388751d9adae6b0ed400978a",
    importpath = "golang.org/x/oauth2",
)

go_repository(
    name = "com_github_golang_glog",
    commit = "23def4e6c14b4da8ac2ed8007337bc5eb5007998",
    importpath = "github.com/golang/glog",
)

go_repository(
    name = "com_github_google_uuid",
    build_file_generation = "on",
    commit = "c2e93f3ae59f2904160ceaab466009f965df46d6",
    importpath = "github.com/google/uuid",
)

go_repository(
    name = "org_golang_x_sync",
    commit = "112230192c580c3556b8cee6403af37a4fc5f28c",
    importpath = "golang.org/x/sync",
)

go_repository(
    name = "com_google_cloud_go",
    commit = "09ad026a62f0561b7f7e276569eda11a6afc9773",
    importpath = "cloud.google.com/go",
)

go_repository(
    name = "com_github_pborman_uuid",
    commit = "8b1b92947f46224e3b97bb1a3a5b0382be00d31e",
    importpath = "github.com/pborman/uuid",
)

# Needed for the googleapis protos used by com_github_bazelbuild_remote_apis
# below.
http_archive(
    name = "googleapis",
    build_file = "BUILD.googleapis",
    sha256 = "7b6ea252f0b8fb5cd722f45feb83e115b689909bbb6a393a873b6cbad4ceae1d",
    strip_prefix = "googleapis-143084a2624b6591ee1f9d23e7f5241856642f4d",
    urls = ["https://github.com/googleapis/googleapis/archive/143084a2624b6591ee1f9d23e7f5241856642f4d.zip"],
)

go_repository(
    name = "com_github_bazelbuild_remote_apis",
    commit = "c0682f068a6044f395a7e28526abe1de56beffa8",
    importpath = "github.com/bazelbuild/remote-apis",
)
load("@com_github_bazelbuild_remote_apis//:repository_rules.bzl", "switched_rules_by_language")
switched_rules_by_language(
    name = "bazel_remote_apis_imports",
    go = True,
)

go_repository(
    name = "com_github_kylelemons_godebug",
    commit = "9ff306d4fbead574800b66369df5b6144732d58e",
    importpath = "github.com/kylelemons/godebug",
)
