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
#  * For more details: https://github.com/bazelbuild/bazel-gazelle#update-repos
#  * Invoke with:
#    bazel run //:gazelle -- update-repos example.com/new/repo -to_macro remote-apis-sdks-deps.bzl%remote_apis_sdks_go_deps
load ("//:remote-apis-sdks-deps.bzl", "remote_apis_sdks_go_deps")
remote_apis_sdks_go_deps()

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
