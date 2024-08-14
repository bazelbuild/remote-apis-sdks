workspace(name = "bazel_remote_apis_sdks")

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "io_bazel_rules_go",
    sha256 = "278b7ff5a826f3dc10f04feaf0b70d48b68748ccd512d7f98bf442077f043fe3",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_go/releases/download/v0.41.0/rules_go-v0.41.0.zip",
        "https://github.com/bazelbuild/rules_go/releases/download/v0.41.0/rules_go-v0.41.0.zip",
    ],
)

http_archive(
    name = "bazel_gazelle",
    sha256 = "29218f8e0cebe583643cbf93cae6f971be8a2484cdcfa1e45057658df8d54002",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-gazelle/releases/download/v0.32.0/bazel-gazelle-v0.32.0.tar.gz",
        "https://github.com/bazelbuild/bazel-gazelle/releases/download/v0.32.0/bazel-gazelle-v0.32.0.tar.gz",
    ],
)

load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies", "go_repository")
load("@io_bazel_rules_go//go:deps.bzl", "go_register_toolchains", "go_rules_dependencies")

go_rules_dependencies()

go_register_toolchains(version = "1.20.7")

# Need "build_file_proto_mode" argument.
go_repository(
    name = "org_golang_google_grpc",
    importpath = "google.golang.org/grpc",
    sum = "h1:KH3VH9y/MgNQg1dE7b3XfVK0GsPSIzJwdF617gUSbvY=",
    version = "v1.64.0",
)

# Need "build_file_proto_mode" argument.
go_repository(
    name = "org_golang_google_api",
    importpath = "google.golang.org/api",
    sum = "h1:PNMeRDwo1pJdgNcFQ9GstuLe/noWKIc89pRWRLMvLwE=",
    version = "v0.183.0",
)

# Insert go_repository rules before this one to override specific deps.
gazelle_dependencies()

# Insert oauth2 before remote_apis_sdks_deps() to override version
go_repository(
    name = "org_golang_x_oauth2",
    importpath = "golang.org/x/oauth2",
    sum = "h1:tsimM75w1tF/uws5rbeHzIWxEqElMehnc+iW793zsZs=",
    version = "v0.21.0",
)

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
    sha256 = "89de2bb5c5a1e2ff1a1791de19686d54507e971b849efe49f7bdd188521062c5",
    strip_prefix = "googleapis-46bc6f2d612c42644f061b4c22eedd762cd72909",
    urls = ["https://github.com/googleapis/googleapis/archive/46bc6f2d612c42644f061b4c22eedd762cd72909.zip"],
)

load("@googleapis//:repository_rules.bzl", "switched_rules_by_language")

switched_rules_by_language(
    name = "com_google_googleapis_imports",
)

go_repository(
    name = "com_github_bazelbuild_remote_apis",
    importpath = "github.com/bazelbuild/remote-apis",
    replace = "github.com/bentekkie/remote-apis",
    sum = "h1:HQRzfdwOBeFbSK0vO8ZfSaqLhKKOJDOMF62bFfggwJ8=",
    version = "v0.0.0-20240605144600-b36ef32b88ff",
)

load("@com_github_bazelbuild_remote_apis//:remote_apis_deps.bzl", "remote_apis_go_deps")

remote_apis_go_deps()
