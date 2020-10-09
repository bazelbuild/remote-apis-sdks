"""Load dependencies needed to depend on the remote-apis-sdks repo."""

load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies", "go_repository")

def _maybe(repo_rule, name, **kwargs):
    if name not in native.existing_rules():
        repo_rule(name = name, **kwargs)

def remote_apis_sdks_go_deps():
    """Load dependencies needed to depend on the Go Remote Execution SDK."""
    _maybe(
        go_repository,
        name = "com_github_pkg_errors",
        importpath = "github.com/pkg/errors",
        tag = "v0.8.1",
    )
    _maybe(
        go_repository,
        name = "com_github_golang_protobuf",
        importpath = "github.com/golang/protobuf",
        tag = "v1.3.2",
    )
    _maybe(
        go_repository,
        name = "com_github_google_go_cmp",
        tag = "v0.3.1",
        importpath = "github.com/google/go-cmp",
    )
    _maybe(
        go_repository,
        name = "org_golang_google_grpc",
        build_file_proto_mode = "disable",
        importpath = "google.golang.org/grpc",
        tag = "v1.30.0",
    )
    _maybe(
        go_repository,
        name = "org_golang_x_oauth2",
        commit = "9f3314589c9a9136388751d9adae6b0ed400978a",
        importpath = "golang.org/x/oauth2",
    )
    _maybe(
        go_repository,
        name = "com_github_golang_glog",
        commit = "23def4e6c14b4da8ac2ed8007337bc5eb5007998",
        importpath = "github.com/golang/glog",
    )
    _maybe(
        go_repository,
        name = "com_github_google_uuid",
        build_file_generation = "on",
        importpath = "github.com/google/uuid",
        tag = "v1.1.1",
    )
    _maybe(
        go_repository,
        name = "org_golang_x_net",
        importpath = "golang.org/x/net",
        commit = "57efc9c3d9f91fb3277f8da1cff370539c4d3dc5",
    )
    _maybe(
        go_repository,
        name = "org_golang_x_sync",
        commit = "112230192c580c3556b8cee6403af37a4fc5f28c",
        importpath = "golang.org/x/sync",
    )
    _maybe(
        go_repository,
        name = "org_golang_x_text",
        importpath = "golang.org/x/text",
        commit = "a9a820217f98f7c8a207ec1e45a874e1fe12c478",
    )
    _maybe(
        go_repository,
        name = "org_golang_x_sys",
        importpath = "golang.org/x/sys",
        commit = "acbc56fc7007d2a01796d5bde54f39e3b3e95945",
    )
    _maybe(
        go_repository,
        name = "com_github_pborman_uuid",
        importpath = "github.com/pborman/uuid",
        tag = "v1.2.0",
    )
    _maybe(
        go_repository,
        name = "com_github_bazelbuild_remote_apis",
        importpath = "github.com/bazelbuild/remote-apis",
        tag = "v2.0.0",
    )
    _maybe(
        go_repository,
        name = "com_github_kylelemons_godebug",
        commit = "9ff306d4fbead574800b66369df5b6144732d58e",
        importpath = "github.com/kylelemons/godebug",
    )
    _maybe(
        go_repository,
        name = "com_google_cloud_go",
        commit = "09ad026a62f0561b7f7e276569eda11a6afc9773",
        importpath = "cloud.google.com/go",
    )
