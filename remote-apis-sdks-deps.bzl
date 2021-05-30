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
        tag = "v0.9.1",
    )
    _maybe(
        go_repository,
        name = "com_github_golang_protobuf",
        importpath = "github.com/golang/protobuf",
        tag = "v1.4.2",
    )
    _maybe(
        go_repository,
        name = "com_github_google_go_cmp",
        tag = "v0.5.1",
        importpath = "github.com/google/go-cmp",
    )
    _maybe(
        go_repository,
        name = "org_golang_google_api",
        build_file_proto_mode = "disable",
        importpath = "google.golang.org/api",
        tag = "v0.30.0",
    )
    _maybe(
        go_repository,
        name = "org_golang_google_grpc",
        build_file_proto_mode = "disable",
        importpath = "google.golang.org/grpc",
        tag = "v1.31.0",
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
        commit = "62affa334b73ec65ed44a326519ac12c421905e3",
    )
    _maybe(
        go_repository,
        name = "org_golang_x_oauth2",
        commit = "bf48bf16ab8d622ce64ec6ce98d2c98f916b6303",
        importpath = "golang.org/x/oauth2",
    )
    _maybe(
        go_repository,
        name = "org_golang_x_sync",
        commit = "6e8e738ad208923de99951fe0b48239bfd864f28",
        importpath = "golang.org/x/sync",
    )
    _maybe(
        go_repository,
        name = "org_golang_x_sys",
        importpath = "golang.org/x/sys",
        commit = "be1d3432aa8f4fd677757447c4c9e7ff9bf25f73",
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
        commit = "3b7a4b50d27bb040fc4139bf69b4489377b89136",  # 2021-04-30,
    )
    _maybe(
        go_repository,
        name = "com_google_cloud_go",
        commit = "09ad026a62f0561b7f7e276569eda11a6afc9773",
        tag = "v0.65.0",
        importpath = "cloud.google.com/go",
    )
    _maybe(
        go_repository,
        name = "com_github_golang_snappy",
        importpath = "github.com/golang/snappy",
        tag = "v0.0.3",
    )
    _maybe(
        go_repository,
        name = "com_github_klauspost_compress",
        importpath = "github.com/klauspost/compress",
        tag = "v1.12.3",
    )
    _maybe(
        go_repository,
        name = "com_github_mostynb_zstdpool_syncpool",
        importpath = "github.com/mostynb/zstdpool-syncpool",
        tag = "v0.0.7",
    )
