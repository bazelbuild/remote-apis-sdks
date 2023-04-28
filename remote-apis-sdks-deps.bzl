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
        name = "org_golang_google_protobuf",
        importpath = "google.golang.org/protobuf",
        tag = "v1.27.1",
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
        tag = "v1.52.3",
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
        name = "com_github_pborman_uuid",
        importpath = "github.com/pborman/uuid",
        tag = "v1.2.0",
    )
    _maybe(
        go_repository,
        name = "com_github_bazelbuild_remote_apis",
        importpath = "github.com/bazelbuild/remote-apis",
        commit = "35aee1c4a4250d3df846f7ba3e4a4e66cb014ecd",  # 2023-04-11
    )
    _maybe(
        go_repository,
        name = "com_google_cloud_go_compute_metadata",
        importpath = "cloud.google.com/go/compute/metadata",
        sum = "h1:efOwf5ymceDhK6PKMnnrTHP4pppY5L22mle96M1yP48=",
        version = "v0.2.1",
    )
    _maybe(
        go_repository,
        name = "com_google_cloud_go_compute",
        version = "v0.1.0",
        sum = "h1:rSUBvAyVwNJ5uQCKNJFMwPtTvJkfN38b6Pvb9zZoqJ8=",
        importpath = "cloud.google.com/go/compute",
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
    _maybe(
        go_repository,
        name = "com_github_pkg_xattr",
        importpath = "github.com/pkg/xattr",
        tag = "v0.4.4",
    )
