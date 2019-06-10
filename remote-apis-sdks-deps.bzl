"""Load dependencies needed to depend on the remote-apis-sdks repo."""

# TODO(foox): Unsure if we need to load the bazel rules here, or if the load() in the WORKSPACE is sufficient.
#http_archive(
#    name = "io_bazel_rules_go",
#    sha256 = "86ae934bd4c43b99893fc64be9d9fc684b81461581df7ea8fc291c816f5ee8c5",
#    urls = ["https://github.com/bazelbuild/rules_go/releases/download/0.18.3/rules_go-0.18.3.tar.gz"],
#)
load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies", "go_repository")
#gazelle_dependencies()

def remote_apis_sdks_go_deps():
  """Load dependencies needed to depend on the Go Remote Execution SDK."""
  if "com_github_pkg_errors" not in native.existing_rules():
    go_repository(
        name = "com_github_pkg_errors",
        commit = "27936f6d90f9c8e1145f11ed52ffffbfdb9e0af7",
        importpath = "github.com/pkg/errors",
    )
  if "com_github_golang_protobuf" not in native.existing_rules():
    go_repository(
        name = "com_github_golang_protobuf",
        commit = "e91709a02e0e8ff8b86b7aa913fdc9ae9498e825",
        importpath = "github.com/golang/protobuf",
    )
  if "com_github_google_go_cmp" not in native.existing_rules():
    go_repository(
        name = "com_github_google_go_cmp",
        commit = "6f77996f0c42f7b84e5a2b252227263f93432e9b",
        importpath = "github.com/google/go-cmp",
    )
  if "org_golang_google_grpc" not in native.existing_rules():
    go_repository(
        name = "org_golang_google_grpc",
        commit = "a8b5bd3c39ac82177c7bad36e1dd695096cd0ef5",
        importpath = "google.golang.org/grpc",
    )
  if "org_golang_x_oauth2" not in native.existing_rules():
    go_repository(
        name = "org_golang_x_oauth2",
        commit = "9f3314589c9a9136388751d9adae6b0ed400978a",
        importpath = "golang.org/x/oauth2",
    )
  if "com_github_golang_glog" not in native.existing_rules():
    go_repository(
        name = "com_github_golang_glog",
        commit = "23def4e6c14b4da8ac2ed8007337bc5eb5007998",
        importpath = "github.com/golang/glog",
    )
  if "com_github_google_uuid" not in native.existing_rules():
    go_repository(
        name = "com_github_google_uuid",
        build_file_generation = "on",
        commit = "c2e93f3ae59f2904160ceaab466009f965df46d6",
        importpath = "github.com/google/uuid",
    )
  if "org_golang_x_sync" not in native.existing_rules():
    go_repository(
        name = "org_golang_x_sync",
        commit = "112230192c580c3556b8cee6403af37a4fc5f28c",
        importpath = "golang.org/x/sync",
    )
  if "com_google_cloud_go" not in native.existing_rules():
    go_repository(
        name = "com_google_cloud_go",
        commit = "09ad026a62f0561b7f7e276569eda11a6afc9773",
        importpath = "cloud.google.com/go",
    )
  if "com_github_pborman_uuid" not in native.existing_rules():
    go_repository(
        name = "com_github_pborman_uuid",
        commit = "8b1b92947f46224e3b97bb1a3a5b0382be00d31e",
        importpath = "github.com/pborman/uuid",
    )


