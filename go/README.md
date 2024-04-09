# Go SDK

## Usage
TODO(jsharpe): Write usage instructions.

### remotetool

`remotetool` is a CLI tool to interact with remote build execution. In particular, blobs and input trees can be downloaded and uploaded from CAS, actions and their inputs can be downloaded to be either re-run locally or remotely.

#### Common flags
Flags that describe the RBE instance used and the authentication method are common to most `remotetool` invocations. The `--instance`, `--service`, and credential flags (`--use_application_default_credentials`, `--use_gce_credentials`, `--credential_file=FILE`, one of which must be set), in particular, are further documented under [pkg/flags/flags.go](pkg/flags/flags.go).

`--alsologtostderr` and `-v VERBOSITY_LEVEL` can be used to tune the verbosity and location of the output, e.g. `--alsologtosrderr -v 1`.

#### Downloading an action's inputs and metadata
```
bazelisk run //go/cmd/remotetool -- \
  --operation=download_action \
  COMMON_FLAGS \
  --digest=DIGEST \
  --path=PATH
```
- For `COMMON_FLAGS`, see the [above section](#common-flags)
- The `--digest` flag is formatted as `"{hash}/{size}"` and can typically be obtained from logs of the tool that originally ran the action.
- The `--path` flag is the destination where the action inputs will be downloaded (under `PATH/input`), as well as:
  - metadata relating to the action (e.g. command hash, input tree hash, individual input file hashes...)
  - `run_command.sh`, a script that contains the command line of the action to be re-run
  - `run_locally.sh`, a script that contains the docker invocation to re-run the action locally

The action can then be re-run locally by running `./run_locally.sh`, or remotely using the instructions below. When running the action locally, local modifications made to the `input` folder will be picked up automatically, but not if re-running the action remotely, read on for more on this.

#### Re-running a downloaded action remotely

```
bazelisk run //go/cmd/remotetool -- \
  --operation=execute_action \
  COMMON_FLAGS
  [--action_root=PATH|--digest=DIGEST] \
  --path OUTPUT_PATH
```
- For `COMMON_FLAGS`, see the [above section](#common-flags).
- An action digest formatted as `"{hash}/{size}"` can be provided directly using the `--digest` flag, or a previously downloaded action can be used with `--action_root`. The `--action_root` path should point to the `--path` of a previous `download_action` invocation. Specifically, the `--action_root` folder must contain a `cmd.textproto` and `ac.textproto` files.
- `--path` is the destination where the outputs of the action will be downloaded

#### Running a modified version of a downloaded action

1. Download an action to a given `PATH` following the [instructions for downloading an action](#downloading-an-actions-inputs-and-metadata),
1. modify the input under `PATH/input` as needed,
1. run the action and be sure to include the `--action_root=PATH` argument.

Alternatively, you could also:
1. upload the newly formed input directory using `remotetool`:
   ```
   bazelisk run //go/cmd/remotetool -- \
     --operation=upload_dir \
     COMMON_FLAGS \
     --path=PATH/input
   ```
   `remotetool` will log the digest of the newly uploaded input folder, `NEW_DIGEST`.
1. In the `ac.textproto` file under `PATH`, update the `input_root_digest` fields with the `NEW_DIGEST` information.
1. Run the action using the [instructions for re-running a downloaded action](#re-running-a-downloaded-action-remotely)

## Development

Update `BUILD.bazel` files with (with `fix` to be more aggressive):

```
bazel run //:gazelle [fix]
```

Update `go.mod` files with:

```
go mod tidy
```

Format all files with:

```
gofmt -w go
```

Check your code for lint/vet warnings (install `golint` with `sudo apt-get install golint`):

```
golint ./...
go vet ./...
```

Run all tests:

```
bazel test ...
```

or

```
go test ./...
```
