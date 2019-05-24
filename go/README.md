# Go SDK

## Usage

TODO(jsharpe): Write usage instructions.

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
