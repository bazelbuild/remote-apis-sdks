# Bazel Creds Interface Mapper

## Usage
`bazelisk run //cmd/bazelcredswrapper -- --credentials_helper_path=[Path to the user's bazel-style credshelper] --uri=[URI of the credentials request]`

This will run the user provided bazel-style credentials helper and output credentials in a format support by the `remote-apis-sdks`. For usage with `remote-apis-sdks`, we first need to build the `bazelcredswrapper` binary along with the sdks. Then whereever you set your flags to configure the sdks, you also set the following flags:

`--credentials_helper=path/to/remote-apis-sdks/bazel-bin/go/cmd/bazelcredswrapper/bazelcredswrapper_/bazelcredswrapper`

`--credentials_helper_args=--credentials_helper_path=[Path to user's bazel-style credshelper] --uri=[URI of the credentials request]`
