# GRPC Balancer
This package is a forked version of https://github.com/GoogleCloudPlatform/grpc-gcp-go.

We use this primarily to create new sub-connections when we reach
maximum number of streams (100 for GFE) on a given connection.

Refer to https://github.com/grpc/grpc/issues/21386 for status on the long-term fix
for this issue.