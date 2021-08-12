#!/bin/bash

cd $(dirname $0)

set -eux

tmp=$(mktemp "tmp.XXXXXX")

dd if=/dev/random of=$tmp bs=16M count=8
time go run ./go/cmd/remotetool -alsologtostderr -v 9 -service 'remotebuildexecution.googleapis.com:443' \
   --operation upload_blob -use_application_default_credentials \
   --instance=projects/chromium-swarm/instances/default_instance \
   --path ./$tmp

# time isolated archive -isolate-server isolateserver.appspot.com -files .:$tmp

rm $tmp
