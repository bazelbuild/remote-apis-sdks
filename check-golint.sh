#!/bin/bash

linterrs=$(golint -min_confidence 1.0 ./...)
[ -z "$linterrs" ] && exit 0

# Some files are not golint'd. Print message and fail.

echo >&2 "Please fix the following lint errors:"
echo >&2 "$linterrs"

exit 1
