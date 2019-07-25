#!/bin/bash

# Copies git hooks from .githooks/ to .git/hooks/, overriding any hooks you may
# have previously had there.
cp .githooks/* .git/hooks/
for h in `ls .githooks/* | xargs basename`; do
  chmod ug+x .git/hooks/$h
done
