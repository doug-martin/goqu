#!/usr/bin/env bash

set -e
echo "" > coverage.txt

go test -v -race ./...

# The SQL Server integration tests live in their own module so the root go.mod does not
# require a SQL Server driver (and its golang.org/x/crypto dependency). That module needs
# a newer Go toolchain than the root module supports, so it is skipped on older ones.
minor=$(go env GOVERSION | sed -E 's/^go1\.([0-9]+).*/\1/')
if [ "$minor" -ge 25 ]; then
  (cd integration && go test -v -race ./...)
else
  echo "skipping ./integration (requires Go 1.25+, running $(go env GOVERSION))"
fi
