#!/usr/bin/env bash

set -e
echo "" > coverage.txt

go test -v -race ./...

# The SQL Server integration tests live in their own module so the root go.mod does not
# require a SQL Server driver (and its golang.org/x/crypto dependency).
(cd integration && go test -v -race ./...)
