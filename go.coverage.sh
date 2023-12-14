#!/usr/bin/env bash

set -e
echo "" > coverage.txt

go test -coverprofile=coverage.txt -coverpkg=./... ./...