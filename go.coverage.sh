#!/usr/bin/env bash

set -e
echo "" > coverage.txt

go test -coverprofile=coverage.txt -coverpkg=./... ./...

# See go.test.sh: the SQL Server integration tests are a separate module that needs Go 1.25+.
# Their coverage of the root module's packages is appended to the same profile.
minor=$(go env GOVERSION | sed -E 's/^go1\.([0-9]+).*/\1/')
if [ "$minor" -ge 25 ]; then
  (cd integration && go test -coverprofile=integration-coverage.txt -coverpkg=github.com/doug-martin/goqu/v9/... ./...)
  tail -n +2 integration/integration-coverage.txt >> coverage.txt
  rm -f integration/integration-coverage.txt
else
  echo "skipping ./integration (requires Go 1.25+, running $(go env GOVERSION))"
fi
