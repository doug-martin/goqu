#!/usr/bin/env bash

set -e
echo "" > coverage.txt

go test -coverprofile=coverage.txt -coverpkg=./... ./...

# See go.test.sh: the SQL Server integration tests are a separate module.
# Their coverage of the root module's packages is appended to the same profile.
(cd integration && go test -coverprofile=integration-coverage.txt -coverpkg=github.com/doug-martin/goqu/v9/... ./...)
tail -n +2 integration/integration-coverage.txt >> coverage.txt
rm -f integration/integration-coverage.txt
