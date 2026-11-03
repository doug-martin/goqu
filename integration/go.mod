module github.com/doug-martin/goqu/v9/integration

go 1.26.0

require (
	github.com/doug-martin/goqu/v9 v9.19.0
	github.com/microsoft/go-mssqldb v1.11.2
	github.com/stretchr/testify v1.12.1
)

require (
	github.com/golang-sql/civil v0.0.0-20220223132316-b832511892a9 // indirect
	github.com/golang-sql/sqlexp v0.1.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	go.yaml.in/yaml/v3 v3.0.6 // indirect
	golang.org/x/crypto v0.55.0 // indirect
	golang.org/x/text v0.41.0 // indirect
)

replace github.com/doug-martin/goqu/v9 => ../

godebug x509negativeserial=1
