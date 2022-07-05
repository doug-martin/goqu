package postgres

import (
	"github.com/doug-martin/goqu/v9"
)

const DialectName = "postgres"

func DialectOptions() *goqu.SQLDialectOptions {
	do := goqu.DefaultDialectOptions()
	do.PlaceHolderFragment = []byte("$")
	do.IncludePlaceholderNum = true
	return do
}

func init() {
	goqu.RegisterDialect(DialectName, DialectOptions())
}
