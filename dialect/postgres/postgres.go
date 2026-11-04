package postgres

import (
	"github.com/doug-martin/goqu/v9"
)

func DialectOptions() *goqu.SQLDialectOptions {
	do := goqu.DefaultDialectOptions()
	do.PlaceHolderFragment = []byte("$")
	do.IncludePlaceholderNum = true
	do.QuoteCollation = true
	do.BinaryLiteralPrefix = []byte("decode('")
	do.BinaryLiteralSuffix = []byte("', 'hex')")
	do.BinaryLiteralBytes = []byte{0}
	return do
}

func init() {
	goqu.RegisterDialect("postgres", DialectOptions())
}
