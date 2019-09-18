package goqu_test

import (
	"fmt"

	"github.com/doug-martin/goqu/v9"
)

func ExampleRegisterDialect() {
	opts := goqu.DefaultDialectOptions()
	opts.QuoteRune = '`'
	goqu.RegisterDialect("custom-dialect", opts)

	dialect := goqu.Dialect("custom-dialect")

	ds := dialect.From("test")

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM `test` []
}
