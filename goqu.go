/*
goqu an idiomatch SQL builder, and query package.

      __ _  ___   __ _ _   _
     / _` |/ _ \ / _` | | | |
    | (_| | (_) | (_| | |_| |
     \__, |\___/ \__, |\__,_|
     |___/          |_|


Please see https://github.com/doug-martin/goqu for an introduction to goqu.
*/
package goqu

import (
	"github.com/doug-martin/goqu/v7/internal/util"
)

type DialectWrapper struct {
	dialect string
}

// Creates a new DialectWrapper to create goqu.Datasets or goqu.Databases with the specified dialect.
func Dialect(dialect string) DialectWrapper {
	return DialectWrapper{dialect: dialect}
}

func (dw DialectWrapper) From(table ...interface{}) *Dataset {
	return From(table...).WithDialect(dw.dialect)
}

func (dw DialectWrapper) DB(db SQLDatabase) *Database {
	return newDatabase(dw.dialect, db)
}

func New(dialect string, db SQLDatabase) *Database {
	return newDatabase(dialect, db)
}

// Set the column rename function. This is used for struct fields that do not have a db tag to specify the column name
// By default all struct fields that do not have a db tag will be converted lowercase
func SetColumnRenameFunction(renameFunc func(string) string) {
	util.SetColumnRenameFunction(renameFunc)
}
