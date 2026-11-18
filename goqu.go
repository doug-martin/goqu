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
	"reflect"
	"time"

	"github.com/doug-martin/goqu/v9/internal/util"
	"github.com/doug-martin/goqu/v9/sqlgen"
)

type DialectWrapper struct {
	dialect string
}

// Creates a new DialectWrapper to create goqu.Datasets or goqu.Databases with the specified dialect.
func Dialect(dialect string) DialectWrapper {
	return DialectWrapper{dialect: dialect}
}

// Create a new dataset for creating SELECT sql statements
func (dw DialectWrapper) From(table ...interface{}) *SelectDataset {
	return From(table...).WithDialect(dw.dialect)
}

// Create a new dataset for creating SELECT sql statements
func (dw DialectWrapper) Select(cols ...interface{}) *SelectDataset {
	return newDataset(dw.dialect, nil).Select(cols...)
}

// Create a new dataset for creating UPDATE sql statements
func (dw DialectWrapper) Update(table interface{}) *UpdateDataset {
	return Update(table).WithDialect(dw.dialect)
}

// Create a new dataset for creating INSERT sql statements
func (dw DialectWrapper) Insert(table interface{}) *InsertDataset {
	return Insert(table).WithDialect(dw.dialect)
}

// Create a new dataset for creating DELETE sql statements
func (dw DialectWrapper) Delete(table interface{}) *DeleteDataset {
	return Delete(table).WithDialect(dw.dialect)
}

// Create a new dataset for creating TRUNCATE sql statements
func (dw DialectWrapper) Truncate(table ...interface{}) *TruncateDataset {
	return Truncate(table...).WithDialect(dw.dialect)
}

func (dw DialectWrapper) DB(db SQLDatabase) *Database {
	return newDatabase(dw.dialect, db)
}

func New(dialect string, db SQLDatabase) *Database {
	return newDatabase(dialect, db)
}

// Set the behavior when encountering struct fields that do not have a db tag.
// By default this is false; if set to true any field without a db tag will not
// be targeted by Select or Scan operations.
func SetIgnoreUntaggedFields(ignore bool) {
	util.SetIgnoreUntaggedFields(ignore)
}

// Set the column rename function. This is used for struct fields that do not have a db tag to specify the column name
// By default all struct fields that do not have a db tag will be converted lowercase
func SetColumnRenameFunction(renameFunc func(string) string) {
	util.SetColumnRenameFunction(renameFunc)
}

// Marks a type as a single value, so a struct field of that type is one column instead of being flattened into one
// column per field when a struct is used for inserts, updates and scans. Use it for struct types that are not a
// sql.Scanner, such as pgx types that only implement pgtype.BinaryEncoder/BinaryDecoder, or a type the driver
// encodes itself. Pointers to the type are covered too, and the goqu:"opaque" struct tag does the same for a single
// field.
//
// Prepared statements pass the value to the driver as is. Interpolated statements use the value's driver.Valuer, and
// return an encode error for a type that does not implement it. Register types once, at start up.
func RegisterOpaqueType(t reflect.Type) {
	util.RegisterOpaqueType(t)
}

// Generic form of RegisterOpaqueType: goqu.RegisterOpaque[pgtype.Text]()
func RegisterOpaque[T any]() {
	util.RegisterOpaqueType(reflect.TypeFor[T]())
}

// Set the location to use when interpolating time.Time instances. See https://golang.org/pkg/time/#LoadLocation
// NOTE: This has no effect when using prepared statements.
func SetTimeLocation(loc *time.Location) {
	sqlgen.SetTimeLocation(loc)
}
