# Struct Tags

`goqu` reads two struct tags when mapping structs to and from columns.

* `db:"column_name"` - the column name for the field. Use `db:"-"` to ignore the field entirely.
* `goqu:"opt1,opt2"` - comma separated options that change how the field is used. The options are listed below.

```go
type User struct {
	ID        int64   `db:"id" goqu:"skipinsert,skipupdate"`
	FirstName string  `db:"first_name"`
	Address   *string `db:"address" goqu:"omitnil"`
	Notes     string  `db:"notes" goqu:"defaultifempty"`
}
```

## `goqu` options

| Option           | Applies to    | Effect |
|------------------|---------------|--------|
| `skipinsert`     | INSERT        | The field is not included in `INSERT` statements. See [Inserting](./inserting.md#insert-structs). |
| `skipupdate`     | UPDATE        | The field is not included in `UPDATE` statements. See [Updating](./updating.md). |
| `skipselect`     | SELECT        | The field is not added to the columns selected for the struct (`Select(&s)` and `ScanStruct`/`ScanStructs` without an explicit `Select`). The column is still scanned when it is selected, e.g. a column that comes from a join. |
| `defaultifempty` | INSERT/UPDATE | When the field is the zero value, `DEFAULT` is used instead of the value (`sqlite3` does not support `DEFAULT` in `UPDATE ... SET`). See [Inserting](./inserting.md) and [Updating](./updating.md). |
| `omitnil`        | INSERT/UPDATE | The field is skipped when it is a nil pointer. Useful for partial updates. |
| `omitempty`      | INSERT/UPDATE | The field is skipped when it is the zero value (including nil pointers). |
| `opaque`         | all           | The field is one column and is never flattened into the columns of its struct type. See [Opaque types](#opaque-types). |

Options can be combined, e.g. `goqu:"skipinsert,skipupdate"`.

`skipinsert`, `skipupdate` and `skipselect` on a nested struct field apply to every column flattened out of that struct (e.g. `Bar Bar `goqu:"skipinsert"`` skips `"bar"."b"` on insert).

## Opaque types

A struct-typed field is flattened into one column per field (`Address Address` becomes `"address"."street"`, ...)
unless its type implements `sql.Scanner`. A type that is meant to be a single value but is not a `sql.Scanner`
(a driver specific type such as a pgx type that only implements `pgtype.BinaryEncoder`/`BinaryDecoder`, or a struct that
is stored as JSON) can be marked as opaque, so it is one column. The tag also works on a slice field (e.g. `[]string`
for a JSON column) to send it as one value instead of a list.

For one field, use the `opaque` option (it also works on an embedded field of an exported type):

```go
type Event struct {
	ID      int
	Payload Payload `db:"payload" goqu:"opaque"`
}
```

For a type used in many structs, register it once at start up. Pointers to the type are covered too:

```go
goqu.RegisterOpaque[Payload]()
// or, with a reflect.Type:
goqu.RegisterOpaqueType(reflect.TypeOf(Payload{}))
```

How the value is sent:

* **Prepared statements** pass the value to `database/sql` as one argument, unchanged: a slice is not expanded into a
  list and a `driver.Valuer` is not unwrapped. `database/sql` only accepts it if
  the driver can handle the type (a driver that implements `driver.NamedValueChecker`, or a type that is a
  `driver.Valuer`); otherwise the call fails with an unsupported type error from the driver.
* **Interpolated statements** use the value's `driver.Valuer` when it has one, and return an encode error otherwise (a
  native binary encoding cannot be written as a SQL literal).
* **Scanning** goes through `database/sql`: the column is scanned into a value of the field's type and copied onto the
  struct, so the driver must return a value assignable to that type (as a driver that decodes its own types does), or
  the type must implement `sql.Scanner`. A driver that returns plain `driver.Value`s (strings, bytes, ...) does not
  call a type's own binary decoder, such as pgx's `BinaryDecoder`; for those, wrap the type with a `sql.Scanner`/
  `driver.Valuer` or use the driver's native API.

Notes:

* A type that implements `driver.Valuer` but not `sql.Scanner` is still flattened by default. Mark it opaque to write it
  with its `Value()`.
* Registering a type resets goqu's cached struct column maps, so it also applies to structs that were already used. Register
  types once at start up rather than while queries are running.
* `opaque` on an embedded field of an unexported type leaves the field out, because its value cannot be read.
* Only one pointer level is unwrapped (`T` and `*T`, not `**T`), like `sql.Scanner` detection.
* The struct you pass as a row (`Rows(row)`, `Set(row)`, `ScanStruct(&row)`) is always expanded into its columns; opaque
  only applies to the fields inside it.

## Notes

* Tags are read from exported fields, including fields of embedded structs.
* `skipinsert`, `skipupdate` and `skipselect` change which columns are written or selected; scanning always uses the `db` tag. Only the `SELECT` columns are affected: `INSERT` columns (`Cols(&s)`), `GROUP BY`, `DISTINCT ON` and `RETURNING` still expand to every column. If every column of a struct has `skipselect`, `Select(&s)` returns an error (an empty list would select `*`). To select a field that has `skipselect`, add its column next to the struct: `Select(&user, goqu.I("groups.name").As("group_name"))`.
* The `db` tag also controls scanning, so fields without a matching column in the result set cause an error. Select the columns your struct needs, or use a struct that matches the query.
* On `UPDATE`, `defaultifempty` only writes `DEFAULT` when the Go field holds its zero value. If you reuse a struct populated by `ScanStruct` (where e.g. `ModifiedAt` already holds the previous timestamp), zero that field before `Set(row)`, pass `goqu.Default()` in a `goqu.Record`, or manage the column in the database (`ON UPDATE CURRENT_TIMESTAMP` or a trigger) with `goqu:"skipinsert,skipupdate"`.
