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
| `defaultifempty` | INSERT        | When the field is the zero value, `DEFAULT` is used instead of the value. See [Inserting](./inserting.md). |
| `omitnil`        | INSERT/UPDATE | The field is skipped when it is a nil pointer. Useful for partial updates. |
| `omitempty`      | INSERT/UPDATE | The field is skipped when it is the zero value (including nil pointers). |

Options can be combined, e.g. `goqu:"skipinsert,skipupdate"`.

## Notes

* Tags are read from exported fields, including fields of embedded structs.
* Struct tags only affect `INSERT` and `UPDATE`. `SELECT` columns and `ScanStruct`/`ScanStructs` use the `db` tag. To control which columns are selected, pass them explicitly with `Select(...)`.
* The `db` tag also controls scanning, so fields without a matching column in the result set cause an error. Select the columns your struct needs, or use a struct that matches the query.
