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
| `defaultifempty` | INSERT        | When the field is the zero value, `DEFAULT` is used instead of the value. See [Inserting](./inserting.md). |
| `omitnil`        | INSERT/UPDATE | The field is skipped when it is a nil pointer. Useful for partial updates. |
| `omitempty`      | INSERT/UPDATE | The field is skipped when it is the zero value (including nil pointers). |

Options can be combined, e.g. `goqu:"skipinsert,skipupdate"`.

`skipinsert`, `skipupdate` and `skipselect` on a nested struct field apply to every column flattened out of that struct (e.g. `Bar Bar `goqu:"skipinsert"`` skips `"bar"."b"` on insert).

## Notes

* Tags are read from exported fields, including fields of embedded structs.
* `skipinsert`, `skipupdate` and `skipselect` change which columns are written or selected; scanning always uses the `db` tag. Only the `SELECT` columns are affected: `INSERT` columns (`Cols(&s)`), `GROUP BY`, `DISTINCT ON` and `RETURNING` still expand to every column. If every column of a struct has `skipselect`, `Select(&s)` returns an error (an empty list would select `*`). To select a field that has `skipselect`, add its column next to the struct: `Select(&user, goqu.I("groups.name").As("group_name"))`.
* The `db` tag also controls scanning, so fields without a matching column in the result set cause an error. Select the columns your struct needs, or use a struct that matches the query.
