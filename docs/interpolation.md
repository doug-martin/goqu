# Prepared Statements

By default the `goqu` will interpolate all parameters, if you do not want to have values interpolated you can use the [`Prepared`](http://godoc.org/github.com/doug-martin/goqu#SelectDataset.Prepared) method to prevent this.

**Note** For the examples all placeholders are `?` this will be dialect specific when using other examples (e.g. Postgres `$1, $2...`)

```go

preparedDs := db.From("items").Prepared(true)

sql, args, _ := preparedDs.Where(goqu.Ex{
	"col1": "a",
	"col2": 1,
	"col3": true,
	"col4": false,
	"col5": []string{"a", "b", "c"},
}).ToSQL()
fmt.Println(sql, args)

sql, args, _ = preparedDs.Insert().Rows(
	goqu.Record{"name": "Test1", "address": "111 Test Addr"},
	goqu.Record{"name": "Test2", "address": "112 Test Addr"},
).ToSQL()
fmt.Println(sql, args)

sql, args, _ = preparedDs.Update().Set(
	goqu.Record{"name": "Test", "address": "111 Test Addr"},
).ToSQL()
fmt.Println(sql, args)

sql, args, _ = preparedDs.
	Delete().
	Where(goqu.Ex{"id": goqu.Op{"gt": 10}}).
	ToSQL()
fmt.Println(sql, args)

// Output:
// SELECT * FROM "items" WHERE (("col1" = ?) AND ("col2" = ?) AND ("col3" IS TRUE) AND ("col4" IS FALSE) AND ("col5" IN (?, ?, ?))) [a 1 a b c]
// INSERT INTO "items" ("address", "name") VALUES (?, ?), (?, ?) [111 Test Addr Test1 112 Test Addr Test2]
// UPDATE "items" SET "address"=?,"name"=? [111 Test Addr Test]
// DELETE FROM "items" WHERE ("id" > ?) [10]
```

When setting prepared to true executing the SQL using the different querying methods will also use the non-interpolated SQL also.

```go
var items []Item
sql, args, _ := db.From("items").Prepared(true).Where(goqu.Ex{
	"col1": "a",
	"col2": 1,
}).ScanStructs(&items)

//Is the same as
db.ScanStructs(&items, `SELECT * FROM "items" WHERE (("col1" = ?) AND ("col2" = ?))`,  "a", 1)
```


## Unsigned integers

Unsigned integer values (`uint`, `uint8`...`uint64`) are supported in both interpolated and prepared queries.

* A value that fits in an `int64` (up to `9223372036854775807`) is written, or passed as an argument, as an `int64`, so the generated SQL and arguments are the same as for the equivalent signed value.
* A `uint64` greater than `math.MaxInt64` is written as its decimal literal when interpolated (e.g. `9223372036854775808`) and passed to the driver as a `uint64` when using `Prepared(true)`.

Whether the database accepts a value that large depends on the driver and the column type. Many databases (for example SQL Server `BIGINT`, PostgreSQL `BIGINT`) only store signed 64-bit integers, so such a value is typically rejected by the database or the driver rather than by `goqu`.

## Other value types

Values of types `goqu` has no built-in mapping for (for example structs, maps, and arrays such as a `uuid.UUID`) behave differently depending on the mode:

* **Interpolated** (the default): `goqu` must write the value into the SQL text, so it returns an encode error unless the type implements [`driver.Valuer`](https://pkg.go.dev/database/sql/driver#Valuer).
* **Prepared** (`Prepared(true)`): the value is passed through as an argument and the driver decides whether it can bind it. If it cannot, the error is returned by the driver when the query is executed rather than by `goqu` when the SQL is built.

Functions, channels and complex numbers can never be bound by a driver, so they result in an encode error in both modes.

## Byte slices

Interpolated `[]byte` values are written as a string literal when they are valid UTF-8, and as a binary (hex) literal otherwise, using the dialect's `BinaryLiteralPrefix` and `BinaryLiteralSuffix` (`X'28fa'` by default, `decode('28fa', 'hex')` for `postgres`, `0x28fa` for `sqlserver`). The `postgres` and `sqlserver` dialects also use a binary literal when the bytes contain a NUL byte, as it cannot be part of a text literal.

**NOTE** Binary data that is valid UTF-8 and has no NUL byte is still written as a string, so a value containing a backslash is read by PostgreSQL as a `bytea` escape sequence. When you store arbitrary binary data in a `bytea` column, use `Prepared(true)` or pass the value as a hex literal yourself.
