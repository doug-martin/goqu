# Dialect

Dialects allow goqu the build the correct SQL for each database. There are four dialects that come packaged with `goqu`

* [mysql](../dialect/mysql/mysql.go) - `import _ "github.com/doug-martin/goqu/v9/dialect/mysql"`
* [postgres](../dialect/postgres/postgres.go) - `import _ "github.com/doug-martin/goqu/v9/dialect/postgres"`
* [sqlite3](../dialect/sqlite3/sqlite3.go) - `import _ "github.com/doug-martin/goqu/v9/dialect/sqlite3"`
* [sqlserver](../dialect/sqlserver/sqlserver.go) - `import _ "github.com/doug-martin/goqu/v9/dialect/sqlserver"`

**NOTE** Dialects work like drivers in go where they are not registered until you import the package.

`goqu.Dialect` falls back to the `default` dialect if the name is not registered, for example when the import is
missing. To fail instead, use `goqu.RequireDialect` (returns an error) or `goqu.MustDialect` (panics):

```go
db := goqu.MustDialect("postgres")
```

To use one dialect for the package level `goqu.From`, `goqu.Select`, `goqu.Insert`, `goqu.Update`, `goqu.Delete` and
`goqu.Truncate` functions, call `goqu.SetDefaultDialect` once at startup, after importing the dialect. It panics if the
dialect is not registered. Datasets that choose a dialect explicitly (`goqu.Dialect`, `WithDialect`, `goqu.New`) are
not affected. Use `goqu.SetDefaultPrepared(true)` to make prepared statements the default.

**NOTE** A dataset keeps the dialect it was created with. Call `SetDefaultDialect` before creating any datasets, and
do not rely on package level variables such as `var q = goqu.From("t")`, which are initialized before `init()` runs.

```go
import _ "github.com/doug-martin/goqu/v9/dialect/postgres"

func init() {
	goqu.SetDefaultDialect("postgres")
	goqu.SetDefaultPrepared(true)
}
```

Below are examples for each dialect. Notice how the dialect is imported and then looked up using `goqu.Dialect`

<a name="postgres"></a>
### Postgres
```go
import (
  "fmt"
  "github.com/doug-martin/goqu/v9"
  // import the dialect
  _ "github.com/doug-martin/goqu/v9/dialect/postgres"
)

// look up the dialect
dialect := goqu.Dialect("postgres")

// use dialect.From to get a dataset to build your SQL
ds := dialect.From("test").Where(goqu.Ex{"id": 10})
sql, args, err := ds.ToSQL()
if err != nil{
  fmt.Println("An error occurred while generating the SQL", err.Error())
}else{
  fmt.Println(sql, args)
}
```

Output:
```
SELECT * FROM "test" WHERE "id" = 10 []
```

<a name="mysql"></a>
### MySQL
```go
import (
  "fmt"
  "github.com/doug-martin/goqu/v9"
  // import the dialect
  _ "github.com/doug-martin/goqu/v9/dialect/mysql"
)

// look up the dialect
dialect := goqu.Dialect("mysql")

// use dialect.From to get a dataset to build your SQL
ds := dialect.From("test").Where(goqu.Ex{"id": 10})
sql, args, err := ds.ToSQL()
if err != nil{
  fmt.Println("An error occurred while generating the SQL", err.Error())
}else{
  fmt.Println(sql, args)
}
```

Output:
```
SELECT * FROM `test` WHERE `id` = 10 []
```

<a name="sqlite3"></a>
### SQLite3
```go
import (
  "fmt"
  "github.com/doug-martin/goqu/v9"
  // import the dialect
  _ "github.com/doug-martin/goqu/v9/dialect/sqlite3"
)

// look up the dialect
dialect := goqu.Dialect("sqlite3")

// use dialect.From to get a dataset to build your SQL
ds := dialect.From("test").Where(goqu.Ex{"id": 10})
sql, args, err := ds.ToSQL()
if err != nil{
  fmt.Println("An error occurred while generating the SQL", err.Error())
}else{
  fmt.Println(sql, args)
}
```

Output:
```
SELECT * FROM `test` WHERE `id` = 10 []
```

<a name="sqlserver"></a>
### SQLServer
```go
import (
  "fmt"
  "github.com/doug-martin/goqu/v9"
  // import the dialect
  _ "github.com/doug-martin/goqu/v9/dialect/sqlserver"
)

// look up the dialect
dialect := goqu.Dialect("sqlserver")

// use dialect.From to get a dataset to build your SQL
ds := dialect.From("test").Where(goqu.Ex{"id": 10})
sql, args, err := ds.ToSQL()
if err != nil{
  fmt.Println("An error occurred while generating the SQL", err.Error())
}else{
  fmt.Println(sql, args)
}
```

Output:
```
SELECT * FROM "test" WHERE "id" = 10 []
```

### Subqueries, CTEs and compound queries

A statement is always rendered with a single dialect. Subqueries, CTEs, compound queries (`UNION` etc.) and the
select used by `Insert().FromQuery` are rendered with the dialect of the statement that contains them, whatever
dialect they were built with. For example a subquery created with `goqu.From` inside a `postgres` prepared statement
uses `$1` placeholders and double quoted identifiers. A dataset with a custom `SQLDialect` set through `SetDialect`
keeps rendering with that dialect. `Insert().FromQuery` still panics if the select explicitly uses a different
registered dialect than the insert.

### Executing Queries 

You can also create a `goqu.Database` instance to query records.

In the example below notice that we imported the dialect and driver for side effect only.

```go
import (
  "database/sql"
  "github.com/doug-martin/goqu/v9"
  _ "github.com/doug-martin/goqu/v9/dialect/postgres"
  _ "github.com/lib/pq"
)

dialect := goqu.Dialect("postgres")

pgDb, err := sql.Open("postgres", "user=postgres dbname=goqupostgres sslmode=disable ")
if err != nil {
  panic(err.Error())
}
db := dialect.DB(pgDb)

// "SELECT COUNT(*) FROM "user";
if count, err := db.From("user").Count(); err != nil {
  fmt.Println(err.Error())
}else{
  fmt.Printf("User count = %d", count)
}
```

<a name="custom-dialects"></a>
## Custom Dialects

Dialects in goqu are the foundation of building the correct SQL for each DB dialect.

### Dialect Options

Most SQL dialects share a majority of their syntax, for this reason `goqu` has a [default set of dialect options](http://godoc.org/github.com/doug-martin/goqu/#DefaultDialectOptions) that can be used as a base for any new Dialect.

When creating a new `SQLDialect` you just need to override the default values that are documented in [`SQLDialectOptions`](http://godoc.org/github.com/doug-martin/goqu/#SQLDialectOptions).

Take a look at [`postgres`](../dialect/postgres/postgres.go), [`mysql`](../dialect/mysql/mysql.go) and [`sqlite3`](../dialect/sqlite3/sqlite3.go) for examples.

### Creating a custom dialect

When creating a new dialect you must register it using [`RegisterDialect`](http://godoc.org/github.com/doug-martin/goqu/#RegisterDialect). This method requires 2 arguments.

1. `dialect string` - The name of your dialect
2. `opts SQLDialectOptions` - The custom options for your dialect

For example you could create a custom dialect that replaced the default quote `'"'` with a backtick <code>`</code>
```go
opts := goqu.DefaultDialectOptions()
opts.QuoteRune = '`'
goqu.RegisterDialect("custom-dialect", opts)

dialect := goqu.Dialect("custom-dialect")

ds := dialect.From("test")

sql, args, _ := ds.ToSQL()
fmt.Println(sql, args)
```

Output:
```
SELECT * FROM `test` []
```

Another example is a dialect that does not use the `AS` keyword for aliases, such as Oracle (`FROM blah b` instead of `FROM blah AS b`):

```go
opts := goqu.DefaultDialectOptions()
opts.AsFragment = []byte(" ")
goqu.RegisterDialect("oracle-like", opts)

sql, _, _ := goqu.Dialect("oracle-like").From(goqu.T("blah").As("b")).ToSQL()
fmt.Println(sql)
```

Output:
```
SELECT * FROM "blah" "b"
```

`SliceAsArray` writes slices used as insert and update values as an array literal (`'{"a","b"}'`) instead of lists
(`('a', 'b')`). It is on for `postgres`, see [Insert Slices As Arrays](./inserting.md#insert-arrays).

`UseEqualsForBools` writes a comparison with a boolean as `=` / `!=` instead of `IS` / `IS NOT` (`"a" = TRUE` instead of
`"a" IS TRUE`) for databases that do not support `IS` with a boolean, such as Trino. `IS NOT TRUE` / `IS NOT FALSE` (and
`Neq(true)`) also match NULL rows, which a plain `!=` would drop, so they are written as
`("a" != TRUE OR "a" IS NULL)` and the rows returned do not change. In prepared mode the boolean is a bound argument
instead of the literal `TRUE`/`FALSE`. `IS NULL` and `IS NOT NULL` are not changed. It is off by default.

A database without `LIMIT` or `TOP` (such as Oracle 19c) paginates with `OFFSET n ROWS FETCH NEXT m ROWS ONLY`. Start
from the default `SelectSQLOrder` (so `WITH`, `JOIN`, `GROUP BY`, `HAVING`, compounds and `FOR` are kept), replace
`OrderSQLFragment` with `OrderWithOffsetFetchSQLFragment` and drop the separate limit and offset fragments. Set
`FetchFragment` (the default `" "` does not produce valid SQL) and `AlwaysOffsetFetch`, so a limit without an offset
still renders `OFFSET 0 ROWS FETCH NEXT m ROWS ONLY`. Without `AlwaysOffsetFetch` the fragment only renders for an
offset above 0 and needs an `ORDER BY` (an offset without one is an error, and a limit without an offset is not
rendered, so a dialect without `TOP` or `LIMIT` should set the option). Do not combine `AlwaysOffsetFetch` with `SelectWithLimitSQLFragment` (`TOP`) or
`LimitSQLFragment`, the limit would be rendered twice.

```go
import (
	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/sqlgen"
)

opts := goqu.DefaultDialectOptions()
opts.FetchFragment = []byte(" FETCH NEXT ")
opts.AlwaysOffsetFetch = true

order := make([]sqlgen.SQLFragmentType, 0, len(opts.SelectSQLOrder))
for _, f := range opts.SelectSQLOrder {
	switch f {
	case sqlgen.OrderSQLFragment:
		order = append(order, sqlgen.OrderWithOffsetFetchSQLFragment)
	case sqlgen.LimitSQLFragment, sqlgen.OffsetSQLFragment:
		// rendered by OrderWithOffsetFetchSQLFragment
	default:
		order = append(order, f)
	}
}
opts.SelectSQLOrder = order
goqu.RegisterDialect("oracle-19c", opts)
```

For more examples look at [`postgres`](../dialect/postgres/postgres.go), [`mysql`](../dialect/mysql/mysql.go) and [`sqlite3`](../dialect/sqlite3/sqlite3.go) for examples.

