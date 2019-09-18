# Dialect

Dialects allow goqu the build the correct SQL for each database. There are three dialects that come packaged with `goqu`

* [mysql](./dialect/mysql/mysql.go) - `import _ "github.com/doug-martin/goqu/v9/dialect/mysql"`
* [postgres](./dialect/postgres/postgres.go) - `import _ "github.com/doug-martin/goqu/v9/dialect/postgres"`
* [sqlite3](./dialect/sqlite3/sqlite3.go) - `import _ "github.com/doug-martin/goqu/v9/dialect/sqlite3"`

**NOTE** Dialects work like drivers in go where they are not registered until you import the package.

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

Most SQL dialects share a majority of their syntax, for this reason `goqu` has a [default set of dialect options]((http://godoc.org/github.com/doug-martin/goqu/#DefaultDialectOptions)) that can be used as a base for any new Dialect.

When creating a new `SQLDialect` you just need to override the default values that are documented in [`SQLDialectOptions`](http://godoc.org/github.com/doug-martin/goqu/#SQLDialectOptions).

Take a look at [`postgres`](./dialect/postgres/postgres.go), [`mysql`](./dialect/mysql/mysql.go) and [`sqlite3`](./dialect/sqlite3/sqlite3.go) for examples.

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

For more examples look at [`postgres`](./dialect/postgres/postgres.go), [`mysql`](./dialect/mysql/mysql.go) and [`sqlite3`](./dialect/sqlite3/sqlite3.go) for examples.

