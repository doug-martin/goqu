```
  __ _  ___   __ _ _   _
 / _` |/ _ \ / _` | | | |
| (_| | (_) | (_| | |_| |
 \__, |\___/ \__, |\__,_|
 |___/          |_|
```
[![GitHub tag](https://img.shields.io/github/tag/doug-martin/goqu.svg?style=flat)](https://github.com/doug-martin/goqu/releases)
[![Build Status](https://travis-ci.org/doug-martin/goqu.svg?branch=master)](https://travis-ci.org/doug-martin/goqu)
[![GoDoc](https://godoc.org/github.com/doug-martin/goqu?status.png)](http://godoc.org/github.com/doug-martin/goqu)
[![codecov](https://codecov.io/gh/doug-martin/goqu/branch/master/graph/badge.svg)](https://codecov.io/gh/doug-martin/goqu)
[![Go Report Card](https://goreportcard.com/badge/github.com/doug-martin/goqu/v7)](https://goreportcard.com/report/github.com/doug-martin/goqu/v7)

`goqu` is an expressive SQL builder and executor

## Installation

If using go modules.

```sh
go get -u github.com/doug-martin/goqu/v7
```

If you are not using go modules...

**NOTE** You should still be able to use this package if you are using go version `>v1.10` but, you will need to drop the version from the package. `import "github.com/doug-martin/goqu/v7` -> `import "github.com/doug-martin/goqu"`

```sh
go get -u github.com/doug-martin/goqu
```


[Migrating Between Versions](#migrating)

## Features

`goqu` comes with many features but here are a few of the more notable ones

* Query Builder
* Parameter interpolation (e.g `SELECT * FROM "items" WHERE "id" = ?` -> `SELECT * FROM "items" WHERE "id" = 1`)
* Built from the ground up with multiple dialects in mind
* Insert, Multi Insert, Update, and Delete support
* Scanning of rows to struct[s] or primitive value[s]

While goqu may support the scanning of rows into structs it is not intended to be used as an ORM if you are looking for common ORM features like associations,
or hooks I would recommend looking at some of the great ORM libraries such as:

* [gorm](https://github.com/jinzhu/gorm)
* [hood](https://github.com/eaigner/hood)

## Why?

We tried a few other sql builders but each was a thin wrapper around sql fragments that we found error prone. `goqu` was built with the following goals in mind:

* Make the generation of SQL easy and enjoyable
* Create an expressive DSL that would find common errors with SQL at compile time.
* Provide a DSL that accounts for the common SQL expressions, NOT every nuance for each database.
* Provide developers the ability to:
  * Use SQL when desired
  * Easily scan results into primitive values and structs
  * Use the native sql.Db methods when desired

## Usage

* [Dialect](#dialect)
  * [Postgres](#postgres)
  * [MySQL](#mysql)
  * [SQLite3](#sqlite3)
* [Dataset](#dataset)
  * [Building SQL](#building-sql)
    * [Expressions](#expressions)
      * [`Ex{}`](#ex) - Expression map filtering
      * [`ExOr{}`](#ex-or) - ORed expression map filtering
      * [`S()`](#S) - Schema identifiers
      * [`T()`](#T) - Table identifiers
      * [`C()`](#C) - Column identifiers
      * [`I()`](#I) - Parsing identifiers wit
      * [`L()`](#L) - Literal SQL expressions
      * [`And()`](#and) - ANDed sql expressions
      * [`OR()`](#or) - ORed sql expressions
      * [Complex Example](#complex-example)
    * [Querying](#querying)
      * [Executing Queries](#executing-queries) 
      * [Dataset](#dataset)
        * [Prepared Statements](#dataset_prepared)
* [Database](#database)
  * [Transactions](#transactions)
* [Logging](#logging)
* [Custom Dialects](#custom-dialects)

<a name="dialect"></a>
## Dialect

Dialects allow goqu the build the correct SQL for each database. There are three dialects that come packaged with `goqu`

* [mysql](./dialect/mysql/mysql.go) - `import _ "github.com/doug-martin/goqu/v7/dialect/mysql"`
* [postgres](./dialect/postgres/postgres.go) - `import _ "github.com/doug-martin/goqu/v7/dialect/postgres"`
* [sqlite3](./dialect/sqlite3/sqlite3.go) - `import _ "github.com/doug-martin/goqu/v7/dialect/sqlite3"`

**NOTE** Dialects work like drivers in go where they are not registered until you import the package.

Below are examples for each dialect. Notice how the dialect is imported and then looked up using `goqu.Dialect`

<a name="postgres"></a>
### Postgres
```go
import (
  "fmt"
  "github.com/doug-martin/goqu/v7"
  // import the dialect
  _ "github.com/doug-martin/goqu/v7/dialect/postgres"
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
  "github.com/doug-martin/goqu/v7"
  // import the dialect
  _ "github.com/doug-martin/goqu/v7/dialect/mysql"
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
  "github.com/doug-martin/goqu/v7"
  // import the dialect
  _ "github.com/doug-martin/goqu/v7/dialect/sqlite3"
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

<a name="dataset"></a>
## Dataset

A [`goqu.Dataset`](https://godoc.org/github.com/doug-martin/goqu#Dataset) is the most commonly used data structure used in `goqu`. A `Dataset` can be used to:
* [build SQL](#building-sql) - When used with a `dialect` and `expressions` a dataset is an expressive SQL builder
* [execute queries](#querying) - When used with a `goqu.Database` a `goqu.Dataset` can be used to:
  * [`ScanStruct`](#ds-scan-struct) - scan into a struct
  * [`ScanStructs`](#ds-scan-structs) - scan into a slice of structs
  * [`ScanVal`](#ds-scan-val) - scan into a primitive value or a `driver.Valuer`
  * [`ScanVals`](#ds-scan-vals) - scan into a slice of primitive values or `driver.Valuer`s
  * [`Count`](#ds-count) - count the number of records in a table
  * [`Pluck`](#ds-pluck) - pluck a column from a table
  * [`Insert`](#ds-insert) - insert records into a table
  * [`Update`](#ds-update) - update records in a table
  * [`Delete`](#ds-delete) - delete records in a table

<a name="building-sql"></a>
### Building SQL

To build SQL with a dialect you can use `goqu.Dialect`

**NOTE** if you use do not create a `goqu.Database` you can only create SQL 

```go
import (
  "fmt"
  "github.com/doug-martin/goqu/v7"
  _ "github.com/doug-martin/goqu/v7/dialect/postgres"
)

dialect := goqu.Dialect("postgres")

//interpolated sql
ds := dialect.From("test").Where(goqu.Ex{"id": 10})
sql, args, err := ds.ToSQL()
if err != nil{
  fmt.Println("An error occurred while generating the SQL", err.Error())
}else{
  fmt.Println(sql, args)
}

//prepared sql
sql, args, err := ds.Prepared(true).ToSQL()
if err != nil{
  fmt.Println("An error occurred while generating the SQL", err.Error())
}else{
  fmt.Println(sql, args)
}

```

Output:
```
SELECT * FROM "test" WHERE "id" = 10 []
SELECT * FROM "test" WHERE "id" = $1 [10]
```

<a name="expressions"></a>
### Expressions

`goqu` provides an idiomatic DSL for generating SQL. Datasets only act as a clause builder (i.e. Where, From, Select), most of these clause methods accept Expressions which are the building blocks for your SQL statement, you can think of them as fragments of SQL.

The entry points for expressions are:

<a name="ex"></a>
* [`Ex{}`](https://godoc.org/github.com/doug-martin/goqu#Ex) - A map where the key will become an Identifier and the Key is the value, this is most commonly used in the Where clause. By default `Ex` will use the equality operator except in cases where the equality operator will not work, see the example below.
  ```go
  sql, _, _ := db.From("items").Where(goqu.Ex{
	  "col1": "a",
	  "col2": 1,
	  "col3": true,
	  "col4": false,
	  "col5": nil,
	  "col6": []string{"a", "b", "c"},
  }).ToSQL()
  fmt.Println(sql)
  ```

  Output:
  ```sql
  SELECT * FROM "items" WHERE (("col1" = 'a') AND ("col2" = 1) AND ("col3" IS TRUE) AND ("col4" IS FALSE) AND ("col5" IS NULL) AND ("col6" IN ('a', 'b', 'c')))
  ```

  You can also use the [`Op`](https://godoc.org/github.com/doug-martin/goqu#Op) map which allows you to create more complex expressions using the map syntax. When using the `Op` map the key is the name of the comparison you want to make (e.g. `"neq"`, `"like"`, `"is"`, `"in"`), the key is case insensitive.
  ```go
  sql, _, _ := db.From("items").Where(goqu.Ex{
    "col1": goqu.Op{"neq": "a"},
    "col3": goqu.Op{"isNot": true},
    "col6": goqu.Op{"notIn": []string{"a", "b", "c"}},
  }).ToSQL()
  fmt.Println(sql)
  ```

  Output:
  ```sql
  SELECT * FROM "items" WHERE (("col1" != 'a') AND ("col3" IS NOT TRUE) AND ("col6" NOT IN ('a', 'b', 'c')))
  ```
  For a more complete examples see the [`Op`](https://godoc.org/github.com/doug-martin/goqu#Op) and [`Ex`](https://godoc.org/github.com/doug-martin/goqu#Ex) docs

<a name="ex-or"></a>
* [`ExOr{}`](https://godoc.org/github.com/doug-martin/goqu#ExOr) - A map where the key will become an Identifier and the Key is the value, this is most commonly used in the Where clause. By default `ExOr` will use the equality operator except in cases where the equality operator will not work, see the example below.
  ```go
  sql, _, _ := db.From("items").Where(goqu.ExOr{
	  "col1": "a",
	  "col2": 1,
	  "col3": true,
	  "col4": false,
	  "col5": nil,
	  "col6": []string{"a", "b", "c"},
  }).ToSQL()
  fmt.Println(sql)
  ```
  
  Output:
  ```sql
  SELECT * FROM "items" WHERE (("col1" = 'a') OR ("col2" = 1) OR ("col3" IS TRUE) OR ("col4" IS FALSE) OR ("col5" IS NULL) OR ("col6" IN ('a', 'b', 'c')))
  ```
  
  You can also use the [`Op`](https://godoc.org/github.com/doug-martin/goqu#Op) map which allows you to create more complex expressions using the map syntax. When using the `Op` map the key is the name of the comparison you want to make (e.g. `"neq"`, `"like"`, `"is"`, `"in"`), the key is case insensitive.
  
  ```go
  sql, _, _ := db.From("items").Where(goqu.ExOr{
    "col1": goqu.Op{"neq": "a"},
    "col3": goqu.Op{"isNot": true},
    "col6": goqu.Op{"notIn": []string{"a", "b", "c"}},
  }).ToSQL()
  fmt.Println(sql)
  ```
  
  Output:
  ```sql
  SELECT * FROM "items" WHERE (("col1" != 'a') OR ("col3" IS NOT TRUE) OR ("col6" NOT IN ('a', 'b', 'c')))
  ```
  For a more complete examples see the [`Op`](https://godoc.org/github.com/doug-martin/goqu#Op) and [`ExOr`](https://godoc.org/github.com/doug-martin/goqu#Ex) docs

<a name="S"></a>
* [`S()`](https://godoc.org/github.com/doug-martin/goqu#S) - An Identifier that represents a schema. With a schema identifier you can fully qualify tables and columns.
  ```go
  s := goqu.S("my_schema")

  // "my_schema"."my_table"
  t := s.Table("my_table")

  // "my_schema"."my_table"."my_column"

  sql, _, _ := goqu.From(t).Select(t.Col("my_column").ToSQL()
  // SELECT "my_schema"."my_table"."my_column" FROM "my_schema"."my_table"
  fmt.Println(sql)
  ```

<a name="T"></a>
* [`T()`](https://godoc.org/github.com/doug-martin/goqu#T) - An Identifier that represents a Table. With a Table identifier you can fully qualify columns.
  ```go
  t := s.Table("my_table")

  sql, _, _ := goqu.From(t).Select(t.Col("my_column").ToSQL()
  // SELECT "my_table"."my_column" FROM "my_table"
  fmt.Println(sql)

  // qualify the table with a schema
  sql, _, _ := goqu.From(t.Schema("my_schema")).Select(t.Col("my_column").ToSQL()
  // SELECT "my_table"."my_column" FROM "my_schema"."my_table"
  fmt.Println(sql)
  ```

<a name="C"></a>
* [`C()`](https://godoc.org/github.com/doug-martin/goqu#C) - An Identifier that represents a Column. See the [docs]((https://godoc.org/github.com/doug-martin/goqu#C)) for more examples
  ```go
  sql, _, _ := goqu.From("table").Where(goqu.C("col").Eq(10)).ToSQL()
  // SELECT * FROM "table" WHERE "col" = 10
  fmt.Println(sql)
  ```

<a name="I"></a>
* [`I()`](https://godoc.org/github.com/doug-martin/goqu#I) - An Identifier represents a schema, table, or column or any combination. `I` parses identifiers seperated by a `.` character.
  ```go
  // with three parts it is assumed you have provided a schema, table and column
  goqu.I("my_schema.table.col") == goqu.S("my_schema").Table("table").Col("col")

  // with two parts it is assumed you have provided a table and column
  goqu.I("table.col") == goqu.T("table").Col("col")

  // with a single value it is the same as calling goqu.C
  goqu.I("col") == goqu.C("col")

  ```

<a name="L"></a>
* [`L()`](https://godoc.org/github.com/doug-martin/goqu#L) - An SQL literal. You may find yourself in a situation where an IdentifierExpression cannot expression an SQL fragment that your database supports. In that case you can use a LiteralExpression
  ```go
  // manual casting
  goqu.L(`"json"::TEXT = "other_json"::text`)

  // custom function invocation
  goqu.L(`custom_func("a")`)

  // postgres JSON access
  goqu.L(`"json_col"->>'someField'`).As("some_field")
  ```
  
  You can also use placeholders in your literal with a `?` character. `goqu` will handle changing it to what the dialect needs (e.g. `?` mysql, `$1` postgres, `?` sqlite3). 

  **NOTE** If your query is not prepared the placeholders will be properly interpolated.

  ```go
  goqu.L("col IN (?, ?, ?)", "a", "b", "c") 
  ```

  Putting it together
  
  ```go
  ds := db.From("test").Where(
    goqu.L(`("json"::TEXT = "other_json"::TEXT)`),
    goqu.L("col IN (?, ?, ?)", "a", "b", "c"),
  )

  sql, args, _ := ds.ToSQL()
  fmt.Println(sql, args)

  sql, args, _ := ds.Prepared(true).ToSQL()
  fmt.Println(sql, args)
  ```

  Output:
  ```sql
  SELECT * FROM "test" WHERE ("json"::TEXT = "other_json"::TEXT) AND col IN ('a', 'b', 'c') []
  -- assuming postgres dialect
  SELECT * FROM "test" WHERE ("json"::TEXT = "other_json"::TEXT) AND col IN ($1, $2, $3) [a, b, c]
  ```

<a name="and"></a>
* [`And()`](https://godoc.org/github.com/doug-martin/goqu#And) - You can use the `And` function to AND multiple expressions together.

  **NOTE** By default goqu will AND expressions together

  ```go
  ds := goqu.From("test").Where(
	  goqu.And(
		  goqu.C("col").Gt(10),
		  goqu.C("col").Lt(20),
	  ),
  )
  sql, args, _ := ds.ToSQL()
  fmt.Println(sql, args)

  sql, args, _ = ds.Prepared(true).ToSQL()
  fmt.Println(sql, args)
  ```

  Output:
  ```sql
  SELECT * FROM "test" WHERE (("col" > 10) AND ("col" < 20)) []
  SELECT * FROM "test" WHERE (("col" > ?) AND ("col" < ?)) [10 20]
  ```

<a name="or"></a>
* [`Or()`](https://godoc.org/github.com/doug-martin/goqu#Or) - You can use the `Or` function to OR multiple expressions together.

  ```go
  ds := goqu.From("test").Where(
	  goqu.Or(
		  goqu.C("col").Eq(10),
		  goqu.C("col").Eq(20),
	  ),
  )
  sql, args, _ := ds.ToSQL()
  fmt.Println(sql, args)

  sql, args, _ = ds.Prepared(true).ToSQL()
  fmt.Println(sql, args)
  ```

  Output:
  ```sql
  SELECT * FROM "test" WHERE (("col" = 10) OR ("col" = 20)) []
  SELECT * FROM "test" WHERE (("col" = ?) OR ("col" = ?)) [10 20]
  ```

  You can also use `Or` and `And` functions in tandem which will give you control not only over how the Expressions are joined together, but also how they are grouped
 
  ```go
  ds := goqu.From("items").Where(
	  goqu.Or(
		  goqu.C("a").Gt(10),
  	  goqu.And(
			  goqu.C("b").Eq(100),
			  goqu.C("c").Neq("test"),
		  ),
	  ),
  )
  sql, args, _ := ds.ToSQL()
  fmt.Println(sql, args)

  sql, args, _ = ds.Prepared(true).ToSQL()
  fmt.Println(sql, args)
  ```

  Output:
  ```sql
  SELECT * FROM "items" WHERE (("a" > 10) OR (("b" = 100) AND ("c" != 'test'))) []
  SELECT * FROM "items" WHERE (("a" > ?) OR (("b" = ?) AND ("c" != ?))) [10 100 test]
  ```

  You can also use Or with the map syntax
  ```go
  ds := goqu.From("test").Where(
	  goqu.Or(
	    // Ex will be anded together
      goqu.Ex{
        "col1": 1,
        "col2": true,
      },
      goqu.Ex{
        "col3": nil,
        "col4": "foo",
      },
    ),
  )
  sql, args, _ := ds.ToSQL()
  fmt.Println(sql, args)

  sql, args, _ = ds.Prepared(true).ToSQL()
  fmt.Println(sql, args)
  ```

  Output:
  ```sql
  SELECT * FROM "test" WHERE ((("col1" = 1) AND ("col2" IS TRUE)) OR (("col3" IS NULL) AND ("col4" = 'foo'))) []
  SELECT * FROM "test" WHERE ((("col1" = ?) AND ("col2" IS TRUE)) OR (("col3" IS NULL) AND ("col4" = ?))) [1 foo]
  ```
<a name="complex-example"></a>
### Complex Example

Using the Ex map syntax
```go
ds := db.From("test").
  Select(goqu.COUNT("*")).
  InnerJoin(goqu.I("test2"), goqu.On(goqu.Ex{"test.fkey": goqu.I("test2.id")})).
  LeftJoin(goqu.I("test3"), goqu.On(goqu.Ex{"test2.fkey": goqu.I("test3.id")})).
  Where(
    goqu.Ex{
      "test.name":    goqu.Op{"like": regexp.MustCompile("^(a|b)")},
      "test2.amount": goqu.Op{"isNot": nil},
    },
    goqu.ExOr{
      "test3.id":     nil,
      "test3.status": []string{"passed", "active", "registered"},
    },
  ).
  Order(goqu.I("test.created").Desc().NullsLast()).
  GroupBy(goqu.I("test.user_id")).
  Having(goqu.AVG("test3.age").Gt(10))

sql, args, _ := ds.ToSQL()
fmt.Println(sql)

sql, args, _ := ds.Prepared(true).ToSQL()
fmt.Println(sql)
```

Using the Expression syntax
```go
ds := db.From("test").
  Select(goqu.COUNT("*")).
  InnerJoin(goqu.I("test2"), goqu.On(goqu.I("test.fkey").Eq(goqu.I("test2.id")))).
  LeftJoin(goqu.I("test3"), goqu.On(goqu.I("test2.fkey").Eq(goqu.I("test3.id")))).
  Where(
    goqu.I("test.name").Like(regexp.MustCompile("^(a|b)")),
    goqu.I("test2.amount").IsNotNull(),
    goqu.Or(
      goqu.I("test3.id").IsNull(),
      goqu.I("test3.status").In("passed", "active", "registered"),
    ),
  ).
  Order(goqu.I("test.created").Desc().NullsLast()).
  GroupBy(goqu.I("test.user_id")).
  Having(goqu.AVG("test3.age").Gt(10))

sql, args, _ := ds.ToSQL()
fmt.Println(sql)

sql, args, _ := ds.Prepared(true).ToSQL()
fmt.Println(sql)
```

Both examples generate the following SQL

```sql
-- interpolated
SELECT COUNT(*)
FROM "test"
         INNER JOIN "test2" ON ("test"."fkey" = "test2"."id")
         LEFT JOIN "test3" ON ("test2"."fkey" = "test3"."id")
WHERE ((("test"."name" ~ '^(a|b)') AND ("test2"."amount" IS NOT NULL)) AND
       (("test3"."id" IS NULL) OR ("test3"."status" IN ('passed', 'active', 'registered'))))
GROUP BY "test"."user_id"
HAVING (AVG("test3"."age") > 10)
ORDER BY "test"."created" DESC NULLS LAST []

-- prepared
SELECT COUNT(*)
FROM "test"
         INNER JOIN "test2" ON ("test"."fkey" = "test2"."id")
         LEFT JOIN "test3" ON ("test2"."fkey" = "test3"."id")
WHERE ((("test"."name" ~ ?) AND ("test2"."amount" IS NOT NULL)) AND
       (("test3"."id" IS NULL) OR ("test3"."status" IN (?, ?, ?))))
GROUP BY "test"."user_id"
HAVING (AVG("test3"."age") > ?)
ORDER BY "test"."created" DESC NULLS LAST [^(a|b) passed active registered 10]
```

<a name="querying"></a>
## Querying

`goqu` also has basic query support through the use of either the Database or the Dataset.

<a name="executing-queries"></a>
### Executing Queries 

You can also create a `goqu.Database` instance to query records.

In the example below notice that we imported the dialect and driver for side effect only.

```go
import (
  "database/sql"
  "github.com/doug-martin/goqu/v7"
  _ "github.com/doug-martin/goqu/v7/dialect/postgres"
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

<a name="ds-scan-structs"></a>
* [`ScanStructs`](http://godoc.org/github.com/doug-martin/goqu#Dataset.ScanStructs) - scans rows into a slice of structs

  **NOTE** [`ScanStructs`](http://godoc.org/github.com/doug-martin/goqu#Dataset.ScanStructs) will only select the columns that can be scanned in to the structs unless you have explicitly selected certain columns.

  ```go
  type User struct{
    FirstName string `db:"first_name"`
    LastName  string `db:"last_name"`
  }

  var users []User
  //SELECT "first_name", "last_name" FROM "user";
  if err := db.From("user").ScanStructs(&users); err != nil{
    panic(err.Error())
  }
  fmt.Printf("\n%+v", users)

  var users []User
  //SELECT "first_name" FROM "user";
  if err := db.From("user").Select("first_name").ScanStructs(&users); err != nil{
    panic(err.Error())
  }
  fmt.Printf("\n%+v", users)
  ```

<a name="ds-scan-struct"></a>
* [`ScanStruct`](http://godoc.org/github.com/doug-martin/goqu#Dataset.ScanStruct) - scans a row into a slice a struct, returns false if a row wasnt found

  **NOTE** [`ScanStruct`](http://godoc.org/github.com/doug-martin/goqu#Dataset.ScanStruct) will only select the columns that can be scanned in to the struct unless you have explicitly selected certain columns.

  ```go
  type User struct{
    FirstName string `db:"first_name"`
    LastName  string `db:"last_name"`
  }

  var user User
  // SELECT "first_name", "last_name" FROM "user" LIMIT 1;
  found, err := db.From("user").ScanStruct(&user)
  if err != nil{
    fmt.Println(err.Error())
    return
  }
  if !found {
    fmt.Println("No user found")
  } else {
    fmt.Printf("\nFound user: %+v", user)
  }
  ```


  **NOTE** Using the `goqu.SetColumnRenameFunction` function, you can change the function that's used to rename struct fields when struct tags aren't defined

  ```go
  import "strings"

  goqu.SetColumnRenameFunction(strings.ToUpper)

  type User struct{
    FirstName string
    LastName string
  }

  var user User
  //SELECT "FIRSTNAME", "LASTNAME" FROM "user" LIMIT 1;
  found, err := db.From("user").ScanStruct(&user)
  // ...
  ```
<a name="ds-scan-vals"></a>
* [`ScanVals`](http://godoc.org/github.com/doug-martin/goqu#Dataset.ScanVals) - scans a rows of 1 column into a slice of primitive values
  ```go
  var ids []int64
  if err := db.From("user").Select("id").ScanVals(&ids); err != nil{
    fmt.Println(err.Error())
    return
  }
  fmt.Printf("\n%+v", ids)
  ```

<a name="ds-scan-val"></a>
* [`ScanVal`](http://godoc.org/github.com/doug-martin/goqu#Dataset.ScanVal) - scans a row of 1 column into a primitive value, returns false if a row wasnt found.   

  **Note** when using the dataset a `LIMIT` of 1 is automatically applied.
  ```go
  var id int64
  found, err := db.From("user").Select("id").ScanVal(&id)
  if err != nil{
    fmt.Println(err.Error())
    return
  }
  if !found{
    fmt.Println("No id found")
  }else{
    fmt.Printf("\nFound id: %d", id)
  }
  ```

<a name="ds-count"></a>
* [`Count`](http://godoc.org/github.com/doug-martin/goqu#Dataset.Count) - Returns the count for the current query
  ```go
  count, err := db.From("user").Count()
  if err != nil{
    fmt.Println(err.Error())
    return
  }
  fmt.Printf("\nCount:= %d", count)
  ```

<a name="ds-pluck"></a>
* [`Pluck`](http://godoc.org/github.com/doug-martin/goqu#Dataset.Pluck) - Selects a single column and stores the results into a slice of primitive values
  ```go
  var ids []int64
  if err := db.From("user").Pluck(&ids, "id"); err != nil{
    fmt.Println(err.Error())
    return
  }
  fmt.Printf("\nIds := %+v", ids)
  ```

<a name="ds-insert"></a>
* [`Insert`](http://godoc.org/github.com/doug-martin/goqu#Dataset.Insert) - Creates an `INSERT` statement and returns a [`QueryExecutor`](http://godoc.org/github.com/doug-martin/goqu/exec/#QueryExecutor) to execute the statement
  ```go
  insert := db.From("user").Insert(goqu.Record{
    "first_name": "Bob", 
    "last_name":  "Yukon", 
    "created":    time.Now(),
  })
  if _, err := insert.Exec(); err != nil{
    fmt.Println(err.Error())
    return
  }
  ```
  
  Insert will also handle multi inserts if supported by the database
  
  ```go
  users := []goqu.Record{
    {"first_name": "Bob",   "last_name": "Yukon", "created": time.Now()},
    {"first_name": "Sally", "last_name": "Yukon", "created": time.Now()},
    {"first_name": "Jimmy", "last_name": "Yukon", "created": time.Now()},
  }
  if _, err := db.From("user").Insert(users).Exec(); err != nil{
    fmt.Println(err.Error())
    return
  }
  ```
  
  If your database supports the `RETURN` clause you can also use the different Scan methods to get results
  ```go
  var ids []int64
  users := []goqu.Record{
    {"first_name": "Bob",   "last_name": "Yukon", "created": time.Now()},
    {"first_name": "Sally", "last_name": "Yukon", "created": time.Now()},
    {"first_name": "Jimmy", "last_name": "Yukon", "created": time.Now()},
  }
  if err := db.From("user").Returning(goqu.C("id")).Insert(users).ScanVals(&ids); err != nil{
    fmt.Println(err.Error())
    return
  }
  ```

<a name="ds-update"></a>
* [`Update`](http://godoc.org/github.com/doug-martin/goqu#Dataset.Update) - Creates an `UPDATE` statement and returns [`QueryExecutor`](http://godoc.org/github.com/doug-martin/goqu/exec/#QueryExecutor) to execute the statement

  ```go
  update := db.From("user").
    Where(goqu.C("status").Eq("inactive")).
    Update(goqu.Record{"password": nil, "updated": time.Now()})
  if _, err := update.Exec(); err != nil{
    fmt.Println(err.Error())
    return
  }
  ```

  If your database supports the `RETURN` clause you can also use the different Scan methods to get results
  ```go
  var ids []int64
  update := db.From("user").
    Where(goqu.Ex{"status":"inactive"}).
    Returning("id").
    Update(goqu.Record{"password": nil, "updated": time.Now()})
  if err := update.ScanVals(&ids); err != nil{
    fmt.Println(err.Error())
    return
  }
  ```

<a name="ds-delete"></a>
* [`Delete`](http://godoc.org/github.com/doug-martin/goqu#Dataset.Delete) - Creates an `DELETE` statement and returns a [`QueryExecutor`](http://godoc.org/github.com/doug-martin/goqu/exec/#QueryExecutor) to execute the statement
  ```go
  delete := db.From("invoice").
    Where(goqu.Ex{"status":"paid"}).
    Delete()
  if _, err := delete.Exec(); err != nil{
    fmt.Println(err.Error())
    return
  }
  ```

  If your database supports the `RETURN` clause you can also use the different Scan methods to get results

  ```go
  var ids []int64
  delete := db.From("invoice").
    Where(goqu.C("status").Eq("paid")).
    Returning(goqu.C("id")).
    Delete()
  if err := delete.ScanVals(&ids); err != nil{
    fmt.Println(err.Error())
    return
  }
  ```

<a name="dataset_prepared"></a>
#### Prepared Statements

By default the `Dataset` will interpolate all parameters, if you do not want to have values interpolated you can use the [`Prepared`](http://godoc.org/github.com/doug-martin/goqu#Dataset.Prepared) method to prevent this.

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

sql, args, _ = preparedDs.ToInsertSQL(
	goqu.Record{"name": "Test1", "address": "111 Test Addr"},
	goqu.Record{"name": "Test2", "address": "112 Test Addr"},
)
fmt.Println(sql, args)

sql, args, _ = preparedDs.ToUpdateSQL(
	goqu.Record{"name": "Test", "address": "111 Test Addr"},
)
fmt.Println(sql, args)

sql, args, _ = preparedDs.
	Where(goqu.Ex{"id": goqu.Op{"gt": 10}}).
	ToDeleteSQL()
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


<a name="database"></a>
### Database

The Database also allows you to execute queries but expects raw SQL to execute. The supported methods are

* [`Exec`](http://godoc.org/github.com/doug-martin/goqu#Database.Exec)
* [`Prepare`](http://godoc.org/github.com/doug-martin/goqu#Database.Prepare)
* [`Query`](http://godoc.org/github.com/doug-martin/goqu#Database.Query)
* [`QueryRow`](http://godoc.org/github.com/doug-martin/goqu#Database.QueryRow)
* [`ScanStructs`](http://godoc.org/github.com/doug-martin/goqu#Database.ScanStructs)
* [`ScanStruct`](http://godoc.org/github.com/doug-martin/goqu#Database.ScanStruct)
* [`ScanVals`](http://godoc.org/github.com/doug-martin/goqu#Database.ScanVals)
* [`ScanVal`](http://godoc.org/github.com/doug-martin/goqu#Database.ScanVal)
* [`Begin`](http://godoc.org/github.com/doug-martin/goqu#Database.Begin)

<a name="transactions"></a>
### Transactions

`goqu` has builtin support for transactions to make the use of the Datasets and querying seamless

```go
tx, err := db.Begin()
if err != nil{
   return err
}
//use tx.From to get a dataset that will execute within this transaction
update := tx.From("user").
    Where(goqu.Ex("password": nil}).
    Update(goqu.Record{"status": "inactive"})
if _, err = update.Exec(); err != nil{
    if rErr := tx.Rollback(); rErr != nil{
        return rErr
    }
    return err
}
if err = tx.Commit(); err != nil{
    return err
}
return
```

The [`TxDatabase`](http://godoc.org/github.com/doug-martin/goqu/#TxDatabase)  also has all methods that the [`Database`](http://godoc.org/github.com/doug-martin/goqu/#Database) has along with

* [`Commit`](http://godoc.org/github.com/doug-martin/goqu#TxDatabase.Commit)
* [`Rollback`](http://godoc.org/github.com/doug-martin/goqu#TxDatabase.Rollback)
* [`Wrap`](http://godoc.org/github.com/doug-martin/goqu#TxDatabase.Wrap)

#### Wrap

The [`TxDatabase.Wrap`](http://godoc.org/github.com/doug-martin/goqu/#TxDatabase.Wrap) is a convience method for automatically handling `COMMIT` and `ROLLBACK`

```go
tx, err := db.Begin()
if err != nil{
   return err
}
err = tx.Wrap(func() error{
  update := tx.From("user").
      Where(goqu.Ex("password": nil}).
      Update(goqu.Record{"status": "inactive"})
  return update.Exec()
})
//err will be the original error from the update statement, unless there was an error executing ROLLBACK
if err != nil{
    return err
}
```

<a name="logging"></a>
## Logging

To enable trace logging of SQL statements use the [`Database.Logger`](http://godoc.org/github.com/doug-martin/goqu/#Database.Logger) method to set your logger.

**NOTE** The logger must implement the [`Logger`](http://godoc.org/github.com/doug-martin/goqu/#Logger) interface

**NOTE** If you start a transaction using a database your set a logger on the transaction will inherit that logger automatically


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

<a name="migrating"></a>
## Migrating Between Versions

### `<v7 to v7`

* Updated all sql generations methods to from `Sql` to `SQL`
    * `ToSql` -> `ToSQL`
    * `ToInsertSql` -> `ToInsertSQL`
    * `ToUpdateSql` -> `ToUpdateSQL`
    * `ToDeleteSql` -> `ToDeleteSQL`
    * `ToTruncateSql` -> `ToTruncateSQL`
* Abstracted out `dialect_options` from the adapter to make the dialect self contained.
    * This also removed the `dataset<->adapter` co dependency making the dialect self contained.
    * Added new dialect options to specify the order than SQL statements are built.
* Refactored the `goqu.I` method.
    * Added new `goqu.S`, `goqu.T` and `goqu.C` methods to clarify why type of identifier you are using.
    * `goqu.I` should only be used when you have a qualified identifier (e.g. `goqu.I("my_schema.my_table.my_col")
* Added new `goqu.Dialect` method to make using `goqu` as an SQL builder easier.


<a name="contributions"></a>
## Contributions

I am always welcoming contributions of any type. Please open an issue or create a PR if you find an issue with any of the following.

* An issue with Documentation
* You found the documentation lacking in some way

If you have an issue with the package please include the following

* The dialect you are using
* A description of the problem
* A short example of how to reproduce (if applicable)

Without those basics it can be difficult to reproduce your issue locally. You may be asked for more information but that is a good starting point.

### New Features

New features and/or enhancements are great and I encourage you to either submit a PR or create an issue. In both cases include the following as the need/requirement may not be readily apparent.

1. The use case
2. A short example

If you are issuing a PR also also include the following

1. Tests - otherwise the PR will not be merged
2. Documentation - otherwise the PR will not be merged
3. Examples - [If applicable] see example_test.go for examples

If you find an issue you want to work on please comment on it letting other people know you are looking at it and I will assign the issue to you.

If want to work on an issue but dont know where to start just leave a comment and I'll be more than happy to point you in the right direction.

### Running tests
The test suite requires a postgres and mysql database. You can override the mysql/postgres connection strings with the [`MYSQL_URI` and `PG_URI` environment variables](https://github.com/doug-martin/goqu/blob/2fe3349/docker-compose.yml#L26)*

```sh
go test -v -race ./...
```

You can also run the tests in a container using [docker-compose](https://docs.docker.com/compose/).

```sh
GO_VERSION=latest docker-compose run goqu
```

## License

`goqu` is released under the [MIT License](http://www.opensource.org/licenses/MIT).



