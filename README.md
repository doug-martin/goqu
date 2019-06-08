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
[![Go Report Card](https://goreportcard.com/badge/github.com/doug-martin/goqu/v6)](https://goreportcard.com/report/github.com/doug-martin/goqu/v6)

`goqu` is an expressive SQL builder

* [Basics](#basics)
* [Expressions](#expressions)
    * [Complex Example](#complex-example)
* [Querying](#querying)
    * [Dataset](#dataset)
        * [Prepared Statements](#dataset_prepared)
    * [Database](#database)
    * [Transactions](#transactions)
* [Logging](#logging)
* [Adapters](#adapters)
* [Contributions](#contributions)
* [Changelog](https://github.com/doug-martin/goqu/tree/master/HISTORY.md)

This library was built with the following goals:

* Make the generation of SQL easy and enjoyable
* Provide a DSL that accounts for the common SQL expressions, NOT every nuance for each database.
* Allow users to use SQL when desired
* Provide a simple query API for scanning rows
* Allow the user to use the native sql.Db methods when desired

## Features

`goqu` comes with many features but here are a few of the more notable ones

* Query Builder
* Parameter interpolation (e.g `SELECT * FROM "items" WHERE "id" = ?` -> `SELECT * FROM "items" WHERE "id" = 1`)
* Built from the ground up with adapters in mind
* Insert, Multi Insert, Update, and Delete support
* Scanning of rows to struct[s] or primitive value[s]

While goqu may support the scanning of rows into structs it is not intended to be used as an ORM if you are looking for common ORM features like associations,
or hooks I would recommend looking at some of the great ORM libraries such as:

* [gorm](https://github.com/jinzhu/gorm)
* [hood](https://github.com/eaigner/hood)


## Installation

```sh
go get -u github.com/doug-martin/goqu/v6
```


<a name="basics"></a>
## Basics

In order to start using goqu with your database you need to load an adapter. We have included some adapters by default.

1. Postgres - `import "github.com/doug-martin/goqu/v6/adapters/postgres"`
2. MySQL - `import "github.com/doug-martin/goqu/v6/adapters/mysql"`
3. SQLite3 - `import "github.com/doug-martin/goqu/v6/adapters/sqlite3"`

Adapters in goqu work the same way as a driver with the database in that they register themselves with goqu once loaded.

```go
import (
  "database/sql"
  "github.com/doug-martin/goqu/v6"
  _ "github.com/doug-martin/goqu/v6/adapters/postgres"
  _ "github.com/lib/pq"
)
```
Notice that we imported the adapter and driver for side effect only.

Once you have your adapter and driver loaded you can create a goqu.Database instance

```go
pgDb, err := sql.Open("postgres", "user=postgres dbname=goqupostgres sslmode=disable ")
if err != nil {
    panic(err.Error())
}
db := goqu.New("postgres", pgDb)
```
Now that you have your goqu.Database you can build your SQL and it will be formatted appropriately for the provided dialect.

```go
//interpolated sql
sql, _, _ := db.From("user").Where(goqu.Ex{
    "id": 10,
}).ToSql()
fmt.Println(sql)

//prepared sql
sql, args, _ := db.From("user").
    Prepared(true).
    Where(goqu.Ex{
        "id": 10,
    }).
    ToSql()
fmt.Println(sql)
```
Output
```sql
SELECT * FROM "user" WHERE "id" = 10
SELECT * FROM "user" WHERE "id" = $1
```

<a name="expressions"></a>
### Expressions

`goqu` provides an idiomatic DSL for generating SQL however the Dataset only provides the different clause methods (e.g. Where, From, Select), most of these clause methods accept Expressions(with a few exceptions) which are the building blocks for your SQL statement, you can think of them as fragments of SQL.

The entry points for expressions are:

* [`Ex{}`](https://godoc.org/github.com/doug-martin/goqu#Ex) - A map where the key will become an Identifier and the Key is the value, this is most commonly used in the Where clause. By default `Ex` will use the equality operator except in cases where the equality operator will not work, see the example below.
```go
sql, _, _ := db.From("items").Where(goqu.Ex{
	"col1": "a",
	"col2": 1,
	"col3": true,
	"col4": false,
	"col5": nil,
	"col6": []string{"a", "b", "c"},
}).ToSql()
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
}).ToSql()
fmt.Println(sql)
```
Output:
```sql
SELECT * FROM "items" WHERE (("col1" != 'a') AND ("col3" IS NOT TRUE) AND ("col6" NOT IN ('a', 'b', 'c')))
```
For a more complete examples see the [`Op`](https://godoc.org/github.com/doug-martin/goqu#Op) and [`Ex`](https://godoc.org/github.com/doug-martin/goqu#Ex) docs

* [`ExOr{}`](https://godoc.org/github.com/doug-martin/goqu#ExOr) - A map where the key will become an Identifier and the Key is the value, this is most commonly used in the Where clause. By default `ExOr` will use the equality operator except in cases where the equality operator will not work, see the example below.
```go
sql, _, _ := db.From("items").Where(goqu.ExOr{
	"col1": "a",
	"col2": 1,
	"col3": true,
	"col4": false,
	"col5": nil,
	"col6": []string{"a", "b", "c"},
}).ToSql()
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
}).ToSql()
fmt.Println(sql)
```
Output:
```sql
SELECT * FROM "items" WHERE (("col1" != 'a') OR ("col3" IS NOT TRUE) OR ("col6" NOT IN ('a', 'b', 'c')))
```
For a more complete examples see the [`Op`](https://godoc.org/github.com/doug-martin/goqu#Op) and [`ExOr`](https://godoc.org/github.com/doug-martin/goqu#Ex) docs

* [`I()`](https://godoc.org/github.com/doug-martin/goqu#I) - An Identifier represents a schema, table, or column or any combination. You can use this when your expression cannot be expressed via the [`Ex`](https://godoc.org/github.com/doug-martin/goqu#Ex) map (e.g. Cast).
```go
goqu.I("my_schema.table.col")
goqu.I("table.col")
goqu.I("col")
```
If you look at the [`IdentiferExpression`](https://godoc.org/github.com/doug-martin/goqu#IdentifierExpression) docs it implements many of your common sql operations that you would perform.
```go
goqu.I("col").Eq(10)
goqu.I("col").In([]int64{1,2,3,4})
goqu.I("col").Like(regexp.MustCompile("^(a|b)")
goqu.I("col").IsNull()
```
Please see the exmaples for [`I()`](https://godoc.org/github.com/doug-martin/goqu#example-I) to see more in depth examples

* [`L()`](https://godoc.org/github.com/doug-martin/goqu#example-L) - An SQL literal. You may find yourself in a situation where an IdentifierExpression cannot expression an SQL fragment that your database supports. In that case you can use a LiteralExpression
```go
goqu.L(`"col"::TEXT = ""other_col"::text`)
```
You can also use placeholders in your literal. When using the LiteralExpressions placeholders are normalized to the ? character and will be transformed to the correct placeholder for your adapter (e.g. `?` mysql, `$1` postgres, `?` sqlite3)
```go
goqu.L("col IN (?, ?, ?)", "a", "b", "c")
```
Putting it together
```go
sql, _, _ := db.From("test").Where(
   goqu.I("col").Eq(10),
   goqu.L(`"json"::TEXT = "other_json"::TEXT`),
).ToSql()
fmt.Println(sql)
```
```sql
SELECT * FROM "test" WHERE (("col" = 10) AND "json"::TEXT = "other_json"::TEXT)
```
Both the Identifier and Literal expressions will be ANDed together by default.
You may however want to have your expressions ORed together you can use the [`Or()`](https://godoc.org/github.com/doug-martin/goqu#example-Or) function to create an ExpressionList
```go
sql, _, _ := db.From("test").Where(
   goqu.Or(
      goqu.I("col").Eq(10),
      goqu.L(`"col"::TEXT = "other_col"::TEXT`),
   ),
).ToSql()
fmt.Println(sql)
```  
```sql
SELECT * FROM "test" WHERE (("col" = 10) OR "col"::TEXT = "other_col"::TEXT)
```

```go
sql, _, _ := db.From("test").Where(
   Or(
      goqu.I("col").Eq(10),
      goqu.L(`"col"::TEXT = "other_col"::TEXT`),
   ),
).ToSql()
fmt.Println(sql)
```
```sql
SELECT * FROM "test" WHERE (("col" = 10) OR "col"::TEXT = "other_col"::TEXT)
```

You can also use Or and the And function in tandem which will give you control not only over how the Expressions are joined together, but also how they are grouped
```go
sql, _, _ := db.From("test").Where(
   goqu.Or(
      goqu.I("a").Gt(10),
      goqu.And(
         goqu.I("b").Eq(100),
         goqu.I("c").Neq("test"),
      ),
   ),
).ToSql()
fmt.Println(sql)
```
Output:
```sql
SELECT * FROM "test" WHERE (("a" > 10) OR (("b" = 100) AND ("c" != 'test')))
```

You can also use Or with the map syntax
```go
sql, _, _ := db.From("test").Where(
	goqu.Or(
        //Ex will be anded together
		goqu.Ex{
			"col1": nil,
			"col2": true,
		},
		goqu.Ex{
			"col3": nil,
			"col4": false,
		},
		goqu.L(`"col"::TEXT = "other_col"::TEXT`),
	),
).ToSql()
fmt.Println(sql)
```
Output:
```sql
SELECT * FROM "test" WHERE ((("col1" IS NULL) AND ("col2" IS TRUE)) OR (("col3" IS NULL) AND ("col4" IS FALSE)) OR "col"::TEXT = "other_col"::TEXT)
```
<a name="complex-example"></a>
### Complex Example

Using the Ex map syntax
```go
sql, _, _ := db.From("test").
	Select(goqu.COUNT("*")).
	InnerJoin(goqu.I("test2"), goqu.On(goqu.I("test.fkey").Eq(goqu.I("test2.id")))).
	LeftJoin(goqu.I("test3"), goqu.On(goqu.I("test2.fkey").Eq(goqu.I("test3.id")))).
	Where(
	goqu.Ex{
		"test.name":    goqu.Op{"like": regexp.MustCompile("^(a|b)")},
		"test2.amount": goqu.Op{"isNot": nil},
	},
	goqu.ExOr{
		"test3.id":     nil,
		"test3.status": []string{"passed", "active", "registered"},
	}).
	Order(goqu.I("test.created").Desc().NullsLast()).
	GroupBy(goqu.I("test.user_id")).
	Having(goqu.AVG("test3.age").Gt(10)).
	ToSql()
fmt.Println(sql)
```

Using the Expression syntax
```go
sql, _, _ := db.From("test").
    Select(goqu.COUNT("*")).
	InnerJoin(goqu.I("test2"), goqu.On(goqu.I("test.fkey").Eq(goqu.I("test2.id")))).
	LeftJoin(goqu.I("test3"), goqu.On(goqu.I("test2.fkey").Eq(goqu.I("test3.id")))).
	Where(
	    goqu.I("test.name").Like(regexp.MustCompile("^(a|b)")),
	    goqu.I("test2.amount").IsNotNull(),
	    goqu.Or(
		    goqu.I("test3.id").IsNull(),
		    goqu.I("test3.status").In("passed", "active", "registered"),
	)).
	Order(goqu.I("test.created").Desc().NullsLast()).
	GroupBy(goqu.I("test.user_id")).
	Having(goqu.AVG("test3.age").Gt(10)).
	ToSql()
fmt.Println(sql)
```

Both examples generate the following SQL

```sql
SELECT COUNT(*)
FROM "test"
  INNER JOIN "test2" ON ("test"."fkey" = "test2"."id")
  LEFT JOIN "test3" ON ("test2"."fkey" = "test3"."id")
WHERE (
  ("test"."name" ~ '^(a|b)') AND
  ("test2"."amount" IS NOT NULL) AND
  (
      ("test3"."id" IS NULL) OR
      ("test3"."status" IN ('passed', 'active', 'registered'))
  )
)
GROUP BY "test"."user_id"
HAVING (AVG("test3"."age") > 10)
ORDER BY "test"."created" DESC NULLS LAST
```

<a name="querying"></a>
## Querying

goqu also has basic query support through the use of either the Database or the Dataset.

<a name="dataset"></a>
### Dataset

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
    fmt.Println(err.Error())
    return
}
fmt.Printf("\n%+v", users)

var users []User
//SELECT "first_name" FROM "user";
if err := db.From("user").Select("first_name").ScanStructs(&users); err != nil{
    fmt.Println(err.Error())
    return
}
fmt.Printf("\n%+v", users)
```

* [`ScanStruct`](http://godoc.org/github.com/doug-martin/goqu#Dataset.ScanStruct) - scans a row into a slice a struct, returns false if a row wasnt found

**NOTE** [`ScanStruct`](http://godoc.org/github.com/doug-martin/goqu#Dataset.ScanStruct) will only select the columns that can be scanned in to the struct unless you have explicitly selected certain columns.

```go

type User struct{
    FirstName string `db:"first_name"`
    LastName  string `db:"last_name"`
}

var user User
//SELECT "first_name", "last_name" FROM "user" LIMIT 1;
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



* [`ScanVals`](http://godoc.org/github.com/doug-martin/goqu#Dataset.ScanVals) - scans a rows of 1 column into a slice of primitive values
```go
var ids []int64
if err := db.From("user").Select("id").ScanVals(&ids); err != nil{
    fmt.Println(err.Error())
    return
}
fmt.Printf("\n%+v", ids)
```

* [`ScanVal`](http://godoc.org/github.com/doug-martin/goqu#Dataset.ScanVal) - scans a row of 1 column into a primitive value, returns false if a row wasnt found. **Note** when using the dataset a `LIMIT` of 1 is automatically applied.
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

* [`Count`](http://godoc.org/github.com/doug-martin/goqu#Dataset.Count) - Returns the count for the current query
```go
count, err := db.From("user").Count()
if err != nil{
    fmt.Println(err.Error())
    return
}
fmt.Printf("\nCount:= %d", count)
```

* [`Pluck`](http://godoc.org/github.com/doug-martin/goqu#Dataset.Pluck) - Selects a single column and stores the results into a slice of primitive values
```go
var ids []int64
if err := db.From("user").Pluck(&ids, "id"); err != nil{
    fmt.Println(err.Error())
    return
}
fmt.Printf("\nIds := %+v", ids)
```

* [`Insert`](http://godoc.org/github.com/doug-martin/goqu#Dataset.Insert) - Creates an `INSERT` statement and returns a [`CrudExec`](http://godoc.org/github.com/doug-martin/goqu#CrudExec) to execute the statement
```go
insert := db.From("user").Insert(goqu.Record{"first_name": "Bob", "last_name":"Yukon", "created": time.Now()})
if _, err := insert.Exec(); err != nil{
    fmt.Println(err.Error())
    return
}
```
Insert will also handle multi inserts if supported by the database
```go
users := []goqu.Record{
    {"first_name": "Bob", "last_name":"Yukon", "created": time.Now()},
    {"first_name": "Sally", "last_name":"Yukon", "created": time.Now()},
    {"first_name": "Jimmy", "last_name":"Yukon", "created": time.Now()},
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
    {"first_name": "Bob", "last_name":"Yukon", "created": time.Now()},
    {"first_name": "Sally", "last_name":"Yukon", "created": time.Now()},
    {"first_name": "Jimmy", "last_name":"Yukon", "created": time.Now()},
}
if err := db.From("user").Returning(goqu.I("id")).Insert(users).ScanVals(&ids); err != nil{
    fmt.Println(err.Error())
    return
}
```

* [`Update`](http://godoc.org/github.com/doug-martin/goqu#Dataset.Update) - Creates an `UPDATE` statement and returns an[`CrudExec`](http://godoc.org/github.com/doug-martin/goqu#CrudExec) to execute the statement
```go
update := db.From("user").
    Where(goqu.I("status").Eq("inactive")).
    Update(goqu.Record{"password": nil, "updated": time.Now()})
if _, err := update.Exec(); err != nil{
    fmt.Println(err.Error())
    return
}
``````
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
* [`Delete`](http://godoc.org/github.com/doug-martin/goqu#Dataset.Delete) - Creates an `DELETE` statement and returns a [`CrudExec`](http://godoc.org/github.com/doug-martin/goqu#CrudExec) to execute the statement
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
    Where(goqu.I("status").Eq("paid")).
    Returning(goqu.I("id")).
    Delete()
if err := delete.ScanVals(&ids); err != nil{
    fmt.Println(err.Error())
    return
}
```

<a name="dataset_prepared"></a>
#### Prepared Statements

By default the `Dataset` will interpolate all parameters, if you do not want to have values interpolated you can use the [`Prepared`](http://godoc.org/github.com/doug-martin/goqu#Dataset.Prepared) method to prevent this.

**Note** For the examples all placeholders are `?` this will be adapter specific when using other examples (e.g. Postgres `$1, $2...`)

```go

preparedDs := db.From("items").Prepared(true)

sql, args, _ := preparedDs.Where(goqu.Ex{
	"col1": "a",
	"col2": 1,
	"col3": true,
	"col4": false,
	"col5": []string{"a", "b", "c"},
}).ToSql()
fmt.Println(sql, args)

sql, args, _ = preparedDs.ToInsertSql(
	goqu.Record{"name": "Test1", "address": "111 Test Addr"},
	goqu.Record{"name": "Test2", "address": "112 Test Addr"},
)
fmt.Println(sql, args)

sql, args, _ = preparedDs.ToUpdateSql(
	goqu.Record{"name": "Test", "address": "111 Test Addr"},
)
fmt.Println(sql, args)

sql, args, _ = preparedDs.
	Where(goqu.Ex{"id": goqu.Op{"gt": 10}}).
	ToDeleteSql()
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
  if _, err = update.Exec(); err != nil{
      return err
  }
  return nil
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


<a name="adapters"></a>
## Adapters

Adapters in goqu are the foundation of building the correct SQL for each DB dialect.

Between most dialects there is a large portion of shared syntax, for this reason we have a [`DefaultAdapter`](http://godoc.org/github.com/doug-martin/goqu/#DefaultAdapter) that can be used as a base for any new Dialect specific adapter.
In fact for most use cases you will not have to override any methods but instead just override the default values as documented for [`DefaultAdapter`](http://godoc.org/github.com/doug-martin/goqu/#DefaultAdapter).

### Literal

The [`DefaultAdapter`](http://godoc.org/github.com/doug-martin/goqu/#DefaultAdapter) has a [`Literal`](http://godoc.org/github.com/doug-martin/goqu/#DefaultAdapter.Literal) function which should be used to serialize all sub expressions or values. This method prevents you from having to re-implement each adapter method while having your adapter methods called correctly.

**How does it work?**

The Literal method delegates back to the [`Dataset.Literal`](http://godoc.org/github.com/doug-martin/goqu/#Dataset.Literal) method which then calls the appropriate method on the adapter acting as a trampoline, between the DefaultAdapter and your Adapter.

For example if your adapter overrode the [`DefaultAdapter.QuoteIdentifier`](http://godoc.org/github.com/doug-martin/goqu/#DefaultAdapter.QuoteIdentifier), method which is used by most methods in the [`DefaultAdapter`](http://godoc.org/github.com/doug-martin/goqu/#DefaultAdapter), we need to ensure that your Adapters QuoteIdentifier method is called instead of the default implementation.

Because the Dataset has a pointer to your Adapter it will call the correct method, so instead of calling `DefaultAdapter.QuoteIdentifier` internally we delegate back to the Dataset by calling the [`Dataset.Literal`](http://godoc.org/github.com/doug-martin/goqu/#Dataset.Literal) which will the call your Adapters method.

```
Dataset.Literal -> Adapter.ExpressionListSql -> Adapter.Literal -> Dataset.Literal -> YourAdapter.QuoteIdentifier
```

It is important to maintain this pattern when writing your own Adapter.

### Registering

When creating your adapters you must register your adapter with [`RegisterAdapter`](http://godoc.org/github.com/doug-martin/goqu/#RegisterAdapter). This method requires 2 arguments.

1. dialect - The dialect for your adapter.
2. datasetAdapterFactory - This is a factory function that will return a new goqu.Adapter  used to create the dialect specific SQL.


For example the code for the postgres adapter is fairly short.
```go
package postgres

import (
    "github.com/doug-martin/goqu/v6"
)

//postgres requires a $ placeholder for prepared statements
const placeholder_rune = '$'

func newDatasetAdapter(ds *goqu.Dataset) goqu.Adapter {
    ret := goqu.NewDefaultAdapter(ds).(*goqu.DefaultAdapter)

    //override the settings required
    ret.PlaceHolderRune = placeholder_rune
    //postgres requires a paceholder number (e.g. $1)
    ret.IncludePlaceholderNum = true
    return ret
}

func init() {
    //register our adapter with goqu
    goqu.RegisterAdapter("postgres", newDatasetAdapter)
}
```

If you are looking to write your own adapter take a look at the postgresm, mysql or sqlite3 adapter located at <https://github.com/doug-martin/goqu/tree/master/adapters>.

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

