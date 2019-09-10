# Selecting

* [Creating a SelectDataset](#create)
* Building SQL
  * [`Select`](#select)
  * [`Distinct`](#distinct)
  * [`From`](#from)
  * [`Join`](#joins)
  * [`Where`](#where)
  * [`Limit`](#limit)
  * [`Offset`](#offset)
  * [`GroupBy`](#group_by)
  * [`Having`](#having)
  * [`Window`](#window)
  * [`SetError`](#seterror)
* Executing Queries
  * [`ScanStructs`](#scan-structs) - Scans rows into a slice of structs
  * [`ScanStruct`](#scan-struct) - Scans a row into a slice a struct, returns false if a row wasnt found
  * [`ScanVals`](#scan-vals)- Scans a rows of 1 column into a slice of primitive values
  * [`ScanVal`](#scan-val) - Scans a row of 1 column into a primitive value, returns false if a row wasnt found.
  * [`Count`](#count) - Returns the count for the current query
  * [`Pluck`](#pluck) - Selects a single column and stores the results into a slice of primitive values

<a name="create"></a>
To create a [`SelectDataset`](https://godoc.org/github.com/doug-martin/goqu/#SelectDataset)  you can use

**[`goqu.From`](https://godoc.org/github.com/doug-martin/goqu/#From) and [`goqu.Select`](https://godoc.org/github.com/doug-martin/goqu/#Select)**

When you just want to create some quick SQL, this mostly follows the `Postgres` with the exception of placeholders for prepared statements.

```go
sql, _, _ := goqu.From("table").ToSQL()
fmt.Println(sql)

sql, _, _ := goqu.Select(goqu.L("NOW()")).ToSQL()
fmt.Println(sql)
```
Output:
```
SELECT * FROM "table"
SELECT NOW()
```

**[`DialectWrapper.From`](https://godoc.org/github.com/doug-martin/goqu/#DialectWrapper.From) and [`DialectWrapper.Select`](https://godoc.org/github.com/doug-martin/goqu/#DialectWrapper.Select)**

Use this when you want to create SQL for a specific `dialect`

```go
// import _ "github.com/doug-martin/goqu/v8/dialect/mysql"

dialect := goqu.Dialect("mysql")

sql, _, _ := dialect.From("table").ToSQL()
fmt.Println(sql)

sql, _, _ := dialect.Select(goqu.L("NOW()")).ToSQL()
fmt.Println(sql)
```
Output:
```
SELECT * FROM `table`
SELECT NOW()
```

**[`Database.From`](https://godoc.org/github.com/doug-martin/goqu/#DialectWrapper.From) and [`Database.Select`](https://godoc.org/github.com/doug-martin/goqu/#DialectWrapper.From)**

Use this when you want to execute the SQL or create SQL for the drivers dialect.

```go
// import _ "github.com/doug-martin/goqu/v8/dialect/mysql"

mysqlDB := //initialize your db
db := goqu.New("mysql", mysqlDB)

sql, _, _ := db.From("table").ToSQL()
fmt.Println(sql)

sql, _, _ := db.Select(goqu.L("NOW()")).ToSQL()
fmt.Println(sql)
```
Output:
```
SELECT * FROM `table`
SELECT NOW()
```

### Examples

For more examples visit the **[Docs](https://godoc.org/github.com/doug-martin/goqu/#SelectDataset)**

<a name="select"></a>
**[`Select`](https://godoc.org/github.com/doug-martin/goqu/#SelectDataset.Select)**

```go
sql, _, _ := goqu.From("test").Select("a", "b", "c").ToSQL()
fmt.Println(sql)
```

Output:
```sql
SELECT "a", "b", "c" FROM "test"
```

You can also ues another dataset in your select

```go
ds := goqu.From("test")
fromDs := ds.Select("age").Where(goqu.C("age").Gt(10))
sql, _, _ := ds.From().Select(fromDs).ToSQL()
fmt.Println(sql)
```

Output:
```
SELECT (SELECT "age" FROM "test" WHERE ("age" > 10))
```

Selecting a literal

```go
sql, _, _ := goqu.From("test").Select(goqu.L("a + b").As("sum")).ToSQL()
fmt.Println(sql)
```

Output:
```
SELECT a + b AS "sum" FROM "test"
```

Select aggregate functions

```go
sql, _, _ := goqu.From("test").Select(
	goqu.COUNT("*").As("age_count"),
	goqu.MAX("age").As("max_age"),
	goqu.AVG("age").As("avg_age"),
).ToSQL()
fmt.Println(sql)
```

Output:
```
SELECT COUNT(*) AS "age_count", MAX("age") AS "max_age", AVG("age") AS "avg_age" FROM "test"
```

Selecting columns from a struct

```go
ds := goqu.From("test")

type myStruct struct {
	Name         string
	Address      string `db:"address"`
	EmailAddress string `db:"email_address"`
}

// Pass with pointer
sql, _, _ := ds.Select(&myStruct{}).ToSQL()
fmt.Println(sql)
```

Output:
```
SELECT "address", "email_address", "name" FROM "test"
```

<a name="distinct"></a>
**[`Distinct`](https://godoc.org/github.com/doug-martin/goqu/#SelectDataset.Distinct)**

```go
sql, _, _ := goqu.From("test").Select("a", "b").Distinct().ToSQL()
fmt.Println(sql)
```

Output:
```
SELECT DISTINCT "a", "b" FROM "test"
```

If you dialect supports `DISTINCT ON` you provide arguments to the `Distinct` method.

**NOTE** currently only the `postgres` and the default dialects support `DISTINCT ON` clauses

```go
sql, _, _ := goqu.From("test").Distinct("a").ToSQL()
fmt.Println(sql)
```
Output:

```
SELECT DISTINCT ON ("a") * FROM "test"
```

You can also provide other expression arguments

With `goqu.L`

```go
sql, _, _ := goqu.From("test").Distinct(goqu.L("COALESCE(?, ?)", goqu.C("a"), "empty")).ToSQL()
fmt.Println(sql)
```
Output:
```
SELECT DISTINCT ON (COALESCE("a", 'empty')) * FROM "test"
```
With `goqu.Coalesce`
```go
sql, _, _ := goqu.From("test").Distinct(goqu.COALESCE(goqu.C("a"), "empty")).ToSQL()
fmt.Println(sql)
```
Output:
```
SELECT DISTINCT ON (COALESCE("a", 'empty')) * FROM "test"
```

<a name="from"></a>
**[`From`](https://godoc.org/github.com/doug-martin/goqu/#SelectDataset.From)**

Overriding the original from
```go
ds := goqu.From("test")
sql, _, _ := ds.From("test2").ToSQL()
fmt.Println(sql)
```

Output:
```
SELECT * FROM "test2"
```

From another dataset

```go
ds := goqu.From("test")
fromDs := ds.Where(goqu.C("age").Gt(10))
sql, _, _ := ds.From(fromDs).ToSQL()
fmt.Println(sql)
```

Output:
```
SELECT * FROM (SELECT * FROM "test" WHERE ("age" > 10)) AS "t1"
```

From an aliased dataset

```go
ds := goqu.From("test")
fromDs := ds.Where(goqu.C("age").Gt(10))
sql, _, _ := ds.From(fromDs.As("test2")).ToSQL()
fmt.Println(sql)
```

Output:
```
SELECT * FROM (SELECT * FROM "test" WHERE ("age" > 10)) AS "test2"
```

<a name="joins"></a>
**[`Join`](https://godoc.org/github.com/doug-martin/goqu/#SelectDataset.Join)**

```go
sql, _, _ := goqu.From("test").Join(
	goqu.T("test2"),
	goqu.On(goqu.Ex{"test.fkey": goqu.I("test2.Id")}),
).ToSQL()
fmt.Println(sql)
```

Output:
```
SELECT * FROM "test" INNER JOIN "test2" ON ("test"."fkey" = "test2"."Id")
```

[`InnerJoin`](https://godoc.org/github.com/doug-martin/goqu/#SelectDataset.InnerJoin)

```go
sql, _, _ := goqu.From("test").InnerJoin(
	goqu.T("test2"),
	goqu.On(goqu.Ex{"test.fkey": goqu.I("test2.Id")}),
).ToSQL()
fmt.Println(sql)
```

Output:
```
SELECT * FROM "test" INNER JOIN "test2" ON ("test"."fkey" = "test2"."Id")
```

[`FullOuterJoin`](https://godoc.org/github.com/doug-martin/goqu/#SelectDataset.FullOuterJoin)

```go
sql, _, _ := goqu.From("test").FullOuterJoin(
	goqu.T("test2"),
	goqu.On(goqu.Ex{
		"test.fkey": goqu.I("test2.Id"),
	}),
).ToSQL()
fmt.Println(sql)
```

Output:
```
SELECT * FROM "test" FULL OUTER JOIN "test2" ON ("test"."fkey" = "test2"."Id")
```

[`RightOuterJoin`](https://godoc.org/github.com/doug-martin/goqu/#SelectDataset.RightOuterJoin)

```go
sql, _, _ := goqu.From("test").RightOuterJoin(
	goqu.T("test2"),
	goqu.On(goqu.Ex{
		"test.fkey": goqu.I("test2.Id"),
	}),
).ToSQL()
fmt.Println(sql)
```

Output:
```
SELECT * FROM "test" RIGHT OUTER JOIN "test2" ON ("test"."fkey" = "test2"."Id")
```

[`LeftOuterJoin`](https://godoc.org/github.com/doug-martin/goqu/#SelectDataset.LeftOuterJoin)

```go
sql, _, _ := goqu.From("test").LeftOuterJoin(
	goqu.T("test2"),
	goqu.On(goqu.Ex{
		"test.fkey": goqu.I("test2.Id"),
	}),
).ToSQL()
fmt.Println(sql)
```

Output:
```
SELECT * FROM "test" LEFT OUTER JOIN "test2" ON ("test"."fkey" = "test2"."Id")
```

[`FullJoin`](https://godoc.org/github.com/doug-martin/goqu/#SelectDataset.FullJoin)

```go
sql, _, _ := goqu.From("test").FullJoin(
	goqu.T("test2"),
	goqu.On(goqu.Ex{
		"test.fkey": goqu.I("test2.Id"),
	}),
).ToSQL()
fmt.Println(sql)
```

Output:
```
SELECT * FROM "test" FULL JOIN "test2" ON ("test"."fkey" = "test2"."Id")
```


[`RightJoin`](https://godoc.org/github.com/doug-martin/goqu/#SelectDataset.RightJoin)

```go
sql, _, _ := goqu.From("test").RightJoin(
	goqu.T("test2"),
	goqu.On(goqu.Ex{
		"test.fkey": goqu.I("test2.Id"),
	}),
).ToSQL()
fmt.Println(sql)
```

Output:
```
SELECT * FROM "test" RIGHT JOIN "test2" ON ("test"."fkey" = "test2"."Id")
```

[`LeftJoin`](https://godoc.org/github.com/doug-martin/goqu/#SelectDataset.LeftJoin)

```go
sql, _, _ := goqu.From("test").LeftJoin(
	goqu.T("test2"),
	goqu.On(goqu.Ex{
		"test.fkey": goqu.I("test2.Id"),
	}),
).ToSQL()
fmt.Println(sql)
```

Output:
```
SELECT * FROM "test" LEFT JOIN "test2" ON ("test"."fkey" = "test2"."Id")
```

[`NaturalJoin`](https://godoc.org/github.com/doug-martin/goqu/#SelectDataset.NaturalJoin)

```go
sql, _, _ := goqu.From("test").NaturalJoin(goqu.T("test2")).ToSQL()
fmt.Println(sql)
```

Output:
```
SELECT * FROM "test" NATURAL JOIN "test2"
```

[`NaturalLeftJoin`](https://godoc.org/github.com/doug-martin/goqu/#SelectDataset.NaturalLeftJoin)

```go
sql, _, _ := goqu.From("test").NaturalLeftJoin(goqu.T("test2")).ToSQL()
fmt.Println(sql)
```

Output:
```
SELECT * FROM "test" NATURAL LEFT JOIN "test2"
```

[`NaturalRightJoin`](https://godoc.org/github.com/doug-martin/goqu/#SelectDataset.NaturalRightJoin)

```go
sql, _, _ := goqu.From("test").NaturalRightJoin(goqu.T("test2")).ToSQL()
fmt.Println(sql)
```

Output:
```
SELECT * FROM "test" NATURAL RIGHT LEFT JOIN "test2"
```

[`NaturalFullJoin`](https://godoc.org/github.com/doug-martin/goqu/#SelectDataset.NaturalFullJoin)

```go
sql, _, _ := goqu.From("test").NaturalFullJoin(goqu.T("test2")).ToSQL()
fmt.Println(sql)
```

Output:
```
SELECT * FROM "test" NATURAL FULL LEFT JOIN "test2"
```

[`CrossJoin`](https://godoc.org/github.com/doug-martin/goqu/#SelectDataset.CrossJoin)

```go
sql, _, _ := goqu.From("test").CrossJoin(goqu.T("test2")).ToSQL()
fmt.Println(sql)
```

Output:
```
SELECT * FROM "test" CROSS JOIN "test2"
```

<a name="where"></a>
**[`Where`](https://godoc.org/github.com/doug-martin/goqu/#SelectDataset.Where)**

You can use `goqu.Ex` to create an ANDed condition
```go
sql, _, _ := goqu.From("test").Where(goqu.Ex{
	"a": goqu.Op{"gt": 10},
	"b": goqu.Op{"lt": 10},
	"c": nil,
	"d": []string{"a", "b", "c"},
}).ToSQL()
fmt.Println(sql)
```

Output:

```
SELECT * FROM "test" WHERE (("a" > 10) AND ("b" < 10) AND ("c" = NULL) AND ("d" IN ('a', 'b', 'c')))
```

You can use `goqu.ExOr` to create an ORed condition

```go
sql, _, _ := goqu.From("test").Where(goqu.ExOr{
	"a": goqu.Op{"gt": 10},
	"b": goqu.Op{"lt": 10},
	"c": nil,
	"d": []string{"a", "b", "c"},
}).ToSQL()
fmt.Println(sql)
```

Output:
```
SELECT * FROM "test" WHERE (("a" > 10) OR ("b" < 10) OR ("c" = NULL) OR ("d" IN ('a', 'b', 'c')))
```

You can use `goqu.Ex` with `goqu.ExOr` for complex expressions

```go
// You can use Or with Ex to Or multiple Ex maps together
sql, _, _ := goqu.From("test").Where(
	goqu.Or(
		goqu.Ex{
			"a": goqu.Op{"gt": 10},
			"b": goqu.Op{"lt": 10},
		},
		goqu.Ex{
			"c": nil,
			"d": []string{"a", "b", "c"},
		},
	),
).ToSQL()
fmt.Println(sql)
```

Output:

```
SELECT * FROM "test" WHERE ((("a" > 10) AND ("b" < 10)) OR (("c" = NULL) AND ("d" IN ('a', 'b', 'c'))))
```

You can also use identifiers to create your where condition

```go
sql, _, _ := goqu.From("test").Where(
	goqu.C("a").Gt(10),
	goqu.C("b").Lt(10),
	goqu.C("c").IsNull(),
	goqu.C("d").In("a", "b", "c"),
).ToSQL()
fmt.Println(sql)
```

Output:
```
SELECT * FROM "test" WHERE (("a" > 10) AND ("b" < 10) AND ("c" = NULL) AND ("d" IN ('a', 'b', 'c')))
```

Using `goqu.Or` to create ORed expression

```go
// You can use a combination of Ors and Ands
sql, _, _ := goqu.From("test").Where(
	goqu.Or(
		goqu.C("a").Gt(10),
		goqu.And(
			goqu.C("b").Lt(10),
			goqu.C("c").IsNull(),
		),
	),
).ToSQL()
fmt.Println(sql)
```

Output:

```
SELECT * FROM "test" WHERE (("a" > 10) OR (("b" < 10) AND ("c" = NULL)))
```

<a name="limit"></a>
**[`Limit`](https://godoc.org/github.com/doug-martin/goqu/#SelectDataset.Limit)**

```go
ds := goqu.From("test").Limit(10)
sql, _, _ := ds.ToSQL()
fmt.Println(sql)
```

Output:

```
SELECT * FROM "test" LIMIT 10
```

<a name="offset"></a>
**[`Offset`](https://godoc.org/github.com/doug-martin/goqu/#SelectDataset.Offset)**

```go
ds := goqu.From("test").Offset(2)
sql, _, _ := ds.ToSQL()
fmt.Println(sql)
```

Output:

```
SELECT * FROM "test" OFFSET 2
```

<a name="group_by"></a>
**[`GroupBy`](https://godoc.org/github.com/doug-martin/goqu/#SelectDataset.GroupBy)**

```go
sql, _, _ := goqu.From("test").
	Select(goqu.SUM("income").As("income_sum")).
	GroupBy("age").
	ToSQL()
fmt.Println(sql)
```

Output:

```
SELECT SUM("income") AS "income_sum" FROM "test" GROUP BY "age"
```

<a name="having"></a>
**[`Having`](https://godoc.org/github.com/doug-martin/goqu/#SelectDataset.Having)**

```go
sql, _, _ = goqu.From("test").GroupBy("age").Having(goqu.SUM("income").Gt(1000)).ToSQL()
fmt.Println(sql)
```

Output:

```
SELECT * FROM "test" GROUP BY "age" HAVING (SUM("income") > 1000)
```


<a name="window"></a>
**[`Window Function`](https://godoc.org/github.com/doug-martin/goqu/#SelectDataset.Window)**

**NOTE** currently only the `postgres`, `mysql8` (NOT `mysql`) and the default dialect support `Window Function`

To use windowing in select you can use the `Over` method on an `SQLFunction`

```go
sql, _, _ := goqu.From("test").Select(
	goqu.ROW_NUMBER().Over(goqu.W().PartitionBy("a").OrderBy(goqu.I("b").Asc())),
)
fmt.Println(sql)
```

Output:

```
SELECT ROW_NUMBER() OVER (PARTITION BY "a" ORDER BY "b") FROM "test"
```

`goqu` also supports the `WINDOW` clause.

```go
sql, _, _ := goqu.From("test").
	Select(goqu.ROW_NUMBER().OverName(goqu.I("w"))).
	Window(goqu.W("w").PartitionBy("a").OrderBy(goqu.I("b").Asc()))
fmt.Println(sql)
```

Output:

```
SELECT ROW_NUMBER() OVER "w" FROM "test" WINDOW "w" AS (PARTITION BY "a" ORDER BY "b")
```

<a name="seterror"></a>
**[`SetError`](https://godoc.org/github.com/doug-martin/goqu/#SelectDataset.SetError)**

Sometimes while building up a query with goqu you will encounter situations where certain
preconditions are not met or some end-user contraint has been violated. While you could
track this error case separately, goqu provides a convenient built-in mechanism to set an
error on a dataset if one has not already been set to simplify query building.

Set an Error on a dataset:

```go
func GetSelect(name string) *goqu.SelectDataset {

    var ds = goqu.From("test")

    if len(name) == 0 {
        return ds.SetError(fmt.Errorf("name is empty"))
    }

    return ds.Select(name)
}

```

This error is returned on any subsequent call to `Error` or `ToSQL`:

```go
var name string = ""
ds = GetSelect(name)
fmt.Println(ds.Error())

sql, args, err = ds.ToSQL()
fmt.Println(err)
```

Output:
```
name is empty
name is empty
```

## Executing Queries

To execute your query use [`goqu.Database#From`](https://godoc.org/github.com/doug-martin/goqu/#Database.From) to create your dataset

<a name="scan-structs"></a>
**[`ScanStructs`](http://godoc.org/github.com/doug-martin/goqu#SelectDataset.ScanStructs)**

Scans rows into a slice of structs

**NOTE** [`ScanStructs`](http://godoc.org/github.com/doug-martin/goqu#SelectDataset.ScanStructs) will only select the columns that can be scanned in to the structs unless you have explicitly selected certain columns.

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

<a name="scan-struct"></a>
**[`ScanStruct`](http://godoc.org/github.com/doug-martin/goqu#SelectDataset.ScanStruct)**

Scans a row into a slice a struct, returns false if a row wasnt found

**NOTE** [`ScanStruct`](http://godoc.org/github.com/doug-martin/goqu#SelectDataset.ScanStruct) will only select the columns that can be scanned in to the struct unless you have explicitly selected certain columns.

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

<a name="scan-vals"></a>
**[`ScanVals`](http://godoc.org/github.com/doug-martin/goqu#SelectDataset.ScanVals)**

Scans a rows of 1 column into a slice of primitive values

```go
var ids []int64
if err := db.From("user").Select("id").ScanVals(&ids); err != nil{
  fmt.Println(err.Error())
  return
}
fmt.Printf("\n%+v", ids)
```

<a name="scan-val"></a>
[`ScanVal`](http://godoc.org/github.com/doug-martin/goqu#SelectDataset.ScanVal)

Scans a row of 1 column into a primitive value, returns false if a row wasnt found.

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

<a name="count"></a>
**[`Count`](http://godoc.org/github.com/doug-martin/goqu#SelectDataset.Count)**

Returns the count for the current query

```go
count, err := db.From("user").Count()
if err != nil{
  fmt.Println(err.Error())
  return
}
fmt.Printf("\nCount:= %d", count)
```

<a name="pluck"></a>
**[`Pluck`](http://godoc.org/github.com/doug-martin/goqu#SelectDataset.Pluck)**

Selects a single column and stores the results into a slice of primitive values

```go
var ids []int64
if err := db.From("user").Pluck(&ids, "id"); err != nil{
  fmt.Println(err.Error())
  return
}
fmt.Printf("\nIds := %+v", ids)
```

