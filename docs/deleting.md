# Deleting

* [Creating A DeleteDataset](#create)
* Examples
  * [Delete All](#delete-all)
  * [Prepared](#prepared)
  * [Where](#where)
  * [Order](#order)
  * [Limit](#limit)
  * [Returning](#returning)
  * [SetError](#seterror)
  * [Executing](#exec)

<a name="create"></a>
To create a [`DeleteDataset`](https://godoc.org/github.com/doug-martin/goqu/#DeleteDataset)  you can use

**[`goqu.Delete`](https://godoc.org/github.com/doug-martin/goqu/#Delete)**

When you just want to create some quick SQL, this mostly follows the `Postgres` with the exception of placeholders for prepared statements.

```go
sql, _, _ := goqu.Delete("table").ToSQL()
fmt.Println(sql)
```
Output:
```
DELETE FROM "table"
```

**[`SelectDataset.Delete`](https://godoc.org/github.com/doug-martin/goqu/#SelectDataset.Delete)**

If you already have a `SelectDataset` you can invoke `Delete()` to get a `DeleteDataset`

**NOTE** This method will also copy over the `WITH`, `WHERE`, `ORDER`, and `LIMIT` from the `SelectDataset`

```go

ds := goqu.From("table")

sql, _, _ := ds.Delete().ToSQL()
fmt.Println(sql)

sql, _, _ = ds.Where(goqu.C("foo").Eq("bar")).Delete().ToSQL()
fmt.Println(sql)
```
Output:
```
DELETE FROM "table"
DELETE FROM "table" WHERE "foo"='bar'
```

**[`DialectWrapper.Delete`](https://godoc.org/github.com/doug-martin/goqu/#DialectWrapper.Delete)**

Use this when you want to create SQL for a specific `dialect`

```go
// import _ "github.com/doug-martin/goqu/v8/dialect/mysql"

dialect := goqu.Dialect("mysql")

sql, _, _ := dialect.Delete("table").ToSQL()
fmt.Println(sql)
```
Output:
```
DELETE FROM `table`
```

**[`Database.Delete`](https://godoc.org/github.com/doug-martin/goqu/#DialectWrapper.Delete)**

Use this when you want to execute the SQL or create SQL for the drivers dialect.

```go
// import _ "github.com/doug-martin/goqu/v8/dialect/mysql"

mysqlDB := //initialize your db
db := goqu.New("mysql", mysqlDB)

sql, _, _ := db.Delete("table").ToSQL()
fmt.Println(sql)
```
Output:
```
DELETE FROM `table`
```

### Examples

For more examples visit the **[Docs](https://godoc.org/github.com/doug-martin/goqu/#DeleteDataset)**

<a name="delete-all"></a>
**Delete All Records**

```go
ds := goqu.Delete("items")

sql, args, _ := ds.ToSQL()
fmt.Println(sql, args)
```

Output:
```
DELETE FROM "items" []
```

<a name="prepared"></a>
**[`Prepared`](https://godoc.org/github.com/doug-martin/goqu/#DeleteDataset.Prepared)**

```go
sql, _, _ := goqu.Delete("test").Where(goqu.Ex{
	"a": goqu.Op{"gt": 10},
	"b": goqu.Op{"lt": 10},
	"c": nil,
	"d": []string{"a", "b", "c"},
}).ToSQL()
fmt.Println(sql)
```

Output:
```
DELETE FROM "test" WHERE (("a" > ?) AND ("b" < ?) AND ("c" = NULL) AND ("d" IN (?, ?, ?))) [10 10 a b c]
```

<a name="where"></a>
**[`Where`](https://godoc.org/github.com/doug-martin/goqu/#DeleteDataset.Where)**

```go
sql, _, _ := goqu.Delete("test").Where(goqu.Ex{
	"a": goqu.Op{"gt": 10},
	"b": goqu.Op{"lt": 10},
	"c": nil,
	"d": []string{"a", "b", "c"},
}).ToSQL()
fmt.Println(sql)
```

Output:
```
DELETE FROM "test" WHERE (("a" > 10) AND ("b" < 10) AND ("c" = NULL) AND ("d" IN ('a', 'b', 'c')))
```

<a name="order"></a>
**[`Order`](https://godoc.org/github.com/doug-martin/goqu/#DeleteDataset.Order)**

**NOTE** This will only work if your dialect supports it

```go
// import _ "github.com/doug-martin/goqu/v8/dialect/mysql"

ds := goqu.Dialect("mysql").Delete("test").Order(goqu.C("a").Asc())
sql, _, _ := ds.ToSQL()
fmt.Println(sql)
```

Output:
```
DELETE FROM `test` ORDER BY `a` ASC
```

<a name="limit"></a>
**[`Limit`](https://godoc.org/github.com/doug-martin/goqu/#DeleteDataset.Limit)**

**NOTE** This will only work if your dialect supports it

```go
// import _ "github.com/doug-martin/goqu/v8/dialect/mysql"

ds := goqu.Dialect("mysql").Delete("test").Limit(10)
sql, _, _ := ds.ToSQL()
fmt.Println(sql)
```

Output:
```
DELETE FROM `test` LIMIT 10
```

<a name="returning"></a>
**[`Returning`](https://godoc.org/github.com/doug-martin/goqu/#DeleteDataset.Returning)**

Returning a single column example.

```go
ds := goqu.Delete("items")
sql, args, _ := ds.Returning("id").ToSQL()
fmt.Println(sql, args)
```

Output:
```
DELETE FROM "items" RETURNING "id" []
```

Returning multiple columns

```go
sql, _, _ := goqu.Delete("test").Returning("a", "b").ToSQL()
fmt.Println(sql)
```

Output:
```
DELETE FROM "items" RETURNING "a", "b"
```

Returning all columns

```go
sql, _, _ := goqu.Delete("test").Returning(goqu.T("test").All()).ToSQL()
fmt.Println(sql)
```

Output:
```
DELETE FROM "test" RETURNING "test".*
```

<a name="seterror"></a>
**[`SetError`](https://godoc.org/github.com/doug-martin/goqu/#DeleteDataset.SetError)**

Sometimes while building up a query with goqu you will encounter situations where certain
preconditions are not met or some end-user contraint has been violated. While you could
track this error case separately, goqu provides a convenient built-in mechanism to set an
error on a dataset if one has not already been set to simplify query building.

Set an Error on a dataset:

```go
func GetDelete(name string, value string) *goqu.DeleteDataset {

    var ds = goqu.Delete("test")

    if len(name) == 0 {
        return ds.SetError(fmt.Errorf("name is empty"))
    }

    if len(value) == 0 {
        return ds.SetError(fmt.Errorf("value is empty"))
    }

    return ds.Where(goqu.C(name).Eq(value))
}

```

This error is returned on any subsequent call to `Error` or `ToSQL`:

```go
var field, value string
ds = GetDelete(field, value)
fmt.Println(ds.Error())

sql, args, err = ds.ToSQL()
fmt.Println(err)
```

Output:
```
name is empty
name is empty
```

## Executing Deletes

To execute DELETES use [`Database.Delete`](https://godoc.org/github.com/doug-martin/goqu/#Database.Delete) to create your dataset

### Examples

<a name="exec"></a>
**Executing a Delete**
```go
db := getDb()

de := db.Delete("goqu_user").
	Where(goqu.Ex{"first_name": "Bob"}).
	Executor()

if r, err := de.Exec(); err != nil {
	fmt.Println(err.Error())
} else {
	c, _ := r.RowsAffected()
	fmt.Printf("Deleted %d users", c)
}
```

Output:

```
Deleted 1 users
```

If you use the RETURNING clause you can scan into structs or values.

```go
db := getDb()

de := db.Delete("goqu_user").
	Where(goqu.C("last_name").Eq("Yukon")).
	Returning(goqu.C("id")).
	Executor()

var ids []int64
if err := de.ScanVals(&ids); err != nil {
	fmt.Println(err.Error())
} else {
	fmt.Printf("Deleted users [ids:=%+v]", ids)
}
```

Output:

```
Deleted users [ids:=[1 2 3]]
```
