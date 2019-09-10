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
// SELECT * FROM "items" WHERE (("col1" = ?) AND ("col2" = ?) AND ("col3" = TRUE) AND ("col4" = FALSE) AND ("col5" IN (?, ?, ?))) [a 1 a b c]
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

