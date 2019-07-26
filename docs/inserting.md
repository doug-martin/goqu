# Inserting

* [Creating An InsertDataset](#create)
* Examples
  * [Insert Cols and Vals](#insert-cols-vals)
  * [Insert `goqu.Record`](#insert-record)
  * [Insert Structs](#insert-structs)
  * [Insert Map](#insert-map)
  * [Insert From Query](#insert-from-query)
  * [Returning](#returning)
  * [Executing](#executing)
  
<a name="create"></a>  
To create a [`InsertDataset`](https://godoc.org/github.com/doug-martin/goqu/#InsertDataset)  you can use

**[`goqu.Insert`](https://godoc.org/github.com/doug-martin/goqu/#Insert)**

When you just want to create some quick SQL, this mostly follows the `Postgres` with the exception of placeholders for prepared statements.

```go
ds := goqu.Insert("user").Rows(
    goqu.Record{"first_name": "Greg", "last_name": "Farley"},
)
insertSQL, _, _ := ds.ToSQL()
fmt.Println(insertSQL, args)
```
Output:
```
INSERT INTO "user" ("first_name", "last_name") VALUES ('Greg', 'Farley')
```

**[`SelectDataset.Insert`](https://godoc.org/github.com/doug-martin/goqu/#SelectDataset.Insert)**

If you already have a `SelectDataset` you can invoke `Insert()` to get a `InsertDataset`

**NOTE** This method will also copy over the `WITH` clause as well as the `FROM`

```go
ds := goqu.From("user")

ds := ds.Insert().Rows(
    goqu.Record{"first_name": "Greg", "last_name": "Farley"},
)
insertSQL, _, _ := ds.ToSQL()
fmt.Println(insertSQL, args)
```
Output:
```
INSERT INTO "user" ("first_name", "last_name") VALUES ('Greg', 'Farley')
```

**[`DialectWrapper.Insert`](https://godoc.org/github.com/doug-martin/goqu/#DialectWrapper.Insert)**

Use this when you want to create SQL for a specific `dialect`

```go
// import _ "github.com/doug-martin/goqu/v8/dialect/mysql"

dialect := goqu.Dialect("mysql")

ds := dialect.Insert().Rows(
    goqu.Record{"first_name": "Greg", "last_name": "Farley"},
)
insertSQL, _, _ := ds.ToSQL()
fmt.Println(insertSQL, args)
```
Output:
```
INSERT INTO `user` (`first_name`, `last_name`) VALUES ('Greg', 'Farley')
```

**[`Database.Insert`](https://godoc.org/github.com/doug-martin/goqu/#DialectWrapper.Insert)**

Use this when you want to execute the SQL or create SQL for the drivers dialect.

```go
// import _ "github.com/doug-martin/goqu/v8/dialect/mysql"

mysqlDB := //initialize your db
db := goqu.New("mysql", mysqlDB)

ds := db.Insert().Rows(
    goqu.Record{"first_name": "Greg", "last_name": "Farley"},
)
insertSQL, _, _ := ds.ToSQL()
fmt.Println(insertSQL, args)
```
Output:
```
INSERT INTO `user` (`first_name`, `last_name`) VALUES ('Greg', 'Farley')
```

### Examples

For more examples visit the **[Docs](https://godoc.org/github.com/doug-martin/goqu/#InsertDataset)**

<a name="insert-cols-vals"></a>
**Insert with Cols and Vals**

```go
ds := goqu.Insert("user").
	Cols("first_name", "last_name").
	Vals(
		goqu.Vals{"Greg", "Farley"},
		goqu.Vals{"Jimmy", "Stewart"},
		goqu.Vals{"Jeff", "Jeffers"},
	)
insertSQL, args, _ := ds.ToSQL()
fmt.Println(insertSQL, args)
```

Output: 
```sql
INSERT INTO "user" ("first_name", "last_name") VALUES ('Greg', 'Farley'), ('Jimmy', 'Stewart'), ('Jeff', 'Jeffers') []
```

<a name="insert-record"></a>
**Insert `goqu.Record`**

```go
ds := goqu.Insert("user").Rows(
	goqu.Record{"first_name": "Greg", "last_name": "Farley"},
	goqu.Record{"first_name": "Jimmy", "last_name": "Stewart"},
	goqu.Record{"first_name": "Jeff", "last_name": "Jeffers"},
)
insertSQL, args, _ := ds.ToSQL()
fmt.Println(insertSQL, args)
```

Output:
```
INSERT INTO "user" ("first_name", "last_name") VALUES ('Greg', 'Farley'), ('Jimmy', 'Stewart'), ('Jeff', 'Jeffers') []
```

<a name="insert-structs"></a>
**Insert Structs**

```go
type User struct {
	FirstName string `db:"first_name"`
	LastName  string `db:"last_name"`
}
ds := goqu.Insert("user").Rows(
	User{FirstName: "Greg", LastName: "Farley"},
	User{FirstName: "Jimmy", LastName: "Stewart"},
	User{FirstName: "Jeff", LastName: "Jeffers"},
)
insertSQL, args, _ := ds.ToSQL()
fmt.Println(insertSQL, args)
```

Output:
```
INSERT INTO "user" ("first_name", "last_name") VALUES ('Greg', 'Farley'), ('Jimmy', 'Stewart'), ('Jeff', 'Jeffers') []
```

You can skip fields in a struct by using the `skipinsert` tag

```go
type User struct {
	FirstName string `db:"first_name" goqu:"skipinsert"`
	LastName  string `db:"last_name"`
}
ds := goqu.Insert("user").Rows(
	User{FirstName: "Greg", LastName: "Farley"},
	User{FirstName: "Jimmy", LastName: "Stewart"},
	User{FirstName: "Jeff", LastName: "Jeffers"},
)
insertSQL, args, _ := ds.ToSQL()
fmt.Println(insertSQL, args)
```

Output:
```
INSERT INTO "user" ("last_name") VALUES ('Farley'), ('Stewart'), ('Jeffers') []
```

If you want to use the database `DEFAULT` when the struct field is a zero value you can use the `defaultifempty` tag.

```go
type User struct {
	FirstName string `db:"first_name" goqu:"defaultifempty"`
	LastName  string `db:"last_name"`
}
ds := goqu.Insert("user").Rows(
	User{LastName: "Farley"},
	User{FirstName: "Jimmy", LastName: "Stewart"},
	User{LastName: "Jeffers"},
)
insertSQL, args, _ := ds.ToSQL()
fmt.Println(insertSQL, args)
```

Output:
```
INSERT INTO "user" ("first_name", "last_name") VALUES (DEFAULT, 'Farley'), ('Jimmy', 'Stewart'), (DEFAULT, 'Jeffers') []
```

<a name="insert-map"></a>
**Insert `map[string]interface{}`**

```go
ds := goqu.Insert("user").Rows(
	map[string]interface{}{"first_name": "Greg", "last_name": "Farley"},
	map[string]interface{}{"first_name": "Jimmy", "last_name": "Stewart"},
	map[string]interface{}{"first_name": "Jeff", "last_name": "Jeffers"},
)
insertSQL, args, _ := ds.ToSQL()
fmt.Println(insertSQL, args)
```

Output:
```
INSERT INTO "user" ("first_name", "last_name") VALUES ('Greg', 'Farley'), ('Jimmy', 'Stewart'), ('Jeff', 'Jeffers') []
```

<a name="insert-from-query"></a>
**Insert from query**

```go
ds := goqu.Insert("user").Prepared(true).
	FromQuery(goqu.From("other_table"))
insertSQL, args, _ := ds.ToSQL()
fmt.Println(insertSQL, args)
```

Output:
```
INSERT INTO "user" SELECT * FROM "other_table" []
```

You can also specify the columns

```go
ds := goqu.Insert("user").Prepared(true).
	Cols("first_name", "last_name").
	FromQuery(goqu.From("other_table").Select("fn", "ln"))
insertSQL, args, _ := ds.ToSQL()
fmt.Println(insertSQL, args)
```

Output:
```
INSERT INTO "user" ("first_name", "last_name") SELECT "fn", "ln" FROM "other_table" []
```

<a name="returning"></a>
**Returning Clause**

Returning a single column example.

```go
sql, _, _ := goqu.Insert("test").
	Rows(goqu.Record{"a": "a", "b": "b"}).
	Returning("id").
	ToSQL()
fmt.Println(sql)
```

Output:
```
INSERT INTO "test" ("a", "b") VALUES ('a', 'b') RETURNING "id"
```

Returning multiple columns

```go
sql, _, _ = goqu.Insert("test").
	Rows(goqu.Record{"a": "a", "b": "b"}).
	Returning("a", "b").
	ToSQL()
fmt.Println(sql)
```

Output:
```
INSERT INTO "test" ("a", "b") VALUES ('a', 'b') RETURNING "a", "b"
```

Returning all columns

```go
sql, _, _ = goqu.Insert("test").
	Rows(goqu.Record{"a": "a", "b": "b"}).
	Returning(goqu.T("test").All()).
	ToSQL()
fmt.Println(sql)
```

Output:
```
INSERT INTO "test" ("a", "b") VALUES ('a', 'b') RETURNING "test".*
```

<a name="executing"></a>
## Executing Inserts

To execute INSERTS use [`Database.Insert`](https://godoc.org/github.com/doug-martin/goqu/#Database.Insert) to create your dataset

### Examples

**Executing an single Insert**
```go
db := getDb()

insert := db.Insert("goqu_user").Rows(
	goqu.Record{"first_name": "Jed", "last_name": "Riley", "created": time.Now()},
).Executor()

if _, err := insert.Exec(); err != nil {
	fmt.Println(err.Error())
} else {
	fmt.Println("Inserted 1 user")
}
```

Output:

```
Inserted 1 user
```

**Executing multiple inserts**

```go
db := getDb()

users := []goqu.Record{
	{"first_name": "Greg", "last_name": "Farley", "created": time.Now()},
	{"first_name": "Jimmy", "last_name": "Stewart", "created": time.Now()},
	{"first_name": "Jeff", "last_name": "Jeffers", "created": time.Now()},
}

insert := db.Insert("goqu_user").Rows(users).Executor()
if _, err := insert.Exec(); err != nil {
	fmt.Println(err.Error())
} else {
	fmt.Printf("Inserted %d users", len(users))
}

```

Output:
```
Inserted 3 users
```

If you use the RETURNING clause you can scan into structs or values.

```go
db := getDb()

insert := db.Insert("goqu_user").Returning(goqu.C("id")).Rows(
		goqu.Record{"first_name": "Jed", "last_name": "Riley", "created": time.Now()},
).Executor()

var id int64
if _, err := insert.ScanVal(&id); err != nil {
	fmt.Println(err.Error())
} else {
	fmt.Printf("Inserted 1 user id:=%d\n", id)
}
```

Output:

```
Inserted 1 user id:=5
```