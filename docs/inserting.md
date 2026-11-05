# Inserting

* [Creating An InsertDataset](#create)
* Examples
  * [Insert Cols and Vals](#insert-cols-vals)
  * [Insert `goqu.Record`](#insert-record)
  * [Insert Structs](#insert-structs)
  * [Insert Map](#insert-map)
  * [Insert Slices As Arrays](#insert-arrays)
  * [Insert From Query](#insert-from-query)
  * [On Conflict](#onconflict)
  * [Returning](#returning)
  * [SetError](#seterror)
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
// import _ "github.com/doug-martin/goqu/v9/dialect/mysql"

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
// import _ "github.com/doug-martin/goqu/v9/dialect/mysql"

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
If you do not want to set the database field when the struct field is a nil pointer you can use the `omitnil` tag.

```go
type item struct {
    FirstName string  `db:"first_name" goqu:"omitnil"`
    LastName  string  `db:"last_name" goqu:"omitnil"`
    Address1  *string `db:"address1" goqu:"omitnil"`
    Address2  *string `db:"address2" goqu:"omitnil"`
    Address3  *string `db:"address3" goqu:"omitnil"`
}
address1 := "111 Test Addr"
var emptyString string
i := item{
    FirstName: "Test First Name",
    LastName:  "",
    Address1:  &address1,
    Address2:  &emptyString,
    Address3:  nil, // will omit nil pointer
}

insertSQL, args, _ := goqu.Insert("items").Rows(i).ToSQL()
fmt.Println(insertSQL, args)
```

Output:
```
INSERT INTO "items" ("address1", "address2", "first_name", "last_name") VALUES ('111 Test Addr', '', 'Test First Name', '') []
```

If you do not want to set the database field when the struct field is a zero value (including nil pointers) you can use
the `omitempty` tag.

Empty embedded structs implementing the `Valuer` interface (eg. `sql.NullString`) will also be omitted.

```go
type item struct {
    FirstName string  `db:"first_name" goqu:"omitempty"`
    LastName  string  `db:"last_name" goqu:"omitempty"`
    Address1  *string `db:"address1" goqu:"omitempty"`
    Address2  *string `db:"address2" goqu:"omitempty"`
    Address3  *string `db:"address3" goqu:"omitempty"`
}
address1 := "112 Test Addr"
var emptyString string
i := item{
    FirstName: "Test First Name",
    LastName:  "", // will omit zero field
    Address1:  &address1,
    Address2:  &emptyString,
    Address3:  nil, // will omit nil pointer
}
insertSQL, args, _ := goqu.Insert("items").Rows(i).ToSQL()
fmt.Println(insertSQL, args)
```

Output:
```
INSERT INTO "items" ("address1", "address2", "first_name") VALUES ('112 Test Addr', '', 'Test First Name') []
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

`goqu` will also use fields in embedded structs when creating an insert.

**NOTE** unexported fields will be ignored!

```go
type Address struct {
	Street string `db:"address_street"`
	State  string `db:"address_state"`
}
type User struct {
	Address
	FirstName string
	LastName  string
}
ds := goqu.Insert("user").Rows(
	User{Address: Address{Street: "111 Street", State: "NY"}, FirstName: "Greg", LastName: "Farley"},
	User{Address: Address{Street: "211 Street", State: "NY"}, FirstName: "Jimmy", LastName: "Stewart"},
	User{Address: Address{Street: "311 Street", State: "NY"}, FirstName: "Jeff", LastName: "Jeffers"},
)
insertSQL, args, _ := ds.ToSQL()
fmt.Println(insertSQL, args)
```

Output:
```
INSERT INTO "user" ("address_state", "address_street", "firstname", "lastname") VALUES ('NY', '111 Street', 'Greg', 'Farley'), ('NY', '211 Street', 'Jimmy', 'Stewart'), ('NY', '311 Street', 'Jeff', 'Jeffers') []
```

**NOTE** When working with embedded pointers if the embedded struct is nil then the fields will be ignored.

```go
type Address struct {
	Street string
	State  string
}
type User struct {
	*Address
	FirstName string
	LastName  string
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
INSERT INTO "user" ("firstname", "lastname") VALUES ('Greg', 'Farley'), ('Jimmy', 'Stewart'), ('Jeff', 'Jeffers') []
```

You can ignore an embedded struct or struct pointer by using `db:"-"`

```go
type Address struct {
	Street string
	State  string
}
type User struct {
	Address   `db:"-"`
	FirstName string
	LastName  string
}

ds := goqu.Insert("user").Rows(
	User{Address: Address{Street: "111 Street", State: "NY"}, FirstName: "Greg", LastName: "Farley"},
	User{Address: Address{Street: "211 Street", State: "NY"}, FirstName: "Jimmy", LastName: "Stewart"},
	User{Address: Address{Street: "311 Street", State: "NY"}, FirstName: "Jeff", LastName: "Jeffers"},
)
insertSQL, args, _ := ds.ToSQL()
fmt.Println(insertSQL, args)
```

Output:
```
INSERT INTO "user" ("firstname", "lastname") VALUES ('Greg', 'Farley'), ('Jimmy', 'Stewart'), ('Jeff', 'Jeffers') []
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

<a name="insert-arrays"></a>
**Insert Slices As Arrays**

With the `postgres` dialect a slice used as an insert (or update) value is written as an array. Set `SliceAsArray` in
the `SQLDialectOptions` of another dialect to get the same behavior. `[]byte`, `json.RawMessage`, `driver.Valuer`s
(e.g. `pq.Array`) and expressions are written as before. An empty slice is written as `'{}'` and a nil slice as `NULL`.

The array is written as one array text literal, the same value `pq.Array` produces, so it takes the type of the column
(`text[]`, `integer[]`, `uuid[]`, ...) and prepared statements bind it as a single argument.

```go
ds := goqu.Dialect("postgres").Insert("posts").Rows(
    goqu.Record{"title": "New Post", "tags": []string{"a", "b"}},
)
insertSQL, _, _ := ds.ToSQL()
fmt.Println(insertSQL)
```

Output:
```
INSERT INTO "posts" ("tags", "title") VALUES ('{"a","b"}', 'New Post')
```

Slices in predicates (e.g. `In`) keep rendering as a list. Use `goqu.Array` where an array value is needed elsewhere:

```go
sql, _, _ := goqu.Dialect("postgres").From("posts").
    Where(goqu.C("tags").Eq(goqu.Array([]string{"a"}))).ToSQL()
fmt.Println(sql)
```

Output:
```
SELECT * FROM "posts" WHERE ("tags" = '{"a"}')
```

Elements are written like the same value outside of an array: times use the time location (`goqu.SetTimeLocation`) and
the dialect's `TimeFormat`, and byte slices (e.g. `json.RawMessage`, or the `[]byte` a `driver.Valuer` returns) are
text unless they are binary, which are written in the hex form of a `bytea`.

Nested slices are multi dimensional arrays (`[][]int{{1, 2}, {3, 4}}` is `'{{1,2},{3,4}}'`). Postgres only stores
rectangular arrays and cannot store empty sub arrays, so sub arrays with different lengths are an error
(`sqlgen.ErrRaggedArray`) and so is a nil or empty sub array (`sqlgen.ErrEmptyNestedArray`). Elements that cannot be
written in the text form, such as `goqu.L("now()")`, make the array render as `ARRAY[now(), 'a']` instead.

`goqu.Array` is meant for Postgres (or databases with the same array literal). A column of a composite (row) type that
was written with a slice, e.g. `[]interface{}{1, "x"}`, now needs `goqu.L("ROW(?, ?)", 1, "x")` or a `driver.Valuer`
when the dialect has `SliceAsArray`.

Scanning `text[]` columns is up to the driver: scan into `pq.StringArray` (`github.com/lib/pq`) or a `pgtype` type.

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

<a name="onconflict"></a>
**On Conflict Clause**

You can handle conflicts using the `OnConflict` clause. For example, to ignore conflicts, you can use `DoNothing()`.

```go
sql, _, _ := goqu.Insert("test").
	Rows(goqu.Record{"a": "a", "b": "b"}).
	OnConflict(goqu.DoNothing()).
	ToSQL()
fmt.Println(sql)
```

Output:

```
INSERT INTO "test" ("a", "b") VALUES ('a', 'b') ON CONFLICT DO NOTHING
```

To specify the conflict target columns, use `SetCols`. This is supported by dialects that support a conflict target
(Postgres, SQLite3). Using it with a dialect that does not (MySQL, SQL Server) returns an error instead of silently
ignoring the target.

`SetCols` only supports a list of columns. To target a named constraint (`ON CONSTRAINT name`), use
`DoUpdate` with a constraint target instead.

```go
sql, _, _ := goqu.Insert("test").
	Rows(goqu.Record{"a": "a", "b": "b"}).
	OnConflict(goqu.DoNothing().SetCols(exp.NewColumnListExpression("a", "b"))).
	ToSQL()
fmt.Println(sql)
```

Output:

```
INSERT INTO "test" ("a", "b") VALUES ('a', 'b') ON CONFLICT ("a", "b") DO NOTHING
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

<a name="seterror"></a>
**[`SetError`](https://godoc.org/github.com/doug-martin/goqu/#InsertDataset.SetError)**

Sometimes while building up a query with goqu you will encounter situations where certain
preconditions are not met or some end-user contraint has been violated. While you could
track this error case separately, goqu provides a convenient built-in mechanism to set an
error on a dataset if one has not already been set to simplify query building.

Set an Error on a dataset:

```go
func GetInsert(name string, value string) *goqu.InsertDataset {

    var ds = goqu.Insert("test")

    if len(field) == 0 {
        return ds.SetError(fmt.Errorf("name is empty"))
    }

    if len(value) == 0 {
        return ds.SetError(fmt.Errorf("value is empty"))
    }

    return ds.Rows(goqu.Record{name: value})
}

```

This error is returned on any subsequent call to `Error` or `ToSQL`:

```go
var field, value string
ds = GetInsert(field, value)
fmt.Println(ds.Error())

sql, args, err = ds.ToSQL()
fmt.Println(err)
```

Output:
```
name is empty
name is empty
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

**Inserting a very large number of rows**

When executing with prepared statements, every value becomes a placeholder, and databases limit the number of parameters in a single statement (for example `SQLITE_MAX_VARIABLE_NUMBER` in SQLite, 65535 in PostgreSQL and MySQL). Inserting many rows in one statement can fail with errors such as `too many SQL variables`. Interpolated SQL has no placeholders but a very large statement can still exceed the server's maximum packet or statement size and uses a lot of memory.

`goqu` does not split inserts for you. Insert in batches instead, ideally inside a transaction:

```go
const batchSize = 500

tx, err := db.Begin()
if err != nil {
	return err
}
for start := 0; start < len(users); start += batchSize {
	end := min(start+batchSize, len(users))
	if _, err := tx.Insert("goqu_user").Rows(users[start:end]).Executor().Exec(); err != nil {
		_ = tx.Rollback()
		return err
	}
}
return tx.Commit()
```

Choose a batch size so that `rows * columns` stays below your database's parameter limit.

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
