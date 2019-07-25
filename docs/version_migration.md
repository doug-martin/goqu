## Migrating Between Versions

* [To v8](#v8)
* [To v7](#v7)

<a name="v8"></a>
### `v7 to v8`

A major change the the API was made in `v8` to seperate concerns between the different SQL statement types. 

**Why the change?**

1. There were feature requests that could not be cleanly implemented with everything in a single dataset. 
2. Too much functionality was encapsulated in a single datastructure.
    * It was unclear what methods could be used for each SQL statement type.
    * Changing a feature for one statement type had the possiblity of breaking another statement type.
    * Test coverage was decent but was almost solely concerned about SELECT statements, breaking them up allowed for focused testing on each statement type.
    * Most the SQL generation methods (`ToInsertSQL`, `ToUpdateSQL` etc.) took arguments which lead to an ugly API that was not uniform for each statement type, and proved to be inflexible.

**What Changed**

There are now five dataset types, `SelectDataset`, `InsertDataset`, `UpdateDataset`, `DeleteDataset` and `TruncateDataset`

Each dataset type has its own entry point.

* `goqu.From`, `Database#From`, `DialectWrapper#From` - Create SELECT
* `goqu.Insert`, `Database#Insert`, `DialectWrapper#Insert` - Create INSERT
* `goqu.Update`, `Database#db.Update`, `DialectWrapper#Update` - Create UPDATE
* `goqu.Delete`, `Database#Delete`, `DialectWrapper#Delete` - Create DELETE
* `goqu.Truncate`, `Database#Truncate`, `DialectWrapper#Truncate` - Create TRUNCATE
  
`ToInsertSQL`, `ToUpdateSQL`, `ToDeleteSQL`, and `ToTruncateSQL` (and variations of them) methods have been removed from the `SelectDataset`. Instead use the `ToSQL` methods on each dataset type.

Each dataset type will have an `Executor` and `ToSQL` method so a common interface can be created for each type.

**How to insert.**

In older versions of `goqu` there was `ToInsertSQL` method on the `Dataset` in the latest version there is a new entry point to create INSERTS.

```go
// old way
goqu.From("test").ToInsertSQL(...rows)

// new way
goqu.Insert("test").Rows(...rows).ToSQL()
goqu.From("test").Insert().Rows(...rows).ToSQL()
```

In older versions of `goqu` there was `Insert` method on the `Dataset` to execute an insert in the latest version there is a new `Exectutor` method.

```go
// old way
db.From("test").Insert(...rows).Exec()

// new way
db.Insert("test").Rows(...rows).Executor().Exec()
// or
db.From("test").Insert().Rows(...rows).Executor().Exec()
```

The new `InsertDataset` also has an `OnConflict` method that replaces the `ToInsertConflictSQL` and `ToInsertIgnoreSQL`

```go
// old way
goqu.From("test").ToInsertIgnoreSQL(...rows)

// new way
goqu.Insert("test").Rows(...rows).OnConflict(goqu.DoNothing()).ToSQL()
// OR
goqu.From("test").Insert().Rows(...rows).OnConflict(goqu.DoNothing()).ToSQL()
```

```go
// old way
goqu.From("items").ToInsertConflictSQL(
    goqu.DoUpdate("key", goqu.Record{"updated": goqu.L("NOW()")}),
    goqu.Record{"name": "Test1", "address": "111 Test Addr"},
    goqu.Record{"name": "Test2", "address": "112 Test Addr"},
)
fmt.Println(sql, args)

// new way
goqu.Insert("test").
  Rows(
    goqu.Record{"name": "Test1", "address": "111 Test Addr"},
    goqu.Record{"name": "Test2", "address": "112 Test Addr"},
  ).
  OnConflict(goqu.DoUpdate("key", goqu.Record{"updated": goqu.L("NOW()")})).
  ToSQL()
// OR
goqu.From("test").
  Insert().
  Rows(
    goqu.Record{"name": "Test1", "address": "111 Test Addr"},
    goqu.Record{"name": "Test2", "address": "112 Test Addr"},
  ).
  OnConflict(goqu.DoUpdate("key", goqu.Record{"updated": goqu.L("NOW()")})).
  ToSQL()
```

**How to update.**

In older versions of `goqu` there was `ToUpdateSQL` method on the `Dataset` in the latest version there is a new entry point to create UPDATEs.

```go
// old way
goqu.From("items").ToUpdateSQL(
    goqu.Record{"name": "Test", "address": "111 Test Addr"},
)

// new way
goqu.Update("items").
  Set(goqu.Record{"name": "Test", "address": "111 Test Addr"}).
  ToSQL()
// OR
goqu.From("items").
  Update()
  Set(goqu.Record{"name": "Test", "address": "111 Test Addr"}).
  ToSQL()
```

In older versions of `goqu` there was `Insert` method on the `Dataset` to execute an insert in the latest version there is a new `Exectutor` method.

```go
// old way
db.From("items").Update(
    goqu.Record{"name": "Test", "address": "111 Test Addr"},
).Exec()

// new way
db.Update("items").
  Set(goqu.Record{"name": "Test", "address": "111 Test Addr"}).
  Executor().Exec()
// OR
db.From("items").
  Update().
  Set(goqu.Record{"name": "Test", "address": "111 Test Addr"}).
  Executor().Exec()

```

**How to delete.**

In older versions of `goqu` there was `ToDeleteSQL` method on the `Dataset` in the latest version there is a new entry point to create DELETEs.

```go
// old way
goqu.From("items").
  Where(goqu.Ex{"id": goqu.Op{"gt": 10}}).
  ToDeleteSQL()

// new way
goqu.Delete("items").
  Where(goqu.Ex{"id": goqu.Op{"gt": 10}}).
  ToSQL()
// OR
goqu.From("items").
  Delete()
  Where(goqu.Ex{"id": goqu.Op{"gt": 10}}).
  ToSQL()
```

In older versions of `goqu` there was `Delete` method on the `Dataset` to execute an insert in the latest version there is a new `Exectutor` method.

```go
// old way
db.From("items").
  Where(goqu.Ex{"id": goqu.Op{"gt": 10}}).
  Delete().Exec()

// new way
db.Delete("items").
  Where(goqu.Ex{"id": goqu.Op{"gt": 10}}).
  Executor().Exec()
// OR
db.From("items").
  Delete()
  Where(goqu.Ex{"id": goqu.Op{"gt": 10}}).
  Executor().Exec()
```

**How to truncate.**

In older versions of `goqu` there was `ToTruncateSQL` method on the `Dataset` in the latest version there is a new entry point to create TRUNCATEs.

```go
// old way
goqu.From("items").ToTruncateSQL()

// new way
goqu.Truncate("items").ToSQL()
```

<a name="v7"></a>
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



