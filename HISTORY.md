## v4.1.0

* Support for defining WITH clauses for Common Table Expressions (CTE) [#39](https://github.com/doug-martin/goqu/pull/39) - [@Oscil8](https://github.com/Oscil8)

## v4.0

* Prepared(true) issues when using IS NULL comparisson operation [#33](https://github.com/doug-martin/goqu/pull/33) - [@danielfbm](https://github.com/danielfbm)

## v3.3

* Add `upsert` support via `InsertIgnore` and `InsertConflict` methods - [#25](https://github.com/doug-martin/goqu/pull/28) - [@aheuermann](https://github.com/aheuermann)
* Adding vendor dependencies and updating tests to run in docker containers [#29](https://github.com/doug-martin/goqu/pull/29) - [@aheuermann](https://github.com/aheuermann)

## v3.2

* Add range clauses ([NOT] BETWEEN) support - [#25](https://github.com/doug-martin/goqu/pull/25) - [@denisvm](https://github.com/denisvm)
* Readmefix [#26](https://github.com/doug-martin/goqu/pull/26) - [@tiagopotencia](https://github.com/tiagopotencia)

## v3.1.3

* Bugfix for chained Where() [#20](https://github.com/doug-martin/goqu/pull/20) - [@Emreu](https://github.com/Emreu)


## v3.1.2

* Fixing ScanStruct issue with embedded pointers in crud_exec [#20](https://github.com/doug-martin/goqu/pull/20) - [@ruzz311](https://github.com/ruzz311)

## v3.1.1

* Fixing race condition with struct_map_cache in crud_exec [#18](https://github.com/doug-martin/goqu/pull/18) - [@andymoon](https://github.com/andymoon), [@aheuermann](https://github.com/aheuermann)

## v3.1.0

* Version 3.1 [#14](https://github.com/doug-martin/goqu/pull/14) - [@andymoon](https://github.com/andymoon)
    * Fix an issue with a nil pointer access on the inserts and updates.
    * Allowing ScanStructs to take a struct with an embedded pointer to a struct.
    * Change to check if struct is Anonymous when recursing through an embedded struct.
    * Updated to use the latest version of github.com/DATA-DOG/go-sqlmock.

## v3.0.1

* Add literal bytes and update to c2fo testify [#15](https://github.com/doug-martin/goqu/pull/15) - [@TechnotronicOz](https://github.com/TechnotronicOz)

## v3.0.0

* Added support for embedded structs when inserting or updating. [#13](https://github.com/doug-martin/goqu/pull/13) - [@andymoon](https://github.com/andymoon)

## v2.0.3

* Fixed issue with transient columns and the auto select of columns.

## v2.0.2

* Changed references to "github.com/doug-martin/goqu" to "gopkg.in/doug-martin/goqu.v2"

## v2.0.1

* Fixed issue when `ScanStruct(s)` was used with `SelectDistinct` and caused a panic.

## v2.0.0

* When scanning a struct or slice of structs, the struct(s) will be parsed for the column names to select. [#9](https://github.com/doug-martin/goqu/pull/9) - [@technotronicoz](https://github.com/TechnotronicOz)

## v1.0.0

* You can now passed an IdentiferExpression to `As` [#8](https://github.com/doug-martin/goqu/pull/8) - [@croachrose](https://github.com/croachrose)
* Added info about installation through [gopkg.in](http://labix.org/gopkg.in)

## v0.3.1

* Fixed issue setting Logger when starting a new transaction.

## v0.3.0

* Changed sql generation methods to use a common naming convention. `To(Sql|Insert|Update|Delete)`
   * Also changed to have common return values `string, []interface{}, error)`
* Added `Dataset.Prepared` which allows a user to specify whether or not SQL should be interpolated. [#7](https://github.com/doug-martin/goqu/issues/7)
* Updated Docs
    * More examples
* Increased test coverage.

## v0.2.0

* Changed `CrudExec` to not wrap driver errors in a GoquError [#2](https://github.com/doug-martin/goqu/issues/2)
* Added ability to use a dataset in an `Ex` map or `Eq` expression without having to use `In` [#3](https://github.com/doug-martin/goqu/issues/3)
   * `db.From("test").Where(goqu.Ex{"a": db.From("test").Select("b")})`
* Updated readme with links to [`DefaultAdapter`](https://godoc.org/github.com/doug-martin/goqu#DefaultAdapter)

## v0.1.1

* Added SQLite3 adapter [#1](https://github.com/doug-martin/goqu/pull/1) - [@mattn](https://github.com/mattn)

## v0.1.0

* Added:
    * [`Ex`](https://godoc.org/github.com/doug-martin/goqu#Ex)
    * [`ExOr`](https://godoc.org/github.com/doug-martin/goqu#ExOr)
    * [`Op`](https://godoc.org/github.com/doug-martin/goqu#Op)
* More tests and examples
* Added CONTRIBUTING.md
* Added LICENSE information
* Removed godoc introduction in favor of just maintaining the README.

## v0.0.2

* Fixed issue with goqu.New not returning a pointer to a Database

## v0.0.1

* Initial release
