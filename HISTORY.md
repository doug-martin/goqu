# v9.24.0

* [SECURITY] The `sqlserver` dialect escaped a single quote in interpolated strings as `\'`, which SQL Server does not understand, so a quote in a value could end the string literal early (SQL injection when interpolating untrusted values). Only the single quote is now escaped, by doubling it, and quotes, backslashes and control characters are no longer backslash-escaped. Prepared statements were not affected. Upgrade if you use the `sqlserver` dialect with interpolated (non prepared) queries [#411](https://github.com/doug-martin/goqu/issues/411)
* [SECURITY] Escape quote characters embedded in quoted identifiers by doubling them (e.g. `a"b` becomes `"a""b"`). Previously an untrusted value used as an identifier (for example `goqu.C(userInput)`) could break out of the quotes [#428](https://github.com/doug-martin/goqu/issues/428)
* [FEATURE] Add the `UnicodeStringPrefix` dialect option. The `sqlserver` dialect uses it to write interpolated strings with non ASCII characters as `N'...'` (unicode) literals
* [FIX] Interpolated `[]byte` values that are not valid UTF-8 are written as a binary literal (`X'..'`, `decode('..', 'hex')` for postgres, `0x..` for sqlserver) instead of being corrupted. Added the `BinaryLiteralPrefix`, `BinaryLiteralSuffix` and `BinaryLiteralBytes` dialect options (`postgres` and `sqlserver` also use a binary literal for byte slices containing NUL). Valid text is still written as a string [#436](https://github.com/doug-martin/goqu/issues/436), [#335](https://github.com/doug-martin/goqu/issues/335), [#254](https://github.com/doug-martin/goqu/issues/254), [#353](https://github.com/doug-martin/goqu/issues/353)
* [FIX] `skipinsert` and `skipupdate` on a nested or embedded struct field now apply to all of the columns of that struct. Note that `skipinsert`/`skipupdate` on an embedded struct used to be ignored and now take effect [#356](https://github.com/doug-martin/goqu/issues/356)
* [FIX] The `sqlite3` dialect generates `INSERT INTO t DEFAULT VALUES` for an insert without columns instead of an invalid `INSERT INTO t` [#437](https://github.com/doug-martin/goqu/issues/437)
* [FIX] The `sqlserver` dialect applies `Limit` on `UPDATE` as `UPDATE TOP (n) ...` instead of silently dropping it, which updated every matching row [#489](https://github.com/doug-martin/goqu/pull/489) - [@simpleqt](https://github.com/simpleqt)
* [DOCS] Document batching large inserts, scanned time locations, struct/column matching for `ScanStruct` and a custom dialect without `AS` [#305](https://github.com/doug-martin/goqu/issues/305), [#346](https://github.com/doug-martin/goqu/issues/346), [#355](https://github.com/doug-martin/goqu/issues/355), [#374](https://github.com/doug-martin/goqu/issues/374), [#426](https://github.com/doug-martin/goqu/issues/426)
* [DOCS] Add a struct tags reference, document Postgres operators containing `?`, fix broken links in the dialect docs and the update docs examples [#232](https://github.com/doug-martin/goqu/issues/232), [#283](https://github.com/doug-martin/goqu/issues/283), [#323](https://github.com/doug-martin/goqu/issues/323), [#368](https://github.com/doug-martin/goqu/issues/368), [#439](https://github.com/doug-martin/goqu/issues/439)

# v9.23.0

* [FEATURE] Add `SelectDataset.Qualify` and the `SupportsQualify` dialect option for the `QUALIFY` clause (Teradata, Snowflake, BigQuery, ...). Off by default for all built in dialects [#343](https://github.com/doug-martin/goqu/issues/343), [#401](https://github.com/doug-martin/goqu/pull/401) - [@slessard](https://github.com/slessard)
* [FEATURE] Add `SelectDataset.CustomJoin` to write a join clause goqu does not model (e.g. ClickHouse `ARRAY JOIN`) [#423](https://github.com/doug-martin/goqu/issues/423), [#430](https://github.com/doug-martin/goqu/pull/430) - [@darwin67](https://github.com/darwin67)

# v9.22.0

* [PERF] Reduce allocations when generating SQL: pooled SQL builders, typed fast paths for common slice types, bytewise placeholder scanning and pre-sized buffers. Roughly 10-25% faster `ToSQL` with up to 40% fewer allocations in the benchmarks [#444](https://github.com/doug-martin/goqu/pull/444) - [@pquerna](https://github.com/pquerna), [@funkyshu](https://github.com/funkyshu)
* [FIX] Generate SQL for `json.RawMessage` values (and pointers to byte slices) like `[]byte` instead of a list of numbers, and do not turn `Eq`/`Neq`/`In`/`NotIn` on them into a list comparison [#350](https://github.com/doug-martin/goqu/issues/350), [#351](https://github.com/doug-martin/goqu/pull/351) - [@lefinal](https://github.com/lefinal), [@funkyshu](https://github.com/funkyshu)
* [FIX] `ScanStruct` can scan into struct fields of a non-empty interface type such as `sql.Scanner` when the field already holds a non-nil pointer implementing `sql.Scanner` [#399](https://github.com/doug-martin/goqu/pull/399) - [@arkamar](https://github.com/arkamar), [@funkyshu](https://github.com/funkyshu)
* [DOCS] Update the Docker Compose instructions for running the tests and raise the database wait timeouts [#341](https://github.com/doug-martin/goqu/issues/341), [#480](https://github.com/doug-martin/goqu/pull/480)

## Behavior changes to be aware of

* A nil `json.RawMessage` is now written as `NULL` (it used to be an empty string, which is not valid JSON) and compares with `IS NULL` / `IS NOT NULL` in `Eq`/`Neq`.
* `In`/`NotIn` with a single `[]byte` or `json.RawMessage` argument now treats it as one value (`IN ('...')`) instead of generating `IN '...'`.
* `ScanStruct` into a `nil` `sql.Scanner` interface field (or a non-pointer value) now returns a descriptive error instead of the `database/sql` error `unsupported Scan, storing driver.Value type ... into type *sql.Scanner`.

# v9.21.0

* [FEATURE] Add `EXISTS` support: `goqu.Exists(subquery)` and `SelectDataset.Exists()` [#204](https://github.com/doug-martin/goqu/issues/204), [#412](https://github.com/doug-martin/goqu/pull/412), [#477](https://github.com/doug-martin/goqu/pull/477) - [@Yakov-Varnaev](https://github.com/Yakov-Varnaev), [@funkyshu](https://github.com/funkyshu)
* [FEATURE] Add a `Collate` expression (`goqu.Collate(expr, collation)` / `identifier.Collate(collation)`) usable in `WHERE`, `ORDER BY`, etc., with per-dialect collation quoting [#405](https://github.com/doug-martin/goqu/pull/405), [#478](https://github.com/doug-martin/goqu/pull/478) - [@tanner-lai](https://github.com/tanner-lai), [@funkyshu](https://github.com/funkyshu)
* [FEATURE] Add a `NULLIF` expression helper [#406](https://github.com/doug-martin/goqu/issues/406), [#407](https://github.com/doug-martin/goqu/pull/407) - [@PJHRobles](https://github.com/PJHRobles)
* [FEATURE] Allow conflict targets in `ON CONFLICT DO NOTHING` via `goqu.DoNothing().SetCols(...)`. Dialects without conflict target support (MySQL, SQL Server) return an error when a target is set. [#422](https://github.com/doug-martin/goqu/pull/422) - [@pedrohavay](https://github.com/pedrohavay), [@funkyshu](https://github.com/funkyshu)
* [FEATURE] MySQL: add `STRAIGHT_JOIN` support via `SelectDataset.StraightJoin` (requires an `ON` condition) [#442](https://github.com/doug-martin/goqu/issues/442), [#453](https://github.com/doug-martin/goqu/pull/453) - [@vesquen](https://github.com/vesquen)
* [FEATURE] Prepared statements: pass any driver-bindable value (structs, maps, byte arrays such as UUIDs) through as an argument instead of rejecting it; unbindable kinds (funcs, channels, `unsafe.Pointer`, complex numbers) are still rejected [#313](https://github.com/doug-martin/goqu/issues/313), [#316](https://github.com/doug-martin/goqu/pull/316) - [@yz89122](https://github.com/yz89122)
* [FIX] Treat typed nil pointers and invalid `driver.Valuer`s (`sql.NullInt32{}`, ...) as `NULL` in `Eq`/`Neq`/`Is`/`IsNot` and `Ex` [#272](https://github.com/doug-martin/goqu/issues/272), [#438](https://github.com/doug-martin/goqu/issues/438), [#476](https://github.com/doug-martin/goqu/pull/476) - [@funkyshu](https://github.com/funkyshu)
* [FIX] SQL Server: return an error for `ORDER BY` on `DELETE` (new dialect option `ErrorOnUnsupportedOrderByOnDelete`) [#472](https://github.com/doug-martin/goqu/pull/472) - [@funkyshu](https://github.com/funkyshu)
* [FIX] SQL Server: emit `DISTINCT` before `TOP` [#242](https://github.com/doug-martin/goqu/pull/242), [#475](https://github.com/doug-martin/goqu/pull/475) - [@CKehToh](https://github.com/CKehToh), [@funkyshu](https://github.com/funkyshu)
* [FIX] SQLite3: generate `DELETE FROM` for `Truncate` [#330](https://github.com/doug-martin/goqu/pull/330), [#474](https://github.com/doug-martin/goqu/pull/474) - [@darh](https://github.com/darh), [@funkyshu](https://github.com/funkyshu)
* [FIX] MySQL 8: enable `WITH` and `WITH RECURSIVE` common table expressions [#362](https://github.com/doug-martin/goqu/issues/362), [#400](https://github.com/doug-martin/goqu/pull/400), [#473](https://github.com/doug-martin/goqu/pull/473) - [@funkyshu](https://github.com/funkyshu)
* [FIX] Do not wrap a single subselect in double parentheses [#352](https://github.com/doug-martin/goqu/issues/352), [#357](https://github.com/doug-martin/goqu/pull/357) - [@nic11](https://github.com/nic11)
* [DOCS] Fix the `ScanStructContext` description and a typo about placeholders [#424](https://github.com/doug-martin/goqu/pull/424), [#449](https://github.com/doug-martin/goqu/pull/449) - [@shortcuts](https://github.com/shortcuts), [@j0shiJ](https://github.com/j0shiJ)
* [CHORE] Update GitHub Actions dependencies [#465](https://github.com/doug-martin/goqu/pull/465)

## Behavior changes to be aware of

* Comparing a column to a typed null (`(*int)(nil)`, `sql.NullInt32{}` that is not valid, ...) with `Eq`/`Neq`/`Ex` now generates `IS NULL` / `IS NOT NULL` instead of `= NULL` / `!= NULL`. Queries that previously matched no rows will now match `NULL` rows.
* SQL Server `Delete().Order(...)` now returns an error at build time instead of generating SQL the server rejects.
* SQL Server `SELECT DISTINCT ... TOP (n)` is now generated as `SELECT DISTINCT TOP (n) ...`.
* SQLite3 `Truncate()` now generates `DELETE FROM <table>`.
* `goqu.DoNothing()` and `exp.NewDoNothingConflictExpression()` now return `exp.ConflictNothingExpression` (a superset of `exp.ConflictExpression`). Code that reassigns the result to a different conflict type through type inference (`c := goqu.DoNothing(); c = goqu.DoUpdate(...)`) needs an explicit `exp.ConflictExpression` declaration.
* `SetCols` on `DoNothing` for SQLite3 emits a plain `INSERT ... ON CONFLICT (...) DO NOTHING` (not `INSERT OR IGNORE`) so that only the targeted constraint is ignored.

# v9.20.0

**NOTE** The minimum supported Go version is now **1.26** (previously 1.19). See [Supported Go versions](./README.md#supported-go-versions).

* [BREAKING] Raise the minimum Go version to 1.26 [#468](https://github.com/doug-martin/goqu/pull/468) - [@funkyshu](https://github.com/funkyshu)
* [FEATURE] SQL Server: support `Limit` on `Delete` via `DELETE TOP (n)` [#456](https://github.com/doug-martin/goqu/pull/456) - [@saurabh-prakash](https://github.com/saurabh-prakash)
* [FEATURE] Support `uint` values, including `uint64` values above `math.MaxInt64` [#421](https://github.com/doug-martin/goqu/pull/421) - [@walnuts1018](https://github.com/walnuts1018)
* [FEATURE] Allow `[]exp.UpdateExpression` as `Set` values [#389](https://github.com/doug-martin/goqu/pull/389) - [@kvii](https://github.com/kvii)
* [FIX] MySQL: do not add an extra `IGNORE` to `INSERT ... ON DUPLICATE KEY UPDATE` [#435](https://github.com/doug-martin/goqu/pull/435) - [@autowp](https://github.com/autowp)
* [FIX] SQLite3: support `UPDATE ... FROM` [#441](https://github.com/doug-martin/goqu/issues/441), [#457](https://github.com/doug-martin/goqu/pull/457) - [@yousefissa](https://github.com/yousefissa)
* [FIX] SQLite3: support conflict update `WHERE` (`OnConflictUpdateWhere`) [#440](https://github.com/doug-martin/goqu/pull/440) - [@loganintech](https://github.com/loganintech)
* [FIX] SQLite3: allow `RETURNING` clauses [#359](https://github.com/doug-martin/goqu/pull/359) - [@abdusco](https://github.com/abdusco)
* [FIX] Typo in the column list expression builder [#376](https://github.com/doug-martin/goqu/pull/376) - [@philippfrank](https://github.com/philippfrank)
* [DOCS] Fix `Select` docstring [#354](https://github.com/doug-martin/goqu/pull/354) - [@nic11](https://github.com/nic11)
* [CHORE] Clear all open `golang.org/x/crypto` security alerts: the SQL Server driver used by the tests is now `github.com/microsoft/go-mssqldb` [#461](https://github.com/doug-martin/goqu/pull/461), [#469](https://github.com/doug-martin/goqu/pull/469)
* [CHORE] Update dependencies, CI workflows (Compose v2, golangci-lint v2, Codecov v5) and add Dependabot [#386](https://github.com/doug-martin/goqu/pull/386), [#459](https://github.com/doug-martin/goqu/pull/459), [#462](https://github.com/doug-martin/goqu/pull/462), [#468](https://github.com/doug-martin/goqu/pull/468) - [@funkyshu](https://github.com/funkyshu)
* [CHORE] Remove the vendored dependencies [#463](https://github.com/doug-martin/goqu/pull/463)

## Behavior changes to be aware of

* SQL Server `DELETE ... LIMIT n` now generates `DELETE TOP (n) FROM ...`.
* MySQL `INSERT ... ON DUPLICATE KEY UPDATE` no longer generates `INSERT IGNORE ... ON DUPLICATE KEY UPDATE`. `INSERT IGNORE` is still used for `DO NOTHING`.
* `uint64` values greater than `math.MaxInt64` are now encoded (as `uint64` in prepared statements) rather than rejected. Values that fit in an `int64` keep being sent as `int64`.

# v9.19.0
* [FEATURE] Add support for omitempty and omitnil struct tags [#309](https://github.com/doug-martin/goqu/pull/309) - [@randallmlough](https://github.com/randallmlough)

# v9.18.0
* [FEATURE] Add support for aliasing insert datasets to support upsert alias [#306](https://github.com/doug-martin/goqu/pull/306) - [@XIELongDragon](https://github.com/XIELongDragon)
* [FEATURE] Add support for aliasing BooleanExpressions [#307](https://github.com/doug-martin/goqu/pull/307) - [@XIELongDragon](https://github.com/XIELongDragon)

# v9.17.0
* [FEATURE] Add support bitwise operations [#303](https://github.com/doug-martin/goqu/pull/303) - [@XIELongDragon](https://github.com/XIELongDragon)
* [FEATURE] Add support for specifying tables to be locked in ForUpdate, ForNoKeyUpdate, ForKeyShare, ForShare [#299](https://github.com/doug-martin/goqu/pull/299) - [@jbub](https://github.com/jbub)

# v9.16.0
* [FEATURE] Allow ordering by case expression [#282](https://github.com/doug-martin/goqu/issues/282), [#292](https://github.com/doug-martin/goqu/pull/292)

# v9.15.1
* [FIX] Field indexes in the columnMap getting overridden [290](https://github.com/doug-martin/goqu/issues/290), [#291](https://github.com/doug-martin/goqu/pull/291)

# v9.15.0
* [FEATURE] Add SetDefaultPrepared which controls query value interpolation [#288](https://github.com/doug-martin/goqu/pull/288) - [@Deiz](https://github.com/Deiz)

# v9.14.0
* [FEATURE] GroupByAppend to the SelectDataset and SelectClauses [#276](https://github.com/doug-martin/goqu/pull/276), [#287](https://github.com/doug-martin/goqu/pull/287) - [@ashishkf](https://github.com/ashishkf)
* [FEATURE] Allow untagged fields to be ignored [#285](https://github.com/doug-martin/goqu/pull/285) - [@Deiz](https://github.com/Deiz)
* [FIX] Nil valuer [#277](https://github.com/doug-martin/goqu/pull/277) - [@benzolium](https://github.com/benzolium), [@Diggs](https://github.com/Diggs)
* [FIX] Fix old import URL in doc comments [#286](https://github.com/doug-martin/goqu/pull/286) - [@maito1201](https://github.com/maito1201)

# v9.13.0

* [ADDED] ScanStructs, ScanVals to Scanner interface [#273](https://github.com/doug-martin/goqu/issues/273) - [@vlanse](https://github.com/vlanse)

# v9.12.0

* Update golangci-lint and updates for all associated linters
* Update dependencies
  * github.com/DATA-DOG/go-sqlmock  v1.3.3 -> v1.5.0
  * github.com/denisenkom/go-mssqldb v0.0.0-20200206145737-bbfc9a55622e -> v0.10.0
  * github.com/go-sql-driver/mysql v1.4.1 -> v1.6.0
  * github.com/lib/pq v1.2.0 -> v1.10.1
  * github.com/mattn/go-sqlite3 v1.11.0 -> v1.14.7
  * github.com/stretchr/testify -> v1.4.0 -> v1.7.0
  

# v9.11.1

* [FIXED] Avoid mutation of join slice for separate datasets when joins slice capacity is not yet reached [#261](https://github.com/doug-martin/goqu/pull/261) - [@fhaifler](https://github.com/fhaifler)

# v9.11.0

* [FIXED] Use valid 'IS' operator for sqlserver dialect [#240](https://github.com/doug-martin/goqu/pull/240), [#239](https://github.com/doug-martin/goqu/pull/229)  - [@vlanse](https://github.com/vlanse)
* [ADDED] Implement Orderable interface for SQL Functions [#251](https://github.com/doug-martin/goqu/pull/251)  - [@GlebBeloded](https://github.com/GlebBeloded)
* [ADDED] Support for table hint in multi-table MySQL DELETE queries [#252](https://github.com/doug-martin/goqu/pull/252) - [@vlanse](https://github.com/vlanse)

# v9.10.0

* [FIXED] SELECT inherits dialect from INSERT in INSERT FROM SELECT.  [#229](https://github.com/doug-martin/goqu/pull/229), [#223](https://github.com/doug-martin/goqu/issues/223) - [@vlanse](https://github.com/vlanse)
* [FIXED] SQLServer dialect: support prepared statements with TOP.  [#230](https://github.com/doug-martin/goqu/pull/230), [#225](https://github.com/doug-martin/goqu/issues/225) - [@vlanse](https://github.com/vlanse)
* [ADDED] IsPrepared to SQLExpression interface.  [#231](https://github.com/doug-martin/goqu/pull/231) - [@vlanse](https://github.com/vlanse)

# v9.9.0

* [FIXED] SQLite do not add FOR UPDATE in SELECT. [#218](https://github.com/doug-martin/goqu/pull/218) - [@vlanse](https://github.com/vlanse)
* [ADDED] Support for INSERT ON CONFLICT in SQLite. [#218](https://github.com/doug-martin/goqu/pull/218) - [@vlanse](https://github.com/vlanse)

# v9.8.0

* [ADDED] Support for ANY and ALL operators. [#196](https://github.com/doug-martin/goqu/issues/196)
* [ADDED] Support for CASE statements [#193](https://github.com/doug-martin/goqu/issues/193) 
* [ADDED] Support for getting column identifiers from AliasExpressions. [#203](https://github.com/doug-martin/goqu/issues/203)  

# v9.7.1

* Fix all formatting for golangci-lint
* Move to golangci-lint github action

# v9.7.0

* [ADDED] Support for sqlserver dialect [#197](https://github.com/doug-martin/goqu/issues/197),[#205](https://github.com/doug-martin/goqu/issues/205) - [@vlanse](https://github.com/vlanse)

# v9.6.0

* [ADDED] Support for Lateral queries [#182](https://github.com/doug-martin/goqu/issues/182)

# v9.5.1

* [FIXED] WITH clause without a RETURNING clause will panic [#177](https://github.com/doug-martin/goqu/issues/177)
* [FIXED] SQlite dialect escapes single quotes wrong, leads to SQL syntax error [#178](https://github.com/doug-martin/goqu/issues/178)
* [FIXED] Fix ReturnsColumns() nil pointer panic [#181](https://github.com/doug-martin/goqu/issues/181) - [@yeaha](https://github.com/yeaha)
* [FIXED] SelectDataset From with Error [#183](https://github.com/doug-martin/goqu/issues/183)
* [FIXED] Unable to execute union with order by expression [#185](https://github.com/doug-martin/goqu/issues/185)

# v9.5.0

* [ADDED] Ability to use regexp like, ilike, notlike, and notilike without a regexp [#172](https://github.com/doug-martin/goqu/issues/172)

# v9.4.0

* [ADDED] Ability to scan into struct fields from multiple tables [#160](https://github.com/doug-martin/goqu/issues/160)

# v9.3.0

* [ADDED] Using Update, Insert, or Delete datasets in sub selects and CTEs [#164](https://github.com/doug-martin/goqu/issues/164)

# v9.2.0

* [ADDED] exec.Scanner: New exposed scanner supports iterative scanning [#157](https://github.com/doug-martin/goqu/pull/157) - [@akarl](https://github.com/akarl)

# v9.1.0

* [FIXED] ExampleDoUpdate does't work in postgres [#156](https://github.com/doug-martin/goqu/issues/156)
* [FIXED] Issue with timezone being lost [#163](https://github.com/doug-martin/goqu/issues/163) 

# v9.0.1

* [FIXED] Issue where `NULL`, `TRUE` and `FALSE` are interpolated when using an `IS` clause. [#165](https://github.com/doug-martin/goqu/issues/165)
    
# v9.0.0

* Changed `NULL`, `TRUE`, `FALSE` to not be interpolated when creating prepared statements. [#132](https://github.com/doug-martin/goqu/pull/132), [#158](https://github.com/doug-martin/goqu/pull/158) - [@marshallmcmullen](https://github.com/marshallmcmullen)
* Updated dependencies
    * `github.com/lib/pq v1.1.1 -> v1.2.0`
    * `github.com/mattn/go-sqlite3 v1.10.0 -> v1.11.0`
    * `github.com/stretchr/testify v1.3.0 -> v1.4.0`

## v8.6.0

* [ADDED] `SetError()` and `Error()` to all datasets. [#152](https://github.com/doug-martin/goqu/pull/152) and [#150](https://github.com/doug-martin/goqu/pull/150) - [@marshallmcmullen](https://github.com/marshallmcmullen)

## v8.5.0

* [ADDED] Window Function support [#128](https://github.com/doug-martin/goqu/issues/128) - [@Xuyuanp](https://github.com/Xuyuanp)

## v8.4.1

* [FIXED] Returning func be able to handle nil [#140](https://github.com/doug-martin/goqu/issues/140)

## v8.4.0

* Created new `sqlgen` module to encapsulate sql generation
    * Broke SQLDialect inti new SQL generators for each statement type.
* Test refactor
    * Moved to a test case pattern to allow for quickly adding new test cases.
    
## v8.3.2

* [FIXED] Data race during query factory initialization [#133](https://github.com/doug-martin/goqu/issues/133) and [#136](https://github.com/doug-martin/goqu/issues/136) - [@o1egl](https://github.com/o1egl)    

## v8.3.1

* [FIXED] InsertDataset.WithDialect return old dataset [#126](https://github.com/doug-martin/goqu/issues/126) - [@chen56](https://github.com/chen56)
* Test clean up and more testing pattern consistency
    * Changed to use assertion methods off of suite
    * Updated Equals assertions to have expected output first 
* Increase overall test coverage.

## v8.3.0

* [Added] Support for `DISTINCT ON` clauses [#119](https://github.com/doug-martin/goqu/issues/119)

## v8.2.2

* [FIX] Scanner errors on pointers to primitive values [#122](https://github.com/doug-martin/goqu/issues/122)

## v8.2.1

* [FIX] Return an error when an empty identifier is encountered [#115](https://github.com/doug-martin/goqu/issues/115)

## v8.2.0

* [FIX] Fix reflection errors related to nil pointers and unexported fields [#118](https://github.com/doug-martin/goqu/issues/118)
    * Unexported fields are ignored when creating a columnMap
    * Nil embedded pointers will no longer cause a panic
    * Fields on nil embedded pointers will be ignored when creating update or insert statements.
* [ADDED] You can now ingore embedded structs and their fields by using `db:"-"` tag on the embedded struct.

## v8.1.0

* [ADDED] Support column DEFAULT when inserting/updating via struct [#27](https://github.com/doug-martin/goqu/issues/27)

## v8.0.1

* [ADDED] Multi table update support for `mysql` and `postgres` [#60](https://github.com/doug-martin/goqu/issues/60)
* [ADDED] `goqu.V` so values can be used on the LHS of expressions [#104](https://github.com/doug-martin/goqu/issues/104)

## v8.0.0

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


## v7.4.0

* [FIXED] literalTime use t.UTC() , This behavior is different from the original sql.DB [#106](https://github.com/doug-martin/goqu/issues/106) - [chen56](https://github.com/chen56)
* [ADDED] Add new method WithTx for Database [#108](https://github.com/doug-martin/goqu/issues/108) - [Xuyuanp](https://github.com/Xuyuanp)

## v7.3.1

* [ADDED] Exposed `goqu.NewTx` to allow creating a goqu tx directly from a `sql.Tx` instead of using `goqu.Database#Begin` [#95](https://github.com/doug-martin/goqu/issues/95)
* [ADDED] `goqu.Database.BeginTx` [#98](https://github.com/doug-martin/goqu/issues/98)

## v7.3.0

* [ADDED] UPDATE and INSERT should use struct Field name if db tag is not specified [#57](https://github.com/doug-martin/goqu/issues/57)
* [CHANGE] Changed goqu.Database to accept a SQLDatabase interface to allow using goqu.Database with other libraries such as `sqlx` [#95](https://github.com/doug-martin/goqu/issues/95)

## v7.2.0

* [FIXED] Sqlite3 does not accept SELECT * UNION (SELECT *) [#79](https://github.com/doug-martin/goqu/issues/79)
* [FIXED] Where(Ex{}) causes panics [mysql] [#49](https://github.com/doug-martin/goqu/issues/49)
* [ADDED] Support for OrderPrepend [#61](https://github.com/doug-martin/goqu/issues/61)
* [DOCS] Added new section about loading a dialect and using it to build SQL [#44](https://github.com/doug-martin/goqu/issues/44)

## v7.1.0

* [FIXED] Embedded pointers with property names that duplicate parent struct properties. [#23](https://github.com/doug-martin/goqu/issues/23)
* [FIXED] Can't scan values using []byte or []string [#90](https://github.com/doug-martin/goqu/issues/90)
    * When a slice that is `*sql.RawBytes`, `*[]byte` or `sql.Scanner` no errors will be returned. 

## v7.0.1

* Fix issue where structs with pointer fields where not set properly [#86](https://github.com/doug-martin/goqu/pull/86) and [#89](https://github.com/doug-martin/goqu/pull/89) - [@efureev](https://github.com/efureev)

## v7.0.0

**Linting**
* Add linting checks and fixed errors 
    * Renamed all snake_case variables to be camelCase.     
    * Fixed examples to always map to a defined method
* Renamed `adapters` to `dialect` to more closely match their intended purpose.

**API Changes**
* Updated all sql generations methods to from `Sql` to `SQL`
    * `ToSql` -> `ToSQL`
    * `ToInsertSql` -> `ToInsertSQL`
    * `ToUpdateSql` -> `ToUpdateSQL`
    * `ToDeleteSql` -> `ToDeleteSQL`
    * `ToTruncateSql` -> `ToTruncateSQL`
* Abstracted out `dialect_options` from the adapter to make the dialect self contained.
    * This also removed the dataset<->adapter co dependency making the dialect self contained.
* Refactored the `goqu.I` method.
    * Added new `goqu.S`, `goqu.T` and `goqu.C` methods to clarify why type of identifier you are using.
    * `goqu.I` should only be used when you have a qualified identifier (e.g. `goqu.I("my_schema.my_table.my_col")
* Added new `goqu.Dialect` method to make using `goqu` as an SQL builder easier.

**Internal Changes**
* Pulled expressions into their own package
    * Broke up expressions.go into multiple files to make working with and defining them easier.
    * Moved the user facing methods into the main `goqu` to keep the same API as before.
* Added more examples
* Moved non-user facing structs and interfaces to internal modules to clean up API.
* Increased test coverage.
 

## v6.1.0

* Handle nil *time.Time Literal [#73](https://github.com/doug-martin/goqu/pull/73) and [#52](https://github.com/doug-martin/goqu/pull/52) - [@RoarkeRandall](https://github.com/RoarkeRandall) and [@quetz](https://github.com/quetz)
* Add ability to change column rename function [#66](https://github.com/doug-martin/goqu/pull/66) - [@blainehansen](https://github.com/blainehansen)

## v6.0.0

* Updated go support to `1.10`, `1.11` and `1.12`
* Change testify dependency from c2fo/testify back to stretchr/testify.
* Add support for "FOR UPDATE" and "SKIP LOCKED" [#62](https://github.com/doug-martin/goqu/pull/62) - [@btubbs](https://github.com/btubbs)
* Changed to use go modules

## v5.0.0

* Drop go 1.6 support, supported versions are `1.8`, `1.9` and latest
* Add context support [#64](https://github.com/doug-martin/goqu/pull/64) - [@cmoad](https://github.com/cmoad)

## v4.2.0

* Add support for ON CONFLICT when using a dataset [#55](https://github.com/doug-martin/goqu/pull/55) - [@bobrnor](https://github.com/bobrnor)

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
