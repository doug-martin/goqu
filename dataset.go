package goqu

import (
	"context"
	"fmt"

	"github.com/doug-martin/goqu/v7/exec"
	"github.com/doug-martin/goqu/v7/exp"
	"github.com/doug-martin/goqu/v7/internal/errors"
	"github.com/doug-martin/goqu/v7/internal/sb"
)

type (
	// A Dataset is used to build up an SQL statement, each method returns a copy of the current Dataset with options
	// added to it.
	//
	// Once done building up your Dataset you can either call an action method on it to execute the statement or use
	// one of the SQL generation methods.
	//
	// Common SQL clauses are represented as methods on the Dataset (e.g. Where, From, Select, Limit...)
	//   * ToSQL() - Returns a SELECT statement
	//   * ToUpdateSQL() - Returns an UPDATE statement
	//   * ToInsertSQL(rows ...interface{}) - Returns an INSERT statement
	//   * ToDeleteSQL() - Returns a DELETE statement
	//   * ToTruncateSQL() - Returns a TRUNCATE statement.
	//
	// Each SQL generation method returns an interpolated statement. Without interpolation each SQL statement could
	// cause two calls to the database:
	//   1. Prepare the statement
	//   2. Execute the statement with arguments
	//
	// Instead with interpolation the database just executes the statement
	//   sql, _, err := goqu.From("test").Where(goqu.C("a").Eq(10)).ToSQL()
	//   fmt.Println(sql)
	//
	//   // Output:
	//   // SELECT * FROM "test" WHERE "a" = 10
	//
	// Sometimes you might want to generated a prepared statement in which case you would use one of the "Prepared"
	// method on the dataset
	//   sql, args, err := From("test").Prepared(true).Where(I("a").Eq(10)).ToSQL()
	//   fmt.Println(sql, args)
	//
	//   // Output:
	//   // SELECT * FROM "test" WHERE "a" = ? [10]
	//
	// A Dataset can also execute statements directly. By calling:
	//
	//    * ScanStructs(i interface{}) - Scans returned rows into a slice of structs
	//    * ScanStruct(i interface{}) - Scans a single rom into a struct, if no struct is found this method will return
	//    false
	//    * ScanVals(i interface{}) - Scans rows of one columns into a slice of primitive values
	//    * ScanVal(i interface{}) - Scans a single row of one column into a primitive value
	//    * Count() - Returns a count of rows
	//    * Pluck(i interface{}, col string) - Retrives a columns from rows and scans the resules into a slice of
	//    primitive values.
	//
	// Update, Delete, and Insert return an CrudExec struct which can be used to scan values or just execute the
	// statement. You might
	// use the scan methods if the database supports return values. For example
	//    UPDATE "items" SET updated = NOW RETURNING "items".*
	// Could be executed with ScanStructs.
	Dataset struct {
		dialect      SQLDialect
		clauses      exp.Clauses
		isPrepared   bool
		queryFactory exec.QueryFactory
	}
)

var (
	errQueryFactoryNotFoundError = errors.New(
		"unable to execute query did you use goqu.Database#From to create the dataset",
	)
)

// used internally by database to create a database with a specific adapter
func newDataset(d string, queryFactory exec.QueryFactory) *Dataset {
	return &Dataset{
		clauses:      exp.NewClauses(),
		dialect:      GetDialect(d),
		queryFactory: queryFactory,
	}
}

func From(table ...interface{}) *Dataset {
	return newDataset("default", nil).From(table...)
}

// Sets the adapter used to serialize values and create the SQL statement
func (d *Dataset) WithDialect(dl string) *Dataset {
	ds := d.copy(d.GetClauses())
	ds.dialect = GetDialect(dl)
	return ds
}

// Set the parameter interpolation behavior. See examples
//
// prepared: If true the dataset WILL NOT interpolate the parameters.
func (d *Dataset) Prepared(prepared bool) *Dataset {
	ret := d.copy(d.clauses)
	ret.isPrepared = prepared
	return ret
}

func (d *Dataset) IsPrepared() bool {
	return d.isPrepared
}

// Returns the current adapter on the dataset
func (d *Dataset) Dialect() SQLDialect {
	return d.dialect
}

// Returns the current adapter on the dataset
func (d *Dataset) SetDialect(dialect SQLDialect) *Dataset {
	cd := d.copy(d.GetClauses())
	cd.dialect = dialect
	return cd
}

func (d *Dataset) Expression() exp.Expression {
	return d
}

// Clones the dataset
func (d *Dataset) Clone() exp.Expression {
	return d.copy(d.clauses)
}

// Returns the current clauses on the dataset.
func (d *Dataset) GetClauses() exp.Clauses {
	return d.clauses
}

// used interally to copy the dataset
func (d *Dataset) copy(clauses exp.Clauses) *Dataset {
	return &Dataset{
		dialect:      d.dialect,
		clauses:      clauses,
		isPrepared:   d.isPrepared,
		queryFactory: d.queryFactory,
	}
}

// Creates a WITH clause for a common table expression (CTE).
//
// The name will be available to SELECT from in the associated query; and can optionally
// contain a list of column names "name(col1, col2, col3)".
//
// The name will refer to the results of the specified subquery.
func (d *Dataset) With(name string, subquery exp.Expression) *Dataset {
	return d.copy(d.clauses.CommonTablesAppend(exp.NewCommonTableExpression(false, name, subquery)))
}

// Creates a WITH RECURSIVE clause for a common table expression (CTE)
//
// The name will be available to SELECT from in the associated query; and must
// contain a list of column names "name(col1, col2, col3)" for a recursive clause.
//
// The name will refer to the results of the specified subquery. The subquery for
// a recursive query will always end with a UNION or UNION ALL with a clause that
// refers to the CTE by name.
func (d *Dataset) WithRecursive(name string, subquery exp.Expression) *Dataset {
	return d.copy(d.clauses.CommonTablesAppend(exp.NewCommonTableExpression(true, name, subquery)))
}

// Adds columns to the SELECT clause. See examples
// You can pass in the following.
//   string: Will automatically be turned into an identifier
//   Dataset: Will use the SQL generated from that Dataset. If the dataset is aliased it will use that alias as the
//   column name.
//   LiteralExpression: (See Literal) Will use the literal SQL
//   SQLFunction: (See Func, MIN, MAX, COUNT....)
//   Struct: If passing in an instance of a struct, we will parse the struct for the column names to select.
//   See examples
func (d *Dataset) Select(selects ...interface{}) *Dataset {
	return d.copy(d.clauses.SetSelect(exp.NewColumnListExpression(selects...)))
}

// Adds columns to the SELECT DISTINCT clause. See examples
// You can pass in the following.
//   string: Will automatically be turned into an identifier
//   Dataset: Will use the SQL generated from that Dataset. If the dataset is aliased it will use that alias as the
//   column name.
//   LiteralExpression: (See Literal) Will use the literal SQL
//   SQLFunction: (See Func, MIN, MAX, COUNT....)
//   Struct: If passing in an instance of a struct, we will parse the struct for the column names to select.
//   See examples
func (d *Dataset) SelectDistinct(selects ...interface{}) *Dataset {
	return d.copy(d.clauses.SetSelectDistinct(exp.NewColumnListExpression(selects...)))
}

// Resets to SELECT *. If the SelectDistinct was used the returned Dataset will have the the dataset set to SELECT *.
// See examples.
func (d *Dataset) ClearSelect() *Dataset {
	return d.copy(d.clauses.SetSelect(exp.NewColumnListExpression(exp.Star())))
}

// Adds columns to the SELECT clause. See examples
// You can pass in the following.
//   string: Will automatically be turned into an identifier
//   Dataset: Will use the SQL generated from that Dataset. If the dataset is aliased it will use that alias as the
//   column name.
//   LiteralExpression: (See Literal) Will use the literal SQL
//   SQLFunction: (See Func, MIN, MAX, COUNT....)
func (d *Dataset) SelectAppend(selects ...interface{}) *Dataset {
	return d.copy(d.clauses.SelectAppend(exp.NewColumnListExpression(selects...)))
}

// Adds a FROM clause. This return a new dataset with the original sources replaced. See examples.
// You can pass in the following.
//   string: Will automatically be turned into an identifier
//   Dataset: Will be added as a sub select. If the Dataset is not aliased it will automatically be aliased
//   LiteralExpression: (See Literal) Will use the literal SQL
func (d *Dataset) From(from ...interface{}) *Dataset {
	var sources []interface{}
	numSources := 0
	for _, source := range from {
		if sd, ok := source.(*Dataset); ok && !sd.clauses.HasAlias() {
			numSources++
			sources = append(sources, sd.As(fmt.Sprintf("t%d", numSources)))
		} else {
			sources = append(sources, source)
		}
	}
	return d.copy(d.clauses.SetFrom(exp.NewColumnListExpression(sources...)))
}

// Returns a new Dataset with the current one as an source. If the current Dataset is not aliased (See Dataset#As) then
// it will automatically be aliased. See examples.
func (d *Dataset) FromSelf() *Dataset {
	builder := Dataset{
		dialect: d.dialect,
		clauses: exp.NewClauses(),
	}
	return builder.From(d)

}

// Alias to InnerJoin. See examples.
func (d *Dataset) Join(table exp.Expression, condition exp.JoinCondition) *Dataset {
	return d.InnerJoin(table, condition)
}

// Adds an INNER JOIN clause. See examples.
func (d *Dataset) InnerJoin(table exp.Expression, condition exp.JoinCondition) *Dataset {
	return d.joinTable(exp.NewConditionedJoinExpression(exp.InnerJoinType, table, condition))
}

// Adds a FULL OUTER JOIN clause. See examples.
func (d *Dataset) FullOuterJoin(table exp.Expression, condition exp.JoinCondition) *Dataset {
	return d.joinTable(exp.NewConditionedJoinExpression(exp.FullOuterJoinType, table, condition))
}

// Adds a RIGHT OUTER JOIN clause. See examples.
func (d *Dataset) RightOuterJoin(table exp.Expression, condition exp.JoinCondition) *Dataset {
	return d.joinTable(exp.NewConditionedJoinExpression(exp.RightOuterJoinType, table, condition))
}

// Adds a LEFT OUTER JOIN clause. See examples.
func (d *Dataset) LeftOuterJoin(table exp.Expression, condition exp.JoinCondition) *Dataset {
	return d.joinTable(exp.NewConditionedJoinExpression(exp.LeftOuterJoinType, table, condition))
}

// Adds a FULL JOIN clause. See examples.
func (d *Dataset) FullJoin(table exp.Expression, condition exp.JoinCondition) *Dataset {
	return d.joinTable(exp.NewConditionedJoinExpression(exp.FullJoinType, table, condition))
}

// Adds a RIGHT JOIN clause. See examples.
func (d *Dataset) RightJoin(table exp.Expression, condition exp.JoinCondition) *Dataset {
	return d.joinTable(exp.NewConditionedJoinExpression(exp.RightJoinType, table, condition))
}

// Adds a LEFT JOIN clause. See examples.
func (d *Dataset) LeftJoin(table exp.Expression, condition exp.JoinCondition) *Dataset {
	return d.joinTable(exp.NewConditionedJoinExpression(exp.LeftJoinType, table, condition))
}

// Adds a NATURAL JOIN clause. See examples.
func (d *Dataset) NaturalJoin(table exp.Expression) *Dataset {
	return d.joinTable(exp.NewUnConditionedJoinExpression(exp.NaturalJoinType, table))
}

// Adds a NATURAL LEFT JOIN clause. See examples.
func (d *Dataset) NaturalLeftJoin(table exp.Expression) *Dataset {
	return d.joinTable(exp.NewUnConditionedJoinExpression(exp.NaturalLeftJoinType, table))
}

// Adds a NATURAL RIGHT JOIN clause. See examples.
func (d *Dataset) NaturalRightJoin(table exp.Expression) *Dataset {
	return d.joinTable(exp.NewUnConditionedJoinExpression(exp.NaturalRightJoinType, table))
}

// Adds a NATURAL FULL JOIN clause. See examples.
func (d *Dataset) NaturalFullJoin(table exp.Expression) *Dataset {
	return d.joinTable(exp.NewUnConditionedJoinExpression(exp.NaturalFullJoinType, table))
}

// Adds a CROSS JOIN clause. See examples.
func (d *Dataset) CrossJoin(table exp.Expression) *Dataset {
	return d.joinTable(exp.NewUnConditionedJoinExpression(exp.CrossJoinType, table))
}

// Joins this Datasets table with another
func (d *Dataset) joinTable(join exp.JoinExpression) *Dataset {
	return d.copy(d.clauses.JoinsAppend(join))
}

// Adds a WHERE clause. See examples.
func (d *Dataset) Where(expressions ...exp.Expression) *Dataset {
	return d.copy(d.clauses.WhereAppend(expressions...))
}

// Removes the WHERE clause. See examples.
func (d *Dataset) ClearWhere() *Dataset {
	return d.copy(d.clauses.ClearWhere())
}

// Adds a FOR UPDATE clause. See examples.
func (d *Dataset) ForUpdate(waitOption exp.WaitOption) *Dataset {
	return d.withLock(exp.ForUpdate, waitOption)
}

// Adds a FOR NO KEY UPDATE clause. See examples.
func (d *Dataset) ForNoKeyUpdate(waitOption exp.WaitOption) *Dataset {
	return d.withLock(exp.ForNoKeyUpdate, waitOption)
}

// Adds a FOR KEY SHARE clause. See examples.
func (d *Dataset) ForKeyShare(waitOption exp.WaitOption) *Dataset {
	return d.withLock(exp.ForKeyShare, waitOption)
}

// Adds a FOR SHARE clause. See examples.
func (d *Dataset) ForShare(waitOption exp.WaitOption) *Dataset {
	return d.withLock(exp.ForShare, waitOption)
}

func (d *Dataset) withLock(strength exp.LockStrength, option exp.WaitOption) *Dataset {
	return d.copy(d.clauses.SetLock(exp.NewLock(strength, option)))
}

// Adds a GROUP BY clause. See examples.
func (d *Dataset) GroupBy(groupBy ...interface{}) *Dataset {
	return d.copy(d.clauses.SetGroupBy(exp.NewColumnListExpression(groupBy...)))
}

// Adds a HAVING clause. See examples.
func (d *Dataset) Having(expressions ...exp.Expression) *Dataset {
	return d.copy(d.clauses.HavingAppend(expressions...))
}

// Adds a ORDER clause. If the ORDER is currently set it replaces it. See examples.
func (d *Dataset) Order(order ...exp.OrderedExpression) *Dataset {
	return d.copy(d.clauses.SetOrder(order...))
}

// Adds a more columns to the current ORDER BY clause. If no order has be previously specified it is the same as
// calling Order. See examples.
func (d *Dataset) OrderAppend(order ...exp.OrderedExpression) *Dataset {
	return d.copy(d.clauses.OrderAppend(order...))
}

// Adds a more columns to the beginning of the current ORDER BY clause. If no order has be previously specified it is the same as
// calling Order. See examples.
func (d *Dataset) OrderPrepend(order ...exp.OrderedExpression) *Dataset {
	return d.copy(d.clauses.OrderPrepend(order...))
}

// Removes the ORDER BY clause. See examples.
func (d *Dataset) ClearOrder() *Dataset {
	return d.copy(d.clauses.ClearOrder())
}

// Adds a LIMIT clause. If the LIMIT is currently set it replaces it. See examples.
func (d *Dataset) Limit(limit uint) *Dataset {
	if limit > 0 {
		return d.copy(d.clauses.SetLimit(limit))
	}
	return d.copy(d.clauses.ClearLimit())
}

// Adds a LIMIT ALL clause. If the LIMIT is currently set it replaces it. See examples.
func (d *Dataset) LimitAll() *Dataset {
	return d.copy(d.clauses.SetLimit(L("ALL")))
}

// Removes the LIMIT clause.
func (d *Dataset) ClearLimit() *Dataset {
	return d.copy(d.clauses.ClearLimit())
}

// Adds an OFFSET clause. If the OFFSET is currently set it replaces it. See examples.
func (d *Dataset) Offset(offset uint) *Dataset {
	return d.copy(d.clauses.SetOffset(offset))
}

// Removes the OFFSET clause from the Dataset
func (d *Dataset) ClearOffset() *Dataset {
	return d.copy(d.clauses.ClearOffset())
}

// Creates an UNION statement with another dataset.
// If this or the other dataset has a limit or offset it will use that dataset as a subselect in the FROM clause.
// See examples.
func (d *Dataset) Union(other *Dataset) *Dataset {
	return d.withCompound(exp.UnionCompoundType, other.CompoundFromSelf())
}

// Creates an UNION ALL statement with another dataset.
// If this or the other dataset has a limit or offset it will use that dataset as a subselect in the FROM clause.
// See examples.
func (d *Dataset) UnionAll(other *Dataset) *Dataset {
	return d.withCompound(exp.UnionAllCompoundType, other.CompoundFromSelf())
}

// Creates an INTERSECT statement with another dataset.
// If this or the other dataset has a limit or offset it will use that dataset as a subselect in the FROM clause.
// See examples.
func (d *Dataset) Intersect(other *Dataset) *Dataset {
	return d.withCompound(exp.IntersectCompoundType, other.CompoundFromSelf())
}

// Creates an INTERSECT ALL statement with another dataset.
// If this or the other dataset has a limit or offset it will use that dataset as a subselect in the FROM clause.
// See examples.
func (d *Dataset) IntersectAll(other *Dataset) *Dataset {
	return d.withCompound(exp.IntersectAllCompoundType, other.CompoundFromSelf())
}

func (d *Dataset) withCompound(ct exp.CompoundType, other exp.AppendableExpression) *Dataset {
	ce := exp.NewCompoundExpression(ct, other)
	ret := d.CompoundFromSelf()
	ret.clauses = ret.clauses.CompoundsAppend(ce)
	return ret
}

// Used internally to determine if the dataset needs to use iteself as a source.
// If the dataset has an order or limit it will select from itself
func (d *Dataset) CompoundFromSelf() *Dataset {
	if d.clauses.HasOrder() || d.clauses.HasLimit() {
		return d.FromSelf()
	}
	return d.copy(d.clauses)
}

// Adds a RETURNING clause to the dataset if the adapter supports it. Typically used for INSERT, UPDATE or DELETE.
// See examples.
func (d *Dataset) Returning(returning ...interface{}) *Dataset {
	return d.copy(d.clauses.SetReturning(exp.NewColumnListExpression(returning...)))
}

// Sets the alias for this dataset. This is typically used when using a Dataset as a subselect. See examples.
func (d *Dataset) As(alias string) *Dataset {
	return d.copy(d.clauses.SetAlias(T(alias)))
}

// Generates a SELECT sql statement, if Prepared has been called with true then the parameters will not be interpolated.
// See examples.
//
// Errors:
//  * There is an error generating the SQL
func (d *Dataset) ToSQL() (sql string, params []interface{}, err error) {
	return d.selectSQLBuilder().ToSQL()
}

// Appends this Dataset's SELECT statement to the SQLBuilder
// This is used internally for sub-selects by the dialect
func (d *Dataset) AppendSQL(b sb.SQLBuilder) {
	d.dialect.ToSelectSQL(b, d.GetClauses())
}

// Generates the default INSERT statement. If Prepared has been called with true then the statement will not be
// interpolated. See examples. When using structs you may specify a column to be skipped in the insert, (e.g. id) by
// specifying a goqu tag with `skipinsert`
//    type Item struct{
//       Id   uint32 `db:"id" goqu:"skipinsert"`
//       Name string `db:"name"`
//    }
//
// rows: variable number arguments of either map[string]interface, Record, struct, or a single slice argument of the
// accepted types.
//
// Errors:
//  * There is no FROM clause
//  * Different row types passed in, all rows must be of the same type
//  * Maps with different numbers of K/V pairs
//  * Rows of different lengths, (i.e. (Record{"name": "a"}, Record{"name": "a", "age": 10})
//  * Error generating SQL
func (d *Dataset) ToInsertSQL(rows ...interface{}) (sql string, params []interface{}, err error) {
	return d.toInsertSQL(nil, rows...)
}

// Generates the default INSERT IGNORE/ INSERT ... ON CONFLICT DO NOTHING statement. If Prepared has been called with
// true then the statement will not be interpolated. See examples.
//
// c: ConflictExpression action. Can be DoNothing/Ignore or DoUpdate/DoUpdateWhere.
// rows: variable number arguments of either map[string]interface, Record, struct, or a single slice argument of the
// accepted types.
//
// Errors:
//  * There is no FROM clause
//  * Different row types passed in, all rows must be of the same type
//  * Maps with different numbers of K/V pairs
//  * Rows of different lengths, (i.e. (Record{"name": "a"}, Record{"name": "a", "age": 10})
//  * Error generating SQL
func (d *Dataset) ToInsertIgnoreSQL(rows ...interface{}) (sql string, params []interface{}, err error) {
	return d.toInsertSQL(DoNothing(), rows...)
}

// Generates the INSERT [IGNORE] ... ON CONFLICT/DUPLICATE KEY. If Prepared has been called with true then the statement
// will not be interpolated. See examples.
//
// rows: variable number arguments of either map[string]interface, Record, struct, or a single slice argument of the
// accepted types.
//
// Errors:
//  * There is no FROM clause
//  * Different row types passed in, all rows must be of the same type
//  * Maps with different numbers of K/V pairs
//  * Rows of different lengths, (i.e. (Record{"name": "a"}, Record{"name": "a", "age": 10})
//  * Error generating SQL
func (d *Dataset) ToInsertConflictSQL(o exp.ConflictExpression, rows ...interface{}) (sql string, params []interface{}, err error) {
	return d.toInsertSQL(o, rows)
}

// Generates an UPDATE statement. If `Prepared` has been called with true then the statement will not be interpolated.
// When using structs you may specify a column to be skipped in the update, (e.g. created) by specifying a goqu tag with `skipupdate`
//    type Item struct{
//       Id      uint32    `db:"id"
//       Created time.Time `db:"created" goqu:"skipupdate"`
//       Name    string    `db:"name"`
//    }
//
// update: can either be a a map[string]interface{}, Record or a struct
//
// Errors:
//  * The update is not a of type struct, Record, or map[string]interface{}
//  * The update statement has no FROM clause
//  * There is an error generating the SQL
func (d *Dataset) ToUpdateSQL(update interface{}) (sql string, params []interface{}, err error) {
	return d.updateSQLBuilder(update).ToSQL()
}

// Generates a DELETE statement, if Prepared has been called with true then the statement will not be interpolated. See examples.
//
// isPrepared: Set to true to true to ensure values are NOT interpolated
//
// Errors:
//  * There is no FROM clause
//  * Error generating SQL
func (d *Dataset) ToDeleteSQL() (sql string, params []interface{}, err error) {
	return d.deleteSQLBuilder().ToSQL()
}

// Generates the default TRUNCATE statement. See examples.
//
// Errors:
//  * There is no FROM clause
//  * Error generating SQL
func (d *Dataset) ToTruncateSQL() (sql string, params []interface{}, err error) {
	return d.ToTruncateWithOptsSQL(exp.TruncateOptions{})
}

// Generates the default TRUNCATE statement with the specified options. See examples.
//
// opts: Options to use when generating the TRUNCATE statement
//
// Errors:
//  * There is no FROM clause
//  * Error generating SQL
func (d *Dataset) ToTruncateWithOptsSQL(opts exp.TruncateOptions) (sql string, params []interface{}, err error) {
	return d.truncateSQLBuilder(opts).ToSQL()
}

// Generates the SELECT sql for this dataset and uses Exec#ScanStructs to scan the results into a slice of structs.
//
// ScanStructs will only select the columns that can be scanned in to the struct unless you have explicitly selected
// certain columns. See examples.
//
// i: A pointer to a slice of structs
func (d *Dataset) ScanStructs(i interface{}) error {
	return d.ScanStructsContext(context.Background(), i)
}

// Generates the SELECT sql for this dataset and uses Exec#ScanStructsContext to scan the results into a slice of
// structs.
//
// ScanStructsContext will only select the columns that can be scanned in to the struct unless you have explicitly
// selected certain columns. See examples.
//
// i: A pointer to a slice of structs
func (d *Dataset) ScanStructsContext(ctx context.Context, i interface{}) error {
	if d.queryFactory == nil {
		return errQueryFactoryNotFoundError
	}
	ds := d
	if d.GetClauses().IsDefaultSelect() {
		ds = d.Select(i)
	}
	return d.queryFactory.FromSQLBuilder(ds.selectSQLBuilder()).ScanStructsContext(ctx, i)
}

// Generates the SELECT sql for this dataset and uses Exec#ScanStruct to scan the result into a slice of structs
//
// ScanStruct will only select the columns that can be scanned in to the struct unless you have explicitly selected
// certain columns. See examples.
//
// i: A pointer to a structs
func (d *Dataset) ScanStruct(i interface{}) (bool, error) {
	return d.ScanStructContext(context.Background(), i)
}

// Generates the SELECT sql for this dataset and uses Exec#ScanStructContext to scan the result into a slice of structs
//
// ScanStructContext will only select the columns that can be scanned in to the struct unless you have explicitly
// selected certain columns. See examples.
//
// i: A pointer to a structs
func (d *Dataset) ScanStructContext(ctx context.Context, i interface{}) (bool, error) {
	if d.queryFactory == nil {
		return false, errQueryFactoryNotFoundError
	}
	ds := d
	if d.GetClauses().IsDefaultSelect() {
		ds = d.Select(i)
	}
	return d.queryFactory.FromSQLBuilder(ds.Limit(1).selectSQLBuilder()).ScanStructContext(ctx, i)
}

// Generates the SELECT sql for this dataset and uses Exec#ScanVals to scan the results into a slice of primitive values
//
// i: A pointer to a slice of primitive values
func (d *Dataset) ScanVals(i interface{}) error {
	return d.ScanValsContext(context.Background(), i)
}

// Generates the SELECT sql for this dataset and uses Exec#ScanValsContext to scan the results into a slice of primitive
// values
//
// i: A pointer to a slice of primitive values
func (d *Dataset) ScanValsContext(ctx context.Context, i interface{}) error {
	if d.queryFactory == nil {
		return errQueryFactoryNotFoundError
	}
	return d.queryFactory.FromSQLBuilder(d.selectSQLBuilder()).ScanValsContext(ctx, i)
}

// Generates the SELECT sql for this dataset and uses Exec#ScanVal to scan the result into a primitive value
//
// i: A pointer to a primitive value
func (d *Dataset) ScanVal(i interface{}) (bool, error) {
	return d.ScanValContext(context.Background(), i)
}

// Generates the SELECT sql for this dataset and uses Exec#ScanValContext to scan the result into a primitive value
//
// i: A pointer to a primitive value
func (d *Dataset) ScanValContext(ctx context.Context, i interface{}) (bool, error) {
	if d.queryFactory == nil {
		return false, errQueryFactoryNotFoundError
	}
	b := d.Limit(1).selectSQLBuilder()
	return d.queryFactory.FromSQLBuilder(b).ScanValContext(ctx, i)
}

// Generates the SELECT COUNT(*) sql for this dataset and uses Exec#ScanVal to scan the result into an int64.
func (d *Dataset) Count() (int64, error) {
	return d.CountContext(context.Background())
}

// Generates the SELECT COUNT(*) sql for this dataset and uses Exec#ScanValContext to scan the result into an int64.
func (d *Dataset) CountContext(ctx context.Context) (int64, error) {
	var count int64
	_, err := d.Select(COUNT(Star()).As("count")).ScanValContext(ctx, &count)
	return count, err
}

// Generates the SELECT sql only selecting the passed in column and uses Exec#ScanVals to scan the result into a slice
// of primitive values.
//
// i: A slice of primitive values
//
// col: The column to select when generative the SQL
func (d *Dataset) Pluck(i interface{}, col string) error {
	return d.PluckContext(context.Background(), i, col)
}

// Generates the SELECT sql only selecting the passed in column and uses Exec#ScanValsContext to scan the result into a
// slice of primitive values.
//
// i: A slice of primitive values
//
// col: The column to select when generative the SQL
func (d *Dataset) PluckContext(ctx context.Context, i interface{}, col string) error {
	return d.Select(col).ScanValsContext(ctx, i)
}

// Generates the UPDATE sql, and returns an Exec struct with the sql set to the UPDATE statement
//    db.From("test").Update(Record{"name":"Bob", update: time.Now()}).Exec()
//
// See Dataset#ToUpdateSQL for arguments
func (d *Dataset) Update(i interface{}) exec.QueryExecutor {
	return d.queryFactory.FromSQLBuilder(d.updateSQLBuilder(i))
}

// Generates the INSERT sql, and returns an Exec struct with the sql set to the INSERT statement
//    db.From("test").Insert(Record{"name":"Bob"}).Exec()
//
// See Dataset#ToInsertSQL for arguments
func (d *Dataset) Insert(i ...interface{}) exec.QueryExecutor {
	return d.InsertConflict(nil, i...)
}

// Generates the INSERT IGNORE (mysql) or INSERT ... ON CONFLICT DO NOTHING (postgres) and returns an Exec struct.
//    db.From("test").InsertIgnore(DoNothing(), Record{"name":"Bob"}).Exec()
//
// See Dataset#ToInsertConflictSQL for arguments
func (d *Dataset) InsertIgnore(i ...interface{}) exec.QueryExecutor {
	return d.InsertConflict(DoNothing(), i...)
}

// Generates the INSERT sql with (ON CONFLICT/ON DUPLICATE KEY) clause, and returns an Exec struct with the sql set to
// the INSERT statement
//    db.From("test").InsertConflict(DoNothing(), Record{"name":"Bob"}).Exec()
//
// See Dataset#Upsert for arguments
func (d *Dataset) InsertConflict(c exp.ConflictExpression, i ...interface{}) exec.QueryExecutor {
	return d.queryFactory.FromSQLBuilder(d.insertSQLBuilder(c, i...))
}

// Generates the DELETE sql, and returns an Exec struct with the sql set to the DELETE statement
//    db.From("test").Where(I("id").Gt(10)).Exec()
func (d *Dataset) Delete() exec.QueryExecutor {
	return d.queryFactory.FromSQLBuilder(d.deleteSQLBuilder())
}

func (d *Dataset) selectSQLBuilder() sb.SQLBuilder {
	buf := sb.NewSQLBuilder(d.isPrepared)
	d.dialect.ToSelectSQL(buf, d.GetClauses())
	return buf
}

func (d *Dataset) toInsertSQL(ce exp.ConflictExpression, rows ...interface{}) (sql string, params []interface{}, err error) {
	return d.insertSQLBuilder(ce, rows...).ToSQL()
}

func (d *Dataset) insertSQLBuilder(ce exp.ConflictExpression, rows ...interface{}) sb.SQLBuilder {
	buf := sb.NewSQLBuilder(d.isPrepared)
	ie, err := exp.NewInsertExpression(rows...)
	if err != nil {
		return buf.SetError(err)
	}
	d.dialect.ToInsertSQL(buf, d.clauses, ie.SetOnConflict(ce))
	return buf
}

func (d *Dataset) updateSQLBuilder(update interface{}) sb.SQLBuilder {
	buf := sb.NewSQLBuilder(d.isPrepared)
	d.dialect.ToUpdateSQL(buf, d.clauses, update)
	return buf
}

func (d *Dataset) deleteSQLBuilder() sb.SQLBuilder {
	buf := sb.NewSQLBuilder(d.isPrepared)
	d.dialect.ToDeleteSQL(buf, d.clauses)
	return buf
}

func (d *Dataset) truncateSQLBuilder(opts exp.TruncateOptions) sb.SQLBuilder {
	buf := sb.NewSQLBuilder(d.isPrepared)
	d.dialect.ToTruncateSQL(buf, d.clauses, opts)
	return buf
}
