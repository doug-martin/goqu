package goqu

import (
	"reflect"
	"strings"
	"time"
)

type (
	//An adapter interface to be used by a Dataset to generate SQL for a specific dialect.
	//See DefaultAdapter for a concrete implementation and examples.
	Adapter interface {
		//Returns true if the dialect supports ORDER BY expressions in DELETE statements
		SupportsOrderByOnDelete() bool
		//Returns true if the dialect supports ORDER BY expressions in UPDATE statements
		SupportsOrderByOnUpdate() bool
		//Returns true if the dialect supports LIMIT expressions in DELETE statements
		SupportsLimitOnDelete() bool
		//Returns true if the dialect supports LIMIT expressions in UPDATE statements
		SupportsLimitOnUpdate() bool
		//Returns true if the dialect supports RETURN expressions
		SupportsReturn() bool
		//Generates the sql for placeholders. Only invoked when not interpolating values.
		//
		//buf: The current SqlBuilder to write the sql to
		//i: the value that should be added the the sqlbuilders args.
		PlaceHolderSql(buf *SqlBuilder, i interface{}) error
		//Generates the correct beginning sql for an UPDATE statement
		//
		//buf: The current SqlBuilder to write the sql to
		UpdateBeginSql(buf *SqlBuilder) error
		//Generates the correct beginning sql for an INSERT statement
		//
		//buf: The current SqlBuilder to write the sql to
		InsertBeginSql(buf *SqlBuilder, o ConflictExpression) error
		//Generates the correct beginning sql for a DELETE statement
		//
		//buf: The current SqlBuilder to write the sql to
		DeleteBeginSql(buf *SqlBuilder) error
		//Generates the correct beginning sql for a TRUNCATE statement
		//
		//buf: The current SqlBuilder to write the sql to
		TruncateSql(buf *SqlBuilder, cols ColumnList, opts TruncateOptions) error
		//Generates the correct sql for inserting default values in SQL
		//
		//buf: The current SqlBuilder to write the sql to
		DefaultValuesSql(buf *SqlBuilder) error
		//Generates the sql for update expressions
		//
		//buf: The current SqlBuilder to write the sql to
		UpdateExpressionsSql(buf *SqlBuilder, updates ...UpdateExpression) error
		//Generates the sql for the SELECT and ColumnList for a select statement
		//
		//buf: The current SqlBuilder to write the sql to
		SelectSql(buf *SqlBuilder, cols ColumnList) error
		//Generates the sql for the SELECT DISTINCT and ColumnList for a select statement
		//
		//buf: The current SqlBuilder to write the sql to
		SelectDistinctSql(buf *SqlBuilder, cols ColumnList) error
		//Generates the sql for a RETURNING clause
		//
		//buf: The current SqlBuilder to write the sql to
		ReturningSql(buf *SqlBuilder, cols ColumnList) error
		//Generates the sql for a FROM clause
		//
		//buf: The current SqlBuilder to write the sql to
		FromSql(buf *SqlBuilder, from ColumnList) error
		//Generates the sql for a list of columns.
		//
		//buf: The current SqlBuilder to write the sql to
		SourcesSql(buf *SqlBuilder, from ColumnList) error
		//Generates the sql for JoiningClauses clauses
		//
		//buf: The current SqlBuilder to write the sql to
		JoinSql(buf *SqlBuilder, joins JoiningClauses) error
		//Generates the sql for WHERE clause
		//
		//buf: The current SqlBuilder to write the sql to
		WhereSql(buf *SqlBuilder, where ExpressionList) error
		//Generates the sql for GROUP BY clause
		//
		//buf: The current SqlBuilder to write the sql to
		GroupBySql(buf *SqlBuilder, groupBy ColumnList) error
		//Generates the sql for HAVING clause
		//
		//buf: The current SqlBuilder to write the sql to
		HavingSql(buf *SqlBuilder, having ExpressionList) error
		//Generates the sql for COMPOUND expressions, such as UNION, and INTERSECT
		//
		//buf: The current SqlBuilder to write the sql to
		CompoundsSql(buf *SqlBuilder, compounds []CompoundExpression) error
		//Generates the sql for the WITH clauses for common table expressions (CTE)
		//
		//buf: The current SqlBuilder to write the sql to
		CommonTablesSql(buf *SqlBuilder, ctes []CommonTableExpression) error
		//Generates the sql for ORDER BY clause
		//
		//buf: The current SqlBuilder to write the sql to
		OrderSql(buf *SqlBuilder, order ColumnList) error
		//Generates the sql for LIMIT clause
		//
		//buf: The current SqlBuilder to write the sql to
		LimitSql(buf *SqlBuilder, limit interface{}) error
		//Generates the sql for OFFSET clause
		//
		//buf: The current SqlBuilder to write the sql to
		OffsetSql(buf *SqlBuilder, offset uint) error
		//Generates the sql for FOR clause
		//
		//buf: The current SqlBuilder to write the sql to
		ForSql(buf *SqlBuilder, lockingClause Lock) error
		//Generates the sql for another Dataset being used as a sub select.
		//
		//buf: The current SqlBuilder to write the sql to
		DatasetSql(buf *SqlBuilder, builder Dataset) error
		//Correctly quotes an Identifier for use in SQL.
		//
		//buf: The current SqlBuilder to write the sql to
		QuoteIdentifier(buf *SqlBuilder, ident IdentifierExpression) error
		//Generates SQL value for nil
		//
		//buf: The current SqlBuilder to write the sql to
		LiteralNil(buf *SqlBuilder) error
		//Generates SQL value for a bool (e.g. TRUE, FALSE, 1, 0)
		//
		//buf: The current SqlBuilder to write the sql to
		LiteralBool(buf *SqlBuilder, b bool) error
		//Generates SQL value for a time.Time
		//
		//buf: The current SqlBuilder to write the sql to
		LiteralTime(buf *SqlBuilder, t time.Time) error
		//Generates SQL value for float64
		//
		//buf: The current SqlBuilder to write the sql to
		LiteralFloat(buf *SqlBuilder, f float64) error
		//Generates SQL value for an int64
		//
		//buf: The current SqlBuilder to write the sql to
		LiteralInt(buf *SqlBuilder, i int64) error
		//Generates SQL value for a string
		//
		//buf: The current SqlBuilder to write the sql to
		LiteralString(buf *SqlBuilder, s string) error
		//Generates SQL value for a Slice of Bytes
		//
		//buf: The current SqlBuilder to write the sql to
		LiteralBytes(buf *SqlBuilder, bs []byte) error
		//Generates SQL value for a Slice
		//
		//buf: The current SqlBuilder to write the sql to
		SliceValueSql(buf *SqlBuilder, slice reflect.Value) error
		//Generates SQL value for an AliasedExpression
		//
		//buf: The current SqlBuilder to write the sql to
		AliasedExpressionSql(buf *SqlBuilder, aliased AliasedExpression) error
		//Generates SQL value for a BooleanExpression
		//
		//buf: The current SqlBuilder to write the sql to
		BooleanExpressionSql(buf *SqlBuilder, operator BooleanExpression) error
		//Generates SQL value for a RangeExpression
		//
		//buf: The current SqlBuilder to write the sql to
		RangeExpressionSql(buf *SqlBuilder, operator RangeExpression) error
		//Generates SQL value for an OrderedExpression
		//
		//buf: The current SqlBuilder to write the sql to
		OrderedExpressionSql(buf *SqlBuilder, order OrderedExpression) error
		//Generates SQL value for an ExpressionList
		//
		//buf: The current SqlBuilder to write the sql to
		ExpressionListSql(buf *SqlBuilder, expressionList ExpressionList) error
		//Generates SQL value for a SqlFunction
		//
		//buf: The current SqlBuilder to write the sql to
		SqlFunctionExpressionSql(buf *SqlBuilder, sqlFunc SqlFunctionExpression) error
		//Generates SQL value for a CastExpression
		//
		//buf: The current SqlBuilder to write the sql to
		CastExpressionSql(buf *SqlBuilder, casted CastExpression) error
		//Generates SQL value for a CompoundExpression
		//
		//buf: The current SqlBuilder to write the sql to
		CompoundExpressionSql(buf *SqlBuilder, compound CompoundExpression) error
		//Generates SQL value for a CommonTableExpression
		//
		//buf: The current SqlBuilder to write the sql to
		CommonTableExpressionSql(buf *SqlBuilder, commonTable CommonTableExpression) error
		//Generates SQL value for a ColumnList
		//
		//buf: The current SqlBuilder to write the sql to
		ColumnListSql(buf *SqlBuilder, columnList ColumnList) error
		//Generates SQL value for an UpdateExpression
		//
		//buf: The current SqlBuilder to write the sql to
		UpdateExpressionSql(buf *SqlBuilder, update UpdateExpression) error
		Literal(buf *SqlBuilder, i interface{}) error
		//Generates SQL value for a LiteralExpression
		//
		//buf: The current SqlBuilder to write the sql to
		LiteralExpressionSql(buf *SqlBuilder, literal LiteralExpression) error
		//Generates SQL value for an Ex Expression map
		//
		//buf: The current SqlBuilder to write the sql to
		ExpressionMapSql(buf *SqlBuilder, ex Ex) error
		//Generates SQL value for an ExOr Expression map
		//
		//buf: The current SqlBuilder to write the sql to
		ExpressionOrMapSql(buf *SqlBuilder, ex ExOr) error
		//Generates SQL value for the columns in an INSERT statement
		//
		//buf: The current SqlBuilder to write the sql to
		InsertColumnsSql(buf *SqlBuilder, cols ColumnList) error
		//Generates SQL value for the values in an INSERT statement
		//
		//buf: The current SqlBuilder to write the sql to
		InsertValuesSql(buf *SqlBuilder, values [][]interface{}) error
		//Returns true if the dialect supports INSERT IGNORE INTO syntax
		SupportsInsertIgnoreSyntax() bool
		//Returns true if the dialect supports ON CONFLICT (key) expressions
		SupportsConflictTarget() bool
		//Generates SQL value for the ON CONFLICT clause of an INSERT statement
		//
		//buf: The current SqlBuilder to write the sql to
		OnConflictSql(buf *SqlBuilder, o ConflictExpression) error
		//Returns true if the dialect supports a WHERE clause on upsert
		SupportConflictUpdateWhere() bool
		//Returns true if the dialect supports WITH common table expressions
		SupportsWithCTE() bool
		//Returns true if the dialect supports WITH RECURSIVE common table expressions
		SupportsWithRecursiveCTE() bool
	}
)

var ds_adapters = make(map[string]func(dataset *Dataset) Adapter)

//Registers an adapter.
//
//   dialect: The dialect this adapter is for
//   factory: a function that can be called to create a new Adapter for the dialect.
func RegisterAdapter(dialect string, factory func(ds *Dataset) Adapter) {
	dialect = strings.ToLower(dialect)
	ds_adapters[dialect] = factory
}

//Returns true if the dialect has an adapter registered
//
//  dialect: The dialect to test
func HasAdapter(dialect string) bool {
	dialect = strings.ToLower(dialect)
	_, ok := ds_adapters[dialect]
	return ok
}

//Clears the current adapter map, used internally for tests
func removeAdapter(dialect string) {
	if HasAdapter(dialect) {
		dialect = strings.ToLower(dialect)
		delete(ds_adapters, dialect)
	}
}

//Creates the appropriate adapter for the given dialect.
//
//   dialect: the dialect to create an adapter for
//   dataset: The dataset to be used by the adapter
func NewAdapter(dialect string, dataset *Dataset) Adapter {
	dialect = strings.ToLower(dialect)
	if adapterGen, ok := ds_adapters[dialect]; ok {
		return adapterGen(dataset)
	}
	return NewDefaultAdapter(dataset)
}
