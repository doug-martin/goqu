package goqu

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"
)

type (
	countResult struct {
		Count int64 `db:"count"`
	}
	valueSlice []reflect.Value
	Logger     interface {
		Printf(format string, v ...interface{})
	}
	clauses struct {
		CommonTables   []CommonTableExpression
		Select         ColumnList
		SelectDistinct ColumnList
		From           ColumnList
		Joins          JoiningClauses
		Where          ExpressionList
		Alias          IdentifierExpression
		GroupBy        ColumnList
		Having         ExpressionList
		Order          ColumnList
		Limit          interface{}
		Offset         uint
		Returning      ColumnList
		Compounds      []CompoundExpression
		Lock           Lock
	}
	//A Dataset is used to build up an SQL statement, each method returns a copy of the current Dataset with options added to it.
	//Once done building up your Dataset you can either call an action method on it to execute the statement or use one of the SQL generation methods.
	//
	//Common SQL clauses are represented as methods on the Dataset (e.g. Where, From, Select, Limit...)
	//   * Sql() - Returns a SELECT statement
	//   * UpdateSql() - Returns an UPDATE statement
	//   * InsertSql() - Returns an INSERT statement
	//   * DeleteSql() - Returns a DELETE statement
	//   * TruncateSql() - Returns a TRUNCATE statement.
	//
	//Each SQL generation method returns an interpolated statement. Without interpolation each SQL statement could cause two calls to the database:
	//   1. Prepare the statement
	//   2. Execute the statment with arguments
	//Instead with interpolation the database just executes the statement
	//    sql, err := From("test").Where(I("a").Eq(10).Sql() //SELECT * FROM "test" WHERE "a" = 10
	//
	//Sometimes you might want to generated a prepared statement in which case you would use one of the "To" SQL generation methods, with the isPrepared argument set to true.
	//  * ToSql(true) - generates a SELECT statement without the arguments interpolated
	//  * ToUpdateSql(true, update) - generates an UPDATE statement without the arguments interpolated
	//  * ToInsertSql(true, rows....) - generates an INSERT statement without the arguments interpolated
	//  * ToDeleteSql(true) - generates a DELETE statement without arguments interpolated
	//  * ToTruncateSql(true, opts) - generates a TRUNCATE statement without arguments interpolated
	//
	//    sql, args, err := From("test").Where(I("a").Eq(10).ToSql(true) //sql := SELECT * FROM "test" WHERE "a" = ? args:=[]interface{}{10}
	//
	//A Dataset can also execute statements directly. By calling:
	//
	//    * ScanStructs(i interface{}) - Scans returned rows into a slice of structs
	//    * ScanStruct(i interface{}) - Scans a single rom into a struct, if no struct is found this method will return false
	//    * ScanVals(i interface{}) - Scans rows of one columns into a slice of primitive values
	//    * ScanVal(i interface{}) - Scans a single row of one column into a primitive value
	//    * Count() - Returns a count of rows
	//    * Pluck(i interface{}, col string) - Retrives a columns from rows and scans the resules into a slice of primitive values.
	//
	//Update, Delete, and Insert return an CrudExec struct which can be used to scan values or just execute the statment. You might
	//use the scan methods if the database supports return values. For example
	//    UPDATE "items" SET updated = NOW RETURNING "items".*
	//Could be executed with ScanStructs.
	Dataset struct {
		adapter    Adapter
		clauses    clauses
		database   database
		isPrepared bool
	}
)

func (me valueSlice) Len() int           { return len(me) }
func (me valueSlice) Less(i, j int) bool { return me[i].String() < me[j].String() }
func (me valueSlice) Swap(i, j int)      { me[i], me[j] = me[j], me[i] }

func (me valueSlice) Equal(other valueSlice) bool {
	sort.Sort(other)
	for i, key := range me {
		if other[i].String() != key.String() {
			return false
		}
	}
	return true
}

func (me valueSlice) String() string {
	vals := make([]string, me.Len())
	for i, key := range me {
		vals[i] = fmt.Sprintf(`"%s"`, key.String())
	}
	sort.Strings(vals)
	return fmt.Sprintf("[%s]", strings.Join(vals, ","))
}

//Returns a dataset with the DefaultAdapter. Typically you would use Database#From.
func From(table ...interface{}) *Dataset {
	ret := new(Dataset)
	ret.adapter = NewDefaultAdapter(ret)
	ret.clauses = clauses{
		Select: cols(Star()),
	}
	return ret.From(table...)
}

//Creates a WITH clause for a common table expression (CTE).
//
//The name will be available to SELECT from in the associated query; and can optionally
//contain a list of column names "name(col1, col2, col3)".
//
//The name will refer to the results of the specified subquery.
func (me *Dataset) With(name string, subquery *Dataset) *Dataset {
	ret := me.copy()
	ret.clauses.CommonTables = append(ret.clauses.CommonTables, With(false, name, subquery))
	return ret
}

//Creates a WITH RECURSIVE clause for a common table expression (CTE)
//
//The name will be available to SELECT from in the associated query; and must
//contain a list of column names "name(col1, col2, col3)" for a recursive clause.
//
//The name will refer to the results of the specified subquery. The subquery for
//a recursive query will always end with a UNION or UNION ALL with a clause that
//refers to the CTE by name.
func (me *Dataset) WithRecursive(name string, subquery *Dataset) *Dataset {
	ret := me.copy()
	ret.clauses.CommonTables = append(ret.clauses.CommonTables, With(true, name, subquery))
	return ret
}

//used internally by database to create a database with a specific adapter
func withDatabase(db database) *Dataset {
	ret := new(Dataset)
	ret.database = db
	ret.clauses = clauses{
		Select: cols(Star()),
	}
	ret.adapter = db.queryAdapter(ret)
	return ret
}

//Sets the adapter used to serialize values and create the SQL statement
func (me *Dataset) SetAdapter(adapter Adapter) *Dataset {
	me.adapter = adapter
	return me
}

//Set the parameter interpolation behavior. See examples
//
//prepared: If true the dataset WILL NOT interpolate the parameters.
func (me *Dataset) Prepared(prepared bool) *Dataset {
	ret := me.copy()
	ret.isPrepared = prepared
	return ret
}

//Returns the current adapter on the dataset
func (me *Dataset) Adapter() Adapter {
	return me.adapter
}

func (me *Dataset) Expression() Expression {
	return me
}

//Clones the dataset
func (me *Dataset) Clone() Expression {
	return me.copy()
}

//Returns the current clauses on the dataset.
func (me *Dataset) GetClauses() clauses {
	return me.clauses
}

//used interally to copy the dataset
func (me Dataset) copy() *Dataset {
	return &me
}

//Returns true if the dataset has a FROM clause
func (me *Dataset) hasSources() bool {
	return me.clauses.From != nil && len(me.clauses.From.Columns()) > 0
}

//This method is used to serialize:
//   * Primitive Values (e.g. float64, int64, string, bool, time.Time, or nil)
//   * Expressions
//
//buf: The SqlBuilder to write the generated SQL to
//
//val: The value to serialize
//
//Errors:
//   * If there is an error generating the SQL
func (me *Dataset) Literal(buf *SqlBuilder, val interface{}) error {
	if val == nil {
		return me.adapter.LiteralNil(buf)
	}
	if v, ok := val.(Expression); ok {
		return me.expressionSql(buf, v)
	} else if v, ok := val.(int); ok {
		return me.adapter.LiteralInt(buf, int64(v))
	} else if v, ok := val.(int32); ok {
		return me.adapter.LiteralInt(buf, int64(v))
	} else if v, ok := val.(int64); ok {
		return me.adapter.LiteralInt(buf, v)
	} else if v, ok := val.(float32); ok {
		return me.adapter.LiteralFloat(buf, float64(v))
	} else if v, ok := val.(float64); ok {
		return me.adapter.LiteralFloat(buf, v)
	} else if v, ok := val.(string); ok {
		return me.adapter.LiteralString(buf, v)
	} else if v, ok := val.([]byte); ok {
		return me.adapter.LiteralBytes(buf, v)
	} else if v, ok := val.(bool); ok {
		return me.adapter.LiteralBool(buf, v)
	} else if v, ok := val.(time.Time); ok {
		return me.adapter.LiteralTime(buf, v)
	} else if v, ok := val.(*time.Time); ok {
		if v == nil {
			return me.adapter.LiteralNil(buf)
		}
		return me.adapter.LiteralTime(buf, *v)
	} else if v, ok := val.(driver.Valuer); ok {
		dVal, err := v.Value()
		if err != nil {
			return NewGoquError(err.Error())
		}
		return me.Literal(buf, dVal)
	}
	return me.reflectSql(buf, val)
}

func (me *Dataset) isUint(k reflect.Kind) bool {
	return (k == reflect.Uint) ||
		(k == reflect.Uint8) ||
		(k == reflect.Uint16) ||
		(k == reflect.Uint32) ||
		(k == reflect.Uint64)
}

func (me *Dataset) isInt(k reflect.Kind) bool {
	return (k == reflect.Int) ||
		(k == reflect.Int8) ||
		(k == reflect.Int16) ||
		(k == reflect.Int32) ||
		(k == reflect.Int64)
}

func (me *Dataset) isFloat(k reflect.Kind) bool {
	return (k == reflect.Float32) ||
		(k == reflect.Float64)
}

func (me *Dataset) reflectSql(buf *SqlBuilder, val interface{}) error {
	v := reflect.Indirect(reflect.ValueOf(val))
	valKind := v.Kind()
	if valKind == reflect.Invalid {
		return me.adapter.LiteralNil(buf)
	} else if valKind == reflect.Slice {
		if b, ok := val.([]byte); ok {
			return me.Literal(buf, b)
		}
		return me.adapter.SliceValueSql(buf, v)
	} else if me.isInt(valKind) {
		return me.Literal(buf, v.Int())
	} else if me.isUint(valKind) {
		return me.Literal(buf, int64(v.Uint()))
	} else if me.isFloat(valKind) {
		return me.Literal(buf, v.Float())
	} else if valKind == reflect.String {
		return me.Literal(buf, v.String())
	} else if valKind == reflect.Bool {
		return me.Literal(buf, v.Bool())
	}
	return newEncodeError(fmt.Sprintf("Unable to encode value %+v", val))
}

func (me *Dataset) expressionSql(buf *SqlBuilder, expression Expression) error {
	if e, ok := expression.(ColumnList); ok {
		return me.adapter.ColumnListSql(buf, e)
	} else if e, ok := expression.(ExpressionList); ok {
		return me.adapter.ExpressionListSql(buf, e)
	} else if e, ok := expression.(LiteralExpression); ok {
		return me.adapter.LiteralExpressionSql(buf, e)
	} else if e, ok := expression.(IdentifierExpression); ok {
		return me.adapter.QuoteIdentifier(buf, e)
	} else if e, ok := expression.(AliasedExpression); ok {
		return me.adapter.AliasedExpressionSql(buf, e)
	} else if e, ok := expression.(BooleanExpression); ok {
		return me.adapter.BooleanExpressionSql(buf, e)
	} else if e, ok := expression.(RangeExpression); ok {
		return me.adapter.RangeExpressionSql(buf, e)
	} else if e, ok := expression.(OrderedExpression); ok {
		return me.adapter.OrderedExpressionSql(buf, e)
	} else if e, ok := expression.(UpdateExpression); ok {
		return me.adapter.UpdateExpressionSql(buf, e)
	} else if e, ok := expression.(SqlFunctionExpression); ok {
		return me.adapter.SqlFunctionExpressionSql(buf, e)
	} else if e, ok := expression.(CastExpression); ok {
		return me.adapter.CastExpressionSql(buf, e)
	} else if e, ok := expression.(*Dataset); ok {
		return me.adapter.DatasetSql(buf, *e)
	} else if e, ok := expression.(CommonTableExpression); ok {
		return me.adapter.CommonTableExpressionSql(buf, e)
	} else if e, ok := expression.(CompoundExpression); ok {
		return me.adapter.CompoundExpressionSql(buf, e)
	} else if e, ok := expression.(Ex); ok {
		return me.adapter.ExpressionMapSql(buf, e)
	} else if e, ok := expression.(ExOr); ok {
		return me.adapter.ExpressionOrMapSql(buf, e)
	}
	return NewGoquError("Unsupported expression type %T", expression)
}
