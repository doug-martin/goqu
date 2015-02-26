package gql

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
	}
	Dataset struct {
		adapter  Adapter
		clauses  clauses
		database database
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

func From(table ...interface{}) *Dataset {
	ret := new(Dataset)
	ret.adapter = NewDsAdapter("default", ret)
	ret.clauses = clauses{
		Select: cols(Star()),
		From:   cols(table...),
	}
	return ret
}

func withDatabase(db database) *Dataset {
	ret := new(Dataset)
	ret.database = db
	ret.clauses = clauses{
		Select: cols(Star()),
	}
	ret.adapter = db.QueryAdapter(ret)
	return ret
}

func (me *Dataset) SetAdapter(adapter Adapter) *Dataset {
	me.adapter = adapter
	return me
}

func (me *Dataset) Adapter() Adapter {
	return me.adapter
}

func (me *Dataset) Expression() Expression {
	return me
}

func (me *Dataset) Clone() Expression {
	return me.copy()
}

func (me *Dataset) GetClauses() clauses {
	return me.clauses
}

func (me Dataset) copy() *Dataset {
	return &me
}

func (me *Dataset) hasSources() bool {
	return me.clauses.From != nil && len(me.clauses.From.Columns()) > 0
}

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
	} else if v, ok := val.(bool); ok {
		return me.adapter.LiteralBool(buf, v)
	} else if v, ok := val.([]byte); ok {
		return me.adapter.LiteralString(buf, string(v))
	} else if v, ok := val.(time.Time); ok {
		return me.adapter.LiteralTime(buf, v)
	} else if v, ok := val.(*time.Time); ok {
		return me.adapter.LiteralTime(buf, *v)
	} else if v, ok := val.(driver.Valuer); ok {
		dVal, err := v.Value()
		if err != nil {
			return NewGqlError(err.Error())
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
	} else if valKind == reflect.Struct {
		return NewGqlError(fmt.Sprintf("Unable to encode value %+v", val))
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
	} else if e, ok := expression.(CompoundExpression); ok {
		return me.adapter.CompoundExpressionSql(buf, e)
	}
	return NewGqlError("Unsupported expression type %T", expression)
}
