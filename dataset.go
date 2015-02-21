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
		Joins          joiningClauses
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

func (me joiningClause) Clone() joiningClause {
	return joiningClause{joinType: me.joinType, isConditioned: me.isConditioned, table: me.table.Clone(), condition: me.condition.Clone().(JoinExpression)}
}

func (me joiningClauses) Clone() joiningClauses {
	ret := make(joiningClauses, len(me))
	for i, jc := range me {
		ret[i] = jc.Clone()
	}
	return ret
}

func (me clauses) Clone() clauses {
	ret := clauses{
		Joins:  me.Joins.Clone(),
		Limit:  me.Limit,
		Offset: me.Offset,
	}
	if me.Select != nil {
		ret.Select = me.Select.Clone().(ColumnList)
	}
	if me.SelectDistinct != nil {
		ret.SelectDistinct = me.SelectDistinct.Clone().(ColumnList)
	}
	if me.From != nil {
		ret.From = me.From.Clone().(ColumnList)
	}
	if me.Returning != nil {
		ret.Returning = me.Returning.Clone().(ColumnList)
	}
	if me.Alias != nil {
		ret.Alias = me.Alias.Clone().(IdentifierExpression)
	}
	if me.Where != nil {
		ret.Where = me.Where.Clone().(ExpressionList)
	}
	if me.GroupBy != nil {
		ret.GroupBy = me.GroupBy.Clone().(ColumnList)
	}
	if me.Having != nil {
		ret.Having = me.Having.Clone().(ExpressionList)
	}
	if me.Order != nil {
		ret.Order = me.Order.Clone().(ColumnList)
	}
	if me.Compounds != nil && len(me.Compounds) > 0 {
		ret.Compounds = make([]CompoundExpression, len(me.Compounds))
		for i, compound := range me.Compounds {
			ret.Compounds[i] = compound.Clone().(CompoundExpression)
		}
	}
	return ret
}

func From(table ...interface{}) Dataset {
	ret := new(Dataset)
	ret.adapter = newAdapter("", ret)
	ret.clauses = clauses{
		Select: cols(Star()),
		From:   cols(table...),
	}
	return *ret
}

func withDatabase(db database) Dataset {
	ret := new(Dataset)
	ret.database = db
	ret.clauses = clauses{
		Select: cols(Star()),
	}
	ret.adapter = db.QueryAdapter(ret)
	return *ret
}

func (me Dataset) Expression() Expression {
	return me
}

func (me Dataset) Clone() Expression {
	return me.copy()
}

func (me Dataset) getClauses() clauses {
	return me.clauses.Clone()
}

func (me Dataset) copy() Dataset {
	ret := Dataset{
		database: me.database,
		adapter:  me.adapter,
		clauses:  me.clauses.Clone(),
	}
	return ret
}

func (me Dataset) hasSources() bool {
	return me.clauses.From != nil && len(me.clauses.From.Columns()) > 0
}

func (me Dataset) Literal(val interface{}) (string, error) {
	switch val.(type) {
	case driver.Valuer:
		dVal, err := val.(driver.Valuer).Value()
		if err != nil {
			return "", newGqlError(err.Error())
		}
		return me.Literal(dVal)
	}

	v := reflect.Indirect(reflect.ValueOf(val))
	switch v.Kind() {
	case reflect.Invalid:
		return me.adapter.LiteralNil()
	case reflect.Slice:
		if b, ok := val.([]byte); ok {
			return me.adapter.LiteralString(string(b))
		}
		return me.adapter.SliceValueSql(v)
	case reflect.Struct:
		val = v.Interface()
		switch val.(type) {
		case time.Time:
			return me.adapter.LiteralTime(val.(time.Time))
		case Expression:
			return me.expressionSql(val.(Expression))
		default:
			return "", newGqlError(fmt.Sprintf("Unable to encode value %+v", val))
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return me.adapter.LiteralInt(v.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return me.adapter.LiteralInt(int64(v.Uint()))
	case reflect.Uint64:
		u64 := v.Uint()
		if u64 >= 1<<63 {
			return "", newGqlError("uint64 values with high bit set are not supported")
		}
		return me.adapter.LiteralInt(int64(v.Uint()))
	case reflect.Float32, reflect.Float64:
		return me.adapter.LiteralFloat(v.Float())
	case reflect.String:
		return me.adapter.LiteralString(v.String())
	case reflect.Bool:
		return me.adapter.LiteralBool(v.Bool())
	}
	return "", newEncodeError(fmt.Sprintf("Unable to encode value %+v", val))
}

func (me Dataset) expressionSql(expression Expression) (string, error) {
	switch expression.(type) {
	case Dataset:
		return me.adapter.DatasetSql(expression.(Dataset))
	case ColumnList:
		return me.adapter.ColumnListSql(expression.(ColumnList))
	case ExpressionList:
		return me.adapter.ExpressionListSql(expression.(ExpressionList))
	case LiteralExpression:
		return me.adapter.LiteralExpressionSql(expression.(LiteralExpression))
	case IdentifierExpression:
		return me.adapter.QuoteIdentifier(expression.(IdentifierExpression))
	case AliasedExpression:
		return me.adapter.AliasedExpressionSql(expression.(AliasedExpression))
	case BooleanExpression:
		return me.adapter.BooleanExpressionSql(expression.(BooleanExpression))
	case OrderedExpression:
		return me.adapter.OrderedExpressionSql(expression.(OrderedExpression))
	case UpdateExpression:
		return me.adapter.UpdateExpressionSql(expression.(UpdateExpression))
	case SqlFunctionExpression:
		return me.adapter.SqlFunctionExpressionSql(expression.(SqlFunctionExpression))
	case CastExpression:
		return me.adapter.CastExpressionSql(expression.(CastExpression))
	case CompoundExpression:
		return me.adapter.CompoundExpressionSql(expression.(CompoundExpression))
	}
	return "", newGqlError("Unsupported expression type %T", expression)
}

