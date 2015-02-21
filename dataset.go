package gql

import (
	"database/sql/driver"
	"fmt"
	"github.com/c2fo/c2fo-go/lib/tags"
	"reflect"
	"sort"
	"strings"
	"time"
)

type (
	countResult struct {
		Count int64 `db:"count"`
	}
	valueSlice      []reflect.Value
	TruncateOptions struct {
		Cascade  bool
		Restrict bool
		Identity string
	}
	Logger interface {
		Printf(format string, v ...interface{})
	}
	joiningClause struct {
		joinType      JoinType
		isConditioned bool
		table         Expression
		condition     JoinExpression
	}
	joiningClauses []joiningClause
	clauses        struct {
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

var (
	struct_map_cache       = make(map[interface{}]ColumnMap)
	conditioned_join_types = map[JoinType]bool{
		inner_join:       true,
		full_outer_join:  true,
		right_outer_join: true,
		left_outer_join:  true,
		full_join:        true,
		right_join:       true,
		left_join:        true,
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

func (me Dataset) As(alias string) Dataset {
	ret := me.copy()
	ret.clauses.Alias = I(alias)
	return ret
}

func (me Dataset) From(from ...interface{}) Dataset {
	ret := me.copy()
	var sources []interface{}
	numSources := 0
	for _, source := range from {
		if d, ok := source.(Dataset); ok && d.clauses.Alias == nil {
			numSources++
			sources = append(sources, d.As(fmt.Sprintf("t%d", numSources)))
		} else {
			sources = append(sources, source)
		}

	}
	ret.clauses.From = cols(sources...)
	return ret
}

func (me Dataset) FromSelf() Dataset {
	builder := Dataset{}
	builder.database = me.database
	builder.adapter = me.adapter
	builder.clauses = clauses{
		Select: cols(Star()),
	}
	return builder.From(me)

}

func (me Dataset) Returning(returning ...interface{}) Dataset {
	ret := me.copy()
	ret.clauses.Returning = cols(returning...)
	return ret
}

func (me Dataset) Join(table Expression, condition JoinExpression) Dataset {
	return me.InnerJoin(table, condition)
}

func (me Dataset) InnerJoin(table Expression, condition JoinExpression) Dataset {
	return me.JoinTable(inner_join, table, condition)
}
func (me Dataset) FullOuterJoin(table Expression, condition JoinExpression) Dataset {
	return me.JoinTable(full_outer_join, table, condition)
}
func (me Dataset) RightOuterJoin(table Expression, condition JoinExpression) Dataset {
	return me.JoinTable(right_outer_join, table, condition)
}
func (me Dataset) LeftOuterJoin(table Expression, condition JoinExpression) Dataset {
	return me.JoinTable(left_outer_join, table, condition)
}
func (me Dataset) FullJoin(table Expression, condition JoinExpression) Dataset {
	return me.JoinTable(full_join, table, condition)
}
func (me Dataset) RightJoin(table Expression, condition JoinExpression) Dataset {
	return me.JoinTable(right_join, table, condition)
}
func (me Dataset) LeftJoin(table Expression, condition JoinExpression) Dataset {
	return me.JoinTable(left_join, table, condition)
}
func (me Dataset) NaturalJoin(table Expression) Dataset {
	return me.JoinTable(natural_join, table, nil)
}
func (me Dataset) NaturalLeftJoin(table Expression) Dataset {
	return me.JoinTable(natural_left_join, table, nil)
}
func (me Dataset) NaturalRightJoin(table Expression) Dataset {
	return me.JoinTable(natural_right_join, table, nil)
}
func (me Dataset) NaturalFullJoin(table Expression) Dataset {
	return me.JoinTable(natural_full_join, table, nil)
}

func (me Dataset) CrossJoin(table Expression) Dataset {
	return me.JoinTable(cross_join, table, nil)
}

func (me Dataset) JoinTable(joinType JoinType, table Expression, condition JoinExpression) Dataset {
	ret := me.copy()
	isConditioned := conditioned_join_types[joinType]
	ret.clauses.Joins = append(ret.clauses.Joins, joiningClause{joinType: joinType, isConditioned: isConditioned, table: table, condition: condition})
	return ret
}

func (me Dataset) Select(selects ...interface{}) Dataset {
	ret := me.copy()
	ret.clauses.SelectDistinct = nil
	ret.clauses.Select = cols(selects...)
	return ret
}

func (me Dataset) SelectDistinct(selects ...interface{}) Dataset {
	ret := me.copy()
	ret.clauses.Select = nil
	ret.clauses.SelectDistinct = cols(selects...)
	return ret
}

func (me Dataset) ClearSelect() Dataset {
	ret := me.copy()
	ret.clauses.Select = cols(Literal("*"))
	ret.clauses.SelectDistinct = nil
	return ret
}

func (me Dataset) SelectAppend(selects ...interface{}) Dataset {
	ret := me.copy()
	if ret.clauses.SelectDistinct != nil {
		ret.clauses.SelectDistinct = ret.clauses.SelectDistinct.Append(cols(selects...).Columns()...)
	} else {
		ret.clauses.Select = ret.clauses.Select.Append(cols(selects...).Columns()...)
	}
	return ret
}

func (me Dataset) Where(expressions ...Expression) Dataset {
	expLen := len(expressions)
	if expLen > 0 {
		ret := me.copy()
		if ret.clauses.Where == nil {
			ret.clauses.Where = And(expressions...)
		} else {
			ret.clauses.Where = ret.clauses.Where.Append(expressions...)
		}
		return ret
	}
	return me
}

func (me Dataset) ClearWhere() Dataset {
	ret := me.copy()
	ret.clauses.Where = nil
	return ret
}

func (me Dataset) GroupBy(groupBy ...interface{}) Dataset {
	ret := me.copy()
	ret.clauses.GroupBy = cols(groupBy...)
	return ret
}

func (me Dataset) Having(expressions ...Expression) Dataset {
	expLen := len(expressions)
	if expLen > 0 {
		ret := me.copy()
		if ret.clauses.Having == nil {
			ret.clauses.Having = And(expressions...)
		} else {
			ret.clauses.Having = ret.clauses.Having.Append(expressions...)
		}
		return ret
	}
	return me
}

func (me Dataset) Order(order ...OrderedExpression) Dataset {
	ret := me.copy()
	ret.clauses.Order = orderList(order...)
	return ret
}
func (me Dataset) OrderAppend(order ...OrderedExpression) Dataset {
	if me.clauses.Order == nil {
		return me.Order(order...)
	} else {
		ret := me.copy()
		ret.clauses.Order = ret.clauses.Order.Append(orderList(order...).Columns()...)
		return ret
	}
	return me

}
func (me Dataset) ClearOrder() Dataset {
	ret := me.copy()
	ret.clauses.Order = nil
	return ret
}

func (me Dataset) Limit(limit uint) Dataset {
	ret := me.copy()
	if limit > 0 {
		ret.clauses.Limit = limit
	} else {
		ret.clauses.Limit = nil
	}
	return ret
}

func (me Dataset) LimitAll() Dataset {
	ret := me.copy()
	ret.clauses.Limit = Literal("ALL")
	return ret
}

func (me Dataset) ClearLimit() Dataset {
	return me.Limit(0)
}

func (me Dataset) Offset(offset uint) Dataset {
	ret := me.copy()
	ret.clauses.Offset = offset
	return ret
}

func (me Dataset) ClearOffset() Dataset {
	return me.Offset(0)
}

func (me Dataset) Union(other Dataset) Dataset {
	ret := me.CompoundFromSelf()
	ret.clauses.Compounds = append(ret.clauses.Compounds, Union(other.CompoundFromSelf()))
	return ret
}
func (me Dataset) UnionAll(other Dataset) Dataset {
	ret := me.CompoundFromSelf()
	ret.clauses.Compounds = append(ret.clauses.Compounds, UnionAll(other.CompoundFromSelf()))
	return ret
}
func (me Dataset) Intersect(other Dataset) Dataset {
	ret := me.CompoundFromSelf()
	ret.clauses.Compounds = append(ret.clauses.Compounds, Intersect(other.CompoundFromSelf()))
	return ret
}

func (me Dataset) IntersectAll(other Dataset) Dataset {
	ret := me.CompoundFromSelf()
	ret.clauses.Compounds = append(ret.clauses.Compounds, IntersectAll(other.CompoundFromSelf()))
	return ret
}

func (me Dataset) CompoundFromSelf() Dataset {
	if me.clauses.Order != nil || me.clauses.Limit != nil {
		return me.FromSelf()
	}
	return me
}

func (me Dataset) Sql() (string, error) {
	var (
		err       error
		sql       string
		selectSql []string
	)
	if me.clauses.SelectDistinct != nil {
		if sql, err = me.adapter.SelectDistinctSql(me.clauses.SelectDistinct); err != nil {
			return "", err
		}
	} else {
		if sql, err = me.adapter.SelectSql(me.clauses.Select); err != nil {
			return "", err
		}
	}
	selectSql = append(selectSql, sql)
	if sql, err = me.adapter.FromSql(me.clauses.From); err != nil {
		return "", err
	}
	selectSql = append(selectSql, sql)
	if sql, err = me.adapter.JoinSql(me.clauses.Joins); err != nil {
		return "", err
	}
	selectSql = append(selectSql, sql)
	if sql, err = me.adapter.WhereSql(me.clauses.Where); err != nil {
		return "", err
	}
	selectSql = append(selectSql, sql)
	if sql, err = me.adapter.GroupBySql(me.clauses.GroupBy); err != nil {
		return "", err
	}
	selectSql = append(selectSql, sql)
	if sql, err = me.adapter.HavingSql(me.clauses.Having); err != nil {
		return "", newGqlError(err.Error())
	}
	selectSql = append(selectSql, sql)
	if sql, err = me.adapter.CompoundsSql(me.clauses.Compounds); err != nil {
		return "", newGqlError(err.Error())
	}
	selectSql = append(selectSql, sql)
	if sql, err = me.adapter.OrderSql(me.clauses.Order); err != nil {
		return "", newGqlError(err.Error())
	}
	selectSql = append(selectSql, sql)
	if sql, err = me.adapter.LimitSql(me.clauses.Limit); err != nil {
		return "", newGqlError(err.Error())
	}
	selectSql = append(selectSql, sql)
	if sql, err = me.adapter.OffsetSql(me.clauses.Offset); err != nil {
		return "", newGqlError(err.Error())
	}
	selectSql = append(selectSql, sql)
	return strings.Join(selectSql, ""), nil
}

func (me Dataset) UpdateSql(update interface{}) (string, error) {
	if !me.hasSources() {
		return "", newGqlError("No source found when generating update sql")
	}
	updateValue := reflect.Indirect(reflect.ValueOf(update))
	var updates []UpdateExpression
	switch updateValue.Kind() {
	case reflect.Map:
		keys := valueSlice(updateValue.MapKeys())
		sort.Sort(keys)
		for _, key := range keys {
			updates = append(updates, I(key.String()).Set(updateValue.MapIndex(key).Interface()))
		}
	case reflect.Struct:
		for j := 0; j < updateValue.NumField(); j++ {
			f := updateValue.Field(j)
			t := updateValue.Type().Field(j)
			if me.canUpdateField(t) {
				updates = append(updates, I(t.Tag.Get("db")).Set(f.Interface()))
			}
		}
	default:
		return "", newGqlError("Unsupported update interface type %+v", updateValue.Type())
	}

	return me.updateSql(updates...)
}

func (me Dataset) InsertSql(rows ...interface{}) (string, error) {
	if !me.hasSources() {
		return "", newGqlError("No source found when generating insert sql")
	}
	switch len(rows) {
	case 0:
		return me.insertSql(nil, nil)
	case 1:
		switch rows[0].(type) {
		case Dataset:
			return me.insertFromSql(rows[0].(Dataset))
		}

	}
	columns, vals, err := me.getInsertColsAndVals(rows...)
	if err != nil {
		return "", err
	}
	return me.insertSql(columns, vals)
}

func (me Dataset) DeleteSql() (string, error) {
	var (
		err       error
		sql       string
		deleteSql []string
	)
	if !me.hasSources() {
		return "", newGqlError("No source found when generating delete sql")
	}
	if sql, err = me.adapter.DeleteBeginSql(); err != nil {
		return "", err
	}
	deleteSql = append(deleteSql, sql)
	if sql, err = me.adapter.FromSql(me.clauses.From); err != nil {
		return "", err
	}
	deleteSql = append(deleteSql, sql)

	if sql, err = me.adapter.WhereSql(me.clauses.Where); err != nil {
		return "", err
	}
	deleteSql = append(deleteSql, sql)

	if sql, err = me.adapter.ReturningSql(me.clauses.Returning); err != nil {
		return "", newGqlError(err.Error())
	}
	deleteSql = append(deleteSql, sql)
	return strings.Join(deleteSql, ""), nil
}

func (me Dataset) TruncateSql() (string, error) {
	return me.TruncateWithOptsSql(TruncateOptions{})
}

func (me Dataset) TruncateWithOptsSql(opts TruncateOptions) (string, error) {
	if !me.hasSources() {
		return "", newGqlError("No source found when generating truncate sql")
	}
	return me.adapter.TruncateSql(me.clauses.From, opts)
}

func (me Dataset) hasSources() bool {
	return me.clauses.From != nil && len(me.clauses.From.Columns()) > 0
}

func (me Dataset) canInsertField(field reflect.StructField) bool {
	gqlTag, dbTag := tags.TagOptions(field.Tag.Get("gql")), field.Tag.Get("db")
	return !gqlTag.Contains("skipinsert") && dbTag != "" && dbTag != "-"
}

func (me Dataset) canUpdateField(field reflect.StructField) bool {
	gqlTag, dbTag := tags.TagOptions(field.Tag.Get("gql")), field.Tag.Get("db")
	return !gqlTag.Contains("skipupdate") && dbTag != "" && dbTag != "-"
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

func (me Dataset) getInsertColsAndVals(rows ...interface{}) (columns ColumnList, vals [][]interface{}, err error) {
	var mapKeys valueSlice
	rowValue := reflect.Indirect(reflect.ValueOf(rows[0]))
	rowType := rowValue.Type()
	rowKind := rowValue.Kind()
	vals = make([][]interface{}, len(rows))
	for i, row := range rows {
		if rowType != reflect.Indirect(reflect.ValueOf(row)).Type() {
			return nil, nil, newGqlError("Rows must be all the same type expected %+v got %+v", rowType, reflect.Indirect(reflect.ValueOf(row)).Type())
		}
		newRowValue := reflect.Indirect(reflect.ValueOf(row))
		switch rowKind {
		case reflect.Map:
			if columns == nil {
				mapKeys = valueSlice(newRowValue.MapKeys())
				sort.Sort(mapKeys)
				colKeys := make([]interface{}, len(mapKeys))
				for j, key := range mapKeys {
					colKeys[j] = key.Interface()
				}
				columns = cols(colKeys...)
			}
			newMapKeys := valueSlice(newRowValue.MapKeys())
			if len(newMapKeys) != len(mapKeys) {
				return nil, nil, newGqlError("Rows with different value length expected %d got %d", len(mapKeys), len(newMapKeys))
			}
			if !mapKeys.Equal(newMapKeys) {
				return nil, nil, newGqlError("Rows with different keys expected %s got %s", mapKeys.String(), newMapKeys.String())
			}
			rowVals := make([]interface{}, len(mapKeys))
			for j, key := range mapKeys {
				rowVals[j] = newRowValue.MapIndex(key).Interface()
			}
			vals[i] = rowVals
		case reflect.Struct:
			var (
				rowCols []interface{}
				rowVals []interface{}
			)
			for j := 0; j < newRowValue.NumField(); j++ {
				f := newRowValue.Field(j)
				t := newRowValue.Type().Field(j)
				if me.canInsertField(t) {
					if columns == nil {
						rowCols = append(rowCols, t.Tag.Get("db"))
					}
					rowVals = append(rowVals, f.Interface())
				}
			}
			if columns == nil {
				columns = cols(rowCols...)
			}
			vals[i] = rowVals
		default:
			return nil, nil, newGqlError("Unsupported insert must be map or struct type %+v", row)
		}
	}
	return columns, vals, nil
}

func (me Dataset) expressionSql(expression Expression) (string, error) {
	switch expression.(type) {
	case Dataset:
		return me.adapter.BuilderSql(expression.(Dataset))
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

func (me Dataset) insertSql(cols ColumnList, values [][]interface{}) (string, error) {
	var (
		err        error
		sql        string
		insertStmt []string
	)

	if sql, err = me.adapter.InsertBeginSql(); err != nil {
		return "", err
	}
	insertStmt = append(insertStmt, sql)
	if sql, err = me.adapter.SourcesSql(me.clauses.From); err != nil {
		return "", newGqlError(err.Error())
	}
	insertStmt = append(insertStmt, sql)
	if cols == nil {
		if sql, err = me.adapter.DefaultValuesSql(); err != nil {
			return "", newGqlError(err.Error())
		}
		insertStmt = append(insertStmt, sql)
	} else {
		if sql, err = me.adapter.InsertColumnsSql(cols); err != nil {
			return "", newGqlError(err.Error())
		}
		insertStmt = append(insertStmt, sql)
		if sql, err = me.adapter.InsertValuesSql(values); err != nil {
			return "", newGqlError(err.Error())
		}
		insertStmt = append(insertStmt, sql)
	}
	if sql, err = me.adapter.ReturningSql(me.clauses.Returning); err != nil {
		return "", err
	}
	insertStmt = append(insertStmt, sql)
	return strings.Join(insertStmt, ""), nil
}

func (me Dataset) insertFromSql(other Dataset) (string, error) {
	var (
		err        error
		sql        string
		insertStmt []string
	)

	if sql, err = me.adapter.InsertBeginSql(); err != nil {
		return "", err
	}
	insertStmt = append(insertStmt, sql)

	if sql, err = me.adapter.SourcesSql(me.clauses.From); err != nil {
		return "", newGqlError(err.Error())
	}
	insertStmt = append(insertStmt, sql)

	if sql, err = other.Sql(); err != nil {
		return "", err
	}
	insertStmt = append(insertStmt, " "+sql)
	if sql, err = me.adapter.ReturningSql(me.clauses.Returning); err != nil {
		return "", err
	}
	insertStmt = append(insertStmt, sql)
	return strings.Join(insertStmt, ""), nil
}

func (me Dataset) updateSql(updates ...UpdateExpression) (string, error) {
	var (
		err        error
		sql        string
		updateStmt []string
	)
	if sql, err = me.adapter.UpdateBeginSql(); err != nil {
		return "", err
	}
	updateStmt = append(updateStmt, sql)
	if sql, err = me.adapter.SourcesSql(me.clauses.From); err != nil {
		return "", err
	}
	updateStmt = append(updateStmt, sql)
	if sql, err = me.adapter.UpdateExpressionsSql(updates...); err != nil {
		return "", err
	}
	updateStmt = append(updateStmt, sql)
	sql, err = me.adapter.WhereSql(me.clauses.Where)
	if err != nil {
		return "", err
	}
	updateStmt = append(updateStmt, sql)
	sql, err = me.adapter.ReturningSql(me.clauses.Returning)
	if err != nil {
		return "", err
	}
	updateStmt = append(updateStmt, sql)
	return strings.Join(updateStmt, ""), nil
}

func (me Dataset) Query(i interface{}) (bool, error) {
	var (
		sql string
		err error
	)
	switch reflect.Indirect(reflect.ValueOf(i)).Kind() {
	case reflect.Struct:
		sql, err = me.Limit(1).Sql()
	case reflect.Slice:
		sql, err = me.Sql()
	default:
		return false, newGqlError("Type must be a pointer to a slice or struct when calling Query")
	}
	if err != nil {
		return false, newGqlQueryError(err.Error())
	}
	return me.database.Select(i, sql)
}

func (me Dataset) Count() (int64, error) {
	var count countResult
	if _, err := me.Select(COUNT(Star()).As("count")).Query(&count); err != nil {
		return 0, err
	}
	return count.Count, nil
}

func (me Dataset) Pluck(i interface{}, col string) error {
	var (
		results selectResults
		sql     string
		err     error
	)
	val := reflect.ValueOf(i)
	if val.Kind() != reflect.Ptr {
		return newGqlError("Type must be a pointer to a slice when calling Pluck")
	}
	//create a temp column map
	val = reflect.Indirect(val)
	if val.Kind() != reflect.Slice {
		return newGqlError("Type must be a pointer to a slice when calling Pluck")
	}
	t, _, isSliceOfPointers := getTypeInfo(i, val)
	cm := ColumnMap{col: ColumnData{ColumnName: col, FieldName: col, GoType: t}}
	if sql, err = me.Select(col).Sql(); err != nil {
		return err
	}
	if results, err = me.database.SelectIntoMap(cm, sql); err != nil {
		return err
	}
	if len(results) > 0 {
		for _, result := range results {
			row := reflect.ValueOf(result[col])
			if isSliceOfPointers {
				val.Set(reflect.Append(val, row.Addr()))
			} else {
				val.Set(reflect.Append(val, reflect.Indirect(row)))
			}
		}
	}
	return nil
}

func (me Dataset) Update(i interface{}) (int64, error) {
	sql, err := me.UpdateSql(i)
	if err != nil {
		return 0, err
	}
	return me.database.Update(sql)
}

func (me Dataset) Delete() (int64, error) {
	sql, err := me.DeleteSql()
	if err != nil {
		return 0, err
	}
	return me.database.Delete(sql)
}
