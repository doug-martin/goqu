package gql

import (
	"fmt"
	"strings"
)

type (
	joiningClause struct {
		joinType      JoinType
		isConditioned bool
		table         Expression
		condition     JoinExpression
	}
	joiningClauses []joiningClause
)

var (
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

func (me Dataset) Join(table Expression, condition JoinExpression) Dataset {
	return me.InnerJoin(table, condition)
}

func (me Dataset) InnerJoin(table Expression, condition JoinExpression) Dataset {
	return me.joinTable(inner_join, table, condition)
}
func (me Dataset) FullOuterJoin(table Expression, condition JoinExpression) Dataset {
	return me.joinTable(full_outer_join, table, condition)
}
func (me Dataset) RightOuterJoin(table Expression, condition JoinExpression) Dataset {
	return me.joinTable(right_outer_join, table, condition)
}
func (me Dataset) LeftOuterJoin(table Expression, condition JoinExpression) Dataset {
	return me.joinTable(left_outer_join, table, condition)
}
func (me Dataset) FullJoin(table Expression, condition JoinExpression) Dataset {
	return me.joinTable(full_join, table, condition)
}
func (me Dataset) RightJoin(table Expression, condition JoinExpression) Dataset {
	return me.joinTable(right_join, table, condition)
}
func (me Dataset) LeftJoin(table Expression, condition JoinExpression) Dataset {
	return me.joinTable(left_join, table, condition)
}
func (me Dataset) NaturalJoin(table Expression) Dataset {
	return me.joinTable(natural_join, table, nil)
}
func (me Dataset) NaturalLeftJoin(table Expression) Dataset {
	return me.joinTable(natural_left_join, table, nil)
}
func (me Dataset) NaturalRightJoin(table Expression) Dataset {
	return me.joinTable(natural_right_join, table, nil)
}
func (me Dataset) NaturalFullJoin(table Expression) Dataset {
	return me.joinTable(natural_full_join, table, nil)
}

func (me Dataset) CrossJoin(table Expression) Dataset {
	return me.joinTable(cross_join, table, nil)
}

func (me Dataset) joinTable(joinType JoinType, table Expression, condition JoinExpression) Dataset {
	ret := me.copy()
	isConditioned := conditioned_join_types[joinType]
	ret.clauses.Joins = append(ret.clauses.Joins, joiningClause{joinType: joinType, isConditioned: isConditioned, table: table, condition: condition})
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

func (me Dataset) Returning(returning ...interface{}) Dataset {
	ret := me.copy()
	ret.clauses.Returning = cols(returning...)
	return ret
}

func (me Dataset) As(alias string) Dataset {
	ret := me.copy()
	ret.clauses.Alias = I(alias)
	return ret
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
