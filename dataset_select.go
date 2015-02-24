package gql

import (
	"fmt"
)

var (
	conditioned_join_types = map[JoinType]bool{
		INNER_JOIN:       true,
		FULL_OUTER_JOIN:  true,
		RIGHT_OUTER_JOIN: true,
		LEFT_OUTER_JOIN:  true,
		FULL_JOIN:        true,
		RIGHT_JOIN:       true,
		LEFT_JOIN:        true,
	}
)

func (me *Dataset) Select(selects ...interface{}) *Dataset {
	ret := me.copy()
	ret.clauses.SelectDistinct = nil
	ret.clauses.Select = cols(selects...)
	return ret
}

func (me *Dataset) SelectDistinct(selects ...interface{}) *Dataset {
	ret := me.copy()
	ret.clauses.Select = nil
	ret.clauses.SelectDistinct = cols(selects...)
	return ret
}

func (me *Dataset) ClearSelect() *Dataset {
	ret := me.copy()
	ret.clauses.Select = cols(Literal("*"))
	ret.clauses.SelectDistinct = nil
	return ret
}

func (me *Dataset) SelectAppend(selects ...interface{}) *Dataset {
	ret := me.copy()
	if ret.clauses.SelectDistinct != nil {
		ret.clauses.SelectDistinct = ret.clauses.SelectDistinct.Append(cols(selects...).Columns()...)
	} else {
		ret.clauses.Select = ret.clauses.Select.Append(cols(selects...).Columns()...)
	}
	return ret
}

func (me *Dataset) From(from ...interface{}) *Dataset {
	ret := me.copy()
	var sources []interface{}
	numSources := 0
	for _, source := range from {
		if d, ok := source.(*Dataset); ok && d.clauses.Alias == nil {
			numSources++
			sources = append(sources, d.As(fmt.Sprintf("t%d", numSources)))
		} else {
			sources = append(sources, source)
		}
	}
	ret.clauses.From = cols(sources...)
	return ret
}

func (me *Dataset) FromSelf() *Dataset {
	builder := Dataset{}
	builder.database = me.database
	builder.adapter = me.adapter
	builder.clauses = clauses{
		Select: cols(Star()),
	}
	return builder.From(me)

}

func (me *Dataset) Join(table Expression, condition JoinExpression) *Dataset {
	return me.InnerJoin(table, condition)
}

func (me *Dataset) InnerJoin(table Expression, condition JoinExpression) *Dataset {
	return me.joinTable(INNER_JOIN, table, condition)
}
func (me *Dataset) FullOuterJoin(table Expression, condition JoinExpression) *Dataset {
	return me.joinTable(FULL_OUTER_JOIN, table, condition)
}
func (me *Dataset) RightOuterJoin(table Expression, condition JoinExpression) *Dataset {
	return me.joinTable(RIGHT_OUTER_JOIN, table, condition)
}
func (me *Dataset) LeftOuterJoin(table Expression, condition JoinExpression) *Dataset {
	return me.joinTable(LEFT_OUTER_JOIN, table, condition)
}
func (me *Dataset) FullJoin(table Expression, condition JoinExpression) *Dataset {
	return me.joinTable(FULL_JOIN, table, condition)
}
func (me *Dataset) RightJoin(table Expression, condition JoinExpression) *Dataset {
	return me.joinTable(RIGHT_JOIN, table, condition)
}
func (me *Dataset) LeftJoin(table Expression, condition JoinExpression) *Dataset {
	return me.joinTable(LEFT_JOIN, table, condition)
}
func (me *Dataset) NaturalJoin(table Expression) *Dataset {
	return me.joinTable(NATURAL_JOIN, table, nil)
}
func (me *Dataset) NaturalLeftJoin(table Expression) *Dataset {
	return me.joinTable(NATURAL_LEFT_JOIN, table, nil)
}
func (me *Dataset) NaturalRightJoin(table Expression) *Dataset {
	return me.joinTable(NATURAL_RIGHT_JOIN, table, nil)
}
func (me *Dataset) NaturalFullJoin(table Expression) *Dataset {
	return me.joinTable(NATURAL_FULL_JOIN, table, nil)
}

func (me *Dataset) CrossJoin(table Expression) *Dataset {
	return me.joinTable(CROSS_JOIN, table, nil)
}

func (me *Dataset) joinTable(joinType JoinType, table Expression, condition JoinExpression) *Dataset {
	ret := me.copy()
	isConditioned := conditioned_join_types[joinType]
	ret.clauses.Joins = append(ret.clauses.Joins, JoiningClause{JoinType: joinType, IsConditioned: isConditioned, Table: table, Condition: condition})
	return ret
}

func (me *Dataset) Where(expressions ...Expression) *Dataset {
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

func (me *Dataset) ClearWhere() *Dataset {
	ret := me.copy()
	ret.clauses.Where = nil
	return ret
}

func (me *Dataset) GroupBy(groupBy ...interface{}) *Dataset {
	ret := me.copy()
	ret.clauses.GroupBy = cols(groupBy...)
	return ret
}

func (me *Dataset) Having(expressions ...Expression) *Dataset {
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

func (me *Dataset) Order(order ...OrderedExpression) *Dataset {
	ret := me.copy()
	ret.clauses.Order = orderList(order...)
	return ret
}

func (me *Dataset) OrderAppend(order ...OrderedExpression) *Dataset {
	if me.clauses.Order == nil {
		return me.Order(order...)
	} else {
		ret := me.copy()
		ret.clauses.Order = ret.clauses.Order.Append(orderList(order...).Columns()...)
		return ret
	}
	return me

}
func (me *Dataset) ClearOrder() *Dataset {
	ret := me.copy()
	ret.clauses.Order = nil
	return ret
}

func (me *Dataset) Limit(limit uint) *Dataset {
	ret := me.copy()
	if limit > 0 {
		ret.clauses.Limit = limit
	} else {
		ret.clauses.Limit = nil
	}
	return ret
}

func (me *Dataset) LimitAll() *Dataset {
	ret := me.copy()
	ret.clauses.Limit = Literal("ALL")
	return ret
}

func (me *Dataset) ClearLimit() *Dataset {
	return me.Limit(0)
}

func (me *Dataset) Offset(offset uint) *Dataset {
	ret := me.copy()
	ret.clauses.Offset = offset
	return ret
}

func (me *Dataset) ClearOffset() *Dataset {
	return me.Offset(0)
}

func (me *Dataset) Union(other *Dataset) *Dataset {
	ret := me.CompoundFromSelf()
	ret.clauses.Compounds = append(ret.clauses.Compounds, Union(other.CompoundFromSelf()))
	return ret
}
func (me *Dataset) UnionAll(other *Dataset) *Dataset {
	ret := me.CompoundFromSelf()
	ret.clauses.Compounds = append(ret.clauses.Compounds, UnionAll(other.CompoundFromSelf()))
	return ret
}
func (me *Dataset) Intersect(other *Dataset) *Dataset {
	ret := me.CompoundFromSelf()
	ret.clauses.Compounds = append(ret.clauses.Compounds, Intersect(other.CompoundFromSelf()))
	return ret
}

func (me *Dataset) IntersectAll(other *Dataset) *Dataset {
	ret := me.CompoundFromSelf()
	ret.clauses.Compounds = append(ret.clauses.Compounds, IntersectAll(other.CompoundFromSelf()))
	return ret
}

func (me *Dataset) CompoundFromSelf() *Dataset {
	if me.clauses.Order != nil || me.clauses.Limit != nil {
		return me.FromSelf()
	}
	return me.copy()
}

func (me *Dataset) Returning(returning ...interface{}) *Dataset {
	ret := me.copy()
	ret.clauses.Returning = cols(returning...)
	return ret
}

func (me *Dataset) As(alias string) *Dataset {
	ret := me.copy()
	ret.clauses.Alias = I(alias)
	return ret
}

func (me *Dataset) Sql() (string, error) {
	sql, _, err := me.ToSql(false)
	return sql, err
}

func (me *Dataset) ToSql(isPrepared bool) (string, []interface{}, error) {
	buf := NewSqlBuilder(isPrepared)
	if err := me.selectSqlWriteTo(buf); err != nil {
		return "", nil, err
	}
	sql, args := buf.ToSql()
	return sql, args, nil
}

func (me *Dataset) selectSqlWriteTo(buf *SqlBuilder) error {
	if me.clauses.SelectDistinct != nil {
		if err := me.adapter.SelectDistinctSql(buf, me.clauses.SelectDistinct); err != nil {
			return err
		}
	} else {
		if err := me.adapter.SelectSql(buf, me.clauses.Select); err != nil {
			return err
		}
	}
	if err := me.adapter.FromSql(buf, me.clauses.From); err != nil {
		return err
	}
	if err := me.adapter.JoinSql(buf, me.clauses.Joins); err != nil {
		return err
	}
	if err := me.adapter.WhereSql(buf, me.clauses.Where); err != nil {
		return err
	}
	if err := me.adapter.GroupBySql(buf, me.clauses.GroupBy); err != nil {
		return err
	}
	if err := me.adapter.HavingSql(buf, me.clauses.Having); err != nil {
		return err
	}
	if err := me.adapter.CompoundsSql(buf, me.clauses.Compounds); err != nil {
		return err
	}
	if err := me.adapter.OrderSql(buf, me.clauses.Order); err != nil {
		return err
	}
	if err := me.adapter.LimitSql(buf, me.clauses.Limit); err != nil {
		return err
	}
	return me.adapter.OffsetSql(buf, me.clauses.Offset)

}
