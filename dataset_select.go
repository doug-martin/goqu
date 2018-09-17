package goqu

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

//Adds columns to the SELECT clause. See examples
//You can pass in the following.
//   string: Will automatically be turned into an identifier
//   Dataset: Will use the SQL generated from that Dataset. If the dataset is aliased it will use that alias as the column name.
//   LiteralExpression: (See Literal) Will use the literal SQL
//   SqlFunction: (See Func, MIN, MAX, COUNT....)
//   Struct: If passing in an instance of a struct, we will parse the struct for the column names to select. See examples
func (me *Dataset) Select(selects ...interface{}) *Dataset {
	ret := me.copy()
	ret.clauses.SelectDistinct = nil
	ret.clauses.Select = cols(selects...)
	return ret
}

//Adds columns to the SELECT DISTINCT clause. See examples
//You can pass in the following.
//   string: Will automatically be turned into an identifier
//   Dataset: Will use the SQL generated from that Dataset. If the dataset is aliased it will use that alias as the column name.
//   LiteralExpression: (See Literal) Will use the literal SQL
//   SqlFunction: (See Func, MIN, MAX, COUNT....)
//   Struct: If passing in an instance of a struct, we will parse the struct for the column names to select. See examples
func (me *Dataset) SelectDistinct(selects ...interface{}) *Dataset {
	ret := me.copy()
	ret.clauses.Select = nil
	ret.clauses.SelectDistinct = cols(selects...)
	return ret
}

//Resets to SELECT *. If the SelectDistinct was used the returned Dataset will have the the dataset set to SELECT *. See examples.
func (me *Dataset) ClearSelect() *Dataset {
	ret := me.copy()
	ret.clauses.Select = cols(Literal("*"))
	ret.clauses.SelectDistinct = nil
	return ret
}

//Returns true if using default SELECT *
func (me *Dataset) isDefaultSelect() bool {
	ret := false
	if me.clauses.Select != nil {
		selects := me.clauses.Select.Columns()
		if len(selects) == 1 {
			if l, ok := selects[0].(LiteralExpression); ok && l.Literal() == "*" {
				ret = true
			}
		}
	}
	return ret
}

//Adds columns to the SELECT clause. See examples
//You can pass in the following.
//   string: Will automatically be turned into an identifier
//   Dataset: Will use the SQL generated from that Dataset. If the dataset is aliased it will use that alias as the column name.
//   LiteralExpression: (See Literal) Will use the literal SQL
//   SqlFunction: (See Func, MIN, MAX, COUNT....)
func (me *Dataset) SelectAppend(selects ...interface{}) *Dataset {
	ret := me.copy()
	if ret.clauses.SelectDistinct != nil {
		ret.clauses.SelectDistinct = ret.clauses.SelectDistinct.Append(cols(selects...).Columns()...)
	} else {
		ret.clauses.Select = ret.clauses.Select.Append(cols(selects...).Columns()...)
	}
	return ret
}

//Adds a FROM clause. This return a new dataset with the original sources replaced. See examples.
//You can pass in the following.
//   string: Will automatically be turned into an identifier
//   Dataset: Will be added as a sub select. If the Dataset is not aliased it will automatically be aliased
//   LiteralExpression: (See Literal) Will use the literal SQL
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

//Returns a new Dataset with the current one as an source. If the current Dataset is not aliased (See Dataset#As) then it will automatically be aliased. See examples.
func (me *Dataset) FromSelf() *Dataset {
	builder := Dataset{}
	builder.database = me.database
	builder.adapter = me.adapter
	builder.clauses = clauses{
		Select: cols(Star()),
	}
	return builder.From(me)

}

//Alias to InnerJoin. See examples.
func (me *Dataset) Join(table Expression, condition joinExpression) *Dataset {
	return me.InnerJoin(table, condition)
}

//Adds an INNER JOIN clause. See examples.
func (me *Dataset) InnerJoin(table Expression, condition joinExpression) *Dataset {
	return me.joinTable(INNER_JOIN, table, condition)
}

//Adds a FULL OUTER JOIN clause. See examples.
func (me *Dataset) FullOuterJoin(table Expression, condition joinExpression) *Dataset {
	return me.joinTable(FULL_OUTER_JOIN, table, condition)
}

//Adds a RIGHT OUTER JOIN clause. See examples.
func (me *Dataset) RightOuterJoin(table Expression, condition joinExpression) *Dataset {
	return me.joinTable(RIGHT_OUTER_JOIN, table, condition)
}

//Adds a LEFT OUTER JOIN clause. See examples.
func (me *Dataset) LeftOuterJoin(table Expression, condition joinExpression) *Dataset {
	return me.joinTable(LEFT_OUTER_JOIN, table, condition)
}

//Adds a FULL JOIN clause. See examples.
func (me *Dataset) FullJoin(table Expression, condition joinExpression) *Dataset {
	return me.joinTable(FULL_JOIN, table, condition)
}

//Adds a RIGHT JOIN clause. See examples.
func (me *Dataset) RightJoin(table Expression, condition joinExpression) *Dataset {
	return me.joinTable(RIGHT_JOIN, table, condition)
}

//Adds a LEFT JOIN clause. See examples.
func (me *Dataset) LeftJoin(table Expression, condition joinExpression) *Dataset {
	return me.joinTable(LEFT_JOIN, table, condition)
}

//Adds a NATURAL JOIN clause. See examples.
func (me *Dataset) NaturalJoin(table Expression) *Dataset {
	return me.joinTable(NATURAL_JOIN, table, nil)
}

//Adds a NATURAL LEFT JOIN clause. See examples.
func (me *Dataset) NaturalLeftJoin(table Expression) *Dataset {
	return me.joinTable(NATURAL_LEFT_JOIN, table, nil)
}

//Adds a NATURAL RIGHT JOIN clause. See examples.
func (me *Dataset) NaturalRightJoin(table Expression) *Dataset {
	return me.joinTable(NATURAL_RIGHT_JOIN, table, nil)
}

//Adds a NATURAL FULL JOIN clause. See examples.
func (me *Dataset) NaturalFullJoin(table Expression) *Dataset {
	return me.joinTable(NATURAL_FULL_JOIN, table, nil)
}

//Adds a CROSS JOIN clause. See examples.
func (me *Dataset) CrossJoin(table Expression) *Dataset {
	return me.joinTable(CROSS_JOIN, table, nil)
}

//Joins this Datasets table with another
func (me *Dataset) joinTable(joinType JoinType, table Expression, condition joinExpression) *Dataset {
	ret := me.copy()
	isConditioned := conditioned_join_types[joinType]
	ret.clauses.Joins = append(ret.clauses.Joins, JoiningClause{JoinType: joinType, IsConditioned: isConditioned, Table: table, Condition: condition})
	return ret
}

//Adds a WHERE clause. See examples.
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

//Removes the WHERE clause. See examples.
func (me *Dataset) ClearWhere() *Dataset {
	ret := me.copy()
	ret.clauses.Where = nil
	return ret
}

//Adds a FOR UPDATE clause. See examples.
func (me *Dataset) ForUpdate(waitOption WaitOption) *Dataset {
	ret := me.copy()
	ret.clauses.Lock = Lock{
		Strength:   FOR_UPDATE,
		WaitOption: waitOption,
	}
	return ret
}

//Adds a FOR NO KEY UPDATE clause. See examples.
func (me *Dataset) ForNoKeyUpdate(waitOption WaitOption) *Dataset {
	ret := me.copy()
	ret.clauses.Lock = Lock{
		Strength:   FOR_NO_KEY_UPDATE,
		WaitOption: waitOption,
	}
	return ret
}

//Adds a FOR KEY SHARE clause. See examples.
func (me *Dataset) ForKeyShare(waitOption WaitOption) *Dataset {
	ret := me.copy()
	ret.clauses.Lock = Lock{
		Strength:   FOR_KEY_SHARE,
		WaitOption: waitOption,
	}
	return ret
}

//Adds a FOR SHARE clause. See examples.
func (me *Dataset) ForShare(waitOption WaitOption) *Dataset {
	ret := me.copy()
	ret.clauses.Lock = Lock{
		Strength:   FOR_SHARE,
		WaitOption: waitOption,
	}
	return ret
}

//Adds a GROUP BY clause. See examples.
func (me *Dataset) GroupBy(groupBy ...interface{}) *Dataset {
	ret := me.copy()
	ret.clauses.GroupBy = cols(groupBy...)
	return ret
}

//Adds a HAVING clause. See examples.
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

//Adds a ORDER clause. If the ORDER is currently set it replaces it. See examples.
func (me *Dataset) Order(order ...OrderedExpression) *Dataset {
	ret := me.copy()
	ret.clauses.Order = orderList(order...)
	return ret
}

//Adds a more columns to the current ORDER BY clause. If no order has be previously specified it is the same as calling Order. See examples.
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

//Removes the ORDER BY clause. See examples.
func (me *Dataset) ClearOrder() *Dataset {
	ret := me.copy()
	ret.clauses.Order = nil
	return ret
}

//Adds a LIMIT clause. If the LIMIT is currently set it replaces it. See examples.
func (me *Dataset) Limit(limit uint) *Dataset {
	ret := me.copy()
	if limit > 0 {
		ret.clauses.Limit = limit
	} else {
		ret.clauses.Limit = nil
	}
	return ret
}

//Adds a LIMIT ALL clause. If the LIMIT is currently set it replaces it. See examples.
func (me *Dataset) LimitAll() *Dataset {
	ret := me.copy()
	ret.clauses.Limit = Literal("ALL")
	return ret
}

//Removes the LIMIT clause.
func (me *Dataset) ClearLimit() *Dataset {
	return me.Limit(0)
}

//Adds an OFFSET clause. If the OFFSET is currently set it replaces it. See examples.
func (me *Dataset) Offset(offset uint) *Dataset {
	ret := me.copy()
	ret.clauses.Offset = offset
	return ret
}

//Removes the OFFSET clause from the Dataset
func (me *Dataset) ClearOffset() *Dataset {
	return me.Offset(0)
}

//Creates an UNION statement with another dataset.
// If this or the other dataset has a limit or offset it will use that dataset as a subselect in the FROM clause. See examples.
func (me *Dataset) Union(other *Dataset) *Dataset {
	ret := me.compoundFromSelf()
	ret.clauses.Compounds = append(ret.clauses.Compounds, Union(other.compoundFromSelf()))
	return ret
}

//Creates an UNION ALL statement with another dataset.
// If this or the other dataset has a limit or offset it will use that dataset as a subselect in the FROM clause. See examples.
func (me *Dataset) UnionAll(other *Dataset) *Dataset {
	ret := me.compoundFromSelf()
	ret.clauses.Compounds = append(ret.clauses.Compounds, UnionAll(other.compoundFromSelf()))
	return ret
}

//Creates an INTERSECT statement with another dataset.
// If this or the other dataset has a limit or offset it will use that dataset as a subselect in the FROM clause. See examples.
func (me *Dataset) Intersect(other *Dataset) *Dataset {
	ret := me.compoundFromSelf()
	ret.clauses.Compounds = append(ret.clauses.Compounds, Intersect(other.compoundFromSelf()))
	return ret
}

//Creates an INTERSECT ALL statement with another dataset.
// If this or the other dataset has a limit or offset it will use that dataset as a subselect in the FROM clause. See examples.
func (me *Dataset) IntersectAll(other *Dataset) *Dataset {
	ret := me.compoundFromSelf()
	ret.clauses.Compounds = append(ret.clauses.Compounds, IntersectAll(other.compoundFromSelf()))
	return ret
}

//Used internally to determine if the dataset needs to use iteself as a source.
//If the dataset has an order or limit it will select from itself
func (me *Dataset) compoundFromSelf() *Dataset {
	if me.clauses.Order != nil || me.clauses.Limit != nil {
		return me.FromSelf()
	}
	return me.copy()
}

//Adds a RETURNING clause to the dataset if the adapter supports it. Typically used for INSERT, UPDATE or DELETE. See examples.
func (me *Dataset) Returning(returning ...interface{}) *Dataset {
	ret := me.copy()
	ret.clauses.Returning = cols(returning...)
	return ret
}

//Sets the alias for this dataset. This is typically used when using a Dataset as a subselect. See examples.
func (me *Dataset) As(alias string) *Dataset {
	ret := me.copy()
	ret.clauses.Alias = I(alias)
	return ret
}

//Generates a SELECT sql statement, if Prepared has been called with true then the parameters will not be interpolated. See examples.
//
//Errors:
//  * There is an error generating the SQL
func (me *Dataset) ToSql() (string, []interface{}, error) {
	buf := NewSqlBuilder(me.isPrepared)
	if err := me.selectSqlWriteTo(buf); err != nil {
		return "", nil, err
	}
	sql, args := buf.ToSql()
	return sql, args, nil
}

//Does actual sql generation of sql, accepts an sql builder so other methods can call when creating subselects and needing prepared sql.
func (me *Dataset) selectSqlWriteTo(buf *SqlBuilder) error {
	if err := me.adapter.CommonTablesSql(buf, me.clauses.CommonTables); err != nil {
		return err
	}
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
	if err := me.adapter.OffsetSql(buf, me.clauses.Offset); err != nil {
		return err
	}
	return me.adapter.ForSql(buf, me.clauses.Lock)
}
