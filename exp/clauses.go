package exp

type (
	Clauses interface {
		HasSources() bool
		IsDefaultSelect() bool
		clone() *clauses

		Select() ColumnListExpression
		SelectAppend(cl ColumnListExpression) Clauses
		SetSelect(cl ColumnListExpression) Clauses

		SelectDistinct() ColumnListExpression
		HasSelectDistinct() bool
		SetSelectDistinct(cl ColumnListExpression) Clauses

		From() ColumnListExpression
		SetFrom(cl ColumnListExpression) Clauses

		HasAlias() bool
		Alias() IdentifierExpression
		SetAlias(ie IdentifierExpression) Clauses

		Joins() JoinExpressions
		JoinsAppend(jc JoinExpression) Clauses

		Where() ExpressionList
		ClearWhere() Clauses
		WhereAppend(expressions ...Expression) Clauses

		Having() ExpressionList
		ClearHaving() Clauses
		HavingAppend(expressions ...Expression) Clauses

		Order() ColumnListExpression
		HasOrder() bool
		ClearOrder() Clauses
		SetOrder(oes ...OrderedExpression) Clauses
		OrderAppend(...OrderedExpression) Clauses

		GroupBy() ColumnListExpression
		SetGroupBy(cl ColumnListExpression) Clauses

		Limit() interface{}
		HasLimit() bool
		ClearLimit() Clauses
		SetLimit(limit interface{}) Clauses

		Offset() uint
		ClearOffset() Clauses
		SetOffset(offset uint) Clauses

		Compounds() []CompoundExpression
		CompoundsAppend(ce CompoundExpression) Clauses

		Lock() Lock
		SetLock(l Lock) Clauses

		CommonTables() []CommonTableExpression
		CommonTablesAppend(cte CommonTableExpression) Clauses

		Returning() ColumnListExpression
		HasReturning() bool
		SetReturning(cl ColumnListExpression) Clauses
	}
	clauses struct {
		commonTables   []CommonTableExpression
		selectColumns  ColumnListExpression
		selectDistinct ColumnListExpression
		from           ColumnListExpression
		joins          JoinExpressions
		where          ExpressionList
		alias          IdentifierExpression
		groupBy        ColumnListExpression
		having         ExpressionList
		order          ColumnListExpression
		limit          interface{}
		offset         uint
		returning      ColumnListExpression
		compounds      []CompoundExpression
		lock           Lock
	}
)

func NewClauses() Clauses {
	return &clauses{
		selectColumns: NewColumnListExpression(Star()),
	}
}

func (c *clauses) HasSources() bool {
	return c.from != nil && len(c.from.Columns()) > 0
}

func (c *clauses) IsDefaultSelect() bool {
	ret := false
	if c.selectColumns != nil {
		selects := c.selectColumns.Columns()
		if len(selects) == 1 {
			if l, ok := selects[0].(LiteralExpression); ok && l.Literal() == "*" {
				ret = true
			}
		}
	}
	return ret
}

func (c *clauses) clone() *clauses {
	return &clauses{
		commonTables:   c.commonTables,
		selectColumns:  c.selectColumns,
		selectDistinct: c.selectDistinct,
		from:           c.from,
		joins:          c.joins,
		where:          c.where,
		alias:          c.alias,
		groupBy:        c.groupBy,
		having:         c.having,
		order:          c.order,
		limit:          c.limit,
		offset:         c.offset,
		returning:      c.returning,
		compounds:      c.compounds,
		lock:           c.lock,
	}
}

func (c *clauses) CommonTables() []CommonTableExpression {
	return c.commonTables
}
func (c *clauses) CommonTablesAppend(cte CommonTableExpression) Clauses {
	ret := c.clone()
	ret.commonTables = append(ret.commonTables, cte)
	return ret
}

func (c *clauses) Select() ColumnListExpression {
	return c.selectColumns
}
func (c *clauses) SelectAppend(cl ColumnListExpression) Clauses {
	ret := c.clone()
	if ret.selectDistinct != nil {
		ret.selectDistinct = ret.selectDistinct.Append(cl.Columns()...)
	} else {
		ret.selectColumns = ret.selectColumns.Append(cl.Columns()...)
	}
	return ret
}

func (c *clauses) SetSelect(cl ColumnListExpression) Clauses {
	ret := c.clone()
	ret.selectDistinct = nil
	ret.selectColumns = cl
	return ret
}

func (c *clauses) SelectDistinct() ColumnListExpression {
	return c.selectDistinct
}
func (c *clauses) HasSelectDistinct() bool {
	return c.selectDistinct != nil
}
func (c *clauses) SetSelectDistinct(cl ColumnListExpression) Clauses {
	ret := c.clone()
	ret.selectColumns = nil
	ret.selectDistinct = cl
	return ret
}

func (c *clauses) From() ColumnListExpression {
	return c.from
}
func (c *clauses) SetFrom(cl ColumnListExpression) Clauses {
	ret := c.clone()
	ret.from = cl
	return ret
}

func (c *clauses) HasAlias() bool {
	return c.alias != nil
}

func (c *clauses) Alias() IdentifierExpression {
	return c.alias
}

func (c *clauses) SetAlias(ie IdentifierExpression) Clauses {
	ret := c.clone()
	ret.alias = ie
	return ret
}

func (c *clauses) Joins() JoinExpressions {
	return c.joins
}
func (c *clauses) JoinsAppend(jc JoinExpression) Clauses {
	ret := c.clone()
	ret.joins = append(ret.joins, jc)
	return ret
}

func (c *clauses) Where() ExpressionList {
	return c.where
}

func (c *clauses) ClearWhere() Clauses {
	ret := c.clone()
	ret.where = nil
	return ret
}

func (c *clauses) WhereAppend(expressions ...Expression) Clauses {
	expLen := len(expressions)
	if expLen == 0 {
		return c
	}
	ret := c.clone()
	if ret.where == nil {
		ret.where = NewExpressionList(AndType, expressions...)
	} else {
		ret.where = ret.where.Append(expressions...)
	}
	return ret
}

func (c *clauses) Having() ExpressionList {
	return c.having
}

func (c *clauses) ClearHaving() Clauses {
	ret := c.clone()
	ret.having = nil
	return ret
}

func (c *clauses) HavingAppend(expressions ...Expression) Clauses {
	expLen := len(expressions)
	if expLen == 0 {
		return c
	}
	ret := c.clone()
	if ret.having == nil {
		ret.having = NewExpressionList(AndType, expressions...)
	} else {
		ret.having = ret.having.Append(expressions...)
	}
	return ret
}

func (c *clauses) Lock() Lock {
	return c.lock
}
func (c *clauses) SetLock(l Lock) Clauses {
	ret := c.clone()
	ret.lock = l
	return ret
}

func (c *clauses) Order() ColumnListExpression {
	return c.order
}

func (c *clauses) HasOrder() bool {
	return c.order != nil
}

func (c *clauses) ClearOrder() Clauses {
	ret := c.clone()
	ret.order = nil
	return ret
}

func (c *clauses) SetOrder(oes ...OrderedExpression) Clauses {
	ret := c.clone()
	ret.order = NewOrderedColumnList(oes...)
	return ret
}

func (c *clauses) OrderAppend(oes ...OrderedExpression) Clauses {
	if c.order == nil {
		return c.SetOrder(oes...)
	}
	ret := c.clone()
	ret.order = ret.order.Append(NewOrderedColumnList(oes...).Columns()...)
	return ret
}

func (c *clauses) GroupBy() ColumnListExpression {
	return c.groupBy
}
func (c *clauses) SetGroupBy(cl ColumnListExpression) Clauses {
	ret := c.clone()
	ret.groupBy = cl
	return ret
}

func (c *clauses) Limit() interface{} {
	return c.limit
}

func (c *clauses) HasLimit() bool {
	return c.limit != nil
}

func (c *clauses) ClearLimit() Clauses {
	ret := c.clone()
	ret.limit = nil
	return ret
}

func (c *clauses) SetLimit(limit interface{}) Clauses {
	ret := c.clone()
	ret.limit = limit
	return ret
}

func (c *clauses) Offset() uint {
	return c.offset
}

func (c *clauses) ClearOffset() Clauses {
	ret := c.clone()
	ret.offset = 0
	return ret
}
func (c *clauses) SetOffset(offset uint) Clauses {
	ret := c.clone()
	ret.offset = offset
	return ret
}

func (c *clauses) Compounds() []CompoundExpression {
	return c.compounds
}
func (c *clauses) CompoundsAppend(ce CompoundExpression) Clauses {
	ret := c.clone()
	ret.compounds = append(ret.compounds, ce)
	return ret
}

func (c *clauses) Returning() ColumnListExpression {
	return c.returning
}

func (c *clauses) HasReturning() bool {
	return c.returning != nil
}

func (c *clauses) SetReturning(cl ColumnListExpression) Clauses {
	ret := c.clone()
	ret.returning = cl
	return ret
}
