package exp

type (
	orderedExpression struct {
		sortExpression Expression
		direction      SortDirection
		nullSortType   NullSortType
	}
)

// used internally to create a new SORT_ASC OrderedExpression
func asc(exp Expression) OrderedExpression {
	return NewOrderedExpression(exp, AscDir, NoNullsSortType)
}

// used internally to create a new SORT_DESC OrderedExpression
func desc(exp Expression) OrderedExpression {
	return NewOrderedExpression(exp, DescSortDir, NoNullsSortType)
}

// used internally to create a new SORT_ASC OrderedExpression
func NewOrderedExpression(exp Expression, direction SortDirection, sortType NullSortType) OrderedExpression {
	return orderedExpression{sortExpression: exp, direction: direction, nullSortType: sortType}
}

// Used internally to create an OrderedExpression that renders the expression without ASC or DESC
func unordered(exp Expression) OrderedExpression {
	return NewOrderedExpression(exp, NoSortDir, NoNullsSortType)
}

// Creates an OrderedExpression from a literal that is written as is, without an appended ASC or DESC. Use it when the
// literal contains the direction itself or another ORDER BY form (e.g. ClickHouse WITH FILL).
func NewOrderLiteral(sql string, args ...any) OrderedExpression {
	return unordered(NewLiteralExpression(sql, args...))
}

func (oe orderedExpression) Clone() Expression {
	return NewOrderedExpression(oe.sortExpression, oe.direction, oe.nullSortType)
}

func (oe orderedExpression) Expression() Expression {
	return oe
}

func (oe orderedExpression) SortExpression() Expression {
	return oe.sortExpression
}

func (oe orderedExpression) IsAsc() bool {
	return oe.direction == AscDir
}

func (oe orderedExpression) NullSortType() NullSortType {
	return oe.nullSortType
}

// Returns false when the expression carries its own sort direction (see NoSortDir) so none should be rendered.
func (oe orderedExpression) HasDirection() bool {
	return oe.direction != NoSortDir
}

func (oe orderedExpression) NullsFirst() OrderedExpression {
	return NewOrderedExpression(oe.sortExpression, oe.direction, NullsFirstSortType)
}

func (oe orderedExpression) NullsLast() OrderedExpression {
	return NewOrderedExpression(oe.sortExpression, oe.direction, NullsLastSortType)
}
