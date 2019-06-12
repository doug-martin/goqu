package exp

type (
	orderedExpression struct {
		sortExpression Expression
		direction      sortDirection
		nullSortType   nullSortType
	}
)

// used internally to create a new SORT_ASC OrderedExpression
func asc(exp Expression) OrderedExpression {
	return orderedExpression{sortExpression: exp, direction: ascDir, nullSortType: NoNullsSortType}
}

// used internally to create a new SORT_DESC OrderedExpression
func desc(exp Expression) OrderedExpression {
	return orderedExpression{sortExpression: exp, direction: descSortDir, nullSortType: NoNullsSortType}
}

func (oe orderedExpression) Clone() Expression {
	return orderedExpression{sortExpression: oe.sortExpression, direction: oe.direction, nullSortType: oe.nullSortType}
}

func (oe orderedExpression) Expression() Expression {
	return oe
}

func (oe orderedExpression) SortExpression() Expression {
	return oe.sortExpression
}

func (oe orderedExpression) IsAsc() bool {
	return oe.direction == ascDir
}

func (oe orderedExpression) NullSortType() nullSortType {
	return oe.nullSortType
}

func (oe orderedExpression) NullsFirst() OrderedExpression {
	return orderedExpression{sortExpression: oe.sortExpression, direction: oe.direction, nullSortType: NullsFirstSortType}
}

func (oe orderedExpression) NullsLast() OrderedExpression {
	return orderedExpression{sortExpression: oe.sortExpression, direction: oe.direction, nullSortType: NullsLastSortType}
}
