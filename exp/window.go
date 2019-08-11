package exp

type sqlWindowExpression struct {
	name          string
	parent        string
	partitionCols ColumnListExpression
	orderCols     ColumnListExpression
}

func NewWindowExpression(window string, parent string, partitionCols, orderCols ColumnListExpression) WindowExpression {
	if partitionCols == nil {
		partitionCols = NewColumnListExpression()
	}
	if orderCols == nil {
		orderCols = NewColumnListExpression()
	}
	return sqlWindowExpression{
		name:          window,
		parent:        parent,
		partitionCols: partitionCols,
		orderCols:     orderCols,
	}
}

func (we sqlWindowExpression) clone() sqlWindowExpression {
	return sqlWindowExpression{
		name:          we.name,
		parent:        we.parent,
		partitionCols: we.partitionCols.Clone().(ColumnListExpression),
		orderCols:     we.orderCols.Clone().(ColumnListExpression),
	}
}

func (we sqlWindowExpression) Clone() Expression {
	return we.clone()
}

func (we sqlWindowExpression) Expression() Expression {
	return we
}

func (we sqlWindowExpression) Name() string {
	return we.name
}

func (we sqlWindowExpression) Parent() string {
	return we.parent
}

func (we sqlWindowExpression) PartitionCols() ColumnListExpression {
	return we.partitionCols
}

func (we sqlWindowExpression) OrderCols() ColumnListExpression {
	return we.orderCols
}

func (we sqlWindowExpression) PartitionBy(cols ...interface{}) WindowExpression {
	ret := we.clone()
	ret.partitionCols = NewColumnListExpression(cols...)
	return ret
}

func (we sqlWindowExpression) OrderBy(cols ...interface{}) WindowExpression {
	ret := we.clone()
	ret.orderCols = NewColumnListExpression(cols...)
	return ret
}

func (we sqlWindowExpression) Inherit(parent string) WindowExpression {
	ret := we.clone()
	ret.parent = parent
	return ret
}
