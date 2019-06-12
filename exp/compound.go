package exp

type compound struct {
	t   CompoundType
	rhs Expression
}

func NewCompoundExpression(ct CompoundType, rhs Expression) CompoundExpression {
	return compound{t: ct, rhs: rhs}
}

func (c compound) Expression() Expression { return c }

func (c compound) Clone() Expression {
	return compound{t: c.t, rhs: c.rhs.Clone().(SQLExpression)}
}

func (c compound) Type() CompoundType { return c.t }
func (c compound) RHS() Expression    { return c.rhs }
