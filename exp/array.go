package exp

type array struct {
	elems any
}

// Creates a new ArrayExpression for a slice. The slice renders as an array value instead of a list.
//
//	NewArrayExpression([]string{"a", "b"}) -> '{"a","b"}'
func NewArrayExpression(elems any) ArrayExpression {
	return array{elems: elems}
}

func (a array) Elements() any {
	return a.elems
}

func (a array) Clone() Expression {
	return array{elems: a.elems}
}

func (a array) Expression() Expression { return a }
