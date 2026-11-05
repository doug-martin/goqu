package exp

type array struct {
	elems interface{}
}

// Creates a new ArrayExpression for a slice. The slice renders as an array value instead of a list.
//
//	NewArrayExpression([]string{"a", "b"}) -> '{"a","b"}'
func NewArrayExpression(elems interface{}) ArrayExpression {
	return array{elems: elems}
}

func (a array) Elements() interface{} {
	return a.elems
}

func (a array) Clone() Expression {
	return array{elems: a.elems}
}

func (a array) Expression() Expression { return a }
