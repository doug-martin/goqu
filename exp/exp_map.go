package exp

type (
	// A map of expressions to be ANDed together where the keys are string that will be used as Identifiers and values
	// will be used in a boolean operation.
	// The Ex map can be used in tandem with Op map to create more complex expression such as LIKE, GT, LT...
	// See examples.
	Ex map[string]interface{}
	// A map of expressions to be ORed together where the keys are string that will be used as Identifiers and values
	// will be used in a boolean operation.
	// The Ex map can be used in tandem with Op map to create more complex expression such as LIKE, GT, LT...
	// See examples.
	ExOr map[string]interface{}
	// Used in tandem with the Ex map to create complex comparisons such as LIKE, GT, LT... See examples
	Op map[string]interface{}
)

func (e Ex) Expression() Expression {
	return e
}

func (e Ex) Clone() Expression {
	ret := Ex{}
	for key, val := range e {
		ret[key] = val
	}
	return ret
}

func (e Ex) ToExpressions() (ExpressionList, error) {
	return mapToExpressionList(e, AndType)
}

func (eo ExOr) Expression() Expression {
	return eo
}

func (eo ExOr) Clone() Expression {
	ret := Ex{}
	for key, val := range eo {
		ret[key] = val
	}
	return ret
}

func (eo ExOr) ToExpressions() (ExpressionList, error) {
	return mapToExpressionList(eo, OrType)
}
