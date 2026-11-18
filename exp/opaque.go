package exp

type opaque struct {
	value any
}

// Wraps a value that must be sent as a single value, never expanded (a slice is not turned into a list, a struct
// is not flattened). Prepared statements bind the value unchanged, interpolated statements use its driver.Valuer
// or return an encode error. It is created for struct fields marked as opaque.
func NewOpaqueExpression(value any) OpaqueExpression {
	return opaque{value: value}
}

func (o opaque) Clone() Expression {
	return NewOpaqueExpression(o.value)
}

func (o opaque) Expression() Expression {
	return o
}

func (o opaque) Value() any {
	return o.value
}
