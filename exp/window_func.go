package exp

type sqlWindowFunctionExpression struct {
	name       string
	args       []interface{}
	windowName string
	window     WindowExpression
}

func NewSQLWindowFunctionExpression(name string, args ...interface{}) SQLWindowFunctionExpression {
	return sqlWindowFunctionExpression{
		name: name,
		args: args,
	}
}

func (swfe sqlWindowFunctionExpression) clone() sqlWindowFunctionExpression {
	return sqlWindowFunctionExpression{
		name:       swfe.name,
		args:       swfe.args,
		windowName: swfe.windowName,
		window:     swfe.window,
	}
}

func (swfe sqlWindowFunctionExpression) Clone() Expression {
	return swfe.clone()
}
func (swfe sqlWindowFunctionExpression) Expression() Expression {
	return swfe
}
func (swfe sqlWindowFunctionExpression) As(val interface{}) AliasedExpression {
	return aliased(swfe, val)
}
func (swfe sqlWindowFunctionExpression) Eq(val interface{}) BooleanExpression  { return eq(swfe, val) }
func (swfe sqlWindowFunctionExpression) Neq(val interface{}) BooleanExpression { return neq(swfe, val) }
func (swfe sqlWindowFunctionExpression) Gt(val interface{}) BooleanExpression  { return gt(swfe, val) }
func (swfe sqlWindowFunctionExpression) Gte(val interface{}) BooleanExpression { return gte(swfe, val) }
func (swfe sqlWindowFunctionExpression) Lt(val interface{}) BooleanExpression  { return lt(swfe, val) }
func (swfe sqlWindowFunctionExpression) Lte(val interface{}) BooleanExpression { return lte(swfe, val) }
func (swfe sqlWindowFunctionExpression) Between(val RangeVal) RangeExpression {
	return between(swfe, val)
}
func (swfe sqlWindowFunctionExpression) NotBetween(val RangeVal) RangeExpression {
	return notBetween(swfe, val)
}
func (swfe sqlWindowFunctionExpression) Like(val interface{}) BooleanExpression {
	return like(swfe, val)
}
func (swfe sqlWindowFunctionExpression) NotLike(val interface{}) BooleanExpression {
	return notLike(swfe, val)
}
func (swfe sqlWindowFunctionExpression) ILike(val interface{}) BooleanExpression {
	return iLike(swfe, val)
}
func (swfe sqlWindowFunctionExpression) NotILike(val interface{}) BooleanExpression {
	return notILike(swfe, val)
}
func (swfe sqlWindowFunctionExpression) In(vals ...interface{}) BooleanExpression {
	return in(swfe, vals...)
}
func (swfe sqlWindowFunctionExpression) NotIn(vals ...interface{}) BooleanExpression {
	return notIn(swfe, vals...)
}
func (swfe sqlWindowFunctionExpression) Is(val interface{}) BooleanExpression { return is(swfe, val) }
func (swfe sqlWindowFunctionExpression) IsNot(val interface{}) BooleanExpression {
	return isNot(swfe, val)
}
func (swfe sqlWindowFunctionExpression) IsNull() BooleanExpression     { return is(swfe, nil) }
func (swfe sqlWindowFunctionExpression) IsNotNull() BooleanExpression  { return isNot(swfe, nil) }
func (swfe sqlWindowFunctionExpression) IsTrue() BooleanExpression     { return is(swfe, true) }
func (swfe sqlWindowFunctionExpression) IsNotTrue() BooleanExpression  { return isNot(swfe, true) }
func (swfe sqlWindowFunctionExpression) IsFalse() BooleanExpression    { return is(swfe, false) }
func (swfe sqlWindowFunctionExpression) IsNotFalse() BooleanExpression { return isNot(swfe, false) }

func (swfe sqlWindowFunctionExpression) Name() string { return swfe.name }

func (swfe sqlWindowFunctionExpression) Args() []interface{} { return swfe.args }

func (swfe sqlWindowFunctionExpression) Window() WindowExpression {
	return swfe.window
}

func (swfe sqlWindowFunctionExpression) WindowName() string {
	return swfe.windowName
}

func (swfe sqlWindowFunctionExpression) Over(we WindowExpression) SQLWindowFunctionExpression {
	ret := swfe.clone()
	ret.window = we
	return ret
}

func (swfe sqlWindowFunctionExpression) OverName(name string) SQLWindowFunctionExpression {
	ret := swfe.clone()
	ret.windowName = name
	return ret
}

func (swfe sqlWindowFunctionExpression) HasWindow() bool {
	return swfe.window != nil
}

func (swfe sqlWindowFunctionExpression) HasWindowName() bool {
	return swfe.windowName != ""
}
