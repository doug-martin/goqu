package exp

type arithmetic struct {
	lhs Expression
	rhs interface{}
	op  ArithmeticOperation
}

func NewArithmeticExpression(op ArithmeticOperation, lhs Expression, rhs interface{}) ArithmeticExpression {
	return arithmetic{op: op, lhs: lhs, rhs: rhs}
}

func (a arithmetic) Clone() Expression {
	return NewArithmeticExpression(a.op, a.lhs.Clone(), a.rhs)
}

func (a arithmetic) RHS() interface{} {
	return a.rhs
}

func (a arithmetic) LHS() Expression {
	return a.lhs
}

func (a arithmetic) Op() ArithmeticOperation {
	return a.op
}

func (a arithmetic) Expression() Expression                           { return a }
func (a arithmetic) As(val interface{}) AliasedExpression             { return NewAliasExpression(a, val) }
func (a arithmetic) Eq(val interface{}) BooleanExpression             { return eq(a, val) }
func (a arithmetic) Neq(val interface{}) BooleanExpression            { return neq(a, val) }
func (a arithmetic) Gt(val interface{}) BooleanExpression             { return gt(a, val) }
func (a arithmetic) Gte(val interface{}) BooleanExpression            { return gte(a, val) }
func (a arithmetic) Lt(val interface{}) BooleanExpression             { return lt(a, val) }
func (a arithmetic) Lte(val interface{}) BooleanExpression            { return lte(a, val) }
func (a arithmetic) Asc() OrderedExpression                           { return asc(a) }
func (a arithmetic) Desc() OrderedExpression                          { return desc(a) }
func (a arithmetic) Like(i interface{}) BooleanExpression             { return like(a, i) }
func (a arithmetic) NotLike(i interface{}) BooleanExpression          { return notLike(a, i) }
func (a arithmetic) ILike(i interface{}) BooleanExpression            { return iLike(a, i) }
func (a arithmetic) NotILike(i interface{}) BooleanExpression         { return notILike(a, i) }
func (a arithmetic) RegexpLike(val interface{}) BooleanExpression     { return regexpLike(a, val) }
func (a arithmetic) RegexpNotLike(val interface{}) BooleanExpression  { return regexpNotLike(a, val) }
func (a arithmetic) RegexpILike(val interface{}) BooleanExpression    { return regexpILike(a, val) }
func (a arithmetic) RegexpNotILike(val interface{}) BooleanExpression { return regexpNotILike(a, val) }
func (a arithmetic) In(i ...interface{}) BooleanExpression            { return in(a, i...) }
func (a arithmetic) NotIn(i ...interface{}) BooleanExpression         { return notIn(a, i...) }
func (a arithmetic) Is(i interface{}) BooleanExpression               { return is(a, i) }
func (a arithmetic) IsNot(i interface{}) BooleanExpression            { return isNot(a, i) }
func (a arithmetic) IsNull() BooleanExpression                        { return is(a, nil) }
func (a arithmetic) IsNotNull() BooleanExpression                     { return isNot(a, nil) }
func (a arithmetic) IsTrue() BooleanExpression                        { return is(a, true) }
func (a arithmetic) IsNotTrue() BooleanExpression                     { return isNot(a, true) }
func (a arithmetic) IsFalse() BooleanExpression                       { return is(a, false) }
func (a arithmetic) IsNotFalse() BooleanExpression                    { return isNot(a, false) }
func (a arithmetic) Distinct() SQLFunctionExpression                  { return NewSQLFunctionExpression("DISTINCT", a) }
func (a arithmetic) Between(val RangeVal) RangeExpression             { return between(a, val) }
func (a arithmetic) NotBetween(val RangeVal) RangeExpression          { return notBetween(a, val) }
func (a arithmetic) Cast(t string) CastExpression                     { return NewCastExpression(a, t) }
func (a arithmetic) BitwiseInversion() BitwiseExpression              { return bitwiseInversion(a) }
func (a arithmetic) BitwiseOr(val interface{}) BitwiseExpression      { return bitwiseOr(a, val) }
func (a arithmetic) BitwiseAnd(val interface{}) BitwiseExpression     { return bitwiseAnd(a, val) }
func (a arithmetic) BitwiseXor(val interface{}) BitwiseExpression     { return bitwiseXor(a, val) }
func (a arithmetic) BitwiseLeftShift(val interface{}) BitwiseExpression {
	return bitwiseLeftShift(a, val)
}
func (a arithmetic) BitwiseRightShift(val interface{}) BitwiseExpression {
	return bitwiseRightShift(a, val)
}

func arithmeticAdd(lhs Expression, rhs interface{}) ArithmeticExpression {
	return NewArithmeticExpression(ArithmeticAddOp, lhs, rhs)
}
func arithmeticSub(lhs Expression, rhs interface{}) ArithmeticExpression {
	return NewArithmeticExpression(ArithmeticSubOp, lhs, rhs)
}
func arithmeticMul(lhs Expression, rhs interface{}) ArithmeticExpression {
	return NewArithmeticExpression(ArithmeticMulOp, lhs, rhs)
}
func arithmeticDiv(lhs Expression, rhs interface{}) ArithmeticExpression {
	return NewArithmeticExpression(ArithmeticDivOp, lhs, rhs)
}
