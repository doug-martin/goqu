package exp

type collate struct {
	collated  Expression
	collation string
}

// Creates a new CollateExpression that applies a collation to an expression
//
//	NewCollateExpression(I("a"), "en_US") -> "a" COLLATE en_US
func NewCollateExpression(e Expression, collation string) CollateExpression {
	return collate{collated: e, collation: collation}
}

func (c collate) Collated() Expression {
	return c.collated
}

func (c collate) Collation() string {
	return c.collation
}

func (c collate) Clone() Expression {
	return collate{collated: c.collated.Clone(), collation: c.collation}
}

func (c collate) Expression() Expression                           { return c }
func (c collate) As(val interface{}) AliasedExpression             { return NewAliasExpression(c, val) }
func (c collate) Eq(val interface{}) BooleanExpression             { return eq(c, val) }
func (c collate) Neq(val interface{}) BooleanExpression            { return neq(c, val) }
func (c collate) Gt(val interface{}) BooleanExpression             { return gt(c, val) }
func (c collate) Gte(val interface{}) BooleanExpression            { return gte(c, val) }
func (c collate) Lt(val interface{}) BooleanExpression             { return lt(c, val) }
func (c collate) Lte(val interface{}) BooleanExpression            { return lte(c, val) }
func (c collate) Asc() OrderedExpression                           { return asc(c) }
func (c collate) Desc() OrderedExpression                          { return desc(c) }
func (c collate) Like(i interface{}) BooleanExpression             { return like(c, i) }
func (c collate) NotLike(i interface{}) BooleanExpression          { return notLike(c, i) }
func (c collate) ILike(i interface{}) BooleanExpression            { return iLike(c, i) }
func (c collate) NotILike(i interface{}) BooleanExpression         { return notILike(c, i) }
func (c collate) RegexpLike(val interface{}) BooleanExpression     { return regexpLike(c, val) }
func (c collate) RegexpNotLike(val interface{}) BooleanExpression  { return regexpNotLike(c, val) }
func (c collate) RegexpILike(val interface{}) BooleanExpression    { return regexpILike(c, val) }
func (c collate) RegexpNotILike(val interface{}) BooleanExpression { return regexpNotILike(c, val) }
func (c collate) In(i ...interface{}) BooleanExpression            { return in(c, i...) }
func (c collate) NotIn(i ...interface{}) BooleanExpression         { return notIn(c, i...) }
func (c collate) Is(i interface{}) BooleanExpression               { return is(c, i) }
func (c collate) IsNot(i interface{}) BooleanExpression            { return isNot(c, i) }
func (c collate) IsNull() BooleanExpression                        { return is(c, nil) }
func (c collate) IsNotNull() BooleanExpression                     { return isNot(c, nil) }
func (c collate) IsTrue() BooleanExpression                        { return is(c, true) }
func (c collate) IsNotTrue() BooleanExpression                     { return isNot(c, true) }
func (c collate) IsFalse() BooleanExpression                       { return is(c, false) }
func (c collate) IsNotFalse() BooleanExpression                    { return isNot(c, false) }
func (c collate) Distinct() SQLFunctionExpression                  { return NewSQLFunctionExpression("DISTINCT", c) }
func (c collate) Between(val RangeVal) RangeExpression             { return between(c, val) }
func (c collate) NotBetween(val RangeVal) RangeExpression          { return notBetween(c, val) }
