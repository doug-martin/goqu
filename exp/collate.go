package exp

type collate struct {
	collated  Expression
	collation any
}

// Creates a new CollateExpression that applies a collation to an expression
//
//	NewCollateExpression(I("a"), "en_US") -> "a" COLLATE en_US
//	NewCollateExpression(I("a"), I("pg_catalog.default")) -> "a" COLLATE "pg_catalog"."default"
func NewCollateExpression(e Expression, collation any) CollateExpression {
	return collate{collated: e, collation: collation}
}

func (c collate) Collated() Expression {
	return c.collated
}

func (c collate) Collation() any {
	return c.collation
}

func (c collate) Clone() Expression {
	collation := c.collation
	if e, ok := collation.(Expression); ok {
		collation = e.Clone()
	}
	return collate{collated: c.collated.Clone(), collation: collation}
}

func (c collate) Expression() Expression                   { return c }
func (c collate) As(val any) AliasedExpression             { return NewAliasExpression(c, val) }
func (c collate) Eq(val any) BooleanExpression             { return eq(c, val) }
func (c collate) Neq(val any) BooleanExpression            { return neq(c, val) }
func (c collate) Gt(val any) BooleanExpression             { return gt(c, val) }
func (c collate) Gte(val any) BooleanExpression            { return gte(c, val) }
func (c collate) Lt(val any) BooleanExpression             { return lt(c, val) }
func (c collate) Lte(val any) BooleanExpression            { return lte(c, val) }
func (c collate) Asc() OrderedExpression                   { return asc(c) }
func (c collate) Desc() OrderedExpression                  { return desc(c) }
func (c collate) Like(i any) BooleanExpression             { return like(c, i) }
func (c collate) NotLike(i any) BooleanExpression          { return notLike(c, i) }
func (c collate) ILike(i any) BooleanExpression            { return iLike(c, i) }
func (c collate) NotILike(i any) BooleanExpression         { return notILike(c, i) }
func (c collate) RegexpLike(val any) BooleanExpression     { return regexpLike(c, val) }
func (c collate) RegexpNotLike(val any) BooleanExpression  { return regexpNotLike(c, val) }
func (c collate) RegexpILike(val any) BooleanExpression    { return regexpILike(c, val) }
func (c collate) RegexpNotILike(val any) BooleanExpression { return regexpNotILike(c, val) }
func (c collate) In(i ...any) BooleanExpression            { return in(c, i...) }
func (c collate) NotIn(i ...any) BooleanExpression         { return notIn(c, i...) }
func (c collate) Is(i any) BooleanExpression               { return is(c, i) }
func (c collate) IsNot(i any) BooleanExpression            { return isNot(c, i) }
func (c collate) IsNull() BooleanExpression                { return is(c, nil) }
func (c collate) IsNotNull() BooleanExpression             { return isNot(c, nil) }
func (c collate) IsTrue() BooleanExpression                { return is(c, true) }
func (c collate) IsNotTrue() BooleanExpression             { return isNot(c, true) }
func (c collate) IsFalse() BooleanExpression               { return is(c, false) }
func (c collate) IsNotFalse() BooleanExpression            { return isNot(c, false) }
func (c collate) Distinct() SQLFunctionExpression          { return NewSQLFunctionExpression("DISTINCT", c) }
func (c collate) Between(val RangeVal) RangeExpression     { return between(c, val) }
func (c collate) NotBetween(val RangeVal) RangeExpression  { return notBetween(c, val) }
