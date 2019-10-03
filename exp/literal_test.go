package exp

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type literalExpressionSuite struct {
	suite.Suite
	le LiteralExpression
}

func TestLiteralExpressionSuite(t *testing.T) {
	suite.Run(t, &literalExpressionSuite{
		le: NewLiteralExpression("? + ?", 1, 2),
	})
}

func (les *literalExpressionSuite) TestClone() {
	les.Equal(les.le, les.le.Clone())
}

func (les *literalExpressionSuite) TestExpression() {
	les.Equal(les.le, les.le.Expression())
}

func (les *literalExpressionSuite) TestLiteral() {
	les.Equal("? + ?", les.le.Literal())
}

func (les *literalExpressionSuite) TestArgs() {
	les.Equal([]interface{}{1, 2}, les.le.Args())
}

func (les *literalExpressionSuite) TestAllOthers() {
	le := les.le
	rv := NewRangeVal(1, 2)
	pattern := "literal like%"
	inVals := []interface{}{1, 2}
	testCases := []struct {
		Ex       Expression
		Expected Expression
	}{
		{Ex: le.As("a"), Expected: aliased(le, "a")},
		{Ex: le.Eq(1), Expected: NewBooleanExpression(EqOp, le, 1)},
		{Ex: le.Neq(1), Expected: NewBooleanExpression(NeqOp, le, 1)},
		{Ex: le.Gt(1), Expected: NewBooleanExpression(GtOp, le, 1)},
		{Ex: le.Gte(1), Expected: NewBooleanExpression(GteOp, le, 1)},
		{Ex: le.Lt(1), Expected: NewBooleanExpression(LtOp, le, 1)},
		{Ex: le.Lte(1), Expected: NewBooleanExpression(LteOp, le, 1)},
		{Ex: le.Asc(), Expected: asc(le)},
		{Ex: le.Desc(), Expected: desc(le)},
		{Ex: le.Between(rv), Expected: between(le, rv)},
		{Ex: le.NotBetween(rv), Expected: notBetween(le, rv)},
		{Ex: le.Like(pattern), Expected: NewBooleanExpression(LikeOp, le, pattern)},
		{Ex: le.NotLike(pattern), Expected: NewBooleanExpression(NotLikeOp, le, pattern)},
		{Ex: le.ILike(pattern), Expected: NewBooleanExpression(ILikeOp, le, pattern)},
		{Ex: le.NotILike(pattern), Expected: NewBooleanExpression(NotILikeOp, le, pattern)},
		{Ex: le.RegexpLike(pattern), Expected: NewBooleanExpression(RegexpLikeOp, le, pattern)},
		{Ex: le.RegexpNotLike(pattern), Expected: NewBooleanExpression(RegexpNotLikeOp, le, pattern)},
		{Ex: le.RegexpILike(pattern), Expected: NewBooleanExpression(RegexpILikeOp, le, pattern)},
		{Ex: le.RegexpNotILike(pattern), Expected: NewBooleanExpression(RegexpNotILikeOp, le, pattern)},
		{Ex: le.In(inVals), Expected: NewBooleanExpression(InOp, le, inVals)},
		{Ex: le.NotIn(inVals), Expected: NewBooleanExpression(NotInOp, le, inVals)},
		{Ex: le.Is(true), Expected: NewBooleanExpression(IsOp, le, true)},
		{Ex: le.IsNot(true), Expected: NewBooleanExpression(IsNotOp, le, true)},
		{Ex: le.IsNull(), Expected: NewBooleanExpression(IsOp, le, nil)},
		{Ex: le.IsNotNull(), Expected: NewBooleanExpression(IsNotOp, le, nil)},
		{Ex: le.IsTrue(), Expected: NewBooleanExpression(IsOp, le, true)},
		{Ex: le.IsNotTrue(), Expected: NewBooleanExpression(IsNotOp, le, true)},
		{Ex: le.IsFalse(), Expected: NewBooleanExpression(IsOp, le, false)},
		{Ex: le.IsNotFalse(), Expected: NewBooleanExpression(IsNotOp, le, false)},
	}

	for _, tc := range testCases {
		les.Equal(tc.Expected, tc.Ex)
	}
}
