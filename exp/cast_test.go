package exp

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type castExpressionSuite struct {
	suite.Suite
	ce CastExpression
}

func TestCastExpressionSuite(t *testing.T) {
	suite.Run(t, &castExpressionSuite{
		ce: NewCastExpression(NewIdentifierExpression("", "", "a"), "TEXT"),
	})
}

func (ces *castExpressionSuite) TestClone() {
	ces.Equal(ces.ce, ces.ce.Clone())
}

func (ces *castExpressionSuite) TestExpression() {
	ces.Equal(ces.ce, ces.ce.Expression())
}

func (ces *castExpressionSuite) TestCasted() {
	ces.Equal(NewIdentifierExpression("", "", "a"), ces.ce.Casted())
}
func (ces *castExpressionSuite) TestType() {
	ces.Equal(NewLiteralExpression("TEXT"), ces.ce.Type())
}

func (ces *castExpressionSuite) TestAllOthers() {
	ce := ces.ce
	rv := NewRangeVal(1, 2)
	pattern := "cast like%"
	inVals := []interface{}{1, 2}
	testCases := []struct {
		Ex       Expression
		Expected Expression
	}{
		{Ex: ce.As("a"), Expected: aliased(ce, "a")},
		{Ex: ce.Eq(1), Expected: NewBooleanExpression(EqOp, ce, 1)},
		{Ex: ce.Neq(1), Expected: NewBooleanExpression(NeqOp, ce, 1)},
		{Ex: ce.Gt(1), Expected: NewBooleanExpression(GtOp, ce, 1)},
		{Ex: ce.Gte(1), Expected: NewBooleanExpression(GteOp, ce, 1)},
		{Ex: ce.Lt(1), Expected: NewBooleanExpression(LtOp, ce, 1)},
		{Ex: ce.Lte(1), Expected: NewBooleanExpression(LteOp, ce, 1)},
		{Ex: ce.Asc(), Expected: asc(ce)},
		{Ex: ce.Desc(), Expected: desc(ce)},
		{Ex: ce.Between(rv), Expected: between(ce, rv)},
		{Ex: ce.NotBetween(rv), Expected: notBetween(ce, rv)},
		{Ex: ce.Like(pattern), Expected: NewBooleanExpression(LikeOp, ce, pattern)},
		{Ex: ce.NotLike(pattern), Expected: NewBooleanExpression(NotLikeOp, ce, pattern)},
		{Ex: ce.ILike(pattern), Expected: NewBooleanExpression(ILikeOp, ce, pattern)},
		{Ex: ce.NotILike(pattern), Expected: NewBooleanExpression(NotILikeOp, ce, pattern)},
		{Ex: ce.RegexpLike(pattern), Expected: NewBooleanExpression(RegexpLikeOp, ce, pattern)},
		{Ex: ce.RegexpNotLike(pattern), Expected: NewBooleanExpression(RegexpNotLikeOp, ce, pattern)},
		{Ex: ce.RegexpILike(pattern), Expected: NewBooleanExpression(RegexpILikeOp, ce, pattern)},
		{Ex: ce.RegexpNotILike(pattern), Expected: NewBooleanExpression(RegexpNotILikeOp, ce, pattern)},
		{Ex: ce.In(inVals), Expected: NewBooleanExpression(InOp, ce, inVals)},
		{Ex: ce.NotIn(inVals), Expected: NewBooleanExpression(NotInOp, ce, inVals)},
		{Ex: ce.Is(true), Expected: NewBooleanExpression(IsOp, ce, true)},
		{Ex: ce.IsNot(true), Expected: NewBooleanExpression(IsNotOp, ce, true)},
		{Ex: ce.IsNull(), Expected: NewBooleanExpression(IsOp, ce, nil)},
		{Ex: ce.IsNotNull(), Expected: NewBooleanExpression(IsNotOp, ce, nil)},
		{Ex: ce.IsTrue(), Expected: NewBooleanExpression(IsOp, ce, true)},
		{Ex: ce.IsNotTrue(), Expected: NewBooleanExpression(IsNotOp, ce, true)},
		{Ex: ce.IsFalse(), Expected: NewBooleanExpression(IsOp, ce, false)},
		{Ex: ce.IsNotFalse(), Expected: NewBooleanExpression(IsNotOp, ce, false)},
		{Ex: ce.Distinct(), Expected: NewSQLFunctionExpression("DISTINCT", ce)},
	}

	for _, tc := range testCases {
		ces.Equal(tc.Expected, tc.Ex)
	}
}
