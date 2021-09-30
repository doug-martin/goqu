package exp_test

import (
	"testing"

	"github.com/doug-martin/goqu/v9/exp"
	"github.com/stretchr/testify/suite"
)

type literalExpressionSuite struct {
	suite.Suite
	le exp.LiteralExpression
}

func TestLiteralExpressionSuite(t *testing.T) {
	suite.Run(t, &literalExpressionSuite{
		le: exp.NewLiteralExpression("? + ?", 1, 2),
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
	rv := exp.NewRangeVal(1, 2)
	pattern := "literal like%"
	inVals := []interface{}{1, 2}
	bitwiseVals := 2
	testCases := []struct {
		Ex       exp.Expression
		Expected exp.Expression
	}{
		{Ex: le.As("a"), Expected: exp.NewAliasExpression(le, "a")},
		{Ex: le.Eq(1), Expected: exp.NewBooleanExpression(exp.EqOp, le, 1)},
		{Ex: le.Neq(1), Expected: exp.NewBooleanExpression(exp.NeqOp, le, 1)},
		{Ex: le.Gt(1), Expected: exp.NewBooleanExpression(exp.GtOp, le, 1)},
		{Ex: le.Gte(1), Expected: exp.NewBooleanExpression(exp.GteOp, le, 1)},
		{Ex: le.Lt(1), Expected: exp.NewBooleanExpression(exp.LtOp, le, 1)},
		{Ex: le.Lte(1), Expected: exp.NewBooleanExpression(exp.LteOp, le, 1)},
		{Ex: le.Asc(), Expected: exp.NewOrderedExpression(le, exp.AscDir, exp.NoNullsSortType)},
		{Ex: le.Desc(), Expected: exp.NewOrderedExpression(le, exp.DescSortDir, exp.NoNullsSortType)},
		{Ex: le.Between(rv), Expected: exp.NewRangeExpression(exp.BetweenOp, le, rv)},
		{Ex: le.NotBetween(rv), Expected: exp.NewRangeExpression(exp.NotBetweenOp, le, rv)},
		{Ex: le.Like(pattern), Expected: exp.NewBooleanExpression(exp.LikeOp, le, pattern)},
		{Ex: le.NotLike(pattern), Expected: exp.NewBooleanExpression(exp.NotLikeOp, le, pattern)},
		{Ex: le.ILike(pattern), Expected: exp.NewBooleanExpression(exp.ILikeOp, le, pattern)},
		{Ex: le.NotILike(pattern), Expected: exp.NewBooleanExpression(exp.NotILikeOp, le, pattern)},
		{Ex: le.RegexpLike(pattern), Expected: exp.NewBooleanExpression(exp.RegexpLikeOp, le, pattern)},
		{Ex: le.RegexpNotLike(pattern), Expected: exp.NewBooleanExpression(exp.RegexpNotLikeOp, le, pattern)},
		{Ex: le.RegexpILike(pattern), Expected: exp.NewBooleanExpression(exp.RegexpILikeOp, le, pattern)},
		{Ex: le.RegexpNotILike(pattern), Expected: exp.NewBooleanExpression(exp.RegexpNotILikeOp, le, pattern)},
		{Ex: le.In(inVals), Expected: exp.NewBooleanExpression(exp.InOp, le, inVals)},
		{Ex: le.NotIn(inVals), Expected: exp.NewBooleanExpression(exp.NotInOp, le, inVals)},
		{Ex: le.Is(true), Expected: exp.NewBooleanExpression(exp.IsOp, le, true)},
		{Ex: le.IsNot(true), Expected: exp.NewBooleanExpression(exp.IsNotOp, le, true)},
		{Ex: le.IsNull(), Expected: exp.NewBooleanExpression(exp.IsOp, le, nil)},
		{Ex: le.IsNotNull(), Expected: exp.NewBooleanExpression(exp.IsNotOp, le, nil)},
		{Ex: le.IsTrue(), Expected: exp.NewBooleanExpression(exp.IsOp, le, true)},
		{Ex: le.IsNotTrue(), Expected: exp.NewBooleanExpression(exp.IsNotOp, le, true)},
		{Ex: le.IsFalse(), Expected: exp.NewBooleanExpression(exp.IsOp, le, false)},
		{Ex: le.IsNotFalse(), Expected: exp.NewBooleanExpression(exp.IsNotOp, le, false)},
		{Ex: le.BitwiseInversion(), Expected: exp.NewBitwiseExpression(exp.BitwiseInversionOp, nil, le)},
		{Ex: le.BitwiseOr(bitwiseVals), Expected: exp.NewBitwiseExpression(exp.BitwiseOrOp, le, bitwiseVals)},
		{Ex: le.BitwiseAnd(bitwiseVals), Expected: exp.NewBitwiseExpression(exp.BitwiseAndOp, le, bitwiseVals)},
		{Ex: le.BitwiseXor(bitwiseVals), Expected: exp.NewBitwiseExpression(exp.BitwiseXorOp, le, bitwiseVals)},
		{Ex: le.BitwiseLeftShift(bitwiseVals), Expected: exp.NewBitwiseExpression(exp.BitwiseLeftShiftOp, le, bitwiseVals)},
		{Ex: le.BitwiseRightShift(bitwiseVals), Expected: exp.NewBitwiseExpression(exp.BitwiseRightShiftOp, le, bitwiseVals)},
	}

	for _, tc := range testCases {
		les.Equal(tc.Expected, tc.Ex)
	}
}
