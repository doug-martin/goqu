package exp_test

import (
	"testing"

	"github.com/doug-martin/goqu/v9/exp"
	"github.com/stretchr/testify/suite"
)

type castExpressionSuite struct {
	suite.Suite
	ce exp.CastExpression
}

func TestCastExpressionSuite(t *testing.T) {
	suite.Run(t, &castExpressionSuite{
		ce: exp.NewCastExpression(exp.NewIdentifierExpression("", "", "a"), "TEXT"),
	})
}

func (ces *castExpressionSuite) TestClone() {
	ces.Equal(ces.ce, ces.ce.Clone())
}

func (ces *castExpressionSuite) TestExpression() {
	ces.Equal(ces.ce, ces.ce.Expression())
}

func (ces *castExpressionSuite) TestCasted() {
	ces.Equal(exp.NewIdentifierExpression("", "", "a"), ces.ce.Casted())
}

func (ces *castExpressionSuite) TestType() {
	ces.Equal(exp.NewLiteralExpression("TEXT"), ces.ce.Type())
}

func (ces *castExpressionSuite) TestAllOthers() {
	ce := ces.ce
	rv := exp.NewRangeVal(1, 2)
	pattern := "cast like%"
	inVals := []interface{}{1, 2}
	testCases := []struct {
		Ex       exp.Expression
		Expected exp.Expression
	}{
		{Ex: ce.As("a"), Expected: exp.NewAliasExpression(ce, "a")},
		{Ex: ce.Eq(1), Expected: exp.NewBooleanExpression(exp.EqOp, ce, 1)},
		{Ex: ce.Neq(1), Expected: exp.NewBooleanExpression(exp.NeqOp, ce, 1)},
		{Ex: ce.Gt(1), Expected: exp.NewBooleanExpression(exp.GtOp, ce, 1)},
		{Ex: ce.Gte(1), Expected: exp.NewBooleanExpression(exp.GteOp, ce, 1)},
		{Ex: ce.Lt(1), Expected: exp.NewBooleanExpression(exp.LtOp, ce, 1)},
		{Ex: ce.Lte(1), Expected: exp.NewBooleanExpression(exp.LteOp, ce, 1)},
		{Ex: ce.Asc(), Expected: exp.NewOrderedExpression(ce, exp.AscDir, exp.NoNullsSortType)},
		{Ex: ce.Desc(), Expected: exp.NewOrderedExpression(ce, exp.DescSortDir, exp.NoNullsSortType)},
		{Ex: ce.Between(rv), Expected: exp.NewRangeExpression(exp.BetweenOp, ce, rv)},
		{Ex: ce.NotBetween(rv), Expected: exp.NewRangeExpression(exp.NotBetweenOp, ce, rv)},
		{Ex: ce.Like(pattern), Expected: exp.NewBooleanExpression(exp.LikeOp, ce, pattern)},
		{Ex: ce.NotLike(pattern), Expected: exp.NewBooleanExpression(exp.NotLikeOp, ce, pattern)},
		{Ex: ce.ILike(pattern), Expected: exp.NewBooleanExpression(exp.ILikeOp, ce, pattern)},
		{Ex: ce.NotILike(pattern), Expected: exp.NewBooleanExpression(exp.NotILikeOp, ce, pattern)},
		{Ex: ce.RegexpLike(pattern), Expected: exp.NewBooleanExpression(exp.RegexpLikeOp, ce, pattern)},
		{Ex: ce.RegexpNotLike(pattern), Expected: exp.NewBooleanExpression(exp.RegexpNotLikeOp, ce, pattern)},
		{Ex: ce.RegexpILike(pattern), Expected: exp.NewBooleanExpression(exp.RegexpILikeOp, ce, pattern)},
		{Ex: ce.RegexpNotILike(pattern), Expected: exp.NewBooleanExpression(exp.RegexpNotILikeOp, ce, pattern)},
		{Ex: ce.In(inVals), Expected: exp.NewBooleanExpression(exp.InOp, ce, inVals)},
		{Ex: ce.NotIn(inVals), Expected: exp.NewBooleanExpression(exp.NotInOp, ce, inVals)},
		{Ex: ce.Is(true), Expected: exp.NewBooleanExpression(exp.IsOp, ce, true)},
		{Ex: ce.IsNot(true), Expected: exp.NewBooleanExpression(exp.IsNotOp, ce, true)},
		{Ex: ce.IsNull(), Expected: exp.NewBooleanExpression(exp.IsOp, ce, nil)},
		{Ex: ce.IsNotNull(), Expected: exp.NewBooleanExpression(exp.IsNotOp, ce, nil)},
		{Ex: ce.IsTrue(), Expected: exp.NewBooleanExpression(exp.IsOp, ce, true)},
		{Ex: ce.IsNotTrue(), Expected: exp.NewBooleanExpression(exp.IsNotOp, ce, true)},
		{Ex: ce.IsFalse(), Expected: exp.NewBooleanExpression(exp.IsOp, ce, false)},
		{Ex: ce.IsNotFalse(), Expected: exp.NewBooleanExpression(exp.IsNotOp, ce, false)},
		{Ex: ce.Distinct(), Expected: exp.NewSQLFunctionExpression("DISTINCT", ce)},
	}

	for _, tc := range testCases {
		ces.Equal(tc.Expected, tc.Ex)
	}
}
