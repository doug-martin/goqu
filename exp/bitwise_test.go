package exp_test

import (
	"testing"

	"github.com/doug-martin/goqu/v9/exp"
	"github.com/stretchr/testify/suite"
)

type bitwiseExpressionSuite struct {
	suite.Suite
}

func TestBitwiseExpressionSuite(t *testing.T) {
	suite.Run(t, &bitwiseExpressionSuite{})
}

func (bes *bitwiseExpressionSuite) TestClone() {
	be := exp.NewBitwiseExpression(exp.BitwiseAndOp, exp.NewIdentifierExpression("", "", "col"), 1)
	bes.Equal(be, be.Clone())
}

func (bes *bitwiseExpressionSuite) TestExpression() {
	be := exp.NewBitwiseExpression(exp.BitwiseAndOp, exp.NewIdentifierExpression("", "", "col"), 1)
	bes.Equal(be, be.Expression())
}

func (bes *bitwiseExpressionSuite) TestAs() {
	be := exp.NewBitwiseExpression(exp.BitwiseInversionOp, exp.NewIdentifierExpression("", "", "col"), 1)
	bes.Equal(exp.NewAliasExpression(be, "a"), be.As("a"))
}

func (bes *bitwiseExpressionSuite) TestAsc() {
	be := exp.NewBitwiseExpression(exp.BitwiseAndOp, exp.NewIdentifierExpression("", "", "col"), 1)
	bes.Equal(exp.NewOrderedExpression(be, exp.AscDir, exp.NoNullsSortType), be.Asc())
}

func (bes *bitwiseExpressionSuite) TestDesc() {
	be := exp.NewBitwiseExpression(exp.BitwiseOrOp, exp.NewIdentifierExpression("", "", "col"), 1)
	bes.Equal(exp.NewOrderedExpression(be, exp.DescSortDir, exp.NoNullsSortType), be.Desc())
}

func (bes *bitwiseExpressionSuite) TestAllOthers() {
	be := exp.NewBitwiseExpression(exp.BitwiseRightShiftOp, exp.NewIdentifierExpression("", "", "col"), 1)
	rv := exp.NewRangeVal(1, 2)
	pattern := "bitwiseExp like%"
	inVals := []interface{}{1, 2}
	testCases := []struct {
		Ex       exp.Expression
		Expected exp.Expression
	}{
		{Ex: be.Eq(1), Expected: exp.NewBooleanExpression(exp.EqOp, be, 1)},
		{Ex: be.Neq(1), Expected: exp.NewBooleanExpression(exp.NeqOp, be, 1)},
		{Ex: be.Gt(1), Expected: exp.NewBooleanExpression(exp.GtOp, be, 1)},
		{Ex: be.Gte(1), Expected: exp.NewBooleanExpression(exp.GteOp, be, 1)},
		{Ex: be.Lt(1), Expected: exp.NewBooleanExpression(exp.LtOp, be, 1)},
		{Ex: be.Lte(1), Expected: exp.NewBooleanExpression(exp.LteOp, be, 1)},
		{Ex: be.Between(rv), Expected: exp.NewRangeExpression(exp.BetweenOp, be, rv)},
		{Ex: be.NotBetween(rv), Expected: exp.NewRangeExpression(exp.NotBetweenOp, be, rv)},
		{Ex: be.Like(pattern), Expected: exp.NewBooleanExpression(exp.LikeOp, be, pattern)},
		{Ex: be.NotLike(pattern), Expected: exp.NewBooleanExpression(exp.NotLikeOp, be, pattern)},
		{Ex: be.ILike(pattern), Expected: exp.NewBooleanExpression(exp.ILikeOp, be, pattern)},
		{Ex: be.NotILike(pattern), Expected: exp.NewBooleanExpression(exp.NotILikeOp, be, pattern)},
		{Ex: be.RegexpLike(pattern), Expected: exp.NewBooleanExpression(exp.RegexpLikeOp, be, pattern)},
		{Ex: be.RegexpNotLike(pattern), Expected: exp.NewBooleanExpression(exp.RegexpNotLikeOp, be, pattern)},
		{Ex: be.RegexpILike(pattern), Expected: exp.NewBooleanExpression(exp.RegexpILikeOp, be, pattern)},
		{Ex: be.RegexpNotILike(pattern), Expected: exp.NewBooleanExpression(exp.RegexpNotILikeOp, be, pattern)},
		{Ex: be.In(inVals), Expected: exp.NewBooleanExpression(exp.InOp, be, inVals)},
		{Ex: be.NotIn(inVals), Expected: exp.NewBooleanExpression(exp.NotInOp, be, inVals)},
		{Ex: be.Is(true), Expected: exp.NewBooleanExpression(exp.IsOp, be, true)},
		{Ex: be.IsNot(true), Expected: exp.NewBooleanExpression(exp.IsNotOp, be, true)},
		{Ex: be.IsNull(), Expected: exp.NewBooleanExpression(exp.IsOp, be, nil)},
		{Ex: be.IsNotNull(), Expected: exp.NewBooleanExpression(exp.IsNotOp, be, nil)},
		{Ex: be.IsTrue(), Expected: exp.NewBooleanExpression(exp.IsOp, be, true)},
		{Ex: be.IsNotTrue(), Expected: exp.NewBooleanExpression(exp.IsNotOp, be, true)},
		{Ex: be.IsFalse(), Expected: exp.NewBooleanExpression(exp.IsOp, be, false)},
		{Ex: be.IsNotFalse(), Expected: exp.NewBooleanExpression(exp.IsNotOp, be, false)},
		{Ex: be.Distinct(), Expected: exp.NewSQLFunctionExpression("DISTINCT", be)},
	}

	for _, tc := range testCases {
		bes.Equal(tc.Expected, tc.Ex)
	}
}
