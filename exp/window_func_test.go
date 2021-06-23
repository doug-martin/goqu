package exp_test

import (
	"testing"

	"github.com/doug-martin/goqu/v9/exp"
	"github.com/stretchr/testify/suite"
)

type sqlWindowFunctionExpressionTest struct {
	suite.Suite
	fn exp.SQLFunctionExpression
}

func TestSQLWindowFunctionExpressionSuite(t *testing.T) {
	suite.Run(t, &sqlWindowFunctionExpressionTest{
		fn: exp.NewSQLFunctionExpression("COUNT", exp.Star()),
	})
}

func (swfet *sqlWindowFunctionExpressionTest) TestClone() {
	wf := exp.NewSQLWindowFunctionExpression(swfet.fn, exp.NewIdentifierExpression("", "", "a"), nil)
	wf2 := wf.Clone()
	swfet.Equal(wf, wf2)
}

func (swfet *sqlWindowFunctionExpressionTest) TestExpression() {
	wf := exp.NewSQLWindowFunctionExpression(swfet.fn, exp.NewIdentifierExpression("", "", "a"), nil)
	wf2 := wf.Expression()
	swfet.Equal(wf, wf2)
}

func (swfet *sqlWindowFunctionExpressionTest) TestFunc() {
	wf := exp.NewSQLWindowFunctionExpression(swfet.fn, exp.NewIdentifierExpression("", "", "a"), nil)
	swfet.Equal(swfet.fn, wf.Func())
}

func (swfet *sqlWindowFunctionExpressionTest) TestWindow() {
	w := exp.NewWindowExpression(
		exp.NewIdentifierExpression("", "", "w"),
		nil,
		nil,
		nil,
	)
	wf := exp.NewSQLWindowFunctionExpression(swfet.fn, exp.NewIdentifierExpression("", "", "a"), nil)
	swfet.False(wf.HasWindow())

	wf = swfet.fn.Over(w)
	swfet.True(wf.HasWindow())
	swfet.Equal(wf.Window(), w)
}

func (swfet *sqlWindowFunctionExpressionTest) TestWindowName() {
	windowName := exp.NewIdentifierExpression("", "", "a")
	wf := exp.NewSQLWindowFunctionExpression(swfet.fn, nil, nil)
	swfet.False(wf.HasWindowName())

	wf = swfet.fn.OverName(windowName)
	swfet.True(wf.HasWindowName())
	swfet.Equal(wf.WindowName(), windowName)
}

func (swfet *sqlWindowFunctionExpressionTest) TestAllOthers() {
	wf := exp.NewSQLWindowFunctionExpression(swfet.fn, nil, nil)

	rv := exp.NewRangeVal(1, 2)
	pattern := "a%"
	inVals := []interface{}{1, 2}
	testCases := []struct {
		Ex       exp.Expression
		Expected exp.Expression
	}{
		{Ex: wf.As("a"), Expected: exp.NewAliasExpression(wf, "a")},
		{Ex: wf.Eq(1), Expected: exp.NewBooleanExpression(exp.EqOp, wf, 1)},
		{Ex: wf.Neq(1), Expected: exp.NewBooleanExpression(exp.NeqOp, wf, 1)},
		{Ex: wf.Gt(1), Expected: exp.NewBooleanExpression(exp.GtOp, wf, 1)},
		{Ex: wf.Gte(1), Expected: exp.NewBooleanExpression(exp.GteOp, wf, 1)},
		{Ex: wf.Lt(1), Expected: exp.NewBooleanExpression(exp.LtOp, wf, 1)},
		{Ex: wf.Lte(1), Expected: exp.NewBooleanExpression(exp.LteOp, wf, 1)},
		{Ex: wf.Between(rv), Expected: exp.NewRangeExpression(exp.BetweenOp, wf, rv)},
		{Ex: wf.NotBetween(rv), Expected: exp.NewRangeExpression(exp.NotBetweenOp, wf, rv)},
		{Ex: wf.Like(pattern), Expected: exp.NewBooleanExpression(exp.LikeOp, wf, pattern)},
		{Ex: wf.NotLike(pattern), Expected: exp.NewBooleanExpression(exp.NotLikeOp, wf, pattern)},
		{Ex: wf.ILike(pattern), Expected: exp.NewBooleanExpression(exp.ILikeOp, wf, pattern)},
		{Ex: wf.NotILike(pattern), Expected: exp.NewBooleanExpression(exp.NotILikeOp, wf, pattern)},
		{Ex: wf.RegexpLike(pattern), Expected: exp.NewBooleanExpression(exp.RegexpLikeOp, wf, pattern)},
		{Ex: wf.RegexpNotLike(pattern), Expected: exp.NewBooleanExpression(exp.RegexpNotLikeOp, wf, pattern)},
		{Ex: wf.RegexpILike(pattern), Expected: exp.NewBooleanExpression(exp.RegexpILikeOp, wf, pattern)},
		{Ex: wf.RegexpNotILike(pattern), Expected: exp.NewBooleanExpression(exp.RegexpNotILikeOp, wf, pattern)},
		{Ex: wf.In(inVals), Expected: exp.NewBooleanExpression(exp.InOp, wf, inVals)},
		{Ex: wf.NotIn(inVals), Expected: exp.NewBooleanExpression(exp.NotInOp, wf, inVals)},
		{Ex: wf.Is(true), Expected: exp.NewBooleanExpression(exp.IsOp, wf, true)},
		{Ex: wf.IsNot(true), Expected: exp.NewBooleanExpression(exp.IsNotOp, wf, true)},
		{Ex: wf.IsNull(), Expected: exp.NewBooleanExpression(exp.IsOp, wf, nil)},
		{Ex: wf.IsNotNull(), Expected: exp.NewBooleanExpression(exp.IsNotOp, wf, nil)},
		{Ex: wf.IsTrue(), Expected: exp.NewBooleanExpression(exp.IsOp, wf, true)},
		{Ex: wf.IsNotTrue(), Expected: exp.NewBooleanExpression(exp.IsNotOp, wf, true)},
		{Ex: wf.IsFalse(), Expected: exp.NewBooleanExpression(exp.IsOp, wf, false)},
		{Ex: wf.IsNotFalse(), Expected: exp.NewBooleanExpression(exp.IsNotOp, wf, false)},
		{Ex: wf.Desc(), Expected: exp.NewOrderedExpression(wf, exp.DescSortDir, exp.NoNullsSortType)},
		{Ex: wf.Asc(), Expected: exp.NewOrderedExpression(wf, exp.AscDir, exp.NoNullsSortType)},
	}

	for _, tc := range testCases {
		swfet.Equal(tc.Expected, tc.Ex)
	}
}
