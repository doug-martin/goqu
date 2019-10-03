package exp

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type sqlWindowFunctionExpressionTest struct {
	suite.Suite
	fn SQLFunctionExpression
}

func TestSQLWindowFunctionExpressionSuite(t *testing.T) {
	suite.Run(t, &sqlWindowFunctionExpressionTest{
		fn: NewSQLFunctionExpression("COUNT", Star()),
	})
}

func (swfet *sqlWindowFunctionExpressionTest) TestClone() {
	wf := NewSQLWindowFunctionExpression(swfet.fn, NewIdentifierExpression("", "", "a"), nil)
	wf2 := wf.Clone()
	swfet.Equal(wf, wf2)
}

func (swfet *sqlWindowFunctionExpressionTest) TestExpression() {
	wf := NewSQLWindowFunctionExpression(swfet.fn, NewIdentifierExpression("", "", "a"), nil)
	wf2 := wf.Expression()
	swfet.Equal(wf, wf2)
}

func (swfet *sqlWindowFunctionExpressionTest) TestFunc() {
	wf := NewSQLWindowFunctionExpression(swfet.fn, NewIdentifierExpression("", "", "a"), nil)
	swfet.Equal(swfet.fn, wf.Func())
}

func (swfet *sqlWindowFunctionExpressionTest) TestWindow() {
	w := NewWindowExpression(
		NewIdentifierExpression("", "", "w"),
		nil,
		nil,
		nil,
	)
	wf := NewSQLWindowFunctionExpression(swfet.fn, NewIdentifierExpression("", "", "a"), nil)
	swfet.False(wf.HasWindow())

	wf = swfet.fn.Over(w)
	swfet.True(wf.HasWindow())
	swfet.Equal(wf.Window(), w)
}

func (swfet *sqlWindowFunctionExpressionTest) TestWindowName() {
	windowName := NewIdentifierExpression("", "", "a")
	wf := NewSQLWindowFunctionExpression(swfet.fn, nil, nil)
	swfet.False(wf.HasWindowName())

	wf = swfet.fn.OverName(windowName)
	swfet.True(wf.HasWindowName())
	swfet.Equal(wf.WindowName(), windowName)
}

func (swfet *sqlWindowFunctionExpressionTest) TestAllOthers() {
	wf := NewSQLWindowFunctionExpression(swfet.fn, nil, nil)

	rv := NewRangeVal(1, 2)
	pattern := "a%"
	inVals := []interface{}{1, 2}
	testCases := []struct {
		Ex       Expression
		Expected Expression
	}{
		{Ex: wf.As("a"), Expected: aliased(wf, "a")},
		{Ex: wf.Eq(1), Expected: NewBooleanExpression(EqOp, wf, 1)},
		{Ex: wf.Neq(1), Expected: NewBooleanExpression(NeqOp, wf, 1)},
		{Ex: wf.Gt(1), Expected: NewBooleanExpression(GtOp, wf, 1)},
		{Ex: wf.Gte(1), Expected: NewBooleanExpression(GteOp, wf, 1)},
		{Ex: wf.Lt(1), Expected: NewBooleanExpression(LtOp, wf, 1)},
		{Ex: wf.Lte(1), Expected: NewBooleanExpression(LteOp, wf, 1)},
		{Ex: wf.Between(rv), Expected: between(wf, rv)},
		{Ex: wf.NotBetween(rv), Expected: notBetween(wf, rv)},
		{Ex: wf.Like(pattern), Expected: NewBooleanExpression(LikeOp, wf, pattern)},
		{Ex: wf.NotLike(pattern), Expected: NewBooleanExpression(NotLikeOp, wf, pattern)},
		{Ex: wf.ILike(pattern), Expected: NewBooleanExpression(ILikeOp, wf, pattern)},
		{Ex: wf.NotILike(pattern), Expected: NewBooleanExpression(NotILikeOp, wf, pattern)},
		{Ex: wf.RegexpLike(pattern), Expected: NewBooleanExpression(RegexpLikeOp, wf, pattern)},
		{Ex: wf.RegexpNotLike(pattern), Expected: NewBooleanExpression(RegexpNotLikeOp, wf, pattern)},
		{Ex: wf.RegexpILike(pattern), Expected: NewBooleanExpression(RegexpILikeOp, wf, pattern)},
		{Ex: wf.RegexpNotILike(pattern), Expected: NewBooleanExpression(RegexpNotILikeOp, wf, pattern)},
		{Ex: wf.In(inVals), Expected: NewBooleanExpression(InOp, wf, inVals)},
		{Ex: wf.NotIn(inVals), Expected: NewBooleanExpression(NotInOp, wf, inVals)},
		{Ex: wf.Is(true), Expected: NewBooleanExpression(IsOp, wf, true)},
		{Ex: wf.IsNot(true), Expected: NewBooleanExpression(IsNotOp, wf, true)},
		{Ex: wf.IsNull(), Expected: NewBooleanExpression(IsOp, wf, nil)},
		{Ex: wf.IsNotNull(), Expected: NewBooleanExpression(IsNotOp, wf, nil)},
		{Ex: wf.IsTrue(), Expected: NewBooleanExpression(IsOp, wf, true)},
		{Ex: wf.IsNotTrue(), Expected: NewBooleanExpression(IsNotOp, wf, true)},
		{Ex: wf.IsFalse(), Expected: NewBooleanExpression(IsOp, wf, false)},
		{Ex: wf.IsNotFalse(), Expected: NewBooleanExpression(IsNotOp, wf, false)},
	}

	for _, tc := range testCases {
		swfet.Equal(tc.Expected, tc.Ex)
	}
}
