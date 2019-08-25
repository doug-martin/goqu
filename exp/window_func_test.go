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

	expAs := wf.As("a")
	swfet.Equal(expAs.Aliased(), wf)

	expEq := wf.Eq(1)
	swfet.Equal(expEq.LHS(), wf)
	swfet.Equal(expEq.Op(), EqOp)
	swfet.Equal(expEq.RHS(), 1)

	expNeq := wf.Neq(1)
	swfet.Equal(expNeq.LHS(), wf)
	swfet.Equal(expNeq.Op(), NeqOp)
	swfet.Equal(expNeq.RHS(), 1)

	expGt := wf.Gt(1)
	swfet.Equal(expGt.LHS(), wf)
	swfet.Equal(expGt.Op(), GtOp)
	swfet.Equal(expGt.RHS(), 1)

	expGte := wf.Gte(1)
	swfet.Equal(expGte.LHS(), wf)
	swfet.Equal(expGte.Op(), GteOp)
	swfet.Equal(expGte.RHS(), 1)

	expLt := wf.Lt(1)
	swfet.Equal(expLt.LHS(), wf)
	swfet.Equal(expLt.Op(), LtOp)
	swfet.Equal(expLt.RHS(), 1)

	expLte := wf.Lte(1)
	swfet.Equal(expLte.LHS(), wf)
	swfet.Equal(expLte.Op(), LteOp)
	swfet.Equal(expLte.RHS(), 1)

	rv := NewRangeVal(1, 2)
	expBetween := wf.Between(rv)
	swfet.Equal(expBetween.LHS(), wf)
	swfet.Equal(expBetween.Op(), BetweenOp)
	swfet.Equal(expBetween.RHS(), rv)

	expNotBetween := wf.NotBetween(rv)
	swfet.Equal(expNotBetween.LHS(), wf)
	swfet.Equal(expNotBetween.Op(), NotBetweenOp)
	swfet.Equal(expNotBetween.RHS(), rv)

	pattern := "a%"
	expLike := wf.Like(pattern)
	swfet.Equal(expLike.LHS(), wf)
	swfet.Equal(expLike.Op(), LikeOp)
	swfet.Equal(expLike.RHS(), pattern)

	expNotLike := wf.NotLike(pattern)
	swfet.Equal(expNotLike.LHS(), wf)
	swfet.Equal(expNotLike.Op(), NotLikeOp)
	swfet.Equal(expNotLike.RHS(), pattern)

	expILike := wf.ILike(pattern)
	swfet.Equal(expILike.LHS(), wf)
	swfet.Equal(expILike.Op(), ILikeOp)
	swfet.Equal(expILike.RHS(), pattern)

	expNotILike := wf.NotILike(pattern)
	swfet.Equal(expNotILike.LHS(), wf)
	swfet.Equal(expNotILike.Op(), NotILikeOp)
	swfet.Equal(expNotILike.RHS(), pattern)

	vals := []interface{}{1, 2}
	expIn := wf.In(vals)
	swfet.Equal(expIn.LHS(), wf)
	swfet.Equal(expIn.Op(), InOp)
	swfet.Equal(expIn.RHS(), vals)

	expNotIn := wf.NotIn(vals)
	swfet.Equal(expNotIn.LHS(), wf)
	swfet.Equal(expNotIn.Op(), NotInOp)
	swfet.Equal(expNotIn.RHS(), vals)

	obj := 1
	expIs := wf.Is(obj)
	swfet.Equal(expIs.LHS(), wf)
	swfet.Equal(expIs.Op(), IsOp)
	swfet.Equal(expIs.RHS(), obj)

	expIsNot := wf.IsNot(obj)
	swfet.Equal(expIsNot.LHS(), wf)
	swfet.Equal(expIsNot.Op(), IsNotOp)
	swfet.Equal(expIsNot.RHS(), obj)

	expIsNull := wf.IsNull()
	swfet.Equal(expIsNull.LHS(), wf)
	swfet.Equal(expIsNull.Op(), IsOp)
	swfet.Nil(expIsNull.RHS())

	expIsNotNull := wf.IsNotNull()
	swfet.Equal(expIsNotNull.LHS(), wf)
	swfet.Equal(expIsNotNull.Op(), IsNotOp)
	swfet.Nil(expIsNotNull.RHS())

	expIsTrue := wf.IsTrue()
	swfet.Equal(expIsTrue.LHS(), wf)
	swfet.Equal(expIsTrue.Op(), IsOp)
	swfet.Equal(expIsTrue.RHS(), true)

	expIsNotTrue := wf.IsNotTrue()
	swfet.Equal(expIsNotTrue.LHS(), wf)
	swfet.Equal(expIsNotTrue.Op(), IsNotOp)
	swfet.Equal(expIsNotTrue.RHS(), true)

	expIsFalse := wf.IsFalse()
	swfet.Equal(expIsFalse.LHS(), wf)
	swfet.Equal(expIsFalse.Op(), IsOp)
	swfet.Equal(expIsFalse.RHS(), false)

	expIsNotFalse := wf.IsNotFalse()
	swfet.Equal(expIsNotFalse.LHS(), wf)
	swfet.Equal(expIsNotFalse.Op(), IsNotOp)
	swfet.Equal(expIsNotFalse.RHS(), false)
}
