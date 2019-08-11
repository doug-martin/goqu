package exp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type sqlWindowFunctionExpressionTest struct {
	suite.Suite
}

func TestSQLWindowFunctionExpressionSuite(t *testing.T) {
	suite.Run(t, new(sqlWindowFunctionExpressionTest))
}

func (swfet *sqlWindowFunctionExpressionTest) TestClone() {
	t := swfet.T()
	wf := NewSQLWindowFunctionExpression("f1", "a")
	wf2 := wf.Clone()
	assert.Equal(t, wf, wf2)
}

func (swfet *sqlWindowFunctionExpressionTest) TestExpression() {
	t := swfet.T()
	wf := NewSQLWindowFunctionExpression("f1", "a")
	wf2 := wf.Expression()
	assert.Equal(t, wf, wf2)
}

func (swfet *sqlWindowFunctionExpressionTest) TestName() {
	t := swfet.T()
	wf := NewSQLWindowFunctionExpression("f1", "a")
	assert.Equal(t, wf.Name(), "f1")
}

func (swfet *sqlWindowFunctionExpressionTest) TestArgs() {
	t := swfet.T()
	wf := NewSQLWindowFunctionExpression("f1", "a")
	assert.Equal(t, wf.Args(), []interface{}{"a"})
}

func (swfet *sqlWindowFunctionExpressionTest) TestWindow() {
	t := swfet.T()
	w := NewWindowExpression("w", "", nil, nil)
	wf := NewSQLWindowFunctionExpression("f1", "a")
	assert.False(t, wf.HasWindow())

	wf = wf.Over(w)
	assert.True(t, wf.HasWindow())
	assert.Equal(t, wf.Window(), w)
}

func (swfet *sqlWindowFunctionExpressionTest) TestWindowName() {
	t := swfet.T()
	windowName := "w"
	wf := NewSQLWindowFunctionExpression("f1", "a")
	assert.False(t, wf.HasWindowName())

	wf = wf.OverName(windowName)
	assert.True(t, wf.HasWindowName())
	assert.Equal(t, wf.WindowName(), windowName)
}

func (swfet *sqlWindowFunctionExpressionTest) TestAllOthers() {
	t := swfet.T()
	wf := NewSQLWindowFunctionExpression("f1", "a")

	expAs := wf.As("a")
	assert.Equal(t, expAs.Aliased(), wf)

	expEq := wf.Eq(1)
	assert.Equal(t, expEq.LHS(), wf)
	assert.Equal(t, expEq.Op(), EqOp)
	assert.Equal(t, expEq.RHS(), 1)

	expNeq := wf.Neq(1)
	assert.Equal(t, expNeq.LHS(), wf)
	assert.Equal(t, expNeq.Op(), NeqOp)
	assert.Equal(t, expNeq.RHS(), 1)

	expGt := wf.Gt(1)
	assert.Equal(t, expGt.LHS(), wf)
	assert.Equal(t, expGt.Op(), GtOp)
	assert.Equal(t, expGt.RHS(), 1)

	expGte := wf.Gte(1)
	assert.Equal(t, expGte.LHS(), wf)
	assert.Equal(t, expGte.Op(), GteOp)
	assert.Equal(t, expGte.RHS(), 1)

	expLt := wf.Lt(1)
	assert.Equal(t, expLt.LHS(), wf)
	assert.Equal(t, expLt.Op(), LtOp)
	assert.Equal(t, expLt.RHS(), 1)

	expLte := wf.Lte(1)
	assert.Equal(t, expLte.LHS(), wf)
	assert.Equal(t, expLte.Op(), LteOp)
	assert.Equal(t, expLte.RHS(), 1)

	rv := NewRangeVal(1, 2)
	expBetween := wf.Between(rv)
	assert.Equal(t, expBetween.LHS(), wf)
	assert.Equal(t, expBetween.Op(), BetweenOp)
	assert.Equal(t, expBetween.RHS(), rv)

	expNotBetween := wf.NotBetween(rv)
	assert.Equal(t, expNotBetween.LHS(), wf)
	assert.Equal(t, expNotBetween.Op(), NotBetweenOp)
	assert.Equal(t, expNotBetween.RHS(), rv)

	pattern := "a%"
	expLike := wf.Like(pattern)
	assert.Equal(t, expLike.LHS(), wf)
	assert.Equal(t, expLike.Op(), LikeOp)
	assert.Equal(t, expLike.RHS(), pattern)

	expNotLike := wf.NotLike(pattern)
	assert.Equal(t, expNotLike.LHS(), wf)
	assert.Equal(t, expNotLike.Op(), NotLikeOp)
	assert.Equal(t, expNotLike.RHS(), pattern)

	expILike := wf.ILike(pattern)
	assert.Equal(t, expILike.LHS(), wf)
	assert.Equal(t, expILike.Op(), ILikeOp)
	assert.Equal(t, expILike.RHS(), pattern)

	expNotILike := wf.NotILike(pattern)
	assert.Equal(t, expNotILike.LHS(), wf)
	assert.Equal(t, expNotILike.Op(), NotILikeOp)
	assert.Equal(t, expNotILike.RHS(), pattern)

	vals := []interface{}{1, 2}
	expIn := wf.In(vals)
	assert.Equal(t, expIn.LHS(), wf)
	assert.Equal(t, expIn.Op(), InOp)
	assert.Equal(t, expIn.RHS(), vals)

	expNotIn := wf.NotIn(vals)
	assert.Equal(t, expNotIn.LHS(), wf)
	assert.Equal(t, expNotIn.Op(), NotInOp)
	assert.Equal(t, expNotIn.RHS(), vals)

	obj := 1
	expIs := wf.Is(obj)
	assert.Equal(t, expIs.LHS(), wf)
	assert.Equal(t, expIs.Op(), IsOp)
	assert.Equal(t, expIs.RHS(), obj)

	expIsNot := wf.IsNot(obj)
	assert.Equal(t, expIsNot.LHS(), wf)
	assert.Equal(t, expIsNot.Op(), IsNotOp)
	assert.Equal(t, expIsNot.RHS(), obj)

	expIsNull := wf.IsNull()
	assert.Equal(t, expIsNull.LHS(), wf)
	assert.Equal(t, expIsNull.Op(), IsOp)
	assert.Nil(t, expIsNull.RHS())

	expIsNotNull := wf.IsNotNull()
	assert.Equal(t, expIsNotNull.LHS(), wf)
	assert.Equal(t, expIsNotNull.Op(), IsNotOp)
	assert.Nil(t, expIsNotNull.RHS())

	expIsTrue := wf.IsTrue()
	assert.Equal(t, expIsTrue.LHS(), wf)
	assert.Equal(t, expIsTrue.Op(), IsOp)
	assert.Equal(t, expIsTrue.RHS(), true)

	expIsNotTrue := wf.IsNotTrue()
	assert.Equal(t, expIsNotTrue.LHS(), wf)
	assert.Equal(t, expIsNotTrue.Op(), IsNotOp)
	assert.Equal(t, expIsNotTrue.RHS(), true)

	expIsFalse := wf.IsFalse()
	assert.Equal(t, expIsFalse.LHS(), wf)
	assert.Equal(t, expIsFalse.Op(), IsOp)
	assert.Equal(t, expIsFalse.RHS(), false)

	expIsNotFalse := wf.IsNotFalse()
	assert.Equal(t, expIsNotFalse.LHS(), wf)
	assert.Equal(t, expIsNotFalse.Op(), IsNotOp)
	assert.Equal(t, expIsNotFalse.RHS(), false)
}
