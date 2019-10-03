package exp

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type sqlFunctionExpressionSuite struct {
	suite.Suite
	fn SQLFunctionExpression
}

func TestSQLFunctionExpressionSuite(t *testing.T) {
	suite.Run(t, &sqlFunctionExpressionSuite{
		fn: NewSQLFunctionExpression("COUNT", Star()),
	})
}

func (sfes *sqlFunctionExpressionSuite) TestClone() {
	sfes.Equal(sfes.fn, sfes.fn.Clone())
}

func (sfes *sqlFunctionExpressionSuite) TestExpression() {
	sfes.Equal(sfes.fn, sfes.fn.Expression())
}

func (sfes *sqlFunctionExpressionSuite) TestArgs() {
	sfes.Equal([]interface{}{Star()}, sfes.fn.Args())
}

func (sfes *sqlFunctionExpressionSuite) TestName() {
	sfes.Equal("COUNT", sfes.fn.Name())
}

func (sfes *sqlFunctionExpressionSuite) TestAllOthers() {
	fn := sfes.fn

	rv := NewRangeVal(1, 2)
	pattern := "func like%"
	inVals := []interface{}{1, 2}
	testCases := []struct {
		Ex       Expression
		Expected Expression
	}{
		{Ex: fn.As("a"), Expected: aliased(fn, "a")},
		{Ex: fn.Eq(1), Expected: NewBooleanExpression(EqOp, fn, 1)},
		{Ex: fn.Neq(1), Expected: NewBooleanExpression(NeqOp, fn, 1)},
		{Ex: fn.Gt(1), Expected: NewBooleanExpression(GtOp, fn, 1)},
		{Ex: fn.Gte(1), Expected: NewBooleanExpression(GteOp, fn, 1)},
		{Ex: fn.Lt(1), Expected: NewBooleanExpression(LtOp, fn, 1)},
		{Ex: fn.Lte(1), Expected: NewBooleanExpression(LteOp, fn, 1)},
		{Ex: fn.Between(rv), Expected: between(fn, rv)},
		{Ex: fn.NotBetween(rv), Expected: notBetween(fn, rv)},
		{Ex: fn.Like(pattern), Expected: NewBooleanExpression(LikeOp, fn, pattern)},
		{Ex: fn.NotLike(pattern), Expected: NewBooleanExpression(NotLikeOp, fn, pattern)},
		{Ex: fn.ILike(pattern), Expected: NewBooleanExpression(ILikeOp, fn, pattern)},
		{Ex: fn.NotILike(pattern), Expected: NewBooleanExpression(NotILikeOp, fn, pattern)},
		{Ex: fn.RegexpLike(pattern), Expected: NewBooleanExpression(RegexpLikeOp, fn, pattern)},
		{Ex: fn.RegexpNotLike(pattern), Expected: NewBooleanExpression(RegexpNotLikeOp, fn, pattern)},
		{Ex: fn.RegexpILike(pattern), Expected: NewBooleanExpression(RegexpILikeOp, fn, pattern)},
		{Ex: fn.RegexpNotILike(pattern), Expected: NewBooleanExpression(RegexpNotILikeOp, fn, pattern)},
		{Ex: fn.In(inVals), Expected: NewBooleanExpression(InOp, fn, inVals)},
		{Ex: fn.NotIn(inVals), Expected: NewBooleanExpression(NotInOp, fn, inVals)},
		{Ex: fn.Is(true), Expected: NewBooleanExpression(IsOp, fn, true)},
		{Ex: fn.IsNot(true), Expected: NewBooleanExpression(IsNotOp, fn, true)},
		{Ex: fn.IsNull(), Expected: NewBooleanExpression(IsOp, fn, nil)},
		{Ex: fn.IsNotNull(), Expected: NewBooleanExpression(IsNotOp, fn, nil)},
		{Ex: fn.IsTrue(), Expected: NewBooleanExpression(IsOp, fn, true)},
		{Ex: fn.IsNotTrue(), Expected: NewBooleanExpression(IsNotOp, fn, true)},
		{Ex: fn.IsFalse(), Expected: NewBooleanExpression(IsOp, fn, false)},
		{Ex: fn.IsNotFalse(), Expected: NewBooleanExpression(IsNotOp, fn, false)},
	}

	for _, tc := range testCases {
		sfes.Equal(tc.Expected, tc.Ex)
	}
}
