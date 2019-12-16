package exp

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type lateralExpressionSuite struct {
	suite.Suite
}

func TestLateralExpressionSuite(t *testing.T) {
	suite.Run(t, &lateralExpressionSuite{})
}

func (les *lateralExpressionSuite) TestClone() {
	le := NewLateralExpression(newTestAppendableExpression(`SELECT * FROM "test"`, []interface{}{}))
	les.Equal(NewLateralExpression(newTestAppendableExpression(`SELECT * FROM "test"`, []interface{}{})), le.Clone())
}

func (les *lateralExpressionSuite) TestExpression() {
	le := NewLateralExpression(newTestAppendableExpression(`SELECT * FROM "test"`, []interface{}{}))
	les.Equal(le, le.Expression())
}

func (les *lateralExpressionSuite) TestLateral() {
	le := NewLateralExpression(newTestAppendableExpression(`SELECT * FROM "test"`, []interface{}{}))
	les.Equal(newTestAppendableExpression(`SELECT * FROM "test"`, []interface{}{}), le.Table())
}

func (les *lateralExpressionSuite) TestAs() {
	le := NewLateralExpression(newTestAppendableExpression(`SELECT * FROM "test"`, []interface{}{}))
	les.Equal(aliased(le, "foo"), le.As("foo"))
}
