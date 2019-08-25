package exp

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type windowExpressionTest struct {
	suite.Suite
}

func TestWindowExpressionSuite(t *testing.T) {
	suite.Run(t, new(windowExpressionTest))
}

func (wet *windowExpressionTest) TestClone() {
	w := NewWindowExpression(NewIdentifierExpression("", "", "w"), nil, nil, nil)
	w2 := w.Clone()

	wet.Equal(w, w2)
}

func (wet *windowExpressionTest) TestExpression() {
	w := NewWindowExpression(NewIdentifierExpression("", "", "w"), nil, nil, nil)
	w2 := w.Expression()

	wet.Equal(w, w2)
}

func (wet *windowExpressionTest) TestName() {
	name := NewIdentifierExpression("", "", "w")
	w := NewWindowExpression(NewIdentifierExpression("", "", "w"), nil, nil, nil)

	wet.Equal(name, w.Name())
}

func (wet *windowExpressionTest) TestPartitionCols() {
	cols := NewColumnListExpression("a", "b")
	w := NewWindowExpression(NewIdentifierExpression("", "", "w"), nil, cols, nil)

	wet.Equal(cols, w.PartitionCols())
	wet.Equal(cols, w.Clone().(WindowExpression).PartitionCols())
}

func (wet *windowExpressionTest) TestOrderCols() {
	cols := NewColumnListExpression("a", "b")
	w := NewWindowExpression(NewIdentifierExpression("", "", "w"), nil, nil, cols)

	wet.Equal(cols, w.OrderCols())
	wet.Equal(cols, w.Clone().(WindowExpression).OrderCols())
}

func (wet *windowExpressionTest) TestPartitionBy() {
	cols := NewColumnListExpression("a", "b")
	w := NewWindowExpression(NewIdentifierExpression("", "", "w"), nil, nil, nil).PartitionBy("a", "b")

	wet.Equal(cols, w.PartitionCols())
}

func (wet *windowExpressionTest) TestOrderBy() {
	cols := NewColumnListExpression("a", "b")
	w := NewWindowExpression(NewIdentifierExpression("", "", "w"), nil, nil, nil).OrderBy("a", "b")

	wet.Equal(cols, w.OrderCols())
}

func (wet *windowExpressionTest) TestParent() {
	parent := NewIdentifierExpression("", "", "w1")
	w := NewWindowExpression(NewIdentifierExpression("", "", "w"), parent, nil, nil)

	wet.Equal(parent, w.Parent())
}

func (wet *windowExpressionTest) TestInherit() {
	parent := NewIdentifierExpression("", "", "w1")
	w := NewWindowExpression(NewIdentifierExpression("", "", "w"), parent, nil, nil)

	wet.Equal(parent, w.Parent())

	w = w.Inherit("w2")
	wet.Equal(NewIdentifierExpression("", "", "w2"), w.Parent())
}
