package exp_test

import (
	"testing"

	"github.com/doug-martin/goqu/v9/exp"
	"github.com/stretchr/testify/suite"
)

type windowExpressionTest struct {
	suite.Suite
}

func TestWindowExpressionSuite(t *testing.T) {
	suite.Run(t, new(windowExpressionTest))
}

func (wet *windowExpressionTest) TestClone() {
	w := exp.NewWindowExpression(exp.NewIdentifierExpression("", "", "w"), nil, nil, nil)
	w2 := w.Clone()

	wet.Equal(w, w2)
}

func (wet *windowExpressionTest) TestExpression() {
	w := exp.NewWindowExpression(exp.NewIdentifierExpression("", "", "w"), nil, nil, nil)
	w2 := w.Expression()

	wet.Equal(w, w2)
}

func (wet *windowExpressionTest) TestName() {
	name := exp.NewIdentifierExpression("", "", "w")
	w := exp.NewWindowExpression(exp.NewIdentifierExpression("", "", "w"), nil, nil, nil)

	wet.Equal(name, w.Name())
}

func (wet *windowExpressionTest) TestPartitionCols() {
	cols := exp.NewColumnListExpression(nil, "a", "b")
	w := exp.NewWindowExpression(exp.NewIdentifierExpression("", "", "w"), nil, cols, nil)

	wet.Equal(cols, w.PartitionCols())
	wet.Equal(cols, w.Clone().(exp.WindowExpression).PartitionCols())
}

func (wet *windowExpressionTest) TestOrderCols() {
	cols := exp.NewColumnListExpression(nil, "a", "b")
	w := exp.NewWindowExpression(exp.NewIdentifierExpression("", "", "w"), nil, nil, cols)

	wet.Equal(cols, w.OrderCols())
	wet.Equal(cols, w.Clone().(exp.WindowExpression).OrderCols())
}

func (wet *windowExpressionTest) TestPartitionBy() {
	cols := exp.NewColumnListExpression(nil, "a", "b")
	w := exp.NewWindowExpression(exp.NewIdentifierExpression("", "", "w"), nil, nil, nil).PartitionBy("a", "b")

	wet.Equal(cols, w.PartitionCols())
}

func (wet *windowExpressionTest) TestOrderBy() {
	cols := exp.NewColumnListExpression(nil, "a", "b")
	w := exp.NewWindowExpression(exp.NewIdentifierExpression("", "", "w"), nil, nil, nil).OrderBy("a", "b")

	wet.Equal(cols, w.OrderCols())
}

func (wet *windowExpressionTest) TestParent() {
	parent := exp.NewIdentifierExpression("", "", "w1")
	w := exp.NewWindowExpression(exp.NewIdentifierExpression("", "", "w"), parent, nil, nil)

	wet.Equal(parent, w.Parent())
}

func (wet *windowExpressionTest) TestInherit() {
	parent := exp.NewIdentifierExpression("", "", "w1")
	w := exp.NewWindowExpression(exp.NewIdentifierExpression("", "", "w"), parent, nil, nil)

	wet.Equal(parent, w.Parent())

	w = w.Inherit("w2")
	wet.Equal(exp.NewIdentifierExpression("", "", "w2"), w.Parent())
}
