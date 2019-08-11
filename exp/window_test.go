package exp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type windowExpressionTest struct {
	suite.Suite
}

func TestWindowExpressionSuite(t *testing.T) {
	suite.Run(t, new(windowExpressionTest))
}

func (wet *windowExpressionTest) TestClone() {
	t := wet.T()
	w := NewWindowExpression("w", "", nil, nil)
	w2 := w.Clone()

	assert.Equal(t, w, w2)
}

func (wet *windowExpressionTest) TestExpression() {
	t := wet.T()
	w := NewWindowExpression("w", "", nil, nil)
	w2 := w.Expression()

	assert.Equal(t, w, w2)
}

func (wet *windowExpressionTest) TestName() {
	t := wet.T()
	w := NewWindowExpression("w", "", nil, nil)

	assert.Equal(t, "w", w.Name())
}

func (wet *windowExpressionTest) TestPartitionCols() {
	t := wet.T()
	cols := NewColumnListExpression("a", "b")
	w := NewWindowExpression("w", "", cols, nil)

	assert.Equal(t, cols, w.PartitionCols())
	assert.Equal(t, cols, w.Clone().(WindowExpression).PartitionCols())
}

func (wet *windowExpressionTest) TestOrderCols() {
	t := wet.T()
	cols := NewColumnListExpression("a", "b")
	w := NewWindowExpression("w", "", nil, cols)

	assert.Equal(t, cols, w.OrderCols())
	assert.Equal(t, cols, w.Clone().(WindowExpression).OrderCols())
}

func (wet *windowExpressionTest) TestPartitonBy() {
	t := wet.T()
	cols := NewColumnListExpression("a", "b")
	w := NewWindowExpression("w", "", nil, nil).PartitionBy("a", "b")

	assert.Equal(t, cols, w.PartitionCols())
}

func (wet *windowExpressionTest) TestOrderBy() {
	t := wet.T()
	cols := NewColumnListExpression("a", "b")
	w := NewWindowExpression("w", "", nil, nil).OrderBy("a", "b")

	assert.Equal(t, cols, w.OrderCols())
}

func (wet *windowExpressionTest) TestParent() {
	t := wet.T()
	w := NewWindowExpression("w", "w1", nil, nil)

	assert.Equal(t, "w1", w.Parent())
}

func (wet *windowExpressionTest) TestInherit() {
	t := wet.T()
	w := NewWindowExpression("w", "w1", nil, nil)

	assert.Equal(t, "w1", w.Parent())

	w = w.Inherit("w2")
	assert.Equal(t, "w2", w.Parent())
}
