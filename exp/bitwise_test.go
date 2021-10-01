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
	be := exp.NewBitwiseExpression(exp.BitwiseAndOp, exp.NewIdentifierExpression("", "", "col"), 1)
	bes.Equal(exp.NewAliasExpression(be, "a"), be.As("a"))
}

func (bes *bitwiseExpressionSuite) TestAsc() {
	be := exp.NewBitwiseExpression(exp.BitwiseAndOp, exp.NewIdentifierExpression("", "", "col"), 1)
	bes.Equal(exp.NewOrderedExpression(be, exp.AscDir, exp.NoNullsSortType), be.Asc())
}

func (bes *bitwiseExpressionSuite) TestDesc() {
	be := exp.NewBitwiseExpression(exp.BitwiseAndOp, exp.NewIdentifierExpression("", "", "col"), 1)
	bes.Equal(exp.NewOrderedExpression(be, exp.DescSortDir, exp.NoNullsSortType), be.Desc())
}
