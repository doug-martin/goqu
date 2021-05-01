package exp_test

import (
	"testing"

	"github.com/doug-martin/goqu/v9/exp"
	"github.com/stretchr/testify/suite"
)

type aliasExpressionSuite struct {
	suite.Suite
}

func TestAliasExpressionSuite(t *testing.T) {
	suite.Run(t, &aliasExpressionSuite{})
}

func (aes *aliasExpressionSuite) TestClone() {
	ae := exp.NewAliasExpression(exp.NewIdentifierExpression("", "", "col"), "c")
	aes.Equal(ae, ae.Clone())
}

func (aes *aliasExpressionSuite) TestExpression() {
	ae := exp.NewAliasExpression(exp.NewIdentifierExpression("", "", "col"), "c")
	aes.Equal(ae, ae.Expression())
}

func (aes *aliasExpressionSuite) TestAliased() {
	ident := exp.NewIdentifierExpression("", "", "col")
	ae := exp.NewAliasExpression(ident, "c")
	aes.Equal(ident, ae.Aliased())
}

func (aes *aliasExpressionSuite) TestGetAs() {
	ae := exp.NewAliasExpression(exp.NewIdentifierExpression("", "", "col"), "c")
	aes.Equal(exp.NewIdentifierExpression("", "", "c"), ae.GetAs())
}

func (aes *aliasExpressionSuite) TestSchema() {
	si := exp.NewAliasExpression(
		exp.NewIdentifierExpression("", "t", nil),
		exp.NewIdentifierExpression("", "t", nil),
	).Schema("s")
	aes.Equal(exp.NewIdentifierExpression("s", "t", nil), si)
}

func (aes *aliasExpressionSuite) TestTable() {
	si := exp.NewAliasExpression(
		exp.NewIdentifierExpression("schema", "", nil),
		exp.NewIdentifierExpression("s", "", nil),
	).Table("t")
	aes.Equal(exp.NewIdentifierExpression("s", "t", nil), si)
}

func (aes *aliasExpressionSuite) TestCol() {
	si := exp.NewAliasExpression(
		exp.NewIdentifierExpression("", "table", nil),
		exp.NewIdentifierExpression("", "t", nil),
	).Col("c")
	aes.Equal(exp.NewIdentifierExpression("", "t", "c"), si)
}

func (aes *aliasExpressionSuite) TestAll() {
	si := exp.NewAliasExpression(
		exp.NewIdentifierExpression("", "table", nil),
		exp.NewIdentifierExpression("", "t", nil),
	).All()
	aes.Equal(exp.NewIdentifierExpression("", "t", exp.Star()), si)
}
