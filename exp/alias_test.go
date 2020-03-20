package exp

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type aliasExpressionSuite struct {
	suite.Suite
}

func TestAliasExpressionSuite(t *testing.T) {
	suite.Run(t, &aliasExpressionSuite{})
}

func (aes *aliasExpressionSuite) TestClone() {
	ae := aliased(NewIdentifierExpression("", "", "col"), "c")
	aes.Equal(ae, ae.Clone())
}

func (aes *aliasExpressionSuite) TestExpression() {
	ae := aliased(NewIdentifierExpression("", "", "col"), "c")
	aes.Equal(ae, ae.Expression())
}

func (aes *aliasExpressionSuite) TestAliased() {
	ident := NewIdentifierExpression("", "", "col")
	ae := aliased(ident, "c")
	aes.Equal(ident, ae.Aliased())
}

func (aes *aliasExpressionSuite) TestGetAs() {
	ae := aliased(NewIdentifierExpression("", "", "col"), "c")
	aes.Equal(NewIdentifierExpression("", "", "c"), ae.GetAs())
}

func (aes *aliasExpressionSuite) TestSchema() {
	si := aliased(
		NewIdentifierExpression("", "t", nil),
		NewIdentifierExpression("", "t", nil),
	).Schema("s")
	aes.Equal(NewIdentifierExpression("s", "t", nil), si)
}

func (aes *aliasExpressionSuite) TestTable() {
	si := aliased(
		NewIdentifierExpression("schema", "", nil),
		NewIdentifierExpression("s", "", nil),
	).Table("t")
	aes.Equal(NewIdentifierExpression("s", "t", nil), si)
}

func (aes *aliasExpressionSuite) TestCol() {
	si := aliased(
		NewIdentifierExpression("", "table", nil),
		NewIdentifierExpression("", "t", nil),
	).Col("c")
	aes.Equal(NewIdentifierExpression("", "t", "c"), si)
}

func (aes *aliasExpressionSuite) TestAll() {
	si := aliased(
		NewIdentifierExpression("", "table", nil),
		NewIdentifierExpression("", "t", nil),
	).All()
	aes.Equal(NewIdentifierExpression("", "t", Star()), si)
}
