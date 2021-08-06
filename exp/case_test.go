package exp_test

import (
	"testing"

	"github.com/doug-martin/goqu/v9/exp"
	"github.com/stretchr/testify/suite"
)

type caseExpressionSuite struct {
	suite.Suite
}

func TestCaseExpressionSuite(t *testing.T) {
	suite.Run(t, &caseExpressionSuite{})
}

func (ces *caseExpressionSuite) TestClone() {
	ce := exp.NewCaseExpression()
	ces.Equal(ce, ce.Clone())
}

func (ces *caseExpressionSuite) TestExpression() {
	ce := exp.NewCaseExpression()
	ces.Equal(ce, ce.Expression())
}

func (ces *caseExpressionSuite) TestAs() {
	ce := exp.NewCaseExpression()
	ces.Equal(exp.NewAliasExpression(ce, "a"), ce.As("a"))
}

func (ces *caseExpressionSuite) TestValue() {
	ce := exp.NewCaseExpression()
	ces.Nil(ce.GetValue())

	ce = exp.NewCaseExpression().Value(exp.NewIdentifierExpression("", "", "a"))
	ces.Equal(exp.NewIdentifierExpression("", "", "a"), ce.GetValue())
}

func (ces *caseExpressionSuite) TestWhen() {
	condition1 := exp.NewIdentifierExpression("", "", "a").Eq(10)
	condition2 := exp.NewIdentifierExpression("", "", "b").Eq(20)
	ce := exp.NewCaseExpression()
	ces.Equal([]exp.CaseWhen{
		exp.NewCaseWhen(condition1, "a"),
		exp.NewCaseWhen(condition2, "b"),
	}, ce.When(condition1, "a").When(condition2, "b").GetWhens())

	ces.Empty(ce.GetWhens())
}

func (ces *caseExpressionSuite) TestElse() {
	ce := exp.NewCaseExpression()
	ces.Equal(exp.NewCaseElse("a"), ce.Else("a").GetElse())

	ces.Nil(ce.GetElse())
}

func (ces *caseExpressionSuite) TestAsc() {
	ce := exp.NewCaseExpression()
	ces.Equal(exp.NewOrderedExpression(ce, exp.AscDir, exp.NoNullsSortType), ce.Asc())
}

func (ces *caseExpressionSuite) TestDesc() {
	ce := exp.NewCaseExpression()
	ces.Equal(exp.NewOrderedExpression(ce, exp.DescSortDir, exp.NoNullsSortType), ce.Desc())
}

type caseWhenSuite struct {
	suite.Suite
}

func TestCaseWhenSuite(t *testing.T) {
	suite.Run(t, &caseWhenSuite{})
}

func (cws *caseWhenSuite) TestCondition() {
	ce := exp.NewCaseWhen(true, false)
	cws.Equal(true, ce.Condition())
}

func (cws *caseWhenSuite) TestResult() {
	ce := exp.NewCaseWhen(true, false)
	cws.Equal(false, ce.Result())
}

type caseElseSuite struct {
	suite.Suite
}

func TestCaseElseSuite(t *testing.T) {
	suite.Run(t, &caseElseSuite{})
}

func (ces *caseElseSuite) TestResult() {
	ce := exp.NewCaseElse(false)
	ces.Equal(false, ce.Result())
}
