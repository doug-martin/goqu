package exp

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type caseExpressionSuite struct {
	suite.Suite
}

func TestCaseExpressionSuite(t *testing.T) {
	suite.Run(t, &caseExpressionSuite{})
}

func (ces *caseExpressionSuite) TestClone() {
	ce := NewCaseExpression()
	ces.Equal(ce, ce.Clone())
}

func (ces *caseExpressionSuite) TestExpression() {
	ce := NewCaseExpression()
	ces.Equal(ce, ce.Expression())
}

func (ces *caseExpressionSuite) TestAs() {
	ce := NewCaseExpression()
	ces.Equal(aliased(ce, "a"), ce.As("a"))
}

func (ces *caseExpressionSuite) TestValue() {
	ce := NewCaseExpression()
	ces.Nil(ce.GetValue())

	ce = NewCaseExpression().Value(NewIdentifierExpression("", "", "a"))
	ces.Equal(NewIdentifierExpression("", "", "a"), ce.GetValue())
}

func (ces *caseExpressionSuite) TestWhen() {
	condition1 := NewIdentifierExpression("", "", "a").Eq(10)
	condition2 := NewIdentifierExpression("", "", "b").Eq(20)
	ce := NewCaseExpression()
	ces.Equal([]CaseWhen{
		NewCaseWhen(condition1, "a"),
		NewCaseWhen(condition2, "b"),
	}, ce.When(condition1, "a").When(condition2, "b").GetWhens())

	ces.Empty(ce.GetWhens())
}

func (ces *caseExpressionSuite) TestElse() {
	ce := NewCaseExpression()
	ces.Equal(NewCaseElse("a"), ce.Else("a").GetElse())

	ces.Nil(ce.GetElse())
}

type caseWhenSuite struct {
	suite.Suite
}

func TestCaseWhenSuite(t *testing.T) {
	suite.Run(t, &caseWhenSuite{})
}

func (cws *caseWhenSuite) TestCondition() {
	ce := NewCaseWhen(true, false)
	cws.Equal(true, ce.Condition())
}

func (cws *caseWhenSuite) TestResult() {
	ce := NewCaseWhen(true, false)
	cws.Equal(false, ce.Result())
}

type caseElseSuite struct {
	suite.Suite
}

func TestCaseElseSuite(t *testing.T) {
	suite.Run(t, &caseElseSuite{})
}

func (ces *caseElseSuite) TestResult() {
	ce := NewCaseElse(false)
	ces.Equal(false, ce.Result())
}
