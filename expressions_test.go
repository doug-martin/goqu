package goqu

import (
	"testing"

	"github.com/doug-martin/goqu/v9/exp"
	"github.com/stretchr/testify/suite"
)

type (
	goquExpressionsSuite struct {
		suite.Suite
	}
)

func (ges *goquExpressionsSuite) TestCast() {
	ges.Equal(exp.NewCastExpression(C("test"), "string"), Cast(C("test"), "string"))
}

func (ges *goquExpressionsSuite) TestDoNothing() {
	ges.Equal(exp.NewDoNothingConflictExpression(), DoNothing())
}

func (ges *goquExpressionsSuite) TestDoUpdate() {
	ges.Equal(exp.NewDoUpdateConflictExpression("test", Record{"a": "b"}), DoUpdate("test", Record{"a": "b"}))
}

func (ges *goquExpressionsSuite) TestOr() {
	e1 := C("a").Eq("b")
	e2 := C("b").Eq(2)
	ges.Equal(exp.NewExpressionList(exp.OrType, e1, e2), Or(e1, e2))
}

func (ges *goquExpressionsSuite) TestAnd() {
	e1 := C("a").Eq("b")
	e2 := C("b").Eq(2)
	ges.Equal(exp.NewExpressionList(exp.AndType, e1, e2), And(e1, e2))
}

func (ges *goquExpressionsSuite) TestFunc() {
	ges.Equal(exp.NewSQLFunctionExpression("count", L("*")), Func("count", L("*")))
}

func (ges *goquExpressionsSuite) TestDISTINCT() {
	ges.Equal(exp.NewSQLFunctionExpression("DISTINCT", I("col")), DISTINCT("col"))
}

func (ges *goquExpressionsSuite) TestCOUNT() {
	ges.Equal(exp.NewSQLFunctionExpression("COUNT", I("col")), COUNT("col"))
}

func (ges *goquExpressionsSuite) TestMIN() {
	ges.Equal(exp.NewSQLFunctionExpression("MIN", I("col")), MIN("col"))
}

func (ges *goquExpressionsSuite) TestMAX() {
	ges.Equal(exp.NewSQLFunctionExpression("MAX", I("col")), MAX("col"))
}

func (ges *goquExpressionsSuite) TestAVG() {
	ges.Equal(exp.NewSQLFunctionExpression("AVG", I("col")), AVG("col"))
}

func (ges *goquExpressionsSuite) TestFIRST() {
	ges.Equal(exp.NewSQLFunctionExpression("FIRST", I("col")), FIRST("col"))
}

func (ges *goquExpressionsSuite) TestLAST() {
	ges.Equal(exp.NewSQLFunctionExpression("LAST", I("col")), LAST("col"))
}

func (ges *goquExpressionsSuite) TestSUM() {
	ges.Equal(exp.NewSQLFunctionExpression("SUM", I("col")), SUM("col"))
}

func (ges *goquExpressionsSuite) TestCOALESCE() {
	ges.Equal(exp.NewSQLFunctionExpression("COALESCE", I("col"), nil), COALESCE(I("col"), nil))
}

func (ges *goquExpressionsSuite) TestROW_NUMBER() {
	ges.Equal(exp.NewSQLFunctionExpression("ROW_NUMBER"), ROW_NUMBER())
}

func (ges *goquExpressionsSuite) TestRANK() {
	ges.Equal(exp.NewSQLFunctionExpression("RANK"), RANK())
}

func (ges *goquExpressionsSuite) TestDENSE_RANK() {
	ges.Equal(exp.NewSQLFunctionExpression("DENSE_RANK"), DENSE_RANK())
}

func (ges *goquExpressionsSuite) TestPERCENT_RANK() {
	ges.Equal(exp.NewSQLFunctionExpression("PERCENT_RANK"), PERCENT_RANK())
}

func (ges *goquExpressionsSuite) TestCUME_DIST() {
	ges.Equal(exp.NewSQLFunctionExpression("CUME_DIST"), CUME_DIST())
}

func (ges *goquExpressionsSuite) TestNTILE() {
	ges.Equal(exp.NewSQLFunctionExpression("NTILE", 1), NTILE(1))
}

func (ges *goquExpressionsSuite) TestFIRST_VALUE() {
	ges.Equal(exp.NewSQLFunctionExpression("FIRST_VALUE", I("col")), FIRST_VALUE("col"))
}

func (ges *goquExpressionsSuite) TestLAST_VALUE() {
	ges.Equal(exp.NewSQLFunctionExpression("LAST_VALUE", I("col")), LAST_VALUE("col"))
}

func (ges *goquExpressionsSuite) TestNTH_VALUE() {
	ges.Equal(exp.NewSQLFunctionExpression("NTH_VALUE", I("col"), 1), NTH_VALUE("col", 1))
	ges.Equal(exp.NewSQLFunctionExpression("NTH_VALUE", I("col"), 1), NTH_VALUE(C("col"), 1))
}

func (ges *goquExpressionsSuite) TestI() {
	ges.Equal(exp.NewIdentifierExpression("s", "t", "c"), I("s.t.c"))
}

func (ges *goquExpressionsSuite) TestC() {
	ges.Equal(exp.NewIdentifierExpression("", "", "c"), C("c"))
}

func (ges *goquExpressionsSuite) TestS() {
	ges.Equal(exp.NewIdentifierExpression("s", "", ""), S("s"))
}

func (ges *goquExpressionsSuite) TestT() {
	ges.Equal(exp.NewIdentifierExpression("", "t", ""), T("t"))
}

func (ges *goquExpressionsSuite) TestW() {
	ges.Equal(emptyWindow, W())
	ges.Equal(exp.NewWindowExpression(I("a"), nil, nil, nil), W("a"))
	ges.Equal(exp.NewWindowExpression(I("a"), I("b"), nil, nil), W("a", "b"))
	ges.Equal(exp.NewWindowExpression(I("a"), I("b"), nil, nil), W("a", "b", "c"))
}

func (ges *goquExpressionsSuite) TestOn() {
	ges.Equal(exp.NewJoinOnCondition(Ex{"a": "b"}), On(Ex{"a": "b"}))
}

func (ges *goquExpressionsSuite) TestUsing() {
	ges.Equal(exp.NewJoinUsingCondition("a", "b"), Using("a", "b"))
}

func (ges *goquExpressionsSuite) TestL() {
	ges.Equal(exp.NewLiteralExpression("? + ?", 1, 2), L("? + ?", 1, 2))
}

func (ges *goquExpressionsSuite) TestLiteral() {
	ges.Equal(exp.NewLiteralExpression("? + ?", 1, 2), Literal("? + ?", 1, 2))
}

func (ges *goquExpressionsSuite) TestV() {
	ges.Equal(exp.NewLiteralExpression("?", "a"), V("a"))
}

func (ges *goquExpressionsSuite) TestRange() {
	ges.Equal(exp.NewRangeVal("a", "b"), Range("a", "b"))
}

func (ges *goquExpressionsSuite) TestStar() {
	ges.Equal(exp.NewLiteralExpression("*"), Star())
}

func (ges *goquExpressionsSuite) TestDefault() {
	ges.Equal(exp.Default(), Default())
}

func TestGoquExpressions(t *testing.T) {
	suite.Run(t, new(goquExpressionsSuite))
}
