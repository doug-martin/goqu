package goqu_test

import (
	"testing"

	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/stretchr/testify/suite"
)

type (
	goquExpressionsSuite struct {
		suite.Suite
	}
)

func (ges *goquExpressionsSuite) TestCast() {
	ges.Equal(exp.NewCastExpression(goqu.C("test"), "string"), goqu.Cast(goqu.C("test"), "string"))
}

func (ges *goquExpressionsSuite) TestDoNothing() {
	ges.Equal(exp.NewDoNothingConflictExpression(), goqu.DoNothing())
}

func (ges *goquExpressionsSuite) TestDoUpdate() {
	ges.Equal(exp.NewDoUpdateConflictExpression("test", goqu.Record{"a": "b"}), goqu.DoUpdate("test", goqu.Record{"a": "b"}))
}

func (ges *goquExpressionsSuite) TestOr() {
	e1 := goqu.C("a").Eq("b")
	e2 := goqu.C("b").Eq(2)
	ges.Equal(exp.NewExpressionList(exp.OrType, e1, e2), goqu.Or(e1, e2))
}

func (ges *goquExpressionsSuite) TestAnd() {
	e1 := goqu.C("a").Eq("b")
	e2 := goqu.C("b").Eq(2)
	ges.Equal(exp.NewExpressionList(exp.AndType, e1, e2), goqu.And(e1, e2))
}

func (ges *goquExpressionsSuite) TestFunc() {
	ges.Equal(exp.NewSQLFunctionExpression("count", goqu.L("*")), goqu.Func("count", goqu.L("*")))
}

func (ges *goquExpressionsSuite) TestDISTINCT() {
	ges.Equal(exp.NewSQLFunctionExpression("DISTINCT", goqu.I("col")), goqu.DISTINCT("col"))
}

func (ges *goquExpressionsSuite) TestCOUNT() {
	ges.Equal(exp.NewSQLFunctionExpression("COUNT", goqu.I("col")), goqu.COUNT("col"))
}

func (ges *goquExpressionsSuite) TestMIN() {
	ges.Equal(exp.NewSQLFunctionExpression("MIN", goqu.I("col")), goqu.MIN("col"))
}

func (ges *goquExpressionsSuite) TestMAX() {
	ges.Equal(exp.NewSQLFunctionExpression("MAX", goqu.I("col")), goqu.MAX("col"))
}

func (ges *goquExpressionsSuite) TestAVG() {
	ges.Equal(exp.NewSQLFunctionExpression("AVG", goqu.I("col")), goqu.AVG("col"))
}

func (ges *goquExpressionsSuite) TestFIRST() {
	ges.Equal(exp.NewSQLFunctionExpression("FIRST", goqu.I("col")), goqu.FIRST("col"))
}

func (ges *goquExpressionsSuite) TestLAST() {
	ges.Equal(exp.NewSQLFunctionExpression("LAST", goqu.I("col")), goqu.LAST("col"))
}

func (ges *goquExpressionsSuite) TestSUM() {
	ges.Equal(exp.NewSQLFunctionExpression("SUM", goqu.I("col")), goqu.SUM("col"))
}

func (ges *goquExpressionsSuite) TestCOALESCE() {
	ges.Equal(exp.NewSQLFunctionExpression("COALESCE", goqu.I("col"), nil), goqu.COALESCE(goqu.I("col"), nil))
}

func (ges *goquExpressionsSuite) TestROW_NUMBER() {
	ges.Equal(exp.NewSQLFunctionExpression("ROW_NUMBER"), goqu.ROW_NUMBER())
}

func (ges *goquExpressionsSuite) TestRANK() {
	ges.Equal(exp.NewSQLFunctionExpression("RANK"), goqu.RANK())
}

func (ges *goquExpressionsSuite) TestDENSE_RANK() {
	ges.Equal(exp.NewSQLFunctionExpression("DENSE_RANK"), goqu.DENSE_RANK())
}

func (ges *goquExpressionsSuite) TestPERCENT_RANK() {
	ges.Equal(exp.NewSQLFunctionExpression("PERCENT_RANK"), goqu.PERCENT_RANK())
}

func (ges *goquExpressionsSuite) TestCUME_DIST() {
	ges.Equal(exp.NewSQLFunctionExpression("CUME_DIST"), goqu.CUME_DIST())
}

func (ges *goquExpressionsSuite) TestNTILE() {
	ges.Equal(exp.NewSQLFunctionExpression("NTILE", 1), goqu.NTILE(1))
}

func (ges *goquExpressionsSuite) TestFIRST_VALUE() {
	ges.Equal(exp.NewSQLFunctionExpression("FIRST_VALUE", goqu.I("col")), goqu.FIRST_VALUE("col"))
}

func (ges *goquExpressionsSuite) TestLAST_VALUE() {
	ges.Equal(exp.NewSQLFunctionExpression("LAST_VALUE", goqu.I("col")), goqu.LAST_VALUE("col"))
}

func (ges *goquExpressionsSuite) TestNTH_VALUE() {
	ges.Equal(exp.NewSQLFunctionExpression("NTH_VALUE", goqu.I("col"), 1), goqu.NTH_VALUE("col", 1))
	ges.Equal(exp.NewSQLFunctionExpression("NTH_VALUE", goqu.I("col"), 1), goqu.NTH_VALUE(goqu.C("col"), 1))
}

func (ges *goquExpressionsSuite) TestI() {
	ges.Equal(exp.NewIdentifierExpression("s", "t", "c"), goqu.I("s.t.c"))
}

func (ges *goquExpressionsSuite) TestC() {
	ges.Equal(exp.NewIdentifierExpression("", "", "c"), goqu.C("c"))
}

func (ges *goquExpressionsSuite) TestS() {
	ges.Equal(exp.NewIdentifierExpression("s", "", ""), goqu.S("s"))
}

func (ges *goquExpressionsSuite) TestT() {
	ges.Equal(exp.NewIdentifierExpression("", "t", ""), goqu.T("t"))
}

func (ges *goquExpressionsSuite) TestW() {
	ges.Equal(exp.NewWindowExpression(nil, nil, nil, nil), goqu.W())
	ges.Equal(exp.NewWindowExpression(goqu.I("a"), nil, nil, nil), goqu.W("a"))
	ges.Equal(exp.NewWindowExpression(goqu.I("a"), goqu.I("b"), nil, nil), goqu.W("a", "b"))
	ges.Equal(exp.NewWindowExpression(goqu.I("a"), goqu.I("b"), nil, nil), goqu.W("a", "b", "c"))
}

func (ges *goquExpressionsSuite) TestOn() {
	ges.Equal(exp.NewJoinOnCondition(goqu.Ex{"a": "b"}), goqu.On(goqu.Ex{"a": "b"}))
}

func (ges *goquExpressionsSuite) TestUsing() {
	ges.Equal(exp.NewJoinUsingCondition("a", "b"), goqu.Using("a", "b"))
}

func (ges *goquExpressionsSuite) TestL() {
	ges.Equal(exp.NewLiteralExpression("? + ?", 1, 2), goqu.L("? + ?", 1, 2))
}

func (ges *goquExpressionsSuite) TestLiteral() {
	ges.Equal(exp.NewLiteralExpression("? + ?", 1, 2), goqu.Literal("? + ?", 1, 2))
}

func (ges *goquExpressionsSuite) TestV() {
	ges.Equal(exp.NewLiteralExpression("?", "a"), goqu.V("a"))
}

func (ges *goquExpressionsSuite) TestRange() {
	ges.Equal(exp.NewRangeVal("a", "b"), goqu.Range("a", "b"))
}

func (ges *goquExpressionsSuite) TestStar() {
	ges.Equal(exp.NewLiteralExpression("*"), goqu.Star())
}

func (ges *goquExpressionsSuite) TestDefault() {
	ges.Equal(exp.Default(), goqu.Default())
}

func (ges *goquExpressionsSuite) TestLateral() {
	ds := goqu.From("test")
	ges.Equal(exp.NewLateralExpression(ds), goqu.Lateral(ds))
}

func (ges *goquExpressionsSuite) TestAny() {
	ds := goqu.From("test").Select("id")
	ges.Equal(exp.NewSQLFunctionExpression("ANY ", ds), goqu.Any(ds))
}

func (ges *goquExpressionsSuite) TestAll() {
	ds := goqu.From("test").Select("id")
	ges.Equal(exp.NewSQLFunctionExpression("ALL ", ds), goqu.All(ds))
}

func TestGoquExpressions(t *testing.T) {
	suite.Run(t, new(goquExpressionsSuite))
}
