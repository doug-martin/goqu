package exp

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type identifierExpressionSuite struct {
	suite.Suite
}

func TestIdentifierExpressionSuite(t *testing.T) {
	suite.Run(t, new(identifierExpressionSuite))
}

func (ies *identifierExpressionSuite) TestParseIdentifier() {
	cases := []struct {
		ToParse  string
		Expected IdentifierExpression
	}{
		{ToParse: "one", Expected: NewIdentifierExpression("", "", "one")},
		{ToParse: "one.two", Expected: NewIdentifierExpression("", "one", "two")},
		{ToParse: "one.two.three", Expected: NewIdentifierExpression("one", "two", "three")},
	}
	for _, tc := range cases {
		ies.Equal(tc.Expected, ParseIdentifier(tc.ToParse))
	}
}

func (ies *identifierExpressionSuite) TestClone() {
	cases := []struct {
		Expected IdentifierExpression
	}{
		{Expected: NewIdentifierExpression("", "", "one")},
		{Expected: NewIdentifierExpression("", "two", "one")},
		{Expected: NewIdentifierExpression("three", "two", "one")},
	}
	for _, tc := range cases {
		ies.Equal(tc.Expected, tc.Expected.Clone())
	}
}

func (ies *identifierExpressionSuite) TestIsQualified() {
	cases := []struct {
		Ident       IdentifierExpression
		IsQualified bool
	}{
		{Ident: NewIdentifierExpression("", "", "col"), IsQualified: false},
		{Ident: NewIdentifierExpression("", "table", ""), IsQualified: false},
		{Ident: NewIdentifierExpression("", "table", nil), IsQualified: false},
		{Ident: NewIdentifierExpression("schema", "", ""), IsQualified: false},
		{Ident: NewIdentifierExpression("schema", "", nil), IsQualified: false},
		{Ident: NewIdentifierExpression("", "table", NewLiteralExpression("*")), IsQualified: true},
		{Ident: NewIdentifierExpression("", "table", "col"), IsQualified: true},
		{Ident: NewIdentifierExpression("schema", "table", nil), IsQualified: true},
		{Ident: NewIdentifierExpression("schema", "table", NewLiteralExpression("*")), IsQualified: true},
		{Ident: NewIdentifierExpression("schema", "table", ""), IsQualified: true},
		{Ident: NewIdentifierExpression("schema", "table", "col"), IsQualified: true},
		{Ident: NewIdentifierExpression("schema", "", "col"), IsQualified: true},
		{Ident: NewIdentifierExpression("schema", "", NewLiteralExpression("*")), IsQualified: true},
	}
	for _, tc := range cases {
		ies.Equal(tc.IsQualified, tc.Ident.IsQualified(), "expected %s IsQualified to be %b", tc.Ident, tc.IsQualified)
	}
}

func (ies *identifierExpressionSuite) TestGetTable() {
	cases := []struct {
		Ident IdentifierExpression
		Table string
	}{
		{Ident: NewIdentifierExpression("", "", "col"), Table: ""},
		{Ident: NewIdentifierExpression("", "table", "col"), Table: "table"},
		{Ident: NewIdentifierExpression("schema", "", "col"), Table: ""},
		{Ident: NewIdentifierExpression("schema", "table", nil), Table: "table"},
		{Ident: NewIdentifierExpression("schema", "table", "col"), Table: "table"},
	}
	for _, tc := range cases {
		ies.Equal(tc.Table, tc.Ident.GetTable())
	}
}

func (ies *identifierExpressionSuite) TestGetSchema() {
	cases := []struct {
		Ident  IdentifierExpression
		Schema string
	}{
		{Ident: NewIdentifierExpression("", "", "col"), Schema: ""},
		{Ident: NewIdentifierExpression("", "table", "col"), Schema: ""},
		{Ident: NewIdentifierExpression("schema", "", "col"), Schema: "schema"},
		{Ident: NewIdentifierExpression("schema", "table", nil), Schema: "schema"},
		{Ident: NewIdentifierExpression("schema", "table", "col"), Schema: "schema"},
	}
	for _, tc := range cases {
		ies.Equal(tc.Schema, tc.Ident.GetSchema())
	}
}

func (ies *identifierExpressionSuite) TestGetCol() {
	cases := []struct {
		Ident IdentifierExpression
		Col   interface{}
	}{
		{Ident: NewIdentifierExpression("", "", "col"), Col: "col"},
		{Ident: NewIdentifierExpression("", "", "*"), Col: NewLiteralExpression("*")},
		{Ident: NewIdentifierExpression("", "table", "col"), Col: "col"},
		{Ident: NewIdentifierExpression("schema", "", "col"), Col: "col"},
		{Ident: NewIdentifierExpression("schema", "table", nil), Col: nil},
		{Ident: NewIdentifierExpression("schema", "table", "col"), Col: "col"},
	}
	for _, tc := range cases {
		ies.Equal(tc.Col, tc.Ident.GetCol())
	}
}

func (ies *identifierExpressionSuite) TestExpression() {
	i := NewIdentifierExpression("", "", "col")
	ies.Equal(i, i.Expression())
}

func (ies *identifierExpressionSuite) TestAll() {
	cases := []struct {
		Ident IdentifierExpression
	}{
		{Ident: NewIdentifierExpression("", "", "col")},
		{Ident: NewIdentifierExpression("", "table", "col")},
		{Ident: NewIdentifierExpression("schema", "table", "col")},
		{Ident: NewIdentifierExpression("", "", nil)},
		{Ident: NewIdentifierExpression("", "table", nil)},
		{Ident: NewIdentifierExpression("schema", "table", nil)},
	}
	for _, tc := range cases {
		ies.Equal(
			NewIdentifierExpression(tc.Ident.GetSchema(), tc.Ident.GetTable(), NewLiteralExpression("*")),
			tc.Ident.All(),
		)
	}
}

func (ies *identifierExpressionSuite) TestIsEmpty() {
	cases := []struct {
		Ident   IdentifierExpression
		IsEmpty bool
	}{
		{Ident: NewIdentifierExpression("", "", ""), IsEmpty: true},
		{Ident: NewIdentifierExpression("", "", nil), IsEmpty: true},
		{Ident: NewIdentifierExpression("", "", "col"), IsEmpty: false},
		{Ident: NewIdentifierExpression("", "", NewLiteralExpression("*")), IsEmpty: false},
		{Ident: NewIdentifierExpression("", "table", ""), IsEmpty: false},
		{Ident: NewIdentifierExpression("", "table", nil), IsEmpty: false},
		{Ident: NewIdentifierExpression("schema", "", ""), IsEmpty: false},
		{Ident: NewIdentifierExpression("schema", "", nil), IsEmpty: false},
		{Ident: NewIdentifierExpression("", "table", NewLiteralExpression("*")), IsEmpty: false},
		{Ident: NewIdentifierExpression("", "table", "col"), IsEmpty: false},
		{Ident: NewIdentifierExpression("schema", "table", nil), IsEmpty: false},
		{Ident: NewIdentifierExpression("schema", "table", NewLiteralExpression("*")), IsEmpty: false},
		{Ident: NewIdentifierExpression("schema", "table", ""), IsEmpty: false},
		{Ident: NewIdentifierExpression("schema", "table", "col"), IsEmpty: false},
		{Ident: NewIdentifierExpression("schema", "", "col"), IsEmpty: false},
		{Ident: NewIdentifierExpression("schema", "", NewLiteralExpression("*")), IsEmpty: false},
	}
	for _, tc := range cases {
		ies.Equal(tc.IsEmpty, tc.Ident.IsEmpty(), "expected %s IsEmpty to be %b", tc.Ident, tc.IsEmpty)
	}
}

func (ies *identifierExpressionSuite) TestAllOthers() {
	ident := NewIdentifierExpression("", "", "a")
	rv := NewRangeVal(1, 2)
	pattern := "ident like%"
	inVals := []interface{}{1, 2}
	testCases := []struct {
		Ex       Expression
		Expected Expression
	}{
		{Ex: ident.As("a"), Expected: aliased(ident, "a")},
		{Ex: ident.Eq(1), Expected: NewBooleanExpression(EqOp, ident, 1)},
		{Ex: ident.Neq(1), Expected: NewBooleanExpression(NeqOp, ident, 1)},
		{Ex: ident.Gt(1), Expected: NewBooleanExpression(GtOp, ident, 1)},
		{Ex: ident.Gte(1), Expected: NewBooleanExpression(GteOp, ident, 1)},
		{Ex: ident.Lt(1), Expected: NewBooleanExpression(LtOp, ident, 1)},
		{Ex: ident.Lte(1), Expected: NewBooleanExpression(LteOp, ident, 1)},
		{Ex: ident.Asc(), Expected: asc(ident)},
		{Ex: ident.Desc(), Expected: desc(ident)},
		{Ex: ident.Between(rv), Expected: between(ident, rv)},
		{Ex: ident.NotBetween(rv), Expected: notBetween(ident, rv)},
		{Ex: ident.Like(pattern), Expected: NewBooleanExpression(LikeOp, ident, pattern)},
		{Ex: ident.NotLike(pattern), Expected: NewBooleanExpression(NotLikeOp, ident, pattern)},
		{Ex: ident.ILike(pattern), Expected: NewBooleanExpression(ILikeOp, ident, pattern)},
		{Ex: ident.NotILike(pattern), Expected: NewBooleanExpression(NotILikeOp, ident, pattern)},
		{Ex: ident.RegexpLike(pattern), Expected: NewBooleanExpression(RegexpLikeOp, ident, pattern)},
		{Ex: ident.RegexpNotLike(pattern), Expected: NewBooleanExpression(RegexpNotLikeOp, ident, pattern)},
		{Ex: ident.RegexpILike(pattern), Expected: NewBooleanExpression(RegexpILikeOp, ident, pattern)},
		{Ex: ident.RegexpNotILike(pattern), Expected: NewBooleanExpression(RegexpNotILikeOp, ident, pattern)},
		{Ex: ident.In(inVals), Expected: NewBooleanExpression(InOp, ident, inVals)},
		{Ex: ident.NotIn(inVals), Expected: NewBooleanExpression(NotInOp, ident, inVals)},
		{Ex: ident.Is(true), Expected: NewBooleanExpression(IsOp, ident, true)},
		{Ex: ident.IsNot(true), Expected: NewBooleanExpression(IsNotOp, ident, true)},
		{Ex: ident.IsNull(), Expected: NewBooleanExpression(IsOp, ident, nil)},
		{Ex: ident.IsNotNull(), Expected: NewBooleanExpression(IsNotOp, ident, nil)},
		{Ex: ident.IsTrue(), Expected: NewBooleanExpression(IsOp, ident, true)},
		{Ex: ident.IsNotTrue(), Expected: NewBooleanExpression(IsNotOp, ident, true)},
		{Ex: ident.IsFalse(), Expected: NewBooleanExpression(IsOp, ident, false)},
		{Ex: ident.IsNotFalse(), Expected: NewBooleanExpression(IsNotOp, ident, false)},
		{Ex: ident.Distinct(), Expected: NewSQLFunctionExpression("DISTINCT", ident)},
	}

	for _, tc := range testCases {
		ies.Equal(tc.Expected, tc.Ex)
	}
}
