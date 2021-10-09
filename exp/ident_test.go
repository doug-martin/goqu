package exp_test

import (
	"testing"

	"github.com/doug-martin/goqu/v9/exp"
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
		Expected exp.IdentifierExpression
	}{
		{ToParse: "one", Expected: exp.NewIdentifierExpression("", "", "one")},
		{ToParse: "one.two", Expected: exp.NewIdentifierExpression("", "one", "two")},
		{ToParse: "one.two.three", Expected: exp.NewIdentifierExpression("one", "two", "three")},
	}
	for _, tc := range cases {
		ies.Equal(tc.Expected, exp.ParseIdentifier(tc.ToParse))
	}
}

func (ies *identifierExpressionSuite) TestClone() {
	cases := []struct {
		Expected exp.IdentifierExpression
	}{
		{Expected: exp.NewIdentifierExpression("", "", "one")},
		{Expected: exp.NewIdentifierExpression("", "two", "one")},
		{Expected: exp.NewIdentifierExpression("three", "two", "one")},
	}
	for _, tc := range cases {
		ies.Equal(tc.Expected, tc.Expected.Clone())
	}
}

func (ies *identifierExpressionSuite) TestIsQualified() {
	cases := []struct {
		Ident       exp.IdentifierExpression
		IsQualified bool
	}{
		{Ident: exp.NewIdentifierExpression("", "", "col"), IsQualified: false},
		{Ident: exp.NewIdentifierExpression("", "table", ""), IsQualified: false},
		{Ident: exp.NewIdentifierExpression("", "table", nil), IsQualified: false},
		{Ident: exp.NewIdentifierExpression("schema", "", ""), IsQualified: false},
		{Ident: exp.NewIdentifierExpression("schema", "", nil), IsQualified: false},
		{Ident: exp.NewIdentifierExpression("", "table", exp.NewLiteralExpression("*")), IsQualified: true},
		{Ident: exp.NewIdentifierExpression("", "table", "col"), IsQualified: true},
		{Ident: exp.NewIdentifierExpression("schema", "table", nil), IsQualified: true},
		{Ident: exp.NewIdentifierExpression("schema", "table", exp.NewLiteralExpression("*")), IsQualified: true},
		{Ident: exp.NewIdentifierExpression("schema", "table", ""), IsQualified: true},
		{Ident: exp.NewIdentifierExpression("schema", "table", "col"), IsQualified: true},
		{Ident: exp.NewIdentifierExpression("schema", "", "col"), IsQualified: true},
		{Ident: exp.NewIdentifierExpression("schema", "", exp.NewLiteralExpression("*")), IsQualified: true},
	}
	for _, tc := range cases {
		ies.Equal(tc.IsQualified, tc.Ident.IsQualified(), "expected %s IsQualified to be %b", tc.Ident, tc.IsQualified)
	}
}

func (ies *identifierExpressionSuite) TestGetTable() {
	cases := []struct {
		Ident exp.IdentifierExpression
		Table string
	}{
		{Ident: exp.NewIdentifierExpression("", "", "col"), Table: ""},
		{Ident: exp.NewIdentifierExpression("", "table", "col"), Table: "table"},
		{Ident: exp.NewIdentifierExpression("schema", "", "col"), Table: ""},
		{Ident: exp.NewIdentifierExpression("schema", "table", nil), Table: "table"},
		{Ident: exp.NewIdentifierExpression("schema", "table", "col"), Table: "table"},
	}
	for _, tc := range cases {
		ies.Equal(tc.Table, tc.Ident.GetTable())
	}
}

func (ies *identifierExpressionSuite) TestGetSchema() {
	cases := []struct {
		Ident  exp.IdentifierExpression
		Schema string
	}{
		{Ident: exp.NewIdentifierExpression("", "", "col"), Schema: ""},
		{Ident: exp.NewIdentifierExpression("", "table", "col"), Schema: ""},
		{Ident: exp.NewIdentifierExpression("schema", "", "col"), Schema: "schema"},
		{Ident: exp.NewIdentifierExpression("schema", "table", nil), Schema: "schema"},
		{Ident: exp.NewIdentifierExpression("schema", "table", "col"), Schema: "schema"},
	}
	for _, tc := range cases {
		ies.Equal(tc.Schema, tc.Ident.GetSchema())
	}
}

func (ies *identifierExpressionSuite) TestGetCol() {
	cases := []struct {
		Ident exp.IdentifierExpression
		Col   interface{}
	}{
		{Ident: exp.NewIdentifierExpression("", "", "col"), Col: "col"},
		{Ident: exp.NewIdentifierExpression("", "", "*"), Col: exp.NewLiteralExpression("*")},
		{Ident: exp.NewIdentifierExpression("", "table", "col"), Col: "col"},
		{Ident: exp.NewIdentifierExpression("schema", "", "col"), Col: "col"},
		{Ident: exp.NewIdentifierExpression("schema", "table", nil), Col: nil},
		{Ident: exp.NewIdentifierExpression("schema", "table", "col"), Col: "col"},
	}
	for _, tc := range cases {
		ies.Equal(tc.Col, tc.Ident.GetCol())
	}
}

func (ies *identifierExpressionSuite) TestExpression() {
	i := exp.NewIdentifierExpression("", "", "col")
	ies.Equal(i, i.Expression())
}

func (ies *identifierExpressionSuite) TestAll() {
	cases := []struct {
		Ident exp.IdentifierExpression
	}{
		{Ident: exp.NewIdentifierExpression("", "", "col")},
		{Ident: exp.NewIdentifierExpression("", "table", "col")},
		{Ident: exp.NewIdentifierExpression("schema", "table", "col")},
		{Ident: exp.NewIdentifierExpression("", "", nil)},
		{Ident: exp.NewIdentifierExpression("", "table", nil)},
		{Ident: exp.NewIdentifierExpression("schema", "table", nil)},
	}
	for _, tc := range cases {
		ies.Equal(
			exp.NewIdentifierExpression(tc.Ident.GetSchema(), tc.Ident.GetTable(), exp.NewLiteralExpression("*")),
			tc.Ident.All(),
		)
	}
}

func (ies *identifierExpressionSuite) TestIsEmpty() {
	cases := []struct {
		Ident   exp.IdentifierExpression
		IsEmpty bool
	}{
		{Ident: exp.NewIdentifierExpression("", "", ""), IsEmpty: true},
		{Ident: exp.NewIdentifierExpression("", "", nil), IsEmpty: true},
		{Ident: exp.NewIdentifierExpression("", "", "col"), IsEmpty: false},
		{Ident: exp.NewIdentifierExpression("", "", exp.NewLiteralExpression("*")), IsEmpty: false},
		{Ident: exp.NewIdentifierExpression("", "table", ""), IsEmpty: false},
		{Ident: exp.NewIdentifierExpression("", "table", nil), IsEmpty: false},
		{Ident: exp.NewIdentifierExpression("schema", "", ""), IsEmpty: false},
		{Ident: exp.NewIdentifierExpression("schema", "", nil), IsEmpty: false},
		{Ident: exp.NewIdentifierExpression("", "table", exp.NewLiteralExpression("*")), IsEmpty: false},
		{Ident: exp.NewIdentifierExpression("", "table", "col"), IsEmpty: false},
		{Ident: exp.NewIdentifierExpression("schema", "table", nil), IsEmpty: false},
		{Ident: exp.NewIdentifierExpression("schema", "table", exp.NewLiteralExpression("*")), IsEmpty: false},
		{Ident: exp.NewIdentifierExpression("schema", "table", ""), IsEmpty: false},
		{Ident: exp.NewIdentifierExpression("schema", "table", "col"), IsEmpty: false},
		{Ident: exp.NewIdentifierExpression("schema", "", "col"), IsEmpty: false},
		{Ident: exp.NewIdentifierExpression("schema", "", exp.NewLiteralExpression("*")), IsEmpty: false},
	}
	for _, tc := range cases {
		ies.Equal(tc.IsEmpty, tc.Ident.IsEmpty(), "expected %s IsEmpty to be %b", tc.Ident, tc.IsEmpty)
	}
}

func (ies *identifierExpressionSuite) TestAs() {
	cases := []struct {
		Alias    exp.AliasedExpression
		Expected exp.Expression
	}{
		{
			Alias:    exp.NewIdentifierExpression("", "", "col").As("c"),
			Expected: exp.NewAliasExpression(exp.NewIdentifierExpression("", "", "col"), exp.NewIdentifierExpression("", "", "c")),
		},
		{
			Alias:    exp.NewIdentifierExpression("", "table", nil).As("t"),
			Expected: exp.NewAliasExpression(exp.NewIdentifierExpression("", "table", nil), exp.NewIdentifierExpression("", "t", nil)),
		},
		{
			Alias:    exp.NewIdentifierExpression("", "table", nil).As("s.t"),
			Expected: exp.NewAliasExpression(exp.NewIdentifierExpression("", "table", nil), exp.NewIdentifierExpression("", "t", nil)),
		},
		{
			Alias:    exp.NewIdentifierExpression("schema", "", nil).As("s"),
			Expected: exp.NewAliasExpression(exp.NewIdentifierExpression("schema", "", nil), exp.NewIdentifierExpression("s", "", nil)),
		},
	}
	for _, tc := range cases {
		ies.Equal(tc.Expected, tc.Alias)
	}
}

func (ies *identifierExpressionSuite) TestAllOthers() {
	ident := exp.NewIdentifierExpression("", "", "a")
	rv := exp.NewRangeVal(1, 2)
	pattern := "ident like%"
	inVals := []interface{}{1, 2}
	bitwiseVals := 2
	testCases := []struct {
		Ex       exp.Expression
		Expected exp.Expression
	}{
		{Ex: ident.As("a"), Expected: exp.NewAliasExpression(ident, "a")},
		{Ex: ident.Eq(1), Expected: exp.NewBooleanExpression(exp.EqOp, ident, 1)},
		{Ex: ident.Neq(1), Expected: exp.NewBooleanExpression(exp.NeqOp, ident, 1)},
		{Ex: ident.Gt(1), Expected: exp.NewBooleanExpression(exp.GtOp, ident, 1)},
		{Ex: ident.Gte(1), Expected: exp.NewBooleanExpression(exp.GteOp, ident, 1)},
		{Ex: ident.Lt(1), Expected: exp.NewBooleanExpression(exp.LtOp, ident, 1)},
		{Ex: ident.Lte(1), Expected: exp.NewBooleanExpression(exp.LteOp, ident, 1)},
		{Ex: ident.Asc(), Expected: exp.NewOrderedExpression(ident, exp.AscDir, exp.NoNullsSortType)},
		{Ex: ident.Desc(), Expected: exp.NewOrderedExpression(ident, exp.DescSortDir, exp.NoNullsSortType)},
		{Ex: ident.Between(rv), Expected: exp.NewRangeExpression(exp.BetweenOp, ident, rv)},
		{Ex: ident.NotBetween(rv), Expected: exp.NewRangeExpression(exp.NotBetweenOp, ident, rv)},
		{Ex: ident.Like(pattern), Expected: exp.NewBooleanExpression(exp.LikeOp, ident, pattern)},
		{Ex: ident.NotLike(pattern), Expected: exp.NewBooleanExpression(exp.NotLikeOp, ident, pattern)},
		{Ex: ident.ILike(pattern), Expected: exp.NewBooleanExpression(exp.ILikeOp, ident, pattern)},
		{Ex: ident.NotILike(pattern), Expected: exp.NewBooleanExpression(exp.NotILikeOp, ident, pattern)},
		{Ex: ident.RegexpLike(pattern), Expected: exp.NewBooleanExpression(exp.RegexpLikeOp, ident, pattern)},
		{Ex: ident.RegexpNotLike(pattern), Expected: exp.NewBooleanExpression(exp.RegexpNotLikeOp, ident, pattern)},
		{Ex: ident.RegexpILike(pattern), Expected: exp.NewBooleanExpression(exp.RegexpILikeOp, ident, pattern)},
		{Ex: ident.RegexpNotILike(pattern), Expected: exp.NewBooleanExpression(exp.RegexpNotILikeOp, ident, pattern)},
		{Ex: ident.In(inVals), Expected: exp.NewBooleanExpression(exp.InOp, ident, inVals)},
		{Ex: ident.NotIn(inVals), Expected: exp.NewBooleanExpression(exp.NotInOp, ident, inVals)},
		{Ex: ident.Is(true), Expected: exp.NewBooleanExpression(exp.IsOp, ident, true)},
		{Ex: ident.IsNot(true), Expected: exp.NewBooleanExpression(exp.IsNotOp, ident, true)},
		{Ex: ident.IsNull(), Expected: exp.NewBooleanExpression(exp.IsOp, ident, nil)},
		{Ex: ident.IsNotNull(), Expected: exp.NewBooleanExpression(exp.IsNotOp, ident, nil)},
		{Ex: ident.IsTrue(), Expected: exp.NewBooleanExpression(exp.IsOp, ident, true)},
		{Ex: ident.IsNotTrue(), Expected: exp.NewBooleanExpression(exp.IsNotOp, ident, true)},
		{Ex: ident.IsFalse(), Expected: exp.NewBooleanExpression(exp.IsOp, ident, false)},
		{Ex: ident.IsNotFalse(), Expected: exp.NewBooleanExpression(exp.IsNotOp, ident, false)},
		{Ex: ident.Distinct(), Expected: exp.NewSQLFunctionExpression("DISTINCT", ident)},
		{Ex: ident.BitwiseInversion(), Expected: exp.NewBitwiseExpression(exp.BitwiseInversionOp, nil, ident)},
		{Ex: ident.BitwiseOr(bitwiseVals), Expected: exp.NewBitwiseExpression(exp.BitwiseOrOp, ident, bitwiseVals)},
		{Ex: ident.BitwiseAnd(bitwiseVals), Expected: exp.NewBitwiseExpression(exp.BitwiseAndOp, ident, bitwiseVals)},
		{Ex: ident.BitwiseXor(bitwiseVals), Expected: exp.NewBitwiseExpression(exp.BitwiseXorOp, ident, bitwiseVals)},
		{Ex: ident.BitwiseLeftShift(bitwiseVals), Expected: exp.NewBitwiseExpression(exp.BitwiseLeftShiftOp, ident, bitwiseVals)},
		{Ex: ident.BitwiseRightShift(bitwiseVals), Expected: exp.NewBitwiseExpression(exp.BitwiseRightShiftOp, ident, bitwiseVals)},
	}

	for _, tc := range testCases {
		ies.Equal(tc.Expected, tc.Ex)
	}
}
