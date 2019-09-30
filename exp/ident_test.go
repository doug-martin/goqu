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
