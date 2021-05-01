package exp_test

import (
	"testing"

	"github.com/doug-martin/goqu/v9/exp"
	"github.com/stretchr/testify/suite"
)

type exTestSuite struct {
	suite.Suite
}

func TestExSuite(t *testing.T) {
	suite.Run(t, new(exTestSuite))
}

func (ets *exTestSuite) TestExpression() {
	ex := exp.Ex{"a": "b"}
	ets.Equal(ex, ex.Expression())
}

func (ets *exTestSuite) TestClone() {
	ex := exp.Ex{"a": "b"}
	ets.Equal(ex, ex.Clone())
}

func (ets *exTestSuite) TestIsEmpty() {
	ets.False(exp.Ex{"a": "b"}.IsEmpty())
	ets.True(exp.Ex{}.IsEmpty())
}

func (ets *exTestSuite) TestToExpression() {
	ident := exp.NewIdentifierExpression("", "", "a")
	testCases := []struct {
		ExMap exp.Ex
		El    exp.ExpressionList
		Err   string
	}{
		{
			ExMap: exp.Ex{"a": "b"},
			El:    exp.NewExpressionList(exp.AndType, ident.Eq("b")),
		},
		{
			ExMap: exp.Ex{"a": "b", "b": "c"},
			El: exp.NewExpressionList(
				exp.AndType,
				ident.Eq("b"),
				exp.NewIdentifierExpression("", "", "b").Eq("c"),
			),
		},
		{
			ExMap: exp.Ex{"a": exp.Op{"eq": "b"}},
			El:    exp.NewExpressionList(exp.AndType, exp.NewExpressionList(exp.OrType, ident.Eq("b"))),
		},
		{
			ExMap: exp.Ex{"a": exp.Op{"neq": "b"}},
			El:    exp.NewExpressionList(exp.AndType, exp.NewExpressionList(exp.OrType, ident.Neq("b"))),
		},
		{
			ExMap: exp.Ex{"a": exp.Op{"is": nil}},
			El:    exp.NewExpressionList(exp.AndType, exp.NewExpressionList(exp.OrType, ident.Is(nil))),
		},
		{
			ExMap: exp.Ex{"a": exp.Op{"isNot": nil}},
			El:    exp.NewExpressionList(exp.AndType, exp.NewExpressionList(exp.OrType, ident.IsNot(nil))),
		},
		{
			ExMap: exp.Ex{"a": exp.Op{"gt": "b"}},
			El:    exp.NewExpressionList(exp.AndType, exp.NewExpressionList(exp.OrType, ident.Gt("b"))),
		},
		{
			ExMap: exp.Ex{"a": exp.Op{"gte": "b"}},
			El:    exp.NewExpressionList(exp.AndType, exp.NewExpressionList(exp.OrType, ident.Gte("b"))),
		},
		{
			ExMap: exp.Ex{"a": exp.Op{"lt": "b"}},
			El:    exp.NewExpressionList(exp.AndType, exp.NewExpressionList(exp.OrType, ident.Lt("b"))),
		},
		{
			ExMap: exp.Ex{"a": exp.Op{"lte": "b"}},
			El:    exp.NewExpressionList(exp.AndType, exp.NewExpressionList(exp.OrType, ident.Lte("b"))),
		},
		{
			ExMap: exp.Ex{"a": exp.Op{"in": "b"}},
			El:    exp.NewExpressionList(exp.AndType, exp.NewExpressionList(exp.OrType, ident.In("b"))),
		},
		{
			ExMap: exp.Ex{"a": exp.Op{"notIn": "b"}},
			El:    exp.NewExpressionList(exp.AndType, exp.NewExpressionList(exp.OrType, ident.NotIn("b"))),
		},
		{
			ExMap: exp.Ex{"a": exp.Op{"like": "b"}},
			El:    exp.NewExpressionList(exp.AndType, exp.NewExpressionList(exp.OrType, ident.Like("b"))),
		},
		{
			ExMap: exp.Ex{"a": exp.Op{"notLike": "b"}},
			El:    exp.NewExpressionList(exp.AndType, exp.NewExpressionList(exp.OrType, ident.NotLike("b"))),
		},
		{
			ExMap: exp.Ex{"a": exp.Op{"iLike": "b"}},
			El:    exp.NewExpressionList(exp.AndType, exp.NewExpressionList(exp.OrType, ident.ILike("b"))),
		},
		{
			ExMap: exp.Ex{"a": exp.Op{"notILike": "b"}},
			El:    exp.NewExpressionList(exp.AndType, exp.NewExpressionList(exp.OrType, ident.NotILike("b"))),
		},
		{
			ExMap: exp.Ex{"a": exp.Op{"regexpLike": "b"}},
			El:    exp.NewExpressionList(exp.AndType, exp.NewExpressionList(exp.OrType, ident.RegexpLike("b"))),
		},
		{
			ExMap: exp.Ex{"a": exp.Op{"regexpNotLike": "b"}},
			El:    exp.NewExpressionList(exp.AndType, exp.NewExpressionList(exp.OrType, ident.RegexpNotLike("b"))),
		},
		{
			ExMap: exp.Ex{"a": exp.Op{"regexpILike": "b"}},
			El:    exp.NewExpressionList(exp.AndType, exp.NewExpressionList(exp.OrType, ident.RegexpILike("b"))),
		},
		{
			ExMap: exp.Ex{"a": exp.Op{"regexpNotILike": "b"}},
			El:    exp.NewExpressionList(exp.AndType, exp.NewExpressionList(exp.OrType, ident.RegexpNotILike("b"))),
		},
		{
			ExMap: exp.Ex{"a": exp.Op{"between": exp.NewRangeVal("a", "z")}},
			El:    exp.NewExpressionList(exp.AndType, exp.NewExpressionList(exp.OrType, ident.Between(exp.NewRangeVal("a", "z")))),
		},
		{
			ExMap: exp.Ex{"a": exp.Op{"notBetween": exp.NewRangeVal("a", "z")}},
			El:    exp.NewExpressionList(exp.AndType, exp.NewExpressionList(exp.OrType, ident.NotBetween(exp.NewRangeVal("a", "z")))),
		},
		{
			ExMap: exp.Ex{"a": exp.Op{"foo": "z"}},
			Err:   "goqu: unsupported expression type foo",
		},
		{
			ExMap: exp.Ex{"a": exp.Op{"eq": "b", "neq": "c", "gt": "m"}},
			El:    exp.NewExpressionList(exp.AndType, exp.NewExpressionList(exp.OrType, ident.Eq("b"), ident.Gt("m"), ident.Neq("c"))),
		},

		{
			ExMap: exp.Ex{
				"a": "b",
				"c": "d",
			},
			El: exp.NewExpressionList(
				exp.AndType,
				ident.Eq("b"),
				exp.NewIdentifierExpression("", "", "c").Eq("d"),
			),
		},
	}

	for _, tc := range testCases {
		el, err := tc.ExMap.ToExpressions()

		if tc.Err == "" {
			ets.NoError(err)
			ets.Equal(tc.El, el, "For Ex %v", tc.ExMap)
		} else {
			ets.EqualError(err, tc.Err)
		}
	}
}

type exOrTestSuite struct {
	suite.Suite
}

func TestExOrSuite(t *testing.T) {
	suite.Run(t, new(exOrTestSuite))
}

func (ets *exOrTestSuite) TestExpression() {
	ex := exp.ExOr{"a": "b"}
	ets.Equal(ex, ex.Expression())
}

func (ets *exOrTestSuite) TestClone() {
	ex := exp.ExOr{"a": "b"}
	ets.Equal(ex, ex.Clone())
}

func (ets *exOrTestSuite) TestIsEmpty() {
	ets.False(exp.ExOr{"a": "b"}.IsEmpty())
	ets.True(exp.ExOr{}.IsEmpty())
}

func (ets *exOrTestSuite) TestToExpression() {
	ident := exp.NewIdentifierExpression("", "", "a")
	testCases := []struct {
		ExMap exp.ExOr
		El    exp.ExpressionList
		Err   string
	}{
		{
			ExMap: exp.ExOr{"a": "b"},
			El:    exp.NewExpressionList(exp.OrType, ident.Eq("b")),
		},
		{
			ExMap: exp.ExOr{"a": "b", "b": "c"},
			El: exp.NewExpressionList(
				exp.OrType,
				ident.Eq("b"),
				exp.NewIdentifierExpression("", "", "b").Eq("c"),
			),
		},
		{
			ExMap: exp.ExOr{"a": exp.Op{"eq": "b"}},
			El:    exp.NewExpressionList(exp.OrType, exp.NewExpressionList(exp.OrType, ident.Eq("b"))),
		},
		{
			ExMap: exp.ExOr{"a": exp.Op{"neq": "b"}},
			El:    exp.NewExpressionList(exp.OrType, exp.NewExpressionList(exp.OrType, ident.Neq("b"))),
		},
		{
			ExMap: exp.ExOr{"a": exp.Op{"is": nil}},
			El:    exp.NewExpressionList(exp.OrType, exp.NewExpressionList(exp.OrType, ident.Is(nil))),
		},
		{
			ExMap: exp.ExOr{"a": exp.Op{"isNot": nil}},
			El:    exp.NewExpressionList(exp.OrType, exp.NewExpressionList(exp.OrType, ident.IsNot(nil))),
		},
		{
			ExMap: exp.ExOr{"a": exp.Op{"gt": "b"}},
			El:    exp.NewExpressionList(exp.OrType, exp.NewExpressionList(exp.OrType, ident.Gt("b"))),
		},
		{
			ExMap: exp.ExOr{"a": exp.Op{"gte": "b"}},
			El:    exp.NewExpressionList(exp.OrType, exp.NewExpressionList(exp.OrType, ident.Gte("b"))),
		},
		{
			ExMap: exp.ExOr{"a": exp.Op{"lt": "b"}},
			El:    exp.NewExpressionList(exp.OrType, exp.NewExpressionList(exp.OrType, ident.Lt("b"))),
		},
		{
			ExMap: exp.ExOr{"a": exp.Op{"lte": "b"}},
			El:    exp.NewExpressionList(exp.OrType, exp.NewExpressionList(exp.OrType, ident.Lte("b"))),
		},
		{
			ExMap: exp.ExOr{"a": exp.Op{"in": "b"}},
			El:    exp.NewExpressionList(exp.OrType, exp.NewExpressionList(exp.OrType, ident.In("b"))),
		},
		{
			ExMap: exp.ExOr{"a": exp.Op{"notIn": "b"}},
			El:    exp.NewExpressionList(exp.OrType, exp.NewExpressionList(exp.OrType, ident.NotIn("b"))),
		},
		{
			ExMap: exp.ExOr{"a": exp.Op{"like": "b"}},
			El:    exp.NewExpressionList(exp.OrType, exp.NewExpressionList(exp.OrType, ident.Like("b"))),
		},
		{
			ExMap: exp.ExOr{"a": exp.Op{"notLike": "b"}},
			El:    exp.NewExpressionList(exp.OrType, exp.NewExpressionList(exp.OrType, ident.NotLike("b"))),
		},
		{
			ExMap: exp.ExOr{"a": exp.Op{"iLike": "b"}},
			El:    exp.NewExpressionList(exp.OrType, exp.NewExpressionList(exp.OrType, ident.ILike("b"))),
		},
		{
			ExMap: exp.ExOr{"a": exp.Op{"notILike": "b"}},
			El:    exp.NewExpressionList(exp.OrType, exp.NewExpressionList(exp.OrType, ident.NotILike("b"))),
		},
		{
			ExMap: exp.ExOr{"a": exp.Op{"regexpLike": "b"}},
			El:    exp.NewExpressionList(exp.OrType, exp.NewExpressionList(exp.OrType, ident.RegexpLike("b"))),
		},
		{
			ExMap: exp.ExOr{"a": exp.Op{"regexpNotLike": "b"}},
			El:    exp.NewExpressionList(exp.OrType, exp.NewExpressionList(exp.OrType, ident.RegexpNotLike("b"))),
		},
		{
			ExMap: exp.ExOr{"a": exp.Op{"regexpILike": "b"}},
			El:    exp.NewExpressionList(exp.OrType, exp.NewExpressionList(exp.OrType, ident.RegexpILike("b"))),
		},
		{
			ExMap: exp.ExOr{"a": exp.Op{"regexpNotILike": "b"}},
			El:    exp.NewExpressionList(exp.OrType, exp.NewExpressionList(exp.OrType, ident.RegexpNotILike("b"))),
		},
		{
			ExMap: exp.ExOr{"a": exp.Op{"between": exp.NewRangeVal("a", "z")}},
			El:    exp.NewExpressionList(exp.OrType, exp.NewExpressionList(exp.OrType, ident.Between(exp.NewRangeVal("a", "z")))),
		},
		{
			ExMap: exp.ExOr{"a": exp.Op{"notBetween": exp.NewRangeVal("a", "z")}},
			El:    exp.NewExpressionList(exp.OrType, exp.NewExpressionList(exp.OrType, ident.NotBetween(exp.NewRangeVal("a", "z")))),
		},
		{
			ExMap: exp.ExOr{"a": exp.Op{"foo": "z"}},
			Err:   "goqu: unsupported expression type foo",
		},
		{
			ExMap: exp.ExOr{"a": exp.Op{"eq": "b", "neq": "c", "gt": "m"}},
			El:    exp.NewExpressionList(exp.OrType, exp.NewExpressionList(exp.OrType, ident.Eq("b"), ident.Gt("m"), ident.Neq("c"))),
		},

		{
			ExMap: exp.ExOr{
				"a": "b",
				"c": "d",
			},
			El: exp.NewExpressionList(
				exp.OrType,
				ident.Eq("b"),
				exp.NewIdentifierExpression("", "", "c").Eq("d"),
			),
		},
	}

	for _, tc := range testCases {
		el, err := tc.ExMap.ToExpressions()

		if tc.Err == "" {
			ets.NoError(err)
			ets.Equal(tc.El, el, "For Ex %v", tc.ExMap)
		} else {
			ets.EqualError(err, tc.Err)
		}
	}
}
