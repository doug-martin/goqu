package exp

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type exTestSuite struct {
	suite.Suite
}

func TestExSuite(t *testing.T) {
	suite.Run(t, new(exTestSuite))
}

func (ets *exTestSuite) TestExpression() {
	ex := Ex{"a": "b"}
	ets.Equal(ex, ex.Expression())
}

func (ets *exTestSuite) TestClone() {
	ex := Ex{"a": "b"}
	ets.Equal(ex, ex.Clone())
}

func (ets *exTestSuite) TestIsEmpty() {
	ets.False(Ex{"a": "b"}.IsEmpty())
	ets.True(Ex{}.IsEmpty())
}

func (ets *exTestSuite) TestToExpression() {
	ident := NewIdentifierExpression("", "", "a")
	testCases := []struct {
		ExMap Ex
		El    ExpressionList
		Err   string
	}{
		{
			ExMap: Ex{"a": "b"},
			El:    NewExpressionList(AndType, ident.Eq("b")),
		},
		{
			ExMap: Ex{"a": "b", "b": "c"},
			El: NewExpressionList(
				AndType,
				ident.Eq("b"),
				NewIdentifierExpression("", "", "b").Eq("c"),
			),
		},
		{
			ExMap: Ex{"a": Op{"eq": "b"}},
			El:    NewExpressionList(AndType, NewExpressionList(OrType, ident.Eq("b"))),
		},
		{
			ExMap: Ex{"a": Op{"neq": "b"}},
			El:    NewExpressionList(AndType, NewExpressionList(OrType, ident.Neq("b"))),
		},
		{
			ExMap: Ex{"a": Op{"is": nil}},
			El:    NewExpressionList(AndType, NewExpressionList(OrType, ident.Is(nil))),
		},
		{
			ExMap: Ex{"a": Op{"isNot": nil}},
			El:    NewExpressionList(AndType, NewExpressionList(OrType, ident.IsNot(nil))),
		},
		{
			ExMap: Ex{"a": Op{"gt": "b"}},
			El:    NewExpressionList(AndType, NewExpressionList(OrType, ident.Gt("b"))),
		},
		{
			ExMap: Ex{"a": Op{"gte": "b"}},
			El:    NewExpressionList(AndType, NewExpressionList(OrType, ident.Gte("b"))),
		},
		{
			ExMap: Ex{"a": Op{"lt": "b"}},
			El:    NewExpressionList(AndType, NewExpressionList(OrType, ident.Lt("b"))),
		},
		{
			ExMap: Ex{"a": Op{"lte": "b"}},
			El:    NewExpressionList(AndType, NewExpressionList(OrType, ident.Lte("b"))),
		},
		{
			ExMap: Ex{"a": Op{"in": "b"}},
			El:    NewExpressionList(AndType, NewExpressionList(OrType, ident.In("b"))),
		},
		{
			ExMap: Ex{"a": Op{"notIn": "b"}},
			El:    NewExpressionList(AndType, NewExpressionList(OrType, ident.NotIn("b"))),
		},
		{
			ExMap: Ex{"a": Op{"like": "b"}},
			El:    NewExpressionList(AndType, NewExpressionList(OrType, ident.Like("b"))),
		},
		{
			ExMap: Ex{"a": Op{"notLike": "b"}},
			El:    NewExpressionList(AndType, NewExpressionList(OrType, ident.NotLike("b"))),
		},
		{
			ExMap: Ex{"a": Op{"iLike": "b"}},
			El:    NewExpressionList(AndType, NewExpressionList(OrType, ident.ILike("b"))),
		},
		{
			ExMap: Ex{"a": Op{"notILike": "b"}},
			El:    NewExpressionList(AndType, NewExpressionList(OrType, ident.NotILike("b"))),
		},
		{
			ExMap: Ex{"a": Op{"regexpLike": "b"}},
			El:    NewExpressionList(AndType, NewExpressionList(OrType, ident.RegexpLike("b"))),
		},
		{
			ExMap: Ex{"a": Op{"regexpNotLike": "b"}},
			El:    NewExpressionList(AndType, NewExpressionList(OrType, ident.RegexpNotLike("b"))),
		},
		{
			ExMap: Ex{"a": Op{"regexpILike": "b"}},
			El:    NewExpressionList(AndType, NewExpressionList(OrType, ident.RegexpILike("b"))),
		},
		{
			ExMap: Ex{"a": Op{"regexpNotILike": "b"}},
			El:    NewExpressionList(AndType, NewExpressionList(OrType, ident.RegexpNotILike("b"))),
		},
		{
			ExMap: Ex{"a": Op{"between": NewRangeVal("a", "z")}},
			El:    NewExpressionList(AndType, NewExpressionList(OrType, ident.Between(NewRangeVal("a", "z")))),
		},
		{
			ExMap: Ex{"a": Op{"notBetween": NewRangeVal("a", "z")}},
			El:    NewExpressionList(AndType, NewExpressionList(OrType, ident.NotBetween(NewRangeVal("a", "z")))),
		},
		{
			ExMap: Ex{"a": Op{"foo": "z"}},
			Err:   "goqu: unsupported expression type foo",
		},
		{
			ExMap: Ex{"a": Op{"eq": "b", "neq": "c", "gt": "m"}},
			El:    NewExpressionList(AndType, NewExpressionList(OrType, ident.Eq("b"), ident.Gt("m"), ident.Neq("c"))),
		},

		{
			ExMap: Ex{
				"a": "b",
				"c": "d",
			},
			El: NewExpressionList(
				AndType,
				ident.Eq("b"),
				NewIdentifierExpression("", "", "c").Eq("d"),
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
	ex := ExOr{"a": "b"}
	ets.Equal(ex, ex.Expression())
}

func (ets *exOrTestSuite) TestClone() {
	ex := ExOr{"a": "b"}
	ets.Equal(ex, ex.Clone())
}

func (ets *exOrTestSuite) TestIsEmpty() {
	ets.False(ExOr{"a": "b"}.IsEmpty())
	ets.True(ExOr{}.IsEmpty())
}

func (ets *exOrTestSuite) TestToExpression() {
	ident := NewIdentifierExpression("", "", "a")
	testCases := []struct {
		ExMap ExOr
		El    ExpressionList
		Err   string
	}{
		{
			ExMap: ExOr{"a": "b"},
			El:    NewExpressionList(OrType, ident.Eq("b")),
		},
		{
			ExMap: ExOr{"a": "b", "b": "c"},
			El: NewExpressionList(
				OrType,
				ident.Eq("b"),
				NewIdentifierExpression("", "", "b").Eq("c"),
			),
		},
		{
			ExMap: ExOr{"a": Op{"eq": "b"}},
			El:    NewExpressionList(OrType, NewExpressionList(OrType, ident.Eq("b"))),
		},
		{
			ExMap: ExOr{"a": Op{"neq": "b"}},
			El:    NewExpressionList(OrType, NewExpressionList(OrType, ident.Neq("b"))),
		},
		{
			ExMap: ExOr{"a": Op{"is": nil}},
			El:    NewExpressionList(OrType, NewExpressionList(OrType, ident.Is(nil))),
		},
		{
			ExMap: ExOr{"a": Op{"isNot": nil}},
			El:    NewExpressionList(OrType, NewExpressionList(OrType, ident.IsNot(nil))),
		},
		{
			ExMap: ExOr{"a": Op{"gt": "b"}},
			El:    NewExpressionList(OrType, NewExpressionList(OrType, ident.Gt("b"))),
		},
		{
			ExMap: ExOr{"a": Op{"gte": "b"}},
			El:    NewExpressionList(OrType, NewExpressionList(OrType, ident.Gte("b"))),
		},
		{
			ExMap: ExOr{"a": Op{"lt": "b"}},
			El:    NewExpressionList(OrType, NewExpressionList(OrType, ident.Lt("b"))),
		},
		{
			ExMap: ExOr{"a": Op{"lte": "b"}},
			El:    NewExpressionList(OrType, NewExpressionList(OrType, ident.Lte("b"))),
		},
		{
			ExMap: ExOr{"a": Op{"in": "b"}},
			El:    NewExpressionList(OrType, NewExpressionList(OrType, ident.In("b"))),
		},
		{
			ExMap: ExOr{"a": Op{"notIn": "b"}},
			El:    NewExpressionList(OrType, NewExpressionList(OrType, ident.NotIn("b"))),
		},
		{
			ExMap: ExOr{"a": Op{"like": "b"}},
			El:    NewExpressionList(OrType, NewExpressionList(OrType, ident.Like("b"))),
		},
		{
			ExMap: ExOr{"a": Op{"notLike": "b"}},
			El:    NewExpressionList(OrType, NewExpressionList(OrType, ident.NotLike("b"))),
		},
		{
			ExMap: ExOr{"a": Op{"iLike": "b"}},
			El:    NewExpressionList(OrType, NewExpressionList(OrType, ident.ILike("b"))),
		},
		{
			ExMap: ExOr{"a": Op{"notILike": "b"}},
			El:    NewExpressionList(OrType, NewExpressionList(OrType, ident.NotILike("b"))),
		},
		{
			ExMap: ExOr{"a": Op{"regexpLike": "b"}},
			El:    NewExpressionList(OrType, NewExpressionList(OrType, ident.RegexpLike("b"))),
		},
		{
			ExMap: ExOr{"a": Op{"regexpNotLike": "b"}},
			El:    NewExpressionList(OrType, NewExpressionList(OrType, ident.RegexpNotLike("b"))),
		},
		{
			ExMap: ExOr{"a": Op{"regexpILike": "b"}},
			El:    NewExpressionList(OrType, NewExpressionList(OrType, ident.RegexpILike("b"))),
		},
		{
			ExMap: ExOr{"a": Op{"regexpNotILike": "b"}},
			El:    NewExpressionList(OrType, NewExpressionList(OrType, ident.RegexpNotILike("b"))),
		},
		{
			ExMap: ExOr{"a": Op{"between": NewRangeVal("a", "z")}},
			El:    NewExpressionList(OrType, NewExpressionList(OrType, ident.Between(NewRangeVal("a", "z")))),
		},
		{
			ExMap: ExOr{"a": Op{"notBetween": NewRangeVal("a", "z")}},
			El:    NewExpressionList(OrType, NewExpressionList(OrType, ident.NotBetween(NewRangeVal("a", "z")))),
		},
		{
			ExMap: ExOr{"a": Op{"foo": "z"}},
			Err:   "goqu: unsupported expression type foo",
		},
		{
			ExMap: ExOr{"a": Op{"eq": "b", "neq": "c", "gt": "m"}},
			El:    NewExpressionList(OrType, NewExpressionList(OrType, ident.Eq("b"), ident.Gt("m"), ident.Neq("c"))),
		},

		{
			ExMap: ExOr{
				"a": "b",
				"c": "d",
			},
			El: NewExpressionList(
				OrType,
				ident.Eq("b"),
				NewIdentifierExpression("", "", "c").Eq("d"),
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
