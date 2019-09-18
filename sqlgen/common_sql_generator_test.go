package sqlgen

import (
	"testing"

	"github.com/doug-martin/goqu/v9/exp"
	"github.com/doug-martin/goqu/v9/internal/sb"
	"github.com/stretchr/testify/suite"
)

type (
	commonSQLTestCase struct {
		gen        func(builder sb.SQLBuilder)
		sql        string
		isPrepared bool
		err        string
		args       []interface{}
	}
	commonSQLGeneratorSuite struct {
		baseSQLGeneratorSuite
	}
)

func (csgs *commonSQLGeneratorSuite) assertCases(testCases ...commonSQLTestCase) {
	for _, tc := range testCases {
		b := sb.NewSQLBuilder(tc.isPrepared)
		tc.gen(b)
		switch {
		case len(tc.err) > 0:
			csgs.assertErrorSQL(b, tc.err)
		case tc.isPrepared:
			csgs.assertPreparedSQL(b, tc.sql, tc.args)
		default:
			csgs.assertNotPreparedSQL(b, tc.sql)
		}
	}
}

func (csgs *commonSQLGeneratorSuite) TestReturningSQL() {

	returningGen := func(csgs *commonSQLGenerator) func(sb.SQLBuilder) {
		return func(sb sb.SQLBuilder) {
			csgs.ReturningSQL(sb, exp.NewColumnListExpression("a", "b"))
		}
	}

	returningNoColsGen := func(csgs *commonSQLGenerator) func(sb.SQLBuilder) {
		return func(sb sb.SQLBuilder) {
			csgs.ReturningSQL(sb, exp.NewColumnListExpression())
		}
	}

	returningNilExpGen := func(csgs *commonSQLGenerator) func(sb.SQLBuilder) {
		return func(sb sb.SQLBuilder) {
			csgs.ReturningSQL(sb, nil)
		}
	}

	opts := DefaultDialectOptions()
	opts.SupportsReturn = true
	csgs1 := newCommonSQLGenerator("test", opts)

	opts2 := DefaultDialectOptions()
	opts2.SupportsReturn = false
	csgs2 := newCommonSQLGenerator("test", opts2)

	csgs.assertCases(
		commonSQLTestCase{gen: returningGen(csgs1), sql: ` RETURNING "a", "b"`},
		commonSQLTestCase{gen: returningGen(csgs1), sql: ` RETURNING "a", "b"`, isPrepared: true, args: emptyArgs},

		commonSQLTestCase{gen: returningNoColsGen(csgs1), sql: ``},
		commonSQLTestCase{gen: returningNoColsGen(csgs1), sql: ``, isPrepared: true, args: emptyArgs},

		commonSQLTestCase{gen: returningNilExpGen(csgs1), sql: ``},
		commonSQLTestCase{gen: returningNilExpGen(csgs1), sql: ``, isPrepared: true, args: emptyArgs},

		commonSQLTestCase{gen: returningGen(csgs2), err: `goqu: dialect does not support RETURNING clause [dialect=test]`},
		commonSQLTestCase{gen: returningGen(csgs2), err: `goqu: dialect does not support RETURNING clause [dialect=test]`},
	)
}

func (csgs *commonSQLGeneratorSuite) TestFromSQL() {
	fromGen := func(csgs *commonSQLGenerator) func(sb.SQLBuilder) {
		return func(sb sb.SQLBuilder) {
			csgs.FromSQL(sb, exp.NewColumnListExpression("a", "b"))
		}
	}

	fromNoColsGen := func(csgs *commonSQLGenerator) func(sb.SQLBuilder) {
		return func(sb sb.SQLBuilder) {
			csgs.FromSQL(sb, exp.NewColumnListExpression())
		}
	}

	fromNilExpGen := func(csgs *commonSQLGenerator) func(sb.SQLBuilder) {
		return func(sb sb.SQLBuilder) {
			csgs.FromSQL(sb, nil)
		}
	}

	csg := newCommonSQLGenerator("test", DefaultDialectOptions())

	opts := DefaultDialectOptions()
	opts.FromFragment = []byte(" from")
	csgFromFrag := newCommonSQLGenerator("test", opts)

	csgs.assertCases(
		commonSQLTestCase{gen: fromGen(csg), sql: ` FROM "a", "b"`},
		commonSQLTestCase{gen: fromGen(csg), sql: ` FROM "a", "b"`, isPrepared: true, args: emptyArgs},

		commonSQLTestCase{gen: fromNoColsGen(csg), sql: ``},
		commonSQLTestCase{gen: fromNoColsGen(csg), sql: ``, isPrepared: true, args: emptyArgs},

		commonSQLTestCase{gen: fromNilExpGen(csg), sql: ``},
		commonSQLTestCase{gen: fromNilExpGen(csg), sql: ``, isPrepared: true, args: emptyArgs},

		commonSQLTestCase{gen: fromGen(csgFromFrag), sql: ` from "a", "b"`},
		commonSQLTestCase{gen: fromGen(csgFromFrag), sql: ` from "a", "b"`, isPrepared: true, args: emptyArgs},

		commonSQLTestCase{gen: fromNoColsGen(csgFromFrag), sql: ``},
		commonSQLTestCase{gen: fromNoColsGen(csgFromFrag), sql: ``, isPrepared: true, args: emptyArgs},

		commonSQLTestCase{gen: fromNilExpGen(csgFromFrag), sql: ``},
		commonSQLTestCase{gen: fromNilExpGen(csgFromFrag), sql: ``, isPrepared: true, args: emptyArgs},
	)
}

func (csgs *commonSQLGeneratorSuite) TestWhereSQL() {

	whereAndGen := func(csgs *commonSQLGenerator, exps ...exp.Expression) func(sb.SQLBuilder) {
		return func(sb sb.SQLBuilder) {
			csgs.WhereSQL(sb, exp.NewExpressionList(exp.AndType, exps...))
		}
	}

	whereOrGen := func(csgs *commonSQLGenerator, exps ...exp.Expression) func(sb.SQLBuilder) {
		return func(sb sb.SQLBuilder) {
			csgs.WhereSQL(sb, exp.NewExpressionList(exp.OrType, exps...))
		}
	}

	csg := newCommonSQLGenerator("test", DefaultDialectOptions())

	opts := DefaultDialectOptions()
	opts.WhereFragment = []byte(" where ")
	csgWhereFrag := newCommonSQLGenerator("test", opts)

	w := exp.Ex{"a": "b"}
	w2 := exp.Ex{"b": "c"}

	csgs.assertCases(
		commonSQLTestCase{gen: whereAndGen(csg), sql: ``},
		commonSQLTestCase{gen: whereAndGen(csg), sql: ``, isPrepared: true, args: emptyArgs},

		commonSQLTestCase{gen: whereAndGen(csg, w), sql: ` WHERE ("a" = 'b')`},
		commonSQLTestCase{gen: whereAndGen(csg, w), sql: ` WHERE ("a" = ?)`, isPrepared: true, args: []interface{}{"b"}},

		commonSQLTestCase{gen: whereAndGen(csg, w, w2), sql: ` WHERE (("a" = 'b') AND ("b" = 'c'))`},
		commonSQLTestCase{gen: whereAndGen(csg, w, w2), sql: ` WHERE (("a" = ?) AND ("b" = ?))`, isPrepared: true, args: []interface{}{"b", "c"}},

		commonSQLTestCase{gen: whereOrGen(csg), sql: ``},
		commonSQLTestCase{gen: whereOrGen(csg), sql: ``, isPrepared: true, args: emptyArgs},

		commonSQLTestCase{gen: whereOrGen(csg, w), sql: ` WHERE ("a" = 'b')`},
		commonSQLTestCase{gen: whereOrGen(csg, w), sql: ` WHERE ("a" = ?)`, isPrepared: true, args: []interface{}{"b"}},

		commonSQLTestCase{gen: whereOrGen(csg, w, w2), sql: ` WHERE (("a" = 'b') OR ("b" = 'c'))`},
		commonSQLTestCase{gen: whereOrGen(csg, w, w2), sql: ` WHERE (("a" = ?) OR ("b" = ?))`, isPrepared: true, args: []interface{}{"b", "c"}},

		commonSQLTestCase{gen: whereAndGen(csgWhereFrag), sql: ``},
		commonSQLTestCase{gen: whereAndGen(csgWhereFrag), sql: ``, isPrepared: true, args: emptyArgs},

		commonSQLTestCase{gen: whereAndGen(csgWhereFrag, w), sql: ` where ("a" = 'b')`},
		commonSQLTestCase{gen: whereAndGen(csgWhereFrag, w), sql: ` where ("a" = ?)`, isPrepared: true, args: []interface{}{"b"}},

		commonSQLTestCase{gen: whereAndGen(csgWhereFrag, w, w2), sql: ` where (("a" = 'b') AND ("b" = 'c'))`},
		commonSQLTestCase{
			gen:        whereAndGen(csgWhereFrag, w, w2),
			sql:        ` where (("a" = ?) AND ("b" = ?))`,
			isPrepared: true,
			args:       []interface{}{"b", "c"},
		},

		commonSQLTestCase{gen: whereOrGen(csgWhereFrag), sql: ``},
		commonSQLTestCase{gen: whereOrGen(csgWhereFrag), sql: ``, isPrepared: true, args: emptyArgs},

		commonSQLTestCase{gen: whereOrGen(csgWhereFrag, w), sql: ` where ("a" = 'b')`},
		commonSQLTestCase{gen: whereOrGen(csgWhereFrag, w), sql: ` where ("a" = ?)`, isPrepared: true, args: []interface{}{"b"}},

		commonSQLTestCase{gen: whereOrGen(csgWhereFrag, w, w2), sql: ` where (("a" = 'b') OR ("b" = 'c'))`},
		commonSQLTestCase{
			gen:        whereOrGen(csgWhereFrag, w, w2),
			sql:        ` where (("a" = ?) OR ("b" = ?))`,
			isPrepared: true,
			args:       []interface{}{"b", "c"},
		},
	)
}

func (csgs *commonSQLGeneratorSuite) TestOrderSQL() {

	orderGen := func(csgs *commonSQLGenerator, o ...exp.OrderedExpression) func(sb.SQLBuilder) {
		return func(sb sb.SQLBuilder) {
			csgs.OrderSQL(sb, exp.NewOrderedColumnList(o...))
		}
	}

	csg := newCommonSQLGenerator("test", DefaultDialectOptions())

	opts := DefaultDialectOptions()
	// override fragments to ensure they are used
	opts.OrderByFragment = []byte(" order by ")
	opts.AscFragment = []byte(" asc")
	opts.DescFragment = []byte(" desc")
	opts.NullsFirstFragment = []byte(" nulls first")
	opts.NullsLastFragment = []byte(" nulls last")
	csgCustom := newCommonSQLGenerator("test", opts)

	ident := exp.NewIdentifierExpression("", "", "a")
	oa := ident.Asc()
	oanf := ident.Asc().NullsFirst()
	oanl := ident.Asc().NullsLast()

	od := ident.Desc()
	odnf := ident.Desc().NullsFirst()
	odnl := ident.Desc().NullsLast()

	csgs.assertCases(
		commonSQLTestCase{gen: orderGen(csg), sql: ``},
		commonSQLTestCase{gen: orderGen(csg), sql: ``, isPrepared: true, args: emptyArgs},

		commonSQLTestCase{gen: orderGen(csg, oa), sql: ` ORDER BY "a" ASC`},
		commonSQLTestCase{gen: orderGen(csg, oa), sql: ` ORDER BY "a" ASC`, isPrepared: true, args: emptyArgs},

		commonSQLTestCase{gen: orderGen(csg, oanf), sql: ` ORDER BY "a" ASC NULLS FIRST`},
		commonSQLTestCase{gen: orderGen(csg, oanf), sql: ` ORDER BY "a" ASC NULLS FIRST`, isPrepared: true, args: emptyArgs},

		commonSQLTestCase{gen: orderGen(csg, oanl), sql: ` ORDER BY "a" ASC NULLS LAST`},
		commonSQLTestCase{gen: orderGen(csg, oanl), sql: ` ORDER BY "a" ASC NULLS LAST`, isPrepared: true, args: emptyArgs},

		commonSQLTestCase{gen: orderGen(csg, od), sql: ` ORDER BY "a" DESC`},
		commonSQLTestCase{gen: orderGen(csg, od), sql: ` ORDER BY "a" DESC`, isPrepared: true, args: emptyArgs},

		commonSQLTestCase{gen: orderGen(csg, odnf), sql: ` ORDER BY "a" DESC NULLS FIRST`},
		commonSQLTestCase{gen: orderGen(csg, odnf), sql: ` ORDER BY "a" DESC NULLS FIRST`, isPrepared: true, args: emptyArgs},

		commonSQLTestCase{gen: orderGen(csg, odnl), sql: ` ORDER BY "a" DESC NULLS LAST`},
		commonSQLTestCase{gen: orderGen(csg, odnl), sql: ` ORDER BY "a" DESC NULLS LAST`, isPrepared: true, args: emptyArgs},

		commonSQLTestCase{gen: orderGen(csg, oa, od), sql: ` ORDER BY "a" ASC, "a" DESC`},
		commonSQLTestCase{gen: orderGen(csg, oa, od), sql: ` ORDER BY "a" ASC, "a" DESC`, isPrepared: true, args: emptyArgs},

		commonSQLTestCase{gen: orderGen(csgCustom), sql: ``},
		commonSQLTestCase{gen: orderGen(csgCustom), sql: ``, isPrepared: true, args: emptyArgs},

		commonSQLTestCase{gen: orderGen(csgCustom, oa), sql: ` order by "a" asc`},
		commonSQLTestCase{gen: orderGen(csgCustom, oa), sql: ` order by "a" asc`, isPrepared: true, args: emptyArgs},

		commonSQLTestCase{gen: orderGen(csgCustom, oanf), sql: ` order by "a" asc nulls first`},
		commonSQLTestCase{gen: orderGen(csgCustom, oanf), sql: ` order by "a" asc nulls first`, isPrepared: true, args: emptyArgs},

		commonSQLTestCase{gen: orderGen(csgCustom, oanl), sql: ` order by "a" asc nulls last`},
		commonSQLTestCase{gen: orderGen(csgCustom, oanl), sql: ` order by "a" asc nulls last`, isPrepared: true, args: emptyArgs},

		commonSQLTestCase{gen: orderGen(csgCustom, od), sql: ` order by "a" desc`},
		commonSQLTestCase{gen: orderGen(csgCustom, od), sql: ` order by "a" desc`, isPrepared: true, args: emptyArgs},

		commonSQLTestCase{gen: orderGen(csgCustom, odnf), sql: ` order by "a" desc nulls first`},
		commonSQLTestCase{gen: orderGen(csgCustom, odnf), sql: ` order by "a" desc nulls first`, isPrepared: true, args: emptyArgs},

		commonSQLTestCase{gen: orderGen(csgCustom, odnl), sql: ` order by "a" desc nulls last`},
		commonSQLTestCase{gen: orderGen(csgCustom, odnl), sql: ` order by "a" desc nulls last`, isPrepared: true, args: emptyArgs},

		commonSQLTestCase{gen: orderGen(csgCustom, oa, od), sql: ` order by "a" asc, "a" desc`},
		commonSQLTestCase{gen: orderGen(csgCustom, oa, od), sql: ` order by "a" asc, "a" desc`, isPrepared: true, args: emptyArgs},
	)
}

func (csgs *commonSQLGeneratorSuite) TestLimitSQL() {
	limitGen := func(csgs *commonSQLGenerator, l interface{}) func(sb.SQLBuilder) {
		return func(sb sb.SQLBuilder) {
			csgs.LimitSQL(sb, l)
		}
	}

	csg := newCommonSQLGenerator("test", DefaultDialectOptions())

	opts := DefaultDialectOptions()
	opts.LimitFragment = []byte(" limit ")
	csgCustom := newCommonSQLGenerator("test", opts)

	l := int64(10)
	la := exp.NewLiteralExpression("ALL")

	csgs.assertCases(
		commonSQLTestCase{gen: limitGen(csg, nil), sql: ``},
		commonSQLTestCase{gen: limitGen(csg, nil), sql: ``, isPrepared: true, args: emptyArgs},

		commonSQLTestCase{gen: limitGen(csg, l), sql: ` LIMIT 10`},
		commonSQLTestCase{gen: limitGen(csg, l), sql: ` LIMIT ?`, isPrepared: true, args: []interface{}{l}},

		commonSQLTestCase{gen: limitGen(csg, la), sql: ` LIMIT ALL`},
		commonSQLTestCase{gen: limitGen(csg, la), sql: ` LIMIT ALL`, isPrepared: true, args: emptyArgs},

		commonSQLTestCase{gen: limitGen(csgCustom, nil), sql: ``},
		commonSQLTestCase{gen: limitGen(csgCustom, nil), sql: ``, isPrepared: true, args: emptyArgs},

		commonSQLTestCase{gen: limitGen(csgCustom, l), sql: ` limit 10`},
		commonSQLTestCase{gen: limitGen(csgCustom, l), sql: ` limit ?`, isPrepared: true, args: []interface{}{l}},

		commonSQLTestCase{gen: limitGen(csgCustom, la), sql: ` limit ALL`},
		commonSQLTestCase{gen: limitGen(csgCustom, la), sql: ` limit ALL`, isPrepared: true, args: emptyArgs},
	)
}

func (csgs *commonSQLGeneratorSuite) TestUpdateExpressionSQL() {
	updateGen := func(csgs *commonSQLGenerator, ues ...exp.UpdateExpression) func(sb.SQLBuilder) {
		return func(sb sb.SQLBuilder) {
			csgs.UpdateExpressionSQL(sb, ues...)
		}
	}

	csg := newCommonSQLGenerator("test", DefaultDialectOptions())
	ue := exp.NewIdentifierExpression("", "", "col").Set("a")
	ue2 := exp.NewIdentifierExpression("", "", "col2").Set("b")

	csgs.assertCases(
		commonSQLTestCase{gen: updateGen(csg), err: errNoUpdatedValuesProvided.Error()},
		commonSQLTestCase{gen: updateGen(csg), err: errNoUpdatedValuesProvided.Error()},

		commonSQLTestCase{gen: updateGen(csg, ue), sql: `"col"='a'`},
		commonSQLTestCase{gen: updateGen(csg, ue), sql: `"col"=?`, isPrepared: true, args: []interface{}{"a"}},

		commonSQLTestCase{gen: updateGen(csg, ue, ue2), sql: `"col"='a',"col2"='b'`},
		commonSQLTestCase{gen: updateGen(csg, ue, ue2), sql: `"col"=?,"col2"=?`, isPrepared: true, args: []interface{}{"a", "b"}},
	)
}

func TestCommonSQLGenerator(t *testing.T) {
	suite.Run(t, new(commonSQLGeneratorSuite))
}
