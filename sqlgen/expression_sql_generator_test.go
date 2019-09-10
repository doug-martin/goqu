package sqlgen

import (
	"database/sql/driver"
	"fmt"
	"reflect"
	"regexp"
	"testing"
	"time"

	"github.com/doug-martin/goqu/v8/exp"
	"github.com/doug-martin/goqu/v8/internal/errors"
	"github.com/doug-martin/goqu/v8/internal/sb"
	"github.com/stretchr/testify/suite"
)

var emptyArgs = make([]interface{}, 0)

type testAppendableExpression struct {
	exp.AppendableExpression
	sql     string
	args    []interface{}
	err     error
	clauses exp.SelectClauses
}

func newTestAppendableExpression(sql string, args []interface{}, err error, clauses exp.SelectClauses) exp.AppendableExpression {
	if clauses == nil {
		clauses = exp.NewSelectClauses()
	}
	return &testAppendableExpression{sql: sql, args: args, err: err, clauses: clauses}
}

func (tae *testAppendableExpression) Expression() exp.Expression {
	return tae
}

func (tae *testAppendableExpression) GetClauses() exp.SelectClauses {
	return tae.clauses
}

func (tae *testAppendableExpression) Clone() exp.Expression {
	return tae
}

func (tae *testAppendableExpression) AppendSQL(b sb.SQLBuilder) {
	if tae.err != nil {
		b.SetError(tae.err)
		return
	}
	b.WriteStrings(tae.sql)
	if len(tae.args) > 0 {
		b.WriteArg(tae.args...)
	}
}

type (
	expressionTestCase struct {
		val        interface{}
		sql        string
		err        string
		isPrepared bool
		args       []interface{}
	}
	expressionSQLGeneratorSuite struct {
		suite.Suite
	}
)

func (esgs *expressionSQLGeneratorSuite) assertCases(esg ExpressionSQLGenerator, cases ...expressionTestCase) {
	for i, c := range cases {
		b := sb.NewSQLBuilder(c.isPrepared)
		esg.Generate(b, c.val)
		actualSQL, actualArgs, err := b.ToSQL()
		if c.err == "" {
			esgs.NoError(err, "test case %d failed", i)
		} else {
			esgs.EqualError(err, c.err, "test case %d failed", i)
		}
		esgs.Equal(c.sql, actualSQL, "test case %d failed", i)
		if c.isPrepared && c.args != nil || len(c.args) > 0 {
			esgs.Equal(c.args, actualArgs, "test case %d failed", i)
		} else {
			esgs.Empty(actualArgs, "test case %d failed", i)
		}
	}
}

func (esgs *expressionSQLGeneratorSuite) TestDialect() {
	esg := NewExpressionSQLGenerator("test", DefaultDialectOptions())
	esgs.Equal("test", esg.Dialect())
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_ErroredBuilder() {
	esg := NewExpressionSQLGenerator("test", DefaultDialectOptions())
	expectedErr := errors.New("test error")
	b := sb.NewSQLBuilder(false).SetError(expectedErr)
	esg.Generate(b, 1)
	sql, args, err := b.ToSQL()
	esgs.Equal(expectedErr, err)
	esgs.Empty(sql)
	esgs.Empty(args)

	b = sb.NewSQLBuilder(true).SetError(err)
	esg.Generate(b, true)
	sql, args, err = b.ToSQL()
	esgs.Equal(expectedErr, err)
	esgs.Empty(sql)
	esgs.Empty(args)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_Invalid() {
	var b *bool
	esgs.assertCases(
		NewExpressionSQLGenerator("test", DefaultDialectOptions()),
		expressionTestCase{val: b, sql: "NULL"},
		expressionTestCase{val: b, sql: "?", isPrepared: true, args: []interface{}{reflect.ValueOf(nil)}},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_UnsupportedType() {
	type strct struct {
	}
	esgs.assertCases(
		NewExpressionSQLGenerator("test", DefaultDialectOptions()),
		expressionTestCase{val: strct{}, err: "goqu_encode_error: Unable to encode value {}"},
		expressionTestCase{val: strct{}, err: "goqu_encode_error: Unable to encode value {}", isPrepared: true},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_IncludePlaceholderNum() {
	opts := DefaultDialectOptions()
	opts.IncludePlaceholderNum = true
	opts.PlaceHolderRune = '$'
	ex := exp.Ex{
		"a": 1,
		"b": true,
		"c": false,
		"d": []string{"a", "b", "c"},
	}
	esgs.assertCases(
		NewExpressionSQLGenerator("test", opts),
		expressionTestCase{
			val: ex,
			sql: `(("a" = 1) AND ("b" = TRUE) AND ("c" = FALSE) AND ("d" IN ('a', 'b', 'c')))`,
		},
		expressionTestCase{
			val:        ex,
			sql:        `(("a" = $1) AND ("b" = $2) AND ("c" = $3) AND ("d" IN ($4, $5, $6)))`,
			isPrepared: true,
			args:       []interface{}{int64(1), true, false, "a", "b", "c"},
		},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_FloatTypes() {
	var float float64
	esgs.assertCases(
		NewExpressionSQLGenerator("test", DefaultDialectOptions()),
		expressionTestCase{val: float32(10.01), sql: "10.010000228881836"},
		expressionTestCase{val: float32(10.01), sql: "?", isPrepared: true, args: []interface{}{float64(float32(10.01))}},

		expressionTestCase{val: float64(10.01), sql: "10.01"},
		expressionTestCase{val: float64(10.01), sql: "?", isPrepared: true, args: []interface{}{float64(10.01)}},

		expressionTestCase{val: &float, sql: "0"},
		expressionTestCase{val: &float, sql: "?", isPrepared: true, args: []interface{}{float}},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_IntTypes() {
	var i int64
	ints := []interface{}{
		int(10),
		int16(10),
		int32(10),
		int64(10),
		uint(10),
		uint16(10),
		uint32(10),
		uint64(10),
	}
	for _, i := range ints {
		esgs.assertCases(
			NewExpressionSQLGenerator("test", DefaultDialectOptions()),
			expressionTestCase{val: i, sql: "10"},
			expressionTestCase{val: i, sql: "?", isPrepared: true, args: []interface{}{int64(10)}},
		)
	}
	esgs.assertCases(
		NewExpressionSQLGenerator("test", DefaultDialectOptions()),
		expressionTestCase{val: &i, sql: "0"},
		expressionTestCase{val: &i, sql: "?", isPrepared: true, args: []interface{}{i}},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_StringTypes() {
	var str string
	esgs.assertCases(
		NewExpressionSQLGenerator("test", DefaultDialectOptions()),
		expressionTestCase{val: "Hello", sql: "'Hello'"},
		expressionTestCase{val: "Hello", sql: "?", isPrepared: true, args: []interface{}{"Hello"}},

		expressionTestCase{val: "Hello'", sql: "'Hello'''"},
		expressionTestCase{val: "Hello'", sql: "?", isPrepared: true, args: []interface{}{"Hello'"}},

		expressionTestCase{val: &str, sql: "''"},
		expressionTestCase{val: &str, sql: "?", isPrepared: true, args: []interface{}{str}},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_BytesTypes() {
	esgs.assertCases(
		NewExpressionSQLGenerator("test", DefaultDialectOptions()),
		expressionTestCase{val: []byte("Hello"), sql: "'Hello'"},
		expressionTestCase{val: []byte("Hello"), sql: "?", isPrepared: true, args: []interface{}{[]byte("Hello")}},

		expressionTestCase{val: []byte("Hello'"), sql: "'Hello'''"},
		expressionTestCase{val: []byte("Hello'"), sql: "?", isPrepared: true, args: []interface{}{[]byte("Hello'")}},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_BoolTypes() {
	var bl bool
	esgs.assertCases(
		NewExpressionSQLGenerator("test", DefaultDialectOptions()),
		expressionTestCase{val: true, sql: "TRUE"},
		expressionTestCase{val: true, sql: "?", isPrepared: true, args: []interface{}{true}},

		expressionTestCase{val: false, sql: "FALSE"},
		expressionTestCase{val: false, sql: "?", isPrepared: true, args: []interface{}{false}},

		expressionTestCase{val: &bl, sql: "FALSE"},
		expressionTestCase{val: &bl, sql: "?", isPrepared: true, args: []interface{}{bl}},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_TimeTypes() {
	var nt *time.Time

	asiaShanghai, err := time.LoadLocation("Asia/Shanghai")
	esgs.Require().NoError(err)
	testDatas := []time.Time{
		time.Now().UTC(),
		time.Now().In(asiaShanghai),
	}

	for _, n := range testDatas {
		now := n
		esgs.assertCases(
			NewExpressionSQLGenerator("test", DefaultDialectOptions()),
			expressionTestCase{val: now, sql: "'" + now.Format(time.RFC3339Nano) + "'"},
			expressionTestCase{val: now, sql: "?", isPrepared: true, args: []interface{}{now}},

			expressionTestCase{val: &now, sql: "'" + now.Format(time.RFC3339Nano) + "'"},
			expressionTestCase{val: &now, sql: "?", isPrepared: true, args: []interface{}{now}},
		)
	}
	esgs.assertCases(
		NewExpressionSQLGenerator("test", DefaultDialectOptions()),
		expressionTestCase{val: nt, sql: "NULL"},
		expressionTestCase{val: nt, sql: "?", isPrepared: true, args: []interface{}{nt}},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_NilTypes() {
	esgs.assertCases(
		NewExpressionSQLGenerator("test", DefaultDialectOptions()),
		expressionTestCase{val: nil, sql: "NULL"},
		expressionTestCase{val: nil, sql: "?", isPrepared: true, args: []interface{}{nil}},
	)
}

type datasetValuerType struct {
	int int64
	err error
}

func (j datasetValuerType) Value() (driver.Value, error) {
	if j.err != nil {
		return nil, j.err
	}
	return []byte(fmt.Sprintf("Hello World %d", j.int)), nil
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_Valuer() {
	err := errors.New("valuer error")
	esgs.assertCases(
		NewExpressionSQLGenerator("test", DefaultDialectOptions()),
		expressionTestCase{val: datasetValuerType{int: 10}, sql: "'Hello World 10'"},
		expressionTestCase{
			val: datasetValuerType{int: 10}, sql: "?", isPrepared: true, args: []interface{}{[]byte("Hello World 10")},
		},

		expressionTestCase{val: datasetValuerType{err: err}, err: "goqu: valuer error"},
		expressionTestCase{
			val: datasetValuerType{err: err}, isPrepared: true, err: "goqu: valuer error",
		},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_Slice() {
	esgs.assertCases(
		NewExpressionSQLGenerator("test", DefaultDialectOptions()),
		expressionTestCase{val: []string{"a", "b", "c"}, sql: `('a', 'b', 'c')`},
		expressionTestCase{
			val: []string{"a", "b", "c"}, sql: "(?, ?, ?)", isPrepared: true, args: []interface{}{"a", "b", "c"},
		},

		expressionTestCase{val: []byte{'a', 'b', 'c'}, sql: `'abc'`},
		expressionTestCase{
			val: []byte{'a', 'b', 'c'}, sql: "?", isPrepared: true, args: []interface{}{[]byte{'a', 'b', 'c'}},
		},
	)
}

type unknownExpression struct {
}

func (ue unknownExpression) Expression() exp.Expression {
	return ue
}
func (ue unknownExpression) Clone() exp.Expression {
	return ue
}
func (esgs *expressionSQLGeneratorSuite) TestGenerateUnsupportedExpression() {
	errMsg := "goqu: unsupported expression type sqlgen.unknownExpression"
	esgs.assertCases(
		NewExpressionSQLGenerator("test", DefaultDialectOptions()),
		expressionTestCase{val: unknownExpression{}, err: errMsg},
		expressionTestCase{
			val: unknownExpression{}, isPrepared: true, err: errMsg},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_AppendableExpression() {
	ti := exp.NewIdentifierExpression("", "b", "")
	a := newTestAppendableExpression(`select * from "a"`, []interface{}{}, nil, nil)
	aliasedA := newTestAppendableExpression(`select * from "a"`, []interface{}{}, nil, exp.NewSelectClauses().SetAlias(ti))
	argsA := newTestAppendableExpression(`select * from "a" where x=?`, []interface{}{true}, nil, exp.NewSelectClauses().SetAlias(ti))
	ae := newTestAppendableExpression(`select * from "a"`, emptyArgs, errors.New("expected error"), nil)

	esgs.assertCases(
		NewExpressionSQLGenerator("test", DefaultDialectOptions()),
		expressionTestCase{val: a, sql: `(select * from "a")`},
		expressionTestCase{val: a, sql: `(select * from "a")`, isPrepared: true},

		expressionTestCase{val: aliasedA, sql: `(select * from "a") AS "b"`},
		expressionTestCase{val: aliasedA, sql: `(select * from "a") AS "b"`, isPrepared: true},

		expressionTestCase{val: ae, err: "goqu: expected error"},
		expressionTestCase{val: ae, err: "goqu: expected error", isPrepared: true},

		expressionTestCase{val: argsA, sql: `(select * from "a" where x=?) AS "b"`, args: []interface{}{true}},
		expressionTestCase{val: argsA, sql: `(select * from "a" where x=?) AS "b"`, isPrepared: true, args: []interface{}{true}},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_ColumnList() {
	cl := exp.NewColumnListExpression("a", exp.NewLiteralExpression("true"))
	esgs.assertCases(
		NewExpressionSQLGenerator("test", DefaultDialectOptions()),
		expressionTestCase{val: cl, sql: `"a", true`},
		expressionTestCase{val: cl, sql: `"a", true`, isPrepared: true},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_ExpressionList() {

	andEl := exp.NewExpressionList(
		exp.AndType,
		exp.NewIdentifierExpression("", "", "a").Eq("b"),
		exp.NewIdentifierExpression("", "", "c").Neq(1),
	)

	orEl := exp.NewExpressionList(
		exp.OrType,
		exp.NewIdentifierExpression("", "", "a").Eq("b"),
		exp.NewIdentifierExpression("", "", "c").Neq(1),
	)

	andOrEl := exp.NewExpressionList(exp.OrType,
		exp.NewIdentifierExpression("", "", "a").Eq("b"),
		exp.NewExpressionList(exp.AndType,
			exp.NewIdentifierExpression("", "", "c").Neq(1),
			exp.NewIdentifierExpression("", "", "d").Eq(exp.NewLiteralExpression("NOW()")),
		),
	)

	esgs.assertCases(
		NewExpressionSQLGenerator("test", DefaultDialectOptions()),
		expressionTestCase{val: andEl, sql: `(("a" = 'b') AND ("c" != 1))`},
		expressionTestCase{
			val: andEl, sql: `(("a" = ?) AND ("c" != ?))`, isPrepared: true, args: []interface{}{"b", int64(1)},
		},

		expressionTestCase{val: orEl, sql: `(("a" = 'b') OR ("c" != 1))`},
		expressionTestCase{
			val: orEl, sql: `(("a" = ?) OR ("c" != ?))`, isPrepared: true, args: []interface{}{"b", int64(1)},
		},

		expressionTestCase{val: andOrEl, sql: `(("a" = 'b') OR (("c" != 1) AND ("d" = NOW())))`},
		expressionTestCase{
			val:        andOrEl,
			sql:        `(("a" = ?) OR (("c" != ?) AND ("d" = NOW())))`,
			isPrepared: true,
			args:       []interface{}{"b", int64(1)},
		},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_LiteralExpression() {
	noArgsL := exp.NewLiteralExpression(`"b"::DATE = '2010-09-02'`)
	argsL := exp.NewLiteralExpression(`"b" = ? or "c" = ? or d IN ?`, "a", 1, []int{1, 2, 3, 4})

	esgs.assertCases(
		NewExpressionSQLGenerator("test", DefaultDialectOptions()),
		expressionTestCase{val: noArgsL, sql: `"b"::DATE = '2010-09-02'`},
		expressionTestCase{val: noArgsL, sql: `"b"::DATE = '2010-09-02'`, isPrepared: true},

		expressionTestCase{val: argsL, sql: `"b" = 'a' or "c" = 1 or d IN (1, 2, 3, 4)`},
		expressionTestCase{
			val:        argsL,
			sql:        `"b" = ? or "c" = ? or d IN (?, ?, ?, ?)`,
			isPrepared: true,
			args: []interface{}{
				"a",
				int64(1),
				int64(1),
				int64(2),
				int64(3),
				int64(4),
			},
		},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_AliasedExpression() {
	aliasedI := exp.NewIdentifierExpression("", "", "a").As("b")
	aliasedWithII := exp.NewIdentifierExpression("", "", "a").
		As(exp.NewIdentifierExpression("", "", "b"))
	aliasedL := exp.NewLiteralExpression("count(*)").As("count")

	esgs.assertCases(
		NewExpressionSQLGenerator("test", DefaultDialectOptions()),
		expressionTestCase{val: aliasedI, sql: `"a" AS "b"`},
		expressionTestCase{val: aliasedI, sql: `"a" AS "b"`, isPrepared: true},

		expressionTestCase{val: aliasedWithII, sql: `"a" AS "b"`},
		expressionTestCase{val: aliasedWithII, sql: `"a" AS "b"`, isPrepared: true},

		expressionTestCase{val: aliasedL, sql: `count(*) AS "count"`},
		expressionTestCase{val: aliasedL, sql: `count(*) AS "count"`, isPrepared: true},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_BooleanExpression() {
	ae := newTestAppendableExpression(`SELECT "id" FROM "test2"`, emptyArgs, nil, nil)
	re := regexp.MustCompile("(a|b)")
	ident := exp.NewIdentifierExpression("", "", "a")

	esgs.assertCases(
		NewExpressionSQLGenerator("test", DefaultDialectOptions()),
		expressionTestCase{val: ident.Eq(1), sql: `("a" = 1)`},
		expressionTestCase{val: ident.Eq(1), sql: `("a" = ?)`, isPrepared: true, args: []interface{}{int64(1)}},

		expressionTestCase{val: ident.Eq(true), sql: `("a" = TRUE)`},
		expressionTestCase{val: ident.Eq(true), sql: `("a" = ?)`, isPrepared: true, args: []interface{}{true}},

		expressionTestCase{val: ident.Eq(false), sql: `("a" = FALSE)`},
		expressionTestCase{val: ident.Eq(false), sql: `("a" = ?)`, isPrepared: true, args: []interface{}{false}},

		expressionTestCase{val: ident.Eq(nil), sql: `("a" = NULL)`},
		expressionTestCase{val: ident.Eq(nil), sql: `("a" = ?)`, isPrepared: true, args: []interface{}{nil}},

		expressionTestCase{val: ident.Eq([]int64{1, 2, 3}), sql: `("a" IN (1, 2, 3))`},
		expressionTestCase{val: ident.Eq([]int64{1, 2, 3}), sql: `("a" IN (?, ?, ?))`, isPrepared: true, args: []interface{}{
			int64(1), int64(2), int64(3),
		}},

		expressionTestCase{val: ident.Eq(ae), sql: `("a" IN (SELECT "id" FROM "test2"))`},
		expressionTestCase{val: ident.Eq(ae), sql: `("a" IN (SELECT "id" FROM "test2"))`, isPrepared: true},

		expressionTestCase{val: ident.Neq(1), sql: `("a" != 1)`},
		expressionTestCase{val: ident.Neq(1), sql: `("a" != ?)`, isPrepared: true, args: []interface{}{int64(1)}},

		expressionTestCase{val: ident.Neq(true), sql: `("a" != TRUE)`},
		expressionTestCase{val: ident.Neq(true), sql: `("a" != ?)`, isPrepared: true, args: []interface{}{true}},

		expressionTestCase{val: ident.Neq(false), sql: `("a" != FALSE)`},
		expressionTestCase{val: ident.Neq(false), sql: `("a" != ?)`, isPrepared: true, args: []interface{}{false}},

		expressionTestCase{val: ident.Neq(nil), sql: `("a" != NULL)`},
		expressionTestCase{val: ident.Neq(nil), sql: `("a" != ?)`, isPrepared: true, args: []interface{}{nil}},

		expressionTestCase{val: ident.Neq([]int64{1, 2, 3}), sql: `("a" NOT IN (1, 2, 3))`},
		expressionTestCase{val: ident.Neq([]int64{1, 2, 3}), sql: `("a" NOT IN (?, ?, ?))`, isPrepared: true, args: []interface{}{
			int64(1), int64(2), int64(3),
		}},

		expressionTestCase{val: ident.Neq(ae), sql: `("a" NOT IN (SELECT "id" FROM "test2"))`},
		expressionTestCase{val: ident.Neq(ae), sql: `("a" NOT IN (SELECT "id" FROM "test2"))`, isPrepared: true},

		expressionTestCase{val: ident.Is(true), sql: `("a" IS TRUE)`},
		expressionTestCase{val: ident.Is(true), sql: `("a" IS ?)`, isPrepared: true, args: []interface{}{true}},

		expressionTestCase{val: ident.Is(false), sql: `("a" IS FALSE)`},
		expressionTestCase{val: ident.Is(false), sql: `("a" IS ?)`, isPrepared: true, args: []interface{}{false}},

		expressionTestCase{val: ident.Is(nil), sql: `("a" IS NULL)`},
		expressionTestCase{val: ident.Is(nil), sql: `("a" IS ?)`, isPrepared: true, args: []interface{}{nil}},

		expressionTestCase{val: ident.IsNot(true), sql: `("a" IS NOT TRUE)`},
		expressionTestCase{val: ident.IsNot(true), sql: `("a" IS NOT ?)`, isPrepared: true, args: []interface{}{true}},

		expressionTestCase{val: ident.IsNot(false), sql: `("a" IS NOT FALSE)`},
		expressionTestCase{val: ident.IsNot(false), sql: `("a" IS NOT ?)`, isPrepared: true, args: []interface{}{false}},

		expressionTestCase{val: ident.IsNot(nil), sql: `("a" IS NOT NULL)`},
		expressionTestCase{val: ident.IsNot(nil), sql: `("a" IS NOT ?)`, isPrepared: true, args: []interface{}{nil}},

		expressionTestCase{val: ident.Gt(1), sql: `("a" > 1)`},
		expressionTestCase{val: ident.Gt(1), sql: `("a" > ?)`, isPrepared: true, args: []interface{}{int64(1)}},

		expressionTestCase{val: ident.Gte(1), sql: `("a" >= 1)`},
		expressionTestCase{val: ident.Gte(1), sql: `("a" >= ?)`, isPrepared: true, args: []interface{}{int64(1)}},

		expressionTestCase{val: ident.Lt(1), sql: `("a" < 1)`},
		expressionTestCase{val: ident.Lt(1), sql: `("a" < ?)`, isPrepared: true, args: []interface{}{int64(1)}},

		expressionTestCase{val: ident.Lte(1), sql: `("a" <= 1)`},
		expressionTestCase{val: ident.Lte(1), sql: `("a" <= ?)`, isPrepared: true, args: []interface{}{int64(1)}},

		expressionTestCase{val: ident.In([]int64{1, 2, 3}), sql: `("a" IN (1, 2, 3))`},
		expressionTestCase{val: ident.In([]int64{1, 2, 3}), sql: `("a" IN (?, ?, ?))`, isPrepared: true, args: []interface{}{
			int64(1), int64(2), int64(3),
		}},

		expressionTestCase{val: ident.In(ae), sql: `("a" IN ((SELECT "id" FROM "test2")))`},
		expressionTestCase{val: ident.In(ae), sql: `("a" IN ((SELECT "id" FROM "test2")))`, isPrepared: true},

		expressionTestCase{val: ident.NotIn([]int64{1, 2, 3}), sql: `("a" NOT IN (1, 2, 3))`},
		expressionTestCase{val: ident.NotIn([]int64{1, 2, 3}), sql: `("a" NOT IN (?, ?, ?))`, isPrepared: true, args: []interface{}{
			int64(1), int64(2), int64(3),
		}},

		expressionTestCase{val: ident.NotIn(ae), sql: `("a" NOT IN ((SELECT "id" FROM "test2")))`},
		expressionTestCase{val: ident.NotIn(ae), sql: `("a" NOT IN ((SELECT "id" FROM "test2")))`, isPrepared: true},

		expressionTestCase{val: ident.Like("a%"), sql: `("a" LIKE 'a%')`},
		expressionTestCase{val: ident.Like("a%"), sql: `("a" LIKE ?)`, isPrepared: true, args: []interface{}{"a%"}},

		expressionTestCase{val: ident.Like(re), sql: `("a" ~ '(a|b)')`},
		expressionTestCase{val: ident.Like(re), sql: `("a" ~ ?)`, isPrepared: true, args: []interface{}{"(a|b)"}},

		expressionTestCase{val: ident.ILike("a%"), sql: `("a" ILIKE 'a%')`},
		expressionTestCase{val: ident.ILike("a%"), sql: `("a" ILIKE ?)`, isPrepared: true, args: []interface{}{"a%"}},

		expressionTestCase{val: ident.ILike(re), sql: `("a" ~* '(a|b)')`},
		expressionTestCase{val: ident.ILike(re), sql: `("a" ~* ?)`, isPrepared: true, args: []interface{}{"(a|b)"}},

		expressionTestCase{val: ident.NotLike("a%"), sql: `("a" NOT LIKE 'a%')`},
		expressionTestCase{val: ident.NotLike("a%"), sql: `("a" NOT LIKE ?)`, isPrepared: true, args: []interface{}{"a%"}},

		expressionTestCase{val: ident.NotLike(re), sql: `("a" !~ '(a|b)')`},
		expressionTestCase{val: ident.NotLike(re), sql: `("a" !~ ?)`, isPrepared: true, args: []interface{}{"(a|b)"}},

		expressionTestCase{val: ident.NotILike("a%"), sql: `("a" NOT ILIKE 'a%')`},
		expressionTestCase{val: ident.NotILike("a%"), sql: `("a" NOT ILIKE ?)`, isPrepared: true, args: []interface{}{"a%"}},

		expressionTestCase{val: ident.NotILike(re), sql: `("a" !~* '(a|b)')`},
		expressionTestCase{val: ident.NotILike(re), sql: `("a" !~* ?)`, isPrepared: true, args: []interface{}{"(a|b)"}},
	)

	opts := DefaultDialectOptions()
	opts.BooleanOperatorLookup = map[exp.BooleanOperation][]byte{}
	esgs.assertCases(
		NewExpressionSQLGenerator("test", opts),
		expressionTestCase{val: ident.Eq(1), err: "goqu: boolean operator 'eq' not supported"},
		expressionTestCase{val: ident.Neq(1), err: "goqu: boolean operator 'neq' not supported"},
		expressionTestCase{val: ident.Is(true), err: "goqu: boolean operator 'is' not supported"},
		expressionTestCase{val: ident.IsNot(true), err: "goqu: boolean operator 'isnot' not supported"},
		expressionTestCase{val: ident.Gt(1), err: "goqu: boolean operator 'gt' not supported"},
		expressionTestCase{val: ident.Gte(1), err: "goqu: boolean operator 'gte' not supported"},
		expressionTestCase{val: ident.Lt(1), err: "goqu: boolean operator 'lt' not supported"},
		expressionTestCase{val: ident.Lte(1), err: "goqu: boolean operator 'lte' not supported"},
		expressionTestCase{val: ident.In([]int64{1, 2, 3}), err: "goqu: boolean operator 'in' not supported"},
		expressionTestCase{val: ident.NotIn([]int64{1, 2, 3}), err: "goqu: boolean operator 'notin' not supported"},
		expressionTestCase{val: ident.Like("a%"), err: "goqu: boolean operator 'like' not supported"},
		expressionTestCase{val: ident.Like(re), err: "goqu: boolean operator 'regexp like' not supported"},
		expressionTestCase{val: ident.ILike("a%"), err: "goqu: boolean operator 'ilike' not supported"},
		expressionTestCase{val: ident.ILike(re), err: "goqu: boolean operator 'regexp ilike' not supported"},
		expressionTestCase{val: ident.NotLike("a%"), err: "goqu: boolean operator 'notlike' not supported"},
		expressionTestCase{val: ident.NotLike(re), err: "goqu: boolean operator 'regexp notlike' not supported"},
		expressionTestCase{val: ident.NotILike("a%"), err: "goqu: boolean operator 'notilike' not supported"},
		expressionTestCase{val: ident.NotILike(re), err: "goqu: boolean operator 'regexp notilike' not supported"},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_RangeExpression() {
	betweenNum := exp.NewIdentifierExpression("", "", "a").
		Between(exp.NewRangeVal(1, 2))
	notBetweenNum := exp.NewIdentifierExpression("", "", "a").
		NotBetween(exp.NewRangeVal(1, 2))

	betweenStr := exp.NewIdentifierExpression("", "", "a").
		Between(exp.NewRangeVal("aaa", "zzz"))
	notBetweenStr := exp.NewIdentifierExpression("", "", "a").
		NotBetween(exp.NewRangeVal("aaa", "zzz"))
	esgs.assertCases(
		NewExpressionSQLGenerator("test", DefaultDialectOptions()),
		expressionTestCase{val: betweenNum, sql: `("a" BETWEEN 1 AND 2)`},
		expressionTestCase{val: betweenNum, sql: `("a" BETWEEN ? AND ?)`, isPrepared: true, args: []interface{}{
			int64(1),
			int64(2),
		}},

		expressionTestCase{val: notBetweenNum, sql: `("a" NOT BETWEEN 1 AND 2)`},
		expressionTestCase{val: notBetweenNum, sql: `("a" NOT BETWEEN ? AND ?)`, isPrepared: true, args: []interface{}{
			int64(1),
			int64(2),
		}},

		expressionTestCase{val: betweenStr, sql: `("a" BETWEEN 'aaa' AND 'zzz')`},
		expressionTestCase{val: betweenStr, sql: `("a" BETWEEN ? AND ?)`, isPrepared: true, args: []interface{}{
			"aaa",
			"zzz",
		}},

		expressionTestCase{val: notBetweenStr, sql: `("a" NOT BETWEEN 'aaa' AND 'zzz')`},
		expressionTestCase{val: notBetweenStr, sql: `("a" NOT BETWEEN ? AND ?)`, isPrepared: true, args: []interface{}{
			"aaa",
			"zzz",
		}},
	)

	opts := DefaultDialectOptions()
	opts.RangeOperatorLookup = map[exp.RangeOperation][]byte{}
	esgs.assertCases(
		NewExpressionSQLGenerator("test", opts),
		expressionTestCase{val: betweenNum, err: "goqu: range operator between not supported"},
		expressionTestCase{val: betweenNum, err: "goqu: range operator between not supported"},

		expressionTestCase{val: notBetweenNum, err: "goqu: range operator not between not supported"},
		expressionTestCase{val: notBetweenNum, err: "goqu: range operator not between not supported"},

		expressionTestCase{val: betweenStr, err: "goqu: range operator between not supported"},
		expressionTestCase{val: betweenStr, err: "goqu: range operator between not supported"},

		expressionTestCase{val: notBetweenStr, err: "goqu: range operator not between not supported"},
		expressionTestCase{val: notBetweenStr, err: "goqu: range operator not between not supported"},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_OrderedExpression() {

	asc := exp.NewIdentifierExpression("", "", "a").Asc()
	ascNf := exp.NewIdentifierExpression("", "", "a").Asc().NullsFirst()
	ascNl := exp.NewIdentifierExpression("", "", "a").Asc().NullsLast()

	desc := exp.NewIdentifierExpression("", "", "a").Desc()
	descNf := exp.NewIdentifierExpression("", "", "a").Desc().NullsFirst()
	descNl := exp.NewIdentifierExpression("", "", "a").Desc().NullsLast()

	esgs.assertCases(
		NewExpressionSQLGenerator("test", DefaultDialectOptions()),
		expressionTestCase{val: asc, sql: `"a" ASC`},
		expressionTestCase{val: asc, sql: `"a" ASC`, isPrepared: true},

		expressionTestCase{val: ascNf, sql: `"a" ASC NULLS FIRST`},
		expressionTestCase{val: ascNf, sql: `"a" ASC NULLS FIRST`, isPrepared: true},

		expressionTestCase{val: ascNl, sql: `"a" ASC NULLS LAST`},
		expressionTestCase{val: ascNl, sql: `"a" ASC NULLS LAST`, isPrepared: true},

		expressionTestCase{val: desc, sql: `"a" DESC`},
		expressionTestCase{val: desc, sql: `"a" DESC`, isPrepared: true},

		expressionTestCase{val: descNf, sql: `"a" DESC NULLS FIRST`},
		expressionTestCase{val: descNf, sql: `"a" DESC NULLS FIRST`, isPrepared: true},

		expressionTestCase{val: descNl, sql: `"a" DESC NULLS LAST`},
		expressionTestCase{val: descNl, sql: `"a" DESC NULLS LAST`, isPrepared: true},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_UpdateExpression() {
	ue := exp.NewIdentifierExpression("", "", "a").Set(1)
	esgs.assertCases(
		NewExpressionSQLGenerator("test", DefaultDialectOptions()),
		expressionTestCase{val: ue, sql: `"a"=1`},
		expressionTestCase{val: ue, sql: `"a"=?`, isPrepared: true, args: []interface{}{int64(1)}},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_SQLFunctionExpression() {

	min := exp.NewSQLFunctionExpression("MIN", exp.NewIdentifierExpression("", "", "a"))
	coalesce := exp.NewSQLFunctionExpression("COALESCE", exp.NewIdentifierExpression("", "", "a"), "a")
	esgs.assertCases(
		NewExpressionSQLGenerator("test", DefaultDialectOptions()),
		expressionTestCase{val: min, sql: `MIN("a")`},
		expressionTestCase{val: min, sql: `MIN("a")`, isPrepared: true},

		expressionTestCase{val: coalesce, sql: `COALESCE("a", 'a')`},
		expressionTestCase{val: coalesce, sql: `COALESCE("a", ?)`, isPrepared: true, args: []interface{}{"a"}},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_SQLWindowFunctionExpression() {
	sqlWinFunc := exp.NewSQLWindowFunctionExpression(
		exp.NewSQLFunctionExpression("some_func"),
		nil,
		exp.NewWindowExpression(
			nil,
			exp.NewIdentifierExpression("", "", "win"),
			nil,
			nil,
		),
	)
	sqlWinFuncFromWindow := exp.NewSQLWindowFunctionExpression(
		exp.NewSQLFunctionExpression("some_func"),
		exp.NewIdentifierExpression("", "", "win"),
		nil,
	)

	emptyWinFunc := exp.NewSQLWindowFunctionExpression(
		exp.NewSQLFunctionExpression("some_func"),
		nil,
		nil,
	)
	badNamedSQLWinFuncInherit := exp.NewSQLWindowFunctionExpression(
		exp.NewSQLFunctionExpression("some_func"),
		nil,
		exp.NewWindowExpression(
			exp.NewIdentifierExpression("", "", "w"),
			nil,
			nil,
			nil,
		),
	)
	esgs.assertCases(
		NewExpressionSQLGenerator("test", DefaultDialectOptions()),
		expressionTestCase{val: sqlWinFunc, sql: `some_func() OVER ("win")`},
		expressionTestCase{val: sqlWinFunc, sql: `some_func() OVER ("win")`, isPrepared: true},

		expressionTestCase{val: sqlWinFuncFromWindow, sql: `some_func() OVER "win"`},
		expressionTestCase{val: sqlWinFuncFromWindow, sql: `some_func() OVER "win"`, isPrepared: true},

		expressionTestCase{val: emptyWinFunc, sql: `some_func() OVER ()`},
		expressionTestCase{val: emptyWinFunc, sql: `some_func() OVER ()`, isPrepared: true},

		expressionTestCase{val: badNamedSQLWinFuncInherit, err: errUnexpectedNamedWindow.Error()},
		expressionTestCase{val: badNamedSQLWinFuncInherit, err: errUnexpectedNamedWindow.Error(), isPrepared: true},
	)
	opts := DefaultDialectOptions()
	opts.SupportsWindowFunction = false
	esgs.assertCases(
		NewExpressionSQLGenerator("test", opts),
		expressionTestCase{val: sqlWinFunc, err: errWindowNotSupported("test").Error()},
		expressionTestCase{val: sqlWinFunc, err: errWindowNotSupported("test").Error(), isPrepared: true},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_WindowExpression() {
	opts := DefaultDialectOptions()
	opts.WindowPartitionByFragment = []byte("partition by ")
	opts.WindowOrderByFragment = []byte("order by ")

	emptySQLWinFunc := exp.NewWindowExpression(nil, nil, nil, nil)
	namedSQLWinFunc := exp.NewWindowExpression(
		exp.NewIdentifierExpression("", "", "w"), nil, nil, nil,
	)
	inheritSQLWinFunc := exp.NewWindowExpression(
		nil, exp.NewIdentifierExpression("", "", "w"), nil, nil,
	)
	partitionBySQLWinFunc := exp.NewWindowExpression(
		nil, nil, exp.NewColumnListExpression("a", "b"), nil,
	)
	orderBySQLWinFunc := exp.NewWindowExpression(
		nil, nil, nil, exp.NewOrderedColumnList(
			exp.NewIdentifierExpression("", "", "a").Asc(),
			exp.NewIdentifierExpression("", "", "b").Desc(),
		),
	)

	namedInheritPartitionOrderSQLWinFunc := exp.NewWindowExpression(
		exp.NewIdentifierExpression("", "", "w1"),
		exp.NewIdentifierExpression("", "", "w2"),
		exp.NewColumnListExpression("a", "b"),
		exp.NewOrderedColumnList(
			exp.NewIdentifierExpression("", "", "a").Asc(),
			exp.NewIdentifierExpression("", "", "b").Desc(),
		),
	)

	esgs.assertCases(
		NewExpressionSQLGenerator("test", opts),
		expressionTestCase{val: emptySQLWinFunc, sql: `()`},
		expressionTestCase{val: emptySQLWinFunc, sql: `()`, isPrepared: true},

		expressionTestCase{val: namedSQLWinFunc, sql: `"w" AS ()`},
		expressionTestCase{val: namedSQLWinFunc, sql: `"w" AS ()`, isPrepared: true},

		expressionTestCase{val: inheritSQLWinFunc, sql: `("w")`},
		expressionTestCase{val: inheritSQLWinFunc, sql: `("w")`, isPrepared: true},

		expressionTestCase{val: partitionBySQLWinFunc, sql: `(partition by "a", "b")`},
		expressionTestCase{val: partitionBySQLWinFunc, sql: `(partition by "a", "b")`, isPrepared: true},

		expressionTestCase{val: orderBySQLWinFunc, sql: `(order by "a" ASC, "b" DESC)`},
		expressionTestCase{val: orderBySQLWinFunc, sql: `(order by "a" ASC, "b" DESC)`, isPrepared: true},

		expressionTestCase{
			val: namedInheritPartitionOrderSQLWinFunc,
			sql: `"w1" AS ("w2" partition by "a", "b" order by "a" ASC, "b" DESC)`,
		},
		expressionTestCase{
			val:        namedInheritPartitionOrderSQLWinFunc,
			sql:        `"w1" AS ("w2" partition by "a", "b" order by "a" ASC, "b" DESC)`,
			isPrepared: true,
		},
	)

	opts = DefaultDialectOptions()
	opts.SupportsWindowFunction = false
	esgs.assertCases(
		NewExpressionSQLGenerator("test", opts),
		expressionTestCase{val: emptySQLWinFunc, err: errWindowNotSupported("test").Error()},
		expressionTestCase{val: emptySQLWinFunc, err: errWindowNotSupported("test").Error(), isPrepared: true},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_CastExpression() {
	cast := exp.NewIdentifierExpression("", "", "a").Cast("DATE")
	esgs.assertCases(
		NewExpressionSQLGenerator("test", DefaultDialectOptions()),
		expressionTestCase{val: cast, sql: `CAST("a" AS DATE)`},
		expressionTestCase{val: cast, sql: `CAST("a" AS DATE)`, isPrepared: true},
	)
}

// Generates the sql for the WITH clauses for common table expressions (CTE)
func (esgs *expressionSQLGeneratorSuite) TestGenerate_CommonTableExpressionSlice() {
	ae := newTestAppendableExpression(`SELECT * FROM "b"`, emptyArgs, nil, nil)

	cteNoArgs := []exp.CommonTableExpression{
		exp.NewCommonTableExpression(false, "a", ae),
	}
	cteArgs := []exp.CommonTableExpression{
		exp.NewCommonTableExpression(false, "a(x,y)", ae),
	}

	cteRecursiveNoArgs := []exp.CommonTableExpression{
		exp.NewCommonTableExpression(true, "a", ae),
	}
	cteRecursiveArgs := []exp.CommonTableExpression{
		exp.NewCommonTableExpression(true, "a(x,y)", ae),
	}

	allCtes := []exp.CommonTableExpression{
		exp.NewCommonTableExpression(false, "a", ae),
		exp.NewCommonTableExpression(false, "a(x,y)", ae),
	}

	allRecursiveCtes := []exp.CommonTableExpression{
		exp.NewCommonTableExpression(true, "a", ae),
		exp.NewCommonTableExpression(true, "a(x,y)", ae),
	}

	esgs.assertCases(
		NewExpressionSQLGenerator("test", DefaultDialectOptions()),
		expressionTestCase{val: cteNoArgs, sql: `WITH a AS (SELECT * FROM "b") `},
		expressionTestCase{val: cteNoArgs, sql: `WITH a AS (SELECT * FROM "b") `, isPrepared: true},

		expressionTestCase{val: cteArgs, sql: `WITH a(x,y) AS (SELECT * FROM "b") `},
		expressionTestCase{val: cteArgs, sql: `WITH a(x,y) AS (SELECT * FROM "b") `, isPrepared: true},

		expressionTestCase{val: cteRecursiveNoArgs, sql: `WITH RECURSIVE a AS (SELECT * FROM "b") `},
		expressionTestCase{val: cteRecursiveNoArgs, sql: `WITH RECURSIVE a AS (SELECT * FROM "b") `, isPrepared: true},

		expressionTestCase{val: cteRecursiveArgs, sql: `WITH RECURSIVE a(x,y) AS (SELECT * FROM "b") `},
		expressionTestCase{val: cteRecursiveArgs, sql: `WITH RECURSIVE a(x,y) AS (SELECT * FROM "b") `, isPrepared: true},

		expressionTestCase{val: allCtes, sql: `WITH a AS (SELECT * FROM "b"), a(x,y) AS (SELECT * FROM "b") `},
		expressionTestCase{val: allCtes, sql: `WITH a AS (SELECT * FROM "b"), a(x,y) AS (SELECT * FROM "b") `, isPrepared: true},

		expressionTestCase{val: allRecursiveCtes, sql: `WITH RECURSIVE a AS (SELECT * FROM "b"), a(x,y) AS (SELECT * FROM "b") `},
		expressionTestCase{
			val:        allRecursiveCtes,
			sql:        `WITH RECURSIVE a AS (SELECT * FROM "b"), a(x,y) AS (SELECT * FROM "b") `,
			isPrepared: true,
		},
	)
	opts := DefaultDialectOptions()
	opts.SupportsWithCTE = false
	esgs.assertCases(
		NewExpressionSQLGenerator("test", opts),
		expressionTestCase{val: cteNoArgs, err: "goqu: dialect does not support CTE WITH clause [dialect=test]"},
		expressionTestCase{val: cteNoArgs, err: "goqu: dialect does not support CTE WITH clause [dialect=test]", isPrepared: true},

		expressionTestCase{val: cteArgs, err: "goqu: dialect does not support CTE WITH clause [dialect=test]"},
		expressionTestCase{val: cteArgs, err: "goqu: dialect does not support CTE WITH clause [dialect=test]", isPrepared: true},

		expressionTestCase{val: cteRecursiveNoArgs, err: "goqu: dialect does not support CTE WITH clause [dialect=test]"},
		expressionTestCase{val: cteRecursiveNoArgs, err: "goqu: dialect does not support CTE WITH clause [dialect=test]", isPrepared: true},

		expressionTestCase{val: cteRecursiveArgs, err: "goqu: dialect does not support CTE WITH clause [dialect=test]"},
		expressionTestCase{val: cteRecursiveArgs, err: "goqu: dialect does not support CTE WITH clause [dialect=test]", isPrepared: true},
	)
	opts = DefaultDialectOptions()
	opts.SupportsWithCTERecursive = false
	esgs.assertCases(
		NewExpressionSQLGenerator("test", opts),
		expressionTestCase{val: cteNoArgs, sql: `WITH a AS (SELECT * FROM "b") `},
		expressionTestCase{val: cteNoArgs, sql: `WITH a AS (SELECT * FROM "b") `, isPrepared: true},

		expressionTestCase{val: cteArgs, sql: `WITH a(x,y) AS (SELECT * FROM "b") `},
		expressionTestCase{val: cteArgs, sql: `WITH a(x,y) AS (SELECT * FROM "b") `, isPrepared: true},

		expressionTestCase{
			val: cteRecursiveNoArgs,
			err: "goqu: dialect does not support CTE WITH RECURSIVE clause [dialect=test]",
		},
		expressionTestCase{
			val:        cteRecursiveNoArgs,
			err:        "goqu: dialect does not support CTE WITH RECURSIVE clause [dialect=test]",
			isPrepared: true,
		},

		expressionTestCase{
			val: cteRecursiveArgs,
			err: "goqu: dialect does not support CTE WITH RECURSIVE clause [dialect=test]",
		},
		expressionTestCase{
			val:        cteRecursiveArgs,
			err:        "goqu: dialect does not support CTE WITH RECURSIVE clause [dialect=test]",
			isPrepared: true,
		},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_CommonTableExpression() {
	ae := newTestAppendableExpression(`SELECT * FROM "b"`, emptyArgs, nil, nil)

	cteNoArgs := exp.NewCommonTableExpression(false, "a", ae)
	cteArgs := exp.NewCommonTableExpression(false, "a(x,y)", ae)

	cteRecursiveNoArgs := exp.NewCommonTableExpression(true, "a", ae)
	cteRecursiveArgs := exp.NewCommonTableExpression(true, "a(x,y)", ae)

	esgs.assertCases(
		NewExpressionSQLGenerator("test", DefaultDialectOptions()),
		expressionTestCase{val: cteNoArgs, sql: `a AS (SELECT * FROM "b")`},
		expressionTestCase{val: cteNoArgs, sql: `a AS (SELECT * FROM "b")`, isPrepared: true},

		expressionTestCase{val: cteArgs, sql: `a(x,y) AS (SELECT * FROM "b")`},
		expressionTestCase{val: cteArgs, sql: `a(x,y) AS (SELECT * FROM "b")`, isPrepared: true},

		expressionTestCase{val: cteRecursiveNoArgs, sql: `a AS (SELECT * FROM "b")`},
		expressionTestCase{val: cteRecursiveNoArgs, sql: `a AS (SELECT * FROM "b")`, isPrepared: true},

		expressionTestCase{val: cteRecursiveArgs, sql: `a(x,y) AS (SELECT * FROM "b")`},
		expressionTestCase{val: cteRecursiveArgs, sql: `a(x,y) AS (SELECT * FROM "b")`, isPrepared: true},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_CompoundExpression() {
	ae := newTestAppendableExpression(`SELECT * FROM "b"`, emptyArgs, nil, nil)

	u := exp.NewCompoundExpression(exp.UnionCompoundType, ae)
	ua := exp.NewCompoundExpression(exp.UnionAllCompoundType, ae)

	i := exp.NewCompoundExpression(exp.IntersectCompoundType, ae)
	ia := exp.NewCompoundExpression(exp.IntersectAllCompoundType, ae)

	esgs.assertCases(
		NewExpressionSQLGenerator("test", DefaultDialectOptions()),
		expressionTestCase{val: u, sql: ` UNION (SELECT * FROM "b")`},
		expressionTestCase{val: u, sql: ` UNION (SELECT * FROM "b")`, isPrepared: true},

		expressionTestCase{val: ua, sql: ` UNION ALL (SELECT * FROM "b")`},
		expressionTestCase{val: ua, sql: ` UNION ALL (SELECT * FROM "b")`, isPrepared: true},

		expressionTestCase{val: i, sql: ` INTERSECT (SELECT * FROM "b")`},
		expressionTestCase{val: i, sql: ` INTERSECT (SELECT * FROM "b")`, isPrepared: true},

		expressionTestCase{val: ia, sql: ` INTERSECT ALL (SELECT * FROM "b")`},
		expressionTestCase{val: ia, sql: ` INTERSECT ALL (SELECT * FROM "b")`, isPrepared: true},
	)

	opts := DefaultDialectOptions()
	opts.WrapCompoundsInParens = false
	esgs.assertCases(
		NewExpressionSQLGenerator("test", opts),
		expressionTestCase{val: u, sql: ` UNION SELECT * FROM "b"`},
		expressionTestCase{val: u, sql: ` UNION SELECT * FROM "b"`, isPrepared: true},

		expressionTestCase{val: ua, sql: ` UNION ALL SELECT * FROM "b"`},
		expressionTestCase{val: ua, sql: ` UNION ALL SELECT * FROM "b"`, isPrepared: true},

		expressionTestCase{val: i, sql: ` INTERSECT SELECT * FROM "b"`},
		expressionTestCase{val: i, sql: ` INTERSECT SELECT * FROM "b"`, isPrepared: true},

		expressionTestCase{val: ia, sql: ` INTERSECT ALL SELECT * FROM "b"`},
		expressionTestCase{val: ia, sql: ` INTERSECT ALL SELECT * FROM "b"`, isPrepared: true},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_IdentifierExpression() {
	col := exp.NewIdentifierExpression("", "", "col")
	colStar := exp.NewIdentifierExpression("", "", "*")
	table := exp.NewIdentifierExpression("", "table", "")
	schema := exp.NewIdentifierExpression("schema", "", "")
	tableCol := exp.NewIdentifierExpression("", "table", "col")
	schemaTableCol := exp.NewIdentifierExpression("schema", "table", "col")

	parsedCol := exp.ParseIdentifier("col")
	parsedTableCol := exp.ParseIdentifier("table.col")
	parsedSchemaTableCol := exp.ParseIdentifier("schema.table.col")

	esgs.assertCases(
		NewExpressionSQLGenerator("test", DefaultDialectOptions()),
		expressionTestCase{
			val: exp.NewIdentifierExpression("", "", ""),
			err: `goqu: a empty identifier was encountered, please specify a "schema", "table" or "column"`,
		},
		expressionTestCase{
			val: exp.NewIdentifierExpression("", "", nil),
			err: `goqu: a empty identifier was encountered, please specify a "schema", "table" or "column"`,
		},
		expressionTestCase{
			val: exp.NewIdentifierExpression("", "", false),
			err: `goqu: unexpected col type must be string or LiteralExpression received bool`,
		},

		expressionTestCase{val: col, sql: `"col"`},
		expressionTestCase{val: col, sql: `"col"`, isPrepared: true},

		expressionTestCase{val: col.Table("table"), sql: `"table"."col"`},
		expressionTestCase{val: col.Table("table"), sql: `"table"."col"`, isPrepared: true},

		expressionTestCase{val: col.Table("table").Schema("schema"), sql: `"schema"."table"."col"`},
		expressionTestCase{val: col.Table("table").Schema("schema"), sql: `"schema"."table"."col"`, isPrepared: true},

		expressionTestCase{val: colStar, sql: `*`},
		expressionTestCase{val: colStar, sql: `*`, isPrepared: true},

		expressionTestCase{val: colStar.Table("table"), sql: `"table".*`},
		expressionTestCase{val: colStar.Table("table"), sql: `"table".*`, isPrepared: true},

		expressionTestCase{val: colStar.Table("table").Schema("schema"), sql: `"schema"."table".*`},
		expressionTestCase{val: colStar.Table("table").Schema("schema"), sql: `"schema"."table".*`, isPrepared: true},

		expressionTestCase{val: table, sql: `"table"`},
		expressionTestCase{val: table, sql: `"table"`, isPrepared: true},

		expressionTestCase{val: table.Col("col"), sql: `"table"."col"`},
		expressionTestCase{val: table.Col("col"), sql: `"table"."col"`, isPrepared: true},

		expressionTestCase{val: table.Col(nil), sql: `"table"`},
		expressionTestCase{val: table.Col(nil), sql: `"table"`, isPrepared: true},

		expressionTestCase{val: table.Col("*"), sql: `"table".*`},
		expressionTestCase{val: table.Col("*"), sql: `"table".*`, isPrepared: true},

		expressionTestCase{val: table.Schema("schema").Col("col"), sql: `"schema"."table"."col"`},
		expressionTestCase{val: table.Schema("schema").Col("col"), sql: `"schema"."table"."col"`, isPrepared: true},

		expressionTestCase{val: schema, sql: `"schema"`},
		expressionTestCase{val: schema, sql: `"schema"`, isPrepared: true},

		expressionTestCase{val: schema.Table("table"), sql: `"schema"."table"`},
		expressionTestCase{val: schema.Table("table"), sql: `"schema"."table"`, isPrepared: true},

		expressionTestCase{val: schema.Table("table").Col("col"), sql: `"schema"."table"."col"`},
		expressionTestCase{val: schema.Table("table").Col("col"), sql: `"schema"."table"."col"`, isPrepared: true},

		expressionTestCase{val: schema.Table("table").Col(nil), sql: `"schema"."table"`},
		expressionTestCase{val: schema.Table("table").Col(nil), sql: `"schema"."table"`, isPrepared: true},

		expressionTestCase{val: schema.Table("table").Col("*"), sql: `"schema"."table".*`},
		expressionTestCase{val: schema.Table("table").Col("*"), sql: `"schema"."table".*`, isPrepared: true},

		expressionTestCase{val: tableCol, sql: `"table"."col"`},
		expressionTestCase{val: tableCol, sql: `"table"."col"`, isPrepared: true},

		expressionTestCase{val: schemaTableCol, sql: `"schema"."table"."col"`},
		expressionTestCase{val: schemaTableCol, sql: `"schema"."table"."col"`, isPrepared: true},

		expressionTestCase{val: parsedCol, sql: `"col"`},
		expressionTestCase{val: parsedCol, sql: `"col"`, isPrepared: true},

		expressionTestCase{val: parsedTableCol, sql: `"table"."col"`},
		expressionTestCase{val: parsedTableCol, sql: `"table"."col"`, isPrepared: true},

		expressionTestCase{val: parsedSchemaTableCol, sql: `"schema"."table"."col"`},
		expressionTestCase{val: parsedSchemaTableCol, sql: `"schema"."table"."col"`, isPrepared: true},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_ExpressionMap() {
	re := regexp.MustCompile("(a|b)")
	esgs.assertCases(
		NewExpressionSQLGenerator("test", DefaultDialectOptions()),
		expressionTestCase{val: exp.Ex{}},
		expressionTestCase{val: exp.Ex{}, isPrepared: true},

		expressionTestCase{
			val: exp.Ex{"a": exp.Op{"badOp": true}},
			err: "goqu: unsupported expression type map[badOp:%!s(bool=true)]",
		},
		expressionTestCase{
			val:        exp.Ex{"a": exp.Op{"badOp": true}},
			isPrepared: true,
			err:        "goqu: unsupported expression type map[badOp:%!s(bool=true)]",
		},

		expressionTestCase{val: exp.Ex{"a": 1}, sql: `("a" = 1)`},
		expressionTestCase{val: exp.Ex{"a": 1}, sql: `("a" = ?)`, isPrepared: true, args: []interface{}{int64(1)}},

		expressionTestCase{val: exp.Ex{"a": true}, sql: `("a" = TRUE)`},
		expressionTestCase{val: exp.Ex{"a": true}, sql: `("a" = ?)`, isPrepared: true, args: []interface{}{true}},

		expressionTestCase{val: exp.Ex{"a": false}, sql: `("a" = FALSE)`},
		expressionTestCase{val: exp.Ex{"a": false}, sql: `("a" = ?)`, isPrepared: true, args: []interface{}{false}},

		expressionTestCase{val: exp.Ex{"a": nil}, sql: `("a" = NULL)`},
		expressionTestCase{val: exp.Ex{"a": nil}, sql: `("a" = ?)`, isPrepared: true, args: []interface{}{nil}},

		expressionTestCase{val: exp.Ex{"a": []string{"a", "b", "c"}}, sql: `("a" IN ('a', 'b', 'c'))`},
		expressionTestCase{
			val:        exp.Ex{"a": []string{"a", "b", "c"}},
			sql:        `("a" IN (?, ?, ?))`,
			isPrepared: true,
			args:       []interface{}{"a", "b", "c"},
		},

		expressionTestCase{val: exp.Ex{"a": exp.Op{"neq": 1}}, sql: `("a" != 1)`},
		expressionTestCase{val: exp.Ex{"a": exp.Op{"neq": 1}}, sql: `("a" != ?)`, isPrepared: true, args: []interface{}{
			int64(1),
		}},

		expressionTestCase{val: exp.Ex{"a": exp.Op{"isnot": true}}, sql: `("a" IS NOT TRUE)`},
		expressionTestCase{val: exp.Ex{"a": exp.Op{"isnot": true}}, sql: `("a" IS NOT ?)`, isPrepared: true, args: []interface{}{true}},

		expressionTestCase{val: exp.Ex{"a": exp.Op{"gt": 1}}, sql: `("a" > 1)`},
		expressionTestCase{val: exp.Ex{"a": exp.Op{"gt": 1}}, sql: `("a" > ?)`, isPrepared: true, args: []interface{}{
			int64(1),
		}},

		expressionTestCase{val: exp.Ex{"a": exp.Op{"gte": 1}}, sql: `("a" >= 1)`},
		expressionTestCase{val: exp.Ex{"a": exp.Op{"gte": 1}}, sql: `("a" >= ?)`, isPrepared: true, args: []interface{}{
			int64(1),
		}},

		expressionTestCase{val: exp.Ex{"a": exp.Op{"lt": 1}}, sql: `("a" < 1)`},
		expressionTestCase{val: exp.Ex{"a": exp.Op{"lt": 1}}, sql: `("a" < ?)`, isPrepared: true, args: []interface{}{
			int64(1),
		}},

		expressionTestCase{val: exp.Ex{"a": exp.Op{"lte": 1}}, sql: `("a" <= 1)`},
		expressionTestCase{val: exp.Ex{"a": exp.Op{"lte": 1}}, sql: `("a" <= ?)`, isPrepared: true, args: []interface{}{
			int64(1),
		}},

		expressionTestCase{val: exp.Ex{"a": exp.Op{"like": "a%"}}, sql: `("a" LIKE 'a%')`},
		expressionTestCase{
			val:        exp.Ex{"a": exp.Op{"like": "a%"}},
			sql:        `("a" LIKE ?)`,
			isPrepared: true,
			args:       []interface{}{"a%"},
		},

		expressionTestCase{val: exp.Ex{"a": exp.Op{"like": re}}, sql: `("a" ~ '(a|b)')`},
		expressionTestCase{
			val:        exp.Ex{"a": exp.Op{"like": re}},
			sql:        `("a" ~ ?)`,
			isPrepared: true,
			args:       []interface{}{"(a|b)"},
		},

		expressionTestCase{val: exp.Ex{"a": exp.Op{"notLike": "a%"}}, sql: `("a" NOT LIKE 'a%')`},
		expressionTestCase{
			val:        exp.Ex{"a": exp.Op{"notLike": "a%"}},
			sql:        `("a" NOT LIKE ?)`,
			isPrepared: true,
			args:       []interface{}{"a%"},
		},

		expressionTestCase{val: exp.Ex{"a": exp.Op{"notLike": re}}, sql: `("a" !~ '(a|b)')`},
		expressionTestCase{
			val:        exp.Ex{"a": exp.Op{"notLike": re}},
			sql:        `("a" !~ ?)`,
			isPrepared: true,
			args:       []interface{}{"(a|b)"},
		},

		expressionTestCase{val: exp.Ex{"a": exp.Op{"iLike": "a%"}}, sql: `("a" ILIKE 'a%')`},
		expressionTestCase{
			val:        exp.Ex{"a": exp.Op{"iLike": "a%"}},
			sql:        `("a" ILIKE ?)`,
			isPrepared: true,
			args:       []interface{}{"a%"},
		},

		expressionTestCase{val: exp.Ex{"a": exp.Op{"iLike": re}}, sql: `("a" ~* '(a|b)')`},
		expressionTestCase{
			val:        exp.Ex{"a": exp.Op{"iLike": re}},
			sql:        `("a" ~* ?)`,
			isPrepared: true,
			args:       []interface{}{"(a|b)"},
		},

		expressionTestCase{val: exp.Ex{"a": exp.Op{"notILike": "a%"}}, sql: `("a" NOT ILIKE 'a%')`},
		expressionTestCase{
			val:        exp.Ex{"a": exp.Op{"notILike": "a%"}},
			sql:        `("a" NOT ILIKE ?)`,
			isPrepared: true,
			args:       []interface{}{"a%"},
		},

		expressionTestCase{val: exp.Ex{"a": exp.Op{"notILike": re}}, sql: `("a" !~* '(a|b)')`},
		expressionTestCase{
			val:        exp.Ex{"a": exp.Op{"notILike": re}},
			sql:        `("a" !~* ?)`,
			isPrepared: true,
			args:       []interface{}{"(a|b)"},
		},

		expressionTestCase{val: exp.Ex{"a": exp.Op{"in": []string{"a", "b", "c"}}}, sql: `("a" IN ('a', 'b', 'c'))`},
		expressionTestCase{
			val:        exp.Ex{"a": exp.Op{"in": []string{"a", "b", "c"}}},
			sql:        `("a" IN (?, ?, ?))`,
			isPrepared: true,
			args:       []interface{}{"a", "b", "c"},
		},

		expressionTestCase{
			val: exp.Ex{"a": exp.Op{"notIn": []string{"a", "b", "c"}}},
			sql: `("a" NOT IN ('a', 'b', 'c'))`,
		},
		expressionTestCase{
			val:        exp.Ex{"a": exp.Op{"notIn": []string{"a", "b", "c"}}},
			sql:        `("a" NOT IN (?, ?, ?))`,
			isPrepared: true,
			args:       []interface{}{"a", "b", "c"},
		},

		expressionTestCase{
			val: exp.Ex{"a": exp.Op{"between": exp.NewRangeVal("aaa", "zzz")}},
			sql: `("a" BETWEEN 'aaa' AND 'zzz')`,
		},
		expressionTestCase{
			val:        exp.Ex{"a": exp.Op{"between": exp.NewRangeVal("aaa", "zzz")}},
			sql:        `("a" BETWEEN ? AND ?)`,
			isPrepared: true,
			args:       []interface{}{"aaa", "zzz"},
		},

		expressionTestCase{
			val: exp.Ex{"a": exp.Op{"notBetween": exp.NewRangeVal("aaa", "zzz")}},
			sql: `("a" NOT BETWEEN 'aaa' AND 'zzz')`,
		},
		expressionTestCase{
			val:        exp.Ex{"a": exp.Op{"notBetween": exp.NewRangeVal("aaa", "zzz")}},
			sql:        `("a" NOT BETWEEN ? AND ?)`,
			isPrepared: true,
			args:       []interface{}{"aaa", "zzz"},
		},

		expressionTestCase{
			val: exp.Ex{"a": exp.Op{"is": nil, "eq": 10}},
			sql: `(("a" = 10) OR ("a" IS NULL))`,
		},
		expressionTestCase{
			val:        exp.Ex{"a": exp.Op{"is": nil, "eq": 10}},
			sql:        `(("a" = ?) OR ("a" IS ?))`,
			isPrepared: true,
			args:       []interface{}{int64(10), nil},
		},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_ExpressionOrMap() {
	esgs.assertCases(
		NewExpressionSQLGenerator("default", DefaultDialectOptions()),
		expressionTestCase{val: exp.ExOr{}},
		expressionTestCase{val: exp.ExOr{}, isPrepared: true},

		expressionTestCase{
			val: exp.ExOr{"a": exp.Op{"badOp": true}},
			err: "goqu: unsupported expression type map[badOp:%!s(bool=true)]",
		},
		expressionTestCase{
			val:        exp.ExOr{"a": exp.Op{"badOp": true}},
			isPrepared: true,
			err:        "goqu: unsupported expression type map[badOp:%!s(bool=true)]",
		},

		expressionTestCase{val: exp.ExOr{"a": 1, "b": true}, sql: `(("a" = 1) OR ("b" = TRUE))`},
		expressionTestCase{
			val:        exp.ExOr{"a": 1, "b": true},
			sql:        `(("a" = ?) OR ("b" = ?))`,
			isPrepared: true,
			args:       []interface{}{int64(1), true},
		},

		expressionTestCase{
			val: exp.ExOr{"a": 1, "b": []string{"a", "b", "c"}},
			sql: `(("a" = 1) OR ("b" IN ('a', 'b', 'c')))`,
		},
		expressionTestCase{
			val:        exp.ExOr{"a": 1, "b": []string{"a", "b", "c"}},
			sql:        `(("a" = ?) OR ("b" IN (?, ?, ?)))`,
			isPrepared: true,
			args:       []interface{}{int64(1), "a", "b", "c"},
		},
	)
}
func TestExpressionSQLGenerator(t *testing.T) {
	suite.Run(t, new(expressionSQLGeneratorSuite))
}
