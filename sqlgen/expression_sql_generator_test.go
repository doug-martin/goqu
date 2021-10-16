package sqlgen_test

import (
	"database/sql/driver"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/doug-martin/goqu/v9/exp"
	"github.com/doug-martin/goqu/v9/internal/errors"
	"github.com/doug-martin/goqu/v9/internal/sb"
	"github.com/doug-martin/goqu/v9/sqlgen"
	"github.com/stretchr/testify/suite"
)

var emptyArgs = make([]interface{}, 0)

type testAppendableExpression struct {
	sql            string
	args           []interface{}
	err            error
	alias          exp.IdentifierExpression
	returnsColumns bool
}

func newTestAppendableExpression(
	sql string,
	args []interface{},
	err error,
	alias exp.IdentifierExpression) exp.AppendableExpression {
	return &testAppendableExpression{sql: sql, args: args, err: err, alias: alias}
}

func (tae *testAppendableExpression) Expression() exp.Expression {
	return tae
}

func (tae *testAppendableExpression) Clone() exp.Expression {
	return tae
}

func (tae *testAppendableExpression) GetAs() exp.IdentifierExpression {
	return tae.alias
}

func (tae *testAppendableExpression) ReturnsColumns() bool {
	return tae.returnsColumns
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

func (esgs *expressionSQLGeneratorSuite) assertCases(esg sqlgen.ExpressionSQLGenerator, cases ...expressionTestCase) {
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
	esg := sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions())
	esgs.Equal("test", esg.Dialect())
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_ErroredBuilder() {
	esg := sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions())
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
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		expressionTestCase{val: b, sql: "NULL"},
		expressionTestCase{val: b, sql: "?", isPrepared: true, args: []interface{}{nil}},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_UnsupportedType() {
	type strct struct{}
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		expressionTestCase{val: strct{}, err: "goqu_encode_error: Unable to encode value {}"},
		expressionTestCase{val: strct{}, err: "goqu_encode_error: Unable to encode value {}", isPrepared: true},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_IncludePlaceholderNum() {
	opts := sqlgen.DefaultDialectOptions()
	opts.IncludePlaceholderNum = true
	opts.PlaceHolderFragment = []byte("$")
	ex := exp.Ex{
		"a": 1,
		"b": true,
		"c": false,
		"d": []string{"a", "b", "c"},
	}
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", opts),
		expressionTestCase{
			val: ex,
			sql: `(("a" = 1) AND ("b" IS TRUE) AND ("c" IS FALSE) AND ("d" IN ('a', 'b', 'c')))`,
		},
		expressionTestCase{
			val:        ex,
			sql:        `(("a" = $1) AND ("b" IS TRUE) AND ("c" IS FALSE) AND ("d" IN ($2, $3, $4)))`,
			isPrepared: true,
			args:       []interface{}{int64(1), "a", "b", "c"},
		},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_FloatTypes() {
	var float float64
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
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
			sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
			expressionTestCase{val: i, sql: "10"},
			expressionTestCase{val: i, sql: "?", isPrepared: true, args: []interface{}{int64(10)}},
		)
	}
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		expressionTestCase{val: &i, sql: "0"},
		expressionTestCase{val: &i, sql: "?", isPrepared: true, args: []interface{}{i}},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_StringTypes() {
	var str string
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
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
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		expressionTestCase{val: []byte("Hello"), sql: "'Hello'"},
		expressionTestCase{val: []byte("Hello"), sql: "?", isPrepared: true, args: []interface{}{[]byte("Hello")}},

		expressionTestCase{val: []byte("Hello'"), sql: "'Hello'''"},
		expressionTestCase{val: []byte("Hello'"), sql: "?", isPrepared: true, args: []interface{}{[]byte("Hello'")}},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_BoolTypes() {
	var bl bool
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
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

	ts, err := time.Parse(time.RFC3339, "2019-10-01T15:01:00Z")
	esgs.Require().NoError(err)
	originalLoc := sqlgen.GetTimeLocation()

	loc, err := time.LoadLocation("Asia/Shanghai")
	esgs.Require().NoError(err)

	sqlgen.SetTimeLocation(loc)
	// non time
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		expressionTestCase{val: ts, sql: "'2019-10-01T23:01:00+08:00'"},
		expressionTestCase{val: ts, sql: "?", isPrepared: true, args: []interface{}{ts}},

		expressionTestCase{val: &ts, sql: "'2019-10-01T23:01:00+08:00'"},
		expressionTestCase{val: &ts, sql: "?", isPrepared: true, args: []interface{}{ts}},
	)
	sqlgen.SetTimeLocation(time.UTC)
	// utc time
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		expressionTestCase{val: ts, sql: "'2019-10-01T15:01:00Z'"},
		expressionTestCase{val: ts, sql: "?", isPrepared: true, args: []interface{}{ts}},

		expressionTestCase{val: &ts, sql: "'2019-10-01T15:01:00Z'"},
		expressionTestCase{val: &ts, sql: "?", isPrepared: true, args: []interface{}{ts}},
	)
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		expressionTestCase{val: nt, sql: "NULL"},
		expressionTestCase{val: nt, sql: "?", isPrepared: true, args: []interface{}{nil}},
	)
	sqlgen.SetTimeLocation(originalLoc)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_NilTypes() {
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
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
	var val *datasetValuerType
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		expressionTestCase{val: datasetValuerType{int: 10}, sql: "'Hello World 10'"},
		expressionTestCase{
			val: datasetValuerType{int: 10}, sql: "?", isPrepared: true, args: []interface{}{[]byte("Hello World 10")},
		},

		expressionTestCase{val: datasetValuerType{err: err}, err: "goqu: valuer error"},
		expressionTestCase{
			val: datasetValuerType{err: err}, isPrepared: true, err: "goqu: valuer error",
		},
		expressionTestCase{
			val: val, sql: "NULL",
		},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_Slice() {
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
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

type unknownExpression struct{}

func (ue unknownExpression) Expression() exp.Expression {
	return ue
}

func (ue unknownExpression) Clone() exp.Expression {
	return ue
}

func (esgs *expressionSQLGeneratorSuite) TestGenerateUnsupportedExpression() {
	errMsg := "goqu: unsupported expression type sqlgen_test.unknownExpression"
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		expressionTestCase{val: unknownExpression{}, err: errMsg},
		expressionTestCase{
			val: unknownExpression{}, isPrepared: true, err: errMsg,
		},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_AppendableExpression() {
	ti := exp.NewIdentifierExpression("", "b", "")
	a := newTestAppendableExpression(`select * from "a"`, []interface{}{}, nil, nil)
	aliasedA := newTestAppendableExpression(`select * from "a"`, []interface{}{}, nil, ti)
	argsA := newTestAppendableExpression(`select * from "a" where x=?`, []interface{}{true}, nil, ti)
	ae := newTestAppendableExpression(`select * from "a"`, emptyArgs, errors.New("expected error"), nil)

	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
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
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
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
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
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
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
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
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		expressionTestCase{val: aliasedI, sql: `"a" AS "b"`},
		expressionTestCase{val: aliasedI, sql: `"a" AS "b"`, isPrepared: true},

		expressionTestCase{val: aliasedWithII, sql: `"a" AS "b"`},
		expressionTestCase{val: aliasedWithII, sql: `"a" AS "b"`, isPrepared: true},

		expressionTestCase{val: aliasedL, sql: `count(*) AS "count"`},
		expressionTestCase{val: aliasedL, sql: `count(*) AS "count"`, isPrepared: true},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_BooleanExpressionAliased() {
	ident := exp.NewIdentifierExpression("", "", "a")

	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		expressionTestCase{val: ident.Eq(1).As("b"), sql: `("a" = 1) AS "b"`},
		expressionTestCase{val: ident.Eq(1).As("b"), sql: `("a" = ?) AS "b"`,
			isPrepared: true, args: []interface{}{int64(1)}},
	)
}
func (esgs *expressionSQLGeneratorSuite) TestGenerate_BooleanExpression() {
	ae := newTestAppendableExpression(`SELECT "id" FROM "test2"`, emptyArgs, nil, nil)
	re := regexp.MustCompile("[ab]")
	ident := exp.NewIdentifierExpression("", "", "a")

	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		expressionTestCase{val: ident.Eq(1), sql: `("a" = 1)`},
		expressionTestCase{val: ident.Eq(1), sql: `("a" = ?)`, isPrepared: true, args: []interface{}{int64(1)}},

		expressionTestCase{val: ident.Eq(true), sql: `("a" IS TRUE)`},
		expressionTestCase{val: ident.Eq(true), sql: `("a" IS TRUE)`, isPrepared: true},

		expressionTestCase{val: ident.Eq(false), sql: `("a" IS FALSE)`},
		expressionTestCase{val: ident.Eq(false), sql: `("a" IS FALSE)`, isPrepared: true},

		expressionTestCase{val: ident.Eq(nil), sql: `("a" IS NULL)`},
		expressionTestCase{val: ident.Eq(nil), sql: `("a" IS NULL)`, isPrepared: true},

		expressionTestCase{val: ident.Eq([]int64{1, 2, 3}), sql: `("a" IN (1, 2, 3))`},
		expressionTestCase{val: ident.Eq([]int64{1, 2, 3}), sql: `("a" IN (?, ?, ?))`, isPrepared: true, args: []interface{}{
			int64(1), int64(2), int64(3),
		}},

		expressionTestCase{val: ident.Eq(ae), sql: `("a" IN (SELECT "id" FROM "test2"))`},
		expressionTestCase{val: ident.Eq(ae), sql: `("a" IN (SELECT "id" FROM "test2"))`, isPrepared: true},

		expressionTestCase{val: ident.Neq(1), sql: `("a" != 1)`},
		expressionTestCase{val: ident.Neq(1), sql: `("a" != ?)`, isPrepared: true, args: []interface{}{int64(1)}},

		expressionTestCase{val: ident.Neq(true), sql: `("a" IS NOT TRUE)`},
		expressionTestCase{val: ident.Neq(true), sql: `("a" IS NOT TRUE)`, isPrepared: true},

		expressionTestCase{val: ident.Neq(false), sql: `("a" IS NOT FALSE)`},
		expressionTestCase{val: ident.Neq(false), sql: `("a" IS NOT FALSE)`, isPrepared: true},

		expressionTestCase{val: ident.Neq(nil), sql: `("a" IS NOT NULL)`},
		expressionTestCase{val: ident.Neq(nil), sql: `("a" IS NOT NULL)`, isPrepared: true},

		expressionTestCase{val: ident.Neq([]int64{1, 2, 3}), sql: `("a" NOT IN (1, 2, 3))`},
		expressionTestCase{val: ident.Neq([]int64{1, 2, 3}), sql: `("a" NOT IN (?, ?, ?))`, isPrepared: true, args: []interface{}{
			int64(1), int64(2), int64(3),
		}},

		expressionTestCase{val: ident.Neq(ae), sql: `("a" NOT IN (SELECT "id" FROM "test2"))`},
		expressionTestCase{val: ident.Neq(ae), sql: `("a" NOT IN (SELECT "id" FROM "test2"))`, isPrepared: true},

		expressionTestCase{val: ident.Is(true), sql: `("a" IS TRUE)`},
		expressionTestCase{val: ident.Is(true), sql: `("a" IS TRUE)`, isPrepared: true},

		expressionTestCase{val: ident.Is(false), sql: `("a" IS FALSE)`},
		expressionTestCase{val: ident.Is(false), sql: `("a" IS FALSE)`, isPrepared: true},

		expressionTestCase{val: ident.Is(nil), sql: `("a" IS NULL)`},
		expressionTestCase{val: ident.Is(nil), sql: `("a" IS NULL)`, isPrepared: true},

		expressionTestCase{val: ident.IsNot(true), sql: `("a" IS NOT TRUE)`},
		expressionTestCase{val: ident.IsNot(true), sql: `("a" IS NOT TRUE)`, isPrepared: true},

		expressionTestCase{val: ident.IsNot(false), sql: `("a" IS NOT FALSE)`},
		expressionTestCase{val: ident.IsNot(false), sql: `("a" IS NOT FALSE)`, isPrepared: true},

		expressionTestCase{val: ident.IsNot(nil), sql: `("a" IS NOT NULL)`},
		expressionTestCase{val: ident.IsNot(nil), sql: `("a" IS NOT NULL)`, isPrepared: true},

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

		expressionTestCase{val: ident.Like(re), sql: `("a" ~ '[ab]')`},
		expressionTestCase{val: ident.Like(re), sql: `("a" ~ ?)`, isPrepared: true, args: []interface{}{"[ab]"}},

		expressionTestCase{val: ident.ILike("a%"), sql: `("a" ILIKE 'a%')`},
		expressionTestCase{val: ident.ILike("a%"), sql: `("a" ILIKE ?)`, isPrepared: true, args: []interface{}{"a%"}},

		expressionTestCase{val: ident.ILike(re), sql: `("a" ~* '[ab]')`},
		expressionTestCase{val: ident.ILike(re), sql: `("a" ~* ?)`, isPrepared: true, args: []interface{}{"[ab]"}},

		expressionTestCase{val: ident.NotLike("a%"), sql: `("a" NOT LIKE 'a%')`},
		expressionTestCase{val: ident.NotLike("a%"), sql: `("a" NOT LIKE ?)`, isPrepared: true, args: []interface{}{"a%"}},

		expressionTestCase{val: ident.NotLike(re), sql: `("a" !~ '[ab]')`},
		expressionTestCase{val: ident.NotLike(re), sql: `("a" !~ ?)`, isPrepared: true, args: []interface{}{"[ab]"}},

		expressionTestCase{val: ident.NotILike("a%"), sql: `("a" NOT ILIKE 'a%')`},
		expressionTestCase{val: ident.NotILike("a%"), sql: `("a" NOT ILIKE ?)`, isPrepared: true, args: []interface{}{"a%"}},

		expressionTestCase{val: ident.NotILike(re), sql: `("a" !~* '[ab]')`},
		expressionTestCase{val: ident.NotILike(re), sql: `("a" !~* ?)`, isPrepared: true, args: []interface{}{"[ab]"}},
	)

	opts := sqlgen.DefaultDialectOptions()
	opts.BooleanOperatorLookup = map[exp.BooleanOperation][]byte{}
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", opts),
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
		expressionTestCase{val: ident.Like(re), err: "goqu: boolean operator 'regexplike' not supported"},
		expressionTestCase{val: ident.ILike("a%"), err: "goqu: boolean operator 'ilike' not supported"},
		expressionTestCase{val: ident.ILike(re), err: "goqu: boolean operator 'regexpilike' not supported"},
		expressionTestCase{val: ident.NotLike("a%"), err: "goqu: boolean operator 'notlike' not supported"},
		expressionTestCase{val: ident.NotLike(re), err: "goqu: boolean operator 'regexpnotlike' not supported"},
		expressionTestCase{val: ident.NotILike("a%"), err: "goqu: boolean operator 'notilike' not supported"},
		expressionTestCase{val: ident.NotILike(re), err: "goqu: boolean operator 'regexpnotilike' not supported"},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_BitwiseExpression() {
	ident := exp.NewIdentifierExpression("", "", "a")
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		expressionTestCase{val: ident.BitwiseInversion(), sql: `(~ "a")`},
		expressionTestCase{val: ident.BitwiseInversion(), sql: `(~ "a")`, isPrepared: true},

		expressionTestCase{val: ident.BitwiseAnd(1), sql: `("a" & 1)`},
		expressionTestCase{val: ident.BitwiseAnd(1), sql: `("a" & ?)`, isPrepared: true, args: []interface{}{int64(1)}},

		expressionTestCase{val: ident.BitwiseOr(1), sql: `("a" | 1)`},
		expressionTestCase{val: ident.BitwiseOr(1), sql: `("a" | ?)`, isPrepared: true, args: []interface{}{int64(1)}},

		expressionTestCase{val: ident.BitwiseXor(1), sql: `("a" # 1)`},
		expressionTestCase{val: ident.BitwiseXor(1), sql: `("a" # ?)`, isPrepared: true, args: []interface{}{int64(1)}},

		expressionTestCase{val: ident.BitwiseLeftShift(1), sql: `("a" << 1)`},
		expressionTestCase{val: ident.BitwiseLeftShift(1), sql: `("a" << ?)`, isPrepared: true, args: []interface{}{int64(1)}},

		expressionTestCase{val: ident.BitwiseRightShift(1), sql: `("a" >> 1)`},
		expressionTestCase{val: ident.BitwiseRightShift(1), sql: `("a" >> ?)`, isPrepared: true, args: []interface{}{int64(1)}},
	)

	opts := sqlgen.DefaultDialectOptions()
	opts.BitwiseOperatorLookup = map[exp.BitwiseOperation][]byte{}
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", opts),
		expressionTestCase{val: ident.BitwiseInversion(), err: "goqu: bitwise operator 'Inversion' not supported"},
		expressionTestCase{val: ident.BitwiseAnd(1), err: "goqu: bitwise operator 'AND' not supported"},
		expressionTestCase{val: ident.BitwiseOr(1), err: "goqu: bitwise operator 'OR' not supported"},
		expressionTestCase{val: ident.BitwiseXor(1), err: "goqu: bitwise operator 'XOR' not supported"},
		expressionTestCase{val: ident.BitwiseLeftShift(1), err: "goqu: bitwise operator 'Left Shift' not supported"},
		expressionTestCase{val: ident.BitwiseRightShift(1), err: "goqu: bitwise operator 'Right Shift' not supported"},
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
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
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

	opts := sqlgen.DefaultDialectOptions()
	opts.RangeOperatorLookup = map[exp.RangeOperation][]byte{}
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", opts),
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
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
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
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		expressionTestCase{val: ue, sql: `"a"=1`},
		expressionTestCase{val: ue, sql: `"a"=?`, isPrepared: true, args: []interface{}{int64(1)}},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_SQLFunctionExpression() {
	min := exp.NewSQLFunctionExpression("MIN", exp.NewIdentifierExpression("", "", "a"))
	coalesce := exp.NewSQLFunctionExpression("COALESCE", exp.NewIdentifierExpression("", "", "a"), "a")
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
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
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		expressionTestCase{val: sqlWinFunc, sql: `some_func() OVER ("win")`},
		expressionTestCase{val: sqlWinFunc, sql: `some_func() OVER ("win")`, isPrepared: true},

		expressionTestCase{val: sqlWinFuncFromWindow, sql: `some_func() OVER "win"`},
		expressionTestCase{val: sqlWinFuncFromWindow, sql: `some_func() OVER "win"`, isPrepared: true},

		expressionTestCase{val: emptyWinFunc, sql: `some_func() OVER ()`},
		expressionTestCase{val: emptyWinFunc, sql: `some_func() OVER ()`, isPrepared: true},

		expressionTestCase{val: badNamedSQLWinFuncInherit, err: sqlgen.ErrUnexpectedNamedWindow.Error()},
		expressionTestCase{val: badNamedSQLWinFuncInherit, err: sqlgen.ErrUnexpectedNamedWindow.Error(), isPrepared: true},
	)
	opts := sqlgen.DefaultDialectOptions()
	opts.SupportsWindowFunction = false
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", opts),
		expressionTestCase{val: sqlWinFunc, err: sqlgen.ErrWindowNotSupported("test").Error()},
		expressionTestCase{val: sqlWinFunc, err: sqlgen.ErrWindowNotSupported("test").Error(), isPrepared: true},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_WindowExpression() {
	opts := sqlgen.DefaultDialectOptions()
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
		sqlgen.NewExpressionSQLGenerator("test", opts),
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

	opts = sqlgen.DefaultDialectOptions()
	opts.SupportsWindowFunction = false
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", opts),
		expressionTestCase{val: emptySQLWinFunc, err: sqlgen.ErrWindowNotSupported("test").Error()},
		expressionTestCase{val: emptySQLWinFunc, err: sqlgen.ErrWindowNotSupported("test").Error(), isPrepared: true},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_CastExpression() {
	cast := exp.NewIdentifierExpression("", "", "a").Cast("DATE")
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
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
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
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
	opts := sqlgen.DefaultDialectOptions()
	opts.SupportsWithCTE = false
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", opts),
		expressionTestCase{val: cteNoArgs, err: "goqu: dialect does not support CTE WITH clause [dialect=test]"},
		expressionTestCase{val: cteNoArgs, err: "goqu: dialect does not support CTE WITH clause [dialect=test]", isPrepared: true},

		expressionTestCase{val: cteArgs, err: "goqu: dialect does not support CTE WITH clause [dialect=test]"},
		expressionTestCase{val: cteArgs, err: "goqu: dialect does not support CTE WITH clause [dialect=test]", isPrepared: true},

		expressionTestCase{val: cteRecursiveNoArgs, err: "goqu: dialect does not support CTE WITH clause [dialect=test]"},
		expressionTestCase{val: cteRecursiveNoArgs, err: "goqu: dialect does not support CTE WITH clause [dialect=test]", isPrepared: true},

		expressionTestCase{val: cteRecursiveArgs, err: "goqu: dialect does not support CTE WITH clause [dialect=test]"},
		expressionTestCase{val: cteRecursiveArgs, err: "goqu: dialect does not support CTE WITH clause [dialect=test]", isPrepared: true},
	)
	opts = sqlgen.DefaultDialectOptions()
	opts.SupportsWithCTERecursive = false
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", opts),
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
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
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
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		expressionTestCase{val: u, sql: ` UNION (SELECT * FROM "b")`},
		expressionTestCase{val: u, sql: ` UNION (SELECT * FROM "b")`, isPrepared: true},

		expressionTestCase{val: ua, sql: ` UNION ALL (SELECT * FROM "b")`},
		expressionTestCase{val: ua, sql: ` UNION ALL (SELECT * FROM "b")`, isPrepared: true},

		expressionTestCase{val: i, sql: ` INTERSECT (SELECT * FROM "b")`},
		expressionTestCase{val: i, sql: ` INTERSECT (SELECT * FROM "b")`, isPrepared: true},

		expressionTestCase{val: ia, sql: ` INTERSECT ALL (SELECT * FROM "b")`},
		expressionTestCase{val: ia, sql: ` INTERSECT ALL (SELECT * FROM "b")`, isPrepared: true},
	)

	opts := sqlgen.DefaultDialectOptions()
	opts.WrapCompoundsInParens = false
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", opts),
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
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
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

func (esgs *expressionSQLGeneratorSuite) TestGenerate_LateralExpression() {
	lateralExp := exp.NewLateralExpression(newTestAppendableExpression(`SELECT * FROM "test"`, emptyArgs, nil, nil))

	do := sqlgen.DefaultDialectOptions()
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", do),
		expressionTestCase{val: lateralExp, sql: `LATERAL (SELECT * FROM "test")`},
		expressionTestCase{val: lateralExp, sql: `LATERAL (SELECT * FROM "test")`, isPrepared: true},
	)

	do = sqlgen.DefaultDialectOptions()
	do.LateralFragment = []byte("lateral ")
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", do),
		expressionTestCase{val: lateralExp, sql: `lateral (SELECT * FROM "test")`},
		expressionTestCase{val: lateralExp, sql: `lateral (SELECT * FROM "test")`, isPrepared: true},
	)
	do = sqlgen.DefaultDialectOptions()
	do.SupportsLateral = false
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", do),
		expressionTestCase{val: lateralExp, err: "goqu: dialect does not support lateral expressions [dialect=test]"},
		expressionTestCase{val: lateralExp, err: "goqu: dialect does not support lateral expressions [dialect=test]", isPrepared: true},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_CaseExpression() {
	ident := exp.NewIdentifierExpression("", "", "col")
	valueCase := exp.NewCaseExpression().
		Value(ident).
		When(true, "one").
		When(false, "two")
	valueElseCase := exp.NewCaseExpression().
		Value(ident).
		When(1, "one").
		When(2, "two").
		Else("three")
	searchCase := exp.NewCaseExpression().
		When(ident.Gt(1), exp.NewLiteralExpression("? - 1", ident)).
		When(ident.Lt(0), exp.NewLiteralExpression("? + 1", ident))
	searchElseCase := exp.NewCaseExpression().
		When(ident.Gt(1), exp.NewLiteralExpression("? - 1", ident)).
		When(ident.Lt(0), exp.NewLiteralExpression("? + 1", ident)).
		Else(ident)

	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		expressionTestCase{val: valueCase, sql: `CASE "col" WHEN TRUE THEN 'one' WHEN FALSE THEN 'two' END`},
		expressionTestCase{
			val:        valueCase,
			sql:        `CASE "col" WHEN ? THEN ? WHEN ? THEN ? END`,
			isPrepared: true,
			args:       []interface{}{true, "one", false, "two"},
		},

		expressionTestCase{val: valueElseCase, sql: `CASE "col" WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'three' END`},
		expressionTestCase{
			val:        valueElseCase,
			sql:        `CASE "col" WHEN ? THEN ? WHEN ? THEN ? ELSE ? END`,
			isPrepared: true,
			args:       []interface{}{int64(1), "one", int64(2), "two", "three"},
		},

		expressionTestCase{val: searchCase, sql: `CASE  WHEN ("col" > 1) THEN "col" - 1 WHEN ("col" < 0) THEN "col" + 1 END`},
		expressionTestCase{
			val:        searchCase,
			sql:        `CASE  WHEN ("col" > ?) THEN "col" - 1 WHEN ("col" < ?) THEN "col" + 1 END`,
			isPrepared: true,
			args:       []interface{}{int64(1), int64(0)},
		},

		expressionTestCase{val: searchElseCase, sql: `CASE  WHEN ("col" > 1) THEN "col" - 1 WHEN ("col" < 0) THEN "col" + 1 ELSE "col" END`},
		expressionTestCase{
			val:        searchElseCase,
			sql:        `CASE  WHEN ("col" > ?) THEN "col" - 1 WHEN ("col" < ?) THEN "col" + 1 ELSE "col" END`,
			isPrepared: true,
			args:       []interface{}{int64(1), int64(0)},
		},
		expressionTestCase{
			val: exp.NewCaseExpression(),
			err: "goqu: when conditions not found for case statement",
		},
	)

	opts := sqlgen.DefaultDialectOptions()
	opts.CaseFragment = []byte("case ")
	opts.WhenFragment = []byte(" when ")
	opts.ThenFragment = []byte(" then ")
	opts.ElseFragment = []byte(" else ")
	opts.EndFragment = []byte(" end")
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", opts),
		expressionTestCase{val: valueCase, sql: `case "col" when TRUE then 'one' when FALSE then 'two' end`},
		expressionTestCase{
			val:        valueCase,
			sql:        `case "col" when ? then ? when ? then ? end`,
			isPrepared: true,
			args:       []interface{}{true, "one", false, "two"},
		},

		expressionTestCase{val: valueElseCase, sql: `case "col" when 1 then 'one' when 2 then 'two' else 'three' end`},
		expressionTestCase{
			val:        valueElseCase,
			sql:        `case "col" when ? then ? when ? then ? else ? end`,
			isPrepared: true,
			args:       []interface{}{int64(1), "one", int64(2), "two", "three"},
		},

		expressionTestCase{val: searchCase, sql: `case  when ("col" > 1) then "col" - 1 when ("col" < 0) then "col" + 1 end`},
		expressionTestCase{
			val:        searchCase,
			sql:        `case  when ("col" > ?) then "col" - 1 when ("col" < ?) then "col" + 1 end`,
			isPrepared: true,
			args:       []interface{}{int64(1), int64(0)},
		},

		expressionTestCase{val: searchElseCase, sql: `case  when ("col" > 1) then "col" - 1 when ("col" < 0) then "col" + 1 else "col" end`},
		expressionTestCase{
			val:        searchElseCase,
			sql:        `case  when ("col" > ?) then "col" - 1 when ("col" < ?) then "col" + 1 else "col" end`,
			isPrepared: true,
			args:       []interface{}{int64(1), int64(0)},
		},
		expressionTestCase{
			val: exp.NewCaseExpression(),
			err: "goqu: when conditions not found for case statement",
		},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_ExpressionMap() {
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		expressionTestCase{val: exp.Ex{}},
		expressionTestCase{val: exp.Ex{}, isPrepared: true},

		expressionTestCase{val: exp.Ex{"a": 1}, sql: `("a" = 1)`},
		expressionTestCase{val: exp.Ex{"a": 1}, sql: `("a" = ?)`, isPrepared: true, args: []interface{}{int64(1)}},

		expressionTestCase{val: exp.Ex{"a": true}, sql: `("a" IS TRUE)`},
		expressionTestCase{val: exp.Ex{"a": true}, sql: `("a" IS TRUE)`, isPrepared: true},

		expressionTestCase{val: exp.Ex{"a": false}, sql: `("a" IS FALSE)`},
		expressionTestCase{val: exp.Ex{"a": false}, sql: `("a" IS FALSE)`, isPrepared: true},

		expressionTestCase{val: exp.Ex{"a": nil}, sql: `("a" IS NULL)`},
		expressionTestCase{val: exp.Ex{"a": nil}, sql: `("a" IS NULL)`, isPrepared: true},

		expressionTestCase{val: exp.Ex{"a": []string{"a", "b", "c"}}, sql: `("a" IN ('a', 'b', 'c'))`},
		expressionTestCase{
			val:        exp.Ex{"a": []string{"a", "b", "c"}},
			sql:        `("a" IN (?, ?, ?))`,
			isPrepared: true,
			args:       []interface{}{"a", "b", "c"},
		},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_ExpressionMapWithABadOp() {
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		expressionTestCase{
			val: exp.Ex{"a": exp.Op{"badOp": true}},
			err: "goqu: unsupported expression type badOp",
		},
		expressionTestCase{
			val:        exp.Ex{"a": exp.Op{"badOp": true}},
			isPrepared: true,
			err:        "goqu: unsupported expression type badOp",
		},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_ExpressionMapWithNeqOp() {
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		expressionTestCase{val: exp.Ex{"a": exp.Op{"neq": 1}}, sql: `("a" != 1)`},
		expressionTestCase{val: exp.Ex{"a": exp.Op{"neq": 1}}, sql: `("a" != ?)`, isPrepared: true, args: []interface{}{
			int64(1),
		}},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_ExpressionMapWithIsNotOp() {
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		expressionTestCase{val: exp.Ex{"a": exp.Op{"isnot": true}}, sql: `("a" IS NOT TRUE)`},
		expressionTestCase{val: exp.Ex{"a": exp.Op{"isnot": true}}, sql: `("a" IS NOT TRUE)`, isPrepared: true},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_ExpressionMapWithGtOp() {
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		expressionTestCase{val: exp.Ex{"a": exp.Op{"gt": 1}}, sql: `("a" > 1)`},
		expressionTestCase{val: exp.Ex{"a": exp.Op{"gt": 1}}, sql: `("a" > ?)`, isPrepared: true, args: []interface{}{
			int64(1),
		}},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_ExpressionMapWithGteOp() {
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		expressionTestCase{val: exp.Ex{"a": exp.Op{"gte": 1}}, sql: `("a" >= 1)`},
		expressionTestCase{val: exp.Ex{"a": exp.Op{"gte": 1}}, sql: `("a" >= ?)`, isPrepared: true, args: []interface{}{
			int64(1),
		}},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_ExpressionMapWithLtOp() {
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		expressionTestCase{val: exp.Ex{"a": exp.Op{"lt": 1}}, sql: `("a" < 1)`},
		expressionTestCase{val: exp.Ex{"a": exp.Op{"lt": 1}}, sql: `("a" < ?)`, isPrepared: true, args: []interface{}{
			int64(1),
		}},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_ExpressionMapWithLteOp() {
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		expressionTestCase{val: exp.Ex{"a": exp.Op{"lte": 1}}, sql: `("a" <= 1)`},
		expressionTestCase{val: exp.Ex{"a": exp.Op{"lte": 1}}, sql: `("a" <= ?)`, isPrepared: true, args: []interface{}{
			int64(1),
		}},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_ExpressionMapWithLikeOp() {
	re := regexp.MustCompile("[ab]")
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		expressionTestCase{val: exp.Ex{"a": exp.Op{"like": "a%"}}, sql: `("a" LIKE 'a%')`},
		expressionTestCase{
			val:        exp.Ex{"a": exp.Op{"like": "a%"}},
			sql:        `("a" LIKE ?)`,
			isPrepared: true,
			args:       []interface{}{"a%"},
		},

		expressionTestCase{val: exp.Ex{"a": exp.Op{"like": re}}, sql: `("a" ~ '[ab]')`},
		expressionTestCase{
			val:        exp.Ex{"a": exp.Op{"like": re}},
			sql:        `("a" ~ ?)`,
			isPrepared: true,
			args:       []interface{}{"[ab]"},
		},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_ExpressionMapWithNotLikeOp() {
	re := regexp.MustCompile("[ab]")
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		expressionTestCase{val: exp.Ex{"a": exp.Op{"notLike": "a%"}}, sql: `("a" NOT LIKE 'a%')`},
		expressionTestCase{
			val:        exp.Ex{"a": exp.Op{"notLike": "a%"}},
			sql:        `("a" NOT LIKE ?)`,
			isPrepared: true,
			args:       []interface{}{"a%"},
		},

		expressionTestCase{val: exp.Ex{"a": exp.Op{"notLike": re}}, sql: `("a" !~ '[ab]')`},
		expressionTestCase{
			val:        exp.Ex{"a": exp.Op{"notLike": re}},
			sql:        `("a" !~ ?)`,
			isPrepared: true,
			args:       []interface{}{"[ab]"},
		},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_ExpressionMapWithILikeOp() {
	re := regexp.MustCompile("[ab]")
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		expressionTestCase{val: exp.Ex{"a": exp.Op{"iLike": "a%"}}, sql: `("a" ILIKE 'a%')`},
		expressionTestCase{
			val:        exp.Ex{"a": exp.Op{"iLike": "a%"}},
			sql:        `("a" ILIKE ?)`,
			isPrepared: true,
			args:       []interface{}{"a%"},
		},

		expressionTestCase{val: exp.Ex{"a": exp.Op{"iLike": re}}, sql: `("a" ~* '[ab]')`},
		expressionTestCase{
			val:        exp.Ex{"a": exp.Op{"iLike": re}},
			sql:        `("a" ~* ?)`,
			isPrepared: true,
			args:       []interface{}{"[ab]"},
		},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_ExpressionMapWithNotILikeOp() {
	re := regexp.MustCompile("[ab]")
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		expressionTestCase{val: exp.Ex{"a": exp.Op{"notILike": "a%"}}, sql: `("a" NOT ILIKE 'a%')`},
		expressionTestCase{
			val:        exp.Ex{"a": exp.Op{"notILike": "a%"}},
			sql:        `("a" NOT ILIKE ?)`,
			isPrepared: true,
			args:       []interface{}{"a%"},
		},

		expressionTestCase{val: exp.Ex{"a": exp.Op{"notILike": re}}, sql: `("a" !~* '[ab]')`},
		expressionTestCase{
			val:        exp.Ex{"a": exp.Op{"notILike": re}},
			sql:        `("a" !~* ?)`,
			isPrepared: true,
			args:       []interface{}{"[ab]"},
		},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_ExpressionMapWithRegExpLikeOp() {
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),

		expressionTestCase{val: exp.Ex{"a": exp.Op{"regexpLike": "[ab]"}}, sql: `("a" ~ '[ab]')`},
		expressionTestCase{
			val:        exp.Ex{"a": exp.Op{"regexpLike": "[ab]"}},
			sql:        `("a" ~ ?)`,
			isPrepared: true,
			args:       []interface{}{"[ab]"},
		},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_ExpressionMapWithRegExpILikeOp() {
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		expressionTestCase{val: exp.Ex{"a": exp.Op{"regexpILike": "[ab]"}}, sql: `("a" ~* '[ab]')`},
		expressionTestCase{
			val:        exp.Ex{"a": exp.Op{"regexpILike": "[ab]"}},
			sql:        `("a" ~* ?)`,
			isPrepared: true,
			args:       []interface{}{"[ab]"},
		},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_ExpressionMapWithRegExpNotLikeOp() {
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		expressionTestCase{val: exp.Ex{"a": exp.Op{"regexpNotLike": "[ab]"}}, sql: `("a" !~ '[ab]')`},
		expressionTestCase{
			val:        exp.Ex{"a": exp.Op{"regexpNotLike": "[ab]"}},
			sql:        `("a" !~ ?)`,
			isPrepared: true,
			args:       []interface{}{"[ab]"},
		},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_ExpressionMapWithInOp() {
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		expressionTestCase{val: exp.Ex{"a": exp.Op{"in": []string{"a", "b", "c"}}}, sql: `("a" IN ('a', 'b', 'c'))`},
		expressionTestCase{
			val:        exp.Ex{"a": exp.Op{"in": []string{"a", "b", "c"}}},
			sql:        `("a" IN (?, ?, ?))`,
			isPrepared: true,
			args:       []interface{}{"a", "b", "c"},
		},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_ExpressionMapWithNotInOp() {
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
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
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_ExpressionMapBetweenOp() {
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
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
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_ExpressionMapNotBetweenOp() {
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
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
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_ExpressionMapIsOp() {
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		expressionTestCase{
			val: exp.Ex{"a": exp.Op{"is": nil, "eq": 10}},
			sql: `(("a" = 10) OR ("a" IS NULL))`,
		},
		expressionTestCase{
			val:        exp.Ex{"a": exp.Op{"is": nil, "eq": 10}},
			sql:        `(("a" = ?) OR ("a" IS NULL))`,
			isPrepared: true,
			args:       []interface{}{int64(10)},
		},
	)
}

func (esgs *expressionSQLGeneratorSuite) TestGenerate_ExpressionOrMap() {
	esgs.assertCases(
		sqlgen.NewExpressionSQLGenerator("default", sqlgen.DefaultDialectOptions()),
		expressionTestCase{val: exp.ExOr{}},
		expressionTestCase{val: exp.ExOr{}, isPrepared: true},

		expressionTestCase{val: exp.ExOr{"a": exp.Op{"regexpLike": "[ab]"}}, sql: `("a" ~ '[ab]')`},
		expressionTestCase{
			val:        exp.ExOr{"a": exp.Op{"regexpLike": "[ab]"}},
			sql:        `("a" ~ ?)`,
			isPrepared: true,
			args:       []interface{}{"[ab]"},
		},

		expressionTestCase{val: exp.ExOr{"a": exp.Op{"regexpNotLike": "[ab]"}}, sql: `("a" !~ '[ab]')`},
		expressionTestCase{
			val:        exp.ExOr{"a": exp.Op{"regexpNotLike": "[ab]"}},
			sql:        `("a" !~ ?)`,
			isPrepared: true,
			args:       []interface{}{"[ab]"},
		},

		expressionTestCase{val: exp.ExOr{"a": exp.Op{"regexpILike": "[ab]"}}, sql: `("a" ~* '[ab]')`},
		expressionTestCase{
			val:        exp.ExOr{"a": exp.Op{"regexpILike": "[ab]"}},
			sql:        `("a" ~* ?)`,
			isPrepared: true,
			args:       []interface{}{"[ab]"},
		},
		expressionTestCase{val: exp.ExOr{"a": exp.Op{"regexpNotILike": "[ab]"}}, sql: `("a" !~* '[ab]')`},
		expressionTestCase{
			val:        exp.ExOr{"a": exp.Op{"regexpNotILike": "[ab]"}},
			sql:        `("a" !~* ?)`,
			isPrepared: true,
			args:       []interface{}{"[ab]"},
		},

		expressionTestCase{
			val: exp.ExOr{"a": exp.Op{"badOp": true}},
			err: "goqu: unsupported expression type badOp",
		},
		expressionTestCase{
			val:        exp.ExOr{"a": exp.Op{"badOp": true}},
			isPrepared: true,
			err:        "goqu: unsupported expression type badOp",
		},

		expressionTestCase{val: exp.ExOr{"a": 1, "b": true}, sql: `(("a" = 1) OR ("b" IS TRUE))`},
		expressionTestCase{
			val:        exp.ExOr{"a": 1, "b": true},
			sql:        `(("a" = ?) OR ("b" IS TRUE))`,
			isPrepared: true,
			args:       []interface{}{int64(1)},
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
