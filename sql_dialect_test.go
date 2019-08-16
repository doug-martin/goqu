package goqu

import (
	"database/sql/driver"
	"fmt"
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

type dialectTestSuite struct {
	suite.Suite
}

func (dts *dialectTestSuite) assertNotPreparedSQL(b sb.SQLBuilder, expectedSQL string) {
	actualSQL, actualArgs, err := b.ToSQL()
	dts.NoError(err)
	dts.Equal(expectedSQL, actualSQL)
	dts.Empty(actualArgs)
}

func (dts *dialectTestSuite) assertPreparedSQL(
	b sb.SQLBuilder,
	expectedSQL string,
	expectedArgs []interface{},
) {
	actualSQL, actualArgs, err := b.ToSQL()
	dts.NoError(err)
	dts.Equal(expectedSQL, actualSQL)
	dts.Equal(expectedArgs, actualArgs)
}

func (dts *dialectTestSuite) assertErrorSQL(b sb.SQLBuilder, errMsg string) {
	actualSQL, actualArgs, err := b.ToSQL()
	dts.EqualError(err, errMsg)
	dts.Empty(actualSQL)
	dts.Empty(actualArgs)
}

func (dts *dialectTestSuite) TestSupportsReturn() {
	opts := DefaultDialectOptions()
	opts.SupportsReturn = true
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	opts2 := DefaultDialectOptions()
	opts2.SupportsReturn = false
	d2 := sqlDialect{dialect: "test", dialectOptions: opts2}

	dts.True(d.SupportsReturn())
	dts.False(d2.SupportsReturn())
}

func (dts *dialectTestSuite) TestSupportsOrderByOnUpdate() {
	opts := DefaultDialectOptions()
	opts.SupportsOrderByOnUpdate = true
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	opts2 := DefaultDialectOptions()
	opts2.SupportsOrderByOnUpdate = false
	d2 := sqlDialect{dialect: "test", dialectOptions: opts2}

	dts.True(d.SupportsOrderByOnUpdate())
	dts.False(d2.SupportsOrderByOnUpdate())
}

func (dts *dialectTestSuite) TestSupportsLimitOnUpdate() {
	opts := DefaultDialectOptions()
	opts.SupportsLimitOnUpdate = true
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	opts2 := DefaultDialectOptions()
	opts2.SupportsLimitOnUpdate = false
	d2 := sqlDialect{dialect: "test", dialectOptions: opts2}

	dts.True(d.SupportsLimitOnUpdate())
	dts.False(d2.SupportsLimitOnUpdate())
}

func (dts *dialectTestSuite) TestSupportsOrderByOnDelete() {
	opts := DefaultDialectOptions()
	opts.SupportsOrderByOnDelete = true
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	opts2 := DefaultDialectOptions()
	opts2.SupportsOrderByOnDelete = false
	d2 := sqlDialect{dialect: "test", dialectOptions: opts2}

	dts.True(d.SupportsOrderByOnDelete())
	dts.False(d2.SupportsOrderByOnDelete())
}

func (dts *dialectTestSuite) TestSupportsLimitOnDelete() {
	opts := DefaultDialectOptions()
	opts.SupportsLimitOnDelete = true
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	opts2 := DefaultDialectOptions()
	opts2.SupportsLimitOnDelete = false
	d2 := sqlDialect{dialect: "test", dialectOptions: opts2}

	dts.True(d.SupportsLimitOnDelete())
	dts.False(d2.SupportsLimitOnDelete())
}

func (dts *dialectTestSuite) TestToTruncateSQL() {
	opts := DefaultDialectOptions()
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	opts2 := DefaultDialectOptions()
	opts2.TruncateClause = []byte("truncate")
	d2 := sqlDialect{dialect: "test", dialectOptions: opts2}

	tables := exp.NewColumnListExpression("a")
	tc := exp.NewTruncateClauses().SetTable(tables)
	b := sb.NewSQLBuilder(false)

	d.ToTruncateSQL(b, tc)
	dts.assertNotPreparedSQL(b, `TRUNCATE "a"`)

	d2.ToTruncateSQL(b.Clear(), tc)
	dts.assertNotPreparedSQL(b, `truncate "a"`)

	b = sb.NewSQLBuilder(true)
	d.ToTruncateSQL(b, tc)
	dts.assertPreparedSQL(b, `TRUNCATE "a"`, emptyArgs)

	d2.ToTruncateSQL(b.Clear(), tc)
	dts.assertPreparedSQL(b, `truncate "a"`, emptyArgs)
}

func (dts *dialectTestSuite) TestToTruncateSQL_UnsupportedFragment() {
	opts := DefaultDialectOptions()
	opts.TruncateSQLOrder = []SQLFragmentType{UpdateBeginSQLFragment}
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	b := sb.NewSQLBuilder(true)
	d.ToTruncateSQL(b, exp.NewTruncateClauses().SetTable(exp.NewColumnListExpression("a")))
	dts.assertErrorSQL(b, `goqu: unsupported TRUNCATE SQL fragment UpdateBeginSQLFragment`)
}

func (dts *dialectTestSuite) TestToTruncateSQL_WithErroredBuilder() {
	opts := DefaultDialectOptions()
	opts.TruncateSQLOrder = []SQLFragmentType{UpdateBeginSQLFragment}
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	b := sb.NewSQLBuilder(true).SetError(errors.New("expected error"))
	d.ToTruncateSQL(b, exp.NewTruncateClauses().SetTable(exp.NewColumnListExpression("a")))
	dts.assertErrorSQL(b, `goqu: expected error`)
}

func (dts *dialectTestSuite) TestToTruncateSQL_withoutTable() {
	opts := DefaultDialectOptions()
	d := sqlDialect{dialect: "test", dialectOptions: opts}
	b := sb.NewSQLBuilder(false)

	d.ToTruncateSQL(b, exp.NewTruncateClauses())
	dts.assertErrorSQL(b, "goqu: no source found when generating truncate sql")
}

func (dts *dialectTestSuite) TestToTruncateSQL_WithCascade() {
	opts := DefaultDialectOptions()
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	opts2 := DefaultDialectOptions()
	opts2.TruncateClause = []byte("truncate")
	opts2.CascadeFragment = []byte(" cascade")
	d2 := sqlDialect{dialect: "test", dialectOptions: opts2}

	tables := exp.NewColumnListExpression("a")
	tc := exp.NewTruncateClauses().SetTable(tables)
	b := sb.NewSQLBuilder(false)

	d.ToTruncateSQL(b.Clear(), tc.SetOptions(exp.TruncateOptions{Cascade: true}))
	dts.assertNotPreparedSQL(b, `TRUNCATE "a" CASCADE`)

	d2.ToTruncateSQL(b.Clear(), tc.SetOptions(exp.TruncateOptions{Cascade: true}))
	dts.assertNotPreparedSQL(b, `truncate "a" cascade`)

	b = sb.NewSQLBuilder(true)

	d.ToTruncateSQL(b.Clear(), tc.SetOptions(exp.TruncateOptions{Cascade: true}))
	dts.assertPreparedSQL(b, `TRUNCATE "a" CASCADE`, emptyArgs)

	d2.ToTruncateSQL(b.Clear(), tc.SetOptions(exp.TruncateOptions{Cascade: true}))
	dts.assertPreparedSQL(b, `truncate "a" cascade`, emptyArgs)
}

func (dts *dialectTestSuite) TestToTruncateSQL_WithRestrict() {
	opts := DefaultDialectOptions()
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	opts2 := DefaultDialectOptions()
	opts2.TruncateClause = []byte("truncate")
	opts2.RestrictFragment = []byte(" restrict")
	d2 := sqlDialect{dialect: "test", dialectOptions: opts2}

	tables := exp.NewColumnListExpression("a")
	tc := exp.NewTruncateClauses().SetTable(tables)
	b := sb.NewSQLBuilder(false)

	d.ToTruncateSQL(b.Clear(), tc.SetOptions(exp.TruncateOptions{Restrict: true}))
	dts.assertNotPreparedSQL(b, `TRUNCATE "a" RESTRICT`)

	d2.ToTruncateSQL(b.Clear(), tc.SetOptions(exp.TruncateOptions{Restrict: true}))
	dts.assertNotPreparedSQL(b, `truncate "a" restrict`)

	b = sb.NewSQLBuilder(true)

	d.ToTruncateSQL(b.Clear(), tc.SetOptions(exp.TruncateOptions{Restrict: true}))
	dts.assertPreparedSQL(b, `TRUNCATE "a" RESTRICT`, emptyArgs)

	d2.ToTruncateSQL(b.Clear(), tc.SetOptions(exp.TruncateOptions{Restrict: true}))
	dts.assertPreparedSQL(b, `truncate "a" restrict`, emptyArgs)
}

func (dts *dialectTestSuite) TestToTruncateSQL_WithRestart() {
	opts := DefaultDialectOptions()
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	opts2 := DefaultDialectOptions()
	opts2.TruncateClause = []byte("truncate")
	opts2.IdentityFragment = []byte(" identity")
	d2 := sqlDialect{dialect: "test", dialectOptions: opts2}

	tables := exp.NewColumnListExpression("a")
	tc := exp.NewTruncateClauses().SetTable(tables)
	b := sb.NewSQLBuilder(false)

	d.ToTruncateSQL(b.Clear(), tc.SetOptions(exp.TruncateOptions{Identity: "restart"}))
	dts.assertNotPreparedSQL(b, `TRUNCATE "a" RESTART IDENTITY`)

	d2.ToTruncateSQL(b.Clear(), tc.SetOptions(exp.TruncateOptions{Identity: "restart"}))
	dts.assertNotPreparedSQL(b, `truncate "a" RESTART identity`)

	b = sb.NewSQLBuilder(true)

	d.ToTruncateSQL(b.Clear(), tc.SetOptions(exp.TruncateOptions{Identity: "restart"}))
	dts.assertPreparedSQL(b, `TRUNCATE "a" RESTART IDENTITY`, emptyArgs)

	d2.ToTruncateSQL(b.Clear(), tc.SetOptions(exp.TruncateOptions{Identity: "restart"}))
	dts.assertPreparedSQL(b, `truncate "a" RESTART identity`, emptyArgs)
}

func (dts *dialectTestSuite) TestToInsertSQL_UnsupportedFragment() {
	opts := DefaultDialectOptions()
	opts.InsertSQLOrder = []SQLFragmentType{UpdateBeginSQLFragment}
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	b := sb.NewSQLBuilder(true)
	ic := exp.NewInsertClauses().
		SetInto(exp.NewIdentifierExpression("", "test", ""))
	d.ToInsertSQL(b, ic)
	dts.assertErrorSQL(b, `goqu: unsupported INSERT SQL fragment UpdateBeginSQLFragment`)
}

func (dts *dialectTestSuite) TestToInsertSQL_empty() {
	opts := DefaultDialectOptions()
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	opts2 := DefaultDialectOptions()
	opts2.DefaultValuesFragment = []byte(" default values")
	d2 := sqlDialect{dialect: "test", dialectOptions: opts2}
	ic := exp.NewInsertClauses().
		SetInto(exp.NewIdentifierExpression("", "test", ""))

	b := sb.NewSQLBuilder(false)
	d.ToInsertSQL(b, ic)
	dts.assertNotPreparedSQL(b, `INSERT INTO "test" DEFAULT VALUES`)

	d2.ToInsertSQL(b.Clear(), ic)
	dts.assertNotPreparedSQL(b, `INSERT INTO "test" default values`)
}

func (dts *dialectTestSuite) TestToInsertSQL_nilValues() {
	opts := DefaultDialectOptions()
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	opts2 := DefaultDialectOptions()
	d2 := sqlDialect{dialect: "test", dialectOptions: opts2}

	ic := exp.NewInsertClauses().
		SetInto(exp.NewIdentifierExpression("", "test", "")).
		SetCols(exp.NewColumnListExpression("a")).
		SetVals([][]interface{}{
			{nil},
		})

	b := sb.NewSQLBuilder(false)
	d.ToInsertSQL(b, ic)
	dts.assertNotPreparedSQL(b, `INSERT INTO "test" ("a") VALUES (NULL)`)

	d2.ToInsertSQL(b.Clear(), ic)
	dts.assertNotPreparedSQL(b, `INSERT INTO "test" ("a") VALUES (NULL)`)
}

func (dts *dialectTestSuite) TestToInsertSQL_colsAndVals() {
	opts := DefaultDialectOptions()
	opts.LeftParenRune = '{'
	opts.RightParenRune = '}'
	opts.ValuesFragment = []byte(" values ")
	opts.LeftParenRune = '{'
	opts.RightParenRune = '}'
	opts.CommaRune = ';'
	opts.PlaceHolderRune = '#'
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	ic := exp.NewInsertClauses().
		SetInto(exp.NewIdentifierExpression("", "test", "")).
		SetCols(exp.NewColumnListExpression("a", "b")).
		SetVals([][]interface{}{
			{"a1", "b1"},
			{"a2", "b2"},
			{"a3", "b3"},
		})

	bic := ic.SetCols(exp.NewColumnListExpression("a", "b")).
		SetVals([][]interface{}{
			{"a1"},
			{"a2", "b2"},
			{"a3", "b3"},
		})

	b := sb.NewSQLBuilder(false)
	d.ToInsertSQL(b, ic)
	dts.assertNotPreparedSQL(b, `INSERT INTO "test" {"a"; "b"} values {'a1'; 'b1'}; {'a2'; 'b2'}; {'a3'; 'b3'}`)

	b = sb.NewSQLBuilder(true)
	d.ToInsertSQL(b, ic)
	dts.assertPreparedSQL(b, `INSERT INTO "test" {"a"; "b"} values {#; #}; {#; #}; {#; #}`, []interface{}{
		"a1", "b1", "a2", "b2", "a3", "b3",
	})

	d.ToInsertSQL(b.Clear(), bic)
	dts.assertErrorSQL(b, "goqu: rows with different value length expected 1 got 2")
}

func (dts *dialectTestSuite) TestToInsertSQL_withNoInto() {
	opts := DefaultDialectOptions()
	opts.LeftParenRune = '{'
	opts.RightParenRune = '}'
	opts.ValuesFragment = []byte(" values ")
	opts.LeftParenRune = '{'
	opts.RightParenRune = '}'
	opts.CommaRune = ';'
	opts.PlaceHolderRune = '#'
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	ic := exp.NewInsertClauses().
		SetCols(exp.NewColumnListExpression("a", "b")).
		SetVals([][]interface{}{
			{"a1", "b1"},
			{"a2", "b2"},
			{"a3", "b3"},
		})

	b := sb.NewSQLBuilder(false)
	d.ToInsertSQL(b.Clear(), ic)
	dts.assertErrorSQL(b, "goqu: no source found when generating insert sql")
}

func (dts *dialectTestSuite) TestToInsertSQL_withRows() {
	opts := DefaultDialectOptions()
	opts.LeftParenRune = '{'
	opts.RightParenRune = '}'
	opts.ValuesFragment = []byte(" values ")
	opts.LeftParenRune = '{'
	opts.RightParenRune = '}'
	opts.CommaRune = ';'
	opts.PlaceHolderRune = '#'
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	ic := exp.NewInsertClauses().
		SetInto(exp.NewIdentifierExpression("", "test", "")).
		SetRows([]interface{}{
			exp.Record{"a": "a1", "b": "b1"},
			exp.Record{"a": "a2", "b": "b2"},
			exp.Record{"a": "a3", "b": "b3"},
		})

	bic := ic.
		SetRows([]interface{}{
			exp.Record{"a": "a1"},
			exp.Record{"a": "a2", "b": "b2"},
			exp.Record{"a": "a3", "b": "b3"},
		})

	b := sb.NewSQLBuilder(false)
	d.ToInsertSQL(b, ic)
	dts.assertNotPreparedSQL(b, `INSERT INTO "test" {"a"; "b"} values {'a1'; 'b1'}; {'a2'; 'b2'}; {'a3'; 'b3'}`)

	b = sb.NewSQLBuilder(true)
	d.ToInsertSQL(b, ic)
	dts.assertPreparedSQL(b, `INSERT INTO "test" {"a"; "b"} values {#; #}; {#; #}; {#; #}`, []interface{}{
		"a1", "b1", "a2", "b2", "a3", "b3",
	})

	d.ToInsertSQL(b.Clear(), bic)
	dts.assertErrorSQL(b, "goqu: rows with different value length expected 1 got 2")
}

func (dts *dialectTestSuite) TestToInsertSQL_withRowsAppendableExpression() {
	opts := DefaultDialectOptions()
	opts.LeftParenRune = '{'
	opts.RightParenRune = '}'
	opts.ValuesFragment = []byte(" values ")
	opts.LeftParenRune = '{'
	opts.RightParenRune = '}'
	opts.CommaRune = ';'
	opts.PlaceHolderRune = '#'
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	ic := exp.NewInsertClauses().
		SetInto(exp.NewIdentifierExpression("", "test", "")).
		SetRows([]interface{}{newTestAppendableExpression(`select * from "other"`, emptyArgs, nil, nil)})

	b := sb.NewSQLBuilder(false)
	d.ToInsertSQL(b, ic)
	dts.assertNotPreparedSQL(b, `INSERT INTO "test" select * from "other"`)

	b = sb.NewSQLBuilder(true)
	d.ToInsertSQL(b, ic)
	dts.assertPreparedSQL(b, `INSERT INTO "test" select * from "other"`, emptyArgs)
}

func (dts *dialectTestSuite) TestToInsertSQL_withFrom() {
	opts := DefaultDialectOptions()
	opts.LeftParenRune = '{'
	opts.RightParenRune = '}'
	opts.ValuesFragment = []byte(" values ")
	opts.LeftParenRune = '{'
	opts.RightParenRune = '}'
	opts.CommaRune = ';'
	opts.PlaceHolderRune = '#'
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	ic := exp.NewInsertClauses().
		SetInto(exp.NewIdentifierExpression("", "test", "")).
		SetFrom(newTestAppendableExpression(`select c, d from test where a = 'b'`, nil, nil, nil))

	b := sb.NewSQLBuilder(false)
	d.ToInsertSQL(b.Clear(), ic)
	dts.assertNotPreparedSQL(b, `INSERT INTO "test" select c, d from test where a = 'b'`)

	b = sb.NewSQLBuilder(true)
	d.ToInsertSQL(b.Clear(), ic)
	dts.assertPreparedSQL(b, `INSERT INTO "test" select c, d from test where a = 'b'`, emptyArgs)

	ic = ic.SetCols(exp.NewColumnListExpression("a", "b"))

	b = sb.NewSQLBuilder(false)
	d.ToInsertSQL(b.Clear(), ic)
	dts.assertNotPreparedSQL(b, `INSERT INTO "test" {"a"; "b"} select c, d from test where a = 'b'`)

	b = sb.NewSQLBuilder(true)
	d.ToInsertSQL(b.Clear(), ic)
	dts.assertPreparedSQL(b, `INSERT INTO "test" {"a"; "b"} select c, d from test where a = 'b'`, emptyArgs)
}

func (dts *dialectTestSuite) TestToInsertSQL_onConflict() {
	opts := DefaultDialectOptions()
	// make sure the fragments are used
	opts.ConflictFragment = []byte(" on conflict")
	opts.ConflictDoNothingFragment = []byte(" do nothing")
	opts.ConflictDoUpdateFragment = []byte(" do update set ")
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	icnoc := exp.NewInsertClauses().
		SetInto(exp.NewIdentifierExpression("", "test", "")).
		SetCols(exp.NewColumnListExpression("a")).
		SetVals([][]interface{}{
			{"a1"},
			{"a2"},
			{"a3"},
		})

	icdn := icnoc.SetOnConflict(DoNothing())
	icdu := icnoc.SetOnConflict(DoUpdate("test", exp.Record{"a": "b"}))
	icdoc := icnoc.SetOnConflict(DoUpdate("on constraint test", exp.Record{"a": "b"}))
	icduw := icnoc.SetOnConflict(
		exp.NewDoUpdateConflictExpression("test", exp.Record{"a": "b"}).Where(exp.Ex{"foo": true}),
	)

	b := sb.NewSQLBuilder(false)
	d.ToInsertSQL(b, icnoc)
	dts.assertNotPreparedSQL(b, `INSERT INTO "test" ("a") VALUES ('a1'), ('a2'), ('a3')`)

	d.ToInsertSQL(b.Clear(), icdn)
	dts.assertNotPreparedSQL(b, `INSERT INTO "test" ("a") VALUES ('a1'), ('a2'), ('a3') on conflict do nothing`)

	d.ToInsertSQL(b.Clear(), icdu)
	dts.assertNotPreparedSQL(
		b,
		`INSERT INTO "test" ("a") VALUES ('a1'), ('a2'), ('a3') on conflict (test) do update set "a"='b'`,
	)

	d.ToInsertSQL(b.Clear(), icdoc)
	dts.assertNotPreparedSQL(
		b,
		`INSERT INTO "test" ("a") VALUES ('a1'), ('a2'), ('a3') on conflict on constraint test do update set "a"='b'`,
	)

	d.ToInsertSQL(b.Clear(), icduw)
	dts.assertNotPreparedSQL(b,
		`INSERT INTO "test" ("a") VALUES ('a1'), ('a2'), ('a3') on conflict (test) do update set "a"='b' WHERE ("foo" IS TRUE)`,
	)

	b = sb.NewSQLBuilder(true)
	d.ToInsertSQL(b, icdn)
	dts.assertPreparedSQL(b, `INSERT INTO "test" ("a") VALUES (?), (?), (?) on conflict do nothing`, []interface{}{
		"a1", "a2", "a3",
	})

	d.ToInsertSQL(b.Clear(), icdu)
	dts.assertPreparedSQL(
		b,
		`INSERT INTO "test" ("a") VALUES (?), (?), (?) on conflict (test) do update set "a"=?`,
		[]interface{}{"a1", "a2", "a3", "b"},
	)

	d.ToInsertSQL(b.Clear(), icduw)
	dts.assertPreparedSQL(
		b,
		`INSERT INTO "test" ("a") VALUES (?), (?), (?) on conflict (test) do update set "a"=? WHERE ("foo" IS TRUE)`,
		[]interface{}{"a1", "a2", "a3", "b"},
	)
}

func (dts *dialectTestSuite) TestToInsertSQL_withSupportsInsertIgnoreSyntax() {
	opts := DefaultDialectOptions()
	// make sure the fragments are used
	opts.SupportsInsertIgnoreSyntax = true
	opts.InsertIgnoreClause = []byte("insert ignore into")
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	icnoc := exp.NewInsertClauses().
		SetInto(exp.NewIdentifierExpression("", "test", "")).
		SetCols(exp.NewColumnListExpression("a")).
		SetVals([][]interface{}{
			{"a1"},
			{"a2"},
			{"a3"},
		})

	icdn := icnoc.SetOnConflict(DoNothing())
	icdu := icnoc.SetOnConflict(DoUpdate("test", exp.Record{"a": "b"}))
	icdoc := icnoc.SetOnConflict(DoUpdate("on constraint test", exp.Record{"a": "b"}))
	icduw := icnoc.SetOnConflict(
		exp.NewDoUpdateConflictExpression("test", exp.Record{"a": "b"}).Where(exp.Ex{"foo": true}),
	)

	b := sb.NewSQLBuilder(false)

	d.ToInsertSQL(b.Clear(), icdu)
	dts.assertNotPreparedSQL(
		b,
		`insert ignore into "test" ("a") VALUES ('a1'), ('a2'), ('a3') ON CONFLICT (test) DO UPDATE SET "a"='b'`,
	)

	d.ToInsertSQL(b.Clear(), icdoc)
	dts.assertNotPreparedSQL(
		b,
		`insert ignore into "test" ("a") VALUES ('a1'), ('a2'), ('a3') ON CONFLICT on constraint test DO UPDATE SET "a"='b'`,
	)

	d.ToInsertSQL(b.Clear(), icduw)
	dts.assertNotPreparedSQL(b,
		`insert ignore into "test" ("a") VALUES ('a1'), ('a2'), ('a3') ON CONFLICT (test) DO UPDATE SET "a"='b' WHERE ("foo" IS TRUE)`,
	)

	b = sb.NewSQLBuilder(true)
	d.ToInsertSQL(b, icdn)
	dts.assertPreparedSQL(b, `insert ignore into "test" ("a") VALUES (?), (?), (?) ON CONFLICT DO NOTHING`, []interface{}{
		"a1", "a2", "a3",
	})

	d.ToInsertSQL(b.Clear(), icdu)
	dts.assertPreparedSQL(
		b,
		`insert ignore into "test" ("a") VALUES (?), (?), (?) ON CONFLICT (test) DO UPDATE SET "a"=?`,
		[]interface{}{"a1", "a2", "a3", "b"},
	)

	d.ToInsertSQL(b.Clear(), icduw)
	dts.assertPreparedSQL(
		b,
		`insert ignore into "test" ("a") VALUES (?), (?), (?) ON CONFLICT (test) DO UPDATE SET "a"=? WHERE ("foo" IS TRUE)`,
		[]interface{}{"a1", "a2", "a3", "b"},
	)
}

func (dts *dialectTestSuite) TestToInsertSQL_withCommonTables() {
	opts := DefaultDialectOptions()
	opts.WithFragment = []byte("with ")
	opts.RecursiveFragment = []byte("recursive ")
	d := sqlDialect{dialect: "test", dialectOptions: opts}
	tse := newTestAppendableExpression("select * from foo", emptyArgs, nil, nil)
	cte1 := exp.NewCommonTableExpression(false, "test_cte", tse)
	cte2 := exp.NewCommonTableExpression(true, "test_cte", tse)

	ic := exp.NewInsertClauses().
		SetInto(exp.NewIdentifierExpression("", "test_cte", ""))

	b := sb.NewSQLBuilder(false)

	d.ToInsertSQL(b.Clear(), ic.CommonTablesAppend(cte1))
	dts.assertNotPreparedSQL(b, `with test_cte AS (select * from foo) INSERT INTO "test_cte" DEFAULT VALUES`)

	d.ToInsertSQL(b.Clear(), ic.CommonTablesAppend(cte2))
	dts.assertNotPreparedSQL(b, `with recursive test_cte AS (select * from foo) INSERT INTO "test_cte" DEFAULT VALUES`)

	d.ToInsertSQL(b.Clear(), ic.CommonTablesAppend(cte1).CommonTablesAppend(cte2))
	dts.assertNotPreparedSQL(
		b,
		`with recursive test_cte AS (select * from foo), test_cte AS (select * from foo) INSERT INTO "test_cte" DEFAULT VALUES`,
	)

	opts = DefaultDialectOptions()
	opts.SupportsWithCTE = false
	d = sqlDialect{dialect: "test", dialectOptions: opts}

	d.ToInsertSQL(b.Clear(), ic.CommonTablesAppend(cte1))
	dts.assertErrorSQL(b, "goqu: dialect does not support CTE WITH clause [dialect=test]")

	opts = DefaultDialectOptions()
	opts.SupportsWithCTERecursive = false
	d = sqlDialect{dialect: "test", dialectOptions: opts}

	d.ToInsertSQL(b.Clear(), ic.CommonTablesAppend(cte2))
	dts.assertErrorSQL(b, "goqu: dialect does not support CTE WITH RECURSIVE clause [dialect=test]")

	d.ToInsertSQL(b.Clear(), ic.CommonTablesAppend(cte1))
	dts.assertNotPreparedSQL(b, `WITH test_cte AS (select * from foo) INSERT INTO "test_cte" DEFAULT VALUES`)

}

func (dts *dialectTestSuite) TestToUpdateSQL_unsupportedFragment() {
	opts := DefaultDialectOptions()
	opts.UpdateSQLOrder = []SQLFragmentType{InsertBeingSQLFragment}
	d := sqlDialect{dialect: "test", dialectOptions: opts}
	uc := exp.NewUpdateClauses().
		SetTable(exp.NewIdentifierExpression("", "test", "")).
		SetSetValues(exp.Record{"a": "b", "b": "c"})
	b := sb.NewSQLBuilder(true)

	d.ToUpdateSQL(b, uc)
	dts.assertErrorSQL(b, `goqu: unsupported UPDATE SQL fragment InsertBeingSQLFragment`)
}

func (dts *dialectTestSuite) TestToUpdateSQL_empty() {
	opts := DefaultDialectOptions()
	d := sqlDialect{dialect: "test", dialectOptions: opts}
	uc := exp.NewUpdateClauses()

	b := sb.NewSQLBuilder(false)
	d.ToUpdateSQL(b, uc)
	dts.Equal(errNoSourceForUpdate, b.Error())

}

func (dts *dialectTestSuite) TestToUpdateSQL_withBadUpdateValues() {
	opts := DefaultDialectOptions()
	d := sqlDialect{dialect: "test", dialectOptions: opts}
	uc := exp.NewUpdateClauses().
		SetTable(exp.NewIdentifierExpression("", "test", "")).
		SetSetValues(true)

	b := sb.NewSQLBuilder(false)
	d.ToUpdateSQL(b, uc)
	dts.EqualError(b.Error(), "goqu: unsupported update interface type bool")

}

func (dts *dialectTestSuite) TestToUpdateSQL_noSetValues() {
	opts := DefaultDialectOptions()
	d := sqlDialect{dialect: "test", dialectOptions: opts}
	uc := exp.NewUpdateClauses().SetTable(exp.NewIdentifierExpression("", "test", ""))

	b := sb.NewSQLBuilder(false)
	d.ToUpdateSQL(b, uc)
	dts.Equal(errNoSetValuesForUpdate, b.Error())
}

func (dts *dialectTestSuite) TestToUpdateSQL_withFrom() {
	opts := DefaultDialectOptions()
	d := sqlDialect{dialect: "test", dialectOptions: opts}
	uc := exp.NewUpdateClauses().
		SetTable(exp.NewIdentifierExpression("", "test", "")).
		SetSetValues(exp.Record{"foo": "bar"}).
		SetFrom(exp.NewColumnListExpression("other_test"))

	b := sb.NewSQLBuilder(false)
	d.ToUpdateSQL(b, uc)
	dts.NoError(b.Error())
	dts.assertNotPreparedSQL(b, `UPDATE "test" SET "foo"='bar' FROM "other_test"`)

	opts = DefaultDialectOptions()
	opts.UseFromClauseForMultipleUpdateTables = false
	d = sqlDialect{dialect: "test", dialectOptions: opts}
	d.ToUpdateSQL(b.Clear(), uc)
	dts.NoError(b.Error())
	dts.assertNotPreparedSQL(b, `UPDATE "test","other_test" SET "foo"='bar'`)

	opts = DefaultDialectOptions()
	opts.SupportsMultipleUpdateTables = false
	d = sqlDialect{dialect: "test", dialectOptions: opts}
	d.ToUpdateSQL(b.Clear(), uc)
	dts.EqualError(b.Error(), "goqu: test dialect does not support multiple tables in UPDATE")

	opts = DefaultDialectOptions()
	opts.SupportsMultipleUpdateTables = false
	d = sqlDialect{dialect: "test", dialectOptions: opts}
	d.ToUpdateSQL(b.Clear(), uc.SetFrom(nil))
	dts.NoError(b.Error())
	dts.assertNotPreparedSQL(b, `UPDATE "test" SET "foo"='bar'`)

}

func (dts *dialectTestSuite) TestToInsertSQL_withReturning() {
	opts := DefaultDialectOptions()
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	ic := exp.NewInsertClauses().
		SetInto(exp.NewIdentifierExpression("", "test", "")).
		SetCols(exp.NewColumnListExpression("a", "b")).
		SetVals([][]interface{}{
			{"a1", "b1"},
			{"a2", "b2"},
			{"a3", "b3"},
		})
	b := sb.NewSQLBuilder(false)
	d.ToInsertSQL(b, ic.SetReturning(exp.NewColumnListExpression("a", "b")))
	dts.assertNotPreparedSQL(b, `INSERT INTO "test" ("a", "b") VALUES ('a1', 'b1'), ('a2', 'b2'), ('a3', 'b3') RETURNING "a", "b"`)

	b = sb.NewSQLBuilder(true)
	d.ToInsertSQL(b, ic.SetReturning(exp.NewColumnListExpression("a", "b")))
	dts.assertPreparedSQL(b, `INSERT INTO "test" ("a", "b") VALUES (?, ?), (?, ?), (?, ?) RETURNING "a", "b"`, []interface{}{
		"a1", "b1", "a2", "b2", "a3", "b3",
	})
}

func (dts *dialectTestSuite) TestToUpdateSQL_withUpdateExpression() {

	opts := DefaultDialectOptions()
	// make sure the fragments are used
	opts.SetFragment = []byte(" set ")
	d := sqlDialect{dialect: "test", dialectOptions: opts}
	uc := exp.NewUpdateClauses().
		SetTable(exp.NewIdentifierExpression("", "test", ""))

	b := sb.NewSQLBuilder(false)
	d.ToUpdateSQL(b, uc.SetSetValues(exp.Record{"a": "b", "b": "c"}))
	dts.assertNotPreparedSQL(b, `UPDATE "test" set "a"='b',"b"='c'`)

	b = sb.NewSQLBuilder(true)
	d.ToUpdateSQL(b, uc.SetSetValues(exp.Record{"a": "b", "b": "c"}))
	dts.assertPreparedSQL(b, `UPDATE "test" set "a"=?,"b"=?`, []interface{}{"b", "c"})

	b = sb.NewSQLBuilder(true)
	d.ToUpdateSQL(b, uc.SetSetValues(exp.Record{}))
	dts.assertErrorSQL(b, errNoUpdatedValuesProvided.Error())
}

func (dts *dialectTestSuite) TestToUpdateSQL_withOrder() {
	opts := DefaultDialectOptions()
	opts.SupportsOrderByOnUpdate = true
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	opts2 := DefaultDialectOptions()
	opts2.SupportsOrderByOnUpdate = false
	d2 := sqlDialect{dialect: "test", dialectOptions: opts2}

	uc := exp.NewUpdateClauses().
		SetTable(exp.NewIdentifierExpression("", "test", "")).
		SetSetValues(exp.Record{"a": "b", "b": "c"}).
		SetOrder(exp.NewIdentifierExpression("", "", "c").Desc())

	b := sb.NewSQLBuilder(false)
	d.ToUpdateSQL(b.Clear(), uc)
	dts.assertNotPreparedSQL(b, `UPDATE "test" SET "a"='b',"b"='c' ORDER BY "c" DESC`)

	d2.ToUpdateSQL(b.Clear(), uc)
	dts.assertNotPreparedSQL(b, `UPDATE "test" SET "a"='b',"b"='c'`)

	b = sb.NewSQLBuilder(true)
	d.ToUpdateSQL(b.Clear(), uc)
	dts.assertPreparedSQL(b, `UPDATE "test" SET "a"=?,"b"=? ORDER BY "c" DESC`, []interface{}{"b", "c"})

	d2.ToUpdateSQL(b.Clear(), uc)
	dts.assertPreparedSQL(b, `UPDATE "test" SET "a"=?,"b"=?`, []interface{}{"b", "c"})
}

func (dts *dialectTestSuite) TestToUpdateSQL_withLimit() {
	opts := DefaultDialectOptions()
	opts.SupportsLimitOnUpdate = true
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	opts2 := DefaultDialectOptions()
	opts2.SupportsLimitOnUpdate = false
	d2 := sqlDialect{dialect: "test", dialectOptions: opts2}

	uc := exp.NewUpdateClauses().
		SetTable(exp.NewIdentifierExpression("", "test", "")).
		SetSetValues(exp.Record{"a": "b", "b": "c"}).
		SetLimit(10)

	b := sb.NewSQLBuilder(false)
	d.ToUpdateSQL(b.Clear(), uc)
	dts.assertNotPreparedSQL(b, `UPDATE "test" SET "a"='b',"b"='c' LIMIT 10`)

	d2.ToUpdateSQL(b.Clear(), uc)
	dts.assertNotPreparedSQL(b, `UPDATE "test" SET "a"='b',"b"='c'`)

	b = sb.NewSQLBuilder(true)
	d.ToUpdateSQL(b.Clear(), uc)
	dts.assertPreparedSQL(b, `UPDATE "test" SET "a"=?,"b"=? LIMIT ?`, []interface{}{"b", "c", int64(10)})

	d2.ToUpdateSQL(b.Clear(), uc)
	dts.assertPreparedSQL(b, `UPDATE "test" SET "a"=?,"b"=?`, []interface{}{"b", "c"})
}

func (dts *dialectTestSuite) TestToUpdateSQL_withCommonTables() {
	opts := DefaultDialectOptions()
	opts.WithFragment = []byte("with ")
	opts.RecursiveFragment = []byte("recursive ")
	d := sqlDialect{dialect: "test", dialectOptions: opts}
	tse := newTestAppendableExpression("select * from foo", emptyArgs, nil, nil)
	cte1 := exp.NewCommonTableExpression(false, "test_cte", tse)
	cte2 := exp.NewCommonTableExpression(true, "test_cte", tse)

	uc := exp.NewUpdateClauses().
		SetTable(exp.NewIdentifierExpression("", "test_cte", "")).
		SetSetValues(exp.Record{"a": "b", "b": "c"})

	b := sb.NewSQLBuilder(false)

	d.ToUpdateSQL(b.Clear(), uc.CommonTablesAppend(cte1))
	dts.assertNotPreparedSQL(b, `with test_cte AS (select * from foo) UPDATE "test_cte" SET "a"='b',"b"='c'`)

	d.ToUpdateSQL(b.Clear(), uc.CommonTablesAppend(cte2))
	dts.assertNotPreparedSQL(b, `with recursive test_cte AS (select * from foo) UPDATE "test_cte" SET "a"='b',"b"='c'`)

	d.ToUpdateSQL(b.Clear(), uc.CommonTablesAppend(cte1).CommonTablesAppend(cte2))
	dts.assertNotPreparedSQL(
		b,
		`with recursive test_cte AS (select * from foo), test_cte AS (select * from foo) UPDATE "test_cte" SET "a"='b',"b"='c'`,
	)

	opts = DefaultDialectOptions()
	opts.SupportsWithCTE = false
	d = sqlDialect{dialect: "test", dialectOptions: opts}

	d.ToUpdateSQL(b.Clear(), uc.CommonTablesAppend(cte1))
	dts.assertErrorSQL(b, "goqu: dialect does not support CTE WITH clause [dialect=test]")

	opts = DefaultDialectOptions()
	opts.SupportsWithCTERecursive = false
	d = sqlDialect{dialect: "test", dialectOptions: opts}

	d.ToUpdateSQL(b.Clear(), uc.CommonTablesAppend(cte2))
	dts.assertErrorSQL(b, "goqu: dialect does not support CTE WITH RECURSIVE clause [dialect=test]")

	d.ToUpdateSQL(b.Clear(), uc.CommonTablesAppend(cte1))
	dts.assertNotPreparedSQL(b, `WITH test_cte AS (select * from foo) UPDATE "test_cte" SET "a"='b',"b"='c'`)

}

func (dts *dialectTestSuite) TestToDeleteSQL() {
	opts := DefaultDialectOptions()
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	opts2 := DefaultDialectOptions()
	opts2.DeleteClause = []byte("delete")
	d2 := sqlDialect{dialect: "test", dialectOptions: opts2}

	dc := exp.NewDeleteClauses().SetFrom(exp.NewIdentifierExpression("", "test", ""))
	b := sb.NewSQLBuilder(false)
	d.ToDeleteSQL(b, dc)
	dts.assertNotPreparedSQL(b, `DELETE FROM "test"`)

	d2.ToDeleteSQL(b.Clear(), dc)
	dts.assertNotPreparedSQL(b, `delete FROM "test"`)

	b = sb.NewSQLBuilder(true)
	d.ToDeleteSQL(b, dc)
	dts.assertNotPreparedSQL(b, `DELETE FROM "test"`)

	d2.ToDeleteSQL(b.Clear(), dc)
	dts.assertNotPreparedSQL(b, `delete FROM "test"`)
}

func (dts *dialectTestSuite) TestToUpdateSQL_withUnsupportedFragment() {
	opts := DefaultDialectOptions()
	opts.DeleteSQLOrder = []SQLFragmentType{InsertBeingSQLFragment}
	d := sqlDialect{dialect: "test", dialectOptions: opts}
	dc := exp.NewDeleteClauses().SetFrom(exp.NewIdentifierExpression("", "test", ""))
	b := sb.NewSQLBuilder(true)

	d.ToDeleteSQL(b, dc)
	dts.assertErrorSQL(b, `goqu: unsupported DELETE SQL fragment InsertBeingSQLFragment`)
}

func (dts *dialectTestSuite) TestToDeleteSQL_noFrom() {
	opts := DefaultDialectOptions()
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	dc := exp.NewDeleteClauses()
	b := sb.NewSQLBuilder(false)
	d.ToDeleteSQL(b, dc)
	dts.assertErrorSQL(b, errNoSourceForDelete.Error())

	b = sb.NewSQLBuilder(true)
	d.ToDeleteSQL(b, dc)
	dts.assertErrorSQL(b, errNoSourceForDelete.Error())
}

func (dts *dialectTestSuite) TestToDeleteSQL_withErroredBuilder() {
	opts := DefaultDialectOptions()
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	dc := exp.NewDeleteClauses().SetFrom(exp.NewIdentifierExpression("", "test", ""))
	b := sb.NewSQLBuilder(false).SetError(errors.New("expected error"))
	d.ToDeleteSQL(b, dc)
	dts.assertErrorSQL(b, "goqu: expected error")

	b = sb.NewSQLBuilder(true).SetError(errors.New("expected error"))
	d.ToDeleteSQL(b, dc)
	dts.assertErrorSQL(b, "goqu: expected error")
}

func (dts *dialectTestSuite) TestToDeleteSQL_withWhere() {
	opts := DefaultDialectOptions()
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	dc := exp.NewDeleteClauses().
		SetFrom(exp.NewIdentifierExpression("", "test", "")).
		WhereAppend(exp.NewLiteralExpression(`"a"=?`, 1))
	b := sb.NewSQLBuilder(false)
	d.ToDeleteSQL(b, dc)
	dts.assertNotPreparedSQL(b, `DELETE FROM "test" WHERE "a"=1`)

	b = sb.NewSQLBuilder(true)
	d.ToDeleteSQL(b, dc)
	dts.assertPreparedSQL(b, `DELETE FROM "test" WHERE "a"=?`, []interface{}{
		int64(1),
	})
}

func (dts *dialectTestSuite) TestToDeleteSQL_withOrder() {
	opts := DefaultDialectOptions()
	opts.SupportsOrderByOnDelete = true
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	opts2 := DefaultDialectOptions()
	opts2.SupportsOrderByOnDelete = false
	d2 := sqlDialect{dialect: "test", dialectOptions: opts2}

	dc := exp.NewDeleteClauses().
		SetFrom(exp.NewIdentifierExpression("", "test", "")).
		SetOrder(exp.NewIdentifierExpression("", "", "c").Desc())
	b := sb.NewSQLBuilder(false)
	d.ToDeleteSQL(b.Clear(), dc)
	dts.assertNotPreparedSQL(b, `DELETE FROM "test" ORDER BY "c" DESC`)

	d2.ToDeleteSQL(b.Clear(), dc)
	dts.assertNotPreparedSQL(b, `DELETE FROM "test"`)

	b = sb.NewSQLBuilder(true)
	d.ToDeleteSQL(b.Clear(), dc)
	dts.assertPreparedSQL(b, `DELETE FROM "test" ORDER BY "c" DESC`, emptyArgs)

	d2.ToDeleteSQL(b.Clear(), dc)
	dts.assertPreparedSQL(b, `DELETE FROM "test"`, emptyArgs)
}

func (dts *dialectTestSuite) TestToDeleteSQL_withLimit() {
	opts := DefaultDialectOptions()
	opts.SupportsLimitOnDelete = true
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	opts2 := DefaultDialectOptions()
	opts2.SupportsLimitOnDelete = false
	d2 := sqlDialect{dialect: "test", dialectOptions: opts2}

	dc := exp.NewDeleteClauses().
		SetFrom(exp.NewIdentifierExpression("", "test", "")).
		SetLimit(1)
	b := sb.NewSQLBuilder(false)
	d.ToDeleteSQL(b.Clear(), dc)
	dts.assertNotPreparedSQL(b, `DELETE FROM "test" LIMIT 1`)

	d2.ToDeleteSQL(b.Clear(), dc)
	dts.assertNotPreparedSQL(b, `DELETE FROM "test"`)

	b = sb.NewSQLBuilder(true)
	d.ToDeleteSQL(b.Clear(), dc)
	dts.assertPreparedSQL(b, `DELETE FROM "test" LIMIT ?`, []interface{}{int64(1)})

	d2.ToDeleteSQL(b.Clear(), dc)
	dts.assertPreparedSQL(b, `DELETE FROM "test"`, emptyArgs)
}

func (dts *dialectTestSuite) TestToDeleteSQL_withReturning() {
	opts := DefaultDialectOptions()
	opts.SupportsReturn = true
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	opts2 := DefaultDialectOptions()
	opts2.SupportsReturn = false
	d2 := sqlDialect{dialect: "test", dialectOptions: opts2}

	dc := exp.NewDeleteClauses().
		SetFrom(exp.NewIdentifierExpression("", "test", "")).
		SetReturning(exp.NewColumnListExpression("a", "b"))
	b := sb.NewSQLBuilder(false)
	d.ToDeleteSQL(b.Clear(), dc)
	dts.assertNotPreparedSQL(b, `DELETE FROM "test" RETURNING "a", "b"`)

	d2.ToDeleteSQL(b.Clear(), dc)
	dts.assertErrorSQL(b, `goqu: dialect does not support RETURNING clause [dialect=test]`)

	b = sb.NewSQLBuilder(true)
	d.ToDeleteSQL(b.Clear(), dc)
	dts.assertPreparedSQL(b, `DELETE FROM "test" RETURNING "a", "b"`, emptyArgs)

	d2.ToDeleteSQL(b.Clear(), dc)
	dts.assertErrorSQL(b, `goqu: dialect does not support RETURNING clause [dialect=test]`)
}

func (dts *dialectTestSuite) TestToDeleteSQL_withCommonTables() {
	opts := DefaultDialectOptions()
	opts.WithFragment = []byte("with ")
	opts.RecursiveFragment = []byte("recursive ")
	d := sqlDialect{dialect: "test", dialectOptions: opts}
	tse := newTestAppendableExpression("select * from foo", emptyArgs, nil, nil)
	cte1 := exp.NewCommonTableExpression(false, "test_cte", tse)
	cte2 := exp.NewCommonTableExpression(true, "test_cte", tse)

	dc := exp.NewDeleteClauses().
		SetFrom(exp.NewIdentifierExpression("", "test_cte", ""))

	b := sb.NewSQLBuilder(false)

	d.ToDeleteSQL(b.Clear(), dc.CommonTablesAppend(cte1))
	dts.assertNotPreparedSQL(b, `with test_cte AS (select * from foo) DELETE FROM "test_cte"`)

	d.ToDeleteSQL(b.Clear(), dc.CommonTablesAppend(cte2))
	dts.assertNotPreparedSQL(b, `with recursive test_cte AS (select * from foo) DELETE FROM "test_cte"`)

	d.ToDeleteSQL(b.Clear(), dc.CommonTablesAppend(cte1).CommonTablesAppend(cte2))
	dts.assertNotPreparedSQL(
		b,
		`with recursive test_cte AS (select * from foo), test_cte AS (select * from foo) DELETE FROM "test_cte"`,
	)

	opts = DefaultDialectOptions()
	opts.SupportsWithCTE = false
	d = sqlDialect{dialect: "test", dialectOptions: opts}

	d.ToDeleteSQL(b.Clear(), dc.CommonTablesAppend(cte1))
	dts.assertErrorSQL(b, "goqu: dialect does not support CTE WITH clause [dialect=test]")

	opts = DefaultDialectOptions()
	opts.SupportsWithCTERecursive = false
	d = sqlDialect{dialect: "test", dialectOptions: opts}

	d.ToDeleteSQL(b.Clear(), dc.CommonTablesAppend(cte2))
	dts.assertErrorSQL(b, "goqu: dialect does not support CTE WITH RECURSIVE clause [dialect=test]")

	d.ToDeleteSQL(b.Clear(), dc.CommonTablesAppend(cte1))
	dts.assertNotPreparedSQL(b, `WITH test_cte AS (select * from foo) DELETE FROM "test_cte"`)

}

func (dts *dialectTestSuite) TestToSelectSQL() {
	opts := DefaultDialectOptions()
	// make sure the fragments are used
	opts.SelectClause = []byte("select")
	opts.StarRune = '#'
	d := sqlDialect{dialect: "test", dialectOptions: opts}
	sc := exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test"))
	scWithCols := sc.SetSelect(exp.NewColumnListExpression("a", "b"))
	b := sb.NewSQLBuilder(false)

	d.ToSelectSQL(b, sc)
	dts.assertNotPreparedSQL(b, `select # FROM "test"`)

	d.ToSelectSQL(b.Clear(), scWithCols)
	dts.assertNotPreparedSQL(b, `select "a", "b" FROM "test"`)

	b = sb.NewSQLBuilder(true)
	d.ToSelectSQL(b, sc)
	dts.assertPreparedSQL(b, `select # FROM "test"`, emptyArgs)

	d.ToSelectSQL(b.Clear(), scWithCols)
	dts.assertPreparedSQL(b, `select "a", "b" FROM "test"`, emptyArgs)
}

func (dts *dialectTestSuite) TestToSelectSQL_UnsupportedFragment() {
	opts := DefaultDialectOptions()
	opts.SelectSQLOrder = []SQLFragmentType{InsertBeingSQLFragment}
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	b := sb.NewSQLBuilder(true)
	c := exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test"))
	d.ToSelectSQL(b, c)
	dts.assertErrorSQL(b, `goqu: unsupported SELECT SQL fragment InsertBeingSQLFragment`)
}

func (dts *dialectTestSuite) TestToSelectSQL_WithErroredBuilder() {
	opts := DefaultDialectOptions()
	opts.SelectSQLOrder = []SQLFragmentType{InsertBeingSQLFragment}
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	b := sb.NewSQLBuilder(true).SetError(errors.New("test error"))
	c := exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test"))
	d.ToSelectSQL(b, c)
	dts.assertErrorSQL(b, `goqu: test error`)
}

func (dts *dialectTestSuite) TestToSelectSQL_withDistinct() {
	opts := DefaultDialectOptions()
	// make sure the fragments are used
	opts.SelectClause = []byte("select")
	opts.StarRune = '#'
	opts.DistinctFragment = []byte("distinct")
	opts.OnFragment = []byte(" on ")
	opts.SupportsDistinctOn = true
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	sc := exp.NewSelectClauses().SetDistinct(exp.NewColumnListExpression())
	scDistinctOn := sc.SetDistinct(exp.NewColumnListExpression("a", "b"))
	b := sb.NewSQLBuilder(false)
	d.SelectSQL(b, sc)
	dts.assertNotPreparedSQL(b, `select distinct #`)

	d.SelectSQL(b.Clear(), scDistinctOn)
	dts.assertNotPreparedSQL(b, `select distinct on ("a", "b") #`)

	b = sb.NewSQLBuilder(true)
	d.SelectSQL(b.Clear(), sc)
	dts.assertPreparedSQL(b, `select distinct #`, emptyArgs)

	d.SelectSQL(b.Clear(), scDistinctOn)
	dts.assertPreparedSQL(b, `select distinct on ("a", "b") #`, emptyArgs)

	opts = DefaultDialectOptions()
	opts.OnFragment = []byte(" on ")
	opts.SupportsDistinctOn = false
	d = sqlDialect{dialect: "test", dialectOptions: opts}

	b = sb.NewSQLBuilder(false)
	d.SelectSQL(b, sc)
	dts.assertNotPreparedSQL(b, `SELECT DISTINCT *`)

	d.SelectSQL(b.Clear(), scDistinctOn)
	dts.assertErrorSQL(b, "goqu: dialect does not support DISTINCT ON clause [dialect=test]")

	b = sb.NewSQLBuilder(true)
	d.SelectSQL(b.Clear(), sc)
	dts.assertPreparedSQL(b, `SELECT DISTINCT *`, emptyArgs)

	d.SelectSQL(b.Clear(), scDistinctOn)
	dts.assertErrorSQL(b, "goqu: dialect does not support DISTINCT ON clause [dialect=test]")
}

func (dts *dialectTestSuite) TestToSelectSQL_withFromSQL() {
	opts := DefaultDialectOptions()
	// make sure the fragments are used
	opts.FromFragment = []byte(" from")
	d := sqlDialect{dialect: "test", dialectOptions: opts}
	sc := exp.NewSelectClauses().
		SetFrom(exp.NewColumnListExpression("a", "b"))
	b := sb.NewSQLBuilder(false)
	d.ToSelectSQL(b.Clear(), sc)
	dts.assertNotPreparedSQL(b, `SELECT * from "a", "b"`)

	b = sb.NewSQLBuilder(true)
	d.ToSelectSQL(b.Clear(), sc)
	dts.assertPreparedSQL(b, `SELECT * from "a", "b"`, emptyArgs)

	sc = exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression())
	b = sb.NewSQLBuilder(false)
	d.ToSelectSQL(b.Clear(), sc)
	dts.assertNotPreparedSQL(b, `SELECT *`)

	b = sb.NewSQLBuilder(true)
	d.ToSelectSQL(b.Clear(), sc)
	dts.assertPreparedSQL(b, `SELECT *`, emptyArgs)
}

func (dts *dialectTestSuite) TestToSelectSQL_withJoin() {
	opts := DefaultDialectOptions()
	d := sqlDialect{dialect: "test", dialectOptions: opts}
	sc := exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test"))
	ti := exp.NewIdentifierExpression("", "test2", "")
	uj := exp.NewUnConditionedJoinExpression(exp.NaturalJoinType, ti)
	cjo := exp.NewConditionedJoinExpression(exp.LeftJoinType, ti, exp.NewJoinOnCondition(exp.Ex{"a": "foo"}))
	cju := exp.NewConditionedJoinExpression(exp.LeftJoinType, ti, exp.NewJoinUsingCondition("a"))

	b := sb.NewSQLBuilder(false)
	d.ToSelectSQL(b.Clear(), sc.JoinsAppend(uj))
	dts.assertNotPreparedSQL(b, `SELECT * FROM "test" NATURAL JOIN "test2"`)

	d.ToSelectSQL(b.Clear(), sc.JoinsAppend(cjo))
	dts.assertNotPreparedSQL(b, `SELECT * FROM "test" LEFT JOIN "test2" ON ("a" = 'foo')`)

	d.ToSelectSQL(b.Clear(), sc.JoinsAppend(cju))
	dts.assertNotPreparedSQL(b, `SELECT * FROM "test" LEFT JOIN "test2" USING ("a")`)

	d.ToSelectSQL(b.Clear(), sc.JoinsAppend(uj).JoinsAppend(cjo).JoinsAppend(cju))
	dts.assertNotPreparedSQL(
		b,
		`SELECT * FROM "test" NATURAL JOIN "test2" LEFT JOIN "test2" ON ("a" = 'foo') LEFT JOIN "test2" USING ("a")`,
	)

	b = sb.NewSQLBuilder(true)
	d.ToSelectSQL(b.Clear(), sc.JoinsAppend(uj))
	dts.assertPreparedSQL(b, `SELECT * FROM "test" NATURAL JOIN "test2"`, emptyArgs)

	d.ToSelectSQL(b.Clear(), sc.JoinsAppend(cjo))
	dts.assertPreparedSQL(b, `SELECT * FROM "test" LEFT JOIN "test2" ON ("a" = ?)`, []interface{}{"foo"})

	d.ToSelectSQL(b.Clear(), sc.JoinsAppend(cju))
	dts.assertPreparedSQL(b, `SELECT * FROM "test" LEFT JOIN "test2" USING ("a")`, emptyArgs)

	d.ToSelectSQL(b.Clear(), sc.JoinsAppend(uj).JoinsAppend(cjo).JoinsAppend(cju))
	dts.assertPreparedSQL(
		b,
		`SELECT * FROM "test" NATURAL JOIN "test2" LEFT JOIN "test2" ON ("a" = ?) LEFT JOIN "test2" USING ("a")`,
		[]interface{}{"foo"},
	)

	opts2 := DefaultDialectOptions()
	// override fragements to make sure dialect is used
	opts2.UsingFragment = []byte(" using ")
	opts2.OnFragment = []byte(" on ")
	opts2.JoinTypeLookup = map[exp.JoinType][]byte{
		exp.LeftJoinType:    []byte(" left join "),
		exp.NaturalJoinType: []byte(" natural join "),
	}
	d2 := sqlDialect{dialect: "test", dialectOptions: opts2}

	b = sb.NewSQLBuilder(false)
	d2.ToSelectSQL(b.Clear(), sc.JoinsAppend(uj))
	dts.assertNotPreparedSQL(b, `SELECT * FROM "test" natural join "test2"`)

	d2.ToSelectSQL(b.Clear(), sc.JoinsAppend(cjo))
	dts.assertNotPreparedSQL(b, `SELECT * FROM "test" left join "test2" on ("a" = 'foo')`)

	d2.ToSelectSQL(b.Clear(), sc.JoinsAppend(cju))
	dts.assertNotPreparedSQL(b, `SELECT * FROM "test" left join "test2" using ("a")`)

	d2.ToSelectSQL(b.Clear(), sc.JoinsAppend(uj).JoinsAppend(cjo).JoinsAppend(cju))
	dts.assertNotPreparedSQL(
		b,
		`SELECT * FROM "test" natural join "test2" left join "test2" on ("a" = 'foo') left join "test2" using ("a")`,
	)

	rj := exp.NewConditionedJoinExpression(exp.RightJoinType, ti, exp.NewJoinUsingCondition(exp.NewIdentifierExpression("", "", "a")))
	d2.ToSelectSQL(b.Clear(), sc.JoinsAppend(rj))
	dts.assertErrorSQL(b, "goqu: dialect does not support RightJoinType")

	badJoin := exp.NewConditionedJoinExpression(exp.LeftJoinType, ti, exp.NewJoinUsingCondition())
	d2.ToSelectSQL(b.Clear(), sc.JoinsAppend(badJoin))
	dts.assertErrorSQL(b, "goqu: join condition required for conditioned join LeftJoinType")
}

func (dts *dialectTestSuite) TestToSelectSQL_withWhere() {
	opts := DefaultDialectOptions()
	opts.WhereFragment = []byte(" where ")
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	sc := exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test"))
	w := exp.Ex{"a": "b"}
	w2 := exp.Ex{"b": "c"}

	b := sb.NewSQLBuilder(false)
	d.ToSelectSQL(b.Clear(), sc.WhereAppend(w))
	dts.assertNotPreparedSQL(b, `SELECT * FROM "test" where ("a" = 'b')`)

	d.ToSelectSQL(b.Clear(), sc.WhereAppend(w, w2))
	dts.assertNotPreparedSQL(b, `SELECT * FROM "test" where (("a" = 'b') AND ("b" = 'c'))`)

	b = sb.NewSQLBuilder(true)
	d.ToSelectSQL(b.Clear(), sc.WhereAppend(w))
	dts.assertPreparedSQL(b, `SELECT * FROM "test" where ("a" = ?)`, []interface{}{"b"})

	d.ToSelectSQL(b.Clear(), sc.WhereAppend(w, w2))
	dts.assertPreparedSQL(b, `SELECT * FROM "test" where (("a" = ?) AND ("b" = ?))`, []interface{}{"b", "c"})
}

func (dts *dialectTestSuite) TestToSelectSQL_withGroupBy() {
	opts := DefaultDialectOptions()
	opts.GroupByFragment = []byte(" group by ")
	d := sqlDialect{dialect: "test", dialectOptions: opts}
	sc := exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test"))
	c1 := exp.NewIdentifierExpression("", "", "a")
	c2 := exp.NewIdentifierExpression("", "", "b")

	b := sb.NewSQLBuilder(false)
	d.ToSelectSQL(b.Clear(), sc.SetGroupBy(exp.NewColumnListExpression(c1)))
	dts.assertNotPreparedSQL(b, `SELECT * FROM "test" group by "a"`)

	d.ToSelectSQL(b.Clear(), sc.SetGroupBy(exp.NewColumnListExpression(c1, c2)))
	dts.assertNotPreparedSQL(b, `SELECT * FROM "test" group by "a", "b"`)

	b = sb.NewSQLBuilder(true)
	d.ToSelectSQL(b.Clear(), sc.SetGroupBy(exp.NewColumnListExpression(c1)))
	dts.assertPreparedSQL(b, `SELECT * FROM "test" group by "a"`, emptyArgs)

	d.ToSelectSQL(b.Clear(), sc.SetGroupBy(exp.NewColumnListExpression(c1, c2)))
	dts.assertPreparedSQL(b, `SELECT * FROM "test" group by "a", "b"`, emptyArgs)
}

func (dts *dialectTestSuite) TestToSelectSQL_withHaving() {
	opts := DefaultDialectOptions()
	opts.HavingFragment = []byte(" having ")
	d := sqlDialect{dialect: "test", dialectOptions: opts}
	sc := exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test"))
	w := exp.Ex{"a": "b"}
	w2 := exp.Ex{"b": "c"}

	b := sb.NewSQLBuilder(false)
	d.ToSelectSQL(b.Clear(), sc.HavingAppend(w))
	dts.assertNotPreparedSQL(b, `SELECT * FROM "test" having ("a" = 'b')`)

	d.ToSelectSQL(b.Clear(), sc.HavingAppend(w, w2))
	dts.assertNotPreparedSQL(b, `SELECT * FROM "test" having (("a" = 'b') AND ("b" = 'c'))`)

	b = sb.NewSQLBuilder(true)
	d.ToSelectSQL(b.Clear(), sc.HavingAppend(w))
	dts.assertPreparedSQL(b, `SELECT * FROM "test" having ("a" = ?)`, []interface{}{"b"})

	d.ToSelectSQL(b.Clear(), sc.HavingAppend(w, w2))
	dts.assertPreparedSQL(b, `SELECT * FROM "test" having (("a" = ?) AND ("b" = ?))`, []interface{}{"b", "c"})
}

func (dts *dialectTestSuite) TestToSelectSQL_withOrder() {
	opts := DefaultDialectOptions()
	// override fragments to ensure they are used
	opts.OrderByFragment = []byte(" order by ")
	opts.AscFragment = []byte(" asc")
	opts.DescFragment = []byte(" desc")
	opts.NullsFirstFragment = []byte(" nulls first")
	opts.NullsLastFragment = []byte(" nulls last")
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	sc := exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test"))
	oa := exp.NewIdentifierExpression("", "", "a").Asc()
	oanf := exp.NewIdentifierExpression("", "", "a").Asc().NullsFirst()
	oanl := exp.NewIdentifierExpression("", "", "a").Asc().NullsLast()
	od := exp.NewIdentifierExpression("", "", "a").Desc()
	odnf := exp.NewIdentifierExpression("", "", "a").Desc().NullsFirst()
	odnl := exp.NewIdentifierExpression("", "", "a").Desc().NullsLast()

	b := sb.NewSQLBuilder(false)
	d.ToSelectSQL(b.Clear(), sc.SetOrder(oa))
	dts.assertNotPreparedSQL(b, `SELECT * FROM "test" order by "a" asc`)

	d.ToSelectSQL(b.Clear(), sc.SetOrder(oanf))
	dts.assertNotPreparedSQL(b, `SELECT * FROM "test" order by "a" asc nulls first`)

	d.ToSelectSQL(b.Clear(), sc.SetOrder(oanl))
	dts.assertNotPreparedSQL(b, `SELECT * FROM "test" order by "a" asc nulls last`)

	d.ToSelectSQL(b.Clear(), sc.SetOrder(od))
	dts.assertNotPreparedSQL(b, `SELECT * FROM "test" order by "a" desc`)

	d.ToSelectSQL(b.Clear(), sc.SetOrder(odnf))
	dts.assertNotPreparedSQL(b, `SELECT * FROM "test" order by "a" desc nulls first`)

	d.ToSelectSQL(b.Clear(), sc.SetOrder(odnl))
	dts.assertNotPreparedSQL(b, `SELECT * FROM "test" order by "a" desc nulls last`)

	d.ToSelectSQL(b.Clear(), sc.SetOrder(oa, od))
	dts.assertNotPreparedSQL(b, `SELECT * FROM "test" order by "a" asc, "a" desc`)

	b = sb.NewSQLBuilder(true)
	d.ToSelectSQL(b.Clear(), sc.SetOrder(oa))
	dts.assertPreparedSQL(b, `SELECT * FROM "test" order by "a" asc`, emptyArgs)

	d.ToSelectSQL(b.Clear(), sc.SetOrder(oanf))
	dts.assertPreparedSQL(b, `SELECT * FROM "test" order by "a" asc nulls first`, emptyArgs)

	d.ToSelectSQL(b.Clear(), sc.SetOrder(oanl))
	dts.assertPreparedSQL(b, `SELECT * FROM "test" order by "a" asc nulls last`, emptyArgs)

	d.ToSelectSQL(b.Clear(), sc.SetOrder(od))
	dts.assertPreparedSQL(b, `SELECT * FROM "test" order by "a" desc`, emptyArgs)

	d.ToSelectSQL(b.Clear(), sc.SetOrder(odnf))
	dts.assertPreparedSQL(b, `SELECT * FROM "test" order by "a" desc nulls first`, emptyArgs)

	d.ToSelectSQL(b.Clear(), sc.SetOrder(odnl))
	dts.assertPreparedSQL(b, `SELECT * FROM "test" order by "a" desc nulls last`, emptyArgs)

	d.ToSelectSQL(b.Clear(), sc.SetOrder(oa, od))
	dts.assertPreparedSQL(b, `SELECT * FROM "test" order by "a" asc, "a" desc`, emptyArgs)

}

func (dts *dialectTestSuite) TestToSelectSQL_withLimit() {
	opts := DefaultDialectOptions()
	opts.LimitFragment = []byte(" limit ")
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	sc := exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test"))
	b := sb.NewSQLBuilder(false)
	d.ToSelectSQL(b.Clear(), sc.SetLimit(10))
	dts.assertNotPreparedSQL(b, `SELECT * FROM "test" limit 10`)

	d.ToSelectSQL(b.Clear(), sc.SetLimit(0))
	dts.assertNotPreparedSQL(b, `SELECT * FROM "test" limit 0`)

	d.ToSelectSQL(b.Clear(), sc.SetLimit(exp.NewLiteralExpression("ALL")))
	dts.assertNotPreparedSQL(b, `SELECT * FROM "test" limit ALL`)

	b = sb.NewSQLBuilder(true)
	d.ToSelectSQL(b.Clear(), sc.SetLimit(10))
	dts.assertPreparedSQL(b, `SELECT * FROM "test" limit ?`, []interface{}{int64(10)})

	d.ToSelectSQL(b.Clear(), sc.SetLimit(0))
	dts.assertPreparedSQL(b, `SELECT * FROM "test" limit ?`, []interface{}{int64(0)})

	d.ToSelectSQL(b.Clear(), sc.SetLimit(exp.NewLiteralExpression("ALL")))
	dts.assertPreparedSQL(b, `SELECT * FROM "test" limit ALL`, emptyArgs)
}

func (dts *dialectTestSuite) TestToSelectSQL_withOffset() {
	opts := DefaultDialectOptions()
	opts.OffsetFragment = []byte(" offset ")
	d := sqlDialect{dialect: "test", dialectOptions: opts}
	sc := exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test"))

	b := sb.NewSQLBuilder(false)
	d.ToSelectSQL(b.Clear(), sc.SetOffset(10))
	dts.assertNotPreparedSQL(b, `SELECT * FROM "test" offset 10`)

	d.ToSelectSQL(b.Clear(), sc.SetOffset(0))
	dts.assertNotPreparedSQL(b, `SELECT * FROM "test"`)

	b = sb.NewSQLBuilder(true)
	d.ToSelectSQL(b.Clear(), sc.SetOffset(10))
	dts.assertPreparedSQL(b, `SELECT * FROM "test" offset ?`, []interface{}{int64(10)})

	d.ToSelectSQL(b.Clear(), sc.SetOffset(0))
	dts.assertPreparedSQL(b, `SELECT * FROM "test"`, emptyArgs)
}

func (dts *dialectTestSuite) TestToSelectSQL_withCommonTables() {
	opts := DefaultDialectOptions()
	opts.WithFragment = []byte("with ")
	opts.RecursiveFragment = []byte("recursive ")
	d := sqlDialect{dialect: "test", dialectOptions: opts}
	tse := newTestAppendableExpression("select * from foo", emptyArgs, nil, nil)
	cte1 := exp.NewCommonTableExpression(false, "test_cte", tse)
	cte2 := exp.NewCommonTableExpression(true, "test_cte", tse)

	sc := exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test_cte"))

	b := sb.NewSQLBuilder(false)

	d.ToSelectSQL(b.Clear(), sc.CommonTablesAppend(cte1))
	dts.assertNotPreparedSQL(b, `with test_cte AS (select * from foo) SELECT * FROM "test_cte"`)

	d.ToSelectSQL(b.Clear(), sc.CommonTablesAppend(cte2))
	dts.assertNotPreparedSQL(b, `with recursive test_cte AS (select * from foo) SELECT * FROM "test_cte"`)

	d.ToSelectSQL(b.Clear(), sc.CommonTablesAppend(cte1).CommonTablesAppend(cte2))
	dts.assertNotPreparedSQL(
		b,
		`with recursive test_cte AS (select * from foo), test_cte AS (select * from foo) SELECT * FROM "test_cte"`,
	)

	opts = DefaultDialectOptions()
	opts.SupportsWithCTE = false
	d = sqlDialect{dialect: "test", dialectOptions: opts}

	d.ToSelectSQL(b.Clear(), sc.CommonTablesAppend(cte1))
	dts.assertErrorSQL(b, "goqu: dialect does not support CTE WITH clause [dialect=test]")

	opts = DefaultDialectOptions()
	opts.SupportsWithCTERecursive = false
	d = sqlDialect{dialect: "test", dialectOptions: opts}

	d.ToSelectSQL(b.Clear(), sc.CommonTablesAppend(cte2))
	dts.assertErrorSQL(b, "goqu: dialect does not support CTE WITH RECURSIVE clause [dialect=test]")

	d.ToSelectSQL(b.Clear(), sc.CommonTablesAppend(cte1))
	dts.assertNotPreparedSQL(b, `WITH test_cte AS (select * from foo) SELECT * FROM "test_cte"`)

}

func (dts *dialectTestSuite) TestToSelectSQL_withCompounds() {
	opts := DefaultDialectOptions()
	opts.UnionFragment = []byte(" union ")
	opts.UnionAllFragment = []byte(" union all ")
	opts.IntersectFragment = []byte(" intersect ")
	opts.IntersectAllFragment = []byte(" intersect all ")
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	tse := newTestAppendableExpression("select * from foo", emptyArgs, nil, nil)

	sc := exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test"))
	b := sb.NewSQLBuilder(false)

	u := exp.NewCompoundExpression(exp.UnionCompoundType, tse)
	d.ToSelectSQL(b.Clear(), sc.CompoundsAppend(u))
	dts.assertNotPreparedSQL(b, `SELECT * FROM "test" union (select * from foo)`)

	ua := exp.NewCompoundExpression(exp.UnionAllCompoundType, tse)
	d.ToSelectSQL(b.Clear(), sc.CompoundsAppend(ua))
	dts.assertNotPreparedSQL(b, `SELECT * FROM "test" union all (select * from foo)`)

	i := exp.NewCompoundExpression(exp.IntersectCompoundType, tse)
	d.ToSelectSQL(b.Clear(), sc.CompoundsAppend(i))
	dts.assertNotPreparedSQL(b, `SELECT * FROM "test" intersect (select * from foo)`)

	ia := exp.NewCompoundExpression(exp.IntersectAllCompoundType, tse)
	d.ToSelectSQL(b.Clear(), sc.CompoundsAppend(ia))
	dts.assertNotPreparedSQL(b, `SELECT * FROM "test" intersect all (select * from foo)`)

	d.ToSelectSQL(b.Clear(), sc.CompoundsAppend(u).CompoundsAppend(ua).CompoundsAppend(i).CompoundsAppend(ia))
	dts.assertNotPreparedSQL(
		b,
		`SELECT * FROM "test"`+
			` union (select * from foo)`+
			` union all (select * from foo)`+
			` intersect (select * from foo)`+
			` intersect all (select * from foo)`,
	)

}

func (dts *dialectTestSuite) TestToSelectSQL_withFor() {
	opts := DefaultDialectOptions()
	opts.ForUpdateFragment = []byte(" for update ")
	opts.ForNoKeyUpdateFragment = []byte(" for no key update ")
	opts.ForShareFragment = []byte(" for share ")
	opts.ForKeyShareFragment = []byte(" for key share ")
	opts.NowaitFragment = []byte("nowait")
	opts.SkipLockedFragment = []byte("skip locked")
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	sc := exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test"))
	b := sb.NewSQLBuilder(false)

	d.ToSelectSQL(b.Clear(), sc.SetLock(exp.NewLock(exp.ForNolock, exp.Wait)))
	dts.assertNotPreparedSQL(b, `SELECT * FROM "test"`)

	d.ToSelectSQL(b.Clear(), sc.SetLock(exp.NewLock(exp.ForShare, exp.Wait)))
	dts.assertNotPreparedSQL(b, `SELECT * FROM "test" for share `)

	d.ToSelectSQL(b.Clear(), sc.SetLock(exp.NewLock(exp.ForShare, exp.NoWait)))
	dts.assertNotPreparedSQL(b, `SELECT * FROM "test" for share nowait`)

	d.ToSelectSQL(b.Clear(), sc.SetLock(exp.NewLock(exp.ForShare, exp.SkipLocked)))
	dts.assertNotPreparedSQL(b, `SELECT * FROM "test" for share skip locked`)

	d.ToSelectSQL(b.Clear(), sc.SetLock(exp.NewLock(exp.ForKeyShare, exp.Wait)))
	dts.assertNotPreparedSQL(b, `SELECT * FROM "test" for key share `)

	d.ToSelectSQL(b.Clear(), sc.SetLock(exp.NewLock(exp.ForKeyShare, exp.NoWait)))
	dts.assertNotPreparedSQL(b, `SELECT * FROM "test" for key share nowait`)

	d.ToSelectSQL(b.Clear(), sc.SetLock(exp.NewLock(exp.ForKeyShare, exp.SkipLocked)))
	dts.assertNotPreparedSQL(b, `SELECT * FROM "test" for key share skip locked`)

	d.ToSelectSQL(b.Clear(), sc.SetLock(exp.NewLock(exp.ForUpdate, exp.Wait)))
	dts.assertNotPreparedSQL(b, `SELECT * FROM "test" for update `)

	d.ToSelectSQL(b.Clear(), sc.SetLock(exp.NewLock(exp.ForUpdate, exp.NoWait)))
	dts.assertNotPreparedSQL(b, `SELECT * FROM "test" for update nowait`)

	d.ToSelectSQL(b.Clear(), sc.SetLock(exp.NewLock(exp.ForUpdate, exp.SkipLocked)))
	dts.assertNotPreparedSQL(b, `SELECT * FROM "test" for update skip locked`)

	d.ToSelectSQL(b.Clear(), sc.SetLock(exp.NewLock(exp.ForNoKeyUpdate, exp.Wait)))
	dts.assertNotPreparedSQL(b, `SELECT * FROM "test" for no key update `)

	d.ToSelectSQL(b.Clear(), sc.SetLock(exp.NewLock(exp.ForNoKeyUpdate, exp.NoWait)))
	dts.assertNotPreparedSQL(b, `SELECT * FROM "test" for no key update nowait`)

	d.ToSelectSQL(b.Clear(), sc.SetLock(exp.NewLock(exp.ForNoKeyUpdate, exp.SkipLocked)))
	dts.assertNotPreparedSQL(b, `SELECT * FROM "test" for no key update skip locked`)
}

func (dts *dialectTestSuite) TestLiteral_FloatTypes() {
	d := sqlDialect{dialect: "test", dialectOptions: DefaultDialectOptions()}
	var float float64

	b := sb.NewSQLBuilder(false)
	d.Literal(b.Clear(), float32(10.01))
	dts.assertNotPreparedSQL(b, "10.010000228881836")

	d.Literal(b.Clear(), float64(10.01))
	dts.assertNotPreparedSQL(b, "10.01")

	d.Literal(b.Clear(), &float)
	dts.assertNotPreparedSQL(b, "0")

	b = sb.NewSQLBuilder(true)
	d.Literal(b.Clear(), float32(10.01))
	dts.assertPreparedSQL(b, "?", []interface{}{float64(float32(10.01))})

	d.Literal(b.Clear(), float64(10.01))
	dts.assertPreparedSQL(b, "?", []interface{}{float64(10.01)})

	d.Literal(b.Clear(), &float)
	dts.assertPreparedSQL(b, "?", []interface{}{float})
}

func (dts *dialectTestSuite) TestLiteral_IntTypes() {
	d := sqlDialect{dialect: "test", dialectOptions: DefaultDialectOptions()}
	var i int64
	b := sb.NewSQLBuilder(false)
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
		d.Literal(b.Clear(), i)
		dts.assertNotPreparedSQL(b, "10")
	}
	d.Literal(b.Clear(), &i)
	dts.assertNotPreparedSQL(b, "0")

	b = sb.NewSQLBuilder(true)
	for _, i := range ints {
		d.Literal(b.Clear(), i)
		dts.assertPreparedSQL(b, "?", []interface{}{int64(10)})
	}
	d.Literal(b.Clear(), &i)
	dts.assertPreparedSQL(b, "?", []interface{}{i})
}

func (dts *dialectTestSuite) TestLiteral_StringTypes() {
	d := sqlDialect{dialect: "test", dialectOptions: DefaultDialectOptions()}
	var str string

	b := sb.NewSQLBuilder(false)
	d.Literal(b.Clear(), "Hello")
	dts.assertNotPreparedSQL(b, "'Hello'")

	// should escape single quotes
	d.Literal(b.Clear(), "Hello'")
	dts.assertNotPreparedSQL(b, "'Hello'''")

	d.Literal(b.Clear(), &str)
	dts.assertNotPreparedSQL(b, "''")

	b = sb.NewSQLBuilder(true)
	d.Literal(b.Clear(), "Hello")
	dts.assertPreparedSQL(b, "?", []interface{}{"Hello"})

	// should escape single quotes
	d.Literal(b.Clear(), "Hello'")
	dts.assertPreparedSQL(b, "?", []interface{}{"Hello'"})

	d.Literal(b.Clear(), &str)
	dts.assertPreparedSQL(b, "?", []interface{}{str})
}

func (dts *dialectTestSuite) TestLiteral_BytesTypes() {
	d := sqlDialect{dialect: "test", dialectOptions: DefaultDialectOptions()}

	b := sb.NewSQLBuilder(false)
	d.Literal(b.Clear(), []byte("Hello"))
	dts.assertNotPreparedSQL(b, "'Hello'")

	// should escape single quotes
	d.Literal(b.Clear(), []byte("Hello'"))
	dts.assertNotPreparedSQL(b, "'Hello'''")

	b = sb.NewSQLBuilder(true)
	d.Literal(b.Clear(), []byte("Hello"))
	dts.assertPreparedSQL(b, "?", []interface{}{[]byte("Hello")})

	// should escape single quotes
	d.Literal(b.Clear(), []byte("Hello'"))
	dts.assertPreparedSQL(b, "?", []interface{}{[]byte("Hello'")})
}

func (dts *dialectTestSuite) TestLiteral_BoolTypes() {
	var bl bool

	d := sqlDialect{dialect: "test", dialectOptions: DefaultDialectOptions()}

	b := sb.NewSQLBuilder(false)
	d.Literal(b.Clear(), true)
	dts.assertNotPreparedSQL(b, "TRUE")

	d.Literal(b.Clear(), false)
	dts.assertNotPreparedSQL(b, "FALSE")

	d.Literal(b.Clear(), &bl)
	dts.assertNotPreparedSQL(b, "FALSE")

	b = sb.NewSQLBuilder(true)
	d.Literal(b.Clear(), true)
	dts.assertPreparedSQL(b, "?", []interface{}{true})

	d.Literal(b.Clear(), false)
	dts.assertPreparedSQL(b, "?", []interface{}{false})

	d.Literal(b.Clear(), &bl)
	dts.assertPreparedSQL(b, "?", []interface{}{bl})
}

func (dts *dialectTestSuite) TestLiteral_TimeTypes() {
	d := sqlDialect{dialect: "default", dialectOptions: DefaultDialectOptions()}
	var nt *time.Time
	asiaShanghai, err := time.LoadLocation("Asia/Shanghai")
	dts.Require().NoError(err)
	testDatas := []time.Time{
		time.Now().UTC(),
		time.Now().In(asiaShanghai),
	}

	for _, n := range testDatas {
		var now = n
		b := sb.NewSQLBuilder(false)
		d.Literal(b.Clear(), now)
		dts.assertNotPreparedSQL(b, "'"+now.Format(time.RFC3339Nano)+"'")

		d.Literal(b.Clear(), &now)
		dts.assertNotPreparedSQL(b, "'"+now.Format(time.RFC3339Nano)+"'")

		d.Literal(b.Clear(), nt)
		dts.assertNotPreparedSQL(b, "NULL")

		b = sb.NewSQLBuilder(true)
		d.Literal(b.Clear(), now)
		dts.assertPreparedSQL(b, "?", []interface{}{now})

		d.Literal(b.Clear(), &now)
		dts.assertPreparedSQL(b, "?", []interface{}{now})

		d.Literal(b.Clear(), nt)
		dts.assertPreparedSQL(b, "NULL", emptyArgs)
	}
}

func (dts *dialectTestSuite) TestLiteral_NilTypes() {
	d := sqlDialect{dialect: "test", dialectOptions: DefaultDialectOptions()}
	b := sb.NewSQLBuilder(false)
	d.Literal(b.Clear(), nil)
	dts.assertNotPreparedSQL(b, "NULL")

	b = sb.NewSQLBuilder(true)
	d.Literal(b.Clear(), nil)
	dts.assertPreparedSQL(b, "NULL", []interface{}{})
}

type datasetValuerType int64

func (j datasetValuerType) Value() (driver.Value, error) {
	return []byte(fmt.Sprintf("Hello World %d", j)), nil
}

func (dts *dialectTestSuite) TestLiteral_Valuer() {
	b := sb.NewSQLBuilder(false)
	d := sqlDialect{dialect: "test", dialectOptions: DefaultDialectOptions()}

	d.Literal(b.Clear(), datasetValuerType(10))
	dts.assertNotPreparedSQL(b, "'Hello World 10'")

	b = sb.NewSQLBuilder(true)
	d.Literal(b.Clear(), datasetValuerType(10))
	dts.assertPreparedSQL(b, "?", []interface{}{[]byte("Hello World 10")})
}

func (dts *dialectTestSuite) TestLiteral_Slice() {
	b := sb.NewSQLBuilder(false)
	d := sqlDialect{dialect: "test", dialectOptions: DefaultDialectOptions()}
	d.Literal(b.Clear(), []string{"a", "b", "c"})
	dts.assertNotPreparedSQL(b, `('a', 'b', 'c')`)

	b = sb.NewSQLBuilder(true)
	d.Literal(b.Clear(), []string{"a", "b", "c"})
	dts.assertPreparedSQL(b, `(?, ?, ?)`, []interface{}{"a", "b", "c"})
}

type unknownExpression struct {
}

func (ue unknownExpression) Expression() exp.Expression {
	return ue
}
func (ue unknownExpression) Clone() exp.Expression {
	return ue
}
func (dts *dialectTestSuite) TestLiteralUnsupportedExpression() {
	d := sqlDialect{dialect: "test", dialectOptions: DefaultDialectOptions()}
	b := sb.NewSQLBuilder(false)
	d.Literal(b.Clear(), unknownExpression{})
	dts.assertErrorSQL(b, "goqu: unsupported expression type goqu.unknownExpression")
}

func (dts *dialectTestSuite) TestLiteral_AppendableExpression() {
	d := sqlDialect{dialect: "test", dialectOptions: DefaultDialectOptions()}
	ti := exp.NewIdentifierExpression("", "b", "")
	a := newTestAppendableExpression(`select * from "a"`, []interface{}{}, nil, nil)
	aliasedA := newTestAppendableExpression(`select * from "a"`, []interface{}{}, nil, exp.NewSelectClauses().SetAlias(ti))
	argsA := newTestAppendableExpression(`select * from "a" where x=?`, []interface{}{true}, nil, exp.NewSelectClauses().SetAlias(ti))
	ae := newTestAppendableExpression(`select * from "a"`, emptyArgs, errors.New("expected error"), nil)

	b := sb.NewSQLBuilder(false)
	d.Literal(b.Clear(), a)
	dts.assertNotPreparedSQL(b, `(select * from "a")`)

	d.Literal(b.Clear(), aliasedA)
	dts.assertNotPreparedSQL(b, `(select * from "a") AS "b"`)

	d.Literal(b.Clear(), ae)
	dts.assertErrorSQL(b, "goqu: expected error")

	b = sb.NewSQLBuilder(true)
	d.Literal(b.Clear(), a)
	dts.assertPreparedSQL(b, `(select * from "a")`, emptyArgs)

	d.Literal(b.Clear(), aliasedA)
	dts.assertPreparedSQL(b, `(select * from "a") AS "b"`, emptyArgs)

	d.Literal(b.Clear(), argsA)
	dts.assertPreparedSQL(b, `(select * from "a" where x=?) AS "b"`, []interface{}{true})
}

func (dts *dialectTestSuite) TestLiteral_ColumnList() {
	d := sqlDialect{dialect: "test", dialectOptions: DefaultDialectOptions()}

	b := sb.NewSQLBuilder(false)
	d.Literal(b.Clear(), exp.NewColumnListExpression("a", exp.NewLiteralExpression("true")))
	dts.assertNotPreparedSQL(b, `"a", true`)

	b = sb.NewSQLBuilder(true)
	d.Literal(b.Clear(), exp.NewColumnListExpression("a", exp.NewLiteralExpression("true")))
	dts.assertPreparedSQL(b, `"a", true`, emptyArgs)
}

func (dts *dialectTestSuite) TestLiteral_ExpressionList() {
	d := sqlDialect{dialect: "test", dialectOptions: DefaultDialectOptions()}

	b := sb.NewSQLBuilder(false)
	d.Literal(b.Clear(), exp.NewExpressionList(
		exp.AndType,
		exp.NewIdentifierExpression("", "", "a").Eq("b"),
		exp.NewIdentifierExpression("", "", "c").Neq(1),
	))
	dts.assertNotPreparedSQL(b, `(("a" = 'b') AND ("c" != 1))`)

	d.Literal(b.Clear(), exp.NewExpressionList(
		exp.OrType,
		exp.NewIdentifierExpression("", "", "a").Eq("b"),
		exp.NewIdentifierExpression("", "", "c").Neq(1),
	))
	dts.assertNotPreparedSQL(b, `(("a" = 'b') OR ("c" != 1))`)

	d.Literal(b.Clear(), exp.NewExpressionList(exp.OrType,
		exp.NewIdentifierExpression("", "", "a").Eq("b"),
		exp.NewExpressionList(exp.AndType,
			exp.NewIdentifierExpression("", "", "c").Neq(1),
			exp.NewIdentifierExpression("", "", "d").Eq(exp.NewLiteralExpression("NOW()")),
		),
	))
	dts.assertNotPreparedSQL(b, `(("a" = 'b') OR (("c" != 1) AND ("d" = NOW())))`)

	b = sb.NewSQLBuilder(true)
	d.Literal(b.Clear(), exp.NewExpressionList(
		exp.AndType,
		exp.NewIdentifierExpression("", "", "a").Eq("b"),
		exp.NewIdentifierExpression("", "", "c").Neq(1),
	))
	dts.assertPreparedSQL(b, `(("a" = ?) AND ("c" != ?))`, []interface{}{"b", int64(1)})

	d.Literal(b.Clear(), exp.NewExpressionList(
		exp.OrType,
		exp.NewIdentifierExpression("", "", "a").Eq("b"),
		exp.NewIdentifierExpression("", "", "c").Neq(1)),
	)
	dts.assertPreparedSQL(b, `(("a" = ?) OR ("c" != ?))`, []interface{}{"b", int64(1)})

	d.Literal(b.Clear(), exp.NewExpressionList(
		exp.OrType,
		exp.NewIdentifierExpression("", "", "a").Eq("b"),
		exp.NewExpressionList(
			exp.AndType,
			exp.NewIdentifierExpression("", "", "c").Neq(1),
			exp.NewIdentifierExpression("", "", "d").Eq(exp.NewLiteralExpression("NOW()")),
		),
	))
	dts.assertPreparedSQL(b, `(("a" = ?) OR (("c" != ?) AND ("d" = NOW())))`, []interface{}{"b", int64(1)})
}

func (dts *dialectTestSuite) TestLiteral_LiteralExpression() {
	d := sqlDialect{dialect: "test", dialectOptions: DefaultDialectOptions()}

	b := sb.NewSQLBuilder(false)
	d.Literal(b.Clear(), exp.NewLiteralExpression(`"b"::DATE = '2010-09-02'`))
	dts.assertNotPreparedSQL(b, `"b"::DATE = '2010-09-02'`)

	d.Literal(b.Clear(), exp.NewLiteralExpression(
		`"b" = ? or "c" = ? or d IN ?`,
		"a", 1, []int{1, 2, 3, 4}),
	)
	dts.assertNotPreparedSQL(b, `"b" = 'a' or "c" = 1 or d IN (1, 2, 3, 4)`)

	b = sb.NewSQLBuilder(true)
	d.Literal(b.Clear(), exp.NewLiteralExpression(`"b"::DATE = '2010-09-02'`))
	dts.assertPreparedSQL(b, `"b"::DATE = '2010-09-02'`, emptyArgs)

	d.Literal(b.Clear(), exp.NewLiteralExpression(
		`"b" = ? or "c" = ? or d IN ?`,
		"a", 1, []int{1, 2, 3, 4},
	))
	dts.assertPreparedSQL(b, `"b" = ? or "c" = ? or d IN (?, ?, ?, ?)`, []interface{}{
		"a",
		int64(1),
		int64(1),
		int64(2),
		int64(3),
		int64(4),
	})
}

func (dts *dialectTestSuite) TestLiteral_AliasedExpression() {
	d := sqlDialect{dialect: "test", dialectOptions: DefaultDialectOptions()}

	b := sb.NewSQLBuilder(false)
	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").As("b"))
	dts.assertNotPreparedSQL(b, `"a" AS "b"`)

	d.Literal(b.Clear(), exp.NewLiteralExpression("count(*)").As("count"))
	dts.assertNotPreparedSQL(b, `count(*) AS "count"`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").
		As(exp.NewIdentifierExpression("", "", "b")))
	dts.assertNotPreparedSQL(b, `"a" AS "b"`)

	b = sb.NewSQLBuilder(true)
	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").As("b"))
	dts.assertPreparedSQL(b, `"a" AS "b"`, emptyArgs)

	d.Literal(b.Clear(), exp.NewLiteralExpression("count(*)").As("count"))
	dts.assertPreparedSQL(b, `count(*) AS "count"`, emptyArgs)
}

func (dts *dialectTestSuite) TestLiteral_BooleanExpression() {
	d := sqlDialect{dialect: "test", dialectOptions: DefaultDialectOptions()}

	ae := newTestAppendableExpression(`SELECT "id" FROM "test2"`, emptyArgs, nil, nil)
	ident := exp.NewIdentifierExpression("", "", "a")
	b := sb.NewSQLBuilder(false)

	d.Literal(b.Clear(), ident.Eq(1))
	dts.assertNotPreparedSQL(b, `("a" = 1)`)

	d.Literal(b.Clear(), ident.Eq(true))
	dts.assertNotPreparedSQL(b, `("a" IS TRUE)`)

	d.Literal(b.Clear(), ident.Eq(false))
	dts.assertNotPreparedSQL(b, `("a" IS FALSE)`)

	d.Literal(b.Clear(), ident.Eq(nil))
	dts.assertNotPreparedSQL(b, `("a" IS NULL)`)

	d.Literal(b.Clear(), ident.Eq([]int64{1, 2, 3}))
	dts.assertNotPreparedSQL(b, `("a" IN (1, 2, 3))`)

	d.Literal(b.Clear(), ident.Eq(ae))
	dts.assertNotPreparedSQL(b, `("a" IN (SELECT "id" FROM "test2"))`)

	d.Literal(b.Clear(), ident.Neq(1))
	dts.assertNotPreparedSQL(b, `("a" != 1)`)

	d.Literal(b.Clear(), ident.Neq(true))
	dts.assertNotPreparedSQL(b, `("a" IS NOT TRUE)`)

	d.Literal(b.Clear(), ident.Neq(false))
	dts.assertNotPreparedSQL(b, `("a" IS NOT FALSE)`)

	d.Literal(b.Clear(), ident.Neq(nil))
	dts.assertNotPreparedSQL(b, `("a" IS NOT NULL)`)

	d.Literal(b.Clear(), ident.Neq([]int64{1, 2, 3}))
	dts.assertNotPreparedSQL(b, `("a" NOT IN (1, 2, 3))`)

	d.Literal(b.Clear(), ident.Neq(ae))
	dts.assertNotPreparedSQL(b, `("a" NOT IN (SELECT "id" FROM "test2"))`)

	d.Literal(b.Clear(), ident.Is(nil))
	dts.assertNotPreparedSQL(b, `("a" IS NULL)`)

	d.Literal(b.Clear(), ident.Is(false))
	dts.assertNotPreparedSQL(b, `("a" IS FALSE)`)

	d.Literal(b.Clear(), ident.Is(true))
	dts.assertNotPreparedSQL(b, `("a" IS TRUE)`)

	d.Literal(b.Clear(), ident.IsNot(nil))
	dts.assertNotPreparedSQL(b, `("a" IS NOT NULL)`)

	d.Literal(b.Clear(), ident.IsNot(false))
	dts.assertNotPreparedSQL(b, `("a" IS NOT FALSE)`)

	d.Literal(b.Clear(), ident.IsNot(true))
	dts.assertNotPreparedSQL(b, `("a" IS NOT TRUE)`)

	d.Literal(b.Clear(), ident.Gt(1))
	dts.assertNotPreparedSQL(b, `("a" > 1)`)

	d.Literal(b.Clear(), ident.Gte(1))
	dts.assertNotPreparedSQL(b, `("a" >= 1)`)

	d.Literal(b.Clear(), ident.Lt(1))
	dts.assertNotPreparedSQL(b, `("a" < 1)`)

	d.Literal(b.Clear(), ident.Lte(1))
	dts.assertNotPreparedSQL(b, `("a" <= 1)`)

	d.Literal(b.Clear(), ident.In([]int{1, 2, 3}))
	dts.assertNotPreparedSQL(b, `("a" IN (1, 2, 3))`)

	d.Literal(b.Clear(), ident.NotIn([]int{1, 2, 3}))
	dts.assertNotPreparedSQL(b, `("a" NOT IN (1, 2, 3))`)

	d.Literal(b.Clear(), ident.Like("a%"))
	dts.assertNotPreparedSQL(b, `("a" LIKE 'a%')`)

	d.Literal(b.Clear(), ident.
		Like(regexp.MustCompile("(a|b)")))
	dts.assertNotPreparedSQL(b, `("a" ~ '(a|b)')`)

	d.Literal(b.Clear(), ident.NotLike("a%"))
	dts.assertNotPreparedSQL(b, `("a" NOT LIKE 'a%')`)

	d.Literal(b.Clear(), ident.
		NotLike(regexp.MustCompile("(a|b)")))
	dts.assertNotPreparedSQL(b, `("a" !~ '(a|b)')`)

	d.Literal(b.Clear(), ident.ILike("a%"))
	dts.assertNotPreparedSQL(b, `("a" ILIKE 'a%')`)

	d.Literal(b.Clear(), ident.
		ILike(regexp.MustCompile("(a|b)")))
	dts.assertNotPreparedSQL(b, `("a" ~* '(a|b)')`)

	d.Literal(b.Clear(), ident.NotILike("a%"))
	dts.assertNotPreparedSQL(b, `("a" NOT ILIKE 'a%')`)

	d.Literal(b.Clear(), ident.
		NotILike(regexp.MustCompile("(a|b)")))
	dts.assertNotPreparedSQL(b, `("a" !~* '(a|b)')`)

	b = sb.NewSQLBuilder(true)
	d.Literal(b.Clear(), ident.Eq(1))
	dts.assertPreparedSQL(b, `("a" = ?)`, []interface{}{int64(1)})

	d.Literal(b.Clear(), ident.Eq(true))
	dts.assertPreparedSQL(b, `("a" IS TRUE)`, []interface{}{})

	d.Literal(b.Clear(), ident.Eq(false))
	dts.assertPreparedSQL(b, `("a" IS FALSE)`, emptyArgs)

	d.Literal(b.Clear(), ident.Eq(nil))
	dts.assertPreparedSQL(b, `("a" IS NULL)`, emptyArgs)

	d.Literal(b.Clear(), ident.Eq([]int64{1, 2, 3}))
	dts.assertPreparedSQL(b, `("a" IN (?, ?, ?))`, []interface{}{int64(1), int64(2), int64(3)})

	d.Literal(b.Clear(), ident.Neq(1))
	dts.assertPreparedSQL(b, `("a" != ?)`, []interface{}{int64(1)})

	d.Literal(b.Clear(), ident.Neq(true))
	dts.assertPreparedSQL(b, `("a" IS NOT TRUE)`, emptyArgs)

	d.Literal(b.Clear(), ident.Neq(false))
	dts.assertPreparedSQL(b, `("a" IS NOT FALSE)`, emptyArgs)

	d.Literal(b.Clear(), ident.Neq(nil))
	dts.assertPreparedSQL(b, `("a" IS NOT NULL)`, emptyArgs)

	d.Literal(b.Clear(), ident.Neq([]int64{1, 2, 3}))
	dts.assertPreparedSQL(b, `("a" NOT IN (?, ?, ?))`, []interface{}{int64(1), int64(2), int64(3)})

	d.Literal(b.Clear(), ident.Is(nil))
	dts.assertPreparedSQL(b, `("a" IS NULL)`, emptyArgs)

	d.Literal(b.Clear(), ident.Is(false))
	dts.assertPreparedSQL(b, `("a" IS FALSE)`, emptyArgs)

	d.Literal(b.Clear(), ident.Is(true))
	dts.assertPreparedSQL(b, `("a" IS TRUE)`, emptyArgs)

	d.Literal(b.Clear(), ident.IsNot(nil))
	dts.assertPreparedSQL(b, `("a" IS NOT NULL)`, emptyArgs)

	d.Literal(b.Clear(), ident.IsNot(false))
	dts.assertPreparedSQL(b, `("a" IS NOT FALSE)`, emptyArgs)

	d.Literal(b.Clear(), ident.IsNot(true))
	dts.assertPreparedSQL(b, `("a" IS NOT TRUE)`, emptyArgs)

	d.Literal(b.Clear(), ident.Gt(1))
	dts.assertPreparedSQL(b, `("a" > ?)`, []interface{}{int64(1)})

	d.Literal(b.Clear(), ident.Gte(1))
	dts.assertPreparedSQL(b, `("a" >= ?)`, []interface{}{int64(1)})

	d.Literal(b.Clear(), ident.Lt(1))
	dts.assertPreparedSQL(b, `("a" < ?)`, []interface{}{int64(1)})

	d.Literal(b.Clear(), ident.Lte(1))
	dts.assertPreparedSQL(b, `("a" <= ?)`, []interface{}{int64(1)})

	d.Literal(b.Clear(), ident.In([]int{1, 2, 3}))
	dts.assertPreparedSQL(b, `("a" IN (?, ?, ?))`, []interface{}{int64(1), int64(2), int64(3)})

	d.Literal(b.Clear(), ident.NotIn([]int{1, 2, 3}))
	dts.assertPreparedSQL(b, `("a" NOT IN (?, ?, ?))`, []interface{}{int64(1), int64(2), int64(3)})

	d.Literal(b.Clear(), ident.Like("a%"))
	dts.assertPreparedSQL(b, `("a" LIKE ?)`, []interface{}{"a%"})

	d.Literal(b.Clear(), ident.
		Like(regexp.MustCompile("(a|b)")))
	dts.assertPreparedSQL(b, `("a" ~ ?)`, []interface{}{"(a|b)"})

	d.Literal(b.Clear(), ident.NotLike("a%"))
	dts.assertPreparedSQL(b, `("a" NOT LIKE ?)`, []interface{}{"a%"})

	d.Literal(b.Clear(), ident.
		NotLike(regexp.MustCompile("(a|b)")))
	dts.assertPreparedSQL(b, `("a" !~ ?)`, []interface{}{"(a|b)"})

	d.Literal(b.Clear(), ident.ILike("a%"))
	dts.assertPreparedSQL(b, `("a" ILIKE ?)`, []interface{}{"a%"})

	d.Literal(b.Clear(), ident.
		ILike(regexp.MustCompile("(a|b)")))
	dts.assertPreparedSQL(b, `("a" ~* ?)`, []interface{}{"(a|b)"})

	d.Literal(b.Clear(), ident.NotILike("a%"))
	dts.assertPreparedSQL(b, `("a" NOT ILIKE ?)`, []interface{}{"a%"})

	d.Literal(b.Clear(), ident.
		NotILike(regexp.MustCompile("(a|b)")))
	dts.assertPreparedSQL(b, `("a" !~* ?)`, []interface{}{"(a|b)"})

	// test unsupported op
	opts := DefaultDialectOptions()
	opts.BooleanOperatorLookup = map[exp.BooleanOperation][]byte{}
	d = sqlDialect{dialect: "test", dialectOptions: opts}
	b = sb.NewSQLBuilder(false)
	d.Literal(b.Clear(), ident.Eq(1))
	dts.assertErrorSQL(b, "goqu: boolean operator 'eq' not supported")
	d.Literal(b.Clear(), ident.Neq(1))
	dts.assertErrorSQL(b, "goqu: boolean operator 'neq' not supported")
	d.Literal(b.Clear(), ident.Is(true))
	dts.assertErrorSQL(b, "goqu: boolean operator 'is' not supported")
	d.Literal(b.Clear(), ident.IsNot(true))
	dts.assertErrorSQL(b, "goqu: boolean operator 'isnot' not supported")
	d.Literal(b.Clear(), ident.Gt(1))
	dts.assertErrorSQL(b, "goqu: boolean operator 'gt' not supported")
	d.Literal(b.Clear(), ident.Gte(1))
	dts.assertErrorSQL(b, "goqu: boolean operator 'gte' not supported")
	d.Literal(b.Clear(), ident.Lt(1))
	dts.assertErrorSQL(b, "goqu: boolean operator 'lt' not supported")
	d.Literal(b.Clear(), ident.Lte(1))
	dts.assertErrorSQL(b, "goqu: boolean operator 'lte' not supported")
	d.Literal(b.Clear(), ident.In(1, 2, 3))
	dts.assertErrorSQL(b, "goqu: boolean operator 'in' not supported")
	d.Literal(b.Clear(), ident.NotIn(1, 2, 3))
	dts.assertErrorSQL(b, "goqu: boolean operator 'notin' not supported")
	d.Literal(b.Clear(), ident.Like("a%"))
	dts.assertErrorSQL(b, "goqu: boolean operator 'like' not supported")
	d.Literal(b.Clear(), ident.NotLike("a%"))
	dts.assertErrorSQL(b, "goqu: boolean operator 'notlike' not supported")
	d.Literal(b.Clear(), ident.ILike("a%"))
	dts.assertErrorSQL(b, "goqu: boolean operator 'ilike' not supported")
	d.Literal(b.Clear(), ident.NotILike("a%"))
	dts.assertErrorSQL(b, "goqu: boolean operator 'notilike' not supported")
	d.Literal(b.Clear(), ident.Like(regexp.MustCompile("(a|b)")))
	dts.assertErrorSQL(b, "goqu: boolean operator 'regexp like' not supported")
	d.Literal(b.Clear(), ident.NotLike(regexp.MustCompile("(a|b)")))
	dts.assertErrorSQL(b, "goqu: boolean operator 'regexp notlike' not supported")
	d.Literal(b.Clear(), ident.ILike(regexp.MustCompile("(a|b)")))
	dts.assertErrorSQL(b, "goqu: boolean operator 'regexp ilike' not supported")
	d.Literal(b.Clear(), ident.NotILike(regexp.MustCompile("(a|b)")))
	dts.assertErrorSQL(b, "goqu: boolean operator 'regexp notilike' not supported")
}

func (dts *dialectTestSuite) TestLiteral_RangeExpression() {
	d := sqlDialect{dialect: "test", dialectOptions: DefaultDialectOptions()}

	b := sb.NewSQLBuilder(false)
	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").
		Between(exp.NewRangeVal(1, 2)))
	dts.assertNotPreparedSQL(b, `("a" BETWEEN 1 AND 2)`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").
		NotBetween(exp.NewRangeVal(1, 2)))
	dts.assertNotPreparedSQL(b, `("a" NOT BETWEEN 1 AND 2)`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").
		Between(exp.NewRangeVal("aaa", "zzz")))
	dts.assertNotPreparedSQL(b, `("a" BETWEEN 'aaa' AND 'zzz')`)

	b = sb.NewSQLBuilder(true)
	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").
		Between(exp.NewRangeVal(1, 2)))
	dts.assertPreparedSQL(b, `("a" BETWEEN ? AND ?)`, []interface{}{int64(1), int64(2)})

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").
		NotBetween(exp.NewRangeVal(1, 2)))
	dts.assertPreparedSQL(b, `("a" NOT BETWEEN ? AND ?)`, []interface{}{int64(1), int64(2)})

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").
		Between(exp.NewRangeVal("aaa", "zzz")))
	dts.assertPreparedSQL(b, `("a" BETWEEN ? AND ?)`, []interface{}{"aaa", "zzz"})
}

func (dts *dialectTestSuite) TestLiteral_OrderedExpression() {
	d := sqlDialect{dialect: "test", dialectOptions: DefaultDialectOptions()}

	b := sb.NewSQLBuilder(false)
	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Asc())
	dts.assertNotPreparedSQL(b, `"a" ASC`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Desc())
	dts.assertNotPreparedSQL(b, `"a" DESC`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Asc().NullsLast())
	dts.assertNotPreparedSQL(b, `"a" ASC NULLS LAST`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Desc().NullsLast())
	dts.assertNotPreparedSQL(b, `"a" DESC NULLS LAST`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Asc().NullsFirst())
	dts.assertNotPreparedSQL(b, `"a" ASC NULLS FIRST`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Desc().NullsFirst())
	dts.assertNotPreparedSQL(b, `"a" DESC NULLS FIRST`)

	b = sb.NewSQLBuilder(true)
	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Asc())
	dts.assertPreparedSQL(b, `"a" ASC`, emptyArgs)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Desc())
	dts.assertPreparedSQL(b, `"a" DESC`, emptyArgs)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Asc().NullsLast())
	dts.assertPreparedSQL(b, `"a" ASC NULLS LAST`, emptyArgs)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Desc().NullsLast())
	dts.assertPreparedSQL(b, `"a" DESC NULLS LAST`, emptyArgs)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Asc().NullsFirst())
	dts.assertPreparedSQL(b, `"a" ASC NULLS FIRST`, emptyArgs)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Desc().NullsFirst())
	dts.assertPreparedSQL(b, `"a" DESC NULLS FIRST`, emptyArgs)
}

func (dts *dialectTestSuite) TestLiteral_UpdateExpression() {
	d := sqlDialect{dialect: "test", dialectOptions: DefaultDialectOptions()}

	b := sb.NewSQLBuilder(false)
	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Set(1))
	dts.assertNotPreparedSQL(b, `"a"=1`)

	b = sb.NewSQLBuilder(true)
	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Set(1))
	dts.assertPreparedSQL(b, `"a"=?`, []interface{}{int64(1)})
}

func (dts *dialectTestSuite) TestLiteral_SQLFunctionExpression() {
	d := sqlDialect{dialect: "test", dialectOptions: DefaultDialectOptions()}

	b := sb.NewSQLBuilder(false)
	d.Literal(b.Clear(), exp.NewSQLFunctionExpression("MIN", exp.NewIdentifierExpression("", "", "a")))
	dts.assertNotPreparedSQL(b, `MIN("a")`)

	d.Literal(b.Clear(), exp.NewSQLFunctionExpression("COALESCE", exp.NewIdentifierExpression("", "", "a"), "a"))
	dts.assertNotPreparedSQL(b, `COALESCE("a", 'a')`)

	b = sb.NewSQLBuilder(true)
	d.Literal(b.Clear(), exp.NewSQLFunctionExpression("MIN", exp.NewIdentifierExpression("", "", "a")))
	dts.assertNotPreparedSQL(b, `MIN("a")`)

	d.Literal(b.Clear(), exp.NewSQLFunctionExpression("COALESCE", exp.NewIdentifierExpression("", "", "a"), "a"))
	dts.assertPreparedSQL(b, `COALESCE("a", ?)`, []interface{}{"a"})

}

func (dts *dialectTestSuite) TestLiteral_CastExpression() {
	d := sqlDialect{dialect: "test", dialectOptions: DefaultDialectOptions()}
	b := sb.NewSQLBuilder(false)
	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Cast("DATE"))
	dts.assertNotPreparedSQL(b, `CAST("a" AS DATE)`)

	b = sb.NewSQLBuilder(true)
	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Cast("DATE"))
	dts.assertPreparedSQL(b, `CAST("a" AS DATE)`, emptyArgs)
}

func (dts *dialectTestSuite) TestLiteral_CommonTableExpression() {
	d := sqlDialect{dialect: "test", dialectOptions: DefaultDialectOptions()}
	ae := newTestAppendableExpression(`SELECT * FROM "b"`, emptyArgs, nil, nil)
	b := sb.NewSQLBuilder(false)
	d.Literal(b.Clear(), exp.NewCommonTableExpression(false, "a", ae))
	dts.assertNotPreparedSQL(b, `a AS (SELECT * FROM "b")`)

	d.Literal(b.Clear(), exp.NewCommonTableExpression(false, "a(x,y)", ae))
	dts.assertNotPreparedSQL(b, `a(x,y) AS (SELECT * FROM "b")`)

	d.Literal(b.Clear(), exp.NewCommonTableExpression(true, "a", ae))
	dts.assertNotPreparedSQL(b, `a AS (SELECT * FROM "b")`)

	d.Literal(b.Clear(), exp.NewCommonTableExpression(true, "a(x,y)", ae))
	dts.assertNotPreparedSQL(b, `a(x,y) AS (SELECT * FROM "b")`)

	b = sb.NewSQLBuilder(true)
	d.Literal(b.Clear(), exp.NewCommonTableExpression(false, "a", ae))
	dts.assertPreparedSQL(b, `a AS (SELECT * FROM "b")`, emptyArgs)

	d.Literal(b.Clear(), exp.NewCommonTableExpression(false, "a(x,y)", ae))
	dts.assertPreparedSQL(b, `a(x,y) AS (SELECT * FROM "b")`, emptyArgs)

	d.Literal(b.Clear(), exp.NewCommonTableExpression(true, "a", ae))
	dts.assertPreparedSQL(b, `a AS (SELECT * FROM "b")`, emptyArgs)

	d.Literal(b.Clear(), exp.NewCommonTableExpression(true, "a(x,y)", ae))
	dts.assertPreparedSQL(b, `a(x,y) AS (SELECT * FROM "b")`, emptyArgs)
}

func (dts *dialectTestSuite) TestLiteral_CompoundExpression() {
	ae := newTestAppendableExpression(`SELECT * FROM "b"`, emptyArgs, nil, nil)

	b := sb.NewSQLBuilder(false)
	d := sqlDialect{dialect: "test", dialectOptions: DefaultDialectOptions()}

	d.Literal(b.Clear(), exp.NewCompoundExpression(exp.UnionCompoundType, ae))
	dts.assertNotPreparedSQL(b, ` UNION (SELECT * FROM "b")`)

	d.Literal(b.Clear(), exp.NewCompoundExpression(exp.UnionAllCompoundType, ae))
	dts.assertNotPreparedSQL(b, ` UNION ALL (SELECT * FROM "b")`)

	d.Literal(b.Clear(), exp.NewCompoundExpression(exp.IntersectCompoundType, ae))
	dts.assertNotPreparedSQL(b, ` INTERSECT (SELECT * FROM "b")`)

	d.Literal(b.Clear(), exp.NewCompoundExpression(exp.IntersectAllCompoundType, ae))
	dts.assertNotPreparedSQL(b, ` INTERSECT ALL (SELECT * FROM "b")`)

	b = sb.NewSQLBuilder(true)
	d.Literal(b.Clear(), exp.NewCompoundExpression(exp.UnionCompoundType, ae))
	dts.assertNotPreparedSQL(b, ` UNION (SELECT * FROM "b")`)

	d.Literal(b.Clear(), exp.NewCompoundExpression(exp.UnionAllCompoundType, ae))
	dts.assertNotPreparedSQL(b, ` UNION ALL (SELECT * FROM "b")`)

	d.Literal(b.Clear(), exp.NewCompoundExpression(exp.IntersectCompoundType, ae))
	dts.assertNotPreparedSQL(b, ` INTERSECT (SELECT * FROM "b")`)

	d.Literal(b.Clear(), exp.NewCompoundExpression(exp.IntersectAllCompoundType, ae))
	dts.assertNotPreparedSQL(b, ` INTERSECT ALL (SELECT * FROM "b")`)
}

func (dts *dialectTestSuite) TestLiteral_IdentifierExpression() {
	d := sqlDialect{dialect: "test", dialectOptions: DefaultDialectOptions()}

	b := sb.NewSQLBuilder(false)
	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", ""))
	dts.assertErrorSQL(b, `goqu: a empty identifier was encountered, please specify a "schema", "table" or "column"`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", nil))
	dts.assertErrorSQL(b, `goqu: a empty identifier was encountered, please specify a "schema", "table" or "column"`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", false))
	dts.assertErrorSQL(b, `goqu: unexpected col type must be string or LiteralExpression received bool`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "col"))
	dts.assertNotPreparedSQL(b, `"col"`)

	d.Literal(b.Clear(), exp.ParseIdentifier("table.col"))
	dts.assertNotPreparedSQL(b, `"table"."col"`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "col").Table("table"))
	dts.assertNotPreparedSQL(b, `"table"."col"`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "table", "col"))
	dts.assertNotPreparedSQL(b, `"table"."col"`)

	d.Literal(b.Clear(), exp.ParseIdentifier("a.b.c"))
	dts.assertNotPreparedSQL(b, `"a"."b"."c"`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("schema", "table", "col"))
	dts.assertNotPreparedSQL(b, `"schema"."table"."col"`)

	d.Literal(b.Clear(), exp.ParseIdentifier("schema.table.*"))
	dts.assertNotPreparedSQL(b, `"schema"."table".*`)

	d.Literal(b.Clear(), exp.ParseIdentifier("table.*"))
	dts.assertNotPreparedSQL(b, `"table".*`)

	b = sb.NewSQLBuilder(true)
	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "col"))
	dts.assertNotPreparedSQL(b, `"col"`)

	d.Literal(b.Clear(), exp.ParseIdentifier("table.col"))
	dts.assertNotPreparedSQL(b, `"table"."col"`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "col").Table("table"))
	dts.assertNotPreparedSQL(b, `"table"."col"`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "table", "col"))
	dts.assertNotPreparedSQL(b, `"table"."col"`)

	d.Literal(b.Clear(), exp.ParseIdentifier("a.b.c"))
	dts.assertNotPreparedSQL(b, `"a"."b"."c"`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("schema", "table", "col"))
	dts.assertNotPreparedSQL(b, `"schema"."table"."col"`)

	d.Literal(b.Clear(), exp.ParseIdentifier("schema.table.*"))
	dts.assertNotPreparedSQL(b, `"schema"."table".*`)

	d.Literal(b.Clear(), exp.ParseIdentifier("table.*"))
	dts.assertNotPreparedSQL(b, `"table".*`)
}

func (dts *dialectTestSuite) TestLiteral_ExpressionMap() {
	d := sqlDialect{dialect: "test", dialectOptions: DefaultDialectOptions()}

	b := sb.NewSQLBuilder(false)
	d.Literal(b.Clear(), exp.Ex{"a": 1})
	dts.assertNotPreparedSQL(b, `("a" = 1)`)

	d.Literal(b.Clear(), exp.Ex{})
	dts.assertNotPreparedSQL(b, ``)

	d.Literal(b.Clear(), exp.Ex{"a": true})
	dts.assertNotPreparedSQL(b, `("a" IS TRUE)`)

	d.Literal(b.Clear(), exp.Ex{"a": false})
	dts.assertNotPreparedSQL(b, `("a" IS FALSE)`)

	d.Literal(b.Clear(), exp.Ex{"a": nil})
	dts.assertNotPreparedSQL(b, `("a" IS NULL)`)

	d.Literal(b.Clear(), exp.Ex{"a": []string{"a", "b", "c"}})
	dts.assertNotPreparedSQL(b, `("a" IN ('a', 'b', 'c'))`)

	d.Literal(b.Clear(), exp.Ex{"a": exp.Op{"neq": 1}})
	dts.assertNotPreparedSQL(b, `("a" != 1)`)

	d.Literal(b.Clear(), exp.Ex{"a": exp.Op{"isnot": true}})
	dts.assertNotPreparedSQL(b, `("a" IS NOT TRUE)`)

	d.Literal(b.Clear(), exp.Ex{"a": exp.Op{"gt": 1}})
	dts.assertNotPreparedSQL(b, `("a" > 1)`)

	d.Literal(b.Clear(), exp.Ex{"a": exp.Op{"gte": 1}})
	dts.assertNotPreparedSQL(b, `("a" >= 1)`)

	d.Literal(b.Clear(), exp.Ex{"a": exp.Op{"lt": 1}})
	dts.assertNotPreparedSQL(b, `("a" < 1)`)

	d.Literal(b.Clear(), exp.Ex{"a": exp.Op{"lte": 1}})
	dts.assertNotPreparedSQL(b, `("a" <= 1)`)

	d.Literal(b.Clear(), exp.Ex{"a": exp.Op{"like": "a%"}})
	dts.assertNotPreparedSQL(b, `("a" LIKE 'a%')`)

	d.Literal(b.Clear(), exp.Ex{"a": exp.Op{"notLike": "a%"}})
	dts.assertNotPreparedSQL(b, `("a" NOT LIKE 'a%')`)

	d.Literal(b.Clear(), exp.Ex{"a": exp.Op{"notLike": "a%"}})
	dts.assertNotPreparedSQL(b, `("a" NOT LIKE 'a%')`)

	d.Literal(b.Clear(), exp.Ex{"a": exp.Op{"in": []string{"a", "b", "c"}}})
	dts.assertNotPreparedSQL(b, `("a" IN ('a', 'b', 'c'))`)

	d.Literal(b.Clear(), exp.Ex{"a": exp.Op{"notIn": []string{"a", "b", "c"}}})
	dts.assertNotPreparedSQL(b, `("a" NOT IN ('a', 'b', 'c'))`)

	d.Literal(b.Clear(), exp.Ex{"a": exp.Op{"is": nil, "eq": 10}})
	dts.assertNotPreparedSQL(b, `(("a" = 10) OR ("a" IS NULL))`)

	d.Literal(b.Clear(), exp.Ex{"a": exp.Op{"between": exp.NewRangeVal(1, 10)}})
	dts.assertNotPreparedSQL(b, `("a" BETWEEN 1 AND 10)`)

	d.Literal(b.Clear(), exp.Ex{"a": exp.Op{"notbetween": exp.NewRangeVal(1, 10)}})
	dts.assertNotPreparedSQL(b, `("a" NOT BETWEEN 1 AND 10)`)

	b = sb.NewSQLBuilder(true)
	d.Literal(b.Clear(), exp.Ex{"a": 1})
	dts.assertPreparedSQL(b, `("a" = ?)`, []interface{}{int64(1)})

	d.Literal(b.Clear(), exp.Ex{"a": true})
	dts.assertPreparedSQL(b, `("a" IS TRUE)`, emptyArgs)

	d.Literal(b.Clear(), exp.Ex{"a": false})
	dts.assertPreparedSQL(b, `("a" IS FALSE)`, emptyArgs)

	d.Literal(b.Clear(), exp.Ex{"a": nil})
	dts.assertPreparedSQL(b, `("a" IS NULL)`, emptyArgs)

	d.Literal(b.Clear(), exp.Ex{"a": []string{"a", "b", "c"}})
	dts.assertPreparedSQL(b, `("a" IN (?, ?, ?))`, []interface{}{"a", "b", "c"})

	d.Literal(b.Clear(), exp.Ex{"a": exp.Op{"neq": 1}})
	dts.assertPreparedSQL(b, `("a" != ?)`, []interface{}{int64(1)})

	d.Literal(b.Clear(), exp.Ex{"a": exp.Op{"isnot": true}})
	dts.assertPreparedSQL(b, `("a" IS NOT TRUE)`, emptyArgs)

	d.Literal(b.Clear(), exp.Ex{"a": exp.Op{"gt": 1}})
	dts.assertPreparedSQL(b, `("a" > ?)`, []interface{}{int64(1)})

	d.Literal(b.Clear(), exp.Ex{"a": exp.Op{"gte": 1}})
	dts.assertPreparedSQL(b, `("a" >= ?)`, []interface{}{int64(1)})

	d.Literal(b.Clear(), exp.Ex{"a": exp.Op{"lt": 1}})
	dts.assertPreparedSQL(b, `("a" < ?)`, []interface{}{int64(1)})

	d.Literal(b.Clear(), exp.Ex{"a": exp.Op{"lte": 1}})
	dts.assertPreparedSQL(b, `("a" <= ?)`, []interface{}{int64(1)})

	d.Literal(b.Clear(), exp.Ex{"a": exp.Op{"like": "a%"}})
	dts.assertPreparedSQL(b, `("a" LIKE ?)`, []interface{}{"a%"})

	d.Literal(b.Clear(), exp.Ex{"a": exp.Op{"notLike": "a%"}})
	dts.assertPreparedSQL(b, `("a" NOT LIKE ?)`, []interface{}{"a%"})

	d.Literal(b.Clear(), exp.Ex{"a": exp.Op{"in": []string{"a", "b", "c"}}})
	dts.assertPreparedSQL(b, `("a" IN (?, ?, ?))`, []interface{}{"a", "b", "c"})

	d.Literal(b.Clear(), exp.Ex{"a": exp.Op{"notIn": []string{"a", "b", "c"}}})
	dts.assertPreparedSQL(b, `("a" NOT IN (?, ?, ?))`, []interface{}{"a", "b", "c"})

	d.Literal(b.Clear(), exp.Ex{"a": exp.Op{"is": nil, "eq": 10}})
	dts.assertPreparedSQL(b, `(("a" = ?) OR ("a" IS NULL))`, []interface{}{int64(10)})

	d.Literal(b.Clear(), exp.Ex{"a": exp.Op{"between": exp.NewRangeVal(1, 10)}})
	dts.assertPreparedSQL(b, `("a" BETWEEN ? AND ?)`, []interface{}{int64(1), int64(10)})

	d.Literal(b.Clear(), exp.Ex{"a": exp.Op{"notbetween": exp.NewRangeVal(1, 10)}})
	dts.assertPreparedSQL(b, `("a" NOT BETWEEN ? AND ?)`, []interface{}{int64(1), int64(10)})
}

func (dts *dialectTestSuite) TestLiteral_ExpressionOrMap() {
	d := sqlDialect{dialect: "test", dialectOptions: DefaultDialectOptions()}

	b := sb.NewSQLBuilder(false)
	d.Literal(b.Clear(), exp.ExOr{"a": 1, "b": true})
	dts.assertNotPreparedSQL(b, `(("a" = 1) OR ("b" IS TRUE))`)

	d.Literal(b.Clear(), exp.ExOr{"a": 1, "b": []string{"a", "b", "c"}})
	dts.assertNotPreparedSQL(b, `(("a" = 1) OR ("b" IN ('a', 'b', 'c')))`)

	b = sb.NewSQLBuilder(true)
	d.Literal(b.Clear(), exp.ExOr{"a": 1, "b": true})
	dts.assertPreparedSQL(b, `(("a" = ?) OR ("b" IS TRUE))`, []interface{}{int64(1)})

	d.Literal(b.Clear(), exp.ExOr{"a": 1, "b": []string{"a", "b", "c"}})
	dts.assertPreparedSQL(b, `(("a" = ?) OR ("b" IN (?, ?, ?)))`, []interface{}{int64(1), "a", "b", "c"})

}
func TestDialectSuite(t *testing.T) {
	suite.Run(t, new(dialectTestSuite))
}
