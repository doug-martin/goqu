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
	"github.com/stretchr/testify/assert"
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
	t := dts.T()
	opts := DefaultDialectOptions()
	opts.SupportsReturn = true
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	opts2 := DefaultDialectOptions()
	opts2.SupportsReturn = false
	d2 := sqlDialect{dialect: "test", dialectOptions: opts2}

	assert.True(t, d.SupportsReturn())
	assert.False(t, d2.SupportsReturn())
}

func (dts *dialectTestSuite) TestSupportsOrderByOnUpdate() {
	t := dts.T()
	opts := DefaultDialectOptions()
	opts.SupportsOrderByOnUpdate = true
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	opts2 := DefaultDialectOptions()
	opts2.SupportsOrderByOnUpdate = false
	d2 := sqlDialect{dialect: "test", dialectOptions: opts2}

	assert.True(t, d.SupportsOrderByOnUpdate())
	assert.False(t, d2.SupportsOrderByOnUpdate())
}

func (dts *dialectTestSuite) TestSupportsLimitOnUpdate() {
	t := dts.T()
	opts := DefaultDialectOptions()
	opts.SupportsLimitOnUpdate = true
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	opts2 := DefaultDialectOptions()
	opts2.SupportsLimitOnUpdate = false
	d2 := sqlDialect{dialect: "test", dialectOptions: opts2}

	assert.True(t, d.SupportsLimitOnUpdate())
	assert.False(t, d2.SupportsLimitOnUpdate())
}

func (dts *dialectTestSuite) TestSupportsOrderByOnDelete() {
	t := dts.T()
	opts := DefaultDialectOptions()
	opts.SupportsOrderByOnDelete = true
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	opts2 := DefaultDialectOptions()
	opts2.SupportsOrderByOnDelete = false
	d2 := sqlDialect{dialect: "test", dialectOptions: opts2}

	assert.True(t, d.SupportsOrderByOnDelete())
	assert.False(t, d2.SupportsOrderByOnDelete())
}

func (dts *dialectTestSuite) TestSupportsLimitOnDelete() {
	t := dts.T()
	opts := DefaultDialectOptions()
	opts.SupportsLimitOnDelete = true
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	opts2 := DefaultDialectOptions()
	opts2.SupportsLimitOnDelete = false
	d2 := sqlDialect{dialect: "test", dialectOptions: opts2}

	assert.True(t, d.SupportsLimitOnDelete())
	assert.False(t, d2.SupportsLimitOnDelete())
}

func (dts *dialectTestSuite) TestUpdateBeginSQL() {
	opts := DefaultDialectOptions()
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	opts2 := DefaultDialectOptions()
	opts2.UpdateClause = []byte("update")
	d2 := sqlDialect{dialect: "test", dialectOptions: opts2}

	b := sb.NewSQLBuilder(false)
	d.UpdateBeginSQL(b)
	dts.assertNotPreparedSQL(b, "UPDATE")

	d2.UpdateBeginSQL(b.Clear())
	dts.assertNotPreparedSQL(b, "update")

	b = sb.NewSQLBuilder(true)
	d.UpdateBeginSQL(b)
	dts.assertPreparedSQL(b, "UPDATE", emptyArgs)

	d2.UpdateBeginSQL(b.Clear())
	dts.assertPreparedSQL(b, "update", emptyArgs)
}

func (dts *dialectTestSuite) TestInsertBeginSQL() {
	opts := DefaultDialectOptions()
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	opts2 := DefaultDialectOptions()
	opts2.InsertClause = []byte("insert into")
	d2 := sqlDialect{dialect: "test", dialectOptions: opts2}

	b := sb.NewSQLBuilder(false)
	d.InsertBeginSQL(b, nil)
	dts.assertNotPreparedSQL(b, "INSERT INTO")

	d2.InsertBeginSQL(b.Clear(), nil)
	dts.assertNotPreparedSQL(b, "insert into")

	b = sb.NewSQLBuilder(true)
	d.InsertBeginSQL(b, nil)
	dts.assertPreparedSQL(b, "INSERT INTO", emptyArgs)

	d2.InsertBeginSQL(b.Clear(), nil)
	dts.assertPreparedSQL(b, "insert into", emptyArgs)
}

func (dts *dialectTestSuite) TestInsertBeginSQL_WithConflictExpression() {
	opts := DefaultDialectOptions()
	opts.SupportsInsertIgnoreSyntax = true
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	opts2 := DefaultDialectOptions()
	opts2.SupportsInsertIgnoreSyntax = true
	opts2.InsertIgnoreClause = []byte("insert ignore into")
	d2 := sqlDialect{dialect: "test", dialectOptions: opts2}
	ce := exp.NewDoNothingConflictExpression()

	b := sb.NewSQLBuilder(false)
	d.InsertBeginSQL(b, ce)
	dts.assertNotPreparedSQL(b, "INSERT IGNORE INTO")

	d2.InsertBeginSQL(b.Clear(), ce)
	dts.assertNotPreparedSQL(b, "insert ignore into")

	b = sb.NewSQLBuilder(true)
	d.InsertBeginSQL(b, ce)
	dts.assertPreparedSQL(b, "INSERT IGNORE INTO", emptyArgs)

	d2.InsertBeginSQL(b.Clear(), ce)
	dts.assertPreparedSQL(b, "insert ignore into", emptyArgs)
}

func (dts *dialectTestSuite) TestDeleteBeginSQL() {
	opts := DefaultDialectOptions()
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	opts2 := DefaultDialectOptions()
	opts2.DeleteClause = []byte("delete")
	d2 := sqlDialect{dialect: "test", dialectOptions: opts2}

	b := sb.NewSQLBuilder(false)
	d.DeleteBeginSQL(b)
	dts.assertNotPreparedSQL(b, "DELETE")

	d2.DeleteBeginSQL(b.Clear())
	dts.assertNotPreparedSQL(b, "delete")

	b = sb.NewSQLBuilder(true)
	d.DeleteBeginSQL(b)
	dts.assertPreparedSQL(b, "DELETE", emptyArgs)

	d2.DeleteBeginSQL(b.Clear())
	dts.assertPreparedSQL(b, "delete", emptyArgs)
}

func (dts *dialectTestSuite) TestTruncateSQL() {
	opts := DefaultDialectOptions()
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	opts2 := DefaultDialectOptions()
	opts2.TruncateClause = []byte("truncate")
	opts2.IdentityFragment = []byte(" identity")
	opts2.CascadeFragment = []byte(" cascade")
	opts2.RestrictFragment = []byte(" restrict")
	d2 := sqlDialect{dialect: "test", dialectOptions: opts2}

	cols := exp.NewColumnListExpression("a")
	b := sb.NewSQLBuilder(false)
	d.TruncateSQL(b, cols, exp.TruncateOptions{})
	dts.assertNotPreparedSQL(b, `TRUNCATE "a"`)

	d2.TruncateSQL(b.Clear(), cols, exp.TruncateOptions{})
	dts.assertNotPreparedSQL(b, `truncate "a"`)

	d.TruncateSQL(b.Clear(), cols, exp.TruncateOptions{Identity: "restart"})
	dts.assertNotPreparedSQL(b, `TRUNCATE "a" RESTART IDENTITY`)

	d2.TruncateSQL(b.Clear(), cols, exp.TruncateOptions{Identity: "restart"})
	dts.assertNotPreparedSQL(b, `truncate "a" RESTART identity`)

	d.TruncateSQL(b.Clear(), cols, exp.TruncateOptions{Cascade: true})
	dts.assertNotPreparedSQL(b, `TRUNCATE "a" CASCADE`)

	d2.TruncateSQL(b.Clear(), cols, exp.TruncateOptions{Cascade: true})
	dts.assertNotPreparedSQL(b, `truncate "a" cascade`)

	d.TruncateSQL(b.Clear(), cols, exp.TruncateOptions{Restrict: true})
	dts.assertNotPreparedSQL(b, `TRUNCATE "a" RESTRICT`)

	d2.TruncateSQL(b.Clear(), cols, exp.TruncateOptions{Restrict: true})
	dts.assertNotPreparedSQL(b, `truncate "a" restrict`)

	b = sb.NewSQLBuilder(true)
	d.TruncateSQL(b, cols, exp.TruncateOptions{})
	dts.assertPreparedSQL(b, `TRUNCATE "a"`, emptyArgs)

	d2.TruncateSQL(b.Clear(), cols, exp.TruncateOptions{})
	dts.assertPreparedSQL(b, `truncate "a"`, emptyArgs)

	d.TruncateSQL(b.Clear(), cols, exp.TruncateOptions{Identity: "restart"})
	dts.assertPreparedSQL(b, `TRUNCATE "a" RESTART IDENTITY`, emptyArgs)

	d2.TruncateSQL(b.Clear(), cols, exp.TruncateOptions{Identity: "restart"})
	dts.assertPreparedSQL(b, `truncate "a" RESTART identity`, emptyArgs)

	d.TruncateSQL(b.Clear(), cols, exp.TruncateOptions{Cascade: true})
	dts.assertPreparedSQL(b, `TRUNCATE "a" CASCADE`, emptyArgs)

	d2.TruncateSQL(b.Clear(), cols, exp.TruncateOptions{Cascade: true})
	dts.assertPreparedSQL(b, `truncate "a" cascade`, emptyArgs)

	d.TruncateSQL(b.Clear(), cols, exp.TruncateOptions{Restrict: true})
	dts.assertPreparedSQL(b, `TRUNCATE "a" RESTRICT`, emptyArgs)

	d2.TruncateSQL(b.Clear(), cols, exp.TruncateOptions{Restrict: true})
	dts.assertPreparedSQL(b, `truncate "a" restrict`, emptyArgs)
}

func (dts *dialectTestSuite) TestInsertSQL_empty() {
	opts := DefaultDialectOptions()
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	opts2 := DefaultDialectOptions()
	opts2.DefaultValuesFragment = []byte(" default values")
	d2 := sqlDialect{dialect: "test", dialectOptions: opts2}
	ic := exp.NewInsertClauses()

	b := sb.NewSQLBuilder(false)
	d.InsertSQL(b, ic)
	dts.assertNotPreparedSQL(b, " DEFAULT VALUES")

	d2.InsertSQL(b.Clear(), ic)
	dts.assertNotPreparedSQL(b, " default values")
}

func (dts *dialectTestSuite) TestInsertSQL_nilValues() {
	opts := DefaultDialectOptions()
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	opts2 := DefaultDialectOptions()
	d2 := sqlDialect{dialect: "test", dialectOptions: opts2}

	ic := exp.NewInsertClauses().SetCols(exp.NewColumnListExpression("a")).
		SetVals([][]interface{}{
			{nil},
		})

	b := sb.NewSQLBuilder(false)
	d.InsertSQL(b, ic)
	dts.assertNotPreparedSQL(b, ` ("a") VALUES (NULL)`)

	d2.InsertSQL(b.Clear(), ic)
	dts.assertNotPreparedSQL(b, ` ("a") VALUES (NULL)`)
}

func (dts *dialectTestSuite) TestInsertSQL() {
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

	bic := ic.SetCols(exp.NewColumnListExpression("a", "b")).
		SetVals([][]interface{}{
			{"a1"},
			{"a2", "b2"},
			{"a3", "b3"},
		})

	b := sb.NewSQLBuilder(false)
	d.InsertSQL(b, ic)
	dts.assertNotPreparedSQL(b, ` {"a"; "b"} values {'a1'; 'b1'}; {'a2'; 'b2'}; {'a3'; 'b3'}`)

	b = sb.NewSQLBuilder(true)
	d.InsertSQL(b, ic)
	dts.assertPreparedSQL(b, ` {"a"; "b"} values {#; #}; {#; #}; {#; #}`, []interface{}{
		"a1", "b1", "a2", "b2", "a3", "b3",
	})

	d.InsertSQL(b.Clear(), bic)
	dts.assertErrorSQL(b, "goqu: rows with different value length expected 1 got 2")
}

func (dts *dialectTestSuite) TestInsertSQL_withRows() {
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
	d.InsertSQL(b, ic)
	dts.assertNotPreparedSQL(b, ` {"a"; "b"} values {'a1'; 'b1'}; {'a2'; 'b2'}; {'a3'; 'b3'}`)

	b = sb.NewSQLBuilder(true)
	d.InsertSQL(b, ic)
	dts.assertPreparedSQL(b, ` {"a"; "b"} values {#; #}; {#; #}; {#; #}`, []interface{}{
		"a1", "b1", "a2", "b2", "a3", "b3",
	})

	d.InsertSQL(b.Clear(), bic)
	dts.assertErrorSQL(b, "goqu: rows with different value length expected 1 got 2")
}

func (dts *dialectTestSuite) TestInsertSQL_colsWithFrom() {
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
		SetFrom(newTestAppendableExpression("select c, d from test where a = 'b'", nil, nil, nil))

	b := sb.NewSQLBuilder(false)
	d.InsertSQL(b, ic)
	dts.assertNotPreparedSQL(b, ` {"a"; "b"} select c, d from test where a = 'b'`)

	b = sb.NewSQLBuilder(true)
	d.InsertSQL(b, ic)
	dts.assertPreparedSQL(b, ` {"a"; "b"} select c, d from test where a = 'b'`, emptyArgs)
}

func (dts *dialectTestSuite) TestInsertSQL_withFrom() {
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
		SetFrom(newTestAppendableExpression("select c, d from test where a = 'b'", nil, nil, nil))

	b := sb.NewSQLBuilder(false)
	d.InsertSQL(b, ic)
	dts.assertNotPreparedSQL(b, ` select c, d from test where a = 'b'`)

	b = sb.NewSQLBuilder(true)
	d.InsertSQL(b, ic)
	dts.assertPreparedSQL(b, ` select c, d from test where a = 'b'`, emptyArgs)
}

func (dts *dialectTestSuite) TestInsertSQL_onConflict() {
	opts := DefaultDialectOptions()
	// make sure the fragments are used
	opts.ConflictFragment = []byte(" on conflict")
	opts.ConflictDoNothingFragment = []byte(" do nothing")
	opts.ConflictDoUpdateFragment = []byte(" do update set ")
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	icnoc := exp.NewInsertClauses().
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
	d.InsertSQL(b, icnoc)
	dts.assertNotPreparedSQL(b, ` ("a") VALUES ('a1'), ('a2'), ('a3')`)

	d.InsertSQL(b.Clear(), icdn)
	dts.assertNotPreparedSQL(b, ` ("a") VALUES ('a1'), ('a2'), ('a3') on conflict do nothing`)

	d.InsertSQL(b.Clear(), icdu)
	dts.assertNotPreparedSQL(
		b,
		` ("a") VALUES ('a1'), ('a2'), ('a3') on conflict (test) do update set "a"='b'`,
	)

	d.InsertSQL(b.Clear(), icdoc)
	dts.assertNotPreparedSQL(b, ` ("a") VALUES ('a1'), ('a2'), ('a3') on conflict on constraint test do update set "a"='b'`)

	d.InsertSQL(b.Clear(), icduw)
	dts.assertNotPreparedSQL(b, ` ("a") VALUES ('a1'), ('a2'), ('a3') on conflict (test) do update set "a"='b' WHERE ("foo" IS TRUE)`)

	b = sb.NewSQLBuilder(true)
	d.InsertSQL(b, icdn)
	dts.assertPreparedSQL(b, ` ("a") VALUES (?), (?), (?) on conflict do nothing`, []interface{}{
		"a1", "a2", "a3",
	})

	d.InsertSQL(b.Clear(), icdu)
	dts.assertPreparedSQL(
		b,
		` ("a") VALUES (?), (?), (?) on conflict (test) do update set "a"=?`,
		[]interface{}{"a1", "a2", "a3", "b"},
	)

	d.InsertSQL(b.Clear(), icduw)
	dts.assertPreparedSQL(
		b,
		` ("a") VALUES (?), (?), (?) on conflict (test) do update set "a"=? WHERE ("foo" IS TRUE)`,
		[]interface{}{"a1", "a2", "a3", "b"},
	)
}

func (dts *dialectTestSuite) TestToUpdateSQL_empty() {
	opts := DefaultDialectOptions()
	d := sqlDialect{dialect: "test", dialectOptions: opts}
	uc := exp.NewUpdateClauses()

	b := sb.NewSQLBuilder(false)
	d.ToUpdateSQL(b, uc)
	dts.Equal(errNoSourceForUpdate, b.Error())

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

func (dts *dialectTestSuite) TestUpdateExpressionsSQL() {
	t := dts.T()

	opts := DefaultDialectOptions()
	// make sure the fragments are used
	opts.SetFragment = []byte(" set ")
	d := sqlDialect{dialect: "test", dialectOptions: opts}
	u, err := exp.NewUpdateExpressions(exp.Record{"a": "b"})
	assert.NoError(t, err)

	b := sb.NewSQLBuilder(false)
	d.UpdateExpressionsSQL(b, u...)
	dts.assertNotPreparedSQL(b, ` set "a"='b'`)

	b = sb.NewSQLBuilder(true)
	d.UpdateExpressionsSQL(b, u...)
	dts.assertPreparedSQL(b, ` set "a"=?`, []interface{}{"b"})
}

func (dts *dialectTestSuite) TestSelectSQL() {
	opts := DefaultDialectOptions()
	// make sure the fragments are used
	opts.SelectClause = []byte("select")
	opts.StarRune = '#'
	d := sqlDialect{dialect: "test", dialectOptions: opts}
	ec := exp.NewColumnListExpression()
	cs := exp.NewColumnListExpression("a", "b")
	b := sb.NewSQLBuilder(false)
	d.SelectSQL(b, ec)
	dts.assertNotPreparedSQL(b, `select #`)

	d.SelectSQL(b.Clear(), cs)
	dts.assertNotPreparedSQL(b, `select "a", "b"`)

	b = sb.NewSQLBuilder(true)
	d.SelectSQL(b, ec)
	dts.assertPreparedSQL(b, `select #`, emptyArgs)

	d.SelectSQL(b.Clear(), cs)
	dts.assertPreparedSQL(b, `select "a", "b"`, emptyArgs)
}

func (dts *dialectTestSuite) TestSelectDistinctSQL() {
	opts := DefaultDialectOptions()
	// make sure the fragments are used
	opts.SelectClause = []byte("select")
	opts.DistinctFragment = []byte(" distinct ")
	d := sqlDialect{dialect: "test", dialectOptions: opts}
	ec := exp.NewColumnListExpression()
	cs := exp.NewColumnListExpression("a", "b")
	b := sb.NewSQLBuilder(false)
	d.SelectDistinctSQL(b, ec)
	dts.assertNotPreparedSQL(b, `select distinct `)

	d.SelectDistinctSQL(b.Clear(), cs)
	dts.assertNotPreparedSQL(b, `select distinct "a", "b"`)

	b = sb.NewSQLBuilder(true)
	d.SelectDistinctSQL(b.Clear(), ec)
	dts.assertPreparedSQL(b, `select distinct `, emptyArgs)

	d.SelectDistinctSQL(b.Clear(), cs)
	dts.assertPreparedSQL(b, `select distinct "a", "b"`, emptyArgs)
}

func (dts *dialectTestSuite) TestReturningSQL() {
	opts := DefaultDialectOptions()
	// make sure the fragments are used
	opts.ReturningFragment = []byte(" returning ")
	d := sqlDialect{dialect: "test", dialectOptions: opts}
	ec := exp.NewColumnListExpression()
	cs := exp.NewColumnListExpression("a", "b")
	b := sb.NewSQLBuilder(false)
	d.ReturningSQL(b, ec)
	dts.assertNotPreparedSQL(b, ``)

	d.ReturningSQL(b.Clear(), cs)
	dts.assertNotPreparedSQL(b, ` returning "a", "b"`)

	b = sb.NewSQLBuilder(true)
	d.ReturningSQL(b.Clear(), ec)
	dts.assertPreparedSQL(b, ``, emptyArgs)

	d.ReturningSQL(b.Clear(), cs)
	dts.assertPreparedSQL(b, ` returning "a", "b"`, emptyArgs)
}

func (dts *dialectTestSuite) TestFromSQL() {
	opts := DefaultDialectOptions()
	// make sure the fragments are used
	opts.FromFragment = []byte(" from")
	d := sqlDialect{dialect: "test", dialectOptions: opts}
	ec := exp.NewColumnListExpression()
	cs := exp.NewColumnListExpression("a", "b")
	b := sb.NewSQLBuilder(false)
	d.FromSQL(b, ec)
	dts.assertNotPreparedSQL(b, ``)

	d.FromSQL(b.Clear(), cs)
	dts.assertNotPreparedSQL(b, ` from "a", "b"`)

	b = sb.NewSQLBuilder(true)
	d.FromSQL(b.Clear(), ec)
	dts.assertPreparedSQL(b, ``, emptyArgs)

	d.FromSQL(b.Clear(), cs)
	dts.assertPreparedSQL(b, ` from "a", "b"`, emptyArgs)
}

func (dts *dialectTestSuite) TestSourcesSQL() {
	opts := DefaultDialectOptions()
	d := sqlDialect{dialect: "test", dialectOptions: opts}
	ec := exp.NewColumnListExpression()
	cs := exp.NewColumnListExpression("a", "b")
	b := sb.NewSQLBuilder(false)
	d.SourcesSQL(b, ec)
	dts.assertNotPreparedSQL(b, ` `)

	d.SourcesSQL(b.Clear(), cs)
	dts.assertNotPreparedSQL(b, ` "a", "b"`)

	b = sb.NewSQLBuilder(true)
	d.SourcesSQL(b.Clear(), ec)
	dts.assertPreparedSQL(b, ` `, emptyArgs)

	d.SourcesSQL(b.Clear(), cs)
	dts.assertPreparedSQL(b, ` "a", "b"`, emptyArgs)
}

func (dts *dialectTestSuite) TestJoinSQL() {
	opts := DefaultDialectOptions()
	d := sqlDialect{dialect: "test", dialectOptions: opts}
	ti := exp.NewIdentifierExpression("", "test", "")
	uj := exp.NewUnConditionedJoinExpression(exp.NaturalJoinType, ti)
	cjo := exp.NewConditionedJoinExpression(exp.LeftJoinType, ti, exp.NewJoinOnCondition(exp.Ex{"a": "foo"}))
	cju := exp.NewConditionedJoinExpression(exp.LeftJoinType, ti, exp.NewJoinUsingCondition("a"))

	b := sb.NewSQLBuilder(false)
	d.JoinSQL(b.Clear(), exp.JoinExpressions{uj})
	dts.assertNotPreparedSQL(b, ` NATURAL JOIN "test"`)

	d.JoinSQL(b.Clear(), exp.JoinExpressions{cjo})
	dts.assertNotPreparedSQL(b, ` LEFT JOIN "test" ON ("a" = 'foo')`)

	d.JoinSQL(b.Clear(), exp.JoinExpressions{cju})
	dts.assertNotPreparedSQL(b, ` LEFT JOIN "test" USING ("a")`)

	d.JoinSQL(b.Clear(), exp.JoinExpressions{uj, cjo, cju})
	dts.assertNotPreparedSQL(b, ` NATURAL JOIN "test" LEFT JOIN "test" ON ("a" = 'foo') LEFT JOIN "test" USING ("a")`)

	d.JoinSQL(b.Clear(), exp.JoinExpressions{})
	dts.assertNotPreparedSQL(b, ``)

	b = sb.NewSQLBuilder(true)
	d.JoinSQL(b.Clear(), exp.JoinExpressions{uj})
	dts.assertPreparedSQL(b, ` NATURAL JOIN "test"`, emptyArgs)

	d.JoinSQL(b.Clear(), exp.JoinExpressions{cjo})
	dts.assertPreparedSQL(b, ` LEFT JOIN "test" ON ("a" = ?)`, []interface{}{"foo"})

	d.JoinSQL(b.Clear(), exp.JoinExpressions{cju})
	dts.assertPreparedSQL(b, ` LEFT JOIN "test" USING ("a")`, emptyArgs)

	d.JoinSQL(b.Clear(), exp.JoinExpressions{uj, cjo, cju})
	dts.assertPreparedSQL(
		b,
		` NATURAL JOIN "test" LEFT JOIN "test" ON ("a" = ?) LEFT JOIN "test" USING ("a")`,
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
	d2.JoinSQL(b.Clear(), exp.JoinExpressions{uj})
	dts.assertNotPreparedSQL(b, ` natural join "test"`)

	d2.JoinSQL(b.Clear(), exp.JoinExpressions{cjo})
	dts.assertNotPreparedSQL(b, ` left join "test" on ("a" = 'foo')`)

	d2.JoinSQL(b.Clear(), exp.JoinExpressions{cju})
	dts.assertNotPreparedSQL(b, ` left join "test" using ("a")`)

	d2.JoinSQL(b.Clear(), exp.JoinExpressions{uj, cjo, cju})
	dts.assertNotPreparedSQL(b, ` natural join "test" left join "test" on ("a" = 'foo') left join "test" using ("a")`)

	rj := exp.NewConditionedJoinExpression(exp.RightJoinType, ti, exp.NewJoinUsingCondition(exp.NewIdentifierExpression("", "", "a")))
	d2.JoinSQL(b.Clear(), exp.JoinExpressions{rj})
	dts.assertErrorSQL(b, "goqu: dialect does not support RightJoinType")

	badJoin := exp.NewConditionedJoinExpression(exp.LeftJoinType, ti, exp.NewJoinUsingCondition())
	d2.JoinSQL(b.Clear(), exp.JoinExpressions{badJoin})
	dts.assertErrorSQL(b, "goqu: join condition required for conditioned join LeftJoinType")
}

func (dts *dialectTestSuite) TestWhereSQL() {
	opts := DefaultDialectOptions()
	opts.WhereFragment = []byte(" where ")
	d := sqlDialect{dialect: "test", dialectOptions: opts}
	w := exp.Ex{"a": "b"}
	w2 := exp.Ex{"b": "c"}

	b := sb.NewSQLBuilder(false)
	d.WhereSQL(b, exp.NewExpressionList(exp.AndType, w))
	dts.assertNotPreparedSQL(b, ` where ("a" = 'b')`)

	d.WhereSQL(b.Clear(), exp.NewExpressionList(exp.AndType, w, w2))
	dts.assertNotPreparedSQL(b, ` where (("a" = 'b') AND ("b" = 'c'))`)

	d.WhereSQL(b.Clear(), exp.NewExpressionList(exp.AndType))
	dts.assertNotPreparedSQL(b, ``)

	b = sb.NewSQLBuilder(true)
	d.WhereSQL(b.Clear(), exp.NewExpressionList(exp.AndType, w))
	dts.assertPreparedSQL(b, ` where ("a" = ?)`, []interface{}{"b"})

	d.WhereSQL(b.Clear(), exp.NewExpressionList(exp.AndType, w, w2))
	dts.assertPreparedSQL(b, ` where (("a" = ?) AND ("b" = ?))`, []interface{}{"b", "c"})
}

func (dts *dialectTestSuite) TestGroupBySQL() {
	opts := DefaultDialectOptions()
	opts.GroupByFragment = []byte(" group by ")
	d := sqlDialect{dialect: "test", dialectOptions: opts}
	c1 := exp.NewIdentifierExpression("", "", "a")
	c2 := exp.NewIdentifierExpression("", "", "b")

	b := sb.NewSQLBuilder(false)
	d.GroupBySQL(b.Clear(), exp.NewColumnListExpression(c1))
	dts.assertNotPreparedSQL(b, ` group by "a"`)

	d.GroupBySQL(b.Clear(), exp.NewColumnListExpression(c1, c2))
	dts.assertNotPreparedSQL(b, ` group by "a", "b"`)

	d.GroupBySQL(b.Clear(), exp.NewColumnListExpression())
	dts.assertNotPreparedSQL(b, ``)

	b = sb.NewSQLBuilder(true)
	d.GroupBySQL(b.Clear(), exp.NewColumnListExpression(c1))
	dts.assertPreparedSQL(b, ` group by "a"`, emptyArgs)

	d.GroupBySQL(b.Clear(), exp.NewColumnListExpression(c1, c2))
	dts.assertPreparedSQL(b, ` group by "a", "b"`, emptyArgs)
}
func (dts *dialectTestSuite) TestHavingSQL() {
	opts := DefaultDialectOptions()
	opts.HavingFragment = []byte(" having ")
	d := sqlDialect{dialect: "test", dialectOptions: opts}
	w := exp.Ex{"a": "b"}
	w2 := exp.Ex{"b": "c"}

	b := sb.NewSQLBuilder(false)
	d.HavingSQL(b, exp.NewExpressionList(exp.AndType, w))
	dts.assertNotPreparedSQL(b, ` having ("a" = 'b')`)

	d.HavingSQL(b.Clear(), exp.NewExpressionList(exp.AndType, w, w2))
	dts.assertNotPreparedSQL(b, ` having (("a" = 'b') AND ("b" = 'c'))`)

	d.HavingSQL(b.Clear(), exp.NewExpressionList(exp.AndType))
	dts.assertNotPreparedSQL(b, ``)

	b = sb.NewSQLBuilder(true)
	d.HavingSQL(b.Clear(), exp.NewExpressionList(exp.AndType, w))
	dts.assertPreparedSQL(b, ` having ("a" = ?)`, []interface{}{"b"})

	d.HavingSQL(b.Clear(), exp.NewExpressionList(exp.AndType, w, w2))
	dts.assertPreparedSQL(b, ` having (("a" = ?) AND ("b" = ?))`, []interface{}{"b", "c"})
}

func (dts *dialectTestSuite) TestOrderSQL() {
	opts := DefaultDialectOptions()
	// override fragments to ensure they are used
	opts.OrderByFragment = []byte(" order by ")
	opts.AscFragment = []byte(" asc")
	opts.DescFragment = []byte(" desc")
	opts.NullsFirstFragment = []byte(" nulls first")
	opts.NullsLastFragment = []byte(" nulls last")
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	oa := exp.NewIdentifierExpression("", "", "a").Asc()
	oanf := exp.NewIdentifierExpression("", "", "a").Asc().NullsFirst()
	oanl := exp.NewIdentifierExpression("", "", "a").Asc().NullsLast()
	od := exp.NewIdentifierExpression("", "", "a").Desc()
	odnf := exp.NewIdentifierExpression("", "", "a").Desc().NullsFirst()
	odnl := exp.NewIdentifierExpression("", "", "a").Desc().NullsLast()

	b := sb.NewSQLBuilder(false)
	d.OrderSQL(b, exp.NewColumnListExpression(oa))
	dts.assertNotPreparedSQL(b, ` order by "a" asc`)

	d.OrderSQL(b.Clear(), exp.NewColumnListExpression(oanf))
	dts.assertNotPreparedSQL(b, ` order by "a" asc nulls first`)

	d.OrderSQL(b.Clear(), exp.NewColumnListExpression(oanl))
	dts.assertNotPreparedSQL(b, ` order by "a" asc nulls last`)

	d.OrderSQL(b.Clear(), exp.NewColumnListExpression(od))
	dts.assertNotPreparedSQL(b, ` order by "a" desc`)

	d.OrderSQL(b.Clear(), exp.NewColumnListExpression(odnf))
	dts.assertNotPreparedSQL(b, ` order by "a" desc nulls first`)

	d.OrderSQL(b.Clear(), exp.NewColumnListExpression(odnl))
	dts.assertNotPreparedSQL(b, ` order by "a" desc nulls last`)

	d.OrderSQL(b.Clear(), exp.NewColumnListExpression())
	dts.assertNotPreparedSQL(b, ``)

	b = sb.NewSQLBuilder(true)
	d.OrderSQL(b, exp.NewColumnListExpression(oa))
	dts.assertPreparedSQL(b, ` order by "a" asc`, emptyArgs)

	d.OrderSQL(b.Clear(), exp.NewColumnListExpression(oanf))
	dts.assertPreparedSQL(b, ` order by "a" asc nulls first`, emptyArgs)

	d.OrderSQL(b.Clear(), exp.NewColumnListExpression(oanl))
	dts.assertPreparedSQL(b, ` order by "a" asc nulls last`, emptyArgs)

	d.OrderSQL(b.Clear(), exp.NewColumnListExpression(od))
	dts.assertPreparedSQL(b, ` order by "a" desc`, emptyArgs)

	d.OrderSQL(b.Clear(), exp.NewColumnListExpression(odnf))
	dts.assertPreparedSQL(b, ` order by "a" desc nulls first`, emptyArgs)

	d.OrderSQL(b.Clear(), exp.NewColumnListExpression(odnl))
	dts.assertPreparedSQL(b, ` order by "a" desc nulls last`, emptyArgs)

	d.OrderSQL(b.Clear(), exp.NewColumnListExpression())
	dts.assertPreparedSQL(b, ``, emptyArgs)

}
func (dts *dialectTestSuite) TestLimitSQL() {
	opts := DefaultDialectOptions()
	opts.LimitFragment = []byte(" limit ")
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	b := sb.NewSQLBuilder(false)
	d.LimitSQL(b, 10)
	dts.assertNotPreparedSQL(b, ` limit 10`)

	d.LimitSQL(b.Clear(), 0)
	dts.assertNotPreparedSQL(b, ` limit 0`)

	d.LimitSQL(b.Clear(), exp.NewLiteralExpression("ALL"))
	dts.assertNotPreparedSQL(b, ` limit ALL`)

	d.LimitSQL(b.Clear(), nil)
	dts.assertNotPreparedSQL(b, ``)

	b = sb.NewSQLBuilder(true)
	d.LimitSQL(b.Clear(), 10)
	dts.assertPreparedSQL(b, ` limit ?`, []interface{}{int64(10)})

	d.LimitSQL(b.Clear(), 0)
	dts.assertPreparedSQL(b, ` limit ?`, []interface{}{int64(0)})

	d.LimitSQL(b.Clear(), exp.NewLiteralExpression("ALL"))
	dts.assertPreparedSQL(b, ` limit ALL`, emptyArgs)

	d.LimitSQL(b.Clear(), nil)
	dts.assertPreparedSQL(b, ``, emptyArgs)
}
func (dts *dialectTestSuite) TestOffsetSQL() {
	opts := DefaultDialectOptions()
	opts.OffsetFragment = []byte(" offset ")
	d := sqlDialect{dialect: "test", dialectOptions: opts}
	o := uint(10)

	b := sb.NewSQLBuilder(false)
	d.OffsetSQL(b.Clear(), o)
	dts.assertNotPreparedSQL(b, ` offset 10`)

	d.OffsetSQL(b.Clear(), 0)
	dts.assertNotPreparedSQL(b, ``)

	b = sb.NewSQLBuilder(true)
	d.OffsetSQL(b.Clear(), o)
	dts.assertPreparedSQL(b, ` offset ?`, []interface{}{int64(o)})

	d.OffsetSQL(b.Clear(), 0)
	dts.assertPreparedSQL(b, ``, emptyArgs)
}

func (dts *dialectTestSuite) TestCommonTablesSQL() {
	opts := DefaultDialectOptions()
	opts.WithFragment = []byte("with ")
	opts.RecursiveFragment = []byte("recursive ")
	d := sqlDialect{dialect: "test", dialectOptions: opts}
	tse := newTestAppendableExpression("select * from foo", emptyArgs, nil, nil)
	cte1 := exp.NewCommonTableExpression(false, "test_cte", tse)
	cte2 := exp.NewCommonTableExpression(true, "test_cte", tse)

	b := sb.NewSQLBuilder(false)
	d.CommonTablesSQL(b.Clear(), []exp.CommonTableExpression{})
	dts.assertNotPreparedSQL(b, ``)

	d.CommonTablesSQL(b.Clear(), []exp.CommonTableExpression{cte1})
	dts.assertNotPreparedSQL(b, `with test_cte AS (select * from foo) `)

	d.CommonTablesSQL(b.Clear(), []exp.CommonTableExpression{cte2})
	dts.assertNotPreparedSQL(b, `with recursive test_cte AS (select * from foo) `)

	d.CommonTablesSQL(b.Clear(), []exp.CommonTableExpression{cte1, cte2})
	dts.assertNotPreparedSQL(
		b,
		`with recursive test_cte AS (select * from foo), test_cte AS (select * from foo) `,
	)

	opts = DefaultDialectOptions()
	opts.SupportsWithCTE = false
	d = sqlDialect{dialect: "test", dialectOptions: opts}

	d.CommonTablesSQL(b.Clear(), []exp.CommonTableExpression{cte1})
	dts.assertErrorSQL(b, "goqu: adapter does not support CTE with clause")

	opts = DefaultDialectOptions()
	opts.SupportsWithCTERecursive = false
	d = sqlDialect{dialect: "test", dialectOptions: opts}

	d.CommonTablesSQL(b.Clear(), []exp.CommonTableExpression{cte2})
	dts.assertErrorSQL(b, "goqu: adapter does not support CTE with recursive clause")

	d.CommonTablesSQL(b.Clear(), []exp.CommonTableExpression{cte1})
	dts.assertNotPreparedSQL(b, `WITH test_cte AS (select * from foo) `)

}

func (dts *dialectTestSuite) TestCompoundsSQL() {
	opts := DefaultDialectOptions()
	opts.UnionFragment = []byte(" union ")
	opts.UnionAllFragment = []byte(" union all ")
	opts.IntersectFragment = []byte(" intersect ")
	opts.IntersectAllFragment = []byte(" intersect all ")
	d := sqlDialect{dialect: "test", dialectOptions: opts}
	tse := newTestAppendableExpression("select * from foo", emptyArgs, nil, nil)
	u := exp.NewCompoundExpression(exp.UnionCompoundType, tse)
	ua := exp.NewCompoundExpression(exp.UnionAllCompoundType, tse)
	i := exp.NewCompoundExpression(exp.IntersectCompoundType, tse)
	ia := exp.NewCompoundExpression(exp.IntersectAllCompoundType, tse)

	b := sb.NewSQLBuilder(false)
	d.CompoundsSQL(b.Clear(), []exp.CompoundExpression{})
	dts.assertNotPreparedSQL(b, ``)

	d.CompoundsSQL(b.Clear(), []exp.CompoundExpression{u})
	dts.assertNotPreparedSQL(b, ` union (select * from foo)`)

	d.CompoundsSQL(b.Clear(), []exp.CompoundExpression{ua})
	dts.assertNotPreparedSQL(b, ` union all (select * from foo)`)

	d.CompoundsSQL(b.Clear(), []exp.CompoundExpression{i})
	dts.assertNotPreparedSQL(b, ` intersect (select * from foo)`)

	d.CompoundsSQL(b.Clear(), []exp.CompoundExpression{ia})
	dts.assertNotPreparedSQL(b, ` intersect all (select * from foo)`)

	d.CompoundsSQL(b.Clear(), []exp.CompoundExpression{u, ua, i, ia})
	dts.assertNotPreparedSQL(
		b,
		` union (select * from foo) union all (select * from foo) intersect (select * from foo) intersect all (select * from foo)`,
	)

}

func (dts *dialectTestSuite) TestForSQL() {
	opts := DefaultDialectOptions()
	opts.ForUpdateFragment = []byte(" for update ")
	opts.ForNoKeyUpdateFragment = []byte(" for no key update ")
	opts.ForShareFragment = []byte(" for share ")
	opts.ForKeyShareFragment = []byte(" for key share ")
	opts.NowaitFragment = []byte("nowait")
	opts.SkipLockedFragment = []byte("skip locked")
	d := sqlDialect{dialect: "test", dialectOptions: opts}

	b := sb.NewSQLBuilder(false)
	d.ForSQL(b.Clear(), exp.NewLock(exp.ForNolock, exp.Wait))
	dts.assertNotPreparedSQL(b, ``)

	d.ForSQL(b.Clear(), exp.NewLock(exp.ForShare, exp.Wait))
	dts.assertNotPreparedSQL(b, ` for share `)

	d.ForSQL(b.Clear(), exp.NewLock(exp.ForShare, exp.NoWait))
	dts.assertNotPreparedSQL(b, ` for share nowait`)

	d.ForSQL(b.Clear(), exp.NewLock(exp.ForShare, exp.SkipLocked))
	dts.assertNotPreparedSQL(b, ` for share skip locked`)

	d.ForSQL(b.Clear(), exp.NewLock(exp.ForKeyShare, exp.Wait))
	dts.assertNotPreparedSQL(b, ` for key share `)

	d.ForSQL(b.Clear(), exp.NewLock(exp.ForKeyShare, exp.NoWait))
	dts.assertNotPreparedSQL(b, ` for key share nowait`)

	d.ForSQL(b.Clear(), exp.NewLock(exp.ForKeyShare, exp.SkipLocked))
	dts.assertNotPreparedSQL(b, ` for key share skip locked`)

	d.ForSQL(b.Clear(), exp.NewLock(exp.ForUpdate, exp.Wait))
	dts.assertNotPreparedSQL(b, ` for update `)

	d.ForSQL(b.Clear(), exp.NewLock(exp.ForUpdate, exp.NoWait))
	dts.assertNotPreparedSQL(b, ` for update nowait`)

	d.ForSQL(b.Clear(), exp.NewLock(exp.ForUpdate, exp.SkipLocked))
	dts.assertNotPreparedSQL(b, ` for update skip locked`)

	d.ForSQL(b.Clear(), exp.NewLock(exp.ForNoKeyUpdate, exp.Wait))
	dts.assertNotPreparedSQL(b, ` for no key update `)

	d.ForSQL(b.Clear(), exp.NewLock(exp.ForNoKeyUpdate, exp.NoWait))
	dts.assertNotPreparedSQL(b, ` for no key update nowait`)

	d.ForSQL(b.Clear(), exp.NewLock(exp.ForNoKeyUpdate, exp.SkipLocked))
	dts.assertNotPreparedSQL(b, ` for no key update skip locked`)

	d.ForSQL(b.Clear(), nil)
	dts.assertNotPreparedSQL(b, ``)

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

	b := sb.NewSQLBuilder(false)
	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Eq(1))
	dts.assertNotPreparedSQL(b, `("a" = 1)`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Eq(true))
	dts.assertNotPreparedSQL(b, `("a" IS TRUE)`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Eq(false))
	dts.assertNotPreparedSQL(b, `("a" IS FALSE)`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Eq(nil))
	dts.assertNotPreparedSQL(b, `("a" IS NULL)`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Eq([]int64{1, 2, 3}))
	dts.assertNotPreparedSQL(b, `("a" IN (1, 2, 3))`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Eq(ae))
	dts.assertNotPreparedSQL(b, `("a" IN (SELECT "id" FROM "test2"))`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Neq(1))
	dts.assertNotPreparedSQL(b, `("a" != 1)`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Neq(true))
	dts.assertNotPreparedSQL(b, `("a" IS NOT TRUE)`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Neq(false))
	dts.assertNotPreparedSQL(b, `("a" IS NOT FALSE)`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Neq(nil))
	dts.assertNotPreparedSQL(b, `("a" IS NOT NULL)`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Neq([]int64{1, 2, 3}))
	dts.assertNotPreparedSQL(b, `("a" NOT IN (1, 2, 3))`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Neq(ae))
	dts.assertNotPreparedSQL(b, `("a" NOT IN (SELECT "id" FROM "test2"))`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Is(nil))
	dts.assertNotPreparedSQL(b, `("a" IS NULL)`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Is(false))
	dts.assertNotPreparedSQL(b, `("a" IS FALSE)`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Is(true))
	dts.assertNotPreparedSQL(b, `("a" IS TRUE)`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").IsNot(nil))
	dts.assertNotPreparedSQL(b, `("a" IS NOT NULL)`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").IsNot(false))
	dts.assertNotPreparedSQL(b, `("a" IS NOT FALSE)`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").IsNot(true))
	dts.assertNotPreparedSQL(b, `("a" IS NOT TRUE)`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Gt(1))
	dts.assertNotPreparedSQL(b, `("a" > 1)`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Gte(1))
	dts.assertNotPreparedSQL(b, `("a" >= 1)`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Lt(1))
	dts.assertNotPreparedSQL(b, `("a" < 1)`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Lte(1))
	dts.assertNotPreparedSQL(b, `("a" <= 1)`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").In([]int{1, 2, 3}))
	dts.assertNotPreparedSQL(b, `("a" IN (1, 2, 3))`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").NotIn([]int{1, 2, 3}))
	dts.assertNotPreparedSQL(b, `("a" NOT IN (1, 2, 3))`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Like("a%"))
	dts.assertNotPreparedSQL(b, `("a" LIKE 'a%')`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").
		Like(regexp.MustCompile("(a|b)")))
	dts.assertNotPreparedSQL(b, `("a" ~ '(a|b)')`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").NotLike("a%"))
	dts.assertNotPreparedSQL(b, `("a" NOT LIKE 'a%')`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").
		NotLike(regexp.MustCompile("(a|b)")))
	dts.assertNotPreparedSQL(b, `("a" !~ '(a|b)')`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").ILike("a%"))
	dts.assertNotPreparedSQL(b, `("a" ILIKE 'a%')`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").
		ILike(regexp.MustCompile("(a|b)")))
	dts.assertNotPreparedSQL(b, `("a" ~* '(a|b)')`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").NotILike("a%"))
	dts.assertNotPreparedSQL(b, `("a" NOT ILIKE 'a%')`)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").
		NotILike(regexp.MustCompile("(a|b)")))
	dts.assertNotPreparedSQL(b, `("a" !~* '(a|b)')`)

	b = sb.NewSQLBuilder(true)
	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Eq(1))
	dts.assertPreparedSQL(b, `("a" = ?)`, []interface{}{int64(1)})

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Eq(true))
	dts.assertPreparedSQL(b, `("a" IS TRUE)`, []interface{}{})

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Eq(false))
	dts.assertPreparedSQL(b, `("a" IS FALSE)`, emptyArgs)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Eq(nil))
	dts.assertPreparedSQL(b, `("a" IS NULL)`, emptyArgs)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Eq([]int64{1, 2, 3}))
	dts.assertPreparedSQL(b, `("a" IN (?, ?, ?))`, []interface{}{int64(1), int64(2), int64(3)})

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Neq(1))
	dts.assertPreparedSQL(b, `("a" != ?)`, []interface{}{int64(1)})

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Neq(true))
	dts.assertPreparedSQL(b, `("a" IS NOT TRUE)`, emptyArgs)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Neq(false))
	dts.assertPreparedSQL(b, `("a" IS NOT FALSE)`, emptyArgs)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Neq(nil))
	dts.assertPreparedSQL(b, `("a" IS NOT NULL)`, emptyArgs)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Neq([]int64{1, 2, 3}))
	dts.assertPreparedSQL(b, `("a" NOT IN (?, ?, ?))`, []interface{}{int64(1), int64(2), int64(3)})

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Is(nil))
	dts.assertPreparedSQL(b, `("a" IS NULL)`, emptyArgs)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Is(false))
	dts.assertPreparedSQL(b, `("a" IS FALSE)`, emptyArgs)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Is(true))
	dts.assertPreparedSQL(b, `("a" IS TRUE)`, emptyArgs)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").IsNot(nil))
	dts.assertPreparedSQL(b, `("a" IS NOT NULL)`, emptyArgs)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").IsNot(false))
	dts.assertPreparedSQL(b, `("a" IS NOT FALSE)`, emptyArgs)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").IsNot(true))
	dts.assertPreparedSQL(b, `("a" IS NOT TRUE)`, emptyArgs)

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Gt(1))
	dts.assertPreparedSQL(b, `("a" > ?)`, []interface{}{int64(1)})

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Gte(1))
	dts.assertPreparedSQL(b, `("a" >= ?)`, []interface{}{int64(1)})

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Lt(1))
	dts.assertPreparedSQL(b, `("a" < ?)`, []interface{}{int64(1)})

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Lte(1))
	dts.assertPreparedSQL(b, `("a" <= ?)`, []interface{}{int64(1)})

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").In([]int{1, 2, 3}))
	dts.assertPreparedSQL(b, `("a" IN (?, ?, ?))`, []interface{}{int64(1), int64(2), int64(3)})

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").NotIn([]int{1, 2, 3}))
	dts.assertPreparedSQL(b, `("a" NOT IN (?, ?, ?))`, []interface{}{int64(1), int64(2), int64(3)})

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").Like("a%"))
	dts.assertPreparedSQL(b, `("a" LIKE ?)`, []interface{}{"a%"})

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").
		Like(regexp.MustCompile("(a|b)")))
	dts.assertPreparedSQL(b, `("a" ~ ?)`, []interface{}{"(a|b)"})

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").NotLike("a%"))
	dts.assertPreparedSQL(b, `("a" NOT LIKE ?)`, []interface{}{"a%"})

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").
		NotLike(regexp.MustCompile("(a|b)")))
	dts.assertPreparedSQL(b, `("a" !~ ?)`, []interface{}{"(a|b)"})

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").ILike("a%"))
	dts.assertPreparedSQL(b, `("a" ILIKE ?)`, []interface{}{"a%"})

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").
		ILike(regexp.MustCompile("(a|b)")))
	dts.assertPreparedSQL(b, `("a" ~* ?)`, []interface{}{"(a|b)"})

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").NotILike("a%"))
	dts.assertPreparedSQL(b, `("a" NOT ILIKE ?)`, []interface{}{"a%"})

	d.Literal(b.Clear(), exp.NewIdentifierExpression("", "", "a").
		NotILike(regexp.MustCompile("(a|b)")))
	dts.assertPreparedSQL(b, `("a" !~* ?)`, []interface{}{"(a|b)"})
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
