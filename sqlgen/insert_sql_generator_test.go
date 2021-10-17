package sqlgen_test

import (
	"testing"

	"github.com/doug-martin/goqu/v9/exp"
	"github.com/doug-martin/goqu/v9/internal/sb"
	"github.com/doug-martin/goqu/v9/sqlgen"
	"github.com/stretchr/testify/suite"
)

type (
	insertTestCase struct {
		clause     exp.InsertClauses
		sql        string
		isPrepared bool
		args       []interface{}
		err        string
	}
	insertSQLGeneratorSuite struct {
		baseSQLGeneratorSuite
	}
)

func (igs *insertSQLGeneratorSuite) assertCases(isg sqlgen.InsertSQLGenerator, testCases ...insertTestCase) {
	for _, tc := range testCases {
		b := sb.NewSQLBuilder(tc.isPrepared)
		isg.Generate(b, tc.clause)
		switch {
		case len(tc.err) > 0:
			igs.assertErrorSQL(b, tc.err)
		case tc.isPrepared:
			igs.assertPreparedSQL(b, tc.sql, tc.args)
		default:
			igs.assertNotPreparedSQL(b, tc.sql)
		}
	}
}

func (igs *insertSQLGeneratorSuite) TestDialect() {
	opts := sqlgen.DefaultDialectOptions()
	d := sqlgen.NewInsertSQLGenerator("test", opts)
	igs.Equal("test", d.Dialect())

	opts2 := sqlgen.DefaultDialectOptions()
	d2 := sqlgen.NewInsertSQLGenerator("test2", opts2)
	igs.Equal("test2", d2.Dialect())
}

func (igs *insertSQLGeneratorSuite) TestGenerate_UnsupportedFragment() {
	opts := sqlgen.DefaultDialectOptions()
	opts.InsertSQLOrder = []sqlgen.SQLFragmentType{sqlgen.UpdateBeginSQLFragment}
	d := sqlgen.NewInsertSQLGenerator("test", opts)

	b := sb.NewSQLBuilder(true)
	ic := exp.NewInsertClauses().
		SetInto(exp.NewIdentifierExpression("", "test", ""))
	d.Generate(b, ic)
	igs.assertErrorSQL(b, `goqu: unsupported INSERT SQL fragment UpdateBeginSQLFragment`)
}

func (igs *insertSQLGeneratorSuite) TestGenerate_empty() {
	ic := exp.NewInsertClauses().
		SetInto(exp.NewIdentifierExpression("", "test", ""))

	igs.assertCases(
		sqlgen.NewInsertSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		insertTestCase{clause: ic, sql: `INSERT INTO "test" DEFAULT VALUES`},
		insertTestCase{clause: ic, sql: `INSERT INTO "test" DEFAULT VALUES`, isPrepared: true},
	)

	opts2 := sqlgen.DefaultDialectOptions()
	opts2.DefaultValuesFragment = []byte(" default values")

	igs.assertCases(
		sqlgen.NewInsertSQLGenerator("test", opts2),
		insertTestCase{clause: ic, sql: `INSERT INTO "test" default values`},
		insertTestCase{clause: ic, sql: `INSERT INTO "test" default values`, isPrepared: true},
	)
}

func (igs *insertSQLGeneratorSuite) TestGenerate_nilValues() {
	ic := exp.NewInsertClauses().
		SetInto(exp.NewIdentifierExpression("", "test", "")).
		SetCols(exp.NewColumnListExpression("a")).
		SetVals([][]interface{}{
			{nil},
		})

	igs.assertCases(
		sqlgen.NewInsertSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		insertTestCase{clause: ic, sql: `INSERT INTO "test" ("a") VALUES (NULL)`},
		insertTestCase{clause: ic, sql: `INSERT INTO "test" ("a") VALUES (?)`, isPrepared: true, args: []interface{}{nil}},
	)
}

func (igs *insertSQLGeneratorSuite) TestGenerate_colsAndVals() {
	opts := sqlgen.DefaultDialectOptions()
	opts.LeftParenRune = '{'
	opts.RightParenRune = '}'
	opts.ValuesFragment = []byte(" values ")
	opts.LeftParenRune = '{'
	opts.RightParenRune = '}'
	opts.CommaRune = ';'
	opts.PlaceHolderFragment = []byte("#")

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

	igs.assertCases(
		sqlgen.NewInsertSQLGenerator("test", opts),
		insertTestCase{clause: ic, sql: `INSERT INTO "test" {"a"; "b"} values {'a1'; 'b1'}; {'a2'; 'b2'}; {'a3'; 'b3'}`},
		insertTestCase{clause: ic, sql: `INSERT INTO "test" {"a"; "b"} values {#; #}; {#; #}; {#; #}`, isPrepared: true, args: []interface{}{
			"a1", "b1", "a2", "b2", "a3", "b3",
		}},

		insertTestCase{clause: bic, err: `goqu: rows with different value length expected 1 got 2`},
		insertTestCase{clause: bic, err: `goqu: rows with different value length expected 1 got 2`, isPrepared: true},
	)
}

func (igs *insertSQLGeneratorSuite) TestGenerate_withNoInto() {
	opts := sqlgen.DefaultDialectOptions()
	opts.LeftParenRune = '{'
	opts.RightParenRune = '}'
	opts.ValuesFragment = []byte(" values ")
	opts.LeftParenRune = '{'
	opts.RightParenRune = '}'
	opts.CommaRune = ';'
	opts.PlaceHolderFragment = []byte("#")

	ic := exp.NewInsertClauses().
		SetCols(exp.NewColumnListExpression("a", "b")).
		SetVals([][]interface{}{
			{"a1", "b1"},
			{"a2", "b2"},
			{"a3", "b3"},
		})
	expectedErr := "goqu: no source found when generating insert sql"
	igs.assertCases(
		sqlgen.NewInsertSQLGenerator("test", opts),
		insertTestCase{clause: ic, err: expectedErr},
		insertTestCase{clause: ic, err: expectedErr, isPrepared: true},
	)
}

func (igs *insertSQLGeneratorSuite) TestGenerate_withRows() {
	opts := sqlgen.DefaultDialectOptions()
	opts.LeftParenRune = '{'
	opts.RightParenRune = '}'
	opts.ValuesFragment = []byte(" values ")
	opts.LeftParenRune = '{'
	opts.RightParenRune = '}'
	opts.CommaRune = ';'
	opts.PlaceHolderFragment = []byte("#")

	ic := exp.NewInsertClauses().
		SetInto(exp.NewIdentifierExpression("", "test", "")).
		SetRows([]interface{}{
			exp.Record{"a": "a1", "b": "b1"},
			exp.Record{"a": "a2", "b": "b2"},
			exp.Record{"a": "a3", "b": "b3"},
		})

	bic := ic.SetRows([]interface{}{
		exp.Record{"a": "a1"},
		exp.Record{"a": "a2", "b": "b2"},
		exp.Record{"a": "a3", "b": "b3"},
	})

	igs.assertCases(
		sqlgen.NewInsertSQLGenerator("test", opts),
		insertTestCase{clause: ic, sql: `INSERT INTO "test" {"a"; "b"} values {'a1'; 'b1'}; {'a2'; 'b2'}; {'a3'; 'b3'}`},
		insertTestCase{clause: ic, sql: `INSERT INTO "test" {"a"; "b"} values {#; #}; {#; #}; {#; #}`, isPrepared: true, args: []interface{}{
			"a1", "b1", "a2", "b2", "a3", "b3",
		}},

		insertTestCase{clause: bic, err: `goqu: rows with different value length expected 1 got 2`},
		insertTestCase{clause: bic, err: `goqu: rows with different value length expected 1 got 2`, isPrepared: true},
	)
}

func (igs *insertSQLGeneratorSuite) TestGenerate_withEmptyRows() {
	ic := exp.NewInsertClauses().
		SetInto(exp.NewIdentifierExpression("", "test", "")).
		SetRows([]interface{}{exp.Record{}})

	igs.assertCases(
		sqlgen.NewInsertSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		insertTestCase{clause: ic, sql: `INSERT INTO "test" DEFAULT VALUES`},
		insertTestCase{clause: ic, sql: `INSERT INTO "test" DEFAULT VALUES`, isPrepared: true},
	)

	opts2 := sqlgen.DefaultDialectOptions()
	opts2.DefaultValuesFragment = []byte(" default values")

	igs.assertCases(
		sqlgen.NewInsertSQLGenerator("test", opts2),
		insertTestCase{clause: ic, sql: `INSERT INTO "test" default values`},
		insertTestCase{clause: ic, sql: `INSERT INTO "test" default values`, isPrepared: true},
	)
}

func (igs *insertSQLGeneratorSuite) TestGenerate_withRowsAppendableExpression() {
	ic := exp.NewInsertClauses().
		SetInto(exp.NewIdentifierExpression("", "test", "")).
		SetRows([]interface{}{newTestAppendableExpression(`select * from "other"`, emptyArgs, nil, nil)})

	igs.assertCases(
		sqlgen.NewInsertSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		insertTestCase{clause: ic, sql: `INSERT INTO "test" select * from "other"`},
		insertTestCase{clause: ic, sql: `INSERT INTO "test" select * from "other"`, isPrepared: true},
	)
}

func (igs *insertSQLGeneratorSuite) TestGenerate_withFrom() {
	ic := exp.NewInsertClauses().
		SetInto(exp.NewIdentifierExpression("", "test", "")).
		SetFrom(newTestAppendableExpression(`select c, d from test where a = 'b'`, nil, nil, nil))

	icCols := ic.SetCols(exp.NewColumnListExpression("a", "b"))
	igs.assertCases(
		sqlgen.NewInsertSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		insertTestCase{clause: ic, sql: `INSERT INTO "test" select c, d from test where a = 'b'`},
		insertTestCase{clause: ic, sql: `INSERT INTO "test" select c, d from test where a = 'b'`, isPrepared: true},

		insertTestCase{clause: icCols, sql: `INSERT INTO "test" ("a", "b") select c, d from test where a = 'b'`},
		insertTestCase{clause: icCols, sql: `INSERT INTO "test" ("a", "b") select c, d from test where a = 'b'`, isPrepared: true},
	)
}

func (igs *insertSQLGeneratorSuite) TestGenerate_onConflict() {
	opts := sqlgen.DefaultDialectOptions()
	// make sure the fragments are used
	opts.ConflictFragment = []byte(" on conflict")
	opts.ConflictDoNothingFragment = []byte(" do nothing")
	opts.ConflictDoUpdateFragment = []byte(" do update set ")

	ic := exp.NewInsertClauses().
		SetInto(exp.NewIdentifierExpression("", "test", "")).
		SetCols(exp.NewColumnListExpression("a")).
		SetVals([][]interface{}{
			{"a1"},
		})
	icDn := ic.SetOnConflict(exp.NewDoNothingConflictExpression())
	icDu := ic.SetOnConflict(exp.NewDoUpdateConflictExpression("test", exp.Record{"a": "b"}))
	icAsDu := ic.SetAlias(exp.NewIdentifierExpression("", "new", "")).SetOnConflict(
		exp.NewDoUpdateConflictExpression("test", exp.Record{"a": exp.NewIdentifierExpression("", "new", "a")}),
	)
	icDoc := ic.SetOnConflict(exp.NewDoUpdateConflictExpression("on constraint test", exp.Record{"a": "b"}))
	icDuw := ic.SetOnConflict(
		exp.NewDoUpdateConflictExpression("test", exp.Record{"a": "b"}).Where(exp.Ex{"foo": true}),
	)

	icDuNil := ic.SetOnConflict(exp.NewDoUpdateConflictExpression("test", nil))
	icDuBad := ic.SetOnConflict(exp.NewDoUpdateConflictExpression("test", true))

	igs.assertCases(
		sqlgen.NewInsertSQLGenerator("test", opts),
		insertTestCase{clause: icDn, sql: `INSERT INTO "test" ("a") VALUES ('a1') on conflict do nothing`},
		insertTestCase{
			clause:     icDn,
			sql:        `INSERT INTO "test" ("a") VALUES (?) on conflict do nothing`,
			isPrepared: true,
			args:       []interface{}{"a1"},
		},

		insertTestCase{clause: icDu, sql: `INSERT INTO "test" ("a") VALUES ('a1') on conflict (test) do update set "a"='b'`},
		insertTestCase{
			clause:     icDu,
			sql:        `INSERT INTO "test" ("a") VALUES (?) on conflict (test) do update set "a"=?`,
			isPrepared: true,
			args:       []interface{}{"a1", "b"},
		},

		insertTestCase{clause: icAsDu, sql: `INSERT INTO "test" ("a") VALUES ('a1') AS "new" on conflict (test) do update set "a"="new"."a"`},
		insertTestCase{
			clause:     icAsDu,
			sql:        `INSERT INTO "test" ("a") VALUES (?) AS "new" on conflict (test) do update set "a"="new"."a"`,
			isPrepared: true,
			args:       []interface{}{"a1"},
		},

		insertTestCase{clause: icDoc, sql: `INSERT INTO "test" ("a") VALUES ('a1') on conflict on constraint test do update set "a"='b'`},
		insertTestCase{
			clause:     icDoc,
			sql:        `INSERT INTO "test" ("a") VALUES (?) on conflict on constraint test do update set "a"=?`,
			isPrepared: true,
			args:       []interface{}{"a1", "b"},
		},

		insertTestCase{
			clause: icDuw,
			sql:    `INSERT INTO "test" ("a") VALUES ('a1') on conflict (test) do update set "a"='b' WHERE ("foo" IS TRUE)`,
		},
		insertTestCase{
			clause:     icDuw,
			sql:        `INSERT INTO "test" ("a") VALUES (?) on conflict (test) do update set "a"=? WHERE ("foo" IS TRUE)`,
			isPrepared: true,
			args:       []interface{}{"a1", "b"},
		},

		insertTestCase{clause: icDuNil, err: sqlgen.ErrConflictUpdateValuesRequired.Error()},
		insertTestCase{clause: icDuNil, err: sqlgen.ErrConflictUpdateValuesRequired.Error(), isPrepared: true},

		insertTestCase{clause: icDuBad, err: "goqu: unsupported update interface type bool"},
		insertTestCase{clause: icDuBad, err: "goqu: unsupported update interface type bool", isPrepared: true},
	)
	opts.SupportsInsertIgnoreSyntax = true
	opts.InsertIgnoreClause = []byte("insert ignore into")
	igs.assertCases(
		sqlgen.NewInsertSQLGenerator("test", opts),
		insertTestCase{clause: icDn, sql: `insert ignore into "test" ("a") VALUES ('a1') on conflict do nothing`},
		insertTestCase{
			clause:     icDn,
			sql:        `insert ignore into "test" ("a") VALUES (?) on conflict do nothing`,
			isPrepared: true,
			args:       []interface{}{"a1"},
		},

		insertTestCase{
			clause: icDu,
			sql:    `insert ignore into "test" ("a") VALUES ('a1') on conflict (test) do update set "a"='b'`,
		},
		insertTestCase{
			clause:     icDu,
			sql:        `insert ignore into "test" ("a") VALUES (?) on conflict (test) do update set "a"=?`,
			isPrepared: true,
			args:       []interface{}{"a1", "b"},
		},

		insertTestCase{
			clause: icDoc,
			sql:    `insert ignore into "test" ("a") VALUES ('a1') on conflict on constraint test do update set "a"='b'`,
		},
		insertTestCase{
			clause:     icDoc,
			sql:        `insert ignore into "test" ("a") VALUES (?) on conflict on constraint test do update set "a"=?`,
			isPrepared: true,
			args:       []interface{}{"a1", "b"},
		},

		insertTestCase{
			clause: icDuw,
			sql:    `insert ignore into "test" ("a") VALUES ('a1') on conflict (test) do update set "a"='b' WHERE ("foo" IS TRUE)`,
		},
		insertTestCase{
			clause:     icDuw,
			sql:        `insert ignore into "test" ("a") VALUES (?) on conflict (test) do update set "a"=? WHERE ("foo" IS TRUE)`,
			isPrepared: true,
			args:       []interface{}{"a1", "b"},
		},

		insertTestCase{clause: icDuNil, err: sqlgen.ErrConflictUpdateValuesRequired.Error()},
		insertTestCase{clause: icDuNil, err: sqlgen.ErrConflictUpdateValuesRequired.Error(), isPrepared: true},

		insertTestCase{clause: icDuBad, err: "goqu: unsupported update interface type bool"},
		insertTestCase{clause: icDuBad, err: "goqu: unsupported update interface type bool", isPrepared: true},
	)

	opts.SupportsConflictUpdateWhere = false
	expectedErr := "goqu: dialect does not support upsert with where clause [dialect=test]"
	igs.assertCases(
		sqlgen.NewInsertSQLGenerator("test", opts),
		insertTestCase{clause: icDuw, err: expectedErr},
		insertTestCase{clause: icDuw, err: expectedErr, isPrepared: true},
	)
}

func (igs *insertSQLGeneratorSuite) TestGenerate_withCommonTables() {
	opts := sqlgen.DefaultDialectOptions()
	opts.WithFragment = []byte("with ")
	opts.RecursiveFragment = []byte("recursive ")

	tse := newTestAppendableExpression("select * from foo", emptyArgs, nil, nil)

	ic := exp.NewInsertClauses().SetInto(exp.NewIdentifierExpression("", "test_cte", ""))
	icCte1 := ic.CommonTablesAppend(exp.NewCommonTableExpression(false, "test_cte", tse))
	icCte2 := ic.CommonTablesAppend(exp.NewCommonTableExpression(true, "test_cte", tse))

	igs.assertCases(
		sqlgen.NewInsertSQLGenerator("test", opts),
		insertTestCase{
			clause: icCte1,
			sql:    `with test_cte AS (select * from foo) INSERT INTO "test_cte" DEFAULT VALUES`,
		},
		insertTestCase{
			clause:     icCte1,
			sql:        `with test_cte AS (select * from foo) INSERT INTO "test_cte" DEFAULT VALUES`,
			isPrepared: true,
		},

		insertTestCase{
			clause: icCte2,
			sql:    `with recursive test_cte AS (select * from foo) INSERT INTO "test_cte" DEFAULT VALUES`,
		},
		insertTestCase{
			clause:     icCte2,
			sql:        `with recursive test_cte AS (select * from foo) INSERT INTO "test_cte" DEFAULT VALUES`,
			isPrepared: true,
		},
	)

	opts.SupportsWithCTE = false
	expectedErr := "goqu: dialect does not support CTE WITH clause [dialect=test]"
	igs.assertCases(
		sqlgen.NewInsertSQLGenerator("test", opts),
		insertTestCase{clause: icCte1, err: expectedErr},
		insertTestCase{clause: icCte1, err: expectedErr, isPrepared: true},

		insertTestCase{clause: icCte2, err: expectedErr},
		insertTestCase{clause: icCte2, err: expectedErr, isPrepared: true},
	)

	opts.SupportsWithCTE = true
	opts.SupportsWithCTERecursive = false
	expectedErr = "goqu: dialect does not support CTE WITH RECURSIVE clause [dialect=test]"
	igs.assertCases(
		sqlgen.NewInsertSQLGenerator("test", opts),
		insertTestCase{
			clause: icCte1,
			sql:    `with test_cte AS (select * from foo) INSERT INTO "test_cte" DEFAULT VALUES`,
		},
		insertTestCase{
			clause:     icCte1,
			sql:        `with test_cte AS (select * from foo) INSERT INTO "test_cte" DEFAULT VALUES`,
			isPrepared: true,
		},

		insertTestCase{clause: icCte2, err: expectedErr},
		insertTestCase{clause: icCte2, err: expectedErr, isPrepared: true},
	)
}

func (igs *insertSQLGeneratorSuite) TestGenerate_withReturning() {
	ic := exp.NewInsertClauses().
		SetInto(exp.NewIdentifierExpression("", "test", "")).
		SetCols(exp.NewColumnListExpression("a", "b")).
		SetVals([][]interface{}{
			{"a1", "b1"},
		}).
		SetReturning(exp.NewColumnListExpression("a", "b"))

	igs.assertCases(
		sqlgen.NewInsertSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		insertTestCase{clause: ic, sql: `INSERT INTO "test" ("a", "b") VALUES ('a1', 'b1') RETURNING "a", "b"`},
		insertTestCase{clause: ic, sql: `INSERT INTO "test" ("a", "b") VALUES (?, ?) RETURNING "a", "b"`, isPrepared: true, args: []interface{}{
			"a1", "b1",
		}},
	)
}

func TestInsertSQLGenerator(t *testing.T) {
	suite.Run(t, new(insertSQLGeneratorSuite))
}
