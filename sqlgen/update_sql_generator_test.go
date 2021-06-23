package sqlgen_test

import (
	"testing"

	"github.com/doug-martin/goqu/v9/exp"
	"github.com/doug-martin/goqu/v9/internal/sb"
	"github.com/doug-martin/goqu/v9/sqlgen"
	"github.com/stretchr/testify/suite"
)

type (
	updateTestCase struct {
		clause     exp.UpdateClauses
		sql        string
		isPrepared bool
		args       []interface{}
		err        string
	}
	updateSQLGeneratorSuite struct {
		baseSQLGeneratorSuite
	}
)

func (usgs *updateSQLGeneratorSuite) assertCases(usg sqlgen.UpdateSQLGenerator, testCases ...updateTestCase) {
	for _, tc := range testCases {
		b := sb.NewSQLBuilder(tc.isPrepared)
		usg.Generate(b, tc.clause)
		switch {
		case len(tc.err) > 0:
			usgs.assertErrorSQL(b, tc.err)
		case tc.isPrepared:
			usgs.assertPreparedSQL(b, tc.sql, tc.args)
		default:
			usgs.assertNotPreparedSQL(b, tc.sql)
		}
	}
}

func (usgs *updateSQLGeneratorSuite) TestDialect() {
	opts := sqlgen.DefaultDialectOptions()
	d := sqlgen.NewUpdateSQLGenerator("test", opts)
	usgs.Equal("test", d.Dialect())

	opts2 := sqlgen.DefaultDialectOptions()
	d2 := sqlgen.NewUpdateSQLGenerator("test2", opts2)
	usgs.Equal("test2", d2.Dialect())
}

func (usgs *updateSQLGeneratorSuite) TestGenerate_unsupportedFragment() {
	opts := sqlgen.DefaultDialectOptions()
	opts.UpdateSQLOrder = []sqlgen.SQLFragmentType{sqlgen.InsertBeingSQLFragment}

	uc := exp.NewUpdateClauses().
		SetTable(exp.NewIdentifierExpression("", "test", "")).
		SetSetValues(exp.Record{"a": "b", "b": "c"})
	expectedErr := "goqu: unsupported UPDATE SQL fragment InsertBeingSQLFragment"
	usgs.assertCases(
		sqlgen.NewUpdateSQLGenerator("test", opts),
		updateTestCase{clause: uc, err: expectedErr},
		updateTestCase{clause: uc, err: expectedErr, isPrepared: true},
	)
}

func (usgs *updateSQLGeneratorSuite) TestGenerate_empty() {
	uc := exp.NewUpdateClauses()
	usgs.assertCases(
		sqlgen.NewUpdateSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		updateTestCase{clause: uc, err: sqlgen.ErrNoSourceForUpdate.Error()},
		updateTestCase{clause: uc, err: sqlgen.ErrNoSourceForUpdate.Error(), isPrepared: true},
	)
}

func (usgs *updateSQLGeneratorSuite) TestGenerate_withBadUpdateValues() {
	uc := exp.NewUpdateClauses().
		SetTable(exp.NewIdentifierExpression("", "test", "")).
		SetSetValues(true)

	expectedErr := "goqu: unsupported update interface type bool"
	usgs.assertCases(
		sqlgen.NewUpdateSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		updateTestCase{clause: uc, err: expectedErr},
		updateTestCase{clause: uc, err: expectedErr, isPrepared: true},
	)
}

func (usgs *updateSQLGeneratorSuite) TestGenerate_noSetValues() {
	uc := exp.NewUpdateClauses().SetTable(exp.NewIdentifierExpression("", "test", ""))

	expectedErr := sqlgen.ErrNoSetValuesForUpdate.Error()
	usgs.assertCases(
		sqlgen.NewUpdateSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		updateTestCase{clause: uc, err: expectedErr},
		updateTestCase{clause: uc, err: expectedErr, isPrepared: true},
	)
}

func (usgs *updateSQLGeneratorSuite) TestGenerate_withFrom() {
	uc := exp.NewUpdateClauses().
		SetTable(exp.NewIdentifierExpression("", "test", "")).
		SetSetValues(exp.Record{"foo": "bar"}).
		SetFrom(exp.NewColumnListExpression("other_test"))

	ucNullSet := exp.NewUpdateClauses().
		SetTable(exp.NewIdentifierExpression("", "test", "")).
		SetSetValues(exp.Record{"foo": nil}).
		SetFrom(exp.NewColumnListExpression("other_test"))

	opts := sqlgen.DefaultDialectOptions()
	usgs.assertCases(
		sqlgen.NewUpdateSQLGenerator("test", opts),
		updateTestCase{clause: uc, sql: `UPDATE "test" SET "foo"='bar' FROM "other_test"`},
		updateTestCase{clause: uc, sql: `UPDATE "test" SET "foo"=? FROM "other_test"`, isPrepared: true, args: []interface{}{"bar"}},

		updateTestCase{clause: ucNullSet, sql: `UPDATE "test" SET "foo"=NULL FROM "other_test"`},
		updateTestCase{clause: ucNullSet, sql: `UPDATE "test" SET "foo"=? FROM "other_test"`, isPrepared: true, args: []interface{}{nil}},
	)

	opts = sqlgen.DefaultDialectOptions()
	opts.UseFromClauseForMultipleUpdateTables = false
	usgs.assertCases(
		sqlgen.NewUpdateSQLGenerator("test", opts),
		updateTestCase{clause: uc, sql: `UPDATE "test","other_test" SET "foo"='bar'`},
		updateTestCase{clause: uc, sql: `UPDATE "test","other_test" SET "foo"=?`, isPrepared: true, args: []interface{}{"bar"}},

		updateTestCase{clause: ucNullSet, sql: `UPDATE "test","other_test" SET "foo"=NULL`},
		updateTestCase{clause: ucNullSet, sql: `UPDATE "test","other_test" SET "foo"=?`, isPrepared: true, args: []interface{}{nil}},
	)

	opts = sqlgen.DefaultDialectOptions()
	opts.SupportsMultipleUpdateTables = false
	expectedErr := "goqu: test dialect does not support multiple tables in UPDATE"
	usgs.assertCases(
		sqlgen.NewUpdateSQLGenerator("test", opts),
		updateTestCase{clause: uc, err: expectedErr},
		updateTestCase{clause: uc, err: expectedErr, isPrepared: true},
	)
}

func (usgs *updateSQLGeneratorSuite) TestGenerate_withUpdateExpression() {
	opts := sqlgen.DefaultDialectOptions()
	// make sure the fragments are used
	opts.SetFragment = []byte(" set ")
	uc := exp.NewUpdateClauses().
		SetTable(exp.NewIdentifierExpression("", "test", ""))
	ucRecord := uc.SetSetValues(exp.Record{"a": "b", "b": "c"})
	ucRecordNullVal := uc.SetSetValues(exp.Record{"a": "b", "b": nil})
	ucRecordBoolVals := uc.SetSetValues(exp.Record{"a": true, "b": false})
	ucEmptyRecord := uc.SetSetValues(exp.Record{})

	usgs.assertCases(
		sqlgen.NewUpdateSQLGenerator("test", opts),
		updateTestCase{clause: ucRecord, sql: `UPDATE "test" set "a"='b',"b"='c'`},
		updateTestCase{clause: ucRecord, sql: `UPDATE "test" set "a"=?,"b"=?`, isPrepared: true, args: []interface{}{"b", "c"}},

		updateTestCase{clause: ucRecordNullVal, sql: `UPDATE "test" set "a"='b',"b"=NULL`},
		updateTestCase{clause: ucRecordNullVal, sql: `UPDATE "test" set "a"=?,"b"=?`, isPrepared: true, args: []interface{}{"b", nil}},

		updateTestCase{clause: ucRecordBoolVals, sql: `UPDATE "test" set "a"=TRUE,"b"=FALSE`},
		updateTestCase{clause: ucRecordBoolVals, sql: `UPDATE "test" set "a"=?,"b"=?`, isPrepared: true, args: []interface{}{true, false}},

		updateTestCase{clause: ucEmptyRecord, err: sqlgen.ErrNoUpdatedValuesProvided.Error()},
		updateTestCase{clause: ucEmptyRecord, err: sqlgen.ErrNoUpdatedValuesProvided.Error(), isPrepared: true},
	)
}

func (usgs *updateSQLGeneratorSuite) TestGenerate_withOrder() {
	uc := exp.NewUpdateClauses().
		SetTable(exp.NewIdentifierExpression("", "test", "")).
		SetSetValues(exp.Record{"a": "b", "b": "c"}).
		SetOrder(
			exp.NewIdentifierExpression("", "", "a").Asc(),
			exp.NewIdentifierExpression("", "", "b").Desc(),
		)

	opts := sqlgen.DefaultDialectOptions()
	opts.SupportsOrderByOnUpdate = true

	usgs.assertCases(
		sqlgen.NewUpdateSQLGenerator("test", opts),
		updateTestCase{clause: uc, sql: `UPDATE "test" SET "a"='b',"b"='c' ORDER BY "a" ASC, "b" DESC`},
		updateTestCase{
			clause:     uc,
			sql:        `UPDATE "test" SET "a"=?,"b"=? ORDER BY "a" ASC, "b" DESC`,
			isPrepared: true,
			args:       []interface{}{"b", "c"},
		},
	)

	opts = sqlgen.DefaultDialectOptions()
	opts.SupportsOrderByOnUpdate = false
	usgs.assertCases(
		sqlgen.NewUpdateSQLGenerator("test", opts),
		updateTestCase{clause: uc, sql: `UPDATE "test" SET "a"='b',"b"='c'`},
		updateTestCase{clause: uc, sql: `UPDATE "test" SET "a"=?,"b"=?`, isPrepared: true, args: []interface{}{"b", "c"}},
	)
}

func (usgs *updateSQLGeneratorSuite) TestGenerate_withLimit() {
	uc := exp.NewUpdateClauses().
		SetTable(exp.NewIdentifierExpression("", "test", "")).
		SetSetValues(exp.Record{"a": "b", "b": "c"}).
		SetLimit(10)

	opts := sqlgen.DefaultDialectOptions()
	opts.SupportsLimitOnUpdate = true

	usgs.assertCases(
		sqlgen.NewUpdateSQLGenerator("test", opts),
		updateTestCase{clause: uc, sql: `UPDATE "test" SET "a"='b',"b"='c' LIMIT 10`},
		updateTestCase{clause: uc, sql: `UPDATE "test" SET "a"=?,"b"=? LIMIT ?`, isPrepared: true, args: []interface{}{"b", "c", int64(10)}},
	)

	opts = sqlgen.DefaultDialectOptions()
	opts.SupportsLimitOnUpdate = false
	usgs.assertCases(
		sqlgen.NewUpdateSQLGenerator("test", opts),
		updateTestCase{clause: uc, sql: `UPDATE "test" SET "a"='b',"b"='c'`},
		updateTestCase{clause: uc, sql: `UPDATE "test" SET "a"=?,"b"=?`, isPrepared: true, args: []interface{}{"b", "c"}},
	)
}

func (usgs *updateSQLGeneratorSuite) TestGenerate_withCommonTables() {
	tse := newTestAppendableExpression("select * from foo", emptyArgs, nil, nil)
	uc := exp.NewUpdateClauses().
		SetTable(exp.NewIdentifierExpression("", "test_cte", "")).
		SetSetValues(exp.Record{"a": "b", "b": "c"})
	ucCte1 := uc.CommonTablesAppend(exp.NewCommonTableExpression(false, "test_cte", tse))
	ucCte2 := uc.CommonTablesAppend(exp.NewCommonTableExpression(true, "test_cte", tse))

	usgs.assertCases(
		sqlgen.NewUpdateSQLGenerator("test", sqlgen.DefaultDialectOptions()),
		updateTestCase{
			clause: ucCte1,
			sql:    `WITH test_cte AS (select * from foo) UPDATE "test_cte" SET "a"='b',"b"='c'`,
		},
		updateTestCase{
			clause:     ucCte1,
			sql:        `WITH test_cte AS (select * from foo) UPDATE "test_cte" SET "a"=?,"b"=?`,
			isPrepared: true,
			args:       []interface{}{"b", "c"},
		},

		updateTestCase{
			clause: ucCte2,
			sql:    `WITH RECURSIVE test_cte AS (select * from foo) UPDATE "test_cte" SET "a"='b',"b"='c'`,
		},
		updateTestCase{
			clause:     ucCte2,
			sql:        `WITH RECURSIVE test_cte AS (select * from foo) UPDATE "test_cte" SET "a"=?,"b"=?`,
			isPrepared: true,
			args:       []interface{}{"b", "c"},
		},
	)
}

func TestUpdateSQLGenerator(t *testing.T) {
	suite.Run(t, new(updateSQLGeneratorSuite))
}
