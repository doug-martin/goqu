package sqlgen

import (
	"testing"

	"github.com/doug-martin/goqu/v9/exp"
	"github.com/doug-martin/goqu/v9/internal/errors"
	"github.com/doug-martin/goqu/v9/internal/sb"
	"github.com/stretchr/testify/suite"
)

type (
	deleteTestCase struct {
		clause     exp.DeleteClauses
		sql        string
		isPrepared bool
		args       []interface{}
		err        string
	}
	deleteSQLGeneratorSuite struct {
		baseSQLGeneratorSuite
	}
)

func (dsgs *deleteSQLGeneratorSuite) assertCases(dsg DeleteSQLGenerator, testCases ...deleteTestCase) {
	for _, tc := range testCases {
		b := sb.NewSQLBuilder(tc.isPrepared)
		dsg.Generate(b, tc.clause)
		switch {
		case len(tc.err) > 0:
			dsgs.assertErrorSQL(b, tc.err)
		case tc.isPrepared:
			dsgs.assertPreparedSQL(b, tc.sql, tc.args)
		default:
			dsgs.assertNotPreparedSQL(b, tc.sql)
		}
	}
}

func (dsgs *deleteSQLGeneratorSuite) TestDialect() {
	opts := DefaultDialectOptions()
	d := NewDeleteSQLGenerator("test", opts)
	dsgs.Equal("test", d.Dialect())

	opts2 := DefaultDialectOptions()
	d2 := NewDeleteSQLGenerator("test2", opts2)
	dsgs.Equal("test2", d2.Dialect())
}

func (dsgs *deleteSQLGeneratorSuite) TestGenerate() {

	dc := exp.NewDeleteClauses().
		SetFrom(exp.NewIdentifierExpression("", "test", ""))

	dsgs.assertCases(
		NewDeleteSQLGenerator("test", DefaultDialectOptions()),
		deleteTestCase{clause: dc, sql: `DELETE FROM "test"`},
		deleteTestCase{clause: dc, sql: `DELETE FROM "test"`, isPrepared: true},
	)

	opts2 := DefaultDialectOptions()
	opts2.DeleteClause = []byte("delete")

	dsgs.assertCases(
		NewDeleteSQLGenerator("test", opts2),
		deleteTestCase{clause: dc, sql: `delete FROM "test"`},
		deleteTestCase{clause: dc, sql: `delete FROM "test"`, isPrepared: true},
	)
}

func (dsgs *deleteSQLGeneratorSuite) TestGenerate_withUnsupportedFragment() {
	opts := DefaultDialectOptions()
	opts.DeleteSQLOrder = []SQLFragmentType{InsertBeingSQLFragment}
	dc := exp.NewDeleteClauses().
		SetFrom(exp.NewIdentifierExpression("", "test", ""))

	dsgs.assertCases(
		NewDeleteSQLGenerator("test", opts),
		deleteTestCase{clause: dc, err: `goqu: unsupported DELETE SQL fragment InsertBeingSQLFragment`},
		deleteTestCase{clause: dc, err: `goqu: unsupported DELETE SQL fragment InsertBeingSQLFragment`, isPrepared: true},
	)
}

func (dsgs *deleteSQLGeneratorSuite) TestGenerate_noFrom() {
	dc := exp.NewDeleteClauses()
	dsgs.assertCases(
		NewDeleteSQLGenerator("test", DefaultDialectOptions()),
		deleteTestCase{clause: dc, err: errNoSourceForDelete.Error()},
		deleteTestCase{clause: dc, err: errNoSourceForDelete.Error(), isPrepared: true},
	)
}

func (dsgs *deleteSQLGeneratorSuite) TestGenerate_withErroredBuilder() {
	opts := DefaultDialectOptions()
	d := NewDeleteSQLGenerator("test", opts)

	dc := exp.NewDeleteClauses().SetFrom(exp.NewIdentifierExpression("", "test", ""))
	b := sb.NewSQLBuilder(false).SetError(errors.New("expected error"))
	d.Generate(b, dc)
	dsgs.assertErrorSQL(b, "goqu: expected error")

	b = sb.NewSQLBuilder(true).SetError(errors.New("expected error"))
	d.Generate(b, dc)
	dsgs.assertErrorSQL(b, "goqu: expected error")
}

func (dsgs *deleteSQLGeneratorSuite) TestGenerate_withCommonTables() {
	opts := DefaultDialectOptions()
	opts.WithFragment = []byte("with ")
	opts.RecursiveFragment = []byte("recursive ")

	tse := newTestAppendableExpression("select * from foo", emptyArgs, nil, nil)

	dc := exp.NewDeleteClauses().SetFrom(exp.NewIdentifierExpression("", "test_cte", ""))
	dcCte1 := dc.CommonTablesAppend(exp.NewCommonTableExpression(false, "test_cte", tse))
	dcCte2 := dc.CommonTablesAppend(exp.NewCommonTableExpression(true, "test_cte", tse))

	dsgs.assertCases(
		NewDeleteSQLGenerator("test", opts),
		deleteTestCase{clause: dcCte1, sql: `with test_cte AS (select * from foo) DELETE FROM "test_cte"`},
		deleteTestCase{clause: dcCte1, sql: `with test_cte AS (select * from foo) DELETE FROM "test_cte"`, isPrepared: true},

		deleteTestCase{clause: dcCte2, sql: `with recursive test_cte AS (select * from foo) DELETE FROM "test_cte"`},
		deleteTestCase{clause: dcCte2, sql: `with recursive test_cte AS (select * from foo) DELETE FROM "test_cte"`, isPrepared: true},
	)

	opts.SupportsWithCTE = false
	expectedErr := errCTENotSupported("test")
	dsgs.assertCases(
		NewDeleteSQLGenerator("test", opts),
		deleteTestCase{clause: dcCte1, err: expectedErr.Error()},
		deleteTestCase{clause: dcCte1, err: expectedErr.Error(), isPrepared: true},

		deleteTestCase{clause: dcCte2, err: expectedErr.Error()},
		deleteTestCase{clause: dcCte2, err: expectedErr.Error(), isPrepared: true},
	)

	opts.SupportsWithCTE = true
	opts.SupportsWithCTERecursive = false
	expectedErr = errRecursiveCTENotSupported("test")
	dsgs.assertCases(
		NewDeleteSQLGenerator("test", opts),
		deleteTestCase{clause: dcCte1, sql: `with test_cte AS (select * from foo) DELETE FROM "test_cte"`},
		deleteTestCase{clause: dcCte1, sql: `with test_cte AS (select * from foo) DELETE FROM "test_cte"`, isPrepared: true},

		deleteTestCase{clause: dcCte2, err: expectedErr.Error()},
		deleteTestCase{clause: dcCte2, err: expectedErr.Error(), isPrepared: true},
	)
}

func (dsgs *deleteSQLGeneratorSuite) TestGenerate_withWhere() {
	dc := exp.NewDeleteClauses().
		SetFrom(exp.NewIdentifierExpression("", "test", "")).
		WhereAppend(exp.NewLiteralExpression(`"a"=?`, 1))
	dsgs.assertCases(
		NewDeleteSQLGenerator("test", DefaultDialectOptions()),
		deleteTestCase{clause: dc, sql: `DELETE FROM "test" WHERE "a"=1`},
		deleteTestCase{clause: dc, sql: `DELETE FROM "test" WHERE "a"=?`, isPrepared: true, args: []interface{}{
			int64(1),
		}},
	)
}

func (dsgs *deleteSQLGeneratorSuite) TestGenerate_withOrder() {
	opts := DefaultDialectOptions()
	opts.SupportsOrderByOnDelete = true

	dc := exp.NewDeleteClauses().
		SetFrom(exp.NewIdentifierExpression("", "test", "")).
		SetOrder(exp.NewIdentifierExpression("", "", "c").Desc())

	dsgs.assertCases(
		NewDeleteSQLGenerator("test", opts),
		deleteTestCase{clause: dc, sql: `DELETE FROM "test" ORDER BY "c" DESC`},
		deleteTestCase{clause: dc, sql: `DELETE FROM "test" ORDER BY "c" DESC`, isPrepared: true},
	)

	opts.SupportsOrderByOnDelete = false
	dsgs.assertCases(
		NewDeleteSQLGenerator("test", opts),
		deleteTestCase{clause: dc, sql: `DELETE FROM "test"`},
		deleteTestCase{clause: dc, sql: `DELETE FROM "test"`, isPrepared: true},
	)
}

func (dsgs *deleteSQLGeneratorSuite) TestGenerate_withLimit() {
	opts := DefaultDialectOptions()
	opts.SupportsLimitOnDelete = true

	dc := exp.NewDeleteClauses().
		SetFrom(exp.NewIdentifierExpression("", "test", "")).
		SetLimit(1)

	dsgs.assertCases(
		NewDeleteSQLGenerator("test", opts),
		deleteTestCase{clause: dc, sql: `DELETE FROM "test" LIMIT 1`},
		deleteTestCase{clause: dc, sql: `DELETE FROM "test" LIMIT ?`, isPrepared: true, args: []interface{}{int64(1)}},
	)

	opts.SupportsLimitOnDelete = false
	dsgs.assertCases(
		NewDeleteSQLGenerator("test", opts),
		deleteTestCase{clause: dc, sql: `DELETE FROM "test"`},
		deleteTestCase{clause: dc, sql: `DELETE FROM "test"`, isPrepared: true},
	)
}

func (dsgs *deleteSQLGeneratorSuite) TestGenerate_withReturning() {
	opts := DefaultDialectOptions()
	opts.SupportsReturn = true

	dc := exp.NewDeleteClauses().
		SetFrom(exp.NewIdentifierExpression("", "test", "")).
		SetReturning(exp.NewColumnListExpression("a", "b"))

	dsgs.assertCases(
		NewDeleteSQLGenerator("test", opts),
		deleteTestCase{clause: dc, sql: `DELETE FROM "test" RETURNING "a", "b"`},
		deleteTestCase{clause: dc, sql: `DELETE FROM "test" RETURNING "a", "b"`, isPrepared: true},
	)

	opts.SupportsReturn = false
	expectedErr := `goqu: dialect does not support RETURNING clause [dialect=test]`
	dsgs.assertCases(
		NewDeleteSQLGenerator("test", opts),
		deleteTestCase{clause: dc, err: expectedErr},
		deleteTestCase{clause: dc, err: expectedErr, isPrepared: true},
	)
}

func TestDeleteSQLGenerator(t *testing.T) {
	suite.Run(t, new(deleteSQLGeneratorSuite))
}
