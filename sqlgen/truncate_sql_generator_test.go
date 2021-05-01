package sqlgen_test

import (
	"testing"

	"github.com/doug-martin/goqu/v9/exp"
	"github.com/doug-martin/goqu/v9/internal/errors"
	"github.com/doug-martin/goqu/v9/internal/sb"
	"github.com/doug-martin/goqu/v9/sqlgen"
	"github.com/stretchr/testify/suite"
)

type (
	truncateTestCase struct {
		clause     exp.TruncateClauses
		sql        string
		isPrepared bool
		args       []interface{}
		err        string
	}
	truncateSQLGeneratorSuite struct {
		baseSQLGeneratorSuite
	}
)

func (tsgs *truncateSQLGeneratorSuite) assertCases(tsg sqlgen.TruncateSQLGenerator, testCases ...truncateTestCase) {
	for _, tc := range testCases {
		b := sb.NewSQLBuilder(tc.isPrepared)
		tsg.Generate(b, tc.clause)
		switch {
		case len(tc.err) > 0:
			tsgs.assertErrorSQL(b, tc.err)
		case tc.isPrepared:
			tsgs.assertPreparedSQL(b, tc.sql, tc.args)
		default:
			tsgs.assertNotPreparedSQL(b, tc.sql)
		}
	}
}

func (tsgs *truncateSQLGeneratorSuite) TestDialect() {
	opts := sqlgen.DefaultDialectOptions()
	d := sqlgen.NewTruncateSQLGenerator("test", opts)
	tsgs.Equal("test", d.Dialect())

	opts2 := sqlgen.DefaultDialectOptions()
	d2 := sqlgen.NewTruncateSQLGenerator("test2", opts2)
	tsgs.Equal("test2", d2.Dialect())
}

func (tsgs *truncateSQLGeneratorSuite) TestGenerate() {
	opts := sqlgen.DefaultDialectOptions()
	opts.TruncateClause = []byte("truncate")

	tcNoTable := exp.NewTruncateClauses()
	tcSingle := tcNoTable.SetTable(exp.NewColumnListExpression("a"))
	tcMulti := exp.NewTruncateClauses().SetTable(exp.NewColumnListExpression("a", "b"))

	expectedNoSourceErr := "goqu: no source found when generating truncate sql"
	tsgs.assertCases(
		sqlgen.NewTruncateSQLGenerator("test", opts),
		truncateTestCase{clause: tcSingle, sql: `truncate "a"`},
		truncateTestCase{clause: tcSingle, sql: `truncate "a"`, isPrepared: true},

		truncateTestCase{clause: tcMulti, sql: `truncate "a", "b"`},
		truncateTestCase{clause: tcMulti, sql: `truncate "a", "b"`, isPrepared: true},

		truncateTestCase{clause: tcNoTable, err: expectedNoSourceErr},
		truncateTestCase{clause: tcNoTable, err: expectedNoSourceErr, isPrepared: true},
	)
}

func (tsgs *truncateSQLGeneratorSuite) TestGenerate_UnsupportedFragment() {
	opts := sqlgen.DefaultDialectOptions()
	opts.TruncateSQLOrder = []sqlgen.SQLFragmentType{sqlgen.UpdateBeginSQLFragment}
	tc := exp.NewTruncateClauses().SetTable(exp.NewColumnListExpression("a"))
	expectedErr := "goqu: unsupported TRUNCATE SQL fragment UpdateBeginSQLFragment"
	tsgs.assertCases(
		sqlgen.NewTruncateSQLGenerator("test", opts),
		truncateTestCase{clause: tc, err: expectedErr},
		truncateTestCase{clause: tc, err: expectedErr, isPrepared: true},
	)
}

func (tsgs *truncateSQLGeneratorSuite) TestGenerate_WithErroredBuilder() {
	opts := sqlgen.DefaultDialectOptions()
	opts.TruncateSQLOrder = []sqlgen.SQLFragmentType{sqlgen.UpdateBeginSQLFragment}
	d := sqlgen.NewTruncateSQLGenerator("test", opts)

	b := sb.NewSQLBuilder(true).SetError(errors.New("expected error"))
	d.Generate(b, exp.NewTruncateClauses().SetTable(exp.NewColumnListExpression("a")))
	tsgs.assertErrorSQL(b, `goqu: expected error`)
}

func (tsgs *truncateSQLGeneratorSuite) TestGenerate_WithCascade() {
	opts := sqlgen.DefaultDialectOptions()
	opts.CascadeFragment = []byte(" cascade")
	opts.RestrictFragment = []byte(" restrict")
	opts.IdentityFragment = []byte(" identity")

	tc := exp.NewTruncateClauses().SetTable(exp.NewColumnListExpression("a"))
	tcCascade := tc.SetOptions(exp.TruncateOptions{Cascade: true})
	tcRestrict := tc.SetOptions(exp.TruncateOptions{Restrict: true})
	tcRestart := tc.SetOptions(exp.TruncateOptions{Identity: "restart"})

	tsgs.assertCases(
		sqlgen.NewTruncateSQLGenerator("test", opts),
		truncateTestCase{clause: tcCascade, sql: `TRUNCATE "a" cascade`},
		truncateTestCase{clause: tcCascade, sql: `TRUNCATE "a" cascade`, isPrepared: true},

		truncateTestCase{clause: tcRestrict, sql: `TRUNCATE "a" restrict`},
		truncateTestCase{clause: tcRestrict, sql: `TRUNCATE "a" restrict`, isPrepared: true},

		truncateTestCase{clause: tcRestart, sql: `TRUNCATE "a" RESTART identity`},
		truncateTestCase{clause: tcRestart, sql: `TRUNCATE "a" RESTART identity`, isPrepared: true},
	)
}

func TestTruncateSQLGenerator(t *testing.T) {
	suite.Run(t, new(truncateSQLGeneratorSuite))
}
