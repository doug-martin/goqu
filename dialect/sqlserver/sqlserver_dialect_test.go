package sqlserver_test

import (
	"testing"

	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/stretchr/testify/suite"
)

type (
	sqlserverDialectSuite struct {
		suite.Suite
	}
	sqlTestCase struct {
		ds         exp.SQLExpression
		sql        string
		err        string
		isPrepared bool
		args       []interface{}
	}
)

func (sds *sqlserverDialectSuite) GetDs(table string) *goqu.SelectDataset {
	return goqu.Dialect("sqlserver").From(table)
}

func (sds *sqlserverDialectSuite) assertSQL(cases ...sqlTestCase) {
	for i, c := range cases {
		actualSQL, actualArgs, err := c.ds.ToSQL()
		if c.err == "" {
			sds.NoError(err, "test case %d failed", i)
		} else {
			sds.EqualError(err, c.err, "test case %d failed", i)
		}
		sds.Equal(c.sql, actualSQL, "test case %d failed", i)
		if c.isPrepared && c.args != nil || len(c.args) > 0 {
			sds.Equal(c.args, actualArgs, "test case %d failed", i)
		} else {
			sds.Empty(actualArgs, "test case %d failed", i)
		}
	}
}

func (sds *sqlserverDialectSuite) TestBitwiseOperations() {
	col := goqu.C("a")
	ds := sds.GetDs("test")
	sds.assertSQL(
		sqlTestCase{ds: ds.Where(col.BitwiseInversion()), sql: "SELECT * FROM \"test\" WHERE (~ \"a\")"},
		sqlTestCase{ds: ds.Where(col.BitwiseAnd(1)), sql: "SELECT * FROM \"test\" WHERE (\"a\" & 1)"},
		sqlTestCase{ds: ds.Where(col.BitwiseOr(1)), sql: "SELECT * FROM \"test\" WHERE (\"a\" | 1)"},
		sqlTestCase{ds: ds.Where(col.BitwiseXor(1)), sql: "SELECT * FROM \"test\" WHERE (\"a\" ^ 1)"},
		sqlTestCase{ds: ds.Where(col.BitwiseLeftShift(1)), err: "goqu: bitwise operator 'Left Shift' not supported"},
		sqlTestCase{ds: ds.Where(col.BitwiseRightShift(1)), err: "goqu: bitwise operator 'Right Shift' not supported"},
	)
}

func TestDatasetAdapterSuite(t *testing.T) {
	suite.Run(t, new(sqlserverDialectSuite))
}
