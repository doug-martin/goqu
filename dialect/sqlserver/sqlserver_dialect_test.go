package sqlserver_test

import (
	"testing"

	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/stretchr/testify/assert"
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

func (sds *sqlserverDialectSuite) TestSelectDistinctWithLimit() {
	ds := sds.GetDs("test")
	sds.assertSQL(
		// DISTINCT has to come before TOP.
		sqlTestCase{ds: ds.Select("a").Distinct().Limit(1), sql: `SELECT DISTINCT  TOP (1) "a" FROM "test"`},
		sqlTestCase{ds: ds.Distinct().Limit(5), sql: `SELECT DISTINCT  TOP (5) * FROM "test"`},
		sqlTestCase{
			ds:         ds.Select("a").Distinct().Limit(1).Prepared(true),
			sql:        `SELECT DISTINCT  TOP (@p1) "a" FROM "test"`,
			isPrepared: true,
			args:       []interface{}{int64(1)},
		},
		// Without a limit, or with an offset (OFFSET/FETCH instead of TOP), nothing changes.
		sqlTestCase{ds: ds.Select("a").Distinct(), sql: `SELECT DISTINCT "a" FROM "test"`},
		sqlTestCase{
			ds:  ds.Select("a").Distinct().Order(goqu.C("a").Asc()).Limit(1).Offset(2),
			sql: `SELECT DISTINCT "a" FROM "test" ORDER BY "a" ASC OFFSET 2 ROWS FETCH FIRST 1 ROWS ONLY`,
		},
		// DISTINCT ON is still not supported.
		sqlTestCase{
			ds:  ds.Select("a").Distinct("a").Limit(1),
			err: "goqu: dialect does not support DISTINCT ON clause [dialect=sqlserver]",
		},
	)
}

func (sds *sqlserverDialectSuite) TestDeleteWithLimit() {
	ds := goqu.Dialect("sqlserver").Delete("test")
	sds.assertSQL(
		sqlTestCase{
			ds:  ds.Where(goqu.C("a").Gt(1)).Limit(10),
			sql: `DELETE TOP (10) FROM "test" WHERE ("a" > 1)`,
		},
		sqlTestCase{
			ds:  ds.Limit(10),
			sql: `DELETE TOP (10) FROM "test"`,
		},
		// Without a limit the statement is unchanged.
		sqlTestCase{
			ds:  ds.Where(goqu.C("a").Gt(1)),
			sql: `DELETE FROM "test" WHERE ("a" > 1)`,
		},
		sqlTestCase{
			ds:         ds.Where(goqu.C("a").Gt(1)).Limit(10).Prepared(true),
			sql:        `DELETE TOP (@p1) FROM "test" WHERE ("a" > @p2)`,
			isPrepared: true,
			args:       []interface{}{int64(10), int64(1)},
		},
	)
}

func (sds *sqlserverDialectSuite) TestUpdateWithLimit() {
	ds := goqu.Dialect("sqlserver").Update("test").Set(goqu.Record{"a": 1})
	sds.assertSQL(
		sqlTestCase{
			ds:  ds.Where(goqu.C("a").Gt(1)).Limit(10),
			sql: `UPDATE TOP (10) "test" SET "a"=1 WHERE ("a" > 1)`,
		},
		sqlTestCase{
			ds:  ds.Limit(10),
			sql: `UPDATE TOP (10) "test" SET "a"=1`,
		},
		// TOP has to come before the table, also when the update has a FROM clause.
		sqlTestCase{
			ds:  ds.From("other").Limit(10),
			sql: `UPDATE TOP (10) "test" SET "a"=1 FROM "other"`,
		},
		// Without a limit the statement is unchanged.
		sqlTestCase{
			ds:  ds.Where(goqu.C("a").Gt(1)),
			sql: `UPDATE "test" SET "a"=1 WHERE ("a" > 1)`,
		},
		sqlTestCase{
			ds:         ds.Where(goqu.C("a").Gt(1)).Limit(10).Prepared(true),
			sql:        `UPDATE TOP (@p1) "test" SET "a"=@p2 WHERE ("a" > @p3)`,
			isPrepared: true,
			args:       []interface{}{int64(10), int64(1), int64(1)},
		},
	)
}

func (sds *sqlserverDialectSuite) TestDeleteWithOrderIsAnError() {
	ds := goqu.Dialect("sqlserver").Delete("test")
	sds.assertSQL(
		sqlTestCase{
			ds:  ds.Order(goqu.C("a").Asc()),
			err: "goqu: dialect does not support ORDER BY on DELETE [dialect=sqlserver]",
		},
		sqlTestCase{
			ds:  ds.Order(goqu.C("a").Asc()).Limit(10),
			err: "goqu: dialect does not support ORDER BY on DELETE [dialect=sqlserver]",
		},
	)
}

func TestDatasetAdapterSuite(t *testing.T) {
	suite.Run(t, new(sqlserverDialectSuite))
}

func TestInsertOnConflictDoNothingTargetNotSupported(t *testing.T) {
	upd := goqu.Record{"b": 2}
	ds := goqu.Dialect("sqlserver").Insert("test").Rows(goqu.Record{"a": "a"})

	_, _, err := ds.OnConflict(goqu.DoNothing().SetCols(exp.NewColumnListExpression("a"))).ToSQL()
	assert.EqualError(t, err, "goqu: dialect does not support Conflict Target clause [dialect=sqlserver]")

	_, _, err = ds.OnConflict(goqu.DoUpdateSet(upd).TargetCols("a", "b")).ToSQL()
	assert.EqualError(t, err, "goqu: dialect does not support Conflict Target clause [dialect=sqlserver]")

	_, _, err = ds.OnConflict(goqu.DoNothing().OnConstraint("k")).ToSQL()
	assert.EqualError(t, err, "goqu: dialect does not support ON CONFLICT ON CONSTRAINT [dialect=sqlserver]")

	_, _, err = ds.OnConflict(goqu.DoUpdateSet(upd).OnConstraint("k")).ToSQL()
	assert.EqualError(t, err, "goqu: dialect does not support ON CONFLICT ON CONSTRAINT [dialect=sqlserver]")

	// no target is still fine
	_, _, err = ds.OnConflict(goqu.DoNothing()).ToSQL()
	assert.NoError(t, err)
}
