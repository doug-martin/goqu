package mysql_test

import (
	"regexp"
	"testing"

	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type (
	mysqlDialectSuite struct {
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

func (mds *mysqlDialectSuite) GetDs(table string) *goqu.SelectDataset {
	return goqu.Dialect("mysql").From(table)
}

func (mds *mysqlDialectSuite) assertSQL(cases ...sqlTestCase) {
	for i, c := range cases {
		actualSQL, actualArgs, err := c.ds.ToSQL()
		if c.err == "" {
			mds.NoError(err, "test case %d failed", i)
		} else {
			mds.EqualError(err, c.err, "test case %d failed", i)
		}
		mds.Equal(c.sql, actualSQL, "test case %d failed", i)
		if c.isPrepared && c.args != nil || len(c.args) > 0 {
			mds.Equal(c.args, actualArgs, "test case %d failed", i)
		} else {
			mds.Empty(actualArgs, "test case %d failed", i)
		}
	}
}

func (mds *mysqlDialectSuite) TestIdentifiers() {
	ds := mds.GetDs("test")
	mds.assertSQL(
		sqlTestCase{ds: ds.Select(
			"a",
			goqu.I("a.b.c"),
			goqu.I("c.d"),
			goqu.C("test").As("test"),
		), sql: "SELECT `a`, `a`.`b`.`c`, `c`.`d`, `test` AS `test` FROM `test`"},
	)
}

func (mds *mysqlDialectSuite) TestLiteralString() {
	ds := mds.GetDs("test")
	col := goqu.C("a")
	mds.assertSQL(
		sqlTestCase{ds: ds.Where(col.Eq("test")), sql: "SELECT * FROM `test` WHERE (`a` = 'test')"},
		sqlTestCase{ds: ds.Where(col.Eq("test'test")), sql: "SELECT * FROM `test` WHERE (`a` = 'test\\'test')"},
		sqlTestCase{ds: ds.Where(col.Eq(`test"test`)), sql: "SELECT * FROM `test` WHERE (`a` = 'test\\\"test')"},
		sqlTestCase{ds: ds.Where(col.Eq(`test\test`)), sql: "SELECT * FROM `test` WHERE (`a` = 'test\\\\test')"},
		sqlTestCase{ds: ds.Where(col.Eq("test\ntest")), sql: "SELECT * FROM `test` WHERE (`a` = 'test\\ntest')"},
		sqlTestCase{ds: ds.Where(col.Eq("test\rtest")), sql: "SELECT * FROM `test` WHERE (`a` = 'test\\rtest')"},
		sqlTestCase{ds: ds.Where(col.Eq("test\x00test")), sql: "SELECT * FROM `test` WHERE (`a` = 'test\\x00test')"},
		sqlTestCase{ds: ds.Where(col.Eq("test\x1atest")), sql: "SELECT * FROM `test` WHERE (`a` = 'test\\x1atest')"},
	)
}

func (mds *mysqlDialectSuite) TestLiteralBytes() {
	col := goqu.C("a")
	ds := mds.GetDs("test")
	mds.assertSQL(
		sqlTestCase{ds: ds.Where(col.Eq([]byte("test"))), sql: "SELECT * FROM `test` WHERE (`a` = 'test')"},
		sqlTestCase{ds: ds.Where(col.Eq([]byte("test'test"))), sql: "SELECT * FROM `test` WHERE (`a` = 'test\\'test')"},
		sqlTestCase{ds: ds.Where(col.Eq([]byte(`test"test`))), sql: "SELECT * FROM `test` WHERE (`a` = 'test\\\"test')"},
		sqlTestCase{ds: ds.Where(col.Eq([]byte(`test\test`))), sql: "SELECT * FROM `test` WHERE (`a` = 'test\\\\test')"},
		sqlTestCase{ds: ds.Where(col.Eq([]byte("test\ntest"))), sql: "SELECT * FROM `test` WHERE (`a` = 'test\\ntest')"},
		sqlTestCase{ds: ds.Where(col.Eq([]byte("test\rtest"))), sql: "SELECT * FROM `test` WHERE (`a` = 'test\\rtest')"},
		sqlTestCase{ds: ds.Where(col.Eq([]byte("test\x00test"))), sql: "SELECT * FROM `test` WHERE (`a` = 'test\\x00test')"},
		sqlTestCase{ds: ds.Where(col.Eq([]byte("test\x1atest"))), sql: "SELECT * FROM `test` WHERE (`a` = 'test\\x1atest')"},
	)
}

func (mds *mysqlDialectSuite) TestBooleanOperations() {
	col := goqu.C("a")
	ds := mds.GetDs("test")
	mds.assertSQL(
		sqlTestCase{ds: ds.Where(col.Eq(true)), sql: "SELECT * FROM `test` WHERE (`a` IS TRUE)"},
		sqlTestCase{ds: ds.Where(col.Eq(false)), sql: "SELECT * FROM `test` WHERE (`a` IS FALSE)"},
		sqlTestCase{ds: ds.Where(col.Is(true)), sql: "SELECT * FROM `test` WHERE (`a` IS TRUE)"},
		sqlTestCase{ds: ds.Where(col.Is(false)), sql: "SELECT * FROM `test` WHERE (`a` IS FALSE)"},
		sqlTestCase{ds: ds.Where(col.IsTrue()), sql: "SELECT * FROM `test` WHERE (`a` IS TRUE)"},
		sqlTestCase{ds: ds.Where(col.IsFalse()), sql: "SELECT * FROM `test` WHERE (`a` IS FALSE)"},
		sqlTestCase{ds: ds.Where(col.Neq(true)), sql: "SELECT * FROM `test` WHERE (`a` IS NOT TRUE)"},
		sqlTestCase{ds: ds.Where(col.Neq(false)), sql: "SELECT * FROM `test` WHERE (`a` IS NOT FALSE)"},
		sqlTestCase{ds: ds.Where(col.IsNot(true)), sql: "SELECT * FROM `test` WHERE (`a` IS NOT TRUE)"},
		sqlTestCase{ds: ds.Where(col.IsNot(false)), sql: "SELECT * FROM `test` WHERE (`a` IS NOT FALSE)"},
		sqlTestCase{ds: ds.Where(col.IsNotTrue()), sql: "SELECT * FROM `test` WHERE (`a` IS NOT TRUE)"},
		sqlTestCase{ds: ds.Where(col.IsNotFalse()), sql: "SELECT * FROM `test` WHERE (`a` IS NOT FALSE)"},
		sqlTestCase{ds: ds.Where(col.Like("a%")), sql: "SELECT * FROM `test` WHERE (`a` LIKE BINARY 'a%')"},
		sqlTestCase{ds: ds.Where(col.NotLike("a%")), sql: "SELECT * FROM `test` WHERE (`a` NOT LIKE BINARY 'a%')"},
		sqlTestCase{ds: ds.Where(col.ILike("a%")), sql: "SELECT * FROM `test` WHERE (`a` LIKE 'a%')"},
		sqlTestCase{ds: ds.Where(col.NotILike("a%")), sql: "SELECT * FROM `test` WHERE (`a` NOT LIKE 'a%')"},
		sqlTestCase{ds: ds.Where(col.Like(regexp.MustCompile("[ab]"))), sql: "SELECT * FROM `test` WHERE (`a` REGEXP BINARY '[ab]')"},
		sqlTestCase{ds: ds.Where(col.NotLike(regexp.MustCompile("[ab]"))), sql: "SELECT * FROM `test` WHERE (`a` NOT REGEXP BINARY '[ab]')"},
		sqlTestCase{ds: ds.Where(col.ILike(regexp.MustCompile("[ab]"))), sql: "SELECT * FROM `test` WHERE (`a` REGEXP '[ab]')"},
		sqlTestCase{ds: ds.Where(col.NotILike(regexp.MustCompile("[ab]"))), sql: "SELECT * FROM `test` WHERE (`a` NOT REGEXP '[ab]')"},
	)
}

func (mds *mysqlDialectSuite) TestBitwiseOperations() {
	col := goqu.C("a")
	ds := mds.GetDs("test")
	mds.assertSQL(
		sqlTestCase{ds: ds.Where(col.BitwiseInversion()), sql: "SELECT * FROM `test` WHERE (~ `a`)"},
		sqlTestCase{ds: ds.Where(col.BitwiseAnd(1)), sql: "SELECT * FROM `test` WHERE (`a` & 1)"},
		sqlTestCase{ds: ds.Where(col.BitwiseOr(1)), sql: "SELECT * FROM `test` WHERE (`a` | 1)"},
		sqlTestCase{ds: ds.Where(col.BitwiseXor(1)), sql: "SELECT * FROM `test` WHERE (`a` ^ 1)"},
		sqlTestCase{ds: ds.Where(col.BitwiseLeftShift(1)), sql: "SELECT * FROM `test` WHERE (`a` << 1)"},
		sqlTestCase{ds: ds.Where(col.BitwiseRightShift(1)), sql: "SELECT * FROM `test` WHERE (`a` >> 1)"},
	)
}

func (mds *mysqlDialectSuite) TestUpdateSQL() {
	ds := mds.GetDs("test").Update()
	mds.assertSQL(
		sqlTestCase{
			ds: ds.
				Set(goqu.Record{"foo": "bar"}).
				From("test_2").
				Where(goqu.I("test.id").Eq(goqu.I("test_2.test_id"))),
			sql: "UPDATE `test`,`test_2` SET `foo`='bar' WHERE (`test`.`id` = `test_2`.`test_id`)",
		},
	)
}

func (mds *mysqlDialectSuite) TestStraightJoin() {
	on := goqu.On(goqu.I("test.id").Eq(goqu.I("foo.test_id")))
	for _, dialect := range []string{"mysql", "mysql8"} {
		ds := goqu.Dialect(dialect).From("test")
		mds.assertSQL(
			sqlTestCase{
				ds:  ds.StraightJoin(goqu.T("foo"), on),
				sql: "SELECT * FROM `test` STRAIGHT_JOIN `foo` ON (`test`.`id` = `foo`.`test_id`)",
			},
			sqlTestCase{
				ds:  ds.StraightJoin(goqu.T("foo"), goqu.Using("test_id")),
				sql: "SELECT * FROM `test` STRAIGHT_JOIN `foo` USING (`test_id`)",
			},
			sqlTestCase{
				ds:         ds.Prepared(true).StraightJoin(goqu.T("foo"), goqu.On(goqu.I("test.id").Eq(5))),
				sql:        "SELECT * FROM `test` STRAIGHT_JOIN `foo` ON (`test`.`id` = ?)",
				isPrepared: true,
				args:       []any{int64(5)},
			},
			sqlTestCase{
				ds:  ds.Join(goqu.T("a"), goqu.On(goqu.I("test.id").Eq(goqu.I("a.id")))).StraightJoin(goqu.T("b"), goqu.Using("id")),
				sql: "SELECT * FROM `test` INNER JOIN `a` ON (`test`.`id` = `a`.`id`) STRAIGHT_JOIN `b` USING (`id`)",
			},
			sqlTestCase{
				ds:  ds.StraightJoin(goqu.T("foo"), nil),
				err: "goqu: join condition required for conditioned join StraightJoinType",
			},
		)
	}
}

func (mds *mysqlDialectSuite) TestWithCTE() {
	cte := func(dialect string) *goqu.SelectDataset {
		return goqu.Dialect(dialect).From("a").Select("x")
	}
	// MySQL 8 supports common table expressions, plain mysql (5.x) does not.
	mds.assertSQL(
		sqlTestCase{
			ds:  goqu.Dialect("mysql8").From("c").With("c", cte("mysql8")),
			sql: "WITH c AS (SELECT `x` FROM `a`) SELECT * FROM `c`",
		},
		sqlTestCase{
			ds:  goqu.Dialect("mysql8").From("c").WithRecursive("c", cte("mysql8")),
			sql: "WITH RECURSIVE c AS (SELECT `x` FROM `a`) SELECT * FROM `c`",
		},
		sqlTestCase{
			ds:  goqu.Dialect("mysql").From("c").With("c", cte("mysql")),
			err: "goqu: dialect does not support CTE WITH clause [dialect=mysql]",
		},
	)
}

func TestDatasetAdapterSuite(t *testing.T) {
	suite.Run(t, new(mysqlDialectSuite))
}

func TestInsertOnConflictDoNothingTargetNotSupported(t *testing.T) {
	_, _, err := goqu.Dialect("mysql").Insert("test").
		Rows(goqu.Record{"a": "a"}).
		OnConflict(goqu.DoNothing().SetCols(exp.NewColumnListExpression("a"))).
		ToSQL()
	assert.EqualError(t, err, "goqu: dialect does not support Conflict Target clause [dialect=mysql]")

	// no target is still fine
	_, _, err = goqu.Dialect("mysql").Insert("test").
		Rows(goqu.Record{"a": "a"}).
		OnConflict(goqu.DoNothing()).
		ToSQL()
	assert.NoError(t, err)
}
