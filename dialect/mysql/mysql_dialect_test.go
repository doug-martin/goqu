package mysql

import (
	"regexp"
	"testing"

	"github.com/doug-martin/goqu/v9"
	"github.com/stretchr/testify/suite"
)

type mysqlDialectSuite struct {
	suite.Suite
}

func (mds *mysqlDialectSuite) GetDs(table string) *goqu.SelectDataset {
	return goqu.Dialect("mysql").From(table)
}

func (mds *mysqlDialectSuite) TestIdentifiers() {
	ds := mds.GetDs("test")
	sql, _, err := ds.Select("a",
		goqu.I("a.b.c"),
		goqu.I("c.d"),
		goqu.C("test").As("test"),
	).ToSQL()
	mds.NoError(err)
	mds.Equal("SELECT `a`, `a`.`b`.`c`, `c`.`d`, `test` AS `test` FROM `test`", sql)
}

func (mds *mysqlDialectSuite) TestLiteralString() {
	ds := mds.GetDs("test")
	col := goqu.C("a")
	sql, _, err := ds.Where(col.Eq("test")).ToSQL()
	mds.NoError(err)
	mds.Equal("SELECT * FROM `test` WHERE (`a` = 'test')", sql)

	sql, _, err = ds.Where(col.Eq("test'test")).ToSQL()
	mds.NoError(err)
	mds.Equal("SELECT * FROM `test` WHERE (`a` = 'test\\'test')", sql)

	sql, _, err = ds.Where(col.Eq(`test"test`)).ToSQL()
	mds.NoError(err)
	mds.Equal("SELECT * FROM `test` WHERE (`a` = 'test\\\"test')", sql)

	sql, _, err = ds.Where(col.Eq(`test\test`)).ToSQL()
	mds.NoError(err)
	mds.Equal("SELECT * FROM `test` WHERE (`a` = 'test\\\\test')", sql)

	sql, _, err = ds.Where(col.Eq("test\ntest")).ToSQL()
	mds.NoError(err)
	mds.Equal("SELECT * FROM `test` WHERE (`a` = 'test\\ntest')", sql)

	sql, _, err = ds.Where(col.Eq("test\rtest")).ToSQL()
	mds.NoError(err)
	mds.Equal("SELECT * FROM `test` WHERE (`a` = 'test\\rtest')", sql)

	sql, _, err = ds.Where(col.Eq("test\x00test")).ToSQL()
	mds.NoError(err)
	mds.Equal("SELECT * FROM `test` WHERE (`a` = 'test\\x00test')", sql)

	sql, _, err = ds.Where(col.Eq("test\x1atest")).ToSQL()
	mds.NoError(err)
	mds.Equal("SELECT * FROM `test` WHERE (`a` = 'test\\x1atest')", sql)
}

func (mds *mysqlDialectSuite) TestLiteralBytes() {
	col := goqu.C("a")
	ds := mds.GetDs("test")
	sql, _, err := ds.Where(col.Eq([]byte("test"))).ToSQL()
	mds.NoError(err)
	mds.Equal("SELECT * FROM `test` WHERE (`a` = 'test')", sql)

	sql, _, err = ds.Where(col.Eq([]byte("test'test"))).ToSQL()
	mds.NoError(err)
	mds.Equal("SELECT * FROM `test` WHERE (`a` = 'test\\'test')", sql)

	sql, _, err = ds.Where(col.Eq([]byte(`test"test`))).ToSQL()
	mds.NoError(err)
	mds.Equal("SELECT * FROM `test` WHERE (`a` = 'test\\\"test')", sql)

	sql, _, err = ds.Where(col.Eq([]byte(`test\test`))).ToSQL()
	mds.NoError(err)
	mds.Equal("SELECT * FROM `test` WHERE (`a` = 'test\\\\test')", sql)

	sql, _, err = ds.Where(col.Eq([]byte("test\ntest"))).ToSQL()
	mds.NoError(err)
	mds.Equal("SELECT * FROM `test` WHERE (`a` = 'test\\ntest')", sql)

	sql, _, err = ds.Where(col.Eq([]byte("test\rtest"))).ToSQL()
	mds.NoError(err)
	mds.Equal("SELECT * FROM `test` WHERE (`a` = 'test\\rtest')", sql)

	sql, _, err = ds.Where(col.Eq([]byte("test\x00test"))).ToSQL()
	mds.NoError(err)
	mds.Equal("SELECT * FROM `test` WHERE (`a` = 'test\\x00test')", sql)

	sql, _, err = ds.Where(col.Eq([]byte("test\x1atest"))).ToSQL()
	mds.NoError(err)
	mds.Equal("SELECT * FROM `test` WHERE (`a` = 'test\\x1atest')", sql)
}

func (mds *mysqlDialectSuite) TestBooleanOperations() {
	col := goqu.C("a")
	ds := mds.GetDs("test")
	sql, _, err := ds.Where(col.Eq(true)).ToSQL()
	mds.NoError(err)
	mds.Equal("SELECT * FROM `test` WHERE (`a` IS TRUE)", sql)
	sql, _, err = ds.Where(col.Eq(false)).ToSQL()
	mds.NoError(err)
	mds.Equal("SELECT * FROM `test` WHERE (`a` IS FALSE)", sql)
	sql, _, err = ds.Where(col.Is(true)).ToSQL()
	mds.NoError(err)
	mds.Equal("SELECT * FROM `test` WHERE (`a` IS TRUE)", sql)
	sql, _, err = ds.Where(col.Is(false)).ToSQL()
	mds.NoError(err)
	mds.Equal("SELECT * FROM `test` WHERE (`a` IS FALSE)", sql)
	sql, _, err = ds.Where(col.IsTrue()).ToSQL()
	mds.NoError(err)
	mds.Equal("SELECT * FROM `test` WHERE (`a` IS TRUE)", sql)
	sql, _, err = ds.Where(col.IsFalse()).ToSQL()
	mds.NoError(err)
	mds.Equal("SELECT * FROM `test` WHERE (`a` IS FALSE)", sql)

	sql, _, err = ds.Where(col.Neq(true)).ToSQL()
	mds.NoError(err)
	mds.Equal("SELECT * FROM `test` WHERE (`a` IS NOT TRUE)", sql)
	sql, _, err = ds.Where(col.Neq(false)).ToSQL()
	mds.NoError(err)
	mds.Equal("SELECT * FROM `test` WHERE (`a` IS NOT FALSE)", sql)
	sql, _, err = ds.Where(col.IsNot(true)).ToSQL()
	mds.NoError(err)
	mds.Equal("SELECT * FROM `test` WHERE (`a` IS NOT TRUE)", sql)
	sql, _, err = ds.Where(col.IsNot(false)).ToSQL()
	mds.NoError(err)
	mds.Equal("SELECT * FROM `test` WHERE (`a` IS NOT FALSE)", sql)
	sql, _, err = ds.Where(col.IsNotTrue()).ToSQL()
	mds.NoError(err)
	mds.Equal("SELECT * FROM `test` WHERE (`a` IS NOT TRUE)", sql)
	sql, _, err = ds.Where(col.IsNotFalse()).ToSQL()
	mds.NoError(err)
	mds.Equal("SELECT * FROM `test` WHERE (`a` IS NOT FALSE)", sql)

	sql, _, err = ds.Where(col.Like("a%")).ToSQL()
	mds.NoError(err)
	mds.Equal("SELECT * FROM `test` WHERE (`a` LIKE BINARY 'a%')", sql)

	sql, _, err = ds.Where(col.NotLike("a%")).ToSQL()
	mds.NoError(err)
	mds.Equal("SELECT * FROM `test` WHERE (`a` NOT LIKE BINARY 'a%')", sql)

	sql, _, err = ds.Where(col.ILike("a%")).ToSQL()
	mds.NoError(err)
	mds.Equal("SELECT * FROM `test` WHERE (`a` LIKE 'a%')", sql)
	sql, _, err = ds.Where(col.NotILike("a%")).ToSQL()
	mds.NoError(err)
	mds.Equal("SELECT * FROM `test` WHERE (`a` NOT LIKE 'a%')", sql)

	sql, _, err = ds.Where(col.Like(regexp.MustCompile("(a|b)"))).ToSQL()
	mds.NoError(err)
	mds.Equal("SELECT * FROM `test` WHERE (`a` REGEXP BINARY '(a|b)')", sql)
	sql, _, err = ds.Where(col.NotLike(regexp.MustCompile("(a|b)"))).ToSQL()
	mds.NoError(err)
	mds.Equal("SELECT * FROM `test` WHERE (`a` NOT REGEXP BINARY '(a|b)')", sql)
	sql, _, err = ds.Where(col.ILike(regexp.MustCompile("(a|b)"))).ToSQL()
	mds.NoError(err)
	mds.Equal("SELECT * FROM `test` WHERE (`a` REGEXP '(a|b)')", sql)
	sql, _, err = ds.Where(col.NotILike(regexp.MustCompile("(a|b)"))).ToSQL()
	mds.NoError(err)
	mds.Equal("SELECT * FROM `test` WHERE (`a` NOT REGEXP '(a|b)')", sql)
}

func (mds *mysqlDialectSuite) TestUpdateSQL() {
	ds := mds.GetDs("test").Update()
	sql, _, err := ds.
		Set(goqu.Record{"foo": "bar"}).
		From("test_2").
		Where(goqu.I("test.id").Eq(goqu.I("test_2.test_id"))).
		ToSQL()
	mds.NoError(err)
	mds.Equal("UPDATE `test`,`test_2` SET `foo`='bar' WHERE (`test`.`id` = `test_2`.`test_id`)", sql)
}

func TestDatasetAdapterSuite(t *testing.T) {
	suite.Run(t, new(mysqlDialectSuite))
}
