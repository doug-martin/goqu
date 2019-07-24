package mysql

import (
	"regexp"
	"testing"

	"github.com/doug-martin/goqu/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type mysqlDialectSuite struct {
	suite.Suite
}

func (mds *mysqlDialectSuite) GetDs(table string) *goqu.SelectDataset {
	return goqu.Dialect("mysql").From(table)
}

func (mds *mysqlDialectSuite) TestIdentifiers() {
	t := mds.T()
	ds := mds.GetDs("test")
	sql, _, err := ds.Select("a",
		goqu.I("a.b.c"),
		goqu.I("c.d"),
		goqu.C("test").As("test"),
	).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT `a`, `a`.`b`.`c`, `c`.`d`, `test` AS `test` FROM `test`")
}

func (mds *mysqlDialectSuite) TestLiteralString() {
	t := mds.T()
	ds := mds.GetDs("test")
	col := goqu.C("a")
	sql, _, err := ds.Where(col.Eq("test")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test')")

	sql, _, err = ds.Where(col.Eq("test'test")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\'test')")

	sql, _, err = ds.Where(col.Eq(`test"test`)).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\\"test')")

	sql, _, err = ds.Where(col.Eq(`test\test`)).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\\\test')")

	sql, _, err = ds.Where(col.Eq("test\ntest")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\ntest')")

	sql, _, err = ds.Where(col.Eq("test\rtest")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\rtest')")

	sql, _, err = ds.Where(col.Eq("test\x00test")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\x00test')")

	sql, _, err = ds.Where(col.Eq("test\x1atest")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\x1atest')")
}

func (mds *mysqlDialectSuite) TestLiteralBytes() {
	t := mds.T()
	col := goqu.C("a")
	ds := mds.GetDs("test")
	sql, _, err := ds.Where(col.Eq([]byte("test"))).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test')")

	sql, _, err = ds.Where(col.Eq([]byte("test'test"))).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\'test')")

	sql, _, err = ds.Where(col.Eq([]byte(`test"test`))).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\\"test')")

	sql, _, err = ds.Where(col.Eq([]byte(`test\test`))).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\\\test')")

	sql, _, err = ds.Where(col.Eq([]byte("test\ntest"))).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\ntest')")

	sql, _, err = ds.Where(col.Eq([]byte("test\rtest"))).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\rtest')")

	sql, _, err = ds.Where(col.Eq([]byte("test\x00test"))).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\x00test')")

	sql, _, err = ds.Where(col.Eq([]byte("test\x1atest"))).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\x1atest')")
}

func (mds *mysqlDialectSuite) TestBooleanOperations() {
	t := mds.T()
	col := goqu.C("a")
	ds := mds.GetDs("test")
	sql, _, err := ds.Where(col.Eq(true)).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS TRUE)")
	sql, _, err = ds.Where(col.Eq(false)).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS FALSE)")
	sql, _, err = ds.Where(col.Is(true)).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS TRUE)")
	sql, _, err = ds.Where(col.Is(false)).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS FALSE)")
	sql, _, err = ds.Where(col.IsTrue()).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS TRUE)")
	sql, _, err = ds.Where(col.IsFalse()).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS FALSE)")

	sql, _, err = ds.Where(col.Neq(true)).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS NOT TRUE)")
	sql, _, err = ds.Where(col.Neq(false)).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS NOT FALSE)")
	sql, _, err = ds.Where(col.IsNot(true)).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS NOT TRUE)")
	sql, _, err = ds.Where(col.IsNot(false)).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS NOT FALSE)")
	sql, _, err = ds.Where(col.IsNotTrue()).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS NOT TRUE)")
	sql, _, err = ds.Where(col.IsNotFalse()).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS NOT FALSE)")

	sql, _, err = ds.Where(col.Like("a%")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` LIKE BINARY 'a%')")

	sql, _, err = ds.Where(col.NotLike("a%")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` NOT LIKE BINARY 'a%')")

	sql, _, err = ds.Where(col.ILike("a%")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` LIKE 'a%')")
	sql, _, err = ds.Where(col.NotILike("a%")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` NOT LIKE 'a%')")

	sql, _, err = ds.Where(col.Like(regexp.MustCompile("(a|b)"))).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` REGEXP BINARY '(a|b)')")
	sql, _, err = ds.Where(col.NotLike(regexp.MustCompile("(a|b)"))).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` NOT REGEXP BINARY '(a|b)')")
	sql, _, err = ds.Where(col.ILike(regexp.MustCompile("(a|b)"))).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` REGEXP '(a|b)')")
	sql, _, err = ds.Where(col.NotILike(regexp.MustCompile("(a|b)"))).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` NOT REGEXP '(a|b)')")

}

func TestDatasetAdapterSuite(t *testing.T) {
	suite.Run(t, new(mysqlDialectSuite))
}
