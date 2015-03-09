package sqlite3

import (
	"github.com/doug-martin/goqu"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"regexp"
	"testing"
)

type datasetAdapterTest struct {
	suite.Suite
}

func (me *datasetAdapterTest) TestPlaceholderSql() {
	t := me.T()
	buf := goqu.NewSqlBuilder(true)
	dsAdapter := newDatasetAdapter(goqu.From("test"))
	dsAdapter.PlaceHolderSql(buf, 1)
	dsAdapter.PlaceHolderSql(buf, 2)
	dsAdapter.PlaceHolderSql(buf, 3)
	dsAdapter.PlaceHolderSql(buf, 4)
	sql, args := buf.ToSql()
	assert.Equal(t, args, []interface{}{1, 2, 3, 4})
	assert.Equal(t, sql, "????")
}

func (me *datasetAdapterTest) GetDs(table string) *goqu.Dataset {
	ret := goqu.From(table)
	adapter := newDatasetAdapter(ret)
	ret.SetAdapter(adapter)
	return ret
}

func (me *datasetAdapterTest) TestSupportsReturn() {
	t := me.T()
	dsAdapter := me.GetDs("test").Adapter()
	assert.False(t, dsAdapter.SupportsReturn())
}

func (me *datasetAdapterTest) TestSupportsLimitOnDelete() {
	t := me.T()
	dsAdapter := me.GetDs("test").Adapter()
	assert.True(t, dsAdapter.SupportsLimitOnDelete())
}

func (me *datasetAdapterTest) TestSupportsLimitOnUpdate() {
	t := me.T()
	dsAdapter := me.GetDs("test").Adapter()
	assert.True(t, dsAdapter.SupportsLimitOnDelete())
}

func (me *datasetAdapterTest) TestSupportsOrderByOnDelete() {
	t := me.T()
	dsAdapter := me.GetDs("test").Adapter()
	assert.True(t, dsAdapter.SupportsLimitOnDelete())
}

func (me *datasetAdapterTest) TestSupportsOrderByOnUpdate() {
	t := me.T()
	dsAdapter := me.GetDs("test").Adapter()
	assert.True(t, dsAdapter.SupportsLimitOnDelete())
}

func (me *datasetAdapterTest) TestIdentifiers() {
	t := me.T()
	ds := me.GetDs("test")
	sql, err := ds.Select("a", goqu.I("a.b.c"), goqu.I("c.d"), goqu.I("test").As("test")).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT `a`, `a`.`b`.`c`, `c`.`d`, `test` AS `test` FROM `test`")
}

func (me *datasetAdapterTest) TestLiteralString() {
	t := me.T()
	ds := me.GetDs("test")
	sql, err := ds.Where(goqu.I("a").Eq("test")).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test')")

	sql, err = ds.Where(goqu.I("a").Eq("test'test")).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\'test')")

	sql, err = ds.Where(goqu.I("a").Eq(`test"test`)).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\\"test')")

	sql, err = ds.Where(goqu.I("a").Eq(`test\test`)).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\\\test')")

	sql, err = ds.Where(goqu.I("a").Eq("test\ntest")).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\ntest')")

	sql, err = ds.Where(goqu.I("a").Eq("test\rtest")).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\rtest')")

	sql, err = ds.Where(goqu.I("a").Eq("test\x00test")).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\x00test')")

	sql, err = ds.Where(goqu.I("a").Eq("test\x1atest")).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\x1atest')")
}

func (me *datasetAdapterTest) TestBooleanOperations() {
	t := me.T()
	ds := me.GetDs("test")
	sql, err := ds.Where(goqu.I("a").Eq(true)).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS TRUE)")
	sql, err = ds.Where(goqu.I("a").Eq(false)).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS FALSE)")
	sql, err = ds.Where(goqu.I("a").Is(true)).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS TRUE)")
	sql, err = ds.Where(goqu.I("a").Is(false)).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS FALSE)")
	sql, err = ds.Where(goqu.I("a").IsTrue()).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS TRUE)")
	sql, err = ds.Where(goqu.I("a").IsFalse()).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS FALSE)")

	sql, err = ds.Where(goqu.I("a").Neq(true)).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS NOT TRUE)")
	sql, err = ds.Where(goqu.I("a").Neq(false)).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS NOT FALSE)")
	sql, err = ds.Where(goqu.I("a").IsNot(true)).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS NOT TRUE)")
	sql, err = ds.Where(goqu.I("a").IsNot(false)).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS NOT FALSE)")
	sql, err = ds.Where(goqu.I("a").IsNotTrue()).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS NOT TRUE)")
	sql, err = ds.Where(goqu.I("a").IsNotFalse()).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS NOT FALSE)")

	sql, err = ds.Where(goqu.I("a").Like("a%")).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` LIKE BINARY 'a%')")

	sql, err = ds.Where(goqu.I("a").NotLike("a%")).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` NOT LIKE BINARY 'a%')")

	sql, err = ds.Where(goqu.I("a").ILike("a%")).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` LIKE 'a%')")
	sql, err = ds.Where(goqu.I("a").NotILike("a%")).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` NOT LIKE 'a%')")

	sql, err = ds.Where(goqu.I("a").Like(regexp.MustCompile("(a|b)"))).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` REGEXP BINARY '(a|b)')")
	sql, err = ds.Where(goqu.I("a").NotLike(regexp.MustCompile("(a|b)"))).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` NOT REGEXP BINARY '(a|b)')")
	sql, err = ds.Where(goqu.I("a").ILike(regexp.MustCompile("(a|b)"))).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` REGEXP '(a|b)')")
	sql, err = ds.Where(goqu.I("a").NotILike(regexp.MustCompile("(a|b)"))).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` NOT REGEXP '(a|b)')")

}

func TestDatasetAdapterSuite(t *testing.T) {
	suite.Run(t, new(datasetAdapterTest))
}
