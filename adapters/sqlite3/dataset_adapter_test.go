package sqlite3

import (
	"regexp"
	"testing"

	"github.com/doug-martin/goqu/v6"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
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
	sql, _, err := ds.Select("a", goqu.I("a.b.c"), goqu.I("c.d"), goqu.I("test").As("test")).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT `a`, `a`.`b`.`c`, `c`.`d`, `test` AS `test` FROM `test`")
}

func (me *datasetAdapterTest) TestLiteralString() {
	t := me.T()
	ds := me.GetDs("test")
	sql, _, err := ds.Where(goqu.I("a").Eq("test")).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test')")

	sql, _, err = ds.Where(goqu.I("a").Eq("test'test")).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\'test')")

	sql, _, err = ds.Where(goqu.I("a").Eq(`test"test`)).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\\"test')")

	sql, _, err = ds.Where(goqu.I("a").Eq(`test\test`)).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\\\test')")

	sql, _, err = ds.Where(goqu.I("a").Eq("test\ntest")).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\ntest')")

	sql, _, err = ds.Where(goqu.I("a").Eq("test\rtest")).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\rtest')")

	sql, _, err = ds.Where(goqu.I("a").Eq("test\x00test")).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\x00test')")

	sql, _, err = ds.Where(goqu.I("a").Eq("test\x1atest")).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\x1atest')")
}

func (me *datasetAdapterTest) TestLiteralBytes() {
	t := me.T()
	ds := me.GetDs("test")
	sql, _, err := ds.Where(goqu.I("a").Eq([]byte("test"))).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test')")

	sql, _, err = ds.Where(goqu.I("a").Eq([]byte("test'test"))).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\'test')")

	sql, _, err = ds.Where(goqu.I("a").Eq([]byte(`test"test`))).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\\"test')")

	sql, _, err = ds.Where(goqu.I("a").Eq([]byte(`test\test`))).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\\\test')")

	sql, _, err = ds.Where(goqu.I("a").Eq([]byte("test\ntest"))).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\ntest')")

	sql, _, err = ds.Where(goqu.I("a").Eq([]byte("test\rtest"))).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\rtest')")

	sql, _, err = ds.Where(goqu.I("a").Eq([]byte("test\x00test"))).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\x00test')")

	sql, _, err = ds.Where(goqu.I("a").Eq([]byte("test\x1atest"))).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\x1atest')")
}

func (me *datasetAdapterTest) TestBooleanOperations() {
	t := me.T()
	ds := me.GetDs("test")
	sql, _, err := ds.Where(goqu.I("a").Eq(true)).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS 1)")
	sql, _, err = ds.Where(goqu.I("a").Eq(false)).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS 0)")
	sql, _, err = ds.Where(goqu.I("a").Is(true)).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS 1)")
	sql, _, err = ds.Where(goqu.I("a").Is(false)).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS 0)")
	sql, _, err = ds.Where(goqu.I("a").IsTrue()).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS 1)")
	sql, _, err = ds.Where(goqu.I("a").IsFalse()).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS 0)")

	sql, _, err = ds.Where(goqu.I("a").Neq(true)).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS NOT 1)")
	sql, _, err = ds.Where(goqu.I("a").Neq(false)).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS NOT 0)")
	sql, _, err = ds.Where(goqu.I("a").IsNot(true)).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS NOT 1)")
	sql, _, err = ds.Where(goqu.I("a").IsNot(false)).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS NOT 0)")
	sql, _, err = ds.Where(goqu.I("a").IsNotTrue()).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS NOT 1)")
	sql, _, err = ds.Where(goqu.I("a").IsNotFalse()).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS NOT 0)")

	sql, _, err = ds.Where(goqu.I("a").Like("a%")).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` LIKE 'a%')")

	sql, _, err = ds.Where(goqu.I("a").NotLike("a%")).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` NOT LIKE 'a%')")

	sql, _, err = ds.Where(goqu.I("a").ILike("a%")).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` LIKE 'a%')")
	sql, _, err = ds.Where(goqu.I("a").NotILike("a%")).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` NOT LIKE 'a%')")

	sql, _, err = ds.Where(goqu.I("a").Like(regexp.MustCompile("(a|b)"))).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` REGEXP '(a|b)')")
	sql, _, err = ds.Where(goqu.I("a").NotLike(regexp.MustCompile("(a|b)"))).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` NOT REGEXP '(a|b)')")
	sql, _, err = ds.Where(goqu.I("a").ILike(regexp.MustCompile("(a|b)"))).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` REGEXP '(a|b)')")
	sql, _, err = ds.Where(goqu.I("a").NotILike(regexp.MustCompile("(a|b)"))).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` NOT REGEXP '(a|b)')")

}

func TestDatasetAdapterSuite(t *testing.T) {
	suite.Run(t, new(datasetAdapterTest))
}
