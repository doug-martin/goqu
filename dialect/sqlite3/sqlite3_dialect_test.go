package sqlite3

import (
	"regexp"
	"testing"

	"github.com/doug-martin/goqu/v8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type sqlite3DialectSuite struct {
	suite.Suite
}

func (sds *sqlite3DialectSuite) GetDs(table string) *goqu.SelectDataset {
	return goqu.Dialect("sqlite3").From(table)
}

func (sds *sqlite3DialectSuite) TestIdentifiers() {
	t := sds.T()
	ds := sds.GetDs("test")
	sql, _, err := ds.Select(
		"a",
		goqu.I("a.b.c"),
		goqu.I("c.d"),
		goqu.C("test").As("test"),
	).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT `a`, `a`.`b`.`c`, `c`.`d`, `test` AS `test` FROM `test`")
}

func (sds *sqlite3DialectSuite) TestCompoundExpressions() {
	t := sds.T()
	ds1 := sds.GetDs("test").Select("a")
	ds2 := sds.GetDs("test2").Select("b")
	sql, _, err := ds1.Union(ds2).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT `a` FROM `test` UNION SELECT `b` FROM `test2`")

	sql, _, err = ds1.UnionAll(ds2).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT `a` FROM `test` UNION ALL SELECT `b` FROM `test2`")

	sql, _, err = ds1.Intersect(ds2).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT `a` FROM `test` INTERSECT SELECT `b` FROM `test2`")
}

func (sds *sqlite3DialectSuite) TestLiteralString() {
	t := sds.T()
	ds := sds.GetDs("test")
	sql, _, err := ds.Where(goqu.C("a").Eq("test")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test')")

	sql, _, err = ds.Where(goqu.C("a").Eq("test'test")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\'test')")

	sql, _, err = ds.Where(goqu.C("a").Eq(`test"test`)).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\\"test')")

	sql, _, err = ds.Where(goqu.C("a").Eq(`test\test`)).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\\\test')")

	sql, _, err = ds.Where(goqu.C("a").Eq("test\ntest")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\ntest')")

	sql, _, err = ds.Where(goqu.C("a").Eq("test\rtest")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\rtest')")

	sql, _, err = ds.Where(goqu.C("a").Eq("test\x00test")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\x00test')")

	sql, _, err = ds.Where(goqu.C("a").Eq("test\x1atest")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\x1atest')")
}

func (sds *sqlite3DialectSuite) TestLiteralBytes() {
	t := sds.T()
	ds := sds.GetDs("test")
	sql, _, err := ds.Where(goqu.C("a").Eq([]byte("test"))).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test')")

	sql, _, err = ds.Where(goqu.C("a").Eq([]byte("test'test"))).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\'test')")

	sql, _, err = ds.Where(goqu.C("a").Eq([]byte(`test"test`))).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\\"test')")

	sql, _, err = ds.Where(goqu.C("a").Eq([]byte(`test\test`))).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\\\test')")

	sql, _, err = ds.Where(goqu.C("a").Eq([]byte("test\ntest"))).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\ntest')")

	sql, _, err = ds.Where(goqu.C("a").Eq([]byte("test\rtest"))).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\rtest')")

	sql, _, err = ds.Where(goqu.C("a").Eq([]byte("test\x00test"))).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\x00test')")

	sql, _, err = ds.Where(goqu.C("a").Eq([]byte("test\x1atest"))).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` = 'test\\x1atest')")
}

func (sds *sqlite3DialectSuite) TestBooleanOperations() {
	t := sds.T()
	ds := sds.GetDs("test")
	sql, _, err := ds.Where(goqu.C("a").Eq(true)).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS 1)")
	sql, _, err = ds.Where(goqu.C("a").Eq(false)).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS 0)")
	sql, _, err = ds.Where(goqu.C("a").Is(true)).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS 1)")
	sql, _, err = ds.Where(goqu.C("a").Is(false)).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS 0)")
	sql, _, err = ds.Where(goqu.C("a").IsTrue()).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS 1)")
	sql, _, err = ds.Where(goqu.C("a").IsFalse()).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS 0)")

	sql, _, err = ds.Where(goqu.C("a").Neq(true)).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS NOT 1)")
	sql, _, err = ds.Where(goqu.C("a").Neq(false)).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS NOT 0)")
	sql, _, err = ds.Where(goqu.C("a").IsNot(true)).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS NOT 1)")
	sql, _, err = ds.Where(goqu.C("a").IsNot(false)).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS NOT 0)")
	sql, _, err = ds.Where(goqu.C("a").IsNotTrue()).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS NOT 1)")
	sql, _, err = ds.Where(goqu.C("a").IsNotFalse()).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` IS NOT 0)")

	sql, _, err = ds.Where(goqu.C("a").Like("a%")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` LIKE 'a%')")

	sql, _, err = ds.Where(goqu.C("a").NotLike("a%")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` NOT LIKE 'a%')")

	sql, _, err = ds.Where(goqu.C("a").ILike("a%")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` LIKE 'a%')")
	sql, _, err = ds.Where(goqu.C("a").NotILike("a%")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` NOT LIKE 'a%')")

	sql, _, err = ds.Where(goqu.C("a").Like(regexp.MustCompile("(a|b)"))).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` REGEXP '(a|b)')")
	sql, _, err = ds.Where(goqu.C("a").NotLike(regexp.MustCompile("(a|b)"))).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` NOT REGEXP '(a|b)')")
	sql, _, err = ds.Where(goqu.C("a").ILike(regexp.MustCompile("(a|b)"))).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` REGEXP '(a|b)')")
	sql, _, err = ds.Where(goqu.C("a").NotILike(regexp.MustCompile("(a|b)"))).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, sql, "SELECT * FROM `test` WHERE (`a` NOT REGEXP '(a|b)')")

}

func TestDatasetAdapterSuite(t *testing.T) {
	suite.Run(t, new(sqlite3DialectSuite))
}
