package sqlite3

import (
	"regexp"
	"testing"

	"github.com/doug-martin/goqu/v9"

	"github.com/stretchr/testify/suite"
)

type sqlite3DialectSuite struct {
	suite.Suite
}

func (sds *sqlite3DialectSuite) GetDs(table string) *goqu.SelectDataset {
	return goqu.Dialect("sqlite3").From(table)
}

func (sds *sqlite3DialectSuite) TestIdentifiers() {
	ds := sds.GetDs("test")
	sql, _, err := ds.Select(
		"a",
		goqu.I("a.b.c"),
		goqu.I("c.d"),
		goqu.C("test").As("test"),
	).ToSQL()
	sds.NoError(err)
	sds.Equal("SELECT `a`, `a`.`b`.`c`, `c`.`d`, `test` AS `test` FROM `test`", sql)
}

func (sds *sqlite3DialectSuite) TestUpdateSQL_multipleTables() {
	ds := sds.GetDs("test").Update()
	_, _, err := ds.
		Set(goqu.Record{"foo": "bar"}).
		From("test_2").
		Where(goqu.I("test.id").Eq(goqu.I("test_2.test_id"))).
		ToSQL()
	sds.EqualError(err, "goqu: sqlite3 dialect does not support multiple tables in UPDATE")
}

func (sds *sqlite3DialectSuite) TestCompoundExpressions() {
	ds1 := sds.GetDs("test").Select("a")
	ds2 := sds.GetDs("test2").Select("b")
	sql, _, err := ds1.Union(ds2).ToSQL()
	sds.NoError(err)
	sds.Equal("SELECT `a` FROM `test` UNION SELECT `b` FROM `test2`", sql)

	sql, _, err = ds1.UnionAll(ds2).ToSQL()
	sds.NoError(err)
	sds.Equal("SELECT `a` FROM `test` UNION ALL SELECT `b` FROM `test2`", sql)

	sql, _, err = ds1.Intersect(ds2).ToSQL()
	sds.NoError(err)
	sds.Equal("SELECT `a` FROM `test` INTERSECT SELECT `b` FROM `test2`", sql)
}

func (sds *sqlite3DialectSuite) TestLiteralString() {
	ds := sds.GetDs("test")
	sql, _, err := ds.Where(goqu.C("a").Eq("test")).ToSQL()
	sds.NoError(err)
	sds.Equal("SELECT * FROM `test` WHERE (`a` = 'test')", sql)

	sql, _, err = ds.Where(goqu.C("a").Eq("test'test")).ToSQL()
	sds.NoError(err)
	sds.Equal("SELECT * FROM `test` WHERE (`a` = 'test\\'test')", sql)

	sql, _, err = ds.Where(goqu.C("a").Eq(`test"test`)).ToSQL()
	sds.NoError(err)
	sds.Equal("SELECT * FROM `test` WHERE (`a` = 'test\\\"test')", sql)

	sql, _, err = ds.Where(goqu.C("a").Eq(`test\test`)).ToSQL()
	sds.NoError(err)
	sds.Equal("SELECT * FROM `test` WHERE (`a` = 'test\\\\test')", sql)

	sql, _, err = ds.Where(goqu.C("a").Eq("test\ntest")).ToSQL()
	sds.NoError(err)
	sds.Equal("SELECT * FROM `test` WHERE (`a` = 'test\\ntest')", sql)

	sql, _, err = ds.Where(goqu.C("a").Eq("test\rtest")).ToSQL()
	sds.NoError(err)
	sds.Equal("SELECT * FROM `test` WHERE (`a` = 'test\\rtest')", sql)

	sql, _, err = ds.Where(goqu.C("a").Eq("test\x00test")).ToSQL()
	sds.NoError(err)
	sds.Equal("SELECT * FROM `test` WHERE (`a` = 'test\\x00test')", sql)

	sql, _, err = ds.Where(goqu.C("a").Eq("test\x1atest")).ToSQL()
	sds.NoError(err)
	sds.Equal("SELECT * FROM `test` WHERE (`a` = 'test\\x1atest')", sql)
}

func (sds *sqlite3DialectSuite) TestLiteralBytes() {
	ds := sds.GetDs("test")
	sql, _, err := ds.Where(goqu.C("a").Eq([]byte("test"))).ToSQL()
	sds.NoError(err)
	sds.Equal("SELECT * FROM `test` WHERE (`a` = 'test')", sql)

	sql, _, err = ds.Where(goqu.C("a").Eq([]byte("test'test"))).ToSQL()
	sds.NoError(err)
	sds.Equal("SELECT * FROM `test` WHERE (`a` = 'test\\'test')", sql)

	sql, _, err = ds.Where(goqu.C("a").Eq([]byte(`test"test`))).ToSQL()
	sds.NoError(err)
	sds.Equal("SELECT * FROM `test` WHERE (`a` = 'test\\\"test')", sql)

	sql, _, err = ds.Where(goqu.C("a").Eq([]byte(`test\test`))).ToSQL()
	sds.NoError(err)
	sds.Equal("SELECT * FROM `test` WHERE (`a` = 'test\\\\test')", sql)

	sql, _, err = ds.Where(goqu.C("a").Eq([]byte("test\ntest"))).ToSQL()
	sds.NoError(err)
	sds.Equal("SELECT * FROM `test` WHERE (`a` = 'test\\ntest')", sql)

	sql, _, err = ds.Where(goqu.C("a").Eq([]byte("test\rtest"))).ToSQL()
	sds.NoError(err)
	sds.Equal("SELECT * FROM `test` WHERE (`a` = 'test\\rtest')", sql)

	sql, _, err = ds.Where(goqu.C("a").Eq([]byte("test\x00test"))).ToSQL()
	sds.NoError(err)
	sds.Equal("SELECT * FROM `test` WHERE (`a` = 'test\\x00test')", sql)

	sql, _, err = ds.Where(goqu.C("a").Eq([]byte("test\x1atest"))).ToSQL()
	sds.NoError(err)
	sds.Equal("SELECT * FROM `test` WHERE (`a` = 'test\\x1atest')", sql)
}

func (sds *sqlite3DialectSuite) TestBooleanOperations() {
	ds := sds.GetDs("test")
	sql, _, err := ds.Where(goqu.C("a").Eq(true)).ToSQL()
	sds.NoError(err)
	sds.Equal("SELECT * FROM `test` WHERE (`a` IS 1)", sql)
	sql, _, err = ds.Where(goqu.C("a").Eq(false)).ToSQL()
	sds.NoError(err)
	sds.Equal("SELECT * FROM `test` WHERE (`a` IS 0)", sql)
	sql, _, err = ds.Where(goqu.C("a").Is(true)).ToSQL()
	sds.NoError(err)
	sds.Equal("SELECT * FROM `test` WHERE (`a` IS 1)", sql)
	sql, _, err = ds.Where(goqu.C("a").Is(false)).ToSQL()
	sds.NoError(err)
	sds.Equal("SELECT * FROM `test` WHERE (`a` IS 0)", sql)
	sql, _, err = ds.Where(goqu.C("a").IsTrue()).ToSQL()
	sds.NoError(err)
	sds.Equal("SELECT * FROM `test` WHERE (`a` IS 1)", sql)
	sql, _, err = ds.Where(goqu.C("a").IsFalse()).ToSQL()
	sds.NoError(err)
	sds.Equal("SELECT * FROM `test` WHERE (`a` IS 0)", sql)

	sql, _, err = ds.Where(goqu.C("a").Neq(true)).ToSQL()
	sds.NoError(err)
	sds.Equal("SELECT * FROM `test` WHERE (`a` IS NOT 1)", sql)
	sql, _, err = ds.Where(goqu.C("a").Neq(false)).ToSQL()
	sds.NoError(err)
	sds.Equal("SELECT * FROM `test` WHERE (`a` IS NOT 0)", sql)
	sql, _, err = ds.Where(goqu.C("a").IsNot(true)).ToSQL()
	sds.NoError(err)
	sds.Equal("SELECT * FROM `test` WHERE (`a` IS NOT 1)", sql)
	sql, _, err = ds.Where(goqu.C("a").IsNot(false)).ToSQL()
	sds.NoError(err)
	sds.Equal("SELECT * FROM `test` WHERE (`a` IS NOT 0)", sql)
	sql, _, err = ds.Where(goqu.C("a").IsNotTrue()).ToSQL()
	sds.NoError(err)
	sds.Equal("SELECT * FROM `test` WHERE (`a` IS NOT 1)", sql)
	sql, _, err = ds.Where(goqu.C("a").IsNotFalse()).ToSQL()
	sds.NoError(err)
	sds.Equal("SELECT * FROM `test` WHERE (`a` IS NOT 0)", sql)

	sql, _, err = ds.Where(goqu.C("a").Like("a%")).ToSQL()
	sds.NoError(err)
	sds.Equal("SELECT * FROM `test` WHERE (`a` LIKE 'a%')", sql)

	sql, _, err = ds.Where(goqu.C("a").NotLike("a%")).ToSQL()
	sds.NoError(err)
	sds.Equal("SELECT * FROM `test` WHERE (`a` NOT LIKE 'a%')", sql)

	sql, _, err = ds.Where(goqu.C("a").ILike("a%")).ToSQL()
	sds.NoError(err)
	sds.Equal("SELECT * FROM `test` WHERE (`a` LIKE 'a%')", sql)
	sql, _, err = ds.Where(goqu.C("a").NotILike("a%")).ToSQL()
	sds.NoError(err)
	sds.Equal("SELECT * FROM `test` WHERE (`a` NOT LIKE 'a%')", sql)

	sql, _, err = ds.Where(goqu.C("a").Like(regexp.MustCompile("(a|b)"))).ToSQL()
	sds.NoError(err)
	sds.Equal("SELECT * FROM `test` WHERE (`a` REGEXP '(a|b)')", sql)
	sql, _, err = ds.Where(goqu.C("a").NotLike(regexp.MustCompile("(a|b)"))).ToSQL()
	sds.NoError(err)
	sds.Equal("SELECT * FROM `test` WHERE (`a` NOT REGEXP '(a|b)')", sql)
	sql, _, err = ds.Where(goqu.C("a").ILike(regexp.MustCompile("(a|b)"))).ToSQL()
	sds.NoError(err)
	sds.Equal("SELECT * FROM `test` WHERE (`a` REGEXP '(a|b)')", sql)
	sql, _, err = ds.Where(goqu.C("a").NotILike(regexp.MustCompile("(a|b)"))).ToSQL()
	sds.NoError(err)
	sds.Equal("SELECT * FROM `test` WHERE (`a` NOT REGEXP '(a|b)')", sql)

}

func TestDatasetAdapterSuite(t *testing.T) {
	suite.Run(t, new(sqlite3DialectSuite))
}
