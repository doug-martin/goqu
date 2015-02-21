package gql

import (
	"database/sql/driver"
	"fmt"
	"github.com/c2fo/c2fo-go/lib/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"regexp"
	"testing"
	"time"
)

type datasetTest struct {
	suite.Suite
}

func (me *datasetTest) TestUnsupportedType() {
	t := me.T()
	_, err := From("test").Literal(struct{}{})
	assert.Error(t, err)
}

func (me *datasetTest) TestFloatTypes() {
	t := me.T()
	ds := From("test")
	val, err := ds.Literal(float32(10.01))
	assert.NoError(t, err)
	assert.Equal(t, val, "10.010000")

	val, err = ds.Literal(float64(10.01))
	assert.NoError(t, err)
	assert.Equal(t, val, "10.010000")
}

func (me *datasetTest) TestIntTypes() {
	t := me.T()
	ds := From("test")
	val, err := ds.Literal(int(10))
	assert.NoError(t, err)
	assert.Equal(t, val, "10")

	val, err = ds.Literal(int8(10))
	assert.NoError(t, err)
	assert.Equal(t, val, "10")

	val, err = ds.Literal(int16(10))
	assert.NoError(t, err)
	assert.Equal(t, val, "10")

	val, err = ds.Literal(int32(10))
	assert.NoError(t, err)
	assert.Equal(t, val, "10")

	val, err = ds.Literal(int64(10))
	assert.NoError(t, err)
	assert.Equal(t, val, "10")

	val, err = ds.Literal(uint(10))
	assert.NoError(t, err)
	assert.Equal(t, val, "10")

	val, err = ds.Literal(uint8(10))
	assert.NoError(t, err)
	assert.Equal(t, val, "10")

	val, err = ds.Literal(uint16(10))
	assert.NoError(t, err)
	assert.Equal(t, val, "10")

	val, err = ds.Literal(uint32(10))
	assert.NoError(t, err)
	assert.Equal(t, val, "10")

	val, err = ds.Literal(uint64(10))
	assert.NoError(t, err)
	assert.Equal(t, val, "10")
}

func (me *datasetTest) TestStringTypes() {
	t := me.T()
	ds := From("test")
	val, err := ds.Literal("Hello")
	assert.NoError(t, err)
	assert.Equal(t, val, "'Hello'")

	//should esacpe single quotes
	val, err = ds.Literal("hello'")
	assert.NoError(t, err)
	assert.Equal(t, val, "'hello'''")
}

func (me *datasetTest) TestBoolTypes() {
	t := me.T()
	ds := From("test")
	val, err := ds.Literal(true)
	assert.NoError(t, err)
	assert.Equal(t, val, "TRUE")

	val, err = ds.Literal(false)
	assert.NoError(t, err)
	assert.Equal(t, val, "FALSE")
}

func (me *datasetTest) TestTimeTypes() {
	t := me.T()
	ds := From("test")
	now := time.Now()
	val, err := ds.Literal(now)
	assert.NoError(t, err)
	assert.Equal(t, val, "'"+now.Format(time.RFC3339Nano)+"'")

	val, err = ds.Literal(&now)
	assert.NoError(t, err)
	assert.Equal(t, val, "'"+now.Format(time.RFC3339Nano)+"'")
}

func (me *datasetTest) TestNilTypes() {
	t := me.T()
	ds := From("test")
	val, err := ds.Literal(nil)
	assert.NoError(t, err)
	assert.Equal(t, val, "NULL")
}

func (me *datasetTest) TestNullBool() {
	t := me.T()
	ds := From("test")
	val, err := ds.Literal(*utils.NewNullBool(false, false))
	assert.NoError(t, err)
	assert.Equal(t, val, "NULL")

	val, err = ds.Literal(*utils.NewNullBool(false, true))
	assert.NoError(t, err)
	assert.Equal(t, val, "FALSE")

	val, err = ds.Literal(*utils.NewNullBool(true, false))
	assert.NoError(t, err)
	assert.Equal(t, val, "NULL")

	val, err = ds.Literal(*utils.NewNullBool(true, true))
	assert.NoError(t, err)
	assert.Equal(t, val, "TRUE")

	val, err = ds.Literal(utils.NewNullBool(false, false))
	assert.NoError(t, err)
	assert.Equal(t, val, "NULL")

	val, err = ds.Literal(utils.NewNullBool(false, true))
	assert.NoError(t, err)
	assert.Equal(t, val, "FALSE")

	val, err = ds.Literal(utils.NewNullBool(true, false))
	assert.NoError(t, err)
	assert.Equal(t, val, "NULL")

	val, err = ds.Literal(utils.NewNullBool(true, true))
	assert.NoError(t, err)
	assert.Equal(t, val, "TRUE")

}

func (me *datasetTest) TestNullFloat64() {
	t := me.T()
	ds := From("test")
	val, err := ds.Literal(*utils.NewNullFloat64(0, false))
	assert.NoError(t, err)
	assert.Equal(t, val, "NULL")

	val, err = ds.Literal(*utils.NewNullFloat64(10.01, true))
	assert.NoError(t, err)
	assert.Equal(t, val, "10.010000")

	val, err = ds.Literal(utils.NewNullFloat64(0, false))
	assert.NoError(t, err)
	assert.Equal(t, val, "NULL")

	val, err = ds.Literal(utils.NewNullFloat64(10.01, true))
	assert.NoError(t, err)
	assert.Equal(t, val, "10.010000")

}

func (me *datasetTest) TestNullInt64() {
	t := me.T()
	ds := From("test")
	val, err := ds.Literal(*utils.NewNullInt64(0, false))
	assert.NoError(t, err)
	assert.Equal(t, val, "NULL")

	val, err = ds.Literal(*utils.NewNullInt64(10, true))
	assert.NoError(t, err)
	assert.Equal(t, val, "10")

	val, err = ds.Literal(utils.NewNullInt64(0, false))
	assert.NoError(t, err)
	assert.Equal(t, val, "NULL")

	val, err = ds.Literal(utils.NewNullInt64(10, true))
	assert.NoError(t, err)
	assert.Equal(t, val, "10")

}

func (me *datasetTest) TestNullTime() {
	t := me.T()
	ds := From("test")
	now := time.Now()

	val, err := ds.Literal(*utils.NewNullTime(time.Time{}, false))
	assert.NoError(t, err)
	assert.Equal(t, val, "NULL")

	val, err = ds.Literal(*utils.NewNullTime(now, true))
	assert.NoError(t, err)
	assert.Equal(t, val, "'"+now.Format(time.RFC3339Nano)+"'")

	val, err = ds.Literal(utils.NewNullTime(time.Time{}, false))
	assert.NoError(t, err)
	assert.Equal(t, val, "NULL")

	val, err = ds.Literal(utils.NewNullTime(now, true))
	assert.NoError(t, err)
	assert.Equal(t, val, "'"+now.Format(time.RFC3339Nano)+"'")

}

func (me *datasetTest) TestNullString() {
	t := me.T()
	ds := From("test")

	val, err := ds.Literal(*utils.NewNullString("", false))
	assert.NoError(t, err)
	assert.Equal(t, val, "NULL")

	val, err = ds.Literal(*utils.NewNullString("hello", true))
	assert.NoError(t, err)
	assert.Equal(t, val, "'hello'")

	val, err = ds.Literal(*utils.NewNullString("'hello'", true))
	assert.NoError(t, err)
	assert.Equal(t, val, "'''hello'''")

	val, err = ds.Literal(utils.NewNullString("", false))
	assert.NoError(t, err)
	assert.Equal(t, val, "NULL")

	val, err = ds.Literal(utils.NewNullString("hello", true))
	assert.NoError(t, err)
	assert.Equal(t, val, "'hello'")

	val, err = ds.Literal(utils.NewNullString("'hello'", true))
	assert.NoError(t, err)
	assert.Equal(t, val, "'''hello'''")
}

func (me *datasetTest) TestSelect() {
	t := me.T()
	ds1 := From("test")

	sql, err := ds1.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test"`)

	ds2 := ds1.Select("id")
	sql, err = ds2.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "id" FROM "test"`)

	ds3 := ds1.Select("id", "name")
	sql, err = ds3.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "id", "name" FROM "test"`)

	ds4 := ds1.Select(Literal("COUNT(*)").As("count"))
	sql, err = ds4.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT COUNT(*) AS "count" FROM "test"`)

	ds5 := ds1.Select(
		I("id").As("other_id"),
		Literal("COUNT(*)").As("count"),
	)
	sql, err = ds5.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "id" AS "other_id", COUNT(*) AS "count" FROM "test"`)

	ds6 := ds1.Select(
		I("my_table").Col("id").As("other_id"),
		Literal("COUNT(*)").As("count"),
	)
	sql, err = ds6.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "my_table"."id" AS "other_id", COUNT(*) AS "count" FROM "test"`)

	ds7 := ds1.Select(
		I("private").Table("my_table").Col("id").As("other_id"),
		Literal("COUNT(*)").As("count"),
	)
	sql, err = ds7.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "private"."my_table"."id" AS "other_id", COUNT(*) AS "count" FROM "test"`)

	ds8 := ds1.From().Select(ds1.From("test_1").Select("id"))
	sql, err = ds8.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT (SELECT "id" FROM "test_1")`)

	ds9 := ds1.From().Select(ds1.From("test_1").Select("id").As("test_id"))
	sql, err = ds9.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT (SELECT "id" FROM "test_1") AS "test_id"`)

	//should not change original
	sql, err = ds1.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test"`)
}

func (me *datasetTest) TestDistinctSelect() {
	t := me.T()
	ds1 := From("test")

	sql, err := ds1.SelectDistinct("id").Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT DISTINCT "id" FROM "test"`)

	sql, err = ds1.SelectDistinct("id", "name").Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT DISTINCT "id", "name" FROM "test"`)

	sql, err = ds1.SelectDistinct(Literal("COUNT(*)").As("count")).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT DISTINCT COUNT(*) AS "count" FROM "test"`)

	sql, err = ds1.SelectDistinct(
		I("id").As("other_id"),
		Literal("COUNT(*)").As("count"),
	).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT DISTINCT "id" AS "other_id", COUNT(*) AS "count" FROM "test"`)

	sql, err = ds1.SelectDistinct(
		I("my_table").Col("id").As("other_id"),
		Literal("COUNT(*)").As("count"),
	).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT DISTINCT "my_table"."id" AS "other_id", COUNT(*) AS "count" FROM "test"`)

	sql, err = ds1.SelectDistinct(
		I("private").Table("my_table").Col("id").As("other_id"),
		Literal("COUNT(*)").As("count"),
	).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT DISTINCT "private"."my_table"."id" AS "other_id", COUNT(*) AS "count" FROM "test"`)

	sql, err = ds1.From().SelectDistinct(ds1.From("test_1").Select("id")).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT DISTINCT (SELECT "id" FROM "test_1")`)

	sql, err = ds1.From().SelectDistinct(ds1.From("test_1").Select("id").As("test_id")).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT DISTINCT (SELECT "id" FROM "test_1") AS "test_id"`)

	//should not change original
	sql, err = ds1.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test"`)
}

func (me *datasetTest) TestClearSelect() {
	t := me.T()
	ds1 := From("test")

	sql, err := ds1.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test"`)

	b := ds1.Select("a").ClearSelect()
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test"`)
}

func (me *datasetTest) TestSelectAppend() {
	t := me.T()
	ds1 := From("test")

	sql, err := ds1.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test"`)

	b := ds1.Select("a").SelectAppend("b").SelectAppend("c")
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "a", "b", "c" FROM "test"`)
}

func (me *datasetTest) TestFrom() {
	t := me.T()
	ds1 := From("test")

	sql, err := ds1.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test"`)

	ds2 := ds1.From("test2")
	sql, err = ds2.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test2"`)

	ds2 = ds1.From("test2", "test3")
	sql, err = ds2.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test2", "test3"`)

	ds2 = ds1.From(I("test2").As("test_2"), "test3")
	sql, err = ds2.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test2" AS "test_2", "test3"`)

	ds2 = ds1.From(ds1.From("test2"), "test3")
	sql, err = ds2.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM (SELECT * FROM "test2") AS "t1", "test3"`)

	ds2 = ds1.From(ds1.From("test2").As("test_2"), "test3")
	sql, err = ds2.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM (SELECT * FROM "test2") AS "test_2", "test3"`)

	//should not change original
	sql, err = ds1.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test"`)
}

func (me *datasetTest) TestAliasExpressionFrom() {
	t := me.T()
	ds1 := From("test")

	b := ds1.From(I("test2").As("test_2"), "test3")
	sql, err := b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test2" AS "test_2", "test3"`)

	//should not change original
	sql, err = ds1.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test"`)
}

func (me *datasetTest) TestSqlBuilderFrom() {
	t := me.T()
	ds1 := From("test")

	b := ds1.From(ds1.From("test2"), "test3")
	sql, err := b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM (SELECT * FROM "test2") AS "t1", "test3"`)

	b = ds1.From(ds1.From("test2").As("test_2"), "test3")
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM (SELECT * FROM "test2") AS "test_2", "test3"`)

	//should not change original
	sql, err = ds1.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test"`)
}

func (me *datasetTest) TestEmptyWhere() {
	t := me.T()
	ds1 := From("test")

	b := ds1.Where()
	sql, err := b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test"`)
}

func (me *datasetTest) TestEqualityExpressionWhere() {
	t := me.T()
	ds1 := From("test")

	b := ds1.Where(
		I("a").Eq(1),
	)
	sql, err := b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" = 1)`)

	b = ds1.Where(
		I("a").Neq(1),
	)
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" != 1)`)

	b = ds1.Where(
		I("a").Gt(1),
	)
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > 1)`)

	b = ds1.Where(
		I("a").Gte(1),
	)
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" >= 1)`)

	b = ds1.Where(
		I("a").Lt(1),
	)
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" < 1)`)

	b = ds1.Where(
		I("a").Lte(1),
	)
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" <= 1)`)

	b = ds1.Where(
		I("a").Eq(nil),
		I("a").Neq(nil),
	)
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE (("a" IS NULL) AND ("a" IS NOT NULL))`)

	b = ds1.Where(
		I("a").Eq(true),
		I("a").Neq(true),
		I("a").Eq(false),
		I("a").Neq(false),
	)
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE (("a" IS TRUE) AND ("a" IS NOT TRUE) AND ("a" IS FALSE) AND ("a" IS NOT FALSE))`)

	b = ds1.Where(
		I("a").Eq("a"),
		I("b").Neq("b"),
		I("c").Gt("c"),
		I("d").Gte("d"),
		I("e").Lt("e"),
		I("f").Lte("f"),
	)
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE (("a" = 'a') AND ("b" != 'b') AND ("c" > 'c') AND ("d" >= 'd') AND ("e" < 'e') AND ("f" <= 'f'))`)
}

func (me *datasetTest) TestExpressionListWhere() {
	t := me.T()
	ds1 := From("test")
	b := ds1.Where(
		Or(
			I("a").Eq("a"),
			I("b").Neq("b"),
			I("c").Gt("c"),
		),
		Or(
			I("d").Gte("d"),
			I("e").Lt("e"),
			I("f").Lte("f"),
		),
	)
	sql, err := b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ((("a" = 'a') OR ("b" != 'b') OR ("c" > 'c')) AND (("d" >= 'd') OR ("e" < 'e') OR ("f" <= 'f')))`)

	b = ds1.Where(
		Or(
			I("a").Eq("a"),
			And(
				I("b").Neq("b"),
				I("c").Gt("c"),
			),
		),
		Or(
			And(
				I("d").Gte("d"),
				I("e").Lt("e"),
			),
			I("f").Lte("f"),
		),
	)
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ((("a" = 'a') OR (("b" != 'b') AND ("c" > 'c'))) AND ((("d" >= 'd') AND ("e" < 'e')) OR ("f" <= 'f')))`)

	b = ds1.Where(
		Or(
			I("a").Eq(1),
			I("a").Eq(2),
		),
	)
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE (("a" = 1) OR ("a" = 2))`)
}

func (me *datasetTest) TestListExpressionWhere() {
	t := me.T()
	ds1 := From("test")
	b := ds1.Where(
		I("a").In("a", "b", "c", "d"),
	)
	sql, err := b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" IN ('a', 'b', 'c', 'd'))`)

	b = ds1.Where(
		I("a").Eq([]string{"a", "b", "c", "d"}),
	)
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" IN ('a', 'b', 'c', 'd'))`)

	b = ds1.Where(
		I("a").NotIn("a", "b", "c", "d"),
	)
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" NOT IN ('a', 'b', 'c', 'd'))`)

	b = ds1.Where(
		I("a").Neq([]string{"a", "b", "c", "d"}),
	)
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" NOT IN ('a', 'b', 'c', 'd'))`)
}

func (me *datasetTest) TestStringExpressionWhere() {
	t := me.T()
	ds1 := From("test")
	b := ds1.Where(
		I("a").Like("a%"),
	)
	sql, err := b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" LIKE 'a%')`)

	b = ds1.Where(
		I("a").NotLike("a%"),
	)
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" NOT LIKE 'a%')`)

	b = ds1.Where(
		I("a").ILike("a%"),
	)
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" ILIKE 'a%')`)

	b = ds1.Where(
		I("a").NotILike("a%"),
	)
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" NOT ILIKE 'a%')`)

	//with regexp
	b = ds1.Where(
		I("a").Like(regexp.MustCompile("(a|b)")),
	)
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" ~ '(a|b)')`)

	b = ds1.Where(
		I("a").NotLike(regexp.MustCompile("(a|b)")),
	)
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" !~ '(a|b)')`)

	b = ds1.Where(
		I("a").ILike(regexp.MustCompile("(a|b)")),
	)
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" ~* '(a|b)')`)

	b = ds1.Where(
		I("a").NotILike(regexp.MustCompile("(a|b)")),
	)
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" !~* '(a|b)')`)

	//with other valid operators
	b = ds1.Where(
		I("a").Eq(regexp.MustCompile("(a|b)")),
	)
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" ~ '(a|b)')`)

	b = ds1.Where(
		I("a").Neq(regexp.MustCompile("(a|b)")),
	)
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" !~ '(a|b)')`)

	b = ds1.Where(
		I("a").Is(regexp.MustCompile("(a|b)")),
	)
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" ~ '(a|b)')`)

	b = ds1.Where(
		I("a").IsNot(regexp.MustCompile("(a|b)")),
	)
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" !~ '(a|b)')`)
}

func (me *datasetTest) TestBooleanExpressionWhere() {
	t := me.T()
	ds1 := From("test")
	b := ds1.Where(
		I("a").Is(true),
	)
	sql, err := b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" IS TRUE)`)

	b = ds1.Where(
		I("a").Is(false),
	)
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" IS FALSE)`)

	b = ds1.Where(
		I("a").IsNot(true),
	)
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" IS NOT TRUE)`)

	b = ds1.Where(
		I("a").IsNot(false),
	)
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" IS NOT FALSE)`)

	b = ds1.Where(
		I("a").IsNull(),
	)
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" IS NULL)`)

	b = ds1.Where(
		I("a").IsNotNull(),
	)
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" IS NOT NULL)`)

	b = ds1.Where(
		I("a").IsTrue(),
	)
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" IS TRUE)`)

	b = ds1.Where(
		I("a").IsNotTrue(),
	)
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" IS NOT TRUE)`)

	b = ds1.Where(
		I("a").IsFalse(),
	)
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" IS FALSE)`)

	b = ds1.Where(
		I("a").IsNotFalse(),
	)
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" IS NOT FALSE)`)
}

func (me *datasetTest) TestClearWhere() {
	t := me.T()
	ds1 := From("test")

	b := ds1.Where(
		I("a").Eq(1),
	).ClearWhere()
	sql, err := b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test"`)
}

func (me *datasetTest) TestLimit() {
	t := me.T()
	ds1 := From("test")

	b := ds1.Where(
		I("a").Gt(1),
	).Limit(10)
	sql, err := b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > 1) LIMIT 10`)

	b = ds1.Where(
		I("a").Gt(1),
	).Limit(0)
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > 1)`)
}

func (me *datasetTest) TestLimitAll() {
	t := me.T()
	ds1 := From("test")

	b := ds1.Where(
		I("a").Gt(1),
	).LimitAll()
	sql, err := b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > 1) LIMIT ALL`)

	b = ds1.Where(
		I("a").Gt(1),
	).Limit(0).LimitAll()
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > 1) LIMIT ALL`)
}

func (me *datasetTest) TestClearLimit() {
	t := me.T()
	ds1 := From("test")

	b := ds1.Where(
		I("a").Gt(1),
	).LimitAll().ClearLimit()
	sql, err := b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > 1)`)

	b = ds1.Where(
		I("a").Gt(1),
	).Limit(10).ClearLimit()
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > 1)`)
}

func (me *datasetTest) TestOffset() {
	t := me.T()
	ds1 := From("test")

	b := ds1.Where(
		I("a").Gt(1),
	).Offset(10)
	sql, err := b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > 1) OFFSET 10`)

	b = ds1.Where(
		I("a").Gt(1),
	).Offset(0)
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > 1)`)
}

func (me *datasetTest) TestClearOffset() {
	t := me.T()
	ds1 := From("test")

	b := ds1.Where(
		I("a").Gt(1),
	).Offset(10).ClearOffset()
	sql, err := b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > 1)`)
}

func (me *datasetTest) TestGroupBy() {
	t := me.T()
	ds1 := From("test")

	b := ds1.Where(
		I("a").Gt(1),
	).GroupBy("created")
	sql, err := b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > 1) GROUP BY "created"`)

	b = ds1.Where(
		I("a").Gt(1),
	).GroupBy(Literal("created::DATE"))
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > 1) GROUP BY created::DATE`)

	b = ds1.Where(
		I("a").Gt(1),
	).GroupBy("name", Literal("created::DATE"))
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > 1) GROUP BY "name", created::DATE`)
}

func (me *datasetTest) TestHaving() {
	t := me.T()
	ds1 := From("test")

	b := ds1.Having(
		I("a").Gt(1),
	).GroupBy("created")
	sql, err := b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" GROUP BY "created" HAVING ("a" > 1)`)

	b = ds1.Where(
		I("b").IsTrue(),
	).Having(
		I("a").Gt(1),
	).GroupBy("created")
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("b" IS TRUE) GROUP BY "created" HAVING ("a" > 1)`)

	b = ds1.Having(
		I("a").Gt(1),
	)
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" HAVING ("a" > 1)`)
}

func (me *datasetTest) TestOrder() {
	t := me.T()

	ds1 := From("test")

	b := ds1.Order(I("a").Asc())
	sql, err := b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" ORDER BY "a" ASC`)

	b = ds1.Order(Literal(`("a" + "b" > 2)`).Asc())
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" ORDER BY ("a" + "b" > 2) ASC`)

	b = ds1.Order(I("a").Asc(), I("b").Desc())
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" ORDER BY "a" ASC, "b" DESC`)

	b = ds1.Order(I("a").Asc().NullsFirst(), I("b").Desc().NullsLast())
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" ORDER BY "a" ASC NULLS FIRST, "b" DESC NULLS LAST`)

	b = ds1.Order(I("a").Asc().NullsLast(), I("b").Desc().NullsFirst())
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" ORDER BY "a" ASC NULLS LAST, "b" DESC NULLS FIRST`)
}

func (me *datasetTest) TestOrderAppend() {
	t := me.T()
	b := From("test").Order(I("a").Asc().NullsFirst()).OrderAppend(I("b").Desc().NullsLast())
	sql, err := b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" ORDER BY "a" ASC NULLS FIRST, "b" DESC NULLS LAST`)

}

func (me *datasetTest) TestClearOrder() {
	t := me.T()
	b := From("test").Order(I("a").Asc().NullsFirst()).ClearOrder()
	sql, err := b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test"`)
}

func (me *datasetTest) TestJoin() {
	t := me.T()
	ds1 := From("items")

	b := ds1.LeftOuterJoin(
		I("categories"),
		On(I("categories.categoryId").Eq(I("items.id"))),
	)
	sql, err := b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" LEFT OUTER JOIN "categories" ON ("categories"."categoryId" = "items"."id")`)

	b = ds1.LeftOuterJoin(
		I("categories"),
		On(
			I("categories.categoryId").Eq(I("items.id")),
			I("categories.categoryId").In(1, 2, 3),
		),
	)
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" LEFT OUTER JOIN "categories" ON (("categories"."categoryId" = "items"."id") AND ("categories"."categoryId" IN (1, 2, 3)))`)

	b = ds1.Where(
		I("price").Lt(100),
	).RightOuterJoin(
		I("categories"),
		On(I("categories.categoryId").Eq(I("items.id"))),
	)
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" RIGHT OUTER JOIN "categories" ON ("categories"."categoryId" = "items"."id") WHERE ("price" < 100)`)

	b = ds1.FullOuterJoin(
		I("categories"),
		On(I("categories.categoryId").Eq(I("items.id"))),
	).Order(I("stamp").Asc())
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" FULL OUTER JOIN "categories" ON ("categories"."categoryId" = "items"."id") ORDER BY "stamp" ASC`)

	b = ds1.InnerJoin(
		I("b"),
		On(I("b.itemsId").Eq(I("items.id"))),
	).LeftOuterJoin(
		I("c"),
		On(I("c.b_id").Eq(I("b.id"))),
	)
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" INNER JOIN "b" ON ("b"."itemsId" = "items"."id") LEFT OUTER JOIN "c" ON ("c"."b_id" = "b"."id")`)

	b = ds1.InnerJoin(
		I("b"),
		On(I("b.itemsId").Eq(I("items.id"))),
	).LeftOuterJoin(
		I("c"),
		On(I("c.b_id").Eq(I("b.id"))),
	)
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" INNER JOIN "b" ON ("b"."itemsId" = "items"."id") LEFT OUTER JOIN "c" ON ("c"."b_id" = "b"."id")`)

	b = ds1.LeftOuterJoin(I("categories"), On(I("categories.categoryId").Eq(I("items.id"))))
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" LEFT OUTER JOIN "categories" ON ("categories"."categoryId" = "items"."id")`)

	b = ds1.RightOuterJoin(I("categories"), On(I("categories.categoryId").Eq(I("items.id"))))
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" RIGHT OUTER JOIN "categories" ON ("categories"."categoryId" = "items"."id")`)

	b = ds1.FullOuterJoin(I("categories"), On(I("categories.categoryId").Eq(I("items.id"))))
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" FULL OUTER JOIN "categories" ON ("categories"."categoryId" = "items"."id")`)

	b = ds1.InnerJoin(I("categories"), On(I("categories.categoryId").Eq(I("items.id"))))
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" INNER JOIN "categories" ON ("categories"."categoryId" = "items"."id")`)

	b = ds1.LeftJoin(I("categories"), On(I("categories.categoryId").Eq(I("items.id"))))
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" LEFT JOIN "categories" ON ("categories"."categoryId" = "items"."id")`)

	b = ds1.RightJoin(I("categories"), On(I("categories.categoryId").Eq(I("items.id"))))
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" RIGHT JOIN "categories" ON ("categories"."categoryId" = "items"."id")`)

	b = ds1.FullJoin(I("categories"), On(I("categories.categoryId").Eq(I("items.id"))))
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" FULL JOIN "categories" ON ("categories"."categoryId" = "items"."id")`)

	b = ds1.NaturalJoin(I("categories"))
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" NATURAL JOIN "categories"`)

	b = ds1.NaturalLeftJoin(I("categories"))
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" NATURAL LEFT JOIN "categories"`)

	b = ds1.NaturalRightJoin(I("categories"))
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" NATURAL RIGHT JOIN "categories"`)

	b = ds1.NaturalFullJoin(I("categories"))
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" NATURAL FULL JOIN "categories"`)

	b = ds1.CrossJoin(I("categories"))
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" CROSS JOIN "categories"`)

	b = ds1.Join(I("players").As("p"), On(I("p.id").Eq(I("items.playerId"))))
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" INNER JOIN "players" AS "p" ON ("p"."id" = "items"."playerId")`)

	b = ds1.Join(ds1.From("players").As("p"), On(I("p.id").Eq(I("items.playerId"))))
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" INNER JOIN (SELECT * FROM "players") AS "p" ON ("p"."id" = "items"."playerId")`)

	b = ds1.Join(I("v1").Table("test"), On(I("v1.test.id").Eq(I("items.playerId"))))
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" INNER JOIN "v1"."test" ON ("v1"."test"."id" = "items"."playerId")`)

	b = ds1.Join(I("test"), Using(I("name"), I("common_id")))
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" INNER JOIN "test" USING ("name", "common_id")`)

	b = ds1.Join(I("test"), Using("name", "common_id"))
	sql, err = b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" INNER JOIN "test" USING ("name", "common_id")`)

}

func (me *datasetTest) TestInsertSqlWithStructs() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, err := ds1.InsertSql(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test')`)

	sql, err = ds1.InsertSql(
		item{Address: "111 Test Addr", Name: "Test1"},
		item{Address: "211 Test Addr", Name: "Test2"},
		item{Address: "311 Test Addr", Name: "Test3"},
		item{Address: "411 Test Addr", Name: "Test4"},
	)
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('211 Test Addr', 'Test2'), ('311 Test Addr', 'Test3'), ('411 Test Addr', 'Test4')`)
}

func (me *datasetTest) TestInsertSqlWithMaps() {
	t := me.T()
	ds1 := From("items")

	sql, err := ds1.InsertSql(map[string]interface{}{"name": "Test", "address": "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test')`)

	sql, err = ds1.InsertSql(
		map[string]interface{}{"address": "111 Test Addr", "name": "Test1"},
		map[string]interface{}{"address": "211 Test Addr", "name": "Test2"},
		map[string]interface{}{"address": "311 Test Addr", "name": "Test3"},
		map[string]interface{}{"address": "411 Test Addr", "name": "Test4"},
	)
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('211 Test Addr', 'Test2'), ('311 Test Addr', 'Test3'), ('411 Test Addr', 'Test4')`)

	_, err = ds1.InsertSql(
		map[string]interface{}{"address": "111 Test Addr", "name": "Test1"},
		map[string]interface{}{"address": "211 Test Addr"},
		map[string]interface{}{"address": "311 Test Addr", "name": "Test3"},
		map[string]interface{}{"address": "411 Test Addr", "name": "Test4"},
	)
	assert.EqualError(t, err, "gql: Rows with different value length expected 2 got 1")
}

func (me *datasetTest) TestInsertSqlWitSqlBuilder() {
	t := me.T()
	ds1 := From("items")

	sql, err := ds1.InsertSql(From("other_items"))
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" SELECT * FROM "other_items"`)
}

func (me *datasetTest) TestInsertReturning() {
	t := me.T()
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	ds1 := From("items").Returning("id")

	sql, err := ds1.Returning("id").InsertSql(From("other_items"))
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" SELECT * FROM "other_items" RETURNING "id"`)

	sql, err = ds1.InsertSql(map[string]interface{}{"name": "Test", "address": "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test') RETURNING "id"`)

	sql, err = ds1.InsertSql(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test') RETURNING "id"`)
}

func (me *datasetTest) TestInsertSqlWithNoFrom() {
	t := me.T()
	ds1 := From("test").From()
	_, err := ds1.InsertSql(map[string]interface{}{"address": "111 Test Addr", "name": "Test1"})
	assert.EqualError(t, err, "gql: No source found when generating insert sql")
}

func (me *datasetTest) TestInsertSqlWithMapsWithDifferentLengths() {
	t := me.T()
	ds1 := From("items")
	_, err := ds1.InsertSql(
		map[string]interface{}{"address": "111 Test Addr", "name": "Test1"},
		map[string]interface{}{"address": "211 Test Addr"},
		map[string]interface{}{"address": "311 Test Addr", "name": "Test3"},
		map[string]interface{}{"address": "411 Test Addr", "name": "Test4"},
	)
	assert.EqualError(t, err, "gql: Rows with different value length expected 2 got 1")
}

func (me *datasetTest) TestInsertSqlWitDifferentKeys() {
	t := me.T()
	ds1 := From("items")
	_, err := ds1.InsertSql(
		map[string]interface{}{"address": "111 Test Addr", "name": "test"},
		map[string]interface{}{"phoneNumber": 10, "address": "111 Test Addr"},
	)
	assert.EqualError(t, err, `gql: Rows with different keys expected ["address","name"] got ["address","phoneNumber"]`)
}

func (me *datasetTest) TestInsertSqlDifferentTypes() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	type item2 struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	_, err := ds1.InsertSql(
		item{Address: "111 Test Addr", Name: "Test1"},
		item2{Address: "211 Test Addr", Name: "Test2"},
		item{Address: "311 Test Addr", Name: "Test3"},
		item2{Address: "411 Test Addr", Name: "Test4"},
	)
	assert.EqualError(t, err, "gql: Rows must be all the same type expected gql.item got gql.item2")

	_, err = ds1.InsertSql(
		item{Address: "111 Test Addr", Name: "Test1"},
		map[string]interface{}{"address": "211 Test Addr", "name": "Test2"},
		item{Address: "311 Test Addr", Name: "Test3"},
		map[string]interface{}{"address": "411 Test Addr", "name": "Test4"},
	)
	assert.EqualError(t, err, "gql: Rows must be all the same type expected gql.item got map[string]interface {}")
}

func (me *datasetTest) TestInsertWithGqlPkTagSql() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Id      uint32 `db:"id" gql:"pk,skipinsert"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, err := ds1.InsertSql(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test')`)

	sql, err = ds1.InsertSql(map[string]interface{}{"name": "Test", "address": "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test')`)

	sql, err = ds1.InsertSql(
		item{Name: "Test1", Address: "111 Test Addr"},
		item{Name: "Test2", Address: "211 Test Addr"},
		item{Name: "Test3", Address: "311 Test Addr"},
		item{Name: "Test4", Address: "411 Test Addr"},
	)
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('211 Test Addr', 'Test2'), ('311 Test Addr', 'Test3'), ('411 Test Addr', 'Test4')`)
}

func (me *datasetTest) TestInsertWithGqlSkipInsertTagSql() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Id      uint32 `db:"id" gql:"skipinsert"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, err := ds1.InsertSql(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test')`)

	sql, err = ds1.InsertSql(
		item{Name: "Test1", Address: "111 Test Addr"},
		item{Name: "Test2", Address: "211 Test Addr"},
		item{Name: "Test3", Address: "311 Test Addr"},
		item{Name: "Test4", Address: "411 Test Addr"},
	)
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('211 Test Addr', 'Test2'), ('311 Test Addr', 'Test3'), ('411 Test Addr', 'Test4')`)
}

func (me *datasetTest) TestInsertDefaultValues() {
	t := me.T()
	ds1 := From("items")

	sql, err := ds1.InsertSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" DEFAULT VALUES`)

	sql, err = ds1.InsertSql(map[string]interface{}{"name": Default(), "address": Default()})
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES (DEFAULT, DEFAULT)`)

}

func (me *datasetTest) TestUpdateSqlWithStructs() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, err := ds1.UpdateSql(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, sql, `UPDATE "items" SET "address"='111 Test Addr',"name"='Test'`)
}

func (me *datasetTest) TestUpdateSqlWithMaps() {
	t := me.T()
	ds1 := From("items")
	sql, err := ds1.UpdateSql(map[string]interface{}{"name": "Test", "address": "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, sql, `UPDATE "items" SET "address"='111 Test Addr',"name"='Test'`)

}

func (me *datasetTest) TestUpdateSqlWithByteSlice() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Name string `db:"name"`
		Data []byte `db:"data"`
	}
	sql, err := ds1.Returning(I("items").All()).UpdateSql(item{Name: "Test", Data: []byte(`{"someJson":"data"}`)})
	assert.NoError(t, err)
	assert.Equal(t, sql, `UPDATE "items" SET "name"='Test',"data"='{"someJson":"data"}' RETURNING "items".*`)
}

type valuerType []byte

func (j valuerType) Value() (driver.Value, error) {
	return []byte(fmt.Sprintf("%s World", string(j))), nil
}

func (me *datasetTest) TestUpdateSqlWithValuer() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Name string     `db:"name"`
		Data valuerType `db:"data"`
	}
	sql, err := ds1.Returning(I("items").All()).UpdateSql(item{Name: "Test", Data: []byte(`Hello`)})
	assert.NoError(t, err)
	assert.Equal(t, sql, `UPDATE "items" SET "name"='Test',"data"='Hello World' RETURNING "items".*`)
}

func (me *datasetTest) TestUpdateSqlWithUnsupportedType() {
	t := me.T()
	ds1 := From("items")
	_, err := ds1.UpdateSql([]string{"HELLO"})
	assert.EqualError(t, err, "gql: Unsupported update interface type []string")
}

func (me *datasetTest) TestUpdateSqlWithSkipupdateTag() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Address string `db:"address" gql:"skipupdate"`
		Name    string `db:"name"`
	}
	sql, err := ds1.UpdateSql(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, sql, `UPDATE "items" SET "name"='Test'`)
}

func (me *datasetTest) TestUpdateSqlWithWhere() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, err := ds1.Where(I("name").IsNull()).UpdateSql(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, sql, `UPDATE "items" SET "address"='111 Test Addr',"name"='Test' WHERE ("name" IS NULL)`)

	sql, err = ds1.Where(I("name").IsNull()).UpdateSql(map[string]interface{}{"name": "Test", "address": "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, sql, `UPDATE "items" SET "address"='111 Test Addr',"name"='Test' WHERE ("name" IS NULL)`)
}

func (me *datasetTest) TestUpdateSqlWithReturning() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, err := ds1.Returning(I("items").All()).UpdateSql(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, sql, `UPDATE "items" SET "address"='111 Test Addr',"name"='Test' RETURNING "items".*`)

	sql, err = ds1.Where(I("name").IsNull()).Returning(Literal(`"items".*`)).UpdateSql(map[string]interface{}{"name": "Test", "address": "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, sql, `UPDATE "items" SET "address"='111 Test Addr',"name"='Test' WHERE ("name" IS NULL) RETURNING "items".*`)
}

func (me *datasetTest) TestDeleteSql() {
	t := me.T()
	ds1 := From("items")
	sql, err := ds1.DeleteSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `DELETE FROM "items"`)
}

func (me *datasetTest) TestDeleteSqlNoSurces() {
	t := me.T()
	ds1 := From("items")
	_, err := ds1.From().DeleteSql()
	assert.EqualError(t, err, "gql: No source found when generating delete sql")
}

func (me *datasetTest) TestDeleteSqlWithWhere() {
	t := me.T()
	ds1 := From("items")
	sql, err := ds1.Where(I("id").IsNotNull()).DeleteSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `DELETE FROM "items" WHERE ("id" IS NOT NULL)`)
}

func (me *datasetTest) TestDeleteSqlWithReturning() {
	t := me.T()
	ds1 := From("items")
	sql, err := ds1.Returning("id").DeleteSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `DELETE FROM "items" RETURNING "id"`)

	sql, err = ds1.Returning("id").Where(I("id").IsNotNull()).DeleteSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `DELETE FROM "items" WHERE ("id" IS NOT NULL) RETURNING "id"`)
}

func (me *datasetTest) TestTruncateSql() {
	t := me.T()
	ds1 := From("items")
	sql, err := ds1.TruncateSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `TRUNCATE "items"`)
}

func (me *datasetTest) TestTruncateSqlNoSources() {
	t := me.T()
	ds1 := From("items")
	_, err := ds1.From().TruncateSql()
	assert.EqualError(t, err, "gql: No source found when generating truncate sql")
}

func (me *datasetTest) TestTruncateSqlWithOpts() {
	t := me.T()
	ds1 := From("items")
	sql, err := ds1.TruncateWithOptsSql(TruncateOptions{Cascade: true})
	assert.NoError(t, err)
	assert.Equal(t, sql, `TRUNCATE "items" CASCADE`)

	sql, err = ds1.TruncateWithOptsSql(TruncateOptions{Restrict: true})
	assert.NoError(t, err)
	assert.Equal(t, sql, `TRUNCATE "items" RESTRICT`)

	sql, err = ds1.TruncateWithOptsSql(TruncateOptions{Identity: "restart"})
	assert.NoError(t, err)
	assert.Equal(t, sql, `TRUNCATE "items" RESTART IDENTITY`)

	sql, err = ds1.TruncateWithOptsSql(TruncateOptions{Identity: "continue"})
	assert.NoError(t, err)
	assert.Equal(t, sql, `TRUNCATE "items" CONTINUE IDENTITY`)

}

func (me *datasetTest) TestSqlFunctionExpressions() {
	t := me.T()
	ds1 := From("items")
	sql, err := ds1.Select(Func("COUNT", Star())).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT COUNT(*) FROM "items"`)

	sql, err = ds1.Select(COUNT(Star())).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT COUNT(*) FROM "items"`)

	sql, err = ds1.Select(MIN("id")).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT MIN("id") FROM "items"`)

	sql, err = ds1.Select(MAX("id")).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT MAX("id") FROM "items"`)

	sql, err = ds1.Select(AVG("id")).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT AVG("id") FROM "items"`)

	sql, err = ds1.Select(FIRST("id")).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT FIRST("id") FROM "items"`)

	sql, err = ds1.Select(LAST("id")).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT LAST("id") FROM "items"`)

	sql, err = ds1.Select(SUM("amount")).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT SUM("amount") FROM "items"`)

	sql, err = ds1.Select(COALESCE(I("amount"), 0)).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT COALESCE("amount", 0) FROM "items"`)

	sql, err = ds1.Select(DISTINCT("name")).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT DISTINCT("name") FROM "items"`)

	sql, err = ds1.Select(I("name").Distinct()).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT DISTINCT("name") FROM "items"`)

	sql, err = ds1.Select(COUNT(I("name").Distinct())).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT COUNT(DISTINCT("name")) FROM "items"`)
}

func (me *datasetTest) TestSqlFunctionExpressionsWithAliases() {
	t := me.T()
	ds1 := From("items")
	sql, err := ds1.Select(Func("COUNT", Star()).As("count")).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT COUNT(*) AS "count" FROM "items"`)
}

func (me *datasetTest) TestSqlFunctionExpressionsInHaving() {
	t := me.T()
	ds1 := From("items")
	sql, err := ds1.GroupBy("name").Having(SUM("amount").Gt(0)).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" GROUP BY "name" HAVING (SUM("amount") > 0)`)
}

func (me *datasetTest) TestCastExpressions() {
	t := me.T()
	ds1 := From("items")
	sql, err := ds1.Select(I("a").Cast("TEXT").As("a_text")).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT CAST("a" AS TEXT) AS "a_text" FROM "items"`)
}

func (me *datasetTest) TestUnion() {
	t := me.T()
	a := From("invoice").Select("id", "amount").Where(I("amount").Gt(1000))
	b := From("invoice").Select("id", "amount").Where(I("amount").Lt(10))

	sql, err := a.Union(b).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) UNION (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	sql, err = a.Limit(1).Union(b).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) LIMIT 1) AS "t1" UNION (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	sql, err = a.Order(I("id").Asc()).Union(b).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) ORDER BY "id" ASC) AS "t1" UNION (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	sql, err = a.Union(b.Limit(1)).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) UNION (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) LIMIT 1) AS "t1")`)

	sql, err = a.Union(b.Order(I("id").Desc())).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) UNION (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) ORDER BY "id" DESC) AS "t1")`)

	sql, err = a.Limit(1).Union(b.Order(I("id").Desc())).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) LIMIT 1) AS "t1" UNION (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) ORDER BY "id" DESC) AS "t1")`)

	sql, err = a.Union(b).Union(b.Where(I("id").Lt(50))).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) UNION (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10)) UNION (SELECT "id", "amount" FROM "invoice" WHERE (("amount" < 10) AND ("id" < 50)))`)

}

func (me *datasetTest) TestUnionAll() {
	t := me.T()
	a := From("invoice").Select("id", "amount").Where(I("amount").Gt(1000))
	b := From("invoice").Select("id", "amount").Where(I("amount").Lt(10))

	sql, err := a.UnionAll(b).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) UNION ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	sql, err = a.Limit(1).UnionAll(b).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) LIMIT 1) AS "t1" UNION ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	sql, err = a.Order(I("id").Asc()).UnionAll(b).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) ORDER BY "id" ASC) AS "t1" UNION ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	sql, err = a.UnionAll(b.Limit(1)).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) UNION ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) LIMIT 1) AS "t1")`)

	sql, err = a.UnionAll(b.Order(I("id").Desc())).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) UNION ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) ORDER BY "id" DESC) AS "t1")`)

	sql, err = a.Limit(1).UnionAll(b.Order(I("id").Desc())).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) LIMIT 1) AS "t1" UNION ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) ORDER BY "id" DESC) AS "t1")`)
}

func (me *datasetTest) TestIntersect() {
	t := me.T()
	a := From("invoice").Select("id", "amount").Where(I("amount").Gt(1000))
	b := From("invoice").Select("id", "amount").Where(I("amount").Lt(10))

	sql, err := a.Intersect(b).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) INTERSECT (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	sql, err = a.Limit(1).Intersect(b).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) LIMIT 1) AS "t1" INTERSECT (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	sql, err = a.Order(I("id").Asc()).Intersect(b).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) ORDER BY "id" ASC) AS "t1" INTERSECT (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	sql, err = a.Intersect(b.Limit(1)).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) INTERSECT (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) LIMIT 1) AS "t1")`)

	sql, err = a.Intersect(b.Order(I("id").Desc())).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) INTERSECT (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) ORDER BY "id" DESC) AS "t1")`)

	sql, err = a.Limit(1).Intersect(b.Order(I("id").Desc())).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) LIMIT 1) AS "t1" INTERSECT (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) ORDER BY "id" DESC) AS "t1")`)
}

func (me *datasetTest) TestIntersectAll() {
	t := me.T()
	a := From("invoice").Select("id", "amount").Where(I("amount").Gt(1000))
	b := From("invoice").Select("id", "amount").Where(I("amount").Lt(10))

	sql, err := a.IntersectAll(b).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) INTERSECT ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	sql, err = a.Limit(1).IntersectAll(b).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) LIMIT 1) AS "t1" INTERSECT ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	sql, err = a.Order(I("id").Asc()).IntersectAll(b).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) ORDER BY "id" ASC) AS "t1" INTERSECT ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	sql, err = a.IntersectAll(b.Limit(1)).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) INTERSECT ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) LIMIT 1) AS "t1")`)

	sql, err = a.IntersectAll(b.Order(I("id").Desc())).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) INTERSECT ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) ORDER BY "id" DESC) AS "t1")`)

	sql, err = a.Limit(1).IntersectAll(b.Order(I("id").Desc())).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) LIMIT 1) AS "t1" INTERSECT ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) ORDER BY "id" DESC) AS "t1")`)
}

func TestDatasetSuite(t *testing.T) {
	suite.Run(t, new(datasetTest))
}
