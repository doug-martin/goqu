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

func (me *datasetTest) TestLiteralUnsupportedType() {
	t := me.T()
	_, err := From("test").Literal(struct{}{})
	assert.Error(t, err)
}

func (me *datasetTest) TestLiteralFloatTypes() {
	t := me.T()
	ds := From("test")
	val, err := ds.Literal(float32(10.01))
	assert.NoError(t, err)
	assert.Equal(t, val, "10.010000")

	val, err = ds.Literal(float64(10.01))
	assert.NoError(t, err)
	assert.Equal(t, val, "10.010000")
}

func (me *datasetTest) TestLiteralIntTypes() {
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

func (me *datasetTest) TestLiteralStringTypes() {
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

func (me *datasetTest) TestLiteralBoolTypes() {
	t := me.T()
	ds := From("test")
	val, err := ds.Literal(true)
	assert.NoError(t, err)
	assert.Equal(t, val, "TRUE")

	val, err = ds.Literal(false)
	assert.NoError(t, err)
	assert.Equal(t, val, "FALSE")
}

func (me *datasetTest) TestLiteralTimeTypes() {
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

func (me *datasetTest) TestLiteralNilTypes() {
	t := me.T()
	ds := From("test")
	val, err := ds.Literal(nil)
	assert.NoError(t, err)
	assert.Equal(t, val, "NULL")
}

func (me *datasetTest) TestLiteralNullBool() {
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

type datasetValuerType int64

func (j datasetValuerType) Value() (driver.Value, error) {
	return []byte(fmt.Sprintf("Hello World %d", j)), nil
}

func (me *datasetTest) TestLiteralValuer() {
	t := me.T()
	ds := From("test")

	val, err := ds.Literal(datasetValuerType(10))
	assert.NoError(t, err)
	assert.Equal(t, val, "'Hello World 10'")

}

func (me *datasetTest) TestLiteraSlice() {
	t := me.T()
	ds := From("test")
	lit, err := ds.Literal([]string{"a", "b", "c"})
	assert.NoError(t, err)
	assert.Equal(t, lit, `('a', 'b', 'c')`)
}

func (me *datasetTest) TestLiteralDataset() {
	t := me.T()
	ds := From("test")
	lit, err := ds.Literal(From("a"))
	assert.NoError(t, err)
	assert.Equal(t, lit, `(SELECT * FROM "a")`)

	lit, err = ds.Literal(From("a").As("b"))
	assert.NoError(t, err)
	assert.Equal(t, lit, `(SELECT * FROM "a") AS "b"`)
}

func (me *datasetTest) TestLiteralColumnList() {
	t := me.T()
	ds := From("test")
	lit, err := ds.Literal(cols("a", Literal("true")))
	assert.NoError(t, err)
	assert.Equal(t, lit, `"a", true`)
}

func (me *datasetTest) TestLiteralExpressionList() {
	t := me.T()
	ds := From("test")
	lit, err := ds.Literal(And(I("a").Eq("b"), I("c").Neq(1)))
	assert.NoError(t, err)
	assert.Equal(t, lit, `(("a" = 'b') AND ("c" != 1))`)

	lit, err = ds.Literal(Or(I("a").Eq("b"), I("c").Neq(1)))
	assert.NoError(t, err)
	assert.Equal(t, lit, `(("a" = 'b') OR ("c" != 1))`)

	lit, err = ds.Literal(Or(I("a").Eq("b"), And(I("c").Neq(1), I("d").Eq(Literal("NOW()")))))
	assert.NoError(t, err)
	assert.Equal(t, lit, `(("a" = 'b') OR (("c" != 1) AND ("d" = NOW())))`)
}

func (me *datasetTest) TestLiteralLiteralExpression() {
	t := me.T()
	ds := From("test")
	lit, err := ds.Literal(Literal(`"b"::DATE = '2010-09-02'`))
	assert.NoError(t, err)
	assert.Equal(t, lit, `"b"::DATE = '2010-09-02'`)
}

func (me *datasetTest) TestLiteralAliasedExpression() {
	t := me.T()
	ds := From("test")
	lit, err := ds.Literal(I("a").As("b"))
	assert.NoError(t, err)
	assert.Equal(t, lit, `"a" AS "b"`)

	lit, err = ds.Literal(Literal("count(*)").As("count"))
	assert.NoError(t, err)
	assert.Equal(t, lit, `count(*) AS "count"`)
}

func (me *datasetTest) TestBooleanExpression() {
	t := me.T()
	ds := From("test")
	lit, err := ds.Literal(I("a").Eq(1))
	assert.NoError(t, err)
	assert.Equal(t, lit, `("a" = 1)`)

	lit, err = ds.Literal(I("a").Eq(true))
	assert.NoError(t, err)
	assert.Equal(t, lit, `("a" IS TRUE)`)

	lit, err = ds.Literal(I("a").Eq(false))
	assert.NoError(t, err)
	assert.Equal(t, lit, `("a" IS FALSE)`)

	lit, err = ds.Literal(I("a").Eq(nil))
	assert.NoError(t, err)
	assert.Equal(t, lit, `("a" IS NULL)`)

	lit, err = ds.Literal(I("a").Eq([]int64{1, 2, 3}))
	assert.NoError(t, err)
	assert.Equal(t, lit, `("a" IN (1, 2, 3))`)

	lit, err = ds.Literal(I("a").Neq(1))
	assert.NoError(t, err)
	assert.Equal(t, lit, `("a" != 1)`)

	lit, err = ds.Literal(I("a").Neq(true))
	assert.NoError(t, err)
	assert.Equal(t, lit, `("a" IS NOT TRUE)`)

	lit, err = ds.Literal(I("a").Neq(false))
	assert.NoError(t, err)
	assert.Equal(t, lit, `("a" IS NOT FALSE)`)

	lit, err = ds.Literal(I("a").Neq(nil))
	assert.NoError(t, err)
	assert.Equal(t, lit, `("a" IS NOT NULL)`)

	lit, err = ds.Literal(I("a").Neq([]int64{1, 2, 3}))
	assert.NoError(t, err)
	assert.Equal(t, lit, `("a" NOT IN (1, 2, 3))`)

	lit, err = ds.Literal(I("a").Is(nil))
	assert.NoError(t, err)
	assert.Equal(t, lit, `("a" IS NULL)`)

	lit, err = ds.Literal(I("a").Is(false))
	assert.NoError(t, err)
	assert.Equal(t, lit, `("a" IS FALSE)`)

	lit, err = ds.Literal(I("a").Is(true))
	assert.NoError(t, err)
	assert.Equal(t, lit, `("a" IS TRUE)`)

	lit, err = ds.Literal(I("a").IsNot(nil))
	assert.NoError(t, err)
	assert.Equal(t, lit, `("a" IS NOT NULL)`)

	lit, err = ds.Literal(I("a").IsNot(false))
	assert.NoError(t, err)
	assert.Equal(t, lit, `("a" IS NOT FALSE)`)

	lit, err = ds.Literal(I("a").IsNot(true))
	assert.NoError(t, err)
	assert.Equal(t, lit, `("a" IS NOT TRUE)`)

	lit, err = ds.Literal(I("a").Gt(1))
	assert.NoError(t, err)
	assert.Equal(t, lit, `("a" > 1)`)

	lit, err = ds.Literal(I("a").Gte(1))
	assert.NoError(t, err)
	assert.Equal(t, lit, `("a" >= 1)`)

	lit, err = ds.Literal(I("a").Lt(1))
	assert.NoError(t, err)
	assert.Equal(t, lit, `("a" < 1)`)

	lit, err = ds.Literal(I("a").Lte(1))
	assert.NoError(t, err)
	assert.Equal(t, lit, `("a" <= 1)`)

	lit, err = ds.Literal(I("a").In([]int{1, 2, 3}))
	assert.NoError(t, err)
	assert.Equal(t, lit, `("a" IN (1, 2, 3))`)

	lit, err = ds.Literal(I("a").NotIn([]int{1, 2, 3}))
	assert.NoError(t, err)
	assert.Equal(t, lit, `("a" NOT IN (1, 2, 3))`)

	lit, err = ds.Literal(I("a").Like("a%"))
	assert.NoError(t, err)
	assert.Equal(t, lit, `("a" LIKE 'a%')`)

	lit, err = ds.Literal(I("a").Like(regexp.MustCompile("(a|b)")))
	assert.NoError(t, err)
	assert.Equal(t, lit, `("a" ~ '(a|b)')`)

	lit, err = ds.Literal(I("a").NotLike("a%"))
	assert.NoError(t, err)
	assert.Equal(t, lit, `("a" NOT LIKE 'a%')`)

	lit, err = ds.Literal(I("a").NotLike(regexp.MustCompile("(a|b)")))
	assert.NoError(t, err)
	assert.Equal(t, lit, `("a" !~ '(a|b)')`)

	lit, err = ds.Literal(I("a").ILike("a%"))
	assert.NoError(t, err)
	assert.Equal(t, lit, `("a" ILIKE 'a%')`)

	lit, err = ds.Literal(I("a").ILike(regexp.MustCompile("(a|b)")))
	assert.NoError(t, err)
	assert.Equal(t, lit, `("a" ~* '(a|b)')`)

	lit, err = ds.Literal(I("a").NotILike("a%"))
	assert.NoError(t, err)
	assert.Equal(t, lit, `("a" NOT ILIKE 'a%')`)

	lit, err = ds.Literal(I("a").NotILike(regexp.MustCompile("(a|b)")))
	assert.NoError(t, err)
	assert.Equal(t, lit, `("a" !~* '(a|b)')`)
}

func (me *datasetTest) TestLiteralOrderedExpression() {
	t := me.T()
	ds := From("test")
	lit, err := ds.Literal(I("a").Asc())
	assert.NoError(t, err)
	assert.Equal(t, lit, `"a" ASC`)

	lit, err = ds.Literal(I("a").Desc())
	assert.NoError(t, err)
	assert.Equal(t, lit, `"a" DESC`)

	lit, err = ds.Literal(I("a").Asc().NullsLast())
	assert.NoError(t, err)
	assert.Equal(t, lit, `"a" ASC NULLS LAST`)

	lit, err = ds.Literal(I("a").Desc().NullsLast())
	assert.NoError(t, err)
	assert.Equal(t, lit, `"a" DESC NULLS LAST`)

	lit, err = ds.Literal(I("a").Asc().NullsFirst())
	assert.NoError(t, err)
	assert.Equal(t, lit, `"a" ASC NULLS FIRST`)

	lit, err = ds.Literal(I("a").Desc().NullsFirst())
	assert.NoError(t, err)
	assert.Equal(t, lit, `"a" DESC NULLS FIRST`)
}

func (me *datasetTest) TestLiteralUpdateExpression() {
	t := me.T()
	ds := From("test")
	lit, err := ds.Literal(I("a").Set(1))
	assert.NoError(t, err)
	assert.Equal(t, lit, `"a"=1`)
}

func (me *datasetTest) TestLiteralSqlFunctionExpression() {
	t := me.T()
	ds := From("test")
	lit, err := ds.Literal(Func("MIN", I("a")))
	assert.NoError(t, err)
	assert.Equal(t, lit, `MIN("a")`)

	lit, err = ds.Literal(MIN("a"))
	assert.NoError(t, err)
	assert.Equal(t, lit, `MIN("a")`)

	lit, err = ds.Literal(COALESCE(I("a"), "a"))
	assert.NoError(t, err)
	assert.Equal(t, lit, `COALESCE("a", 'a')`)
}

func (me *datasetTest) TestLiteralCastExpression() {
	t := me.T()
	ds := From("test")
	lit, err := ds.Literal(I("a").Cast("DATE"))
	assert.NoError(t, err)
	assert.Equal(t, lit, `CAST("a" AS DATE)`)
}

func (me *datasetTest) TestCompoundExpression() {
	t := me.T()
	ds := From("test")
	lit, err := ds.Literal(Union(From("b")))
	assert.NoError(t, err)
	assert.Equal(t, lit, ` UNION (SELECT * FROM "b")`)

	lit, err = ds.Literal(UnionAll(From("b")))
	assert.NoError(t, err)
	assert.Equal(t, lit, ` UNION ALL (SELECT * FROM "b")`)

	lit, err = ds.Literal(Intersect(From("b")))
	assert.NoError(t, err)
	assert.Equal(t, lit, ` INTERSECT (SELECT * FROM "b")`)

	lit, err = ds.Literal(IntersectAll(From("b")))
	assert.NoError(t, err)
	assert.Equal(t, lit, ` INTERSECT ALL (SELECT * FROM "b")`)
}

func (me *datasetTest) TestLiteralIdentifierExpression() {
	t := me.T()
	ds := From("test")
	lit, err := ds.Literal(I("a"))
	assert.NoError(t, err)
	assert.Equal(t, lit, `"a"`)

	lit, err = ds.Literal(I("a.b"))
	assert.NoError(t, err)
	assert.Equal(t, lit, `"a"."b"`)

	lit, err = ds.Literal(I("a.b.c"))
	assert.NoError(t, err)
	assert.Equal(t, lit, `"a"."b"."c"`)

	lit, err = ds.Literal(I("a.b.*"))
	assert.NoError(t, err)
	assert.Equal(t, lit, `"a"."b".*`)

	lit, err = ds.Literal(I("a.*"))
	assert.NoError(t, err)
	assert.Equal(t, lit, `"a".*`)
}

func TestDatasetSuite(t *testing.T) {
	suite.Run(t, new(datasetTest))
}
