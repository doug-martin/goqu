package gql

import (
	"github.com/stretchr/testify/assert"
)

func (me *datasetTest) TestSelect() {
	t := me.T()
	ds1 := From("test")

	sql, err := ds1.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test"`)

	sql, err = ds1.Select().Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test"`)

	sql, err = ds1.Select("id").Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "id" FROM "test"`)

	sql, err = ds1.Select("id", "name").Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "id", "name" FROM "test"`)

	sql, err = ds1.Select(Literal("COUNT(*)").As("count")).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT COUNT(*) AS "count" FROM "test"`)

	sql, err = ds1.Select(I("id").As("other_id"), Literal("COUNT(*)").As("count")).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "id" AS "other_id", COUNT(*) AS "count" FROM "test"`)

	sql, err = ds1.From().Select(ds1.From("test_1").Select("id")).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT (SELECT "id" FROM "test_1")`)

	sql, err = ds1.From().Select(ds1.From("test_1").Select("id").As("test_id")).Sql()
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

	sql, err := ds1.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test"`)

	sql, err = ds1.SelectDistinct("id").Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT DISTINCT "id" FROM "test"`)

	sql, err = ds1.SelectDistinct("id", "name").Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT DISTINCT "id", "name" FROM "test"`)

	sql, err = ds1.SelectDistinct(Literal("COUNT(*)").As("count")).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT DISTINCT COUNT(*) AS "count" FROM "test"`)

	sql, err = ds1.SelectDistinct(I("id").As("other_id"), Literal("COUNT(*)").As("count")).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT DISTINCT "id" AS "other_id", COUNT(*) AS "count" FROM "test"`)

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

func (me *datasetTest) TestEmptyWhere() {
	t := me.T()
	ds1 := From("test")

	b := ds1.Where()
	sql, err := b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test"`)
}

func (me *datasetTest) TestWhere() {
	t := me.T()
	ds1 := From("test")

	b := ds1.Where(
		I("a").Eq(true),
		I("a").Neq(true),
		I("a").Eq(false),
		I("a").Neq(false),
	)
	sql, err := b.Sql()
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

	b := ds1.Order(I("a").Asc(), Literal(`("a" + "b" > 2)`).Asc())
	sql, err := b.Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" ORDER BY "a" ASC, ("a" + "b" > 2) ASC`)
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

	sql, err := ds1.Join(I("players").As("p"), On(I("p.id").Eq(I("items.playerId")))).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" INNER JOIN "players" AS "p" ON ("p"."id" = "items"."playerId")`)

	sql, err = ds1.Join(ds1.From("players").As("p"), On(I("p.id").Eq(I("items.playerId")))).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" INNER JOIN (SELECT * FROM "players") AS "p" ON ("p"."id" = "items"."playerId")`)

	sql, err = ds1.Join(I("v1").Table("test"), On(I("v1.test.id").Eq(I("items.playerId")))).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" INNER JOIN "v1"."test" ON ("v1"."test"."id" = "items"."playerId")`)

	sql, err = ds1.Join(I("test"), Using(I("name"), I("common_id"))).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" INNER JOIN "test" USING ("name", "common_id")`)

	sql, err = ds1.Join(I("test"), Using("name", "common_id")).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" INNER JOIN "test" USING ("name", "common_id")`)

}

func (me *datasetTest) TestLeftOuterJoin() {
	t := me.T()
	ds1 := From("items")

	sql, err := ds1.LeftOuterJoin(I("categories"), On(I("categories.categoryId").Eq(I("items.id")))).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" LEFT OUTER JOIN "categories" ON ("categories"."categoryId" = "items"."id")`)

	sql, err = ds1.LeftOuterJoin(I("categories"), On(I("categories.categoryId").Eq(I("items.id")), I("categories.categoryId").In(1, 2, 3))).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" LEFT OUTER JOIN "categories" ON (("categories"."categoryId" = "items"."id") AND ("categories"."categoryId" IN (1, 2, 3)))`)

	sql, err = ds1.Where(I("price").Lt(100)).RightOuterJoin(I("categories"), On(I("categories.categoryId").Eq(I("items.id")))).Sql()
}

func (me *datasetTest) TestFullOuterJoin() {
	t := me.T()
	ds1 := From("items")
	sql, err := ds1.FullOuterJoin(I("categories"), On(I("categories.categoryId").Eq(I("items.id")))).Order(I("stamp").Asc()).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" FULL OUTER JOIN "categories" ON ("categories"."categoryId" = "items"."id") ORDER BY "stamp" ASC`)

	sql, err = ds1.FullOuterJoin(I("categories"), On(I("categories.categoryId").Eq(I("items.id")))).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" FULL OUTER JOIN "categories" ON ("categories"."categoryId" = "items"."id")`)
}

func (me *datasetTest) TestInnerJoin() {
	t := me.T()
	ds1 := From("items")
	sql, err := ds1.InnerJoin(I("b"), On(I("b.itemsId").Eq(I("items.id")))).LeftOuterJoin(I("c"), On(I("c.b_id").Eq(I("b.id")))).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" INNER JOIN "b" ON ("b"."itemsId" = "items"."id") LEFT OUTER JOIN "c" ON ("c"."b_id" = "b"."id")`)

	sql, err = ds1.InnerJoin(I("b"), On(I("b.itemsId").Eq(I("items.id")))).LeftOuterJoin(I("c"), On(I("c.b_id").Eq(I("b.id")))).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" INNER JOIN "b" ON ("b"."itemsId" = "items"."id") LEFT OUTER JOIN "c" ON ("c"."b_id" = "b"."id")`)

	sql, err = ds1.InnerJoin(I("categories"), On(I("categories.categoryId").Eq(I("items.id")))).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" INNER JOIN "categories" ON ("categories"."categoryId" = "items"."id")`)
}

func (me *datasetTest) TestRightOuterJoin() {
	t := me.T()
	ds1 := From("items")
	sql, err := ds1.RightOuterJoin(I("categories"), On(I("categories.categoryId").Eq(I("items.id")))).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" RIGHT OUTER JOIN "categories" ON ("categories"."categoryId" = "items"."id")`)
}

func (me *datasetTest) TestLeftJoin() {
	t := me.T()
	ds1 := From("items")
	sql, err := ds1.LeftJoin(I("categories"), On(I("categories.categoryId").Eq(I("items.id")))).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" LEFT JOIN "categories" ON ("categories"."categoryId" = "items"."id")`)
}

func (me *datasetTest) TestRightJoin() {
	t := me.T()
	ds1 := From("items")
	sql, err := ds1.RightJoin(I("categories"), On(I("categories.categoryId").Eq(I("items.id")))).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" RIGHT JOIN "categories" ON ("categories"."categoryId" = "items"."id")`)
}

func (me *datasetTest) TestFullJoin() {
	t := me.T()
	ds1 := From("items")
	sql, err := ds1.FullJoin(I("categories"), On(I("categories.categoryId").Eq(I("items.id")))).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" FULL JOIN "categories" ON ("categories"."categoryId" = "items"."id")`)
}

func (me *datasetTest) TestNaturalJoin() {
	t := me.T()
	ds1 := From("items")
	sql, err := ds1.NaturalJoin(I("categories")).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" NATURAL JOIN "categories"`)
}

func (me *datasetTest) TestNaturalLeftJoin() {
	t := me.T()
	ds1 := From("items")
	sql, err := ds1.NaturalLeftJoin(I("categories")).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" NATURAL LEFT JOIN "categories"`)

}

func (me *datasetTest) TestNaturalRightJoin() {
	t := me.T()
	ds1 := From("items")
	sql, err := ds1.NaturalRightJoin(I("categories")).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" NATURAL RIGHT JOIN "categories"`)
}

func (me *datasetTest) TestNaturalFullJoin() {
	t := me.T()
	ds1 := From("items")
	sql, err := ds1.NaturalFullJoin(I("categories")).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" NATURAL FULL JOIN "categories"`)
}

func (me *datasetTest) TestCrossJoin() {
	t := me.T()
	sql, err := From("items").CrossJoin(I("categories")).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" CROSS JOIN "categories"`)
}

func (me *datasetTest) TestSqlFunctionExpressionsInHaving() {
	t := me.T()
	ds1 := From("items")
	sql, err := ds1.GroupBy("name").Having(SUM("amount").Gt(0)).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" GROUP BY "name" HAVING (SUM("amount") > 0)`)
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

//TO PREPARED

func (me *datasetTest) TestPreparedWhere() {
	t := me.T()
	ds1 := From("test")

	b := ds1.Where(
		I("a").Eq(true),
		I("a").Neq(true),
		I("a").Eq(false),
		I("a").Neq(false),
	)
	sql, args, err := b.ToSql(true)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{true, true, false, false})
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE (("a" IS ?) AND ("a" IS NOT ?) AND ("a" IS ?) AND ("a" IS NOT ?))`)

	b = ds1.Where(
		I("a").Eq("a"),
		I("b").Neq("b"),
		I("c").Gt("c"),
		I("d").Gte("d"),
		I("e").Lt("e"),
		I("f").Lte("f"),
	)
	sql, args, err = b.ToSql(true)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"a", "b", "c", "d", "e", "f"})
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE (("a" = ?) AND ("b" != ?) AND ("c" > ?) AND ("d" >= ?) AND ("e" < ?) AND ("f" <= ?))`)
}

func (me *datasetTest) TestPreparedLimit() {
	t := me.T()
	ds1 := From("test")

	b := ds1.Where(
		I("a").Gt(1),
	).Limit(10)
	sql, args, err := b.ToSql(true)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{1, 10})
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > ?) LIMIT ?`)

	b = ds1.Where(
		I("a").Gt(1),
	).Limit(0)
	sql, args, err = b.ToSql(true)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{1})
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > ?)`)
}

func (me *datasetTest) TestPreparedLimitAll() {
	t := me.T()
	ds1 := From("test")

	b := ds1.Where(
		I("a").Gt(1),
	).LimitAll()
	sql, args, err := b.ToSql(true)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{1})
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > ?) LIMIT ALL`)

	b = ds1.Where(
		I("a").Gt(1),
	).Limit(0).LimitAll()
	sql, args, err = b.ToSql(true)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{1})
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > ?) LIMIT ALL`)
}

func (me *datasetTest) TestPreparedClearLimit() {
	t := me.T()
	ds1 := From("test")

	b := ds1.Where(
		I("a").Gt(1),
	).LimitAll().ClearLimit()
	sql, args, err := b.ToSql(true)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{1})
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > ?)`)

	b = ds1.Where(
		I("a").Gt(1),
	).Limit(10).ClearLimit()
	sql, args, err = b.ToSql(true)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{1})
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > ?)`)
}

func (me *datasetTest) TestPreparedOffset() {
	t := me.T()
	ds1 := From("test")

	b := ds1.Where(
		I("a").Gt(1),
	).Offset(10)
	sql, args, err := b.ToSql(true)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{1, 10})
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > ?) OFFSET ?`)

	b = ds1.Where(
		I("a").Gt(1),
	).Offset(0)
	sql, args, err = b.ToSql(true)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{1})
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > ?)`)
}

func (me *datasetTest) TestPreparedClearOffset() {
	t := me.T()
	ds1 := From("test")

	b := ds1.Where(
		I("a").Gt(1),
	).Offset(10).ClearOffset()
	sql, args, err := b.ToSql(true)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{1})
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > ?)`)
}

func (me *datasetTest) TestPreparedGroupBy() {
	t := me.T()
	ds1 := From("test")

	b := ds1.Where(
		I("a").Gt(1),
	).GroupBy("created")
	sql, args, err := b.ToSql(true)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{1})
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > ?) GROUP BY "created"`)

	b = ds1.Where(
		I("a").Gt(1),
	).GroupBy(Literal("created::DATE"))
	sql, args, err = b.ToSql(true)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{1})
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > ?) GROUP BY created::DATE`)

	b = ds1.Where(
		I("a").Gt(1),
	).GroupBy("name", Literal("created::DATE"))
	sql, args, err = b.ToSql(true)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{1})
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > ?) GROUP BY "name", created::DATE`)
}

func (me *datasetTest) TestPreparedHaving() {
	t := me.T()
	ds1 := From("test")

	b := ds1.Having(
		I("a").Gt(1),
	).GroupBy("created")
	sql, args, err := b.ToSql(true)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{1})
	assert.Equal(t, sql, `SELECT * FROM "test" GROUP BY "created" HAVING ("a" > ?)`)

	b = ds1.Where(
		I("b").IsTrue(),
	).Having(
		I("a").Gt(1),
	).GroupBy("created")
	sql, args, err = b.ToSql(true)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{true, 1})
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("b" IS ?) GROUP BY "created" HAVING ("a" > ?)`)

	b = ds1.Having(
		I("a").Gt(1),
	)
	sql, args, err = b.ToSql(true)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{1})
	assert.Equal(t, sql, `SELECT * FROM "test" HAVING ("a" > ?)`)
}

func (me *datasetTest) TestPreparedJoin() {
	t := me.T()
	ds1 := From("items")

	sql, args, err := ds1.Join(I("players").As("p"), On(I("p.id").Eq(I("items.playerId")))).ToSql(true)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{})
	assert.Equal(t, sql, `SELECT * FROM "items" INNER JOIN "players" AS "p" ON ("p"."id" = "items"."playerId")`)

	sql, args, err = ds1.Join(ds1.From("players").As("p"), On(I("p.id").Eq(I("items.playerId")))).ToSql(true)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{})
	assert.Equal(t, sql, `SELECT * FROM "items" INNER JOIN (SELECT * FROM "players") AS "p" ON ("p"."id" = "items"."playerId")`)

	sql, args, err = ds1.Join(I("v1").Table("test"), On(I("v1.test.id").Eq(I("items.playerId")))).ToSql(true)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{})
	assert.Equal(t, sql, `SELECT * FROM "items" INNER JOIN "v1"."test" ON ("v1"."test"."id" = "items"."playerId")`)

	sql, args, err = ds1.Join(I("test"), Using(I("name"), I("common_id"))).ToSql(true)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{})
	assert.Equal(t, sql, `SELECT * FROM "items" INNER JOIN "test" USING ("name", "common_id")`)

	sql, args, err = ds1.Join(I("test"), Using("name", "common_id")).ToSql(true)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{})
	assert.Equal(t, sql, `SELECT * FROM "items" INNER JOIN "test" USING ("name", "common_id")`)

	sql, args, err = ds1.Join(I("categories"), On(I("categories.categoryId").Eq(I("items.id")), I("categories.categoryId").In(1, 2, 3))).ToSql(true)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{1, 2, 3})
	assert.Equal(t, sql, `SELECT * FROM "items" INNER JOIN "categories" ON (("categories"."categoryId" = "items"."id") AND ("categories"."categoryId" IN (?, ?, ?)))`)

}

func (me *datasetTest) TestPreparedFunctionExpressionsInHaving() {
	t := me.T()
	ds1 := From("items")
	sql, args, err := ds1.GroupBy("name").Having(SUM("amount").Gt(0)).ToSql(true)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{0})
	assert.Equal(t, sql, `SELECT * FROM "items" GROUP BY "name" HAVING (SUM("amount") > ?)`)
}

func (me *datasetTest) TestPreparedUnion() {
	t := me.T()
	a := From("invoice").Select("id", "amount").Where(I("amount").Gt(1000))
	b := From("invoice").Select("id", "amount").Where(I("amount").Lt(10))

	sql, args, err := a.Union(b).ToSql(true)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{1000, 10})
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) UNION (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?))`)

	sql, args, err = a.Limit(1).Union(b).ToSql(true)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{1000, 1, 10})
	assert.Equal(t, sql, `SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) LIMIT ?) AS "t1" UNION (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?))`)

	sql, args, err = a.Union(b.Limit(1)).ToSql(true)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{1000, 10, 1})
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) UNION (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?) LIMIT ?) AS "t1")`)

	sql, args, err = a.Union(b).Union(b.Where(I("id").Lt(50))).ToSql(true)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{1000, 10, 10, 50})
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) UNION (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?)) UNION (SELECT "id", "amount" FROM "invoice" WHERE (("amount" < ?) AND ("id" < ?)))`)

}

func (me *datasetTest) TestPreparedUnionAll() {
	t := me.T()
	a := From("invoice").Select("id", "amount").Where(I("amount").Gt(1000))
	b := From("invoice").Select("id", "amount").Where(I("amount").Lt(10))

	sql, args, err := a.UnionAll(b).ToSql(true)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{1000, 10})
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) UNION ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?))`)

	sql, args, err = a.Limit(1).UnionAll(b).ToSql(true)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{1000, 1, 10})
	assert.Equal(t, sql, `SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) LIMIT ?) AS "t1" UNION ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?))`)

	sql, args, err = a.UnionAll(b.Limit(1)).ToSql(true)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{1000, 10, 1})
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) UNION ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?) LIMIT ?) AS "t1")`)

	sql, args, err = a.UnionAll(b).UnionAll(b.Where(I("id").Lt(50))).ToSql(true)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{1000, 10, 10, 50})
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) UNION ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?)) UNION ALL (SELECT "id", "amount" FROM "invoice" WHERE (("amount" < ?) AND ("id" < ?)))`)
}

func (me *datasetTest) TestPreparedIntersect() {
	t := me.T()
	a := From("invoice").Select("id", "amount").Where(I("amount").Gt(1000))
	b := From("invoice").Select("id", "amount").Where(I("amount").Lt(10))

	sql, args, err := a.Intersect(b).ToSql(true)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{1000, 10})
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) INTERSECT (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?))`)

	sql, args, err = a.Limit(1).Intersect(b).ToSql(true)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{1000, 1, 10})
	assert.Equal(t, sql, `SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) LIMIT ?) AS "t1" INTERSECT (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?))`)

	sql, args, err = a.Intersect(b.Limit(1)).ToSql(true)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{1000, 10, 1})
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) INTERSECT (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?) LIMIT ?) AS "t1")`)

}

func (me *datasetTest) TestPreparedIntersectAll() {
	t := me.T()
	a := From("invoice").Select("id", "amount").Where(I("amount").Gt(1000))
	b := From("invoice").Select("id", "amount").Where(I("amount").Lt(10))

	sql, args, err := a.IntersectAll(b).ToSql(true)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{1000, 10})
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) INTERSECT ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?))`)

	sql, args, err = a.Limit(1).IntersectAll(b).ToSql(true)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{1000, 1, 10})
	assert.Equal(t, sql, `SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) LIMIT ?) AS "t1" INTERSECT ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?))`)

	sql, args, err = a.IntersectAll(b.Limit(1)).ToSql(true)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{1000, 10, 1})
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) INTERSECT ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?) LIMIT ?) AS "t1")`)

}
