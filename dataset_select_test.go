package goqu

import (
	"github.com/stretchr/testify/assert"
)

func (me *datasetTest) TestSelect() {
	t := me.T()
	ds1 := From("test")

	sql, _, err := ds1.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test"`)

	sql, _, err = ds1.Select().ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test"`)

	sql, _, err = ds1.Select("id").ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "id" FROM "test"`)

	sql, _, err = ds1.Select("id", "name").ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "id", "name" FROM "test"`)

	sql, _, err = ds1.Select(Literal("COUNT(*)").As("count")).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT COUNT(*) AS "count" FROM "test"`)

	sql, _, err = ds1.Select(I("id").As("other_id"), Literal("COUNT(*)").As("count")).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "id" AS "other_id", COUNT(*) AS "count" FROM "test"`)

	sql, _, err = ds1.From().Select(ds1.From("test_1").Select("id")).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT (SELECT "id" FROM "test_1")`)

	sql, _, err = ds1.From().Select(ds1.From("test_1").Select("id").As("test_id")).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT (SELECT "id" FROM "test_1") AS "test_id"`)

	sql, _, err = ds1.From().
		Select(
			DISTINCT("a").As("distinct"),
			COUNT("a").As("count"),
			L("CASE WHEN ? THEN ? ELSE ? END", MIN("a").Eq(10), true, false),
			L("CASE WHEN ? THEN ? ELSE ? END", AVG("a").Neq(10), true, false),
			L("CASE WHEN ? THEN ? ELSE ? END", FIRST("a").Gt(10), true, false),
			L("CASE WHEN ? THEN ? ELSE ? END", FIRST("a").Gte(10), true, false),
			L("CASE WHEN ? THEN ? ELSE ? END", LAST("a").Lt(10), true, false),
			L("CASE WHEN ? THEN ? ELSE ? END", LAST("a").Lte(10), true, false),
			SUM("a").As("sum"),
			COALESCE(I("a"), "a").As("colaseced"),
		).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT DISTINCT("a") AS "distinct", COUNT("a") AS "count", CASE WHEN (MIN("a") = 10) THEN TRUE ELSE FALSE END, CASE WHEN (AVG("a") != 10) THEN TRUE ELSE FALSE END, CASE WHEN (FIRST("a") > 10) THEN TRUE ELSE FALSE END, CASE WHEN (FIRST("a") >= 10) THEN TRUE ELSE FALSE END, CASE WHEN (LAST("a") < 10) THEN TRUE ELSE FALSE END, CASE WHEN (LAST("a") <= 10) THEN TRUE ELSE FALSE END, SUM("a") AS "sum", COALESCE("a", 'a') AS "colaseced"`)

	type MyStruct struct {
		Name         string
		Address      string `db:"address"`
		EmailAddress string `db:"email_address"`
		FakeCol      string `db:"-"`
	}
	sql, _, err = ds1.Select(&MyStruct{}).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "address", "email_address", "name" FROM "test"`)

	sql, _, err = ds1.Select(MyStruct{}).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "address", "email_address", "name" FROM "test"`)

	type myStruct2 struct {
		MyStruct
		Zipcode string `db:"zipcode"`
	}

	sql, _, err = ds1.Select(&myStruct2{}).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "address", "email_address", "name", "zipcode" FROM "test"`)

	sql, _, err = ds1.Select(myStruct2{}).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "address", "email_address", "name", "zipcode" FROM "test"`)

	var myStructs []MyStruct
	sql, _, err = ds1.Select(&myStructs).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "address", "email_address", "name" FROM "test"`)

	sql, _, err = ds1.Select(myStructs).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "address", "email_address", "name" FROM "test"`)

	//should not change original
	sql, _, err = ds1.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test"`)
}

func (me *datasetTest) TestSelectDistinct() {
	t := me.T()
	ds1 := From("test")

	sql, _, err := ds1.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test"`)

	sql, _, err = ds1.SelectDistinct("id").ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT DISTINCT "id" FROM "test"`)

	sql, _, err = ds1.SelectDistinct("id", "name").ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT DISTINCT "id", "name" FROM "test"`)

	sql, _, err = ds1.SelectDistinct(Literal("COUNT(*)").As("count")).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT DISTINCT COUNT(*) AS "count" FROM "test"`)

	sql, _, err = ds1.SelectDistinct(I("id").As("other_id"), Literal("COUNT(*)").As("count")).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT DISTINCT "id" AS "other_id", COUNT(*) AS "count" FROM "test"`)

	type MyStruct struct {
		Name         string
		Address      string `db:"address"`
		EmailAddress string `db:"email_address"`
		FakeCol      string `db:"-"`
	}
	sql, _, err = ds1.SelectDistinct(&MyStruct{}).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT DISTINCT "address", "email_address", "name" FROM "test"`)

	sql, _, err = ds1.SelectDistinct(MyStruct{}).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT DISTINCT "address", "email_address", "name" FROM "test"`)

	type myStruct2 struct {
		MyStruct
		Zipcode string `db:"zipcode"`
	}

	sql, _, err = ds1.SelectDistinct(&myStruct2{}).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT DISTINCT "address", "email_address", "name", "zipcode" FROM "test"`)

	sql, _, err = ds1.SelectDistinct(myStruct2{}).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT DISTINCT "address", "email_address", "name", "zipcode" FROM "test"`)

	var myStructs []MyStruct
	sql, _, err = ds1.SelectDistinct(&myStructs).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT DISTINCT "address", "email_address", "name" FROM "test"`)

	sql, _, err = ds1.SelectDistinct(myStructs).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT DISTINCT "address", "email_address", "name" FROM "test"`)

	//should not change original
	sql, _, err = ds1.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test"`)

	//should not change original
	sql, _, err = ds1.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test"`)
}

func (me *datasetTest) TestClearSelect() {
	t := me.T()
	ds1 := From("test")

	sql, _, err := ds1.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test"`)

	b := ds1.Select("a").ClearSelect()
	sql, _, err = b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test"`)
}

func (me *datasetTest) TestSelectAppend() {
	t := me.T()
	ds1 := From("test")

	sql, _, err := ds1.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test"`)

	b := ds1.Select("a").SelectAppend("b").SelectAppend("c")
	sql, _, err = b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "a", "b", "c" FROM "test"`)
}

func (me *datasetTest) TestFrom() {
	t := me.T()
	ds1 := From("test")

	sql, _, err := ds1.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test"`)

	ds2 := ds1.From("test2")
	sql, _, err = ds2.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test2"`)

	ds2 = ds1.From("test2", "test3")
	sql, _, err = ds2.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test2", "test3"`)

	ds2 = ds1.From(I("test2").As("test_2"), "test3")
	sql, _, err = ds2.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test2" AS "test_2", "test3"`)

	ds2 = ds1.From(ds1.From("test2"), "test3")
	sql, _, err = ds2.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM (SELECT * FROM "test2") AS "t1", "test3"`)

	ds2 = ds1.From(ds1.From("test2").As("test_2"), "test3")
	sql, _, err = ds2.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM (SELECT * FROM "test2") AS "test_2", "test3"`)

	//should not change original
	sql, _, err = ds1.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test"`)
}

func (me *datasetTest) TestEmptyWhere() {
	t := me.T()
	ds1 := From("test")

	b := ds1.Where()
	sql, _, err := b.ToSql()
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
	sql, _, err := b.ToSql()
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
	sql, _, err = b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE (("a" = 'a') AND ("b" != 'b') AND ("c" > 'c') AND ("d" >= 'd') AND ("e" < 'e') AND ("f" <= 'f'))`)

	b = ds1.Where(
		I("a").Eq(From("test2").Select("id")),
	)
	sql, _, err = b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" IN (SELECT "id" FROM "test2"))`)

	b = ds1.Where(Ex{
		"a": "a",
		"b": Op{"neq": "b"},
		"c": Op{"gt": "c"},
		"d": Op{"gte": "d"},
		"e": Op{"lt": "e"},
		"f": Op{"lte": "f"},
	})
	sql, _, err = b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE (("a" = 'a') AND ("b" != 'b') AND ("c" > 'c') AND ("d" >= 'd') AND ("e" < 'e') AND ("f" <= 'f'))`)

	b = ds1.Where(Ex{
		"a": From("test2").Select("id"),
	})
	sql, _, err = b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" IN (SELECT "id" FROM "test2"))`)
}

func (me *datasetTest) TestWhereChain() {
	t := me.T()
	ds1 := From("test").Where(
		I("x").Eq(0),
		I("y").Eq(1),
	)

	ds2 := ds1.Where(
		I("z").Eq(2),
	)

	a := ds2.Where(
		I("a").Eq("A"),
	)
	b := ds2.Where(
		I("b").Eq("B"),
	)
	sql, _, err := a.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE (("x" = 0) AND ("y" = 1) AND ("z" = 2) AND ("a" = 'A'))`)
	sql, _, err = b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE (("x" = 0) AND ("y" = 1) AND ("z" = 2) AND ("b" = 'B'))`)
}

func (me *datasetTest) TestClearWhere() {
	t := me.T()
	ds1 := From("test")

	b := ds1.Where(
		I("a").Eq(1),
	).ClearWhere()
	sql, _, err := b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test"`)
}

func (me *datasetTest) TestLimit() {
	t := me.T()
	ds1 := From("test")

	b := ds1.Where(
		I("a").Gt(1),
	).Limit(10)
	sql, _, err := b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > 1) LIMIT 10`)

	b = ds1.Where(
		I("a").Gt(1),
	).Limit(0)
	sql, _, err = b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > 1)`)
}

func (me *datasetTest) TestLimitAll() {
	t := me.T()
	ds1 := From("test")

	b := ds1.Where(
		I("a").Gt(1),
	).LimitAll()
	sql, _, err := b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > 1) LIMIT ALL`)

	b = ds1.Where(
		I("a").Gt(1),
	).Limit(0).LimitAll()
	sql, _, err = b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > 1) LIMIT ALL`)
}

func (me *datasetTest) TestClearLimit() {
	t := me.T()
	ds1 := From("test")

	b := ds1.Where(
		I("a").Gt(1),
	).LimitAll().ClearLimit()
	sql, _, err := b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > 1)`)

	b = ds1.Where(
		I("a").Gt(1),
	).Limit(10).ClearLimit()
	sql, _, err = b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > 1)`)
}

func (me *datasetTest) TestOffset() {
	t := me.T()
	ds1 := From("test")

	b := ds1.Where(
		I("a").Gt(1),
	).Offset(10)
	sql, _, err := b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > 1) OFFSET 10`)

	b = ds1.Where(
		I("a").Gt(1),
	).Offset(0)
	sql, _, err = b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > 1)`)
}

func (me *datasetTest) TestClearOffset() {
	t := me.T()
	ds1 := From("test")

	b := ds1.Where(
		I("a").Gt(1),
	).Offset(10).ClearOffset()
	sql, _, err := b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > 1)`)
}

func (me *datasetTest) TestForUpdate() {
	t := me.T()
	ds1 := From("test")

	b := ds1.Where(
		I("a").Gt(1),
	).ForUpdate(WAIT)
	sql, _, err := b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > 1) FOR UPDATE `)

	b = ds1.Where(
		I("a").Gt(1),
	).ForUpdate(SKIP_LOCKED)
	sql, _, err = b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > 1) FOR UPDATE SKIP LOCKED`)
}

func (me *datasetTest) TestForNoKeyUpdate() {
	t := me.T()
	ds1 := From("test")

	b := ds1.Where(
		I("a").Gt(1),
	).ForNoKeyUpdate(WAIT)
	sql, _, err := b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > 1) FOR NO KEY UPDATE `)

	b = ds1.Where(
		I("a").Gt(1),
	).ForNoKeyUpdate(SKIP_LOCKED)
	sql, _, err = b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > 1) FOR NO KEY UPDATE SKIP LOCKED`)
}

func (me *datasetTest) TestForKeyShare() {
	t := me.T()
	ds1 := From("test")

	b := ds1.Where(
		I("a").Gt(1),
	).ForKeyShare(WAIT)
	sql, _, err := b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > 1) FOR KEY SHARE `)

	b = ds1.Where(
		I("a").Gt(1),
	).ForKeyShare(SKIP_LOCKED)
	sql, _, err = b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > 1) FOR KEY SHARE SKIP LOCKED`)
}

func (me *datasetTest) TestForShare() {
	t := me.T()
	ds1 := From("test")

	b := ds1.Where(
		I("a").Gt(1),
	).ForShare(WAIT)
	sql, _, err := b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > 1) FOR SHARE `)

	b = ds1.Where(
		I("a").Gt(1),
	).ForShare(SKIP_LOCKED)
	sql, _, err = b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > 1) FOR SHARE SKIP LOCKED`)
}

func (me *datasetTest) TestGroupBy() {
	t := me.T()
	ds1 := From("test")

	b := ds1.Where(
		I("a").Gt(1),
	).GroupBy("created")
	sql, _, err := b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > 1) GROUP BY "created"`)

	b = ds1.Where(
		I("a").Gt(1),
	).GroupBy(Literal("created::DATE"))
	sql, _, err = b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > 1) GROUP BY created::DATE`)

	b = ds1.Where(
		I("a").Gt(1),
	).GroupBy("name", Literal("created::DATE"))
	sql, _, err = b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > 1) GROUP BY "name", created::DATE`)
}

func (me *datasetTest) TestHaving() {
	t := me.T()
	ds1 := From("test")

	b := ds1.Having(Ex{
		"a": Op{"gt": 1},
	}).GroupBy("created")
	sql, _, err := b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" GROUP BY "created" HAVING ("a" > 1)`)

	b = ds1.Where(Ex{"b": true}).
		Having(Ex{"a": Op{"gt": 1}}).
		GroupBy("created")
	sql, _, err = b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("b" IS TRUE) GROUP BY "created" HAVING ("a" > 1)`)

	b = ds1.Having(Ex{"a": Op{"gt": 1}})
	sql, _, err = b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" HAVING ("a" > 1)`)

	b = ds1.Having(Ex{"a": Op{"gt": 1}}).Having(Ex{"b": 2})
	sql, _, err = b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" HAVING (("a" > 1) AND ("b" = 2))`)
}

func (me *datasetTest) TestOrder() {
	t := me.T()

	ds1 := From("test")

	b := ds1.Order(I("a").Asc(), Literal(`("a" + "b" > 2)`).Asc())
	sql, _, err := b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" ORDER BY "a" ASC, ("a" + "b" > 2) ASC`)
}

func (me *datasetTest) TestOrderAppend() {
	t := me.T()
	b := From("test").Order(I("a").Asc().NullsFirst()).OrderAppend(I("b").Desc().NullsLast())
	sql, _, err := b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" ORDER BY "a" ASC NULLS FIRST, "b" DESC NULLS LAST`)

	b = From("test").OrderAppend(I("a").Asc().NullsFirst()).OrderAppend(I("b").Desc().NullsLast())
	sql, _, err = b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test" ORDER BY "a" ASC NULLS FIRST, "b" DESC NULLS LAST`)

}

func (me *datasetTest) TestClearOrder() {
	t := me.T()
	b := From("test").Order(I("a").Asc().NullsFirst()).ClearOrder()
	sql, _, err := b.ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "test"`)
}

func (me *datasetTest) TestJoin() {
	t := me.T()
	ds1 := From("items")

	sql, _, err := ds1.Join(I("players").As("p"), On(Ex{"p.id": I("items.playerId")})).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" INNER JOIN "players" AS "p" ON ("p"."id" = "items"."playerId")`)

	sql, _, err = ds1.Join(ds1.From("players").As("p"), On(Ex{"p.id": I("items.playerId")})).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" INNER JOIN (SELECT * FROM "players") AS "p" ON ("p"."id" = "items"."playerId")`)

	sql, _, err = ds1.Join(I("v1").Table("test"), On(Ex{"v1.test.id": I("items.playerId")})).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" INNER JOIN "v1"."test" ON ("v1"."test"."id" = "items"."playerId")`)

	sql, _, err = ds1.Join(I("test"), Using(I("name"), I("common_id"))).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" INNER JOIN "test" USING ("name", "common_id")`)

	sql, _, err = ds1.Join(I("test"), Using("name", "common_id")).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" INNER JOIN "test" USING ("name", "common_id")`)

}

func (me *datasetTest) TestLeftOuterJoin() {
	t := me.T()
	ds1 := From("items")

	sql, _, err := ds1.LeftOuterJoin(I("categories"), On(Ex{
		"categories.categoryId": I("items.id"),
	})).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" LEFT OUTER JOIN "categories" ON ("categories"."categoryId" = "items"."id")`)

	sql, _, err = ds1.LeftOuterJoin(I("categories"), On(I("categories.categoryId").Eq(I("items.id")), I("categories.categoryId").In(1, 2, 3))).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" LEFT OUTER JOIN "categories" ON (("categories"."categoryId" = "items"."id") AND ("categories"."categoryId" IN (1, 2, 3)))`)

	sql, _, err = ds1.Where(I("price").Lt(100)).RightOuterJoin(I("categories"), On(Ex{"categories.categoryId": I("items.id")})).ToSql()
}

func (me *datasetTest) TestFullOuterJoin() {
	t := me.T()
	ds1 := From("items")
	sql, _, err := ds1.
		FullOuterJoin(I("categories"), On(Ex{"categories.categoryId": I("items.id")})).
		Order(I("stamp").Asc()).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" FULL OUTER JOIN "categories" ON ("categories"."categoryId" = "items"."id") ORDER BY "stamp" ASC`)

	sql, _, err = ds1.FullOuterJoin(I("categories"), On(Ex{"categories.categoryId": I("items.id")})).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" FULL OUTER JOIN "categories" ON ("categories"."categoryId" = "items"."id")`)
}

func (me *datasetTest) TestInnerJoin() {
	t := me.T()
	ds1 := From("items")
	sql, _, err := ds1.
		InnerJoin(I("b"), On(Ex{"b.itemsId": I("items.id")})).
		LeftOuterJoin(I("c"), On(Ex{"c.b_id": I("b.id")})).
		ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" INNER JOIN "b" ON ("b"."itemsId" = "items"."id") LEFT OUTER JOIN "c" ON ("c"."b_id" = "b"."id")`)

	sql, _, err = ds1.
		InnerJoin(I("b"), On(Ex{"b.itemsId": I("items.id")})).
		LeftOuterJoin(I("c"), On(Ex{"c.b_id": I("b.id")})).
		ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" INNER JOIN "b" ON ("b"."itemsId" = "items"."id") LEFT OUTER JOIN "c" ON ("c"."b_id" = "b"."id")`)

	sql, _, err = ds1.InnerJoin(I("categories"), On(Ex{"categories.categoryId": I("items.id")})).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" INNER JOIN "categories" ON ("categories"."categoryId" = "items"."id")`)
}

func (me *datasetTest) TestRightOuterJoin() {
	t := me.T()
	ds1 := From("items")
	sql, _, err := ds1.RightOuterJoin(I("categories"), On(Ex{"categories.categoryId": I("items.id")})).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" RIGHT OUTER JOIN "categories" ON ("categories"."categoryId" = "items"."id")`)
}

func (me *datasetTest) TestLeftJoin() {
	t := me.T()
	ds1 := From("items")
	sql, _, err := ds1.LeftJoin(I("categories"), On(Ex{"categories.categoryId": I("items.id")})).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" LEFT JOIN "categories" ON ("categories"."categoryId" = "items"."id")`)
}

func (me *datasetTest) TestRightJoin() {
	t := me.T()
	ds1 := From("items")
	sql, _, err := ds1.RightJoin(I("categories"), On(Ex{"categories.categoryId": I("items.id")})).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" RIGHT JOIN "categories" ON ("categories"."categoryId" = "items"."id")`)
}

func (me *datasetTest) TestFullJoin() {
	t := me.T()
	ds1 := From("items")
	sql, _, err := ds1.FullJoin(I("categories"), On(Ex{"categories.categoryId": I("items.id")})).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" FULL JOIN "categories" ON ("categories"."categoryId" = "items"."id")`)
}

func (me *datasetTest) TestNaturalJoin() {
	t := me.T()
	ds1 := From("items")
	sql, _, err := ds1.NaturalJoin(I("categories")).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" NATURAL JOIN "categories"`)
}

func (me *datasetTest) TestNaturalLeftJoin() {
	t := me.T()
	ds1 := From("items")
	sql, _, err := ds1.NaturalLeftJoin(I("categories")).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" NATURAL LEFT JOIN "categories"`)

}

func (me *datasetTest) TestNaturalRightJoin() {
	t := me.T()
	ds1 := From("items")
	sql, _, err := ds1.NaturalRightJoin(I("categories")).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" NATURAL RIGHT JOIN "categories"`)
}

func (me *datasetTest) TestNaturalFullJoin() {
	t := me.T()
	ds1 := From("items")
	sql, _, err := ds1.NaturalFullJoin(I("categories")).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" NATURAL FULL JOIN "categories"`)
}

func (me *datasetTest) TestCrossJoin() {
	t := me.T()
	sql, _, err := From("items").CrossJoin(I("categories")).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" CROSS JOIN "categories"`)
}

func (me *datasetTest) TestSqlFunctionExpressionsInHaving() {
	t := me.T()
	ds1 := From("items")
	sql, _, err := ds1.GroupBy("name").Having(SUM("amount").Gt(0)).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "items" GROUP BY "name" HAVING (SUM("amount") > 0)`)
}

func (me *datasetTest) TestUnion() {
	t := me.T()
	a := From("invoice").Select("id", "amount").Where(I("amount").Gt(1000))
	b := From("invoice").Select("id", "amount").Where(I("amount").Lt(10))

	sql, _, err := a.Union(b).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) UNION (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	sql, _, err = a.Limit(1).Union(b).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) LIMIT 1) AS "t1" UNION (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	sql, _, err = a.Order(I("id").Asc()).Union(b).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) ORDER BY "id" ASC) AS "t1" UNION (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	sql, _, err = a.Union(b.Limit(1)).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) UNION (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) LIMIT 1) AS "t1")`)

	sql, _, err = a.Union(b.Order(I("id").Desc())).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) UNION (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) ORDER BY "id" DESC) AS "t1")`)

	sql, _, err = a.Limit(1).Union(b.Order(I("id").Desc())).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) LIMIT 1) AS "t1" UNION (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) ORDER BY "id" DESC) AS "t1")`)

	sql, _, err = a.Union(b).Union(b.Where(I("id").Lt(50))).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) UNION (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10)) UNION (SELECT "id", "amount" FROM "invoice" WHERE (("amount" < 10) AND ("id" < 50)))`)

}

func (me *datasetTest) TestUnionAll() {
	t := me.T()
	a := From("invoice").Select("id", "amount").Where(I("amount").Gt(1000))
	b := From("invoice").Select("id", "amount").Where(I("amount").Lt(10))

	sql, _, err := a.UnionAll(b).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) UNION ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	sql, _, err = a.Limit(1).UnionAll(b).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) LIMIT 1) AS "t1" UNION ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	sql, _, err = a.Order(I("id").Asc()).UnionAll(b).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) ORDER BY "id" ASC) AS "t1" UNION ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	sql, _, err = a.UnionAll(b.Limit(1)).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) UNION ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) LIMIT 1) AS "t1")`)

	sql, _, err = a.UnionAll(b.Order(I("id").Desc())).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) UNION ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) ORDER BY "id" DESC) AS "t1")`)

	sql, _, err = a.Limit(1).UnionAll(b.Order(I("id").Desc())).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) LIMIT 1) AS "t1" UNION ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) ORDER BY "id" DESC) AS "t1")`)
}

func (me *datasetTest) TestIntersect() {
	t := me.T()
	a := From("invoice").Select("id", "amount").Where(I("amount").Gt(1000))
	b := From("invoice").Select("id", "amount").Where(I("amount").Lt(10))

	sql, _, err := a.Intersect(b).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) INTERSECT (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	sql, _, err = a.Limit(1).Intersect(b).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) LIMIT 1) AS "t1" INTERSECT (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	sql, _, err = a.Order(I("id").Asc()).Intersect(b).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) ORDER BY "id" ASC) AS "t1" INTERSECT (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	sql, _, err = a.Intersect(b.Limit(1)).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) INTERSECT (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) LIMIT 1) AS "t1")`)

	sql, _, err = a.Intersect(b.Order(I("id").Desc())).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) INTERSECT (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) ORDER BY "id" DESC) AS "t1")`)

	sql, _, err = a.Limit(1).Intersect(b.Order(I("id").Desc())).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) LIMIT 1) AS "t1" INTERSECT (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) ORDER BY "id" DESC) AS "t1")`)
}

func (me *datasetTest) TestIntersectAll() {
	t := me.T()
	a := From("invoice").Select("id", "amount").Where(I("amount").Gt(1000))
	b := From("invoice").Select("id", "amount").Where(I("amount").Lt(10))

	sql, _, err := a.IntersectAll(b).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) INTERSECT ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	sql, _, err = a.Limit(1).IntersectAll(b).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) LIMIT 1) AS "t1" INTERSECT ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	sql, _, err = a.Order(I("id").Asc()).IntersectAll(b).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) ORDER BY "id" ASC) AS "t1" INTERSECT ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	sql, _, err = a.IntersectAll(b.Limit(1)).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) INTERSECT ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) LIMIT 1) AS "t1")`)

	sql, _, err = a.IntersectAll(b.Order(I("id").Desc())).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) INTERSECT ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) ORDER BY "id" DESC) AS "t1")`)

	sql, _, err = a.Limit(1).IntersectAll(b.Order(I("id").Desc())).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) LIMIT 1) AS "t1" INTERSECT ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) ORDER BY "id" DESC) AS "t1")`)
}

//TO PREPARED

func (me *datasetTest) TestPreparedWhere() {
	t := me.T()
	ds1 := From("test")

	b := ds1.Where(Ex{
		"a": true,
		"b": Op{"neq": true},
		"c": false,
		"d": Op{"neq": false},
		"e": nil,
	})
	sql, args, err := b.Prepared(true).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{})
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE (("a" IS TRUE) AND ("b" IS NOT TRUE) AND ("c" IS FALSE) AND ("d" IS NOT FALSE) AND ("e" IS NULL))`)

	b = ds1.Where(Ex{
		"a": "a",
		"b": Op{"neq": "b"},
		"c": Op{"gt": "c"},
		"d": Op{"gte": "d"},
		"e": Op{"lt": "e"},
		"f": Op{"lte": "f"},
		"g": Op{"is": nil},
		"h": Op{"isnot": nil},
	})
	sql, args, err = b.Prepared(true).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"a", "b", "c", "d", "e", "f"})
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE (("a" = ?) AND ("b" != ?) AND ("c" > ?) AND ("d" >= ?) AND ("e" < ?) AND ("f" <= ?) AND ("g" IS NULL) AND ("h" IS NOT NULL))`)
}

func (me *datasetTest) TestPreparedLimit() {
	t := me.T()
	ds1 := From("test")

	b := ds1.Where(I("a").Gt(1)).Limit(10)
	sql, args, err := b.Prepared(true).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1), int64(10)})
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > ?) LIMIT ?`)

	b = ds1.Where(I("a").Gt(1)).Limit(0)
	sql, args, err = b.Prepared(true).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1)})
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > ?)`)
}

func (me *datasetTest) TestPreparedLimitAll() {
	t := me.T()
	ds1 := From("test")

	b := ds1.Where(I("a").Gt(1)).LimitAll()
	sql, args, err := b.Prepared(true).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1)})
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > ?) LIMIT ALL`)

	b = ds1.Where(I("a").Gt(1)).Limit(0).LimitAll()
	sql, args, err = b.Prepared(true).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1)})
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > ?) LIMIT ALL`)
}

func (me *datasetTest) TestPreparedClearLimit() {
	t := me.T()
	ds1 := From("test")

	b := ds1.Where(I("a").Gt(1)).LimitAll().ClearLimit()
	sql, args, err := b.Prepared(true).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1)})
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > ?)`)

	b = ds1.Where(I("a").Gt(1)).Limit(10).ClearLimit()
	sql, args, err = b.Prepared(true).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1)})
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > ?)`)
}

func (me *datasetTest) TestPreparedOffset() {
	t := me.T()
	ds1 := From("test")

	b := ds1.Where(I("a").Gt(1)).Offset(10)
	sql, args, err := b.Prepared(true).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1), int64(10)})
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > ?) OFFSET ?`)

	b = ds1.Where(I("a").Gt(1)).Offset(0)
	sql, args, err = b.Prepared(true).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1)})
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > ?)`)
}

func (me *datasetTest) TestPreparedClearOffset() {
	t := me.T()
	ds1 := From("test")

	b := ds1.Where(I("a").Gt(1)).Offset(10).ClearOffset()
	sql, args, err := b.Prepared(true).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1)})
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > ?)`)
}

func (me *datasetTest) TestPreparedGroupBy() {
	t := me.T()
	ds1 := From("test")

	b := ds1.Where(I("a").Gt(1)).GroupBy("created")
	sql, args, err := b.Prepared(true).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1)})
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > ?) GROUP BY "created"`)

	b = ds1.Where(I("a").Gt(1)).GroupBy(Literal("created::DATE"))
	sql, args, err = b.Prepared(true).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1)})
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > ?) GROUP BY created::DATE`)

	b = ds1.Where(I("a").Gt(1)).GroupBy("name", Literal("created::DATE"))
	sql, args, err = b.Prepared(true).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1)})
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("a" > ?) GROUP BY "name", created::DATE`)
}

func (me *datasetTest) TestPreparedHaving() {
	t := me.T()
	ds1 := From("test")

	b := ds1.Having(I("a").Gt(1)).GroupBy("created")
	sql, args, err := b.Prepared(true).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1)})
	assert.Equal(t, sql, `SELECT * FROM "test" GROUP BY "created" HAVING ("a" > ?)`)

	b = ds1.
		Where(I("b").IsTrue()).
		Having(I("a").Gt(1)).
		GroupBy("created")
	sql, args, err = b.Prepared(true).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1)})
	assert.Equal(t, sql, `SELECT * FROM "test" WHERE ("b" IS TRUE) GROUP BY "created" HAVING ("a" > ?)`)

	b = ds1.Having(I("a").Gt(1))
	sql, args, err = b.Prepared(true).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1)})
	assert.Equal(t, sql, `SELECT * FROM "test" HAVING ("a" > ?)`)
}

func (me *datasetTest) TestPreparedJoin() {
	t := me.T()
	ds1 := From("items")

	sql, args, err := ds1.Join(I("players").As("p"), On(I("p.id").Eq(I("items.playerId")))).Prepared(true).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{})
	assert.Equal(t, sql, `SELECT * FROM "items" INNER JOIN "players" AS "p" ON ("p"."id" = "items"."playerId")`)

	sql, args, err = ds1.Join(ds1.From("players").As("p"), On(I("p.id").Eq(I("items.playerId")))).Prepared(true).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{})
	assert.Equal(t, sql, `SELECT * FROM "items" INNER JOIN (SELECT * FROM "players") AS "p" ON ("p"."id" = "items"."playerId")`)

	sql, args, err = ds1.Join(I("v1").Table("test"), On(I("v1.test.id").Eq(I("items.playerId")))).Prepared(true).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{})
	assert.Equal(t, sql, `SELECT * FROM "items" INNER JOIN "v1"."test" ON ("v1"."test"."id" = "items"."playerId")`)

	sql, args, err = ds1.Join(I("test"), Using(I("name"), I("common_id"))).Prepared(true).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{})
	assert.Equal(t, sql, `SELECT * FROM "items" INNER JOIN "test" USING ("name", "common_id")`)

	sql, args, err = ds1.Join(I("test"), Using("name", "common_id")).Prepared(true).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{})
	assert.Equal(t, sql, `SELECT * FROM "items" INNER JOIN "test" USING ("name", "common_id")`)

	sql, args, err = ds1.Join(I("categories"), On(I("categories.categoryId").Eq(I("items.id")), I("categories.categoryId").In(1, 2, 3))).Prepared(true).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1), int64(2), int64(3)})
	assert.Equal(t, sql, `SELECT * FROM "items" INNER JOIN "categories" ON (("categories"."categoryId" = "items"."id") AND ("categories"."categoryId" IN (?, ?, ?)))`)

}

func (me *datasetTest) TestPreparedFunctionExpressionsInHaving() {
	t := me.T()
	ds1 := From("items")
	sql, args, err := ds1.GroupBy("name").Having(SUM("amount").Gt(0)).Prepared(true).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(0)})
	assert.Equal(t, sql, `SELECT * FROM "items" GROUP BY "name" HAVING (SUM("amount") > ?)`)
}

func (me *datasetTest) TestPreparedUnion() {
	t := me.T()
	a := From("invoice").Select("id", "amount").Where(I("amount").Gt(1000))
	b := From("invoice").Select("id", "amount").Where(I("amount").Lt(10))

	sql, args, err := a.Union(b).Prepared(true).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(10)})
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) UNION (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?))`)

	sql, args, err = a.Limit(1).Union(b).Prepared(true).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(1), int64(10)})
	assert.Equal(t, sql, `SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) LIMIT ?) AS "t1" UNION (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?))`)

	sql, args, err = a.Union(b.Limit(1)).Prepared(true).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(10), int64(1)})
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) UNION (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?) LIMIT ?) AS "t1")`)

	sql, args, err = a.Union(b).Union(b.Where(I("id").Lt(50))).Prepared(true).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(10), int64(10), int64(50)})
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) UNION (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?)) UNION (SELECT "id", "amount" FROM "invoice" WHERE (("amount" < ?) AND ("id" < ?)))`)

}

func (me *datasetTest) TestPreparedUnionAll() {
	t := me.T()
	a := From("invoice").Select("id", "amount").Where(I("amount").Gt(1000))
	b := From("invoice").Select("id", "amount").Where(I("amount").Lt(10))

	sql, args, err := a.UnionAll(b).Prepared(true).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(10)})
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) UNION ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?))`)

	sql, args, err = a.Limit(1).UnionAll(b).Prepared(true).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(1), int64(10)})
	assert.Equal(t, sql, `SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) LIMIT ?) AS "t1" UNION ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?))`)

	sql, args, err = a.UnionAll(b.Limit(1)).Prepared(true).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(10), int64(1)})
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) UNION ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?) LIMIT ?) AS "t1")`)

	sql, args, err = a.UnionAll(b).UnionAll(b.Where(I("id").Lt(50))).Prepared(true).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(10), int64(10), int64(50)})
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) UNION ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?)) UNION ALL (SELECT "id", "amount" FROM "invoice" WHERE (("amount" < ?) AND ("id" < ?)))`)
}

func (me *datasetTest) TestPreparedIntersect() {
	t := me.T()
	a := From("invoice").Select("id", "amount").Where(I("amount").Gt(1000))
	b := From("invoice").Select("id", "amount").Where(I("amount").Lt(10))

	sql, args, err := a.Intersect(b).Prepared(true).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(10)})
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) INTERSECT (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?))`)

	sql, args, err = a.Limit(1).Intersect(b).Prepared(true).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(1), int64(10)})
	assert.Equal(t, sql, `SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) LIMIT ?) AS "t1" INTERSECT (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?))`)

	sql, args, err = a.Intersect(b.Limit(1)).Prepared(true).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(10), int64(1)})
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) INTERSECT (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?) LIMIT ?) AS "t1")`)

}

func (me *datasetTest) TestPreparedIntersectAll() {
	t := me.T()
	a := From("invoice").Select("id", "amount").Where(I("amount").Gt(1000))
	b := From("invoice").Select("id", "amount").Where(I("amount").Lt(10))

	sql, args, err := a.IntersectAll(b).Prepared(true).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(10)})
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) INTERSECT ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?))`)

	sql, args, err = a.Limit(1).IntersectAll(b).Prepared(true).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(1), int64(10)})
	assert.Equal(t, sql, `SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) LIMIT ?) AS "t1" INTERSECT ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?))`)

	sql, args, err = a.IntersectAll(b.Limit(1)).Prepared(true).ToSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(10), int64(1)})
	assert.Equal(t, sql, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) INTERSECT ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?) LIMIT ?) AS "t1")`)

}
