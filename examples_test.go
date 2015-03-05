package goqu

import (
	"fmt"
	"regexp"
)

func ExampleOr() {
	sql, _ := From("test").Where(
		Or(
			I("a").Gt(10),
			I("a").Lt(5),
		),
	).Sql()
	fmt.Println(sql)
	// Output: SELECT * FROM "test" WHERE (("a" > 10) OR ("a" < 5))
}

func ExampleOr_withAnd() {
	sql, _ := From("items").Where(
		Or(
			I("a").Gt(10),
			And(
				I("b").Eq(100),
				I("c").Neq("test"),
			),
		),
	).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "items" WHERE (("a" > 10) OR (("b" = 100) AND ("c" != 'test')))
}

func ExampleAnd() {
	//by default Where assumes an And
	sql, _ := From("test").Where(
		I("a").Gt(10),
		I("b").Lt(5),
	).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" WHERE (("a" > 10) AND ("b" < 5))
}

func ExampleAnd_withOr() {
	sql, _ := From("test").Where(
		I("a").Gt(10),
		Or(
			I("b").Lt(5),
			I("c").In([]string{"hello", "world"}),
		),
	).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" WHERE (("a" > 10) AND (("b" < 5) OR ("c" IN ('hello', 'world'))))
}

func ExampleI() {
	sql, _ := From("test").Where(
		I("a").Eq(10),
		I("b").Lt(10),
		I("d").IsTrue(),
	).Sql()
	fmt.Println(sql)

	//qualify with schema
	sql, _ = From(I("test").Schema("my_schema")).Sql()
	fmt.Println(sql)

	sql, _ = From(I("mychema.test")).Where(
		//qualify with schema, table, and col
		I("my_schema.test.a").Eq(10),
	).Sql()
	fmt.Println(sql)

	//* will be taken literally and no quoted
	sql, _ = From(I("test")).Select(I("test.*")).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" WHERE (("a" = 10) AND ("b" < 10) AND ("d" IS TRUE))
	// SELECT * FROM "my_schema"."test"
	// SELECT * FROM "mychema"."test" WHERE ("my_schema"."test"."a" = 10)
	// SELECT "test".* FROM "test"

}

func ExampleAliasMethods() {
	sql, _ := From("test").Select(I("a").As("as_a")).Sql()
	fmt.Println(sql)

	sql, _ = From("test").Select(COUNT("*").As("count")).Sql()
	fmt.Println(sql)

	sql, _ = From("test").Select(L("sum(amount)").As("total_amount")).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT "a" AS "as_a" FROM "test"
	// SELECT COUNT(*) AS "count" FROM "test"
	// SELECT sum(amount) AS "total_amount" FROM "test"

}

func ExampleComparisonMethods() {
	sql, _ := From("test").Where(I("a").Eq(10)).Sql()
	fmt.Println(sql)

	sql, _ = From("test").Where(I("a").Neq(10)).Sql()
	fmt.Println(sql)

	sql, _ = From("test").Where(I("a").Gt(10)).Sql()
	fmt.Println(sql)

	sql, _ = From("test").Where(I("a").Gte(10)).Sql()
	fmt.Println(sql)

	sql, _ = From("test").Where(I("a").Lt(10)).Sql()
	fmt.Println(sql)

	sql, _ = From("test").Where(I("a").Lte(10)).Sql()
	fmt.Println(sql)

	sql, _ = From("test").Where(L("(a + b)").Eq(10)).Sql()
	fmt.Println(sql)

	sql, _ = From("test").Where(L("(a + b)").Neq(10)).Sql()
	fmt.Println(sql)

	sql, _ = From("test").Where(L("(a + b)").Gt(10)).Sql()
	fmt.Println(sql)

	sql, _ = From("test").Where(L("(a + b)").Gte(10)).Sql()
	fmt.Println(sql)

	sql, _ = From("test").Where(L("(a + b)").Lt(10)).Sql()
	fmt.Println(sql)

	sql, _ = From("test").Where(L("(a + b)").Lte(10)).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" WHERE ("a" = 10)
	// SELECT * FROM "test" WHERE ("a" != 10)
	// SELECT * FROM "test" WHERE ("a" > 10)
	// SELECT * FROM "test" WHERE ("a" >= 10)
	// SELECT * FROM "test" WHERE ("a" < 10)
	// SELECT * FROM "test" WHERE ("a" <= 10)
	// SELECT * FROM "test" WHERE ((a + b) = 10)
	// SELECT * FROM "test" WHERE ((a + b) != 10)
	// SELECT * FROM "test" WHERE ((a + b) > 10)
	// SELECT * FROM "test" WHERE ((a + b) >= 10)
	// SELECT * FROM "test" WHERE ((a + b) < 10)
	// SELECT * FROM "test" WHERE ((a + b) <= 10)
}

func ExampleInMethods() {
	sql, _ := From("test").Where(I("a").In("a", "b", "c")).Sql()
	fmt.Println(sql)

	//with a slice
	sql, _ = From("test").Where(I("a").In([]string{"a", "b", "c"})).Sql()
	fmt.Println(sql)

	sql, _ = From("test").Where(I("a").NotIn("a", "b", "c")).Sql()
	fmt.Println(sql)

	//with a slice
	sql, _ = From("test").Where(I("a").NotIn([]string{"a", "b", "c"})).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" WHERE ("a" IN ('a', 'b', 'c'))
	// SELECT * FROM "test" WHERE ("a" IN ('a', 'b', 'c'))
	// SELECT * FROM "test" WHERE ("a" NOT IN ('a', 'b', 'c'))
	// SELECT * FROM "test" WHERE ("a" NOT IN ('a', 'b', 'c'))
}

func ExampleOrderedMethods() {
	sql, _ := From("test").Order(I("a").Asc()).Sql()
	fmt.Println(sql)

	sql, _ = From("test").Order(I("a").Desc()).Sql()
	fmt.Println(sql)

	sql, _ = From("test").Order(I("a").Desc().NullsFirst()).Sql()
	fmt.Println(sql)

	sql, _ = From("test").Order(I("a").Desc().NullsLast()).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" ORDER BY "a" ASC
	// SELECT * FROM "test" ORDER BY "a" DESC
	// SELECT * FROM "test" ORDER BY "a" DESC NULLS FIRST
	// SELECT * FROM "test" ORDER BY "a" DESC NULLS LAST
}

func ExampleStringMethods() {
	sql, _ := From("test").Where(I("a").Like("%a%")).Sql()
	fmt.Println(sql)

	sql, _ = From("test").Where(I("a").Like(regexp.MustCompile("(a|b)"))).Sql()
	fmt.Println(sql)

	sql, _ = From("test").Where(I("a").NotLike("%a%")).Sql()
	fmt.Println(sql)

	sql, _ = From("test").Where(I("a").NotLike(regexp.MustCompile("(a|b)"))).Sql()
	fmt.Println(sql)

	sql, _ = From("test").Where(I("a").ILike("%a%")).Sql()
	fmt.Println(sql)

	sql, _ = From("test").Where(I("a").ILike(regexp.MustCompile("(a|b)"))).Sql()
	fmt.Println(sql)

	sql, _ = From("test").Where(I("a").NotILike("%a%")).Sql()
	fmt.Println(sql)

	sql, _ = From("test").Where(I("a").NotILike(regexp.MustCompile("(a|b)"))).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" WHERE ("a" LIKE '%a%')
	// SELECT * FROM "test" WHERE ("a" ~ '(a|b)')
	// SELECT * FROM "test" WHERE ("a" NOT LIKE '%a%')
	// SELECT * FROM "test" WHERE ("a" !~ '(a|b)')
	// SELECT * FROM "test" WHERE ("a" ILIKE '%a%')
	// SELECT * FROM "test" WHERE ("a" ~* '(a|b)')
	// SELECT * FROM "test" WHERE ("a" NOT ILIKE '%a%')
	// SELECT * FROM "test" WHERE ("a" !~* '(a|b)')
}

func ExampleBooleanMethods() {
	sql, _ := From("test").Where(I("a").Is(nil)).Sql()
	fmt.Println(sql)

	sql, _ = From("test").Where(I("a").Is(true)).Sql()
	fmt.Println(sql)

	sql, _ = From("test").Where(I("a").Is(false)).Sql()
	fmt.Println(sql)

	sql, _ = From("test").Where(I("a").IsNot(nil)).Sql()
	fmt.Println(sql)

	sql, _ = From("test").Where(I("a").IsNot(true)).Sql()
	fmt.Println(sql)

	sql, _ = From("test").Where(I("a").IsNull(), I("b").IsNull()).Sql()
	fmt.Println(sql)

	sql, _ = From("test").Where(I("a").IsTrue(), I("b").IsNotTrue()).Sql()
	fmt.Println(sql)

	sql, _ = From("test").Where(I("a").IsFalse(), I("b").IsNotFalse()).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" WHERE ("a" IS NULL)
	// SELECT * FROM "test" WHERE ("a" IS TRUE)
	// SELECT * FROM "test" WHERE ("a" IS FALSE)
	// SELECT * FROM "test" WHERE ("a" IS NOT NULL)
	// SELECT * FROM "test" WHERE ("a" IS NOT TRUE)
	// SELECT * FROM "test" WHERE (("a" IS NULL) AND ("b" IS NULL))
	// SELECT * FROM "test" WHERE (("a" IS TRUE) AND ("b" IS NOT TRUE))
	// SELECT * FROM "test" WHERE (("a" IS FALSE) AND ("b" IS NOT FALSE))
}

func ExampleCastMethods() {
	sql, _ := From("test").Where(I("json1").Cast("TEXT").Neq(I("json2").Cast("TEXT"))).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" WHERE (CAST("json1" AS TEXT) != CAST("json2" AS TEXT))
}

func ExampleCast() {
	sql, _ := From("test").Where(I("json1").Cast("TEXT").Neq(I("json2").Cast("TEXT"))).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" WHERE (CAST("json1" AS TEXT) != CAST("json2" AS TEXT))
}

func ExampleDistinctMethods() {
	sql, _ := From("test").Select(COUNT(I("a").Distinct())).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT COUNT(DISTINCT("a")) FROM "test"
}

func ExampleL() {
	sql, _ := From("test").Where(L("a = 1")).Sql()
	fmt.Println(sql)

	sql, _ = From("test").Where(L("a = 1 AND (b = ? OR ? = ?)", "a", I("c"), 0.01)).Sql()
	fmt.Println(sql)

	sql, _ = From("test").Where(L("(? AND ?) OR ?", I("a").Eq(1), I("b").Eq("b"), I("c").In([]string{"a", "b", "c"}))).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" WHERE a = 1
	// SELECT * FROM "test" WHERE a = 1 AND (b = 'a' OR "c" = 0.01)
	// SELECT * FROM "test" WHERE (("a" = 1) AND ("b" = 'b')) OR ("c" IN ('a', 'b', 'c'))
}

func ExampleOn() {
	sql, _ := From("test").Join(I("my_table"), On(I("my_table.fkey").Eq(I("test.id")))).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" INNER JOIN "my_table" ON ("my_table"."fkey" = "test"."id")
}

func ExampleUsing() {
	sql, _ := From("test").Join(I("my_table"), Using(I("common_column"))).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" INNER JOIN "my_table" USING ("common_column")
}

func ExampleDataset_ToUpdateSql() {
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, args, _ := From("items").ToUpdateSql(false, item{Name: "Test", Address: "111 Test Addr"})
	fmt.Printf("\n%s %+v)", sql, args)

	sql, args, _ = From("items").ToUpdateSql(true, item{Name: "Test", Address: "111 Test Addr"})
	fmt.Printf("\n%s %+v)", sql, args)

	sql, args, _ = From("items").ToUpdateSql(false, Record{"name": "Test", "address": "111 Test Addr"})
	fmt.Printf("\n%s %+v)", sql, args)

	sql, args, _ = From("items").ToUpdateSql(true, Record{"name": "Test", "address": "111 Test Addr"})
	fmt.Printf("\n%s %+v)", sql, args)

	sql, args, _ = From("items").ToUpdateSql(false, map[string]interface{}{"name": "Test", "address": "111 Test Addr"})
	fmt.Printf("\n%s %+v)", sql, args)

	sql, args, _ = From("items").ToUpdateSql(true, map[string]interface{}{"name": "Test", "address": "111 Test Addr"})
	fmt.Printf("\n%s %+v)", sql, args)
	// Output:
	// UPDATE "items" SET "address"='111 Test Addr',"name"='Test' [])
	// UPDATE "items" SET "address"=?,"name"=? [111 Test Addr Test])
	// UPDATE "items" SET "address"='111 Test Addr',"name"='Test' [])
	// UPDATE "items" SET "address"=?,"name"=? [111 Test Addr Test])
	// UPDATE "items" SET "address"='111 Test Addr',"name"='Test' [])
	// UPDATE "items" SET "address"=?,"name"=? [111 Test Addr Test])
}

func ExampleDataset_UpdateSql() {
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, _ := From("items").UpdateSql(item{Name: "Test", Address: "111 Test Addr"})
	fmt.Println(sql)

	sql, _ = From("items").UpdateSql(Record{"name": "Test", "address": "111 Test Addr"})
	fmt.Println(sql)
	// Output:
	// UPDATE "items" SET "address"='111 Test Addr',"name"='Test'
	// UPDATE "items" SET "address"='111 Test Addr',"name"='Test'
}

func ExampleDataset_ToSql() {
	sql, args, _ := From("items").Where(I("a").Eq(1)).ToSql(false)
	fmt.Printf("\n%s %+v)", sql, args)
	// Output:

	sql, args, _ = From("items").Where(I("a").Eq(1)).ToSql(true)
	fmt.Printf("\n%s %+v)", sql, args)
	// Output:
	// SELECT * FROM "items" WHERE ("a" = 1) [])
	// SELECT * FROM "items" WHERE ("a" = ?) [1])
}

func ExampleDataset_Sql() {
	sql, _ := From("items").Where(I("a").Eq(1)).Sql()
	fmt.Println(sql)
	// Output: SELECT * FROM "items" WHERE ("a" = 1)
}

func ExampleDataset_As() {
	ds := From("test").As("t")
	sql, _ := From(ds).Sql()
	fmt.Println(sql)
	// Output: SELECT * FROM (SELECT * FROM "test") AS "t"
}

func ExampleDataset_Returning() {
	sql, _ := From("test").Returning("id").InsertSql(Record{"a": "a", "b": "b"})
	fmt.Println(sql)
	sql, _ = From("test").Returning(I("test.*")).InsertSql(Record{"a": "a", "b": "b"})
	fmt.Println(sql)
	sql, _ = From("test").Returning("a", "b").InsertSql(Record{"a": "a", "b": "b"})
	fmt.Println(sql)
	// Output:
	// INSERT INTO "test" ("a", "b") VALUES ('a', 'b') RETURNING "id"
	// INSERT INTO "test" ("a", "b") VALUES ('a', 'b') RETURNING "test".*
	// INSERT INTO "test" ("a", "b") VALUES ('a', 'b') RETURNING "a", "b"
}

func ExampleDataset_Union() {
	sql, _ := From("test").Union(From("test2")).Sql()
	fmt.Println(sql)
	sql, _ = From("test").Limit(1).Union(From("test2")).Sql()
	fmt.Println(sql)
	sql, _ = From("test").Limit(1).Union(From("test2").Order(I("id").Desc())).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" UNION (SELECT * FROM "test2")
	// SELECT * FROM (SELECT * FROM "test" LIMIT 1) AS "t1" UNION (SELECT * FROM "test2")
	// SELECT * FROM (SELECT * FROM "test" LIMIT 1) AS "t1" UNION (SELECT * FROM (SELECT * FROM "test2" ORDER BY "id" DESC) AS "t1")
}

func ExampleDataset_UnionAll() {
	sql, _ := From("test").UnionAll(From("test2")).Sql()
	fmt.Println(sql)
	sql, _ = From("test").Limit(1).UnionAll(From("test2")).Sql()
	fmt.Println(sql)
	sql, _ = From("test").Limit(1).UnionAll(From("test2").Order(I("id").Desc())).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" UNION ALL (SELECT * FROM "test2")
	// SELECT * FROM (SELECT * FROM "test" LIMIT 1) AS "t1" UNION ALL (SELECT * FROM "test2")
	// SELECT * FROM (SELECT * FROM "test" LIMIT 1) AS "t1" UNION ALL (SELECT * FROM (SELECT * FROM "test2" ORDER BY "id" DESC) AS "t1")
}

func ExampleDataset_Intersect() {
	sql, _ := From("test").Intersect(From("test2")).Sql()
	fmt.Println(sql)
	sql, _ = From("test").Limit(1).Intersect(From("test2")).Sql()
	fmt.Println(sql)
	sql, _ = From("test").Limit(1).Intersect(From("test2").Order(I("id").Desc())).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" INTERSECT (SELECT * FROM "test2")
	// SELECT * FROM (SELECT * FROM "test" LIMIT 1) AS "t1" INTERSECT (SELECT * FROM "test2")
	// SELECT * FROM (SELECT * FROM "test" LIMIT 1) AS "t1" INTERSECT (SELECT * FROM (SELECT * FROM "test2" ORDER BY "id" DESC) AS "t1")
}

func ExampleDataset_IntersectAll() {
	sql, _ := From("test").IntersectAll(From("test2")).Sql()
	fmt.Println(sql)
	sql, _ = From("test").Limit(1).IntersectAll(From("test2")).Sql()
	fmt.Println(sql)
	sql, _ = From("test").Limit(1).IntersectAll(From("test2").Order(I("id").Desc())).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" INTERSECT ALL (SELECT * FROM "test2")
	// SELECT * FROM (SELECT * FROM "test" LIMIT 1) AS "t1" INTERSECT ALL (SELECT * FROM "test2")
	// SELECT * FROM (SELECT * FROM "test" LIMIT 1) AS "t1" INTERSECT ALL (SELECT * FROM (SELECT * FROM "test2" ORDER BY "id" DESC) AS "t1")
}

func ExampleDataset_ClearOffset() {
	ds := From("test").Offset(2)
	sql, _ := ds.ClearOffset().Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test"
}

func ExampleDataset_Offset() {
	ds := From("test").Offset(2)
	sql, _ := ds.Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" OFFSET 2
}

func ExampleDataset_Limit() {
	ds := From("test").Limit(10)
	sql, _ := ds.Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" LIMIT 10
}

func ExampleDataset_LimitAll() {
	ds := From("test").LimitAll()
	sql, _ := ds.Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" LIMIT ALL
}

func ExampleDataset_ClearLimit() {
	ds := From("test").Limit(10)
	sql, _ := ds.ClearLimit().Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test"
}

func ExampleDataset_Order() {
	ds := From("test").Order(I("a").Asc())
	sql, _ := ds.Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" ORDER BY "a" ASC
}

func ExampleDataset_OrderAppend() {
	ds := From("test").Order(I("a").Asc())
	sql, _ := ds.OrderAppend(I("b").Desc().NullsLast()).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" ORDER BY "a" ASC, "b" DESC NULLS LAST
}

func ExampleDataset_ClearOrder() {
	ds := From("test").Order(I("a").Asc())
	sql, _ := ds.ClearOrder().Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test"
}

func ExampleDataset_Having() {
	sql, _ := From("test").Having(SUM("income").Gt(1000)).Sql()
	fmt.Println(sql)
	sql, _ = From("test").GroupBy("age").Having(SUM("income").Gt(1000)).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" HAVING (SUM("income") > 1000)
	// SELECT * FROM "test" GROUP BY "age" HAVING (SUM("income") > 1000)
}

func ExampleDataset_Where() {
	//By default everyting is added together
	sql, _ := From("test").Where(
		I("a").Gt(10),
		I("b").Lt(10),
		I("c").IsNull(),
	).Sql()
	fmt.Println(sql)

	//You can use a combination of Ors and Ands
	sql, _ = From("test").Where(
		Or(
			I("a").Gt(10),
			And(
				I("b").Lt(10),
				I("c").IsNull(),
			),
		),
	).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" WHERE (("a" > 10) AND ("b" < 10) AND ("c" IS NULL))
	// SELECT * FROM "test" WHERE (("a" > 10) OR (("b" < 10) AND ("c" IS NULL)))
}

func ExampleDataset_ClearWhere() {
	ds := From("test").Where(
		Or(
			I("a").Gt(10),
			And(
				I("b").Lt(10),
				I("c").IsNull(),
			),
		),
	)
	sql, _ := ds.ClearWhere().Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test"
}

func ExampleDataset_Join() {
	sql, _ := From("test").Join(I("test2"), On(I("test.fkey").Eq(I("test2.Id")))).Sql()
	fmt.Println(sql)
	sql, _ = From("test").Join(I("test2"), Using("common_column")).Sql()
	fmt.Println(sql)
	sql, _ = From("test").Join(From("test2").Where(I("amount").Gt(0)), On(I("test.fkey").Eq(I("test2.Id")))).Sql()
	fmt.Println(sql)
	sql, _ = From("test").Join(From("test2").Where(I("amount").Gt(0)).As("t"), On(I("test.fkey").Eq(I("t.Id")))).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" INNER JOIN "test2" ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" INNER JOIN "test2" USING ("common_column")
	// SELECT * FROM "test" INNER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" INNER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t" ON ("test"."fkey" = "t"."Id")

}

func ExampleDataset_InnerJoin() {
	sql, _ := From("test").InnerJoin(I("test2"), On(I("test.fkey").Eq(I("test2.Id")))).Sql()
	fmt.Println(sql)
	sql, _ = From("test").InnerJoin(I("test2"), Using("common_column")).Sql()
	fmt.Println(sql)
	sql, _ = From("test").InnerJoin(From("test2").Where(I("amount").Gt(0)), On(I("test.fkey").Eq(I("test2.Id")))).Sql()
	fmt.Println(sql)
	sql, _ = From("test").InnerJoin(From("test2").Where(I("amount").Gt(0)).As("t"), On(I("test.fkey").Eq(I("t.Id")))).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" INNER JOIN "test2" ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" INNER JOIN "test2" USING ("common_column")
	// SELECT * FROM "test" INNER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" INNER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t" ON ("test"."fkey" = "t"."Id")
}
func ExampleDataset_FullOuterJoin() {
	sql, _ := From("test").FullOuterJoin(I("test2"), On(I("test.fkey").Eq(I("test2.Id")))).Sql()
	fmt.Println(sql)
	sql, _ = From("test").FullOuterJoin(I("test2"), Using("common_column")).Sql()
	fmt.Println(sql)
	sql, _ = From("test").FullOuterJoin(From("test2").Where(I("amount").Gt(0)), On(I("test.fkey").Eq(I("test2.Id")))).Sql()
	fmt.Println(sql)
	sql, _ = From("test").FullOuterJoin(From("test2").Where(I("amount").Gt(0)).As("t"), On(I("test.fkey").Eq(I("t.Id")))).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" FULL OUTER JOIN "test2" ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" FULL OUTER JOIN "test2" USING ("common_column")
	// SELECT * FROM "test" FULL OUTER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" FULL OUTER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t" ON ("test"."fkey" = "t"."Id")
}
func ExampleDataset_RightOuterJoin() {
	sql, _ := From("test").RightOuterJoin(I("test2"), On(I("test.fkey").Eq(I("test2.Id")))).Sql()
	fmt.Println(sql)
	sql, _ = From("test").RightOuterJoin(I("test2"), Using("common_column")).Sql()
	fmt.Println(sql)
	sql, _ = From("test").RightOuterJoin(From("test2").Where(I("amount").Gt(0)), On(I("test.fkey").Eq(I("test2.Id")))).Sql()
	fmt.Println(sql)
	sql, _ = From("test").RightOuterJoin(From("test2").Where(I("amount").Gt(0)).As("t"), On(I("test.fkey").Eq(I("t.Id")))).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" RIGHT OUTER JOIN "test2" ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" RIGHT OUTER JOIN "test2" USING ("common_column")
	// SELECT * FROM "test" RIGHT OUTER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" RIGHT OUTER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t" ON ("test"."fkey" = "t"."Id")
}
func ExampleDataset_LeftOuterJoin() {
	sql, _ := From("test").LeftOuterJoin(I("test2"), On(I("test.fkey").Eq(I("test2.Id")))).Sql()
	fmt.Println(sql)
	sql, _ = From("test").LeftOuterJoin(I("test2"), Using("common_column")).Sql()
	fmt.Println(sql)
	sql, _ = From("test").LeftOuterJoin(From("test2").Where(I("amount").Gt(0)), On(I("test.fkey").Eq(I("test2.Id")))).Sql()
	fmt.Println(sql)
	sql, _ = From("test").LeftOuterJoin(From("test2").Where(I("amount").Gt(0)).As("t"), On(I("test.fkey").Eq(I("t.Id")))).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" LEFT OUTER JOIN "test2" ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" LEFT OUTER JOIN "test2" USING ("common_column")
	// SELECT * FROM "test" LEFT OUTER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" LEFT OUTER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t" ON ("test"."fkey" = "t"."Id")
}
func ExampleDataset_FullJoin() {
	sql, _ := From("test").FullJoin(I("test2"), On(I("test.fkey").Eq(I("test2.Id")))).Sql()
	fmt.Println(sql)
	sql, _ = From("test").FullJoin(I("test2"), Using("common_column")).Sql()
	fmt.Println(sql)
	sql, _ = From("test").FullJoin(From("test2").Where(I("amount").Gt(0)), On(I("test.fkey").Eq(I("test2.Id")))).Sql()
	fmt.Println(sql)
	sql, _ = From("test").FullJoin(From("test2").Where(I("amount").Gt(0)).As("t"), On(I("test.fkey").Eq(I("t.Id")))).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" FULL JOIN "test2" ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" FULL JOIN "test2" USING ("common_column")
	// SELECT * FROM "test" FULL JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" FULL JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t" ON ("test"."fkey" = "t"."Id")
}
func ExampleDataset_RightJoin() {
	sql, _ := From("test").RightJoin(I("test2"), On(I("test.fkey").Eq(I("test2.Id")))).Sql()
	fmt.Println(sql)
	sql, _ = From("test").RightJoin(I("test2"), Using("common_column")).Sql()
	fmt.Println(sql)
	sql, _ = From("test").RightJoin(From("test2").Where(I("amount").Gt(0)), On(I("test.fkey").Eq(I("test2.Id")))).Sql()
	fmt.Println(sql)
	sql, _ = From("test").RightJoin(From("test2").Where(I("amount").Gt(0)).As("t"), On(I("test.fkey").Eq(I("t.Id")))).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" RIGHT JOIN "test2" ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" RIGHT JOIN "test2" USING ("common_column")
	// SELECT * FROM "test" RIGHT JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" RIGHT JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t" ON ("test"."fkey" = "t"."Id")
}
func ExampleDataset_LeftJoin() {
	sql, _ := From("test").LeftJoin(I("test2"), On(I("test.fkey").Eq(I("test2.Id")))).Sql()
	fmt.Println(sql)
	sql, _ = From("test").LeftJoin(I("test2"), Using("common_column")).Sql()
	fmt.Println(sql)
	sql, _ = From("test").LeftJoin(From("test2").Where(I("amount").Gt(0)), On(I("test.fkey").Eq(I("test2.Id")))).Sql()
	fmt.Println(sql)
	sql, _ = From("test").LeftJoin(From("test2").Where(I("amount").Gt(0)).As("t"), On(I("test.fkey").Eq(I("t.Id")))).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" LEFT JOIN "test2" ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" LEFT JOIN "test2" USING ("common_column")
	// SELECT * FROM "test" LEFT JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" LEFT JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t" ON ("test"."fkey" = "t"."Id")
}
func ExampleDataset_NaturalJoin() {
	sql, _ := From("test").NaturalJoin(I("test2")).Sql()
	fmt.Println(sql)
	sql, _ = From("test").NaturalJoin(From("test2").Where(I("amount").Gt(0))).Sql()
	fmt.Println(sql)
	sql, _ = From("test").NaturalJoin(From("test2").Where(I("amount").Gt(0)).As("t")).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" NATURAL JOIN "test2"
	// SELECT * FROM "test" NATURAL JOIN (SELECT * FROM "test2" WHERE ("amount" > 0))
	// SELECT * FROM "test" NATURAL JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t"
}
func ExampleDataset_NaturalLeftJoin() {
	sql, _ := From("test").NaturalLeftJoin(I("test2")).Sql()
	fmt.Println(sql)
	sql, _ = From("test").NaturalLeftJoin(From("test2").Where(I("amount").Gt(0))).Sql()
	fmt.Println(sql)
	sql, _ = From("test").NaturalLeftJoin(From("test2").Where(I("amount").Gt(0)).As("t")).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" NATURAL LEFT JOIN "test2"
	// SELECT * FROM "test" NATURAL LEFT JOIN (SELECT * FROM "test2" WHERE ("amount" > 0))
	// SELECT * FROM "test" NATURAL LEFT JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t"
}
func ExampleDataset_NaturalRightJoin() {
	sql, _ := From("test").NaturalRightJoin(I("test2")).Sql()
	fmt.Println(sql)
	sql, _ = From("test").NaturalRightJoin(From("test2").Where(I("amount").Gt(0))).Sql()
	fmt.Println(sql)
	sql, _ = From("test").NaturalRightJoin(From("test2").Where(I("amount").Gt(0)).As("t")).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" NATURAL RIGHT JOIN "test2"
	// SELECT * FROM "test" NATURAL RIGHT JOIN (SELECT * FROM "test2" WHERE ("amount" > 0))
	// SELECT * FROM "test" NATURAL RIGHT JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t"
}
func ExampleDataset_NaturalFullJoin() {
	sql, _ := From("test").NaturalFullJoin(I("test2")).Sql()
	fmt.Println(sql)
	sql, _ = From("test").NaturalFullJoin(From("test2").Where(I("amount").Gt(0))).Sql()
	fmt.Println(sql)
	sql, _ = From("test").NaturalFullJoin(From("test2").Where(I("amount").Gt(0)).As("t")).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" NATURAL FULL JOIN "test2"
	// SELECT * FROM "test" NATURAL FULL JOIN (SELECT * FROM "test2" WHERE ("amount" > 0))
	// SELECT * FROM "test" NATURAL FULL JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t"
}

func ExampleDataset_CrossJoin() {
	sql, _ := From("test").CrossJoin(I("test2")).Sql()
	fmt.Println(sql)
	sql, _ = From("test").CrossJoin(From("test2").Where(I("amount").Gt(0))).Sql()
	fmt.Println(sql)
	sql, _ = From("test").CrossJoin(From("test2").Where(I("amount").Gt(0)).As("t")).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" CROSS JOIN "test2"
	// SELECT * FROM "test" CROSS JOIN (SELECT * FROM "test2" WHERE ("amount" > 0))
	// SELECT * FROM "test" CROSS JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t"
}

func ExampleDataset_FromSelf() {
	sql, _ := From("test").FromSelf().Sql()
	fmt.Println(sql)
	sql, _ = From("test").As("my_test_table").FromSelf().Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM (SELECT * FROM "test") AS "t1"
	// SELECT * FROM (SELECT * FROM "test") AS "my_test_table"
}

func ExampleDataset_From() {
	ds := From("test")
	sql, _ := ds.From("test2").Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test2"
}

func ExampleDataset_From_withDataset() {
	ds := From("test")
	fromDs := ds.Where(I("age").Gt(10))
	sql, _ := ds.From(fromDs).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM (SELECT * FROM "test" WHERE ("age" > 10)) AS "t1"
}

func ExampleDataset_From_withAliasedDataset() {
	ds := From("test")
	fromDs := ds.Where(I("age").Gt(10))
	sql, _ := ds.From(fromDs.As("test2")).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM (SELECT * FROM "test" WHERE ("age" > 10)) AS "test2"
}

func ExampleDataset_Select() {
	sql, _ := From("test").Select("a", "b", "c").Sql()
	fmt.Println(sql)
	// Output:
	// SELECT "a", "b", "c" FROM "test"
}

func ExampleDataset_Select_withDataset() {
	ds := From("test")
	fromDs := ds.Select("age").Where(I("age").Gt(10))
	sql, _ := ds.From().Select(fromDs).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT (SELECT "age" FROM "test" WHERE ("age" > 10))
}

func ExampleDataset_Select_withAliasedDataset() {
	ds := From("test")
	fromDs := ds.Select("age").Where(I("age").Gt(10))
	sql, _ := ds.From().Select(fromDs.As("ages")).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT (SELECT "age" FROM "test" WHERE ("age" > 10)) AS "ages"
}

func ExampleDataset_Select_withLiteral() {
	sql, _ := From("test").Select(L("a + b").As("sum")).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT a + b AS "sum" FROM "test"
}

func ExampleDataset_Select_withSqlFunctionExpression() {
	sql, _ := From("test").Select(
		COUNT("*").As("age_count"),
		MAX("age").As("max_age"),
		AVG("age").As("avg_age"),
	).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT COUNT(*) AS "age_count", MAX("age") AS "max_age", AVG("age") AS "avg_age" FROM "test"
}

func ExampleDataset_SelectDistinct() {
	sql, _ := From("test").SelectDistinct("a", "b").Sql()
	fmt.Println(sql)
	// Output:
	// SELECT DISTINCT "a", "b" FROM "test"
}

func ExampleDataset_SelectAppend() {
	ds := From("test").Select("a", "b")
	sql, _ := ds.SelectAppend("c").Sql()
	fmt.Println(sql)
	ds = From("test").SelectDistinct("a", "b")
	sql, _ = ds.SelectAppend("c").Sql()
	fmt.Println(sql)
	// Output:
	// SELECT "a", "b", "c" FROM "test"
	// SELECT DISTINCT "a", "b", "c" FROM "test"
}

func ExampleDataset_ClearSelect() {
	ds := From("test").Select("a", "b")
	sql, _ := ds.ClearSelect().Sql()
	fmt.Println(sql)
	ds = From("test").SelectDistinct("a", "b")
	sql, _ = ds.ClearSelect().Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test"
	// SELECT * FROM "test"
}

func ExampleDataset_ToInsertSql() {
	type item struct {
		Id      uint32 `db:"id" goqu:"skipinsert"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, args, _ := From("items").ToInsertSql(
		false,
		item{Name: "Test1", Address: "111 Test Addr"},
		item{Name: "Test2", Address: "112 Test Addr"},
	)
	fmt.Printf("\n%s %+v", sql, args)

	sql, args, _ = From("items").ToInsertSql(
		false,
		Record{"name": "Test1", "address": "111 Test Addr"},
		Record{"name": "Test2", "address": "112 Test Addr"},
	)
	fmt.Printf("\n%s %+v", sql, args)

	sql, args, _ = From("items").ToInsertSql(
		false,
		[]item{
			{Name: "Test1", Address: "111 Test Addr"},
			{Name: "Test2", Address: "112 Test Addr"},
		})
	fmt.Printf("\n%s %+v", sql, args)

	sql, args, _ = From("items").ToInsertSql(
		false,
		[]Record{
			{"name": "Test1", "address": "111 Test Addr"},
			{"name": "Test2", "address": "112 Test Addr"},
		})

	sql, args, _ = From("items").ToInsertSql(
		true,
		item{Name: "Test1", Address: "111 Test Addr"},
		item{Name: "Test2", Address: "112 Test Addr"},
	)
	fmt.Printf("\n%s %+v", sql, args)

	sql, args, _ = From("items").ToInsertSql(
		true,
		Record{"name": "Test1", "address": "111 Test Addr"},
		Record{"name": "Test2", "address": "112 Test Addr"},
	)
	fmt.Printf("\n%s %+v", sql, args)

	sql, args, _ = From("items").ToInsertSql(
		true,
		[]item{
			{Name: "Test1", Address: "111 Test Addr"},
			{Name: "Test2", Address: "112 Test Addr"},
		})
	fmt.Printf("\n%s %+v", sql, args)

	sql, args, _ = From("items").ToInsertSql(
		true,
		[]Record{
			{"name": "Test1", "address": "111 Test Addr"},
			{"name": "Test2", "address": "112 Test Addr"},
		})
	fmt.Printf("\n%s %+v", sql, args)
	// Output:
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') []
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') []
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') []
	// INSERT INTO "items" ("address", "name") VALUES (?, ?), (?, ?) [111 Test Addr Test1 112 Test Addr Test2]
	// INSERT INTO "items" ("address", "name") VALUES (?, ?), (?, ?) [111 Test Addr Test1 112 Test Addr Test2]
	// INSERT INTO "items" ("address", "name") VALUES (?, ?), (?, ?) [111 Test Addr Test1 112 Test Addr Test2]
	// INSERT INTO "items" ("address", "name") VALUES (?, ?), (?, ?) [111 Test Addr Test1 112 Test Addr Test2]
}

func ExampleDataset_InsertSql() {
	type item struct {
		Id      uint32 `db:"id" goqu:"skipinsert"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, _ := From("items").InsertSql(
		item{Name: "Test1", Address: "111 Test Addr"},
		item{Name: "Test2", Address: "112 Test Addr"},
	)
	fmt.Println(sql)

	sql, _ = From("items").InsertSql(
		Record{"name": "Test1", "address": "111 Test Addr"},
		Record{"name": "Test2", "address": "112 Test Addr"},
	)
	fmt.Println(sql)

	sql, _ = From("items").InsertSql([]item{
		{Name: "Test1", Address: "111 Test Addr"},
		{Name: "Test2", Address: "112 Test Addr"},
	})
	fmt.Println(sql)

	sql, _ = From("items").InsertSql([]Record{
		{"name": "Test1", "address": "111 Test Addr"},
		{"name": "Test2", "address": "112 Test Addr"},
	})
	fmt.Println(sql)
	// Output:
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2')
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2')
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2')
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2')
}

func ExampleDataset_ToDeleteSql() {
	sql, args, _ := From("items").ToDeleteSql(false)
	fmt.Printf("\n%s %+v", sql, args)

	sql, args, _ = From("items").Where(I("id").Gt(10)).ToDeleteSql(false)
	fmt.Printf("\n%s %+v", sql, args)

	sql, args, _ = From("items").Where(I("id").Gt(10)).ToDeleteSql(true)
	fmt.Printf("\n%s %+v", sql, args)

	// Output:
	// DELETE FROM "items" []
	// DELETE FROM "items" WHERE ("id" > 10) []
	// DELETE FROM "items" WHERE ("id" > ?) [10]
}

func ExampleDataset_DeleteSql() {
	sql, _ := From("items").DeleteSql()
	fmt.Println(sql)

	sql, _ = From("items").Where(I("id").Gt(10)).DeleteSql()
	fmt.Println(sql)

	// Output:
	// DELETE FROM "items"
	// DELETE FROM "items" WHERE ("id" > 10)
}

func ExampleDataset_ToTruncateSql() {
	sql, args, _ := From("items").ToTruncateSql(false, TruncateOptions{})
	fmt.Printf("\n%s %+v", sql, args)
	sql, args, _ = From("items").ToTruncateSql(false, TruncateOptions{Cascade: true})
	fmt.Printf("\n%s %+v", sql, args)
	sql, args, _ = From("items").ToTruncateSql(false, TruncateOptions{Restrict: true})
	fmt.Printf("\n%s %+v", sql, args)
	sql, args, _ = From("items").ToTruncateSql(false, TruncateOptions{Identity: "RESTART"})
	fmt.Printf("\n%s %+v", sql, args)
	sql, args, _ = From("items").ToTruncateSql(false, TruncateOptions{Identity: "RESTART", Cascade: true})
	fmt.Printf("\n%s %+v", sql, args)
	sql, args, _ = From("items").ToTruncateSql(false, TruncateOptions{Identity: "RESTART", Restrict: true})
	fmt.Printf("\n%s %+v", sql, args)
	sql, args, _ = From("items").ToTruncateSql(false, TruncateOptions{Identity: "CONTINUE"})
	fmt.Printf("\n%s %+v", sql, args)
	sql, args, _ = From("items").ToTruncateSql(false, TruncateOptions{Identity: "CONTINUE", Cascade: true})
	fmt.Printf("\n%s %+v", sql, args)
	sql, args, _ = From("items").ToTruncateSql(false, TruncateOptions{Identity: "CONTINUE", Restrict: true})
	fmt.Printf("\n%s %+v", sql, args)
	// Output:
	// TRUNCATE "items" []
	// TRUNCATE "items" CASCADE []
	// TRUNCATE "items" RESTRICT []
	// TRUNCATE "items" RESTART IDENTITY []
	// TRUNCATE "items" RESTART IDENTITY CASCADE []
	// TRUNCATE "items" RESTART IDENTITY RESTRICT []
	// TRUNCATE "items" CONTINUE IDENTITY []
	// TRUNCATE "items" CONTINUE IDENTITY CASCADE []
	// TRUNCATE "items" CONTINUE IDENTITY RESTRICT []
}

func ExampleDataset_TruncateSql() {
	sql, _ := From("items").TruncateSql()
	fmt.Println(sql)
	// Output:
	// TRUNCATE "items"
}

func ExampleDataset_TruncateWithOptsSql() {
	sql, _ := From("items").TruncateWithOptsSql(TruncateOptions{})
	fmt.Println(sql)
	sql, _ = From("items").TruncateWithOptsSql(TruncateOptions{Cascade: true})
	fmt.Println(sql)
	sql, _ = From("items").TruncateWithOptsSql(TruncateOptions{Restrict: true})
	fmt.Println(sql)
	sql, _ = From("items").TruncateWithOptsSql(TruncateOptions{Identity: "RESTART"})
	fmt.Println(sql)
	sql, _ = From("items").TruncateWithOptsSql(TruncateOptions{Identity: "RESTART", Cascade: true})
	fmt.Println(sql)
	sql, _ = From("items").TruncateWithOptsSql(TruncateOptions{Identity: "RESTART", Restrict: true})
	fmt.Println(sql)
	sql, _ = From("items").TruncateWithOptsSql(TruncateOptions{Identity: "CONTINUE"})
	fmt.Println(sql)
	sql, _ = From("items").TruncateWithOptsSql(TruncateOptions{Identity: "CONTINUE", Cascade: true})
	fmt.Println(sql)
	sql, _ = From("items").TruncateWithOptsSql(TruncateOptions{Identity: "CONTINUE", Restrict: true})
	fmt.Println(sql)

	// Output:
	// TRUNCATE "items"
	// TRUNCATE "items" CASCADE
	// TRUNCATE "items" RESTRICT
	// TRUNCATE "items" RESTART IDENTITY
	// TRUNCATE "items" RESTART IDENTITY CASCADE
	// TRUNCATE "items" RESTART IDENTITY RESTRICT
	// TRUNCATE "items" CONTINUE IDENTITY
	// TRUNCATE "items" CONTINUE IDENTITY CASCADE
	// TRUNCATE "items" CONTINUE IDENTITY RESTRICT
}
