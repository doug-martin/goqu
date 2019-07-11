package goqu_test

import (
	"fmt"
	"regexp"

	"github.com/doug-martin/goqu/v7"
)

func ExampleDataset() {
	ds := goqu.From("test").
		Select(goqu.COUNT("*")).
		InnerJoin(goqu.T("test2"), goqu.On(goqu.I("test.fkey").Eq(goqu.I("test2.id")))).
		LeftJoin(goqu.T("test3"), goqu.On(goqu.I("test2.fkey").Eq(goqu.I("test3.id")))).
		Where(
			goqu.Ex{
				"test.name": goqu.Op{
					"like": regexp.MustCompile("^(a|b)"),
				},
				"test2.amount": goqu.Op{
					"isNot": nil,
				},
			},
			goqu.ExOr{
				"test3.id":     nil,
				"test3.status": []string{"passed", "active", "registered"},
			}).
		Order(goqu.I("test.created").Desc().NullsLast()).
		GroupBy(goqu.I("test.user_id")).
		Having(goqu.AVG("test3.age").Gt(10))

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// nolint:lll
	// Output:
	// SELECT COUNT(*) FROM "test" INNER JOIN "test2" ON ("test"."fkey" = "test2"."id") LEFT JOIN "test3" ON ("test2"."fkey" = "test3"."id") WHERE ((("test"."name" ~ '^(a|b)') AND ("test2"."amount" IS NOT NULL)) AND (("test3"."id" IS NULL) OR ("test3"."status" IN ('passed', 'active', 'registered')))) GROUP BY "test"."user_id" HAVING (AVG("test3"."age") > 10) ORDER BY "test"."created" DESC NULLS LAST []
	// SELECT COUNT(*) FROM "test" INNER JOIN "test2" ON ("test"."fkey" = "test2"."id") LEFT JOIN "test3" ON ("test2"."fkey" = "test3"."id") WHERE ((("test"."name" ~ ?) AND ("test2"."amount" IS NOT NULL)) AND (("test3"."id" IS NULL) OR ("test3"."status" IN (?, ?, ?)))) GROUP BY "test"."user_id" HAVING (AVG("test3"."age") > ?) ORDER BY "test"."created" DESC NULLS LAST [^(a|b) passed active registered 10]
}

func ExampleDataset_As() {
	ds := goqu.From("test").As("t")
	sql, _, _ := goqu.From(ds).ToSQL()
	fmt.Println(sql)
	// Output: SELECT * FROM (SELECT * FROM "test") AS "t"
}

func ExampleDataset_Returning() {
	sql, _, _ := goqu.From("test").
		Returning("id").
		ToInsertSQL(goqu.Record{"a": "a", "b": "b"})
	fmt.Println(sql)
	sql, _, _ = goqu.From("test").
		Returning(goqu.T("test").All()).
		ToInsertSQL(goqu.Record{"a": "a", "b": "b"})
	fmt.Println(sql)
	sql, _, _ = goqu.From("test").
		Returning("a", "b").
		ToInsertSQL(goqu.Record{"a": "a", "b": "b"})
	fmt.Println(sql)
	// Output:
	// INSERT INTO "test" ("a", "b") VALUES ('a', 'b') RETURNING "id"
	// INSERT INTO "test" ("a", "b") VALUES ('a', 'b') RETURNING "test".*
	// INSERT INTO "test" ("a", "b") VALUES ('a', 'b') RETURNING "a", "b"
}

func ExampleDataset_Union() {
	sql, _, _ := goqu.From("test").
		Union(goqu.From("test2")).
		ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").
		Limit(1).
		Union(goqu.From("test2")).
		ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").
		Limit(1).
		Union(goqu.From("test2").
			Order(goqu.C("id").Desc())).
		ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" UNION (SELECT * FROM "test2")
	// SELECT * FROM (SELECT * FROM "test" LIMIT 1) AS "t1" UNION (SELECT * FROM "test2")
	// SELECT * FROM (SELECT * FROM "test" LIMIT 1) AS "t1" UNION (SELECT * FROM (SELECT * FROM "test2" ORDER BY "id" DESC) AS "t1")
}

func ExampleDataset_UnionAll() {
	sql, _, _ := goqu.From("test").
		UnionAll(goqu.From("test2")).
		ToSQL()
	fmt.Println(sql)
	sql, _, _ = goqu.From("test").
		Limit(1).
		UnionAll(goqu.From("test2")).
		ToSQL()
	fmt.Println(sql)
	sql, _, _ = goqu.From("test").
		Limit(1).
		UnionAll(goqu.From("test2").
			Order(goqu.C("id").Desc())).
		ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" UNION ALL (SELECT * FROM "test2")
	// SELECT * FROM (SELECT * FROM "test" LIMIT 1) AS "t1" UNION ALL (SELECT * FROM "test2")
	// SELECT * FROM (SELECT * FROM "test" LIMIT 1) AS "t1" UNION ALL (SELECT * FROM (SELECT * FROM "test2" ORDER BY "id" DESC) AS "t1")
}

func ExampleDataset_With() {
	sql, _, _ := goqu.From("one").
		With("one", goqu.From().Select(goqu.L("1"))).
		Select(goqu.Star()).
		ToSQL()
	fmt.Println(sql)
	sql, _, _ = goqu.From("derived").
		With("intermed", goqu.From("test").Select(goqu.Star()).Where(goqu.C("x").Gte(5))).
		With("derived", goqu.From("intermed").Select(goqu.Star()).Where(goqu.C("x").Lt(10))).
		Select(goqu.Star()).
		ToSQL()
	fmt.Println(sql)
	sql, _, _ = goqu.From("multi").
		With("multi(x,y)", goqu.From().Select(goqu.L("1"), goqu.L("2"))).
		Select(goqu.C("x"), goqu.C("y")).
		ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").
		With("moved_rows", goqu.From("other").Where(goqu.C("date").Lt(123))).
		ToInsertSQL(goqu.From("moved_rows"))
	fmt.Println(sql)
	sql, _, _ = goqu.From("test").
		With("check_vals(val)", goqu.From().Select(goqu.L("123"))).
		Where(goqu.C("val").Eq(goqu.From("check_vals").Select("val"))).
		ToDeleteSQL()
	fmt.Println(sql)
	sql, _, _ = goqu.From("test").
		With("some_vals(val)", goqu.From().Select(goqu.L("123"))).
		Where(goqu.C("val").Eq(goqu.From("some_vals").Select("val"))).
		ToUpdateSQL(goqu.Record{"name": "Test"})
	fmt.Println(sql)

	// Output:
	// WITH one AS (SELECT 1) SELECT * FROM "one"
	// WITH intermed AS (SELECT * FROM "test" WHERE ("x" >= 5)), derived AS (SELECT * FROM "intermed" WHERE ("x" < 10)) SELECT * FROM "derived"
	// WITH multi(x,y) AS (SELECT 1, 2) SELECT "x", "y" FROM "multi"
	// WITH moved_rows AS (SELECT * FROM "other" WHERE ("date" < 123)) INSERT INTO "test" SELECT * FROM "moved_rows"
	// WITH check_vals(val) AS (SELECT 123) DELETE FROM "test" WHERE ("val" IN (SELECT "val" FROM "check_vals"))
	// WITH some_vals(val) AS (SELECT 123) UPDATE "test" SET "name"='Test' WHERE ("val" IN (SELECT "val" FROM "some_vals"))
}

func ExampleDataset_WithRecursive() {
	sql, _, _ := goqu.From("nums").
		WithRecursive("nums(x)",
			goqu.From().Select(goqu.L("1")).
				UnionAll(goqu.From("nums").
					Select(goqu.L("x+1")).Where(goqu.C("x").Lt(5)))).
		ToSQL()
	fmt.Println(sql)
	// Output:
	// WITH RECURSIVE nums(x) AS (SELECT 1 UNION ALL (SELECT x+1 FROM "nums" WHERE ("x" < 5))) SELECT * FROM "nums"
}

func ExampleDataset_Intersect() {
	sql, _, _ := goqu.From("test").
		Intersect(goqu.From("test2")).
		ToSQL()
	fmt.Println(sql)
	sql, _, _ = goqu.From("test").
		Limit(1).
		Intersect(goqu.From("test2")).
		ToSQL()
	fmt.Println(sql)
	sql, _, _ = goqu.From("test").
		Limit(1).
		Intersect(goqu.From("test2").
			Order(goqu.C("id").Desc())).
		ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" INTERSECT (SELECT * FROM "test2")
	// SELECT * FROM (SELECT * FROM "test" LIMIT 1) AS "t1" INTERSECT (SELECT * FROM "test2")
	// SELECT * FROM (SELECT * FROM "test" LIMIT 1) AS "t1" INTERSECT (SELECT * FROM (SELECT * FROM "test2" ORDER BY "id" DESC) AS "t1")
}

func ExampleDataset_IntersectAll() {
	sql, _, _ := goqu.From("test").
		IntersectAll(goqu.From("test2")).
		ToSQL()
	fmt.Println(sql)
	sql, _, _ = goqu.From("test").
		Limit(1).
		IntersectAll(goqu.From("test2")).
		ToSQL()
	fmt.Println(sql)
	sql, _, _ = goqu.From("test").
		Limit(1).
		IntersectAll(goqu.From("test2").
			Order(goqu.C("id").Desc())).
		ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" INTERSECT ALL (SELECT * FROM "test2")
	// SELECT * FROM (SELECT * FROM "test" LIMIT 1) AS "t1" INTERSECT ALL (SELECT * FROM "test2")
	// SELECT * FROM (SELECT * FROM "test" LIMIT 1) AS "t1" INTERSECT ALL (SELECT * FROM (SELECT * FROM "test2" ORDER BY "id" DESC) AS "t1")
}

func ExampleDataset_ClearOffset() {
	ds := goqu.From("test").
		Offset(2)
	sql, _, _ := ds.
		ClearOffset().
		ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test"
}

func ExampleDataset_Offset() {
	ds := goqu.From("test").
		Offset(2)
	sql, _, _ := ds.ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" OFFSET 2
}

func ExampleDataset_Limit() {
	ds := goqu.From("test").Limit(10)
	sql, _, _ := ds.ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" LIMIT 10
}

func ExampleDataset_LimitAll() {
	ds := goqu.From("test").LimitAll()
	sql, _, _ := ds.ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" LIMIT ALL
}

func ExampleDataset_ClearLimit() {
	ds := goqu.From("test").Limit(10)
	sql, _, _ := ds.ClearLimit().ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test"
}

func ExampleDataset_Order() {
	ds := goqu.From("test").
		Order(goqu.C("a").Asc())
	sql, _, _ := ds.ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" ORDER BY "a" ASC
}

func ExampleDataset_OrderAppend() {
	ds := goqu.From("test").Order(goqu.C("a").Asc())
	sql, _, _ := ds.OrderAppend(goqu.C("b").Desc().NullsLast()).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" ORDER BY "a" ASC, "b" DESC NULLS LAST
}

func ExampleDataset_OrderPrepend() {
	ds := goqu.From("test").Order(goqu.C("a").Asc())
	sql, _, _ := ds.OrderPrepend(goqu.C("b").Desc().NullsLast()).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" ORDER BY "b" DESC NULLS LAST, "a" ASC
}

func ExampleDataset_ClearOrder() {
	ds := goqu.From("test").Order(goqu.C("a").Asc())
	sql, _, _ := ds.ClearOrder().ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test"
}

func ExampleDataset_Having() {
	sql, _, _ := goqu.From("test").Having(goqu.SUM("income").Gt(1000)).ToSQL()
	fmt.Println(sql)
	sql, _, _ = goqu.From("test").GroupBy("age").Having(goqu.SUM("income").Gt(1000)).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" HAVING (SUM("income") > 1000)
	// SELECT * FROM "test" GROUP BY "age" HAVING (SUM("income") > 1000)
}

func ExampleDataset_Where() {
	// By default everything is anded together
	sql, _, _ := goqu.From("test").Where(goqu.Ex{
		"a": goqu.Op{"gt": 10},
		"b": goqu.Op{"lt": 10},
		"c": nil,
		"d": []string{"a", "b", "c"},
	}).ToSQL()
	fmt.Println(sql)
	// You can use ExOr to get ORed expressions together
	sql, _, _ = goqu.From("test").Where(goqu.ExOr{
		"a": goqu.Op{"gt": 10},
		"b": goqu.Op{"lt": 10},
		"c": nil,
		"d": []string{"a", "b", "c"},
	}).ToSQL()
	fmt.Println(sql)
	// You can use Or with Ex to Or multiple Ex maps together
	sql, _, _ = goqu.From("test").Where(
		goqu.Or(
			goqu.Ex{
				"a": goqu.Op{"gt": 10},
				"b": goqu.Op{"lt": 10},
			},
			goqu.Ex{
				"c": nil,
				"d": []string{"a", "b", "c"},
			},
		),
	).ToSQL()
	fmt.Println(sql)
	// By default everything is anded together
	sql, _, _ = goqu.From("test").Where(
		goqu.C("a").Gt(10),
		goqu.C("b").Lt(10),
		goqu.C("c").IsNull(),
		goqu.C("d").In("a", "b", "c"),
	).ToSQL()
	fmt.Println(sql)
	// You can use a combination of Ors and Ands
	sql, _, _ = goqu.From("test").Where(
		goqu.Or(
			goqu.C("a").Gt(10),
			goqu.And(
				goqu.C("b").Lt(10),
				goqu.C("c").IsNull(),
			),
		),
	).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" WHERE (("a" > 10) AND ("b" < 10) AND ("c" IS NULL) AND ("d" IN ('a', 'b', 'c')))
	// SELECT * FROM "test" WHERE (("a" > 10) OR ("b" < 10) OR ("c" IS NULL) OR ("d" IN ('a', 'b', 'c')))
	// SELECT * FROM "test" WHERE ((("a" > 10) AND ("b" < 10)) OR (("c" IS NULL) AND ("d" IN ('a', 'b', 'c'))))
	// SELECT * FROM "test" WHERE (("a" > 10) AND ("b" < 10) AND ("c" IS NULL) AND ("d" IN ('a', 'b', 'c')))
	// SELECT * FROM "test" WHERE (("a" > 10) OR (("b" < 10) AND ("c" IS NULL)))
}

func ExampleDataset_Where_prepared() {
	// By default everything is anded together
	sql, args, _ := goqu.From("test").Prepared(true).Where(goqu.Ex{
		"a": goqu.Op{"gt": 10},
		"b": goqu.Op{"lt": 10},
		"c": nil,
		"d": []string{"a", "b", "c"},
	}).ToSQL()
	fmt.Println(sql, args)
	// You can use ExOr to get ORed expressions together
	sql, args, _ = goqu.From("test").Prepared(true).Where(goqu.ExOr{
		"a": goqu.Op{"gt": 10},
		"b": goqu.Op{"lt": 10},
		"c": nil,
		"d": []string{"a", "b", "c"},
	}).ToSQL()
	fmt.Println(sql, args)
	// You can use Or with Ex to Or multiple Ex maps together
	sql, args, _ = goqu.From("test").Prepared(true).Where(
		goqu.Or(
			goqu.Ex{
				"a": goqu.Op{"gt": 10},
				"b": goqu.Op{"lt": 10},
			},
			goqu.Ex{
				"c": nil,
				"d": []string{"a", "b", "c"},
			},
		),
	).ToSQL()
	fmt.Println(sql, args)
	// By default everything is anded together
	sql, args, _ = goqu.From("test").Prepared(true).Where(
		goqu.C("a").Gt(10),
		goqu.C("b").Lt(10),
		goqu.C("c").IsNull(),
		goqu.C("d").In("a", "b", "c"),
	).ToSQL()
	fmt.Println(sql, args)
	// You can use a combination of Ors and Ands
	sql, args, _ = goqu.From("test").Prepared(true).Where(
		goqu.Or(
			goqu.C("a").Gt(10),
			goqu.And(
				goqu.C("b").Lt(10),
				goqu.C("c").IsNull(),
			),
		),
	).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT * FROM "test" WHERE (("a" > ?) AND ("b" < ?) AND ("c" IS NULL) AND ("d" IN (?, ?, ?))) [10 10 a b c]
	// SELECT * FROM "test" WHERE (("a" > ?) OR ("b" < ?) OR ("c" IS NULL) OR ("d" IN (?, ?, ?))) [10 10 a b c]
	// SELECT * FROM "test" WHERE ((("a" > ?) AND ("b" < ?)) OR (("c" IS NULL) AND ("d" IN (?, ?, ?)))) [10 10 a b c]
	// SELECT * FROM "test" WHERE (("a" > ?) AND ("b" < ?) AND ("c" IS NULL) AND ("d" IN (?, ?, ?))) [10 10 a b c]
	// SELECT * FROM "test" WHERE (("a" > ?) OR (("b" < ?) AND ("c" IS NULL))) [10 10]
}

func ExampleDataset_ClearWhere() {
	ds := goqu.From("test").Where(
		goqu.Or(
			goqu.C("a").Gt(10),
			goqu.And(
				goqu.C("b").Lt(10),
				goqu.C("c").IsNull(),
			),
		),
	)
	sql, _, _ := ds.ClearWhere().ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test"
}

func ExampleDataset_Join() {
	sql, _, _ := goqu.From("test").Join(
		goqu.T("test2"),
		goqu.On(goqu.Ex{"test.fkey": goqu.I("test2.Id")}),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").Join(goqu.T("test2"), goqu.Using("common_column")).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").Join(
		goqu.From("test2").Where(goqu.C("amount").Gt(0)),
		goqu.On(goqu.I("test.fkey").Eq(goqu.T("test2").Col("Id"))),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").Join(
		goqu.From("test2").Where(goqu.C("amount").Gt(0)).As("t"),
		goqu.On(goqu.T("test").Col("fkey").Eq(goqu.T("t").Col("Id"))),
	).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" INNER JOIN "test2" ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" INNER JOIN "test2" USING ("common_column")
	// SELECT * FROM "test" INNER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" INNER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t" ON ("test"."fkey" = "t"."Id")

}

func ExampleDataset_InnerJoin() {
	sql, _, _ := goqu.From("test").InnerJoin(
		goqu.T("test2"),
		goqu.On(goqu.Ex{
			"test.fkey": goqu.I("test2.Id"),
		}),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").InnerJoin(
		goqu.T("test2"),
		goqu.Using("common_column"),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").InnerJoin(
		goqu.From("test2").Where(goqu.C("amount").Gt(0)),
		goqu.On(goqu.I("test.fkey").Eq(goqu.I("test2.Id"))),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").InnerJoin(
		goqu.From("test2").Where(goqu.C("amount").Gt(0)).As("t"),
		goqu.On(goqu.I("test.fkey").Eq(goqu.I("t.Id"))),
	).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" INNER JOIN "test2" ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" INNER JOIN "test2" USING ("common_column")
	// SELECT * FROM "test" INNER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" INNER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t" ON ("test"."fkey" = "t"."Id")
}

func ExampleDataset_FullOuterJoin() {
	sql, _, _ := goqu.From("test").FullOuterJoin(
		goqu.T("test2"),
		goqu.On(goqu.Ex{
			"test.fkey": goqu.I("test2.Id"),
		}),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").FullOuterJoin(
		goqu.T("test2"),
		goqu.Using("common_column"),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").FullOuterJoin(
		goqu.From("test2").Where(goqu.C("amount").Gt(0)),
		goqu.On(goqu.I("test.fkey").Eq(goqu.I("test2.Id"))),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").FullOuterJoin(
		goqu.From("test2").Where(goqu.C("amount").Gt(0)).As("t"),
		goqu.On(goqu.I("test.fkey").Eq(goqu.I("t.Id"))),
	).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" FULL OUTER JOIN "test2" ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" FULL OUTER JOIN "test2" USING ("common_column")
	// SELECT * FROM "test" FULL OUTER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" FULL OUTER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t" ON ("test"."fkey" = "t"."Id")
}

func ExampleDataset_RightOuterJoin() {
	sql, _, _ := goqu.From("test").RightOuterJoin(
		goqu.T("test2"),
		goqu.On(goqu.Ex{
			"test.fkey": goqu.I("test2.Id"),
		}),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").RightOuterJoin(
		goqu.T("test2"),
		goqu.Using("common_column"),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").RightOuterJoin(
		goqu.From("test2").Where(goqu.C("amount").Gt(0)),
		goqu.On(goqu.I("test.fkey").Eq(goqu.I("test2.Id"))),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").RightOuterJoin(
		goqu.From("test2").Where(goqu.C("amount").Gt(0)).As("t"),
		goqu.On(goqu.I("test.fkey").Eq(goqu.I("t.Id"))),
	).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" RIGHT OUTER JOIN "test2" ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" RIGHT OUTER JOIN "test2" USING ("common_column")
	// SELECT * FROM "test" RIGHT OUTER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" RIGHT OUTER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t" ON ("test"."fkey" = "t"."Id")
}

func ExampleDataset_LeftOuterJoin() {
	sql, _, _ := goqu.From("test").LeftOuterJoin(
		goqu.T("test2"),
		goqu.On(goqu.Ex{
			"test.fkey": goqu.I("test2.Id"),
		}),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").LeftOuterJoin(
		goqu.T("test2"),
		goqu.Using("common_column"),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").LeftOuterJoin(
		goqu.From("test2").Where(goqu.C("amount").Gt(0)),
		goqu.On(goqu.I("test.fkey").Eq(goqu.I("test2.Id"))),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").LeftOuterJoin(
		goqu.From("test2").Where(goqu.C("amount").Gt(0)).As("t"),
		goqu.On(goqu.I("test.fkey").Eq(goqu.I("t.Id"))),
	).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" LEFT OUTER JOIN "test2" ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" LEFT OUTER JOIN "test2" USING ("common_column")
	// SELECT * FROM "test" LEFT OUTER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" LEFT OUTER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t" ON ("test"."fkey" = "t"."Id")
}

func ExampleDataset_FullJoin() {
	sql, _, _ := goqu.From("test").FullJoin(
		goqu.T("test2"),
		goqu.On(goqu.Ex{
			"test.fkey": goqu.I("test2.Id"),
		}),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").FullJoin(
		goqu.T("test2"),
		goqu.Using("common_column"),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").FullJoin(
		goqu.From("test2").Where(goqu.C("amount").Gt(0)),
		goqu.On(goqu.I("test.fkey").Eq(goqu.I("test2.Id"))),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").FullJoin(
		goqu.From("test2").Where(goqu.C("amount").Gt(0)).As("t"),
		goqu.On(goqu.I("test.fkey").Eq(goqu.I("t.Id"))),
	).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" FULL JOIN "test2" ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" FULL JOIN "test2" USING ("common_column")
	// SELECT * FROM "test" FULL JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" FULL JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t" ON ("test"."fkey" = "t"."Id")
}

func ExampleDataset_RightJoin() {
	sql, _, _ := goqu.From("test").RightJoin(
		goqu.T("test2"),
		goqu.On(goqu.Ex{
			"test.fkey": goqu.I("test2.Id"),
		}),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").RightJoin(
		goqu.T("test2"),
		goqu.Using("common_column"),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").RightJoin(
		goqu.From("test2").Where(goqu.C("amount").Gt(0)),
		goqu.On(goqu.I("test.fkey").Eq(goqu.I("test2.Id"))),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").RightJoin(
		goqu.From("test2").Where(goqu.C("amount").Gt(0)).As("t"),
		goqu.On(goqu.I("test.fkey").Eq(goqu.I("t.Id"))),
	).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" RIGHT JOIN "test2" ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" RIGHT JOIN "test2" USING ("common_column")
	// SELECT * FROM "test" RIGHT JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" RIGHT JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t" ON ("test"."fkey" = "t"."Id")
}

func ExampleDataset_LeftJoin() {
	sql, _, _ := goqu.From("test").LeftJoin(
		goqu.T("test2"),
		goqu.On(goqu.Ex{
			"test.fkey": goqu.I("test2.Id"),
		}),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").LeftJoin(
		goqu.T("test2"),
		goqu.Using("common_column"),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").LeftJoin(
		goqu.From("test2").Where(goqu.C("amount").Gt(0)),
		goqu.On(goqu.I("test.fkey").Eq(goqu.I("test2.Id"))),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").LeftJoin(
		goqu.From("test2").Where(goqu.C("amount").Gt(0)).As("t"),
		goqu.On(goqu.I("test.fkey").Eq(goqu.I("t.Id"))),
	).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" LEFT JOIN "test2" ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" LEFT JOIN "test2" USING ("common_column")
	// SELECT * FROM "test" LEFT JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" LEFT JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t" ON ("test"."fkey" = "t"."Id")
}

func ExampleDataset_NaturalJoin() {
	sql, _, _ := goqu.From("test").NaturalJoin(goqu.T("test2")).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").NaturalJoin(
		goqu.From("test2").Where(goqu.C("amount").Gt(0)),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").NaturalJoin(
		goqu.From("test2").Where(goqu.C("amount").Gt(0)).As("t"),
	).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" NATURAL JOIN "test2"
	// SELECT * FROM "test" NATURAL JOIN (SELECT * FROM "test2" WHERE ("amount" > 0))
	// SELECT * FROM "test" NATURAL JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t"
}

func ExampleDataset_NaturalLeftJoin() {
	sql, _, _ := goqu.From("test").NaturalLeftJoin(goqu.T("test2")).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").NaturalLeftJoin(
		goqu.From("test2").Where(goqu.C("amount").Gt(0)),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").NaturalLeftJoin(
		goqu.From("test2").Where(goqu.C("amount").Gt(0)).As("t"),
	).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" NATURAL LEFT JOIN "test2"
	// SELECT * FROM "test" NATURAL LEFT JOIN (SELECT * FROM "test2" WHERE ("amount" > 0))
	// SELECT * FROM "test" NATURAL LEFT JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t"
}

func ExampleDataset_NaturalRightJoin() {
	sql, _, _ := goqu.From("test").NaturalRightJoin(goqu.T("test2")).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").NaturalRightJoin(
		goqu.From("test2").Where(goqu.C("amount").Gt(0)),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").NaturalRightJoin(
		goqu.From("test2").Where(goqu.C("amount").Gt(0)).As("t"),
	).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" NATURAL RIGHT JOIN "test2"
	// SELECT * FROM "test" NATURAL RIGHT JOIN (SELECT * FROM "test2" WHERE ("amount" > 0))
	// SELECT * FROM "test" NATURAL RIGHT JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t"
}

func ExampleDataset_NaturalFullJoin() {
	sql, _, _ := goqu.From("test").NaturalFullJoin(goqu.T("test2")).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").NaturalFullJoin(
		goqu.From("test2").Where(goqu.C("amount").Gt(0)),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").NaturalFullJoin(
		goqu.From("test2").Where(goqu.C("amount").Gt(0)).As("t"),
	).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" NATURAL FULL JOIN "test2"
	// SELECT * FROM "test" NATURAL FULL JOIN (SELECT * FROM "test2" WHERE ("amount" > 0))
	// SELECT * FROM "test" NATURAL FULL JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t"
}

func ExampleDataset_CrossJoin() {
	sql, _, _ := goqu.From("test").CrossJoin(goqu.T("test2")).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").CrossJoin(
		goqu.From("test2").Where(goqu.C("amount").Gt(0)),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").CrossJoin(
		goqu.From("test2").Where(goqu.C("amount").Gt(0)).As("t"),
	).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" CROSS JOIN "test2"
	// SELECT * FROM "test" CROSS JOIN (SELECT * FROM "test2" WHERE ("amount" > 0))
	// SELECT * FROM "test" CROSS JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t"
}

func ExampleDataset_FromSelf() {
	sql, _, _ := goqu.From("test").FromSelf().ToSQL()
	fmt.Println(sql)
	sql, _, _ = goqu.From("test").As("my_test_table").FromSelf().ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM (SELECT * FROM "test") AS "t1"
	// SELECT * FROM (SELECT * FROM "test") AS "my_test_table"
}

func ExampleDataset_From() {
	ds := goqu.From("test")
	sql, _, _ := ds.From("test2").ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test2"
}

func ExampleDataset_From_withDataset() {
	ds := goqu.From("test")
	fromDs := ds.Where(goqu.C("age").Gt(10))
	sql, _, _ := ds.From(fromDs).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM (SELECT * FROM "test" WHERE ("age" > 10)) AS "t1"
}

func ExampleDataset_From_withAliasedDataset() {
	ds := goqu.From("test")
	fromDs := ds.Where(goqu.C("age").Gt(10))
	sql, _, _ := ds.From(fromDs.As("test2")).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM (SELECT * FROM "test" WHERE ("age" > 10)) AS "test2"
}

func ExampleDataset_Select() {
	sql, _, _ := goqu.From("test").Select("a", "b", "c").ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT "a", "b", "c" FROM "test"
}

func ExampleDataset_Select_withDataset() {
	ds := goqu.From("test")
	fromDs := ds.Select("age").Where(goqu.C("age").Gt(10))
	sql, _, _ := ds.From().Select(fromDs).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT (SELECT "age" FROM "test" WHERE ("age" > 10))
}

func ExampleDataset_Select_withAliasedDataset() {
	ds := goqu.From("test")
	fromDs := ds.Select("age").Where(goqu.C("age").Gt(10))
	sql, _, _ := ds.From().Select(fromDs.As("ages")).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT (SELECT "age" FROM "test" WHERE ("age" > 10)) AS "ages"
}

func ExampleDataset_Select_withLiteral() {
	sql, _, _ := goqu.From("test").Select(goqu.L("a + b").As("sum")).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT a + b AS "sum" FROM "test"
}

func ExampleDataset_Select_withSQLFunctionExpression() {
	sql, _, _ := goqu.From("test").Select(
		goqu.COUNT("*").As("age_count"),
		goqu.MAX("age").As("max_age"),
		goqu.AVG("age").As("avg_age"),
	).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT COUNT(*) AS "age_count", MAX("age") AS "max_age", AVG("age") AS "avg_age" FROM "test"
}

func ExampleDataset_Select_withStruct() {
	ds := goqu.From("test")

	type myStruct struct {
		Name         string
		Address      string `db:"address"`
		EmailAddress string `db:"email_address"`
	}

	// Pass with pointer
	sql, _, _ := ds.Select(&myStruct{}).ToSQL()
	fmt.Println(sql)

	// Pass instance of
	sql, _, _ = ds.Select(myStruct{}).ToSQL()
	fmt.Println(sql)

	type myStruct2 struct {
		myStruct
		Zipcode string `db:"zipcode"`
	}

	// Pass pointer to struct with embedded struct
	sql, _, _ = ds.Select(&myStruct2{}).ToSQL()
	fmt.Println(sql)

	// Pass instance of struct with embedded struct
	sql, _, _ = ds.Select(myStruct2{}).ToSQL()
	fmt.Println(sql)

	var myStructs []myStruct

	// Pass slice of structs, will only select columns from underlying type
	sql, _, _ = ds.Select(myStructs).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT "address", "email_address", "name" FROM "test"
	// SELECT "address", "email_address", "name" FROM "test"
	// SELECT "address", "email_address", "name", "zipcode" FROM "test"
	// SELECT "address", "email_address", "name", "zipcode" FROM "test"
	// SELECT "address", "email_address", "name" FROM "test"
}

func ExampleDataset_SelectDistinct() {
	sql, _, _ := goqu.From("test").SelectDistinct("a", "b").ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT DISTINCT "a", "b" FROM "test"
}

func ExampleDataset_SelectAppend() {
	ds := goqu.From("test").Select("a", "b")
	sql, _, _ := ds.SelectAppend("c").ToSQL()
	fmt.Println(sql)
	ds = goqu.From("test").SelectDistinct("a", "b")
	sql, _, _ = ds.SelectAppend("c").ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT "a", "b", "c" FROM "test"
	// SELECT DISTINCT "a", "b", "c" FROM "test"
}

func ExampleDataset_ClearSelect() {
	ds := goqu.From("test").Select("a", "b")
	sql, _, _ := ds.ClearSelect().ToSQL()
	fmt.Println(sql)
	ds = goqu.From("test").SelectDistinct("a", "b")
	sql, _, _ = ds.ClearSelect().ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test"
	// SELECT * FROM "test"
}

func ExampleDataset_ToSQL() {
	sql, args, _ := goqu.From("items").Where(goqu.Ex{"a": 1}).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT * FROM "items" WHERE ("a" = 1) []
}

func ExampleDataset_ToSQL_prepared() {
	sql, args, _ := goqu.From("items").Where(goqu.Ex{"a": 1}).Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT * FROM "items" WHERE ("a" = ?) [1]
}

func ExampleDataset_ToUpdateSQL() {
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, args, _ := goqu.From("items").ToUpdateSQL(
		item{Name: "Test", Address: "111 Test Addr"},
	)
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("items").ToUpdateSQL(
		goqu.Record{"name": "Test", "address": "111 Test Addr"},
	)
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("items").ToUpdateSQL(
		map[string]interface{}{"name": "Test", "address": "111 Test Addr"},
	)
	fmt.Println(sql, args)

	// Output:
	// UPDATE "items" SET "address"='111 Test Addr',"name"='Test' []
	// UPDATE "items" SET "address"='111 Test Addr',"name"='Test' []
	// UPDATE "items" SET "address"='111 Test Addr',"name"='Test' []
}

func ExampleDataset_ToUpdateSQL_prepared() {
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	sql, args, _ := goqu.From("items").Prepared(true).ToUpdateSQL(
		item{Name: "Test", Address: "111 Test Addr"},
	)
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("items").Prepared(true).ToUpdateSQL(
		goqu.Record{"name": "Test", "address": "111 Test Addr"},
	)
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("items").Prepared(true).ToUpdateSQL(
		map[string]interface{}{"name": "Test", "address": "111 Test Addr"},
	)
	fmt.Println(sql, args)
	// Output:
	// UPDATE "items" SET "address"=?,"name"=? [111 Test Addr Test]
	// UPDATE "items" SET "address"=?,"name"=? [111 Test Addr Test]
	// UPDATE "items" SET "address"=?,"name"=? [111 Test Addr Test]
}

func ExampleDataset_ToInsertSQL() {
	type item struct {
		ID      uint32 `db:"id" goqu:"skipinsert"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, args, _ := goqu.From("items").ToInsertSQL(
		item{Name: "Test1", Address: "111 Test Addr"},
		item{Name: "Test2", Address: "112 Test Addr"},
	)
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("items").ToInsertSQL(
		goqu.Record{"name": "Test1", "address": "111 Test Addr"},
		goqu.Record{"name": "Test2", "address": "112 Test Addr"},
	)
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("items").ToInsertSQL(
		[]item{
			{Name: "Test1", Address: "111 Test Addr"},
			{Name: "Test2", Address: "112 Test Addr"},
		})
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("items").ToInsertSQL(
		[]goqu.Record{
			{"name": "Test1", "address": "111 Test Addr"},
			{"name": "Test2", "address": "112 Test Addr"},
		})
	fmt.Println(sql, args)
	// Output:
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') []
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') []
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') []
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') []
}

func ExampleDataset_ToInsertSQL_prepared() {
	type item struct {
		ID      uint32 `db:"id" goqu:"skipinsert"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	sql, args, _ := goqu.From("items").Prepared(true).ToInsertSQL(
		item{Name: "Test1", Address: "111 Test Addr"},
		item{Name: "Test2", Address: "112 Test Addr"},
	)
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("items").Prepared(true).ToInsertSQL(
		goqu.Record{"name": "Test1", "address": "111 Test Addr"},
		goqu.Record{"name": "Test2", "address": "112 Test Addr"},
	)
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("items").Prepared(true).ToInsertSQL(
		[]item{
			{Name: "Test1", Address: "111 Test Addr"},
			{Name: "Test2", Address: "112 Test Addr"},
		})
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("items").Prepared(true).ToInsertSQL(
		[]goqu.Record{
			{"name": "Test1", "address": "111 Test Addr"},
			{"name": "Test2", "address": "112 Test Addr"},
		})
	fmt.Println(sql, args)
	// Output:
	// INSERT INTO "items" ("address", "name") VALUES (?, ?), (?, ?) [111 Test Addr Test1 112 Test Addr Test2]
	// INSERT INTO "items" ("address", "name") VALUES (?, ?), (?, ?) [111 Test Addr Test1 112 Test Addr Test2]
	// INSERT INTO "items" ("address", "name") VALUES (?, ?), (?, ?) [111 Test Addr Test1 112 Test Addr Test2]
	// INSERT INTO "items" ("address", "name") VALUES (?, ?), (?, ?) [111 Test Addr Test1 112 Test Addr Test2]
}

func ExampleDataset_ToInsertIgnoreSQL() {
	type item struct {
		ID      uint32 `db:"id" goqu:"skipinsert"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, args, _ := goqu.From("items").ToInsertIgnoreSQL(
		item{Name: "Test1", Address: "111 Test Addr"},
		item{Name: "Test2", Address: "112 Test Addr"},
	)
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("items").ToInsertIgnoreSQL(
		goqu.Record{"name": "Test1", "address": "111 Test Addr"},
		goqu.Record{"name": "Test2", "address": "112 Test Addr"},
	)
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("items").ToInsertIgnoreSQL(
		[]item{
			{Name: "Test1", Address: "111 Test Addr"},
			{Name: "Test2", Address: "112 Test Addr"},
		})
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("items").ToInsertIgnoreSQL(
		[]goqu.Record{
			{"name": "Test1", "address": "111 Test Addr"},
			{"name": "Test2", "address": "112 Test Addr"},
		})
	fmt.Println(sql, args)
	// Output:
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') ON CONFLICT DO NOTHING []
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') ON CONFLICT DO NOTHING []
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') ON CONFLICT DO NOTHING []
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') ON CONFLICT DO NOTHING []
}

func ExampleDataset_ToInsertConflictSQL() {
	type item struct {
		ID      uint32 `db:"id" goqu:"skipinsert"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, args, _ := goqu.From("items").ToInsertConflictSQL(
		goqu.DoNothing(),
		item{Name: "Test1", Address: "111 Test Addr"},
		item{Name: "Test2", Address: "112 Test Addr"},
	)
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("items").ToInsertConflictSQL(
		goqu.DoUpdate("key", goqu.Record{"updated": goqu.L("NOW()")}),
		goqu.Record{"name": "Test1", "address": "111 Test Addr"},
		goqu.Record{"name": "Test2", "address": "112 Test Addr"},
	)
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("items").ToInsertConflictSQL(
		goqu.DoUpdate("key", goqu.Record{"updated": goqu.L("NOW()")}).Where(goqu.C("allow_update").IsTrue()),
		[]item{
			{Name: "Test1", Address: "111 Test Addr"},
			{Name: "Test2", Address: "112 Test Addr"},
		})
	fmt.Println(sql, args)

	// nolint:lll
	// Output:
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') ON CONFLICT DO NOTHING []
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') ON CONFLICT (key) DO UPDATE SET "updated"=NOW() []
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') ON CONFLICT (key) DO UPDATE SET "updated"=NOW() WHERE ("allow_update" IS TRUE) []
}

func ExampleDataset_ToDeleteSQL() {
	sql, args, _ := goqu.From("items").ToDeleteSQL()
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("items").
		Where(goqu.Ex{"id": goqu.Op{"gt": 10}}).
		ToDeleteSQL()
	fmt.Println(sql, args)

	// Output:
	// DELETE FROM "items" []
	// DELETE FROM "items" WHERE ("id" > 10) []
}

func ExampleDataset_ToDeleteSQL_prepared() {
	sql, args, _ := goqu.From("items").Prepared(true).ToDeleteSQL()
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("items").
		Prepared(true).
		Where(goqu.Ex{"id": goqu.Op{"gt": 10}}).
		ToDeleteSQL()
	fmt.Println(sql, args)

	// Output:
	// DELETE FROM "items" []
	// DELETE FROM "items" WHERE ("id" > ?) [10]
}

func ExampleDataset_ToDeleteSQL_withWhere() {
	sql, args, _ := goqu.From("items").Where(goqu.C("id").IsNotNull()).ToDeleteSQL()
	fmt.Println(sql, args)

	// Output:
	// DELETE FROM "items" WHERE ("id" IS NOT NULL) []
}

func ExampleDataset_ToDeleteSQL_withReturning() {
	ds := goqu.From("items")
	sql, args, _ := ds.Returning("id").ToDeleteSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Returning("id").Where(goqu.C("id").IsNotNull()).ToDeleteSQL()
	fmt.Println(sql, args)

	// Output:
	// DELETE FROM "items" RETURNING "id" []
	// DELETE FROM "items" WHERE ("id" IS NOT NULL) RETURNING "id" []
}

func ExampleDataset_ToTruncateSQL() {
	sql, args, _ := goqu.From("items").ToTruncateSQL()
	fmt.Println(sql, args)
	// Output:
	// TRUNCATE "items" []
}

func ExampleDataset_ToTruncateWithOptsSQL() {
	sql, _, _ := goqu.From("items").
		ToTruncateWithOptsSQL(goqu.TruncateOptions{})
	fmt.Println(sql)
	sql, _, _ = goqu.From("items").
		ToTruncateWithOptsSQL(goqu.TruncateOptions{Cascade: true})
	fmt.Println(sql)
	sql, _, _ = goqu.From("items").
		ToTruncateWithOptsSQL(goqu.TruncateOptions{Restrict: true})
	fmt.Println(sql)
	sql, _, _ = goqu.From("items").
		ToTruncateWithOptsSQL(goqu.TruncateOptions{Identity: "RESTART"})
	fmt.Println(sql)
	sql, _, _ = goqu.From("items").
		ToTruncateWithOptsSQL(goqu.TruncateOptions{Identity: "RESTART", Cascade: true})
	fmt.Println(sql)
	sql, _, _ = goqu.From("items").
		ToTruncateWithOptsSQL(goqu.TruncateOptions{Identity: "RESTART", Restrict: true})
	fmt.Println(sql)
	sql, _, _ = goqu.From("items").
		ToTruncateWithOptsSQL(goqu.TruncateOptions{Identity: "CONTINUE"})
	fmt.Println(sql)
	sql, _, _ = goqu.From("items").
		ToTruncateWithOptsSQL(goqu.TruncateOptions{Identity: "CONTINUE", Cascade: true})
	fmt.Println(sql)
	sql, _, _ = goqu.From("items").
		ToTruncateWithOptsSQL(goqu.TruncateOptions{Identity: "CONTINUE", Restrict: true})
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

func ExampleDataset_Prepared() {
	sql, args, _ := goqu.From("items").Prepared(true).Where(goqu.Ex{
		"col1": "a",
		"col2": 1,
		"col3": true,
		"col4": false,
		"col5": []string{"a", "b", "c"},
	}).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("items").Prepared(true).ToInsertSQL(
		goqu.Record{"name": "Test1", "address": "111 Test Addr"},
		goqu.Record{"name": "Test2", "address": "112 Test Addr"},
	)
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("items").Prepared(true).ToUpdateSQL(
		goqu.Record{"name": "Test", "address": "111 Test Addr"},
	)
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("items").
		Prepared(true).
		Where(goqu.Ex{"id": goqu.Op{"gt": 10}}).
		ToDeleteSQL()
	fmt.Println(sql, args)

	// nolint:lll
	// Output:
	// SELECT * FROM "items" WHERE (("col1" = ?) AND ("col2" = ?) AND ("col3" IS TRUE) AND ("col4" IS FALSE) AND ("col5" IN (?, ?, ?))) [a 1 a b c]
	// INSERT INTO "items" ("address", "name") VALUES (?, ?), (?, ?) [111 Test Addr Test1 112 Test Addr Test2]
	// UPDATE "items" SET "address"=?,"name"=? [111 Test Addr Test]
	// DELETE FROM "items" WHERE ("id" > ?) [10]
}
