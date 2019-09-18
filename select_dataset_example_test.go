package goqu_test

import (
	goSQL "database/sql"
	"fmt"
	"os"
	"regexp"

	"github.com/doug-martin/goqu/v9"
	"github.com/lib/pq"
)

const schema = `
        DROP TABLE IF EXISTS "goqu_user";
        CREATE  TABLE "goqu_user" (
            "id" SERIAL PRIMARY KEY NOT NULL,
            "first_name" VARCHAR(45) NOT NULL,
			"last_name" VARCHAR(45) NOT NULL,
			"created" TIMESTAMP NOT NULL DEFAULT now()
		);
        INSERT INTO "goqu_user" ("first_name", "last_name") VALUES
            ('Bob', 'Yukon'),
            ('Sally', 'Yukon'),
			('Vinita', 'Yukon'),
			('John', 'Doe')
    `

const defaultDbURI = "postgres://postgres:@localhost:5435/goqupostgres?sslmode=disable"

var goquDb *goqu.Database

func getDb() *goqu.Database {
	if goquDb == nil {
		dbURI := os.Getenv("PG_URI")
		if dbURI == "" {
			dbURI = defaultDbURI
		}
		uri, err := pq.ParseURL(dbURI)
		if err != nil {
			panic(err)
		}
		pdb, err := goSQL.Open("postgres", uri)
		if err != nil {
			panic(err)
		}
		goquDb = goqu.New("postgres", pdb)
	}
	// reset the db
	if _, err := goquDb.Exec(schema); err != nil {
		panic(err)
	}
	return goquDb
}

func ExampleSelectDataset() {
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

func ExampleSelect() {
	sql, _, _ := goqu.Select(goqu.L("NOW()")).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT NOW()
}

func ExampleFrom() {
	sql, args, _ := goqu.From("test").ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" []
}

func ExampleSelectDataset_As() {
	ds := goqu.From("test").As("t")
	sql, _, _ := goqu.From(ds).ToSQL()
	fmt.Println(sql)
	// Output: SELECT * FROM (SELECT * FROM "test") AS "t"
}

func ExampleSelectDataset_Union() {
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

func ExampleSelectDataset_UnionAll() {
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

func ExampleSelectDataset_With() {
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

	// Output:
	// WITH one AS (SELECT 1) SELECT * FROM "one"
	// WITH intermed AS (SELECT * FROM "test" WHERE ("x" >= 5)), derived AS (SELECT * FROM "intermed" WHERE ("x" < 10)) SELECT * FROM "derived"
	// WITH multi(x,y) AS (SELECT 1, 2) SELECT "x", "y" FROM "multi"
}

func ExampleSelectDataset_WithRecursive() {
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

func ExampleSelectDataset_Intersect() {
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

func ExampleSelectDataset_IntersectAll() {
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

func ExampleSelectDataset_ClearOffset() {
	ds := goqu.From("test").
		Offset(2)
	sql, _, _ := ds.
		ClearOffset().
		ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test"
}

func ExampleSelectDataset_Offset() {
	ds := goqu.From("test").Offset(2)
	sql, _, _ := ds.ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" OFFSET 2
}

func ExampleSelectDataset_Limit() {
	ds := goqu.From("test").Limit(10)
	sql, _, _ := ds.ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" LIMIT 10
}

func ExampleSelectDataset_LimitAll() {
	ds := goqu.From("test").LimitAll()
	sql, _, _ := ds.ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" LIMIT ALL
}

func ExampleSelectDataset_ClearLimit() {
	ds := goqu.From("test").Limit(10)
	sql, _, _ := ds.ClearLimit().ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test"
}

func ExampleSelectDataset_Order() {
	ds := goqu.From("test").Order(goqu.C("a").Asc())
	sql, _, _ := ds.ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" ORDER BY "a" ASC
}

func ExampleSelectDataset_OrderAppend() {
	ds := goqu.From("test").Order(goqu.C("a").Asc())
	sql, _, _ := ds.OrderAppend(goqu.C("b").Desc().NullsLast()).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" ORDER BY "a" ASC, "b" DESC NULLS LAST
}

func ExampleSelectDataset_OrderPrepend() {
	ds := goqu.From("test").Order(goqu.C("a").Asc())
	sql, _, _ := ds.OrderPrepend(goqu.C("b").Desc().NullsLast()).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" ORDER BY "b" DESC NULLS LAST, "a" ASC
}

func ExampleSelectDataset_ClearOrder() {
	ds := goqu.From("test").Order(goqu.C("a").Asc())
	sql, _, _ := ds.ClearOrder().ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test"
}

func ExampleSelectDataset_GroupBy() {
	sql, _, _ := goqu.From("test").
		Select(goqu.SUM("income").As("income_sum")).
		GroupBy("age").
		ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT SUM("income") AS "income_sum" FROM "test" GROUP BY "age"
}

func ExampleSelectDataset_Having() {
	sql, _, _ := goqu.From("test").Having(goqu.SUM("income").Gt(1000)).ToSQL()
	fmt.Println(sql)
	sql, _, _ = goqu.From("test").GroupBy("age").Having(goqu.SUM("income").Gt(1000)).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" HAVING (SUM("income") > 1000)
	// SELECT * FROM "test" GROUP BY "age" HAVING (SUM("income") > 1000)
}

func ExampleSelectDataset_Window() {
	ds := goqu.From("test").
		Select(goqu.ROW_NUMBER().Over(goqu.W().PartitionBy("a").OrderBy(goqu.I("b").Asc())))
	query, args, _ := ds.ToSQL()
	fmt.Println(query, args)

	ds = goqu.From("test").
		Select(goqu.ROW_NUMBER().OverName(goqu.I("w"))).
		Window(goqu.W("w").PartitionBy("a").OrderBy(goqu.I("b").Asc()))
	query, args, _ = ds.ToSQL()
	fmt.Println(query, args)

	ds = goqu.From("test").
		Select(goqu.ROW_NUMBER().OverName(goqu.I("w1"))).
		Window(
			goqu.W("w1").PartitionBy("a"),
			goqu.W("w").Inherit("w1").OrderBy(goqu.I("b").Asc()),
		)
	query, args, _ = ds.ToSQL()
	fmt.Println(query, args)

	ds = goqu.From("test").
		Select(goqu.ROW_NUMBER().Over(goqu.W().Inherit("w").OrderBy("b"))).
		Window(goqu.W("w").PartitionBy("a"))
	query, args, _ = ds.ToSQL()
	fmt.Println(query, args)
	// Output
	// SELECT ROW_NUMBER() OVER (PARTITION BY "a" ORDER BY "b" ASC) FROM "test" []
	// SELECT ROW_NUMBER() OVER "w" FROM "test" WINDOW "w" AS (PARTITION BY "a" ORDER BY "b" ASC) []
	// SELECT ROW_NUMBER() OVER "w" FROM "test" WINDOW "w1" AS (PARTITION BY "a"), "w" AS ("w1" ORDER BY "b" ASC) []
	// SELECT ROW_NUMBER() OVER ("w" ORDER BY "b") FROM "test" WINDOW "w" AS (PARTITION BY "a") []
}

func ExampleSelectDataset_Where() {
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

func ExampleSelectDataset_Where_prepared() {
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
	// SELECT * FROM "test" WHERE (("a" > ?) AND ("b" < ?) AND ("c" IS ?) AND ("d" IN (?, ?, ?))) [10 10 <nil> a b c]
	// SELECT * FROM "test" WHERE (("a" > ?) OR ("b" < ?) OR ("c" IS ?) OR ("d" IN (?, ?, ?))) [10 10 <nil> a b c]
	// SELECT * FROM "test" WHERE ((("a" > ?) AND ("b" < ?)) OR (("c" IS ?) AND ("d" IN (?, ?, ?)))) [10 10 <nil> a b c]
	// SELECT * FROM "test" WHERE (("a" > ?) AND ("b" < ?) AND ("c" IS ?) AND ("d" IN (?, ?, ?))) [10 10 <nil> a b c]
	// SELECT * FROM "test" WHERE (("a" > ?) OR (("b" < ?) AND ("c" IS ?))) [10 10 <nil>]
}

func ExampleSelectDataset_ClearWhere() {
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

func ExampleSelectDataset_Join() {
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

func ExampleSelectDataset_InnerJoin() {
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

func ExampleSelectDataset_FullOuterJoin() {
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

func ExampleSelectDataset_RightOuterJoin() {
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

func ExampleSelectDataset_LeftOuterJoin() {
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

func ExampleSelectDataset_FullJoin() {
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

func ExampleSelectDataset_RightJoin() {
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

func ExampleSelectDataset_LeftJoin() {
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

func ExampleSelectDataset_NaturalJoin() {
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

func ExampleSelectDataset_NaturalLeftJoin() {
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

func ExampleSelectDataset_NaturalRightJoin() {
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

func ExampleSelectDataset_NaturalFullJoin() {
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

func ExampleSelectDataset_CrossJoin() {
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

func ExampleSelectDataset_FromSelf() {
	sql, _, _ := goqu.From("test").FromSelf().ToSQL()
	fmt.Println(sql)
	sql, _, _ = goqu.From("test").As("my_test_table").FromSelf().ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM (SELECT * FROM "test") AS "t1"
	// SELECT * FROM (SELECT * FROM "test") AS "my_test_table"
}

func ExampleSelectDataset_From() {
	ds := goqu.From("test")
	sql, _, _ := ds.From("test2").ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test2"
}

func ExampleSelectDataset_From_withDataset() {
	ds := goqu.From("test")
	fromDs := ds.Where(goqu.C("age").Gt(10))
	sql, _, _ := ds.From(fromDs).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM (SELECT * FROM "test" WHERE ("age" > 10)) AS "t1"
}

func ExampleSelectDataset_From_withAliasedDataset() {
	ds := goqu.From("test")
	fromDs := ds.Where(goqu.C("age").Gt(10))
	sql, _, _ := ds.From(fromDs.As("test2")).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM (SELECT * FROM "test" WHERE ("age" > 10)) AS "test2"
}

func ExampleSelectDataset_Select() {
	sql, _, _ := goqu.From("test").Select("a", "b", "c").ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT "a", "b", "c" FROM "test"
}

func ExampleSelectDataset_Select_withDataset() {
	ds := goqu.From("test")
	fromDs := ds.Select("age").Where(goqu.C("age").Gt(10))
	sql, _, _ := ds.From().Select(fromDs).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT (SELECT "age" FROM "test" WHERE ("age" > 10))
}

func ExampleSelectDataset_Select_withAliasedDataset() {
	ds := goqu.From("test")
	fromDs := ds.Select("age").Where(goqu.C("age").Gt(10))
	sql, _, _ := ds.From().Select(fromDs.As("ages")).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT (SELECT "age" FROM "test" WHERE ("age" > 10)) AS "ages"
}

func ExampleSelectDataset_Select_withLiteral() {
	sql, _, _ := goqu.From("test").Select(goqu.L("a + b").As("sum")).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT a + b AS "sum" FROM "test"
}

func ExampleSelectDataset_Select_withSQLFunctionExpression() {
	sql, _, _ := goqu.From("test").Select(
		goqu.COUNT("*").As("age_count"),
		goqu.MAX("age").As("max_age"),
		goqu.AVG("age").As("avg_age"),
	).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT COUNT(*) AS "age_count", MAX("age") AS "max_age", AVG("age") AS "avg_age" FROM "test"
}

func ExampleSelectDataset_Select_withStruct() {
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

func ExampleSelectDataset_Distinct() {
	sql, _, _ := goqu.From("test").Select("a", "b").Distinct().ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT DISTINCT "a", "b" FROM "test"
}

func ExampleSelectDataset_Distinct_on() {
	sql, _, _ := goqu.From("test").Distinct("a").ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT DISTINCT ON ("a") * FROM "test"
}

func ExampleSelectDataset_Distinct_onWithLiteral() {
	sql, _, _ := goqu.From("test").Distinct(goqu.L("COALESCE(?, ?)", goqu.C("a"), "empty")).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT DISTINCT ON (COALESCE("a", 'empty')) * FROM "test"
}

func ExampleSelectDataset_Distinct_onCoalesce() {
	sql, _, _ := goqu.From("test").Distinct(goqu.COALESCE(goqu.C("a"), "empty")).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT DISTINCT ON (COALESCE("a", 'empty')) * FROM "test"
}

func ExampleSelectDataset_SelectAppend() {
	ds := goqu.From("test").Select("a", "b")
	sql, _, _ := ds.SelectAppend("c").ToSQL()
	fmt.Println(sql)
	ds = goqu.From("test").Select("a", "b").Distinct()
	sql, _, _ = ds.SelectAppend("c").ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT "a", "b", "c" FROM "test"
	// SELECT DISTINCT "a", "b", "c" FROM "test"
}

func ExampleSelectDataset_ClearSelect() {
	ds := goqu.From("test").Select("a", "b")
	sql, _, _ := ds.ClearSelect().ToSQL()
	fmt.Println(sql)
	ds = goqu.From("test").Select("a", "b").Distinct()
	sql, _, _ = ds.ClearSelect().ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test"
	// SELECT * FROM "test"
}

func ExampleSelectDataset_ToSQL() {
	sql, args, _ := goqu.From("items").Where(goqu.Ex{"a": 1}).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT * FROM "items" WHERE ("a" = 1) []
}

func ExampleSelectDataset_ToSQL_prepared() {
	sql, args, _ := goqu.From("items").Where(goqu.Ex{"a": 1}).Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT * FROM "items" WHERE ("a" = ?) [1]
}

func ExampleSelectDataset_Update() {
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, args, _ := goqu.From("items").Update().Set(
		item{Name: "Test", Address: "111 Test Addr"},
	).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("items").Update().Set(
		goqu.Record{"name": "Test", "address": "111 Test Addr"},
	).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("items").Update().Set(
		map[string]interface{}{"name": "Test", "address": "111 Test Addr"},
	).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// UPDATE "items" SET "address"='111 Test Addr',"name"='Test' []
	// UPDATE "items" SET "address"='111 Test Addr',"name"='Test' []
	// UPDATE "items" SET "address"='111 Test Addr',"name"='Test' []
}

func ExampleSelectDataset_Insert() {
	type item struct {
		ID      uint32 `db:"id" goqu:"skipinsert"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, args, _ := goqu.From("items").Insert().Rows(
		item{Name: "Test1", Address: "111 Test Addr"},
		item{Name: "Test2", Address: "112 Test Addr"},
	).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("items").Insert().Rows(
		goqu.Record{"name": "Test1", "address": "111 Test Addr"},
		goqu.Record{"name": "Test2", "address": "112 Test Addr"},
	).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("items").Insert().Rows(
		[]item{
			{Name: "Test1", Address: "111 Test Addr"},
			{Name: "Test2", Address: "112 Test Addr"},
		}).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("items").Insert().Rows(
		[]goqu.Record{
			{"name": "Test1", "address": "111 Test Addr"},
			{"name": "Test2", "address": "112 Test Addr"},
		}).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') []
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') []
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') []
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') []
}

func ExampleSelectDataset_Delete() {
	sql, args, _ := goqu.From("items").Delete().ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("items").
		Where(goqu.Ex{"id": goqu.Op{"gt": 10}}).
		Delete().
		ToSQL()
	fmt.Println(sql, args)

	// Output:
	// DELETE FROM "items" []
	// DELETE FROM "items" WHERE ("id" > 10) []
}

func ExampleSelectDataset_Truncate() {
	sql, args, _ := goqu.From("items").Truncate().ToSQL()
	fmt.Println(sql, args)
	// Output:
	// TRUNCATE "items" []
}

func ExampleSelectDataset_Prepared() {
	sql, args, _ := goqu.From("items").Prepared(true).Where(goqu.Ex{
		"col1": "a",
		"col2": 1,
		"col3": true,
		"col4": false,
		"col5": []string{"a", "b", "c"},
	}).ToSQL()
	fmt.Println(sql, args)
	// nolint:lll
	// Output:
	// SELECT * FROM "items" WHERE (("col1" = ?) AND ("col2" = ?) AND ("col3" IS TRUE) AND ("col4" IS FALSE) AND ("col5" IN (?, ?, ?))) [a 1 a b c]
}

func ExampleSelectDataset_ScanStructs() {
	type User struct {
		FirstName string `db:"first_name"`
		LastName  string `db:"last_name"`
	}
	db := getDb()
	var users []User
	if err := db.From("goqu_user").ScanStructs(&users); err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Printf("\n%+v", users)

	users = users[0:0]
	if err := db.From("goqu_user").Select("first_name").ScanStructs(&users); err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Printf("\n%+v", users)

	// Output:
	// [{FirstName:Bob LastName:Yukon} {FirstName:Sally LastName:Yukon} {FirstName:Vinita LastName:Yukon} {FirstName:John LastName:Doe}]
	// [{FirstName:Bob LastName:} {FirstName:Sally LastName:} {FirstName:Vinita LastName:} {FirstName:John LastName:}]
}

func ExampleSelectDataset_ScanStructs_prepared() {
	type User struct {
		FirstName string `db:"first_name"`
		LastName  string `db:"last_name"`
	}
	db := getDb()

	ds := db.From("goqu_user").
		Prepared(true).
		Where(goqu.Ex{
			"last_name": "Yukon",
		})

	var users []User
	if err := ds.ScanStructs(&users); err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Printf("\n%+v", users)

	// Output:
	// [{FirstName:Bob LastName:Yukon} {FirstName:Sally LastName:Yukon} {FirstName:Vinita LastName:Yukon}]
}

func ExampleSelectDataset_ScanStruct() {
	type User struct {
		FirstName string `db:"first_name"`
		LastName  string `db:"last_name"`
	}
	db := getDb()
	findUserByName := func(name string) {
		var user User
		ds := db.From("goqu_user").Where(goqu.C("first_name").Eq(name))
		found, err := ds.ScanStruct(&user)
		switch {
		case err != nil:
			fmt.Println(err.Error())
		case !found:
			fmt.Printf("No user found for first_name %s\n", name)
		default:
			fmt.Printf("Found user: %+v\n", user)
		}
	}

	findUserByName("Bob")
	findUserByName("Zeb")

	// Output:
	// Found user: {FirstName:Bob LastName:Yukon}
	// No user found for first_name Zeb
}

func ExampleSelectDataset_ScanVals() {
	var ids []int64
	if err := getDb().From("goqu_user").Select("id").ScanVals(&ids); err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Printf("UserIds = %+v", ids)

	// Output:
	// UserIds = [1 2 3 4]
}

func ExampleSelectDataset_ScanVal() {

	db := getDb()
	findUserIDByName := func(name string) {
		var id int64
		ds := db.From("goqu_user").
			Select("id").
			Where(goqu.C("first_name").Eq(name))

		found, err := ds.ScanVal(&id)
		switch {
		case err != nil:
			fmt.Println(err.Error())
		case !found:
			fmt.Printf("No id found for user %s", name)
		default:
			fmt.Printf("\nFound userId: %+v\n", id)
		}
	}

	findUserIDByName("Bob")
	findUserIDByName("Zeb")
	// Output:
	// Found userId: 1
	// No id found for user Zeb
}

func ExampleSelectDataset_Count() {

	if count, err := getDb().From("goqu_user").Count(); err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Printf("\nCount:= %d", count)
	}

	// Output:
	// Count:= 4
}

func ExampleSelectDataset_Pluck() {
	var lastNames []string
	if err := getDb().From("goqu_user").Pluck(&lastNames, "last_name"); err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Printf("LastNames := %+v", lastNames)

	// Output:
	// LastNames := [Yukon Yukon Yukon Doe]
}
