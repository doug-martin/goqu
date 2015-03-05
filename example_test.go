package goqu_test

import (
	"database/sql"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/doug-martin/goqu"
	"regexp"
)

var driver *sql.DB

func init() {
	db, _ := sqlmock.New()
	driver = db
}

func ExampleOr() {
	db := goqu.New("default", driver)
	sql, _ := db.From("test").Where(
		goqu.Or(
			goqu.I("a").Gt(10),
			goqu.I("a").Lt(5),
		),
	).Sql()
	fmt.Println(sql)
	// Output: SELECT * FROM "test" WHERE (("a" > 10) OR ("a" < 5))
}

func ExampleOr_withAnd() {
	db := goqu.New("default", driver)
	sql, _ := db.From("items").Where(
		goqu.Or(
			goqu.I("a").Gt(10),
			goqu.And(
				goqu.I("b").Eq(100),
				goqu.I("c").Neq("test"),
			),
		),
	).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "items" WHERE (("a" > 10) OR (("b" = 100) AND ("c" != 'test')))
}

func ExampleAnd() {
	db := goqu.New("default", driver)
	//by default Where assumes an And
	sql, _ := db.From("test").Where(
		goqu.I("a").Gt(10),
		goqu.I("b").Lt(5),
	).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" WHERE (("a" > 10) AND ("b" < 5))
}

func ExampleAnd_withOr() {
	db := goqu.New("default", driver)
	sql, _ := db.From("test").Where(
		goqu.I("a").Gt(10),
		goqu.Or(
			goqu.I("b").Lt(5),
			goqu.I("c").In([]string{"hello", "world"}),
		),
	).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" WHERE (("a" > 10) AND (("b" < 5) OR ("c" IN ('hello', 'world'))))
}

func ExampleI() {
	db := goqu.New("default", driver)
	sql, _ := db.From("test").Where(
		goqu.I("a").Eq(10),
		goqu.I("b").Lt(10),
		goqu.I("d").IsTrue(),
	).Sql()
	fmt.Println(sql)

	//qualify with schema
	sql, _ = db.From(goqu.I("test").Schema("my_schema")).Sql()
	fmt.Println(sql)

	sql, _ = db.From(goqu.I("mychema.test")).Where(
		//qualify with schema, table, and col
		goqu.I("my_schema.test.a").Eq(10),
	).Sql()
	fmt.Println(sql)

	//* will be taken literally and no quoted
	sql, _ = db.From(goqu.I("test")).Select(goqu.I("test.*")).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" WHERE (("a" = 10) AND ("b" < 10) AND ("d" IS TRUE))
	// SELECT * FROM "my_schema"."test"
	// SELECT * FROM "mychema"."test" WHERE ("my_schema"."test"."a" = 10)
	// SELECT "test".* FROM "test"

}

func ExampleAliasMethods() {
	db := goqu.New("default", driver)
	sql, _ := db.From("test").Select(goqu.I("a").As("as_a")).Sql()
	fmt.Println(sql)

	sql, _ = db.From("test").Select(goqu.COUNT("*").As("count")).Sql()
	fmt.Println(sql)

	sql, _ = db.From("test").Select(goqu.L("sum(amount)").As("total_amount")).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT "a" AS "as_a" FROM "test"
	// SELECT COUNT(*) AS "count" FROM "test"
	// SELECT sum(amount) AS "total_amount" FROM "test"

}

func ExampleComparisonMethods() {
	db := goqu.New("default", driver)
	sql, _ := db.From("test").Where(goqu.I("a").Eq(10)).Sql()
	fmt.Println(sql)

	sql, _ = db.From("test").Where(goqu.I("a").Neq(10)).Sql()
	fmt.Println(sql)

	sql, _ = db.From("test").Where(goqu.I("a").Gt(10)).Sql()
	fmt.Println(sql)

	sql, _ = db.From("test").Where(goqu.I("a").Gte(10)).Sql()
	fmt.Println(sql)

	sql, _ = db.From("test").Where(goqu.I("a").Lt(10)).Sql()
	fmt.Println(sql)

	sql, _ = db.From("test").Where(goqu.I("a").Lte(10)).Sql()
	fmt.Println(sql)

	sql, _ = db.From("test").Where(goqu.L("(a + b)").Eq(10)).Sql()
	fmt.Println(sql)

	sql, _ = db.From("test").Where(goqu.L("(a + b)").Neq(10)).Sql()
	fmt.Println(sql)

	sql, _ = db.From("test").Where(goqu.L("(a + b)").Gt(10)).Sql()
	fmt.Println(sql)

	sql, _ = db.From("test").Where(goqu.L("(a + b)").Gte(10)).Sql()
	fmt.Println(sql)

	sql, _ = db.From("test").Where(goqu.L("(a + b)").Lt(10)).Sql()
	fmt.Println(sql)

	sql, _ = db.From("test").Where(goqu.L("(a + b)").Lte(10)).Sql()
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
	db := goqu.New("default", driver)
	sql, _ := db.From("test").Where(goqu.I("a").In("a", "b", "c")).Sql()
	fmt.Println(sql)

	//with a slice
	sql, _ = db.From("test").Where(goqu.I("a").In([]string{"a", "b", "c"})).Sql()
	fmt.Println(sql)

	sql, _ = db.From("test").Where(goqu.I("a").NotIn("a", "b", "c")).Sql()
	fmt.Println(sql)

	//with a slice
	sql, _ = db.From("test").Where(goqu.I("a").NotIn([]string{"a", "b", "c"})).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" WHERE ("a" IN ('a', 'b', 'c'))
	// SELECT * FROM "test" WHERE ("a" IN ('a', 'b', 'c'))
	// SELECT * FROM "test" WHERE ("a" NOT IN ('a', 'b', 'c'))
	// SELECT * FROM "test" WHERE ("a" NOT IN ('a', 'b', 'c'))
}

func ExampleOrderedMethods() {
	db := goqu.New("default", driver)
	sql, _ := db.From("test").Order(goqu.I("a").Asc()).Sql()
	fmt.Println(sql)

	sql, _ = db.From("test").Order(goqu.I("a").Desc()).Sql()
	fmt.Println(sql)

	sql, _ = db.From("test").Order(goqu.I("a").Desc().NullsFirst()).Sql()
	fmt.Println(sql)

	sql, _ = db.From("test").Order(goqu.I("a").Desc().NullsLast()).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" ORDER BY "a" ASC
	// SELECT * FROM "test" ORDER BY "a" DESC
	// SELECT * FROM "test" ORDER BY "a" DESC NULLS FIRST
	// SELECT * FROM "test" ORDER BY "a" DESC NULLS LAST
}

func ExampleStringMethods() {
	db := goqu.New("default", driver)
	sql, _ := db.From("test").Where(goqu.I("a").Like("%a%")).Sql()
	fmt.Println(sql)

	sql, _ = db.From("test").Where(goqu.I("a").Like(regexp.MustCompile("(a|b)"))).Sql()
	fmt.Println(sql)

	sql, _ = db.From("test").Where(goqu.I("a").NotLike("%a%")).Sql()
	fmt.Println(sql)

	sql, _ = db.From("test").Where(goqu.I("a").NotLike(regexp.MustCompile("(a|b)"))).Sql()
	fmt.Println(sql)

	sql, _ = db.From("test").Where(goqu.I("a").ILike("%a%")).Sql()
	fmt.Println(sql)

	sql, _ = db.From("test").Where(goqu.I("a").ILike(regexp.MustCompile("(a|b)"))).Sql()
	fmt.Println(sql)

	sql, _ = db.From("test").Where(goqu.I("a").NotILike("%a%")).Sql()
	fmt.Println(sql)

	sql, _ = db.From("test").Where(goqu.I("a").NotILike(regexp.MustCompile("(a|b)"))).Sql()
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
	db := goqu.New("default", driver)
	sql, _ := db.From("test").Where(goqu.I("a").Is(nil)).Sql()
	fmt.Println(sql)

	sql, _ = db.From("test").Where(goqu.I("a").Is(true)).Sql()
	fmt.Println(sql)

	sql, _ = db.From("test").Where(goqu.I("a").Is(false)).Sql()
	fmt.Println(sql)

	sql, _ = db.From("test").Where(goqu.I("a").IsNot(nil)).Sql()
	fmt.Println(sql)

	sql, _ = db.From("test").Where(goqu.I("a").IsNot(true)).Sql()
	fmt.Println(sql)

	sql, _ = db.From("test").Where(goqu.I("a").IsNull(), goqu.I("b").IsNull()).Sql()
	fmt.Println(sql)

	sql, _ = db.From("test").Where(goqu.I("a").IsTrue(), goqu.I("b").IsNotTrue()).Sql()
	fmt.Println(sql)

	sql, _ = db.From("test").Where(goqu.I("a").IsFalse(), goqu.I("b").IsNotFalse()).Sql()
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
	db := goqu.New("default", driver)
	sql, _ := db.From("test").Where(goqu.I("json1").Cast("TEXT").Neq(goqu.I("json2").Cast("TEXT"))).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" WHERE (CAST("json1" AS TEXT) != CAST("json2" AS TEXT))
}

func ExampleCast() {
	db := goqu.New("default", driver)
	sql, _ := db.From("test").Where(goqu.I("json1").Cast("TEXT").Neq(goqu.I("json2").Cast("TEXT"))).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" WHERE (CAST("json1" AS TEXT) != CAST("json2" AS TEXT))
}

func ExampleDistinctMethods() {
	db := goqu.New("default", driver)
	sql, _ := db.From("test").Select(goqu.COUNT(goqu.I("a").Distinct())).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT COUNT(DISTINCT("a")) FROM "test"
}

func ExampleL() {
	db := goqu.New("default", driver)
	sql, _ := db.From("test").Where(goqu.L("a = 1")).Sql()
	fmt.Println(sql)

	sql, _ = db.From("test").Where(goqu.L("a = 1 AND (b = ? OR ? = ?)", "a", goqu.I("c"), 0.01)).Sql()
	fmt.Println(sql)

	sql, _ = db.From("test").Where(
		goqu.L(
			"(? AND ?) OR ?",
			goqu.I("a").Eq(1),
			goqu.I("b").Eq("b"),
			goqu.I("c").In([]string{"a", "b", "c"}),
		),
	).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" WHERE a = 1
	// SELECT * FROM "test" WHERE a = 1 AND (b = 'a' OR "c" = 0.01)
	// SELECT * FROM "test" WHERE (("a" = 1) AND ("b" = 'b')) OR ("c" IN ('a', 'b', 'c'))
}

func ExampleOn() {
	db := goqu.New("default", driver)
	sql, _ := db.From("test").Join(
		goqu.I("my_table"),
		goqu.On(goqu.I("my_table.fkey").Eq(goqu.I("test.id"))),
	).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" INNER JOIN "my_table" ON ("my_table"."fkey" = "test"."id")
}

func ExampleUsing() {
	db := goqu.New("default", driver)
	sql, _ := db.From("test").Join(goqu.I("my_table"), goqu.Using(goqu.I("common_column"))).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" INNER JOIN "my_table" USING ("common_column")
}

func ExampleDataset_ToUpdateSql() {
	db := goqu.New("default", driver)
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, args, _ := db.From("items").ToUpdateSql(
		false,
		item{Name: "Test", Address: "111 Test Addr"},
	)
	fmt.Printf("\n%s %+v)", sql, args)

	sql, args, _ = db.From("items").ToUpdateSql(
		true,
		item{Name: "Test", Address: "111 Test Addr"},
	)
	fmt.Printf("\n%s %+v)", sql, args)

	sql, args, _ = db.From("items").ToUpdateSql(
		false,
		goqu.Record{"name": "Test", "address": "111 Test Addr"},
	)
	fmt.Printf("\n%s %+v)", sql, args)

	sql, args, _ = db.From("items").ToUpdateSql(
		true,
		goqu.Record{"name": "Test", "address": "111 Test Addr"},
	)
	fmt.Printf("\n%s %+v)", sql, args)

	sql, args, _ = db.From("items").ToUpdateSql(
		false,
		map[string]interface{}{"name": "Test", "address": "111 Test Addr"},
	)
	fmt.Printf("\n%s %+v)", sql, args)

	sql, args, _ = db.From("items").ToUpdateSql(
		true,
		map[string]interface{}{"name": "Test", "address": "111 Test Addr"},
	)
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
	db := goqu.New("default", driver)
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, _ := db.From("items").UpdateSql(item{Name: "Test", Address: "111 Test Addr"})
	fmt.Println(sql)

	sql, _ = db.From("items").UpdateSql(goqu.Record{"name": "Test", "address": "111 Test Addr"})
	fmt.Println(sql)
	// Output:
	// UPDATE "items" SET "address"='111 Test Addr',"name"='Test'
	// UPDATE "items" SET "address"='111 Test Addr',"name"='Test'
}

func ExampleDataset_ToSql() {
	db := goqu.New("default", driver)
	sql, args, _ := db.From("items").Where(goqu.I("a").Eq(1)).ToSql(false)
	fmt.Printf("\n%s %+v)", sql, args)
	// Output:

	sql, args, _ = db.From("items").Where(goqu.I("a").Eq(1)).ToSql(true)
	fmt.Printf("\n%s %+v)", sql, args)
	// Output:
	// SELECT * FROM "items" WHERE ("a" = 1) [])
	// SELECT * FROM "items" WHERE ("a" = ?) [1])
}

func ExampleDataset_Sql() {
	db := goqu.New("default", driver)
	sql, _ := db.From("items").Where(goqu.I("a").Eq(1)).Sql()
	fmt.Println(sql)
	// Output: SELECT * FROM "items" WHERE ("a" = 1)
}

func ExampleDataset_As() {
	db := goqu.New("default", driver)
	ds := db.From("test").As("t")
	sql, _ := db.From(ds).Sql()
	fmt.Println(sql)
	// Output: SELECT * FROM (SELECT * FROM "test") AS "t"
}

func ExampleDataset_Returning() {
	db := goqu.New("default", driver)
	sql, _ := db.From("test").
		Returning("id").
		InsertSql(goqu.Record{"a": "a", "b": "b"})
	fmt.Println(sql)
	sql, _ = db.From("test").
		Returning(goqu.I("test.*")).
		InsertSql(goqu.Record{"a": "a", "b": "b"})
	fmt.Println(sql)
	sql, _ = db.From("test").
		Returning("a", "b").
		InsertSql(goqu.Record{"a": "a", "b": "b"})
	fmt.Println(sql)
	// Output:
	// INSERT INTO "test" ("a", "b") VALUES ('a', 'b') RETURNING "id"
	// INSERT INTO "test" ("a", "b") VALUES ('a', 'b') RETURNING "test".*
	// INSERT INTO "test" ("a", "b") VALUES ('a', 'b') RETURNING "a", "b"
}

func ExampleDataset_Union() {
	db := goqu.New("default", driver)
	sql, _ := db.From("test").
		Union(db.From("test2")).
		Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").
		Limit(1).
		Union(db.From("test2")).
		Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").
		Limit(1).
		Union(db.From("test2").
		Order(goqu.I("id").Desc())).
		Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" UNION (SELECT * FROM "test2")
	// SELECT * FROM (SELECT * FROM "test" LIMIT 1) AS "t1" UNION (SELECT * FROM "test2")
	// SELECT * FROM (SELECT * FROM "test" LIMIT 1) AS "t1" UNION (SELECT * FROM (SELECT * FROM "test2" ORDER BY "id" DESC) AS "t1")
}

func ExampleDataset_UnionAll() {
	db := goqu.New("default", driver)
	sql, _ := db.From("test").
		UnionAll(db.From("test2")).
		Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").
		Limit(1).
		UnionAll(db.From("test2")).
		Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").
		Limit(1).
		UnionAll(db.From("test2").
		Order(goqu.I("id").Desc())).
		Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" UNION ALL (SELECT * FROM "test2")
	// SELECT * FROM (SELECT * FROM "test" LIMIT 1) AS "t1" UNION ALL (SELECT * FROM "test2")
	// SELECT * FROM (SELECT * FROM "test" LIMIT 1) AS "t1" UNION ALL (SELECT * FROM (SELECT * FROM "test2" ORDER BY "id" DESC) AS "t1")
}

func ExampleDataset_Intersect() {
	db := goqu.New("default", driver)
	sql, _ := db.From("test").
		Intersect(db.From("test2")).
		Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").
		Limit(1).
		Intersect(db.From("test2")).
		Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").
		Limit(1).
		Intersect(db.From("test2").
		Order(goqu.I("id").Desc())).
		Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" INTERSECT (SELECT * FROM "test2")
	// SELECT * FROM (SELECT * FROM "test" LIMIT 1) AS "t1" INTERSECT (SELECT * FROM "test2")
	// SELECT * FROM (SELECT * FROM "test" LIMIT 1) AS "t1" INTERSECT (SELECT * FROM (SELECT * FROM "test2" ORDER BY "id" DESC) AS "t1")
}

func ExampleDataset_IntersectAll() {
	db := goqu.New("default", driver)
	sql, _ := db.From("test").
		IntersectAll(db.From("test2")).
		Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").
		Limit(1).
		IntersectAll(db.From("test2")).
		Sql()
	fmt.Println(sql)
	sql, _ = goqu.
		From("test").
		Limit(1).
		IntersectAll(db.From("test2").
		Order(goqu.I("id").Desc())).
		Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" INTERSECT ALL (SELECT * FROM "test2")
	// SELECT * FROM (SELECT * FROM "test" LIMIT 1) AS "t1" INTERSECT ALL (SELECT * FROM "test2")
	// SELECT * FROM (SELECT * FROM "test" LIMIT 1) AS "t1" INTERSECT ALL (SELECT * FROM (SELECT * FROM "test2" ORDER BY "id" DESC) AS "t1")
}

func ExampleDataset_ClearOffset() {
	db := goqu.New("default", driver)
	ds := db.From("test").
		Offset(2)
	sql, _ := ds.
		ClearOffset().
		Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test"
}

func ExampleDataset_Offset() {
	db := goqu.New("default", driver)
	ds := db.From("test").
		Offset(2)
	sql, _ := ds.Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" OFFSET 2
}

func ExampleDataset_Limit() {
	db := goqu.New("default", driver)
	ds := db.From("test").Limit(10)
	sql, _ := ds.Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" LIMIT 10
}

func ExampleDataset_LimitAll() {
	db := goqu.New("default", driver)
	ds := db.From("test").LimitAll()
	sql, _ := ds.Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" LIMIT ALL
}

func ExampleDataset_ClearLimit() {
	db := goqu.New("default", driver)
	ds := db.From("test").Limit(10)
	sql, _ := ds.ClearLimit().Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test"
}

func ExampleDataset_Order() {
	db := goqu.New("default", driver)
	ds := db.From("test").
		Order(goqu.I("a").Asc())
	sql, _ := ds.Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" ORDER BY "a" ASC
}

func ExampleDataset_OrderAppend() {
	db := goqu.New("default", driver)
	ds := db.From("test").Order(goqu.I("a").Asc())
	sql, _ := ds.OrderAppend(goqu.I("b").Desc().NullsLast()).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" ORDER BY "a" ASC, "b" DESC NULLS LAST
}

func ExampleDataset_ClearOrder() {
	db := goqu.New("default", driver)
	ds := db.From("test").Order(goqu.I("a").Asc())
	sql, _ := ds.ClearOrder().Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test"
}

func ExampleDataset_Having() {
	db := goqu.New("default", driver)
	sql, _ := db.From("test").Having(goqu.SUM("income").Gt(1000)).Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").GroupBy("age").Having(goqu.SUM("income").Gt(1000)).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" HAVING (SUM("income") > 1000)
	// SELECT * FROM "test" GROUP BY "age" HAVING (SUM("income") > 1000)
}

func ExampleDataset_Where() {
	db := goqu.New("default", driver)
	//By default everyting is added together
	sql, _ := db.From("test").Where(
		goqu.I("a").Gt(10),
		goqu.I("b").Lt(10),
		goqu.I("c").IsNull(),
	).Sql()
	fmt.Println(sql)

	//You can use a combination of Ors and Ands
	sql, _ = db.From("test").Where(
		goqu.Or(
			goqu.I("a").Gt(10),
			goqu.And(
				goqu.I("b").Lt(10),
				goqu.I("c").IsNull(),
			),
		),
	).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" WHERE (("a" > 10) AND ("b" < 10) AND ("c" IS NULL))
	// SELECT * FROM "test" WHERE (("a" > 10) OR (("b" < 10) AND ("c" IS NULL)))
}

func ExampleDataset_ClearWhere() {
	db := goqu.New("default", driver)
	ds := db.From("test").Where(
		goqu.Or(
			goqu.I("a").Gt(10),
			goqu.And(
				goqu.I("b").Lt(10),
				goqu.I("c").IsNull(),
			),
		),
	)
	sql, _ := ds.ClearWhere().Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test"
}

func ExampleDataset_Join() {
	db := goqu.New("default", driver)
	sql, _ := db.From("test").Join(goqu.I("test2"), goqu.On(goqu.I("test.fkey").Eq(goqu.I("test2.Id")))).Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").Join(goqu.I("test2"), goqu.Using("common_column")).Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").Join(db.From("test2").Where(goqu.I("amount").Gt(0)), goqu.On(goqu.I("test.fkey").Eq(goqu.I("test2.Id")))).Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").Join(db.From("test2").Where(goqu.I("amount").Gt(0)).As("t"), goqu.On(goqu.I("test.fkey").Eq(goqu.I("t.Id")))).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" INNER JOIN "test2" ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" INNER JOIN "test2" USING ("common_column")
	// SELECT * FROM "test" INNER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" INNER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t" ON ("test"."fkey" = "t"."Id")

}

func ExampleDataset_InnerJoin() {
	db := goqu.New("default", driver)
	sql, _ := db.From("test").InnerJoin(goqu.I("test2"), goqu.On(goqu.I("test.fkey").Eq(goqu.I("test2.Id")))).Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").InnerJoin(goqu.I("test2"), goqu.Using("common_column")).Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").InnerJoin(db.From("test2").Where(goqu.I("amount").Gt(0)), goqu.On(goqu.I("test.fkey").Eq(goqu.I("test2.Id")))).Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").InnerJoin(db.From("test2").Where(goqu.I("amount").Gt(0)).As("t"), goqu.On(goqu.I("test.fkey").Eq(goqu.I("t.Id")))).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" INNER JOIN "test2" ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" INNER JOIN "test2" USING ("common_column")
	// SELECT * FROM "test" INNER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" INNER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t" ON ("test"."fkey" = "t"."Id")
}
func ExampleDataset_FullOuterJoin() {
	db := goqu.New("default", driver)
	sql, _ := db.From("test").FullOuterJoin(goqu.I("test2"), goqu.On(goqu.I("test.fkey").Eq(goqu.I("test2.Id")))).Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").FullOuterJoin(goqu.I("test2"), goqu.Using("common_column")).Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").FullOuterJoin(db.From("test2").Where(goqu.I("amount").Gt(0)), goqu.On(goqu.I("test.fkey").Eq(goqu.I("test2.Id")))).Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").FullOuterJoin(db.From("test2").Where(goqu.I("amount").Gt(0)).As("t"), goqu.On(goqu.I("test.fkey").Eq(goqu.I("t.Id")))).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" FULL OUTER JOIN "test2" ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" FULL OUTER JOIN "test2" USING ("common_column")
	// SELECT * FROM "test" FULL OUTER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" FULL OUTER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t" ON ("test"."fkey" = "t"."Id")
}
func ExampleDataset_RightOuterJoin() {
	db := goqu.New("default", driver)
	sql, _ := db.From("test").RightOuterJoin(goqu.I("test2"), goqu.On(goqu.I("test.fkey").Eq(goqu.I("test2.Id")))).Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").RightOuterJoin(goqu.I("test2"), goqu.Using("common_column")).Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").RightOuterJoin(
		db.From("test2").Where(goqu.I("amount").Gt(0)),
		goqu.On(goqu.I("test.fkey").Eq(goqu.I("test2.Id"))),
	).Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").RightOuterJoin(
		db.From("test2").Where(goqu.I("amount").Gt(0)).As("t"),
		goqu.On(goqu.I("test.fkey").Eq(goqu.I("t.Id"))),
	).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" RIGHT OUTER JOIN "test2" ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" RIGHT OUTER JOIN "test2" USING ("common_column")
	// SELECT * FROM "test" RIGHT OUTER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" RIGHT OUTER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t" ON ("test"."fkey" = "t"."Id")
}
func ExampleDataset_LeftOuterJoin() {
	db := goqu.New("default", driver)
	sql, _ := db.From("test").LeftOuterJoin(goqu.I("test2"), goqu.On(goqu.I("test.fkey").Eq(goqu.I("test2.Id")))).Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").LeftOuterJoin(goqu.I("test2"), goqu.Using("common_column")).Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").LeftOuterJoin(db.From("test2").Where(goqu.I("amount").Gt(0)), goqu.On(goqu.I("test.fkey").Eq(goqu.I("test2.Id")))).Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").LeftOuterJoin(db.From("test2").Where(goqu.I("amount").Gt(0)).As("t"), goqu.On(goqu.I("test.fkey").Eq(goqu.I("t.Id")))).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" LEFT OUTER JOIN "test2" ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" LEFT OUTER JOIN "test2" USING ("common_column")
	// SELECT * FROM "test" LEFT OUTER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" LEFT OUTER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t" ON ("test"."fkey" = "t"."Id")
}
func ExampleDataset_FullJoin() {
	db := goqu.New("default", driver)
	sql, _ := db.From("test").FullJoin(goqu.I("test2"), goqu.On(goqu.I("test.fkey").Eq(goqu.I("test2.Id")))).Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").FullJoin(goqu.I("test2"), goqu.Using("common_column")).Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").FullJoin(db.From("test2").Where(goqu.I("amount").Gt(0)), goqu.On(goqu.I("test.fkey").Eq(goqu.I("test2.Id")))).Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").FullJoin(db.From("test2").Where(goqu.I("amount").Gt(0)).As("t"), goqu.On(goqu.I("test.fkey").Eq(goqu.I("t.Id")))).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" FULL JOIN "test2" ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" FULL JOIN "test2" USING ("common_column")
	// SELECT * FROM "test" FULL JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" FULL JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t" ON ("test"."fkey" = "t"."Id")
}
func ExampleDataset_RightJoin() {
	db := goqu.New("default", driver)
	sql, _ := db.From("test").RightJoin(goqu.I("test2"), goqu.On(goqu.I("test.fkey").Eq(goqu.I("test2.Id")))).Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").RightJoin(goqu.I("test2"), goqu.Using("common_column")).Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").RightJoin(db.From("test2").Where(goqu.I("amount").Gt(0)), goqu.On(goqu.I("test.fkey").Eq(goqu.I("test2.Id")))).Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").RightJoin(db.From("test2").Where(goqu.I("amount").Gt(0)).As("t"), goqu.On(goqu.I("test.fkey").Eq(goqu.I("t.Id")))).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" RIGHT JOIN "test2" ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" RIGHT JOIN "test2" USING ("common_column")
	// SELECT * FROM "test" RIGHT JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" RIGHT JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t" ON ("test"."fkey" = "t"."Id")
}
func ExampleDataset_LeftJoin() {
	db := goqu.New("default", driver)
	sql, _ := db.From("test").LeftJoin(goqu.I("test2"), goqu.On(goqu.I("test.fkey").Eq(goqu.I("test2.Id")))).Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").LeftJoin(goqu.I("test2"), goqu.Using("common_column")).Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").LeftJoin(db.From("test2").Where(goqu.I("amount").Gt(0)), goqu.On(goqu.I("test.fkey").Eq(goqu.I("test2.Id")))).Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").LeftJoin(db.From("test2").Where(goqu.I("amount").Gt(0)).As("t"), goqu.On(goqu.I("test.fkey").Eq(goqu.I("t.Id")))).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" LEFT JOIN "test2" ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" LEFT JOIN "test2" USING ("common_column")
	// SELECT * FROM "test" LEFT JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" LEFT JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t" ON ("test"."fkey" = "t"."Id")
}
func ExampleDataset_NaturalJoin() {
	db := goqu.New("default", driver)
	sql, _ := db.From("test").NaturalJoin(goqu.I("test2")).Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").NaturalJoin(db.From("test2").Where(goqu.I("amount").Gt(0))).Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").NaturalJoin(db.From("test2").Where(goqu.I("amount").Gt(0)).As("t")).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" NATURAL JOIN "test2"
	// SELECT * FROM "test" NATURAL JOIN (SELECT * FROM "test2" WHERE ("amount" > 0))
	// SELECT * FROM "test" NATURAL JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t"
}
func ExampleDataset_NaturalLeftJoin() {
	db := goqu.New("default", driver)
	sql, _ := db.From("test").NaturalLeftJoin(goqu.I("test2")).Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").NaturalLeftJoin(db.From("test2").Where(goqu.I("amount").Gt(0))).Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").NaturalLeftJoin(db.From("test2").Where(goqu.I("amount").Gt(0)).As("t")).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" NATURAL LEFT JOIN "test2"
	// SELECT * FROM "test" NATURAL LEFT JOIN (SELECT * FROM "test2" WHERE ("amount" > 0))
	// SELECT * FROM "test" NATURAL LEFT JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t"
}
func ExampleDataset_NaturalRightJoin() {
	db := goqu.New("default", driver)
	sql, _ := db.From("test").NaturalRightJoin(goqu.I("test2")).Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").NaturalRightJoin(db.From("test2").Where(goqu.I("amount").Gt(0))).Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").NaturalRightJoin(db.From("test2").Where(goqu.I("amount").Gt(0)).As("t")).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" NATURAL RIGHT JOIN "test2"
	// SELECT * FROM "test" NATURAL RIGHT JOIN (SELECT * FROM "test2" WHERE ("amount" > 0))
	// SELECT * FROM "test" NATURAL RIGHT JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t"
}
func ExampleDataset_NaturalFullJoin() {
	db := goqu.New("default", driver)
	sql, _ := db.From("test").NaturalFullJoin(goqu.I("test2")).Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").NaturalFullJoin(db.From("test2").Where(goqu.I("amount").Gt(0))).Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").NaturalFullJoin(db.From("test2").Where(goqu.I("amount").Gt(0)).As("t")).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" NATURAL FULL JOIN "test2"
	// SELECT * FROM "test" NATURAL FULL JOIN (SELECT * FROM "test2" WHERE ("amount" > 0))
	// SELECT * FROM "test" NATURAL FULL JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t"
}

func ExampleDataset_CrossJoin() {
	db := goqu.New("default", driver)
	sql, _ := db.From("test").CrossJoin(goqu.I("test2")).Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").CrossJoin(db.From("test2").Where(goqu.I("amount").Gt(0))).Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").CrossJoin(db.From("test2").Where(goqu.I("amount").Gt(0)).As("t")).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" CROSS JOIN "test2"
	// SELECT * FROM "test" CROSS JOIN (SELECT * FROM "test2" WHERE ("amount" > 0))
	// SELECT * FROM "test" CROSS JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t"
}

func ExampleDataset_FromSelf() {
	db := goqu.New("default", driver)
	sql, _ := db.From("test").FromSelf().Sql()
	fmt.Println(sql)
	sql, _ = db.From("test").As("my_test_table").FromSelf().Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM (SELECT * FROM "test") AS "t1"
	// SELECT * FROM (SELECT * FROM "test") AS "my_test_table"
}

func ExampleDataset_From() {
	db := goqu.New("default", driver)
	ds := db.From("test")
	sql, _ := ds.From("test2").Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test2"
}

func ExampleDataset_From_withDataset() {
	db := goqu.New("default", driver)
	ds := db.From("test")
	fromDs := ds.Where(goqu.I("age").Gt(10))
	sql, _ := ds.From(fromDs).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM (SELECT * FROM "test" WHERE ("age" > 10)) AS "t1"
}

func ExampleDataset_From_withAliasedDataset() {
	db := goqu.New("default", driver)
	ds := db.From("test")
	fromDs := ds.Where(goqu.I("age").Gt(10))
	sql, _ := ds.From(fromDs.As("test2")).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM (SELECT * FROM "test" WHERE ("age" > 10)) AS "test2"
}

func ExampleDataset_Select() {
	db := goqu.New("default", driver)
	sql, _ := db.From("test").Select("a", "b", "c").Sql()
	fmt.Println(sql)
	// Output:
	// SELECT "a", "b", "c" FROM "test"
}

func ExampleDataset_Select_withDataset() {
	db := goqu.New("default", driver)
	ds := db.From("test")
	fromDs := ds.Select("age").Where(goqu.I("age").Gt(10))
	sql, _ := ds.From().Select(fromDs).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT (SELECT "age" FROM "test" WHERE ("age" > 10))
}

func ExampleDataset_Select_withAliasedDataset() {
	db := goqu.New("default", driver)
	ds := db.From("test")
	fromDs := ds.Select("age").Where(goqu.I("age").Gt(10))
	sql, _ := ds.From().Select(fromDs.As("ages")).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT (SELECT "age" FROM "test" WHERE ("age" > 10)) AS "ages"
}

func ExampleDataset_Select_withLiteral() {
	db := goqu.New("default", driver)
	sql, _ := db.From("test").Select(goqu.L("a + b").As("sum")).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT a + b AS "sum" FROM "test"
}

func ExampleDataset_Select_withSqlFunctionExpression() {
	db := goqu.New("default", driver)
	sql, _ := db.From("test").Select(
		goqu.COUNT("*").As("age_count"),
		goqu.MAX("age").As("max_age"),
		goqu.AVG("age").As("avg_age"),
	).Sql()
	fmt.Println(sql)
	// Output:
	// SELECT COUNT(*) AS "age_count", MAX("age") AS "max_age", AVG("age") AS "avg_age" FROM "test"
}

func ExampleDataset_SelectDistinct() {
	db := goqu.New("default", driver)
	sql, _ := db.From("test").SelectDistinct("a", "b").Sql()
	fmt.Println(sql)
	// Output:
	// SELECT DISTINCT "a", "b" FROM "test"
}

func ExampleDataset_SelectAppend() {
	db := goqu.New("default", driver)
	ds := db.From("test").Select("a", "b")
	sql, _ := ds.SelectAppend("c").Sql()
	fmt.Println(sql)
	ds = db.From("test").SelectDistinct("a", "b")
	sql, _ = ds.SelectAppend("c").Sql()
	fmt.Println(sql)
	// Output:
	// SELECT "a", "b", "c" FROM "test"
	// SELECT DISTINCT "a", "b", "c" FROM "test"
}

func ExampleDataset_ClearSelect() {
	db := goqu.New("default", driver)
	ds := db.From("test").Select("a", "b")
	sql, _ := ds.ClearSelect().Sql()
	fmt.Println(sql)
	ds = db.From("test").SelectDistinct("a", "b")
	sql, _ = ds.ClearSelect().Sql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test"
	// SELECT * FROM "test"
}

func ExampleDataset_ToInsertSql() {
	db := goqu.New("default", driver)
	type item struct {
		Id      uint32 `db:"id" goqu:"skipinsert"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, args, _ := db.From("items").ToInsertSql(
		false,
		item{Name: "Test1", Address: "111 Test Addr"},
		item{Name: "Test2", Address: "112 Test Addr"},
	)
	fmt.Printf("\n%s %+v", sql, args)

	sql, args, _ = db.From("items").ToInsertSql(
		false,
		goqu.Record{"name": "Test1", "address": "111 Test Addr"},
		goqu.Record{"name": "Test2", "address": "112 Test Addr"},
	)
	fmt.Printf("\n%s %+v", sql, args)

	sql, args, _ = db.From("items").ToInsertSql(
		false,
		[]item{
			{Name: "Test1", Address: "111 Test Addr"},
			{Name: "Test2", Address: "112 Test Addr"},
		})
	fmt.Printf("\n%s %+v", sql, args)

	sql, args, _ = db.From("items").ToInsertSql(
		false,
		[]goqu.Record{
			{"name": "Test1", "address": "111 Test Addr"},
			{"name": "Test2", "address": "112 Test Addr"},
		})

	sql, args, _ = db.From("items").ToInsertSql(
		true,
		item{Name: "Test1", Address: "111 Test Addr"},
		item{Name: "Test2", Address: "112 Test Addr"},
	)
	fmt.Printf("\n%s %+v", sql, args)

	sql, args, _ = db.From("items").ToInsertSql(
		true,
		goqu.Record{"name": "Test1", "address": "111 Test Addr"},
		goqu.Record{"name": "Test2", "address": "112 Test Addr"},
	)
	fmt.Printf("\n%s %+v", sql, args)

	sql, args, _ = db.From("items").ToInsertSql(
		true,
		[]item{
			{Name: "Test1", Address: "111 Test Addr"},
			{Name: "Test2", Address: "112 Test Addr"},
		})
	fmt.Printf("\n%s %+v", sql, args)

	sql, args, _ = db.From("items").ToInsertSql(
		true,
		[]goqu.Record{
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
	db := goqu.New("default", driver)
	type item struct {
		Id      uint32 `db:"id" goqu:"skipinsert"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, _ := db.From("items").InsertSql(
		item{Name: "Test1", Address: "111 Test Addr"},
		item{Name: "Test2", Address: "112 Test Addr"},
	)
	fmt.Println(sql)

	sql, _ = db.From("items").InsertSql(
		goqu.Record{"name": "Test1", "address": "111 Test Addr"},
		goqu.Record{"name": "Test2", "address": "112 Test Addr"},
	)
	fmt.Println(sql)

	sql, _ = db.From("items").InsertSql([]item{
		{Name: "Test1", Address: "111 Test Addr"},
		{Name: "Test2", Address: "112 Test Addr"},
	})
	fmt.Println(sql)

	sql, _ = db.From("items").InsertSql([]goqu.Record{
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
	db := goqu.New("default", driver)
	sql, args, _ := db.From("items").ToDeleteSql(false)
	fmt.Printf("\n%s %+v", sql, args)

	sql, args, _ = db.From("items").
		Where(goqu.I("id").Gt(10)).
		ToDeleteSql(false)
	fmt.Printf("\n%s %+v", sql, args)

	sql, args, _ = db.From("items").
		Where(goqu.I("id").Gt(10)).
		ToDeleteSql(true)
	fmt.Printf("\n%s %+v", sql, args)

	// Output:
	// DELETE FROM "items" []
	// DELETE FROM "items" WHERE ("id" > 10) []
	// DELETE FROM "items" WHERE ("id" > ?) [10]
}

func ExampleDataset_DeleteSql() {
	db := goqu.New("default", driver)
	sql, _ := db.From("items").DeleteSql()
	fmt.Println(sql)

	sql, _ = db.From("items").
		Where(goqu.I("id").Gt(10)).
		DeleteSql()
	fmt.Println(sql)

	// Output:
	// DELETE FROM "items"
	// DELETE FROM "items" WHERE ("id" > 10)
}

func ExampleDataset_ToTruncateSql() {
	db := goqu.New("default", driver)
	sql, args, _ := db.From("items").
		ToTruncateSql(false, goqu.TruncateOptions{})
	fmt.Printf("\n%s %+v", sql, args)
	sql, args, _ = db.From("items").
		ToTruncateSql(false, goqu.TruncateOptions{Cascade: true})
	fmt.Printf("\n%s %+v", sql, args)
	sql, args, _ = db.From("items").
		ToTruncateSql(false, goqu.TruncateOptions{Restrict: true})
	fmt.Printf("\n%s %+v", sql, args)
	sql, args, _ = db.From("items").
		ToTruncateSql(false, goqu.TruncateOptions{Identity: "RESTART"})
	fmt.Printf("\n%s %+v", sql, args)
	sql, args, _ = db.From("items").
		ToTruncateSql(false, goqu.TruncateOptions{Identity: "RESTART", Cascade: true})
	fmt.Printf("\n%s %+v", sql, args)
	sql, args, _ = db.From("items").
		ToTruncateSql(false, goqu.TruncateOptions{Identity: "RESTART", Restrict: true})
	fmt.Printf("\n%s %+v", sql, args)
	sql, args, _ = db.From("items").
		ToTruncateSql(false, goqu.TruncateOptions{Identity: "CONTINUE"})
	fmt.Printf("\n%s %+v", sql, args)
	sql, args, _ = db.From("items").
		ToTruncateSql(false, goqu.TruncateOptions{Identity: "CONTINUE", Cascade: true})
	fmt.Printf("\n%s %+v", sql, args)
	sql, args, _ = db.From("items").
		ToTruncateSql(false, goqu.TruncateOptions{Identity: "CONTINUE", Restrict: true})
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
	db := goqu.New("default", driver)
	sql, _ := db.From("items").TruncateSql()
	fmt.Println(sql)
	// Output:
	// TRUNCATE "items"
}

func ExampleDataset_TruncateWithOptsSql() {
	db := goqu.New("default", driver)
	sql, _ := db.From("items").
		TruncateWithOptsSql(goqu.TruncateOptions{})
	fmt.Println(sql)
	sql, _ = db.From("items").
		TruncateWithOptsSql(goqu.TruncateOptions{Cascade: true})
	fmt.Println(sql)
	sql, _ = db.From("items").
		TruncateWithOptsSql(goqu.TruncateOptions{Restrict: true})
	fmt.Println(sql)
	sql, _ = db.From("items").
		TruncateWithOptsSql(goqu.TruncateOptions{Identity: "RESTART"})
	fmt.Println(sql)
	sql, _ = db.From("items").
		TruncateWithOptsSql(goqu.TruncateOptions{Identity: "RESTART", Cascade: true})
	fmt.Println(sql)
	sql, _ = db.From("items").
		TruncateWithOptsSql(goqu.TruncateOptions{Identity: "RESTART", Restrict: true})
	fmt.Println(sql)
	sql, _ = db.From("items").
		TruncateWithOptsSql(goqu.TruncateOptions{Identity: "CONTINUE"})
	fmt.Println(sql)
	sql, _ = db.From("items").
		TruncateWithOptsSql(goqu.TruncateOptions{Identity: "CONTINUE", Cascade: true})
	fmt.Println(sql)
	sql, _ = db.From("items").
		TruncateWithOptsSql(goqu.TruncateOptions{Identity: "CONTINUE", Restrict: true})
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
