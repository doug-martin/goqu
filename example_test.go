package goqu_test

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/doug-martin/goqu/v6"

	"github.com/DATA-DOG/go-sqlmock"
)

var driver *sql.DB

func init() {
	db, _, _ := sqlmock.New()
	driver = db
}

func ExampleOr() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("test").Where(goqu.Ex{
		"a": goqu.Op{"gt": 10, "lt": 5},
	}).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Where(
		goqu.Or(
			goqu.I("a").Gt(10),
			goqu.I("a").Lt(5),
		),
	).ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" WHERE (("a" > 10) OR ("a" < 5))
	// SELECT * FROM "test" WHERE (("a" > 10) OR ("a" < 5))
}

func ExampleOr_withAnd() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("items").Where(
		goqu.Or(
			goqu.I("a").Gt(10),
			goqu.Ex{
				"b": 100,
				"c": goqu.Op{"neq": "test"},
			},
		),
	).ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("items").Where(
		goqu.Or(
			goqu.I("a").Gt(10),
			goqu.And(
				goqu.I("b").Eq(100),
				goqu.I("c").Neq("test"),
			),
		),
	).ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "items" WHERE (("a" > 10) OR (("b" = 100) AND ("c" != 'test')))
	// SELECT * FROM "items" WHERE (("a" > 10) OR (("b" = 100) AND ("c" != 'test')))
}

func ExampleAnd() {
	db := goqu.New("default", driver)
	//by default Where assumes an And
	sql, _, _ := db.From("test").Where(goqu.Ex{
		"a": goqu.Op{"gt": 10},
		"b": goqu.Op{"lt": 5},
	}).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Where(
		goqu.I("a").Gt(10),
		goqu.I("b").Lt(5),
	).ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" WHERE (("a" > 10) AND ("b" < 5))
	// SELECT * FROM "test" WHERE (("a" > 10) AND ("b" < 5))
}

func ExampleAnd_withOr() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("test").Where(
		goqu.I("a").Gt(10),
		goqu.Or(
			goqu.I("b").Lt(5),
			goqu.I("c").In([]string{"hello", "world"}),
		),
	).ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" WHERE (("a" > 10) AND (("b" < 5) OR ("c" IN ('hello', 'world'))))
}

func ExampleI() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("test").Where(
		goqu.I("a").Eq(10),
		goqu.I("b").Lt(10),
		goqu.I("d").IsTrue(),
	).ToSql()
	fmt.Println(sql)

	//qualify with schema
	sql, _, _ = db.From(goqu.I("test").Schema("my_schema")).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From(goqu.I("mychema.test")).Where(
		//qualify with schema, table, and col
		goqu.I("my_schema.test.a").Eq(10),
	).ToSql()
	fmt.Println(sql)

	//* will be taken literally and no quoted
	sql, _, _ = db.From(goqu.I("test")).Select(goqu.I("test.*")).ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" WHERE (("a" = 10) AND ("b" < 10) AND ("d" IS TRUE))
	// SELECT * FROM "my_schema"."test"
	// SELECT * FROM "mychema"."test" WHERE ("my_schema"."test"."a" = 10)
	// SELECT "test".* FROM "test"

}

func ExampleAliasMethods() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("test").Select(goqu.I("a").As("as_a")).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Select(goqu.COUNT("*").As("count")).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Select(goqu.L("sum(amount)").As("total_amount")).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Select(goqu.I("a").As(goqu.I("as_a"))).ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT "a" AS "as_a" FROM "test"
	// SELECT COUNT(*) AS "count" FROM "test"
	// SELECT sum(amount) AS "total_amount" FROM "test"
	// SELECT "a" AS "as_a" FROM "test"

}

func ExampleComparisonMethods() {
	db := goqu.New("default", driver)
	//used from an identifier
	sql, _, _ := db.From("test").Where(goqu.I("a").Eq(10)).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Where(goqu.I("a").Neq(10)).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Where(goqu.I("a").Gt(10)).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Where(goqu.I("a").Gte(10)).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Where(goqu.I("a").Lt(10)).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Where(goqu.I("a").Lte(10)).ToSql()
	fmt.Println(sql)
	//used from a literal expression
	sql, _, _ = db.From("test").Where(goqu.L("(a + b)").Eq(10)).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Where(goqu.L("(a + b)").Neq(10)).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Where(goqu.L("(a + b)").Gt(10)).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Where(goqu.L("(a + b)").Gte(10)).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Where(goqu.L("(a + b)").Lt(10)).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Where(goqu.L("(a + b)").Lte(10)).ToSql()
	fmt.Println(sql)

	//used with Ex expression map
	sql, _, _ = db.From("test").Where(goqu.Ex{
		"a": 10,
		"b": goqu.Op{"neq": 10},
		"c": goqu.Op{"gte": 10},
		"d": goqu.Op{"lt": 10},
		"e": goqu.Op{"lte": 10},
	}).ToSql()
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
	// SELECT * FROM "test" WHERE (("a" = 10) AND ("b" != 10) AND ("c" >= 10) AND ("d" < 10) AND ("e" <= 10))
}

func ExampleInMethods() {
	db := goqu.New("default", driver)
	//using identifiers
	sql, _, _ := db.From("test").Where(goqu.I("a").In("a", "b", "c")).ToSql()
	fmt.Println(sql)

	//with a slice
	sql, _, _ = db.From("test").Where(goqu.I("a").In([]string{"a", "b", "c"})).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Where(goqu.I("a").NotIn("a", "b", "c")).ToSql()
	fmt.Println(sql)

	//with a slice
	sql, _, _ = db.From("test").Where(goqu.I("a").NotIn([]string{"a", "b", "c"})).ToSql()
	fmt.Println(sql)

	//using an Ex expression map
	sql, _, _ = db.From("test").Where(goqu.Ex{
		"a": []string{"a", "b", "c"},
	}).ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").Where(goqu.Ex{
		"a": goqu.Op{"notIn": []string{"a", "b", "c"}},
	}).ToSql()
	fmt.Println(sql)

	// Output:
	// SELECT * FROM "test" WHERE ("a" IN ('a', 'b', 'c'))
	// SELECT * FROM "test" WHERE ("a" IN ('a', 'b', 'c'))
	// SELECT * FROM "test" WHERE ("a" NOT IN ('a', 'b', 'c'))
	// SELECT * FROM "test" WHERE ("a" NOT IN ('a', 'b', 'c'))
	// SELECT * FROM "test" WHERE ("a" IN ('a', 'b', 'c'))
	// SELECT * FROM "test" WHERE ("a" NOT IN ('a', 'b', 'c'))
}

func ExampleOrderedMethods() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("test").Order(goqu.I("a").Asc()).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Order(goqu.I("a").Desc()).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Order(goqu.I("a").Desc().NullsFirst()).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Order(goqu.I("a").Desc().NullsLast()).ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" ORDER BY "a" ASC
	// SELECT * FROM "test" ORDER BY "a" DESC
	// SELECT * FROM "test" ORDER BY "a" DESC NULLS FIRST
	// SELECT * FROM "test" ORDER BY "a" DESC NULLS LAST
}

func ExampleRangeMethods() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("test").Where(goqu.I("name").Between(goqu.RangeVal{Start: "a", End: "b"})).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Where(goqu.I("name").NotBetween(goqu.RangeVal{Start: "a", End: "b"})).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Where(goqu.I("x").Between(goqu.RangeVal{Start: goqu.I("y"), End: goqu.I("z")})).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Where(goqu.I("x").NotBetween(goqu.RangeVal{Start: goqu.I("y"), End: goqu.I("z")})).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Where(goqu.L("(a + b)").Between(goqu.RangeVal{Start: 10, End: 100})).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Where(goqu.L("(a + b)").NotBetween(goqu.RangeVal{Start: 10, End: 100})).ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" WHERE ("name" BETWEEN 'a' AND 'b')
	// SELECT * FROM "test" WHERE ("name" NOT BETWEEN 'a' AND 'b')
	// SELECT * FROM "test" WHERE ("x" BETWEEN "y" AND "z")
	// SELECT * FROM "test" WHERE ("x" NOT BETWEEN "y" AND "z")
	// SELECT * FROM "test" WHERE ((a + b) BETWEEN 10 AND 100)
	// SELECT * FROM "test" WHERE ((a + b) NOT BETWEEN 10 AND 100)
}

func ExampleRangeMethods_Ex() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("test").Where(goqu.Ex{"name": goqu.Op{"between": goqu.RangeVal{Start: "a", End: "b"}}}).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Where(goqu.Ex{"name": goqu.Op{"notBetween": goqu.RangeVal{Start: "a", End: "b"}}}).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Where(goqu.Ex{"x": goqu.Op{"between": goqu.RangeVal{Start: goqu.I("y"), End: goqu.I("z")}}}).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Where(goqu.Ex{"x": goqu.Op{"notBetween": goqu.RangeVal{Start: goqu.I("y"), End: goqu.I("z")}}}).ToSql()
	fmt.Println(sql)

	// Output:
	// SELECT * FROM "test" WHERE ("name" BETWEEN 'a' AND 'b')
	// SELECT * FROM "test" WHERE ("name" NOT BETWEEN 'a' AND 'b')
	// SELECT * FROM "test" WHERE ("x" BETWEEN "y" AND "z")
	// SELECT * FROM "test" WHERE ("x" NOT BETWEEN "y" AND "z")
}

func ExampleStringMethods() {
	db := goqu.New("default", driver)
	//using identifiers
	sql, _, _ := db.From("test").Where(goqu.I("a").Like("%a%")).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Where(goqu.I("a").Like(regexp.MustCompile("(a|b)"))).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Where(goqu.I("a").NotLike("%a%")).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Where(goqu.I("a").NotLike(regexp.MustCompile("(a|b)"))).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Where(goqu.I("a").ILike("%a%")).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Where(goqu.I("a").ILike(regexp.MustCompile("(a|b)"))).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Where(goqu.I("a").NotILike("%a%")).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Where(goqu.I("a").NotILike(regexp.MustCompile("(a|b)"))).ToSql()
	fmt.Println(sql)

	//using an Ex expression map
	sql, _, _ = db.From("test").Where(goqu.Ex{
		"a": goqu.Op{"like": "%a%"},
		"b": goqu.Op{"like": regexp.MustCompile("(a|b)")},
		"c": goqu.Op{"iLike": "%a%"},
		"d": goqu.Op{"iLike": regexp.MustCompile("(a|b)")},
		"e": goqu.Op{"notlike": "%a%"},
		"f": goqu.Op{"notLike": regexp.MustCompile("(a|b)")},
		"g": goqu.Op{"notILike": "%a%"},
		"h": goqu.Op{"notILike": regexp.MustCompile("(a|b)")},
	}).ToSql()
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
	// SELECT * FROM "test" WHERE (("a" LIKE '%a%') AND ("b" ~ '(a|b)') AND ("c" ILIKE '%a%') AND ("d" ~* '(a|b)') AND ("e" NOT LIKE '%a%') AND ("f" !~ '(a|b)') AND ("g" NOT ILIKE '%a%') AND ("h" !~* '(a|b)'))
}

func ExampleBooleanMethods() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("test").Where(goqu.I("a").Is(nil)).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Where(goqu.I("a").Is(true)).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Where(goqu.I("a").Is(false)).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Where(goqu.I("a").IsNot(nil)).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Where(goqu.I("a").IsNot(true)).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Where(goqu.I("a").IsNull(), goqu.I("b").IsNull()).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Where(goqu.I("a").IsTrue(), goqu.I("b").IsNotTrue()).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Where(goqu.I("a").IsFalse(), goqu.I("b").IsNotFalse()).ToSql()
	fmt.Println(sql)

	//with an ex expression map
	sql, _, _ = db.From("test").Where(goqu.Ex{
		"a": true,
		"b": false,
		"c": nil,
		"d": goqu.Op{"isNot": true},
		"e": goqu.Op{"isNot": false},
		"f": goqu.Op{"isNot": nil},
	}).ToSql()
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
	// SELECT * FROM "test" WHERE (("a" IS TRUE) AND ("b" IS FALSE) AND ("c" IS NULL) AND ("d" IS NOT TRUE) AND ("e" IS NOT FALSE) AND ("f" IS NOT NULL))
}

func ExampleCastMethods() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("test").Where(goqu.I("json1").Cast("TEXT").Neq(goqu.I("json2").Cast("TEXT"))).ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" WHERE (CAST("json1" AS TEXT) != CAST("json2" AS TEXT))
}

func ExampleCast() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("test").Where(goqu.I("json1").Cast("TEXT").Neq(goqu.I("json2").Cast("TEXT"))).ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" WHERE (CAST("json1" AS TEXT) != CAST("json2" AS TEXT))
}

func ExampleDistinctMethods() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("test").Select(goqu.COUNT(goqu.I("a").Distinct())).ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT COUNT(DISTINCT("a")) FROM "test"
}

func ExampleL() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("test").Where(goqu.L("a = 1")).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Where(goqu.L("a = 1 AND (b = ? OR ? = ?)", "a", goqu.I("c"), 0.01)).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Where(
		goqu.L(
			"(? AND ?) OR ?",
			goqu.I("a").Eq(1),
			goqu.I("b").Eq("b"),
			goqu.I("c").In([]string{"a", "b", "c"}),
		),
	).ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" WHERE a = 1
	// SELECT * FROM "test" WHERE a = 1 AND (b = 'a' OR "c" = 0.01)
	// SELECT * FROM "test" WHERE (("a" = 1) AND ("b" = 'b')) OR ("c" IN ('a', 'b', 'c'))
}

func ExampleOn() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("test").Join(
		goqu.I("my_table"),
		goqu.On(goqu.Ex{"my_table.fkey": goqu.I("test.id")}),
	).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("test").Join(
		goqu.I("my_table"),
		goqu.On(goqu.I("my_table.fkey").Eq(goqu.I("test.id"))),
	).ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" INNER JOIN "my_table" ON ("my_table"."fkey" = "test"."id")
	// SELECT * FROM "test" INNER JOIN "my_table" ON ("my_table"."fkey" = "test"."id")
}

func ExampleUsing() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("test").Join(goqu.I("my_table"), goqu.Using(goqu.I("common_column"))).ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" INNER JOIN "my_table" USING ("common_column")
}

func ExampleDataset_As() {
	db := goqu.New("default", driver)
	ds := db.From("test").As("t")
	sql, _, _ := db.From(ds).ToSql()
	fmt.Println(sql)
	// Output: SELECT * FROM (SELECT * FROM "test") AS "t"
}

func ExampleDataset_Returning() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("test").
		Returning("id").
		ToInsertSql(goqu.Record{"a": "a", "b": "b"})
	fmt.Println(sql)
	sql, _, _ = db.From("test").
		Returning(goqu.I("test.*")).
		ToInsertSql(goqu.Record{"a": "a", "b": "b"})
	fmt.Println(sql)
	sql, _, _ = db.From("test").
		Returning("a", "b").
		ToInsertSql(goqu.Record{"a": "a", "b": "b"})
	fmt.Println(sql)
	// Output:
	// INSERT INTO "test" ("a", "b") VALUES ('a', 'b') RETURNING "id"
	// INSERT INTO "test" ("a", "b") VALUES ('a', 'b') RETURNING "test".*
	// INSERT INTO "test" ("a", "b") VALUES ('a', 'b') RETURNING "a", "b"
}

func ExampleDataset_Union() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("test").
		Union(db.From("test2")).
		ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").
		Limit(1).
		Union(db.From("test2")).
		ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").
		Limit(1).
		Union(db.From("test2").
			Order(goqu.I("id").Desc())).
		ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" UNION (SELECT * FROM "test2")
	// SELECT * FROM (SELECT * FROM "test" LIMIT 1) AS "t1" UNION (SELECT * FROM "test2")
	// SELECT * FROM (SELECT * FROM "test" LIMIT 1) AS "t1" UNION (SELECT * FROM (SELECT * FROM "test2" ORDER BY "id" DESC) AS "t1")
}

func ExampleDataset_UnionAll() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("test").
		UnionAll(db.From("test2")).
		ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").
		Limit(1).
		UnionAll(db.From("test2")).
		ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").
		Limit(1).
		UnionAll(db.From("test2").
			Order(goqu.I("id").Desc())).
		ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" UNION ALL (SELECT * FROM "test2")
	// SELECT * FROM (SELECT * FROM "test" LIMIT 1) AS "t1" UNION ALL (SELECT * FROM "test2")
	// SELECT * FROM (SELECT * FROM "test" LIMIT 1) AS "t1" UNION ALL (SELECT * FROM (SELECT * FROM "test2" ORDER BY "id" DESC) AS "t1")
}

func ExampleDataset_WithCTE() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("one").
		With("one", db.From().Select(goqu.L("1"))).
		Select(goqu.Star()).
		ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("derived").
		With("intermed", db.From("test").Select(goqu.Star()).Where(goqu.I("x").Gte(5))).
		With("derived", db.From("intermed").Select(goqu.Star()).Where(goqu.I("x").Lt(10))).
		Select(goqu.Star()).
		ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("multi").
		With("multi(x,y)", db.From().Select(goqu.L("1"), goqu.L("2"))).
		Select(goqu.I("x"), goqu.I("y")).
		ToSql()
	fmt.Println(sql)
	// Output:
	// WITH one AS (SELECT 1) SELECT * FROM "one"
	// WITH intermed AS (SELECT * FROM "test" WHERE ("x" >= 5)), derived AS (SELECT * FROM "intermed" WHERE ("x" < 10)) SELECT * FROM "derived"
	// WITH multi(x,y) AS (SELECT 1, 2) SELECT "x", "y" FROM "multi"
}

func ExampleDataset_ModifyWithCTE() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("test").
		With("moved_rows", db.From("other").Where(goqu.I("date").Lt(123))).
		ToInsertSql(db.From("moved_rows"))
	fmt.Println(sql)
	sql, _, _ = db.From("test").
		With("check_vals(val)", db.From().Select(goqu.L("123"))).
		Where(goqu.I("val").Eq(db.From("check_vals").Select("val"))).
		ToDeleteSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").
		With("some_vals(val)", db.From().Select(goqu.L("123"))).
		Where(goqu.I("val").Eq(db.From("some_vals").Select("val"))).
		ToUpdateSql(goqu.Record{"name": "Test"})
	fmt.Println(sql)
	// Output:
	// WITH moved_rows AS (SELECT * FROM "other" WHERE ("date" < 123)) INSERT INTO "test" SELECT * FROM "moved_rows"
	// WITH check_vals(val) AS (SELECT 123) DELETE FROM "test" WHERE ("val" IN (SELECT "val" FROM "check_vals"))
	// WITH some_vals(val) AS (SELECT 123) UPDATE "test" SET "name"='Test' WHERE ("val" IN (SELECT "val" FROM "some_vals"))
}

func ExampleDataset_WithCTERecursive() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("nums").
		WithRecursive("nums(x)",
			db.From().Select(goqu.L("1")).
				UnionAll(db.From("nums").
					Select(goqu.L("x+1")).Where(goqu.I("x").Lt(5)))).
		ToSql()
	fmt.Println(sql)
	// Output:
	// WITH RECURSIVE nums(x) AS (SELECT 1 UNION ALL (SELECT x+1 FROM "nums" WHERE ("x" < 5))) SELECT * FROM "nums"
}

func ExampleDataset_Intersect() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("test").
		Intersect(db.From("test2")).
		ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").
		Limit(1).
		Intersect(db.From("test2")).
		ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").
		Limit(1).
		Intersect(db.From("test2").
			Order(goqu.I("id").Desc())).
		ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" INTERSECT (SELECT * FROM "test2")
	// SELECT * FROM (SELECT * FROM "test" LIMIT 1) AS "t1" INTERSECT (SELECT * FROM "test2")
	// SELECT * FROM (SELECT * FROM "test" LIMIT 1) AS "t1" INTERSECT (SELECT * FROM (SELECT * FROM "test2" ORDER BY "id" DESC) AS "t1")
}

func ExampleDataset_IntersectAll() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("test").
		IntersectAll(db.From("test2")).
		ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").
		Limit(1).
		IntersectAll(db.From("test2")).
		ToSql()
	fmt.Println(sql)
	sql, _, _ = goqu.
		From("test").
		Limit(1).
		IntersectAll(db.From("test2").
			Order(goqu.I("id").Desc())).
		ToSql()
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
	sql, _, _ := ds.
		ClearOffset().
		ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test"
}

func ExampleDataset_Offset() {
	db := goqu.New("default", driver)
	ds := db.From("test").
		Offset(2)
	sql, _, _ := ds.ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" OFFSET 2
}

func ExampleDataset_Limit() {
	db := goqu.New("default", driver)
	ds := db.From("test").Limit(10)
	sql, _, _ := ds.ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" LIMIT 10
}

func ExampleDataset_LimitAll() {
	db := goqu.New("default", driver)
	ds := db.From("test").LimitAll()
	sql, _, _ := ds.ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" LIMIT ALL
}

func ExampleDataset_ClearLimit() {
	db := goqu.New("default", driver)
	ds := db.From("test").Limit(10)
	sql, _, _ := ds.ClearLimit().ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test"
}

func ExampleDataset_Order() {
	db := goqu.New("default", driver)
	ds := db.From("test").
		Order(goqu.I("a").Asc())
	sql, _, _ := ds.ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" ORDER BY "a" ASC
}

func ExampleDataset_OrderAppend() {
	db := goqu.New("default", driver)
	ds := db.From("test").Order(goqu.I("a").Asc())
	sql, _, _ := ds.OrderAppend(goqu.I("b").Desc().NullsLast()).ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" ORDER BY "a" ASC, "b" DESC NULLS LAST
}

func ExampleDataset_ClearOrder() {
	db := goqu.New("default", driver)
	ds := db.From("test").Order(goqu.I("a").Asc())
	sql, _, _ := ds.ClearOrder().ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test"
}

func ExampleDataset_Having() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("test").Having(goqu.SUM("income").Gt(1000)).ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").GroupBy("age").Having(goqu.SUM("income").Gt(1000)).ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" HAVING (SUM("income") > 1000)
	// SELECT * FROM "test" GROUP BY "age" HAVING (SUM("income") > 1000)
}

func ExampleDataset_Where() {
	db := goqu.New("default", driver)

	//By default everything is anded together
	sql, _, _ := db.From("test").Where(goqu.Ex{
		"a": goqu.Op{"gt": 10},
		"b": goqu.Op{"lt": 10},
		"c": nil,
		"d": []string{"a", "b", "c"},
	}).ToSql()
	fmt.Println(sql)

	//You can use ExOr to get ORed expressions together
	sql, _, _ = db.From("test").Where(goqu.ExOr{
		"a": goqu.Op{"gt": 10},
		"b": goqu.Op{"lt": 10},
		"c": nil,
		"d": []string{"a", "b", "c"},
	}).ToSql()
	fmt.Println(sql)

	//You can use Or with Ex to Or multiple Ex maps together
	sql, _, _ = db.From("test").Where(
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
	).ToSql()
	fmt.Println(sql)

	//By default everything is anded together
	sql, _, _ = db.From("test").Where(
		goqu.I("a").Gt(10),
		goqu.I("b").Lt(10),
		goqu.I("c").IsNull(),
		goqu.I("d").In("a", "b", "c"),
	).ToSql()
	fmt.Println(sql)

	//You can use a combination of Ors and Ands
	sql, _, _ = db.From("test").Where(
		goqu.Or(
			goqu.I("a").Gt(10),
			goqu.And(
				goqu.I("b").Lt(10),
				goqu.I("c").IsNull(),
			),
		),
	).ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" WHERE (("a" > 10) AND ("b" < 10) AND ("c" IS NULL) AND ("d" IN ('a', 'b', 'c')))
	// SELECT * FROM "test" WHERE (("a" > 10) OR ("b" < 10) OR ("c" IS NULL) OR ("d" IN ('a', 'b', 'c')))
	// SELECT * FROM "test" WHERE ((("a" > 10) AND ("b" < 10)) OR (("c" IS NULL) AND ("d" IN ('a', 'b', 'c'))))
	// SELECT * FROM "test" WHERE (("a" > 10) AND ("b" < 10) AND ("c" IS NULL) AND ("d" IN ('a', 'b', 'c')))
	// SELECT * FROM "test" WHERE (("a" > 10) OR (("b" < 10) AND ("c" IS NULL)))
}

func ExampleDataset_Where_prepared() {
	db := goqu.New("default", driver)

	//By default everything is anded together
	sql, args, _ := db.From("test").Prepared(true).Where(goqu.Ex{
		"a": goqu.Op{"gt": 10},
		"b": goqu.Op{"lt": 10},
		"c": nil,
		"d": []string{"a", "b", "c"},
	}).ToSql()
	fmt.Println(sql, args)

	//You can use ExOr to get ORed expressions together
	sql, args, _ = db.From("test").Prepared(true).Where(goqu.ExOr{
		"a": goqu.Op{"gt": 10},
		"b": goqu.Op{"lt": 10},
		"c": nil,
		"d": []string{"a", "b", "c"},
	}).ToSql()
	fmt.Println(sql, args)

	//You can use Or with Ex to Or multiple Ex maps together
	sql, args, _ = db.From("test").Prepared(true).Where(
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
	).ToSql()
	fmt.Println(sql, args)

	//By default everything is anded together
	sql, args, _ = db.From("test").Prepared(true).Where(
		goqu.I("a").Gt(10),
		goqu.I("b").Lt(10),
		goqu.I("c").IsNull(),
		goqu.I("d").In("a", "b", "c"),
	).ToSql()
	fmt.Println(sql, args)

	//You can use a combination of Ors and Ands
	sql, args, _ = db.From("test").Prepared(true).Where(
		goqu.Or(
			goqu.I("a").Gt(10),
			goqu.And(
				goqu.I("b").Lt(10),
				goqu.I("c").IsNull(),
			),
		),
	).ToSql()
	fmt.Println(sql, args)
	// Output:
	// SELECT * FROM "test" WHERE (("a" > ?) AND ("b" < ?) AND ("c" IS NULL) AND ("d" IN (?, ?, ?))) [10 10 a b c]
	// SELECT * FROM "test" WHERE (("a" > ?) OR ("b" < ?) OR ("c" IS NULL) OR ("d" IN (?, ?, ?))) [10 10 a b c]
	// SELECT * FROM "test" WHERE ((("a" > ?) AND ("b" < ?)) OR (("c" IS NULL) AND ("d" IN (?, ?, ?)))) [10 10 a b c]
	// SELECT * FROM "test" WHERE (("a" > ?) AND ("b" < ?) AND ("c" IS NULL) AND ("d" IN (?, ?, ?))) [10 10 a b c]
	// SELECT * FROM "test" WHERE (("a" > ?) OR (("b" < ?) AND ("c" IS NULL))) [10 10]
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
	sql, _, _ := ds.ClearWhere().ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test"
}

func ExampleDataset_Join() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("test").Join(goqu.I("test2"), goqu.On(goqu.Ex{"test.fkey": goqu.I("test2.Id")})).ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").Join(goqu.I("test2"), goqu.Using("common_column")).ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").Join(db.From("test2").Where(goqu.I("amount").Gt(0)), goqu.On(goqu.I("test.fkey").Eq(goqu.I("test2.Id")))).ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").Join(db.From("test2").Where(goqu.I("amount").Gt(0)).As("t"), goqu.On(goqu.I("test.fkey").Eq(goqu.I("t.Id")))).ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" INNER JOIN "test2" ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" INNER JOIN "test2" USING ("common_column")
	// SELECT * FROM "test" INNER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" INNER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t" ON ("test"."fkey" = "t"."Id")

}

func ExampleDataset_InnerJoin() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("test").InnerJoin(goqu.I("test2"), goqu.On(goqu.Ex{"test.fkey": goqu.I("test2.Id")})).ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").InnerJoin(goqu.I("test2"), goqu.Using("common_column")).ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").InnerJoin(db.From("test2").Where(goqu.I("amount").Gt(0)), goqu.On(goqu.I("test.fkey").Eq(goqu.I("test2.Id")))).ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").InnerJoin(db.From("test2").Where(goqu.I("amount").Gt(0)).As("t"), goqu.On(goqu.I("test.fkey").Eq(goqu.I("t.Id")))).ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" INNER JOIN "test2" ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" INNER JOIN "test2" USING ("common_column")
	// SELECT * FROM "test" INNER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" INNER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t" ON ("test"."fkey" = "t"."Id")
}

func ExampleDataset_FullOuterJoin() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("test").FullOuterJoin(goqu.I("test2"), goqu.On(goqu.Ex{"test.fkey": goqu.I("test2.Id")})).ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").FullOuterJoin(goqu.I("test2"), goqu.Using("common_column")).ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").FullOuterJoin(db.From("test2").Where(goqu.I("amount").Gt(0)), goqu.On(goqu.I("test.fkey").Eq(goqu.I("test2.Id")))).ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").FullOuterJoin(db.From("test2").Where(goqu.I("amount").Gt(0)).As("t"), goqu.On(goqu.I("test.fkey").Eq(goqu.I("t.Id")))).ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" FULL OUTER JOIN "test2" ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" FULL OUTER JOIN "test2" USING ("common_column")
	// SELECT * FROM "test" FULL OUTER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" FULL OUTER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t" ON ("test"."fkey" = "t"."Id")
}

func ExampleDataset_RightOuterJoin() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("test").RightOuterJoin(goqu.I("test2"), goqu.On(goqu.Ex{"test.fkey": goqu.I("test2.Id")})).ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").RightOuterJoin(goqu.I("test2"), goqu.Using("common_column")).ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").RightOuterJoin(
		db.From("test2").Where(goqu.I("amount").Gt(0)),
		goqu.On(goqu.I("test.fkey").Eq(goqu.I("test2.Id"))),
	).ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").RightOuterJoin(
		db.From("test2").Where(goqu.I("amount").Gt(0)).As("t"),
		goqu.On(goqu.I("test.fkey").Eq(goqu.I("t.Id"))),
	).ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" RIGHT OUTER JOIN "test2" ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" RIGHT OUTER JOIN "test2" USING ("common_column")
	// SELECT * FROM "test" RIGHT OUTER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" RIGHT OUTER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t" ON ("test"."fkey" = "t"."Id")
}

func ExampleDataset_LeftOuterJoin() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("test").LeftOuterJoin(goqu.I("test2"), goqu.On(goqu.Ex{"test.fkey": goqu.I("test2.Id")})).ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").LeftOuterJoin(goqu.I("test2"), goqu.Using("common_column")).ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").LeftOuterJoin(db.From("test2").Where(goqu.I("amount").Gt(0)), goqu.On(goqu.I("test.fkey").Eq(goqu.I("test2.Id")))).ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").LeftOuterJoin(db.From("test2").Where(goqu.I("amount").Gt(0)).As("t"), goqu.On(goqu.I("test.fkey").Eq(goqu.I("t.Id")))).ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" LEFT OUTER JOIN "test2" ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" LEFT OUTER JOIN "test2" USING ("common_column")
	// SELECT * FROM "test" LEFT OUTER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" LEFT OUTER JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t" ON ("test"."fkey" = "t"."Id")
}

func ExampleDataset_FullJoin() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("test").FullJoin(goqu.I("test2"), goqu.On(goqu.Ex{"test.fkey": goqu.I("test2.Id")})).ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").FullJoin(goqu.I("test2"), goqu.Using("common_column")).ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").FullJoin(db.From("test2").Where(goqu.I("amount").Gt(0)), goqu.On(goqu.I("test.fkey").Eq(goqu.I("test2.Id")))).ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").FullJoin(db.From("test2").Where(goqu.I("amount").Gt(0)).As("t"), goqu.On(goqu.I("test.fkey").Eq(goqu.I("t.Id")))).ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" FULL JOIN "test2" ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" FULL JOIN "test2" USING ("common_column")
	// SELECT * FROM "test" FULL JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" FULL JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t" ON ("test"."fkey" = "t"."Id")
}

func ExampleDataset_RightJoin() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("test").RightJoin(goqu.I("test2"), goqu.On(goqu.Ex{"test.fkey": goqu.I("test2.Id")})).ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").RightJoin(goqu.I("test2"), goqu.Using("common_column")).ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").RightJoin(db.From("test2").Where(goqu.I("amount").Gt(0)), goqu.On(goqu.I("test.fkey").Eq(goqu.I("test2.Id")))).ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").RightJoin(db.From("test2").Where(goqu.I("amount").Gt(0)).As("t"), goqu.On(goqu.I("test.fkey").Eq(goqu.I("t.Id")))).ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" RIGHT JOIN "test2" ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" RIGHT JOIN "test2" USING ("common_column")
	// SELECT * FROM "test" RIGHT JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" RIGHT JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t" ON ("test"."fkey" = "t"."Id")
}

func ExampleDataset_LeftJoin() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("test").LeftJoin(goqu.I("test2"), goqu.On(goqu.Ex{"test.fkey": goqu.I("test2.Id")})).ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").LeftJoin(goqu.I("test2"), goqu.Using("common_column")).ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").LeftJoin(db.From("test2").Where(goqu.I("amount").Gt(0)), goqu.On(goqu.I("test.fkey").Eq(goqu.I("test2.Id")))).ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").LeftJoin(db.From("test2").Where(goqu.I("amount").Gt(0)).As("t"), goqu.On(goqu.I("test.fkey").Eq(goqu.I("t.Id")))).ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" LEFT JOIN "test2" ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" LEFT JOIN "test2" USING ("common_column")
	// SELECT * FROM "test" LEFT JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) ON ("test"."fkey" = "test2"."Id")
	// SELECT * FROM "test" LEFT JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t" ON ("test"."fkey" = "t"."Id")
}

func ExampleDataset_NaturalJoin() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("test").NaturalJoin(goqu.I("test2")).ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").NaturalJoin(db.From("test2").Where(goqu.I("amount").Gt(0))).ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").NaturalJoin(db.From("test2").Where(goqu.I("amount").Gt(0)).As("t")).ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" NATURAL JOIN "test2"
	// SELECT * FROM "test" NATURAL JOIN (SELECT * FROM "test2" WHERE ("amount" > 0))
	// SELECT * FROM "test" NATURAL JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t"
}

func ExampleDataset_NaturalLeftJoin() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("test").NaturalLeftJoin(goqu.I("test2")).ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").NaturalLeftJoin(db.From("test2").Where(goqu.I("amount").Gt(0))).ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").NaturalLeftJoin(db.From("test2").Where(goqu.I("amount").Gt(0)).As("t")).ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" NATURAL LEFT JOIN "test2"
	// SELECT * FROM "test" NATURAL LEFT JOIN (SELECT * FROM "test2" WHERE ("amount" > 0))
	// SELECT * FROM "test" NATURAL LEFT JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t"
}

func ExampleDataset_NaturalRightJoin() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("test").NaturalRightJoin(goqu.I("test2")).ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").NaturalRightJoin(db.From("test2").Where(goqu.I("amount").Gt(0))).ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").NaturalRightJoin(db.From("test2").Where(goqu.I("amount").Gt(0)).As("t")).ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" NATURAL RIGHT JOIN "test2"
	// SELECT * FROM "test" NATURAL RIGHT JOIN (SELECT * FROM "test2" WHERE ("amount" > 0))
	// SELECT * FROM "test" NATURAL RIGHT JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t"
}

func ExampleDataset_NaturalFullJoin() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("test").NaturalFullJoin(goqu.I("test2")).ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").NaturalFullJoin(db.From("test2").Where(goqu.I("amount").Gt(0))).ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").NaturalFullJoin(db.From("test2").Where(goqu.I("amount").Gt(0)).As("t")).ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" NATURAL FULL JOIN "test2"
	// SELECT * FROM "test" NATURAL FULL JOIN (SELECT * FROM "test2" WHERE ("amount" > 0))
	// SELECT * FROM "test" NATURAL FULL JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t"
}

func ExampleDataset_CrossJoin() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("test").CrossJoin(goqu.I("test2")).ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").CrossJoin(db.From("test2").Where(goqu.I("amount").Gt(0))).ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").CrossJoin(db.From("test2").Where(goqu.I("amount").Gt(0)).As("t")).ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test" CROSS JOIN "test2"
	// SELECT * FROM "test" CROSS JOIN (SELECT * FROM "test2" WHERE ("amount" > 0))
	// SELECT * FROM "test" CROSS JOIN (SELECT * FROM "test2" WHERE ("amount" > 0)) AS "t"
}

func ExampleDataset_FromSelf() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("test").FromSelf().ToSql()
	fmt.Println(sql)
	sql, _, _ = db.From("test").As("my_test_table").FromSelf().ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM (SELECT * FROM "test") AS "t1"
	// SELECT * FROM (SELECT * FROM "test") AS "my_test_table"
}

func ExampleDataset_From() {
	db := goqu.New("default", driver)
	ds := db.From("test")
	sql, _, _ := ds.From("test2").ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test2"
}

func ExampleDataset_From_withDataset() {
	db := goqu.New("default", driver)
	ds := db.From("test")
	fromDs := ds.Where(goqu.I("age").Gt(10))
	sql, _, _ := ds.From(fromDs).ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM (SELECT * FROM "test" WHERE ("age" > 10)) AS "t1"
}

func ExampleDataset_From_withAliasedDataset() {
	db := goqu.New("default", driver)
	ds := db.From("test")
	fromDs := ds.Where(goqu.I("age").Gt(10))
	sql, _, _ := ds.From(fromDs.As("test2")).ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM (SELECT * FROM "test" WHERE ("age" > 10)) AS "test2"
}

func ExampleDataset_Select() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("test").Select("a", "b", "c").ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT "a", "b", "c" FROM "test"
}

func ExampleDataset_Select_withDataset() {
	db := goqu.New("default", driver)
	ds := db.From("test")
	fromDs := ds.Select("age").Where(goqu.I("age").Gt(10))
	sql, _, _ := ds.From().Select(fromDs).ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT (SELECT "age" FROM "test" WHERE ("age" > 10))
}

func ExampleDataset_Select_withAliasedDataset() {
	db := goqu.New("default", driver)
	ds := db.From("test")
	fromDs := ds.Select("age").Where(goqu.I("age").Gt(10))
	sql, _, _ := ds.From().Select(fromDs.As("ages")).ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT (SELECT "age" FROM "test" WHERE ("age" > 10)) AS "ages"
}

func ExampleDataset_Select_withLiteral() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("test").Select(goqu.L("a + b").As("sum")).ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT a + b AS "sum" FROM "test"
}

func ExampleDataset_Select_withSqlFunctionExpression() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("test").Select(
		goqu.COUNT("*").As("age_count"),
		goqu.MAX("age").As("max_age"),
		goqu.AVG("age").As("avg_age"),
	).ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT COUNT(*) AS "age_count", MAX("age") AS "max_age", AVG("age") AS "avg_age" FROM "test"
}

func ExampleDataset_Select_withStruct() {
	db := goqu.New("default", driver)
	ds := db.From("test")

	type myStruct struct {
		Name         string
		Address      string `db:"address"`
		EmailAddress string `db:"email_address"`
	}

	// Pass with pointer
	sql, _, _ := ds.Select(&myStruct{}).ToSql()
	fmt.Println(sql)

	// Pass instance of
	sql, _, _ = ds.Select(myStruct{}).ToSql()
	fmt.Println(sql)

	type myStruct2 struct {
		myStruct
		Zipcode string `db:"zipcode"`
	}

	// Pass pointer to struct with embedded struct
	sql, _, _ = ds.Select(&myStruct2{}).ToSql()
	fmt.Println(sql)

	// Pass instance of struct with embedded struct
	sql, _, _ = ds.Select(myStruct2{}).ToSql()
	fmt.Println(sql)

	var myStructs []myStruct

	// Pass slice of structs, will only select columns from underlying type
	sql, _, _ = ds.Select(myStructs).ToSql()
	fmt.Println(sql)

	// Output:
	// SELECT "address", "email_address", "name" FROM "test"
	// SELECT "address", "email_address", "name" FROM "test"
	// SELECT "address", "email_address", "name", "zipcode" FROM "test"
	// SELECT "address", "email_address", "name", "zipcode" FROM "test"
	// SELECT "address", "email_address", "name" FROM "test"
}

func ExampleDataset_SelectDistinct() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("test").SelectDistinct("a", "b").ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT DISTINCT "a", "b" FROM "test"
}

func ExampleDataset_SelectAppend() {
	db := goqu.New("default", driver)
	ds := db.From("test").Select("a", "b")
	sql, _, _ := ds.SelectAppend("c").ToSql()
	fmt.Println(sql)
	ds = db.From("test").SelectDistinct("a", "b")
	sql, _, _ = ds.SelectAppend("c").ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT "a", "b", "c" FROM "test"
	// SELECT DISTINCT "a", "b", "c" FROM "test"
}

func ExampleDataset_ClearSelect() {
	db := goqu.New("default", driver)
	ds := db.From("test").Select("a", "b")
	sql, _, _ := ds.ClearSelect().ToSql()
	fmt.Println(sql)
	ds = db.From("test").SelectDistinct("a", "b")
	sql, _, _ = ds.ClearSelect().ToSql()
	fmt.Println(sql)
	// Output:
	// SELECT * FROM "test"
	// SELECT * FROM "test"
}

func ExampleDataset_ToSql() {
	db := goqu.New("default", driver)
	sql, args, _ := db.From("items").Where(goqu.Ex{"a": 1}).ToSql()
	fmt.Println(sql, args)
	// Output:
	// SELECT * FROM "items" WHERE ("a" = 1) []
}

func ExampleDataset_ToSql_prepared() {
	db := goqu.New("default", driver)
	sql, args, _ := db.From("items").Where(goqu.Ex{"a": 1}).Prepared(true).ToSql()
	fmt.Println(sql, args)
	// Output:
	// SELECT * FROM "items" WHERE ("a" = ?) [1]
}

func ExampleDataset_ToUpdateSql() {
	db := goqu.New("default", driver)
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, args, _ := db.From("items").ToUpdateSql(
		item{Name: "Test", Address: "111 Test Addr"},
	)
	fmt.Println(sql, args)

	sql, args, _ = db.From("items").ToUpdateSql(
		goqu.Record{"name": "Test", "address": "111 Test Addr"},
	)
	fmt.Println(sql, args)

	sql, args, _ = db.From("items").ToUpdateSql(
		map[string]interface{}{"name": "Test", "address": "111 Test Addr"},
	)
	fmt.Println(sql, args)

	// Output:
	// UPDATE "items" SET "address"='111 Test Addr',"name"='Test' []
	// UPDATE "items" SET "address"='111 Test Addr',"name"='Test' []
	// UPDATE "items" SET "address"='111 Test Addr',"name"='Test' []
}

func ExampleDataset_ToUpdateSql_prepared() {
	db := goqu.New("default", driver)
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	sql, args, _ := db.From("items").Prepared(true).ToUpdateSql(
		item{Name: "Test", Address: "111 Test Addr"},
	)
	fmt.Println(sql, args)

	sql, args, _ = db.From("items").Prepared(true).ToUpdateSql(
		goqu.Record{"name": "Test", "address": "111 Test Addr"},
	)
	fmt.Println(sql, args)

	sql, args, _ = db.From("items").Prepared(true).ToUpdateSql(
		map[string]interface{}{"name": "Test", "address": "111 Test Addr"},
	)
	fmt.Println(sql, args)
	// Output:
	// UPDATE "items" SET "address"=?,"name"=? [111 Test Addr Test]
	// UPDATE "items" SET "address"=?,"name"=? [111 Test Addr Test]
	// UPDATE "items" SET "address"=?,"name"=? [111 Test Addr Test]
}

func ExampleDataset_ToInsertSql() {
	db := goqu.New("default", driver)
	type item struct {
		Id      uint32 `db:"id" goqu:"skipinsert"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, args, _ := db.From("items").ToInsertSql(
		item{Name: "Test1", Address: "111 Test Addr"},
		item{Name: "Test2", Address: "112 Test Addr"},
	)
	fmt.Println(sql, args)

	sql, args, _ = db.From("items").ToInsertSql(
		goqu.Record{"name": "Test1", "address": "111 Test Addr"},
		goqu.Record{"name": "Test2", "address": "112 Test Addr"},
	)
	fmt.Println(sql, args)

	sql, args, _ = db.From("items").ToInsertSql(
		[]item{
			{Name: "Test1", Address: "111 Test Addr"},
			{Name: "Test2", Address: "112 Test Addr"},
		})
	fmt.Println(sql, args)

	sql, args, _ = db.From("items").ToInsertSql(
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

func ExampleDataset_ToInsertSql_prepared() {
	db := goqu.New("default", driver)
	type item struct {
		Id      uint32 `db:"id" goqu:"skipinsert"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	sql, args, _ := db.From("items").Prepared(true).ToInsertSql(
		item{Name: "Test1", Address: "111 Test Addr"},
		item{Name: "Test2", Address: "112 Test Addr"},
	)
	fmt.Println(sql, args)

	sql, args, _ = db.From("items").Prepared(true).ToInsertSql(
		goqu.Record{"name": "Test1", "address": "111 Test Addr"},
		goqu.Record{"name": "Test2", "address": "112 Test Addr"},
	)
	fmt.Println(sql, args)

	sql, args, _ = db.From("items").Prepared(true).ToInsertSql(
		[]item{
			{Name: "Test1", Address: "111 Test Addr"},
			{Name: "Test2", Address: "112 Test Addr"},
		})
	fmt.Println(sql, args)

	sql, args, _ = db.From("items").Prepared(true).ToInsertSql(
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

func ExampleDataset_ToInsertIgnore() {
	db := goqu.New("mysql", driver)
	type item struct {
		Id      uint32 `db:"id" goqu:"skipinsert"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, args, _ := db.From("items").ToInsertIgnoreSql(
		item{Name: "Test1", Address: "111 Test Addr"},
		item{Name: "Test2", Address: "112 Test Addr"},
	)
	fmt.Println(sql, args)

	sql, args, _ = db.From("items").ToInsertIgnoreSql(
		goqu.Record{"name": "Test1", "address": "111 Test Addr"},
		goqu.Record{"name": "Test2", "address": "112 Test Addr"},
	)
	fmt.Println(sql, args)

	sql, args, _ = db.From("items").ToInsertIgnoreSql(
		[]item{
			{Name: "Test1", Address: "111 Test Addr"},
			{Name: "Test2", Address: "112 Test Addr"},
		})
	fmt.Println(sql, args)

	sql, args, _ = db.From("items").ToInsertIgnoreSql(
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

func ExampleDataset_ToInsertConflictSql() {
	db := goqu.New("mysql", driver)
	type item struct {
		Id      uint32 `db:"id" goqu:"skipinsert"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, args, _ := db.From("items").ToInsertConflictSql(
		goqu.DoNothing(),
		item{Name: "Test1", Address: "111 Test Addr"},
		item{Name: "Test2", Address: "112 Test Addr"},
	)
	fmt.Println(sql, args)

	sql, args, _ = db.From("items").ToInsertConflictSql(
		goqu.DoUpdate("key", goqu.Record{"updated": goqu.L("NOW()")}),
		goqu.Record{"name": "Test1", "address": "111 Test Addr"},
		goqu.Record{"name": "Test2", "address": "112 Test Addr"},
	)
	fmt.Println(sql, args)

	sql, args, _ = db.From("items").ToInsertConflictSql(
		goqu.DoUpdate("key", goqu.Record{"updated": goqu.L("NOW()")}).Where(goqu.I("allow_update").IsTrue()),
		[]item{
			{Name: "Test1", Address: "111 Test Addr"},
			{Name: "Test2", Address: "112 Test Addr"},
		})
	fmt.Println(sql, args)
	// Output:
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') ON CONFLICT DO NOTHING []
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') ON CONFLICT (key) DO UPDATE SET "updated"=NOW() []
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') ON CONFLICT (key) DO UPDATE SET "updated"=NOW() WHERE ("allow_update" IS TRUE) []
}

func ExampleDataset_ToDeleteSql() {
	db := goqu.New("default", driver)
	sql, args, _ := db.From("items").ToDeleteSql()
	fmt.Println(sql, args)

	sql, args, _ = db.From("items").
		Where(goqu.Ex{"id": goqu.Op{"gt": 10}}).
		ToDeleteSql()
	fmt.Println(sql, args)

	// Output:
	// DELETE FROM "items" []
	// DELETE FROM "items" WHERE ("id" > 10) []
}

func ExampleDataset_ToDeleteSql_prepared() {
	db := goqu.New("default", driver)
	sql, args, _ := db.From("items").Prepared(true).ToDeleteSql()
	fmt.Println(sql, args)

	sql, args, _ = db.From("items").
		Prepared(true).
		Where(goqu.Ex{"id": goqu.Op{"gt": 10}}).
		ToDeleteSql()
	fmt.Println(sql, args)

	// Output:
	// DELETE FROM "items" []
	// DELETE FROM "items" WHERE ("id" > ?) [10]
}

func ExampleDataset_ToTruncateSql() {
	db := goqu.New("default", driver)
	sql, args, _ := db.From("items").ToTruncateSql()
	fmt.Println(sql, args)
	// Output:
	// TRUNCATE "items" []
}

func ExampleDataset_ToTruncateWithOptsSql() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("items").
		ToTruncateWithOptsSql(goqu.TruncateOptions{})
	fmt.Println(sql)
	sql, _, _ = db.From("items").
		ToTruncateWithOptsSql(goqu.TruncateOptions{Cascade: true})
	fmt.Println(sql)
	sql, _, _ = db.From("items").
		ToTruncateWithOptsSql(goqu.TruncateOptions{Restrict: true})
	fmt.Println(sql)
	sql, _, _ = db.From("items").
		ToTruncateWithOptsSql(goqu.TruncateOptions{Identity: "RESTART"})
	fmt.Println(sql)
	sql, _, _ = db.From("items").
		ToTruncateWithOptsSql(goqu.TruncateOptions{Identity: "RESTART", Cascade: true})
	fmt.Println(sql)
	sql, _, _ = db.From("items").
		ToTruncateWithOptsSql(goqu.TruncateOptions{Identity: "RESTART", Restrict: true})
	fmt.Println(sql)
	sql, _, _ = db.From("items").
		ToTruncateWithOptsSql(goqu.TruncateOptions{Identity: "CONTINUE"})
	fmt.Println(sql)
	sql, _, _ = db.From("items").
		ToTruncateWithOptsSql(goqu.TruncateOptions{Identity: "CONTINUE", Cascade: true})
	fmt.Println(sql)
	sql, _, _ = db.From("items").
		ToTruncateWithOptsSql(goqu.TruncateOptions{Identity: "CONTINUE", Restrict: true})
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

func ExampleEx() {
	db := goqu.New("default", driver)
	sql, args, _ := db.From("items").Where(goqu.Ex{
		"col1": "a",
		"col2": 1,
		"col3": true,
		"col4": false,
		"col5": nil,
		"col6": []string{"a", "b", "c"},
	}).ToSql()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "items" WHERE (("col1" = 'a') AND ("col2" = 1) AND ("col3" IS TRUE) AND ("col4" IS FALSE) AND ("col5" IS NULL) AND ("col6" IN ('a', 'b', 'c'))) []

}

func ExampleEx_prepared() {
	db := goqu.New("default", driver)
	sql, args, _ := db.From("items").Prepared(true).Where(goqu.Ex{
		"col1": "a",
		"col2": 1,
		"col3": true,
		"col4": false,
		"col5": []string{"a", "b", "c"},
	}).ToSql()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "items" WHERE (("col1" = ?) AND ("col2" = ?) AND ("col3" IS TRUE) AND ("col4" IS FALSE) AND ("col5" IN (?, ?, ?))) [a 1 a b c]

}

func ExampleEx_withOp() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("items").Where(goqu.Ex{
		"col1": goqu.Op{"neq": "a"},
		"col3": goqu.Op{"isNot": true},
		"col6": goqu.Op{"notIn": []string{"a", "b", "c"}},
	}).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("items").Where(goqu.Ex{
		"col1": goqu.Op{"gt": 1},
		"col2": goqu.Op{"gte": 1},
		"col3": goqu.Op{"lt": 1},
		"col4": goqu.Op{"lte": 1},
	}).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("items").Where(goqu.Ex{
		"col1": goqu.Op{"like": "a%"},
		"col2": goqu.Op{"notLike": "a%"},
		"col3": goqu.Op{"iLike": "a%"},
		"col4": goqu.Op{"notILike": "a%"},
	}).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("items").Where(goqu.Ex{
		"col1": goqu.Op{"like": regexp.MustCompile("^(a|b)")},
		"col2": goqu.Op{"notLike": regexp.MustCompile("^(a|b)")},
		"col3": goqu.Op{"iLike": regexp.MustCompile("^(a|b)")},
		"col4": goqu.Op{"notILike": regexp.MustCompile("^(a|b)")},
	}).ToSql()
	fmt.Println(sql)

	// Output:
	// SELECT * FROM "items" WHERE (("col1" != 'a') AND ("col3" IS NOT TRUE) AND ("col6" NOT IN ('a', 'b', 'c')))
	// SELECT * FROM "items" WHERE (("col1" > 1) AND ("col2" >= 1) AND ("col3" < 1) AND ("col4" <= 1))
	// SELECT * FROM "items" WHERE (("col1" LIKE 'a%') AND ("col2" NOT LIKE 'a%') AND ("col3" ILIKE 'a%') AND ("col4" NOT ILIKE 'a%'))
	// SELECT * FROM "items" WHERE (("col1" ~ '^(a|b)') AND ("col2" !~ '^(a|b)') AND ("col3" ~* '^(a|b)') AND ("col4" !~* '^(a|b)'))

}

func ExampleEx_withOpPrepared() {
	db := goqu.New("default", driver)
	sql, args, _ := db.From("items").Prepared(true).Where(goqu.Ex{
		"col1": goqu.Op{"neq": "a"},
		"col3": goqu.Op{"isNot": true},
		"col6": goqu.Op{"notIn": []string{"a", "b", "c"}},
	}).ToSql()
	fmt.Println(sql, args)

	sql, args, _ = db.From("items").Prepared(true).Where(goqu.Ex{
		"col1": goqu.Op{"gt": 1},
		"col2": goqu.Op{"gte": 1},
		"col3": goqu.Op{"lt": 1},
		"col4": goqu.Op{"lte": 1},
	}).ToSql()
	fmt.Println(sql, args)

	sql, args, _ = db.From("items").Prepared(true).Where(goqu.Ex{
		"col1": goqu.Op{"like": "a%"},
		"col2": goqu.Op{"notLike": "a%"},
		"col3": goqu.Op{"iLike": "a%"},
		"col4": goqu.Op{"notILike": "a%"},
	}).ToSql()
	fmt.Println(sql, args)

	sql, args, _ = db.From("items").Prepared(true).Where(goqu.Ex{
		"col1": goqu.Op{"like": regexp.MustCompile("^(a|b)")},
		"col2": goqu.Op{"notLike": regexp.MustCompile("^(a|b)")},
		"col3": goqu.Op{"iLike": regexp.MustCompile("^(a|b)")},
		"col4": goqu.Op{"notILike": regexp.MustCompile("^(a|b)")},
	}).ToSql()
	fmt.Println(sql, args)

	sql, args, _ = db.From("items").Prepared(true).Where(goqu.Ex{
		"col1": goqu.Op{"between": goqu.RangeVal{Start: 1, End: 10}},
		"col2": goqu.Op{"notbetween": goqu.RangeVal{Start: 1, End: 10}},
	}).ToSql()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "items" WHERE (("col1" != ?) AND ("col3" IS NOT TRUE) AND ("col6" NOT IN (?, ?, ?))) [a a b c]
	// SELECT * FROM "items" WHERE (("col1" > ?) AND ("col2" >= ?) AND ("col3" < ?) AND ("col4" <= ?)) [1 1 1 1]
	// SELECT * FROM "items" WHERE (("col1" LIKE ?) AND ("col2" NOT LIKE ?) AND ("col3" ILIKE ?) AND ("col4" NOT ILIKE ?)) [a% a% a% a%]
	// SELECT * FROM "items" WHERE (("col1" ~ ?) AND ("col2" !~ ?) AND ("col3" ~* ?) AND ("col4" !~* ?)) [^(a|b) ^(a|b) ^(a|b) ^(a|b)]
	// SELECT * FROM "items" WHERE (("col1" BETWEEN ? AND ?) AND ("col2" NOT BETWEEN ? AND ?)) [1 10 1 10]
}

func ExampleOp() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("items").Where(goqu.Ex{
		"col1": goqu.Op{"neq": "a"},
		"col3": goqu.Op{"isNot": true},
		"col6": goqu.Op{"notIn": []string{"a", "b", "c"}},
	}).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("items").Where(goqu.Ex{
		"col1": goqu.Op{"gt": 1},
		"col2": goqu.Op{"gte": 1},
		"col3": goqu.Op{"lt": 1},
		"col4": goqu.Op{"lte": 1},
	}).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("items").Where(goqu.Ex{
		"col1": goqu.Op{"like": "a%"},
		"col2": goqu.Op{"notLike": "a%"},
		"col3": goqu.Op{"iLike": "a%"},
		"col4": goqu.Op{"notILike": "a%"},
	}).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("items").Where(goqu.Ex{
		"col1": goqu.Op{"like": regexp.MustCompile("^(a|b)")},
		"col2": goqu.Op{"notLike": regexp.MustCompile("^(a|b)")},
		"col3": goqu.Op{"iLike": regexp.MustCompile("^(a|b)")},
		"col4": goqu.Op{"notILike": regexp.MustCompile("^(a|b)")},
	}).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("items").Where(goqu.Ex{
		"col1": goqu.Op{"between": goqu.RangeVal{Start: 1, End: 10}},
		"col2": goqu.Op{"notbetween": goqu.RangeVal{Start: 1, End: 10}},
	}).ToSql()
	fmt.Println(sql)

	// Output:
	// SELECT * FROM "items" WHERE (("col1" != 'a') AND ("col3" IS NOT TRUE) AND ("col6" NOT IN ('a', 'b', 'c')))
	// SELECT * FROM "items" WHERE (("col1" > 1) AND ("col2" >= 1) AND ("col3" < 1) AND ("col4" <= 1))
	// SELECT * FROM "items" WHERE (("col1" LIKE 'a%') AND ("col2" NOT LIKE 'a%') AND ("col3" ILIKE 'a%') AND ("col4" NOT ILIKE 'a%'))
	// SELECT * FROM "items" WHERE (("col1" ~ '^(a|b)') AND ("col2" !~ '^(a|b)') AND ("col3" ~* '^(a|b)') AND ("col4" !~* '^(a|b)'))
	// SELECT * FROM "items" WHERE (("col1" BETWEEN 1 AND 10) AND ("col2" NOT BETWEEN 1 AND 10))
}

func ExampleOp_withMultipleKeys() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("items").Where(goqu.Ex{
		"col1": goqu.Op{"is": nil, "eq": 10},
	}).ToSql()
	fmt.Println(sql)

	// Output:
	//SELECT * FROM "items" WHERE (("col1" = 10) OR ("col1" IS NULL))
}

func ExampleExOr() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("items").Where(goqu.ExOr{
		"col1": "a",
		"col2": 1,
		"col3": true,
		"col4": false,
		"col5": nil,
		"col6": []string{"a", "b", "c"},
	}).ToSql()
	fmt.Println(sql)

	// Output:
	// SELECT * FROM "items" WHERE (("col1" = 'a') OR ("col2" = 1) OR ("col3" IS TRUE) OR ("col4" IS FALSE) OR ("col5" IS NULL) OR ("col6" IN ('a', 'b', 'c')))

}

func ExampleExOr_withOp() {
	db := goqu.New("default", driver)
	sql, _, _ := db.From("items").Where(goqu.ExOr{
		"col1": goqu.Op{"neq": "a"},
		"col3": goqu.Op{"isNot": true},
		"col6": goqu.Op{"notIn": []string{"a", "b", "c"}},
	}).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("items").Where(goqu.ExOr{
		"col1": goqu.Op{"gt": 1},
		"col2": goqu.Op{"gte": 1},
		"col3": goqu.Op{"lt": 1},
		"col4": goqu.Op{"lte": 1},
	}).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("items").Where(goqu.ExOr{
		"col1": goqu.Op{"like": "a%"},
		"col2": goqu.Op{"notLike": "a%"},
		"col3": goqu.Op{"iLike": "a%"},
		"col4": goqu.Op{"notILike": "a%"},
	}).ToSql()
	fmt.Println(sql)

	sql, _, _ = db.From("items").Where(goqu.ExOr{
		"col1": goqu.Op{"like": regexp.MustCompile("^(a|b)")},
		"col2": goqu.Op{"notLike": regexp.MustCompile("^(a|b)")},
		"col3": goqu.Op{"iLike": regexp.MustCompile("^(a|b)")},
		"col4": goqu.Op{"notILike": regexp.MustCompile("^(a|b)")},
	}).ToSql()
	fmt.Println(sql)

	// Output:
	// SELECT * FROM "items" WHERE (("col1" != 'a') OR ("col3" IS NOT TRUE) OR ("col6" NOT IN ('a', 'b', 'c')))
	// SELECT * FROM "items" WHERE (("col1" > 1) OR ("col2" >= 1) OR ("col3" < 1) OR ("col4" <= 1))
	// SELECT * FROM "items" WHERE (("col1" LIKE 'a%') OR ("col2" NOT LIKE 'a%') OR ("col3" ILIKE 'a%') OR ("col4" NOT ILIKE 'a%'))
	// SELECT * FROM "items" WHERE (("col1" ~ '^(a|b)') OR ("col2" !~ '^(a|b)') OR ("col3" ~* '^(a|b)') OR ("col4" !~* '^(a|b)'))

}

func ExampleDataset_Prepared() {

	db := goqu.New("default", driver)
	sql, args, _ := db.From("items").Prepared(true).Where(goqu.Ex{
		"col1": "a",
		"col2": 1,
		"col3": true,
		"col4": false,
		"col5": []string{"a", "b", "c"},
	}).ToSql()
	fmt.Println(sql, args)

	sql, args, _ = db.From("items").Prepared(true).ToInsertSql(
		goqu.Record{"name": "Test1", "address": "111 Test Addr"},
		goqu.Record{"name": "Test2", "address": "112 Test Addr"},
	)
	fmt.Println(sql, args)

	sql, args, _ = db.From("items").Prepared(true).ToUpdateSql(
		goqu.Record{"name": "Test", "address": "111 Test Addr"},
	)
	fmt.Println(sql, args)

	sql, args, _ = db.From("items").
		Prepared(true).
		Where(goqu.Ex{"id": goqu.Op{"gt": 10}}).
		ToDeleteSql()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "items" WHERE (("col1" = ?) AND ("col2" = ?) AND ("col3" IS TRUE) AND ("col4" IS FALSE) AND ("col5" IN (?, ?, ?))) [a 1 a b c]
	// INSERT INTO "items" ("address", "name") VALUES (?, ?), (?, ?) [111 Test Addr Test1 112 Test Addr Test2]
	// UPDATE "items" SET "address"=?,"name"=? [111 Test Addr Test]
	// DELETE FROM "items" WHERE ("id" > ?) [10]

}

func ExampleSetColumnRenameFunction() {
	mDb, mock, _ := sqlmock.New()

	mock.ExpectQuery(`SELECT "ADDRESS", "NAME" FROM "items" LIMIT 1`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"ADDRESS", "NAME"}).FromCSVString("111 Test Addr,Test1"))

	db := goqu.New("db-mock", mDb)

	goqu.SetColumnRenameFunction(strings.ToUpper)

	anonStruct := struct {
		Address string
		Name    string
	}{}
	found, _ := db.From("items").ScanStruct(&anonStruct)
	fmt.Println(found)
	fmt.Println(anonStruct.Address)
	fmt.Println(anonStruct.Name)

	// Output:
	// true
	// 111 Test Addr
	// Test1
}
