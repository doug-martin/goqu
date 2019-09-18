// nolint:lll
package goqu_test

import (
	"fmt"
	"regexp"

	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exp"
)

func ExampleAVG() {
	ds := goqu.From("test").Select(goqu.AVG("col"))
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT AVG("col") FROM "test" []
	// SELECT AVG("col") FROM "test" []
}

func ExampleAVG_as() {
	sql, _, _ := goqu.From("test").Select(goqu.AVG("a").As("a")).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT AVG("a") AS "a" FROM "test"
}

func ExampleAVG_havingClause() {
	ds := goqu.
		From("test").
		Select(goqu.AVG("a").As("avg")).
		GroupBy("a").
		Having(goqu.AVG("a").Gt(10))

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT AVG("a") AS "avg" FROM "test" GROUP BY "a" HAVING (AVG("a") > 10) []
	// SELECT AVG("a") AS "avg" FROM "test" GROUP BY "a" HAVING (AVG("a") > ?) [10]
}

func ExampleAnd() {
	ds := goqu.From("test").Where(
		goqu.And(
			goqu.C("col").Gt(10),
			goqu.C("col").Lt(20),
		),
	)
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" WHERE (("col" > 10) AND ("col" < 20)) []
	// SELECT * FROM "test" WHERE (("col" > ?) AND ("col" < ?)) [10 20]
}

// You can use And with Or to create more complex queries
func ExampleAnd_withOr() {
	ds := goqu.From("test").Where(
		goqu.And(
			goqu.C("col1").IsTrue(),
			goqu.Or(
				goqu.C("col2").Gt(10),
				goqu.C("col2").Lt(20),
			),
		),
	)
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// by default expressions are anded together
	ds = goqu.From("test").Where(
		goqu.C("col1").IsTrue(),
		goqu.Or(
			goqu.C("col2").Gt(10),
			goqu.C("col2").Lt(20),
		),
	)
	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" WHERE (("col1" IS TRUE) AND (("col2" > 10) OR ("col2" < 20))) []
	// SELECT * FROM "test" WHERE (("col1" IS ?) AND (("col2" > ?) OR ("col2" < ?))) [true 10 20]
	// SELECT * FROM "test" WHERE (("col1" IS TRUE) AND (("col2" > 10) OR ("col2" < 20))) []
	// SELECT * FROM "test" WHERE (("col1" IS ?) AND (("col2" > ?) OR ("col2" < ?))) [true 10 20]
}

// You can use ExOr inside of And expression lists.
func ExampleAnd_withExOr() {
	// by default expressions are anded together
	ds := goqu.From("test").Where(
		goqu.C("col1").IsTrue(),
		goqu.ExOr{
			"col2": goqu.Op{"gt": 10},
			"col3": goqu.Op{"lt": 20},
		},
	)
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" WHERE (("col1" IS TRUE) AND (("col2" > 10) OR ("col3" < 20))) []
	// SELECT * FROM "test" WHERE (("col1" IS ?) AND (("col2" > ?) OR ("col3" < ?))) [true 10 20]
}

func ExampleC() {

	sql, args, _ := goqu.From("test").
		Select(goqu.C("*")).
		ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("test").
		Select(goqu.C("col1")).
		ToSQL()
	fmt.Println(sql, args)

	ds := goqu.From("test").Where(
		goqu.C("col1").Eq(10),
		goqu.C("col2").In([]int64{1, 2, 3, 4}),
		goqu.C("col3").Like(regexp.MustCompile("^(a|b)")),
		goqu.C("col4").IsNull(),
	)

	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" []
	// SELECT "col1" FROM "test" []
	// SELECT * FROM "test" WHERE (("col1" = 10) AND ("col2" IN (1, 2, 3, 4)) AND ("col3" ~ '^(a|b)') AND ("col4" IS NULL)) []
	// SELECT * FROM "test" WHERE (("col1" = ?) AND ("col2" IN (?, ?, ?, ?)) AND ("col3" ~ ?) AND ("col4" IS ?)) [10 1 2 3 4 ^(a|b) <nil>]
}

func ExampleC_as() {
	sql, _, _ := goqu.From("test").Select(goqu.C("a").As("as_a")).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").Select(goqu.C("a").As(goqu.C("as_a"))).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT "a" AS "as_a" FROM "test"
	// SELECT "a" AS "as_a" FROM "test"
}

func ExampleC_ordering() {
	sql, args, _ := goqu.From("test").Order(goqu.C("a").Asc()).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("test").Order(goqu.C("a").Asc().NullsFirst()).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("test").Order(goqu.C("a").Asc().NullsLast()).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("test").Order(goqu.C("a").Desc()).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("test").Order(goqu.C("a").Desc().NullsFirst()).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("test").Order(goqu.C("a").Desc().NullsLast()).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" ORDER BY "a" ASC []
	// SELECT * FROM "test" ORDER BY "a" ASC NULLS FIRST []
	// SELECT * FROM "test" ORDER BY "a" ASC NULLS LAST []
	// SELECT * FROM "test" ORDER BY "a" DESC []
	// SELECT * FROM "test" ORDER BY "a" DESC NULLS FIRST []
	// SELECT * FROM "test" ORDER BY "a" DESC NULLS LAST []
}

func ExampleC_cast() {
	sql, _, _ := goqu.From("test").
		Select(goqu.C("json1").Cast("TEXT").As("json_text")).
		ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").Where(
		goqu.C("json1").Cast("TEXT").Neq(
			goqu.C("json2").Cast("TEXT"),
		),
	).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT CAST("json1" AS TEXT) AS "json_text" FROM "test"
	// SELECT * FROM "test" WHERE (CAST("json1" AS TEXT) != CAST("json2" AS TEXT))
}

func ExampleC_comparisons() {
	// used from an identifier
	sql, _, _ := goqu.From("test").Where(goqu.C("a").Eq(10)).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").Where(goqu.C("a").Neq(10)).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").Where(goqu.C("a").Gt(10)).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").Where(goqu.C("a").Gte(10)).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").Where(goqu.C("a").Lt(10)).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").Where(goqu.C("a").Lte(10)).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT * FROM "test" WHERE ("a" = 10)
	// SELECT * FROM "test" WHERE ("a" != 10)
	// SELECT * FROM "test" WHERE ("a" > 10)
	// SELECT * FROM "test" WHERE ("a" >= 10)
	// SELECT * FROM "test" WHERE ("a" < 10)
	// SELECT * FROM "test" WHERE ("a" <= 10)
}

func ExampleC_inOperators() {
	// using identifiers
	sql, _, _ := goqu.From("test").Where(goqu.C("a").In("a", "b", "c")).ToSQL()
	fmt.Println(sql)
	// with a slice
	sql, _, _ = goqu.From("test").Where(goqu.C("a").In([]string{"a", "b", "c"})).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").Where(goqu.C("a").NotIn("a", "b", "c")).ToSQL()
	fmt.Println(sql)
	// with a slice
	sql, _, _ = goqu.From("test").Where(goqu.C("a").NotIn([]string{"a", "b", "c"})).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT * FROM "test" WHERE ("a" IN ('a', 'b', 'c'))
	// SELECT * FROM "test" WHERE ("a" IN ('a', 'b', 'c'))
	// SELECT * FROM "test" WHERE ("a" NOT IN ('a', 'b', 'c'))
	// SELECT * FROM "test" WHERE ("a" NOT IN ('a', 'b', 'c'))
}

func ExampleC_likeComparisons() {
	// using identifiers
	sql, _, _ := goqu.From("test").Where(goqu.C("a").Like("%a%")).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").Where(goqu.C("a").Like(regexp.MustCompile("(a|b)"))).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").Where(goqu.C("a").ILike("%a%")).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").Where(goqu.C("a").ILike(regexp.MustCompile("(a|b)"))).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").Where(goqu.C("a").NotLike("%a%")).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").Where(goqu.C("a").NotLike(regexp.MustCompile("(a|b)"))).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").Where(goqu.C("a").NotILike("%a%")).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").Where(goqu.C("a").NotILike(regexp.MustCompile("(a|b)"))).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT * FROM "test" WHERE ("a" LIKE '%a%')
	// SELECT * FROM "test" WHERE ("a" ~ '(a|b)')
	// SELECT * FROM "test" WHERE ("a" ILIKE '%a%')
	// SELECT * FROM "test" WHERE ("a" ~* '(a|b)')
	// SELECT * FROM "test" WHERE ("a" NOT LIKE '%a%')
	// SELECT * FROM "test" WHERE ("a" !~ '(a|b)')
	// SELECT * FROM "test" WHERE ("a" NOT ILIKE '%a%')
	// SELECT * FROM "test" WHERE ("a" !~* '(a|b)')
}

func ExampleC_isComparisons() {
	sql, args, _ := goqu.From("test").Where(goqu.C("a").Is(nil)).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("test").Where(goqu.C("a").Is(true)).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("test").Where(goqu.C("a").Is(false)).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("test").Where(goqu.C("a").IsNull()).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("test").Where(goqu.C("a").IsTrue()).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("test").Where(goqu.C("a").IsFalse()).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("test").Where(goqu.C("a").IsNot(nil)).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("test").Where(goqu.C("a").IsNot(true)).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("test").Where(goqu.C("a").IsNot(false)).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("test").Where(goqu.C("a").IsNotNull()).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("test").Where(goqu.C("a").IsNotTrue()).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("test").Where(goqu.C("a").IsNotFalse()).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" WHERE ("a" IS NULL) []
	// SELECT * FROM "test" WHERE ("a" IS TRUE) []
	// SELECT * FROM "test" WHERE ("a" IS FALSE) []
	// SELECT * FROM "test" WHERE ("a" IS NULL) []
	// SELECT * FROM "test" WHERE ("a" IS TRUE) []
	// SELECT * FROM "test" WHERE ("a" IS FALSE) []
	// SELECT * FROM "test" WHERE ("a" IS NOT NULL) []
	// SELECT * FROM "test" WHERE ("a" IS NOT TRUE) []
	// SELECT * FROM "test" WHERE ("a" IS NOT FALSE) []
	// SELECT * FROM "test" WHERE ("a" IS NOT NULL) []
	// SELECT * FROM "test" WHERE ("a" IS NOT TRUE) []
	// SELECT * FROM "test" WHERE ("a" IS NOT FALSE) []
}

func ExampleC_betweenComparisons() {
	ds := goqu.From("test").Where(
		goqu.C("a").Between(goqu.Range(1, 10)),
	)
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = goqu.From("test").Where(
		goqu.C("a").NotBetween(goqu.Range(1, 10)),
	)
	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" WHERE ("a" BETWEEN 1 AND 10) []
	// SELECT * FROM "test" WHERE ("a" BETWEEN ? AND ?) [1 10]
	// SELECT * FROM "test" WHERE ("a" NOT BETWEEN 1 AND 10) []
	// SELECT * FROM "test" WHERE ("a" NOT BETWEEN ? AND ?) [1 10]
}

func ExampleCOALESCE() {
	ds := goqu.From("test").Select(
		goqu.COALESCE(goqu.C("a"), "a"),
		goqu.COALESCE(goqu.C("a"), goqu.C("b"), nil),
	)
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT COALESCE("a", 'a'), COALESCE("a", "b", NULL) FROM "test" []
	// SELECT COALESCE("a", ?), COALESCE("a", "b", ?) FROM "test" [a <nil>]
}

func ExampleCOALESCE_as() {
	sql, _, _ := goqu.From("test").Select(goqu.COALESCE(goqu.C("a"), "a").As("a")).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT COALESCE("a", 'a') AS "a" FROM "test"
}

func ExampleCOUNT() {
	ds := goqu.From("test").Select(goqu.COUNT("*"))
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT COUNT(*) FROM "test" []
	// SELECT COUNT(*) FROM "test" []
}

func ExampleCOUNT_as() {
	sql, _, _ := goqu.From("test").Select(goqu.COUNT("*").As("count")).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT COUNT(*) AS "count" FROM "test"
}

func ExampleCOUNT_havingClause() {
	ds := goqu.
		From("test").
		Select(goqu.COUNT("a").As("COUNT")).
		GroupBy("a").
		Having(goqu.COUNT("a").Gt(10))

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT COUNT("a") AS "COUNT" FROM "test" GROUP BY "a" HAVING (COUNT("a") > 10) []
	// SELECT COUNT("a") AS "COUNT" FROM "test" GROUP BY "a" HAVING (COUNT("a") > ?) [10]
}

func ExampleCast() {
	sql, _, _ := goqu.From("test").
		Select(goqu.Cast(goqu.C("json1"), "TEXT").As("json_text")).
		ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").Where(
		goqu.Cast(goqu.C("json1"), "TEXT").Neq(
			goqu.Cast(goqu.C("json2"), "TEXT"),
		),
	).ToSQL()
	fmt.Println(sql)
	// Output:
	// SELECT CAST("json1" AS TEXT) AS "json_text" FROM "test"
	// SELECT * FROM "test" WHERE (CAST("json1" AS TEXT) != CAST("json2" AS TEXT))
}

func ExampleDISTINCT() {
	ds := goqu.From("test").Select(goqu.DISTINCT("col"))
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT DISTINCT("col") FROM "test" []
	// SELECT DISTINCT("col") FROM "test" []
}

func ExampleDISTINCT_as() {
	sql, _, _ := goqu.From("test").Select(goqu.DISTINCT("a").As("distinct_a")).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT DISTINCT("a") AS "distinct_a" FROM "test"
}

func ExampleDefault() {
	ds := goqu.Insert("items")

	sql, args, _ := ds.Rows(goqu.Record{
		"name":    goqu.Default(),
		"address": goqu.Default(),
	}).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).Rows(goqu.Record{
		"name":    goqu.Default(),
		"address": goqu.Default(),
	}).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// INSERT INTO "items" ("address", "name") VALUES (DEFAULT, DEFAULT) []
	// INSERT INTO "items" ("address", "name") VALUES (DEFAULT, DEFAULT) []

}

func ExampleDoNothing() {
	ds := goqu.Insert("items")

	sql, args, _ := ds.Rows(goqu.Record{
		"address": "111 Address",
		"name":    "bob",
	}).OnConflict(goqu.DoNothing()).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).Rows(goqu.Record{
		"address": "111 Address",
		"name":    "bob",
	}).OnConflict(goqu.DoNothing()).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// INSERT INTO "items" ("address", "name") VALUES ('111 Address', 'bob') ON CONFLICT DO NOTHING []
	// INSERT INTO "items" ("address", "name") VALUES (?, ?) ON CONFLICT DO NOTHING [111 Address bob]

}

func ExampleDoUpdate() {
	ds := goqu.Insert("items")

	sql, args, _ := ds.
		Rows(goqu.Record{"address": "111 Address"}).
		OnConflict(goqu.DoUpdate("address", goqu.C("address").Set(goqu.I("EXCLUDED.address")))).
		ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).
		Rows(goqu.Record{"address": "111 Address"}).
		OnConflict(goqu.DoUpdate("address", goqu.C("address").Set(goqu.I("EXCLUDED.address")))).
		ToSQL()
	fmt.Println(sql, args)

	// Output:
	// INSERT INTO "items" ("address") VALUES ('111 Address') ON CONFLICT (address) DO UPDATE SET "address"="EXCLUDED"."address" []
	// INSERT INTO "items" ("address") VALUES (?) ON CONFLICT (address) DO UPDATE SET "address"="EXCLUDED"."address" [111 Address]
}

func ExampleDoUpdate_where() {
	ds := goqu.Insert("items")

	sql, args, _ := ds.
		Rows(goqu.Record{"address": "111 Address"}).
		OnConflict(goqu.DoUpdate(
			"address",
			goqu.C("address").Set(goqu.I("EXCLUDED.address"))).Where(goqu.I("items.updated").IsNull()),
		).
		ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).
		Rows(goqu.Record{"address": "111 Address"}).
		OnConflict(goqu.DoUpdate(
			"address",
			goqu.C("address").Set(goqu.I("EXCLUDED.address"))).Where(goqu.I("items.updated").IsNull()),
		).
		ToSQL()
	fmt.Println(sql, args)

	// Output:
	// INSERT INTO "items" ("address") VALUES ('111 Address') ON CONFLICT (address) DO UPDATE SET "address"="EXCLUDED"."address" WHERE ("items"."updated" IS NULL) []
	// INSERT INTO "items" ("address") VALUES (?) ON CONFLICT (address) DO UPDATE SET "address"="EXCLUDED"."address" WHERE ("items"."updated" IS ?) [111 Address <nil>]
}

func ExampleFIRST() {
	ds := goqu.From("test").Select(goqu.FIRST("col"))
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT FIRST("col") FROM "test" []
	// SELECT FIRST("col") FROM "test" []
}

func ExampleFIRST_as() {
	sql, _, _ := goqu.From("test").Select(goqu.FIRST("a").As("a")).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT FIRST("a") AS "a" FROM "test"
}

// This example shows how to create custom SQL Functions
func ExampleFunc() {
	stragg := func(expression exp.Expression, delimiter string) exp.SQLFunctionExpression {
		return goqu.Func("str_agg", expression, goqu.L(delimiter))
	}
	sql, _, _ := goqu.From("test").Select(stragg(goqu.C("col"), "|")).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT str_agg("col", |) FROM "test"
}

func ExampleI() {
	ds := goqu.From("test").
		Select(
			goqu.I("my_schema.table.col1"),
			goqu.I("table.col2"),
			goqu.I("col3"),
		)

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = goqu.From("test").Select(goqu.I("test.*"))

	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT "my_schema"."table"."col1", "table"."col2", "col3" FROM "test" []
	// SELECT "my_schema"."table"."col1", "table"."col2", "col3" FROM "test" []
	// SELECT "test".* FROM "test" []
	// SELECT "test".* FROM "test" []
}

func ExampleL() {
	ds := goqu.From("test").Where(
		// literal with no args
		goqu.L(`"col"::TEXT = ""other_col"::text`),
		// literal with args they will be interpolated into the sql by default
		goqu.L("col IN (?, ?, ?)", "a", "b", "c"),
	)

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT * FROM "test" WHERE ("col"::TEXT = ""other_col"::text AND col IN ('a', 'b', 'c')) []
	// SELECT * FROM "test" WHERE ("col"::TEXT = ""other_col"::text AND col IN (?, ?, ?)) [a b c]
}

func ExampleL_withArgs() {
	ds := goqu.From("test").Where(
		goqu.L(
			"(? AND ?) OR ?",
			goqu.C("a").Eq(1),
			goqu.C("b").Eq("b"),
			goqu.C("c").In([]string{"a", "b", "c"}),
		),
	)

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT * FROM "test" WHERE (("a" = 1) AND ("b" = 'b')) OR ("c" IN ('a', 'b', 'c')) []
	// SELECT * FROM "test" WHERE (("a" = ?) AND ("b" = ?)) OR ("c" IN (?, ?, ?)) [1 b a b c]
}

func ExampleL_as() {
	sql, _, _ := goqu.From("test").Select(goqu.L("json_col->>'totalAmount'").As("total_amount")).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT json_col->>'totalAmount' AS "total_amount" FROM "test"
}

func ExampleL_comparisons() {
	// used from a literal expression
	sql, _, _ := goqu.From("test").Where(goqu.L("(a + b)").Eq(10)).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").Where(goqu.L("(a + b)").Neq(10)).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").Where(goqu.L("(a + b)").Gt(10)).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").Where(goqu.L("(a + b)").Gte(10)).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").Where(goqu.L("(a + b)").Lt(10)).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").Where(goqu.L("(a + b)").Lte(10)).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT * FROM "test" WHERE ((a + b) = 10)
	// SELECT * FROM "test" WHERE ((a + b) != 10)
	// SELECT * FROM "test" WHERE ((a + b) > 10)
	// SELECT * FROM "test" WHERE ((a + b) >= 10)
	// SELECT * FROM "test" WHERE ((a + b) < 10)
	// SELECT * FROM "test" WHERE ((a + b) <= 10)
}

func ExampleL_inOperators() {
	// using identifiers
	sql, _, _ := goqu.From("test").Where(goqu.L("json_col->>'val'").In("a", "b", "c")).ToSQL()
	fmt.Println(sql)
	// with a slice
	sql, _, _ = goqu.From("test").Where(goqu.L("json_col->>'val'").In([]string{"a", "b", "c"})).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").Where(goqu.L("json_col->>'val'").NotIn("a", "b", "c")).ToSQL()
	fmt.Println(sql)
	// with a slice
	sql, _, _ = goqu.From("test").Where(goqu.L("json_col->>'val'").NotIn([]string{"a", "b", "c"})).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT * FROM "test" WHERE (json_col->>'val' IN ('a', 'b', 'c'))
	// SELECT * FROM "test" WHERE (json_col->>'val' IN ('a', 'b', 'c'))
	// SELECT * FROM "test" WHERE (json_col->>'val' NOT IN ('a', 'b', 'c'))
	// SELECT * FROM "test" WHERE (json_col->>'val' NOT IN ('a', 'b', 'c'))
}

func ExampleL_likeComparisons() {
	// using identifiers
	sql, _, _ := goqu.From("test").Where(goqu.L("(a::text || 'bar')").Like("%a%")).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").Where(
		goqu.L("(a::text || 'bar')").Like(regexp.MustCompile("(a|b)")),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").Where(goqu.L("(a::text || 'bar')").ILike("%a%")).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").Where(
		goqu.L("(a::text || 'bar')").ILike(regexp.MustCompile("(a|b)")),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").Where(goqu.L("(a::text || 'bar')").NotLike("%a%")).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").Where(
		goqu.L("(a::text || 'bar')").NotLike(regexp.MustCompile("(a|b)")),
	).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").Where(goqu.L("(a::text || 'bar')").NotILike("%a%")).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("test").Where(
		goqu.L("(a::text || 'bar')").NotILike(regexp.MustCompile("(a|b)")),
	).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT * FROM "test" WHERE ((a::text || 'bar') LIKE '%a%')
	// SELECT * FROM "test" WHERE ((a::text || 'bar') ~ '(a|b)')
	// SELECT * FROM "test" WHERE ((a::text || 'bar') ILIKE '%a%')
	// SELECT * FROM "test" WHERE ((a::text || 'bar') ~* '(a|b)')
	// SELECT * FROM "test" WHERE ((a::text || 'bar') NOT LIKE '%a%')
	// SELECT * FROM "test" WHERE ((a::text || 'bar') !~ '(a|b)')
	// SELECT * FROM "test" WHERE ((a::text || 'bar') NOT ILIKE '%a%')
	// SELECT * FROM "test" WHERE ((a::text || 'bar') !~* '(a|b)')
}

func ExampleL_isComparisons() {
	sql, args, _ := goqu.From("test").Where(goqu.L("a").Is(nil)).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("test").Where(goqu.L("a").Is(true)).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("test").Where(goqu.L("a").Is(false)).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("test").Where(goqu.L("a").IsNull()).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("test").Where(goqu.L("a").IsTrue()).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("test").Where(goqu.L("a").IsFalse()).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("test").Where(goqu.L("a").IsNot(nil)).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("test").Where(goqu.L("a").IsNot(true)).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("test").Where(goqu.L("a").IsNot(false)).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("test").Where(goqu.L("a").IsNotNull()).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("test").Where(goqu.L("a").IsNotTrue()).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = goqu.From("test").Where(goqu.L("a").IsNotFalse()).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" WHERE (a IS NULL) []
	// SELECT * FROM "test" WHERE (a IS TRUE) []
	// SELECT * FROM "test" WHERE (a IS FALSE) []
	// SELECT * FROM "test" WHERE (a IS NULL) []
	// SELECT * FROM "test" WHERE (a IS TRUE) []
	// SELECT * FROM "test" WHERE (a IS FALSE) []
	// SELECT * FROM "test" WHERE (a IS NOT NULL) []
	// SELECT * FROM "test" WHERE (a IS NOT TRUE) []
	// SELECT * FROM "test" WHERE (a IS NOT FALSE) []
	// SELECT * FROM "test" WHERE (a IS NOT NULL) []
	// SELECT * FROM "test" WHERE (a IS NOT TRUE) []
	// SELECT * FROM "test" WHERE (a IS NOT FALSE) []
}

func ExampleL_betweenComparisons() {
	ds := goqu.From("test").Where(
		goqu.L("(a + b)").Between(goqu.Range(1, 10)),
	)
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = goqu.From("test").Where(
		goqu.L("(a + b)").NotBetween(goqu.Range(1, 10)),
	)
	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" WHERE ((a + b) BETWEEN 1 AND 10) []
	// SELECT * FROM "test" WHERE ((a + b) BETWEEN ? AND ?) [1 10]
	// SELECT * FROM "test" WHERE ((a + b) NOT BETWEEN 1 AND 10) []
	// SELECT * FROM "test" WHERE ((a + b) NOT BETWEEN ? AND ?) [1 10]
}

func ExampleLAST() {
	ds := goqu.From("test").Select(goqu.LAST("col"))
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT LAST("col") FROM "test" []
	// SELECT LAST("col") FROM "test" []
}

func ExampleLAST_as() {
	sql, _, _ := goqu.From("test").Select(goqu.LAST("a").As("a")).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT LAST("a") AS "a" FROM "test"
}

func ExampleMAX() {
	ds := goqu.From("test").Select(goqu.MAX("col"))
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT MAX("col") FROM "test" []
	// SELECT MAX("col") FROM "test" []
}

func ExampleMAX_as() {
	sql, _, _ := goqu.From("test").Select(goqu.MAX("a").As("a")).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT MAX("a") AS "a" FROM "test"
}

func ExampleMAX_havingClause() {
	ds := goqu.
		From("test").
		Select(goqu.MAX("a").As("MAX")).
		GroupBy("a").
		Having(goqu.MAX("a").Gt(10))

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT MAX("a") AS "MAX" FROM "test" GROUP BY "a" HAVING (MAX("a") > 10) []
	// SELECT MAX("a") AS "MAX" FROM "test" GROUP BY "a" HAVING (MAX("a") > ?) [10]
}

func ExampleMIN() {
	ds := goqu.From("test").Select(goqu.MIN("col"))
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT MIN("col") FROM "test" []
	// SELECT MIN("col") FROM "test" []
}

func ExampleMIN_as() {
	sql, _, _ := goqu.From("test").Select(goqu.MIN("a").As("a")).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT MIN("a") AS "a" FROM "test"
}

func ExampleMIN_havingClause() {
	ds := goqu.
		From("test").
		Select(goqu.MIN("a").As("MIN")).
		GroupBy("a").
		Having(goqu.MIN("a").Gt(10))

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT MIN("a") AS "MIN" FROM "test" GROUP BY "a" HAVING (MIN("a") > 10) []
	// SELECT MIN("a") AS "MIN" FROM "test" GROUP BY "a" HAVING (MIN("a") > ?) [10]
}

func ExampleOn() {
	ds := goqu.From("test").Join(
		goqu.T("my_table"),
		goqu.On(goqu.I("my_table.fkey").Eq(goqu.I("other_table.id"))),
	)

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT * FROM "test" INNER JOIN "my_table" ON ("my_table"."fkey" = "other_table"."id") []
	// SELECT * FROM "test" INNER JOIN "my_table" ON ("my_table"."fkey" = "other_table"."id") []
}

func ExampleOn_withEx() {
	ds := goqu.From("test").Join(
		goqu.T("my_table"),
		goqu.On(goqu.Ex{"my_table.fkey": goqu.I("other_table.id")}),
	)

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT * FROM "test" INNER JOIN "my_table" ON ("my_table"."fkey" = "other_table"."id") []
	// SELECT * FROM "test" INNER JOIN "my_table" ON ("my_table"."fkey" = "other_table"."id") []
}

func ExampleOr() {
	ds := goqu.From("test").Where(
		goqu.Or(
			goqu.C("col").Eq(10),
			goqu.C("col").Eq(20),
		),
	)
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" WHERE (("col" = 10) OR ("col" = 20)) []
	// SELECT * FROM "test" WHERE (("col" = ?) OR ("col" = ?)) [10 20]
}

func ExampleOr_withAnd() {
	ds := goqu.From("items").Where(
		goqu.Or(
			goqu.C("a").Gt(10),
			goqu.And(
				goqu.C("b").Eq(100),
				goqu.C("c").Neq("test"),
			),
		),
	)
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT * FROM "items" WHERE (("a" > 10) OR (("b" = 100) AND ("c" != 'test'))) []
	// SELECT * FROM "items" WHERE (("a" > ?) OR (("b" = ?) AND ("c" != ?))) [10 100 test]
}

func ExampleOr_withExMap() {
	ds := goqu.From("test").Where(
		goqu.Or(
			// Ex will be anded together
			goqu.Ex{
				"col1": 1,
				"col2": true,
			},
			goqu.Ex{
				"col3": nil,
				"col4": "foo",
			},
		),
	)
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" WHERE ((("col1" = 1) AND ("col2" IS TRUE)) OR (("col3" IS NULL) AND ("col4" = 'foo'))) []
	// SELECT * FROM "test" WHERE ((("col1" = ?) AND ("col2" IS ?)) OR (("col3" IS ?) AND ("col4" = ?))) [1 true <nil> foo]
}

func ExampleRange_numbers() {
	ds := goqu.From("test").Where(
		goqu.C("col").Between(goqu.Range(1, 10)),
	)
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = goqu.From("test").Where(
		goqu.C("col").NotBetween(goqu.Range(1, 10)),
	)
	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" WHERE ("col" BETWEEN 1 AND 10) []
	// SELECT * FROM "test" WHERE ("col" BETWEEN ? AND ?) [1 10]
	// SELECT * FROM "test" WHERE ("col" NOT BETWEEN 1 AND 10) []
	// SELECT * FROM "test" WHERE ("col" NOT BETWEEN ? AND ?) [1 10]
}

func ExampleRange_strings() {
	ds := goqu.From("test").Where(
		goqu.C("col").Between(goqu.Range("a", "z")),
	)
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = goqu.From("test").Where(
		goqu.C("col").NotBetween(goqu.Range("a", "z")),
	)
	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" WHERE ("col" BETWEEN 'a' AND 'z') []
	// SELECT * FROM "test" WHERE ("col" BETWEEN ? AND ?) [a z]
	// SELECT * FROM "test" WHERE ("col" NOT BETWEEN 'a' AND 'z') []
	// SELECT * FROM "test" WHERE ("col" NOT BETWEEN ? AND ?) [a z]
}

func ExampleRange_identifiers() {
	ds := goqu.From("test").Where(
		goqu.C("col1").Between(goqu.Range(goqu.C("col2"), goqu.C("col3"))),
	)
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = goqu.From("test").Where(
		goqu.C("col1").NotBetween(goqu.Range(goqu.C("col2"), goqu.C("col3"))),
	)
	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" WHERE ("col1" BETWEEN "col2" AND "col3") []
	// SELECT * FROM "test" WHERE ("col1" BETWEEN "col2" AND "col3") []
	// SELECT * FROM "test" WHERE ("col1" NOT BETWEEN "col2" AND "col3") []
	// SELECT * FROM "test" WHERE ("col1" NOT BETWEEN "col2" AND "col3") []
}

func ExampleS() {
	s := goqu.S("test_schema")
	t := s.Table("test")
	sql, args, _ := goqu.
		From(t).
		Select(
			t.Col("col1"),
			t.Col("col2"),
			t.Col("col3"),
		).
		ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT "test_schema"."test"."col1", "test_schema"."test"."col2", "test_schema"."test"."col3" FROM "test_schema"."test" []
}

func ExampleSUM() {
	ds := goqu.From("test").Select(goqu.SUM("col"))
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT SUM("col") FROM "test" []
	// SELECT SUM("col") FROM "test" []
}

func ExampleSUM_as() {
	sql, _, _ := goqu.From("test").Select(goqu.SUM("a").As("a")).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT SUM("a") AS "a" FROM "test"
}

func ExampleSUM_havingClause() {
	ds := goqu.
		From("test").
		Select(goqu.SUM("a").As("SUM")).
		GroupBy("a").
		Having(goqu.SUM("a").Gt(10))

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT SUM("a") AS "SUM" FROM "test" GROUP BY "a" HAVING (SUM("a") > 10) []
	// SELECT SUM("a") AS "SUM" FROM "test" GROUP BY "a" HAVING (SUM("a") > ?) [10]
}

func ExampleStar() {
	ds := goqu.From("test").Select(goqu.Star())

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" []
	// SELECT * FROM "test" []
}

func ExampleT() {
	t := goqu.T("test")
	sql, args, _ := goqu.
		From(t).
		Select(
			t.Col("col1"),
			t.Col("col2"),
			t.Col("col3"),
		).
		ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT "test"."col1", "test"."col2", "test"."col3" FROM "test" []
}

func ExampleUsing() {
	ds := goqu.From("test").Join(
		goqu.T("my_table"),
		goqu.Using("fkey"),
	)

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT * FROM "test" INNER JOIN "my_table" USING ("fkey") []
	// SELECT * FROM "test" INNER JOIN "my_table" USING ("fkey") []
}

func ExampleUsing_withIdentifier() {
	ds := goqu.From("test").Join(
		goqu.T("my_table"),
		goqu.Using(goqu.C("fkey")),
	)

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT * FROM "test" INNER JOIN "my_table" USING ("fkey") []
	// SELECT * FROM "test" INNER JOIN "my_table" USING ("fkey") []
}

func ExampleEx() {

	ds := goqu.From("items").Where(
		goqu.Ex{
			"col1": "a",
			"col2": 1,
			"col3": true,
			"col4": false,
			"col5": nil,
			"col6": []string{"a", "b", "c"},
		},
	)

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "items" WHERE (("col1" = 'a') AND ("col2" = 1) AND ("col3" IS TRUE) AND ("col4" IS FALSE) AND ("col5" IS NULL) AND ("col6" IN ('a', 'b', 'c'))) []
	// SELECT * FROM "items" WHERE (("col1" = ?) AND ("col2" = ?) AND ("col3" IS ?) AND ("col4" IS ?) AND ("col5" IS ?) AND ("col6" IN (?, ?, ?))) [a 1 true false <nil> a b c]
}

func ExampleEx_withOp() {
	sql, args, _ := goqu.From("items").Where(
		goqu.Ex{
			"col1": goqu.Op{"neq": "a"},
			"col3": goqu.Op{"isNot": true},
			"col6": goqu.Op{"notIn": []string{"a", "b", "c"}},
		},
	).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// SELECT * FROM "items" WHERE (("col1" != 'a') AND ("col3" IS NOT TRUE) AND ("col6" NOT IN ('a', 'b', 'c'))) []
}

func ExampleEx_in() {
	// using an Ex expression map
	sql, _, _ := goqu.From("test").Where(goqu.Ex{
		"a": []string{"a", "b", "c"},
	}).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT * FROM "test" WHERE ("a" IN ('a', 'b', 'c'))
}

func ExampleExOr() {
	sql, args, _ := goqu.From("items").Where(
		goqu.ExOr{
			"col1": "a",
			"col2": 1,
			"col3": true,
			"col4": false,
			"col5": nil,
			"col6": []string{"a", "b", "c"},
		},
	).ToSQL()
	fmt.Println(sql, args)

	// nolint:lll
	// Output:
	// SELECT * FROM "items" WHERE (("col1" = 'a') OR ("col2" = 1) OR ("col3" IS TRUE) OR ("col4" IS FALSE) OR ("col5" IS NULL) OR ("col6" IN ('a', 'b', 'c'))) []
}

func ExampleExOr_withOp() {
	sql, _, _ := goqu.From("items").Where(goqu.ExOr{
		"col1": goqu.Op{"neq": "a"},
		"col3": goqu.Op{"isNot": true},
		"col6": goqu.Op{"notIn": []string{"a", "b", "c"}},
	}).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("items").Where(goqu.ExOr{
		"col1": goqu.Op{"gt": 1},
		"col2": goqu.Op{"gte": 1},
		"col3": goqu.Op{"lt": 1},
		"col4": goqu.Op{"lte": 1},
	}).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("items").Where(goqu.ExOr{
		"col1": goqu.Op{"like": "a%"},
		"col2": goqu.Op{"notLike": "a%"},
		"col3": goqu.Op{"iLike": "a%"},
		"col4": goqu.Op{"notILike": "a%"},
	}).ToSQL()
	fmt.Println(sql)

	sql, _, _ = goqu.From("items").Where(goqu.ExOr{
		"col1": goqu.Op{"like": regexp.MustCompile("^(a|b)")},
		"col2": goqu.Op{"notLike": regexp.MustCompile("^(a|b)")},
		"col3": goqu.Op{"iLike": regexp.MustCompile("^(a|b)")},
		"col4": goqu.Op{"notILike": regexp.MustCompile("^(a|b)")},
	}).ToSQL()
	fmt.Println(sql)

	// Output:
	// SELECT * FROM "items" WHERE (("col1" != 'a') OR ("col3" IS NOT TRUE) OR ("col6" NOT IN ('a', 'b', 'c')))
	// SELECT * FROM "items" WHERE (("col1" > 1) OR ("col2" >= 1) OR ("col3" < 1) OR ("col4" <= 1))
	// SELECT * FROM "items" WHERE (("col1" LIKE 'a%') OR ("col2" NOT LIKE 'a%') OR ("col3" ILIKE 'a%') OR ("col4" NOT ILIKE 'a%'))
	// SELECT * FROM "items" WHERE (("col1" ~ '^(a|b)') OR ("col2" !~ '^(a|b)') OR ("col3" ~* '^(a|b)') OR ("col4" !~* '^(a|b)'))

}

func ExampleOp_comparisons() {

	ds := goqu.From("test").Where(goqu.Ex{
		"a": 10,
		"b": goqu.Op{"neq": 10},
		"c": goqu.Op{"gte": 10},
		"d": goqu.Op{"lt": 10},
		"e": goqu.Op{"lte": 10},
	})

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" WHERE (("a" = 10) AND ("b" != 10) AND ("c" >= 10) AND ("d" < 10) AND ("e" <= 10)) []
	// SELECT * FROM "test" WHERE (("a" = ?) AND ("b" != ?) AND ("c" >= ?) AND ("d" < ?) AND ("e" <= ?)) [10 10 10 10 10]
}

func ExampleOp_inComparisons() {
	// using an Ex expression map
	ds := goqu.From("test").Where(goqu.Ex{
		"a": goqu.Op{"in": []string{"a", "b", "c"}},
	})

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = goqu.From("test").Where(goqu.Ex{
		"a": goqu.Op{"notIn": []string{"a", "b", "c"}},
	})

	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" WHERE ("a" IN ('a', 'b', 'c')) []
	// SELECT * FROM "test" WHERE ("a" IN (?, ?, ?)) [a b c]
	// SELECT * FROM "test" WHERE ("a" NOT IN ('a', 'b', 'c')) []
	// SELECT * FROM "test" WHERE ("a" NOT IN (?, ?, ?)) [a b c]
}

func ExampleOp_likeComparisons() {
	// using an Ex expression map
	ds := goqu.From("test").Where(goqu.Ex{
		"a": goqu.Op{"like": "%a%"},
	})
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = goqu.From("test").Where(goqu.Ex{
		"a": goqu.Op{"like": regexp.MustCompile("(a|b)")},
	})

	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = goqu.From("test").Where(goqu.Ex{
		"a": goqu.Op{"iLike": "%a%"},
	})

	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = goqu.From("test").Where(goqu.Ex{
		"a": goqu.Op{"iLike": regexp.MustCompile("(a|b)")},
	})

	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = goqu.From("test").Where(goqu.Ex{
		"a": goqu.Op{"notLike": "%a%"},
	})

	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = goqu.From("test").Where(goqu.Ex{
		"a": goqu.Op{"notLike": regexp.MustCompile("(a|b)")},
	})

	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = goqu.From("test").Where(goqu.Ex{
		"a": goqu.Op{"notILike": "%a%"},
	})

	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = goqu.From("test").Where(goqu.Ex{
		"a": goqu.Op{"notILike": regexp.MustCompile("(a|b)")},
	})
	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" WHERE ("a" LIKE '%a%') []
	// SELECT * FROM "test" WHERE ("a" LIKE ?) [%a%]
	// SELECT * FROM "test" WHERE ("a" ~ '(a|b)') []
	// SELECT * FROM "test" WHERE ("a" ~ ?) [(a|b)]
	// SELECT * FROM "test" WHERE ("a" ILIKE '%a%') []
	// SELECT * FROM "test" WHERE ("a" ILIKE ?) [%a%]
	// SELECT * FROM "test" WHERE ("a" ~* '(a|b)') []
	// SELECT * FROM "test" WHERE ("a" ~* ?) [(a|b)]
	// SELECT * FROM "test" WHERE ("a" NOT LIKE '%a%') []
	// SELECT * FROM "test" WHERE ("a" NOT LIKE ?) [%a%]
	// SELECT * FROM "test" WHERE ("a" !~ '(a|b)') []
	// SELECT * FROM "test" WHERE ("a" !~ ?) [(a|b)]
	// SELECT * FROM "test" WHERE ("a" NOT ILIKE '%a%') []
	// SELECT * FROM "test" WHERE ("a" NOT ILIKE ?) [%a%]
	// SELECT * FROM "test" WHERE ("a" !~* '(a|b)') []
	// SELECT * FROM "test" WHERE ("a" !~* ?) [(a|b)]
}

func ExampleOp_isComparisons() {
	// using an Ex expression map
	ds := goqu.From("test").Where(goqu.Ex{
		"a": true,
	})
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)
	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = goqu.From("test").Where(goqu.Ex{
		"a": goqu.Op{"is": true},
	})
	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)
	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = goqu.From("test").Where(goqu.Ex{
		"a": false,
	})
	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)
	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = goqu.From("test").Where(goqu.Ex{
		"a": goqu.Op{"is": false},
	})
	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)
	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = goqu.From("test").Where(goqu.Ex{
		"a": nil,
	})
	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)
	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = goqu.From("test").Where(goqu.Ex{
		"a": goqu.Op{"is": nil},
	})
	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)
	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = goqu.From("test").Where(goqu.Ex{
		"a": goqu.Op{"isNot": true},
	})
	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)
	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = goqu.From("test").Where(goqu.Ex{
		"a": goqu.Op{"isNot": false},
	})
	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)
	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = goqu.From("test").Where(goqu.Ex{
		"a": goqu.Op{"isNot": nil},
	})
	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)
	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" WHERE ("a" IS TRUE) []
	// SELECT * FROM "test" WHERE ("a" IS ?) [true]
	// SELECT * FROM "test" WHERE ("a" IS TRUE) []
	// SELECT * FROM "test" WHERE ("a" IS ?) [true]
	// SELECT * FROM "test" WHERE ("a" IS FALSE) []
	// SELECT * FROM "test" WHERE ("a" IS ?) [false]
	// SELECT * FROM "test" WHERE ("a" IS FALSE) []
	// SELECT * FROM "test" WHERE ("a" IS ?) [false]
	// SELECT * FROM "test" WHERE ("a" IS NULL) []
	// SELECT * FROM "test" WHERE ("a" IS ?) [<nil>]
	// SELECT * FROM "test" WHERE ("a" IS NULL) []
	// SELECT * FROM "test" WHERE ("a" IS ?) [<nil>]
	// SELECT * FROM "test" WHERE ("a" IS NOT TRUE) []
	// SELECT * FROM "test" WHERE ("a" IS NOT ?) [true]
	// SELECT * FROM "test" WHERE ("a" IS NOT FALSE) []
	// SELECT * FROM "test" WHERE ("a" IS NOT ?) [false]
	// SELECT * FROM "test" WHERE ("a" IS NOT NULL) []
	// SELECT * FROM "test" WHERE ("a" IS NOT ?) [<nil>]
}

func ExampleOp_betweenComparisons() {
	ds := goqu.From("test").Where(goqu.Ex{
		"a": goqu.Op{"between": goqu.Range(1, 10)},
	})
	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = goqu.From("test").Where(goqu.Ex{
		"a": goqu.Op{"notBetween": goqu.Range(1, 10)},
	})
	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" WHERE ("a" BETWEEN 1 AND 10) []
	// SELECT * FROM "test" WHERE ("a" BETWEEN ? AND ?) [1 10]
	// SELECT * FROM "test" WHERE ("a" NOT BETWEEN 1 AND 10) []
	// SELECT * FROM "test" WHERE ("a" NOT BETWEEN ? AND ?) [1 10]
}

// When using a single op with multiple keys they are ORed together
func ExampleOp_withMultipleKeys() {
	ds := goqu.From("items").Where(goqu.Ex{
		"col1": goqu.Op{"is": nil, "eq": 10},
	})

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "items" WHERE (("col1" = 10) OR ("col1" IS NULL)) []
	// SELECT * FROM "items" WHERE (("col1" = ?) OR ("col1" IS ?)) [10 <nil>]
}

func ExampleRecord_insert() {
	ds := goqu.Insert("test")

	records := []goqu.Record{
		{"col1": 1, "col2": "foo"},
		{"col1": 2, "col2": "bar"},
	}

	sql, args, _ := ds.Rows(records).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).Rows(records).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// INSERT INTO "test" ("col1", "col2") VALUES (1, 'foo'), (2, 'bar') []
	// INSERT INTO "test" ("col1", "col2") VALUES (?, ?), (?, ?) [1 foo 2 bar]
}

func ExampleRecord_update() {
	ds := goqu.Update("test")
	update := goqu.Record{"col1": 1, "col2": "foo"}

	sql, args, _ := ds.Set(update).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).Set(update).ToSQL()
	fmt.Println(sql, args)
	// Output:
	// UPDATE "test" SET "col1"=1,"col2"='foo' []
	// UPDATE "test" SET "col1"=?,"col2"=? [1 foo]
}

func ExampleV() {
	ds := goqu.From("user").Select(
		goqu.V(true).As("is_verified"),
		goqu.V(1.2).As("version"),
		"first_name",
		"last_name",
	)

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	ds = goqu.From("user").Where(goqu.V(1).Neq(1))
	sql, args, _ = ds.ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT TRUE AS "is_verified", 1.2 AS "version", "first_name", "last_name" FROM "user" []
	// SELECT * FROM "user" WHERE (1 != 1) []
}

func ExampleV_prepared() {
	ds := goqu.From("user").Select(
		goqu.V(true).As("is_verified"),
		goqu.V(1.2).As("version"),
		"first_name",
		"last_name",
	)

	sql, args, _ := ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	ds = goqu.From("user").Where(goqu.V(1).Neq(1))

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT ? AS "is_verified", ? AS "version", "first_name", "last_name" FROM "user" [true 1.2]
	// SELECT * FROM "user" WHERE (? != ?) [1 1]
}

func ExampleVals() {
	ds := goqu.Insert("user").
		Cols("first_name", "last_name", "is_verified").
		Vals(
			goqu.Vals{"Greg", "Farley", true},
			goqu.Vals{"Jimmy", "Stewart", true},
			goqu.Vals{"Jeff", "Jeffers", false},
		)
	insertSQL, args, _ := ds.ToSQL()
	fmt.Println(insertSQL, args)

	// Output:
	// INSERT INTO "user" ("first_name", "last_name", "is_verified") VALUES ('Greg', 'Farley', TRUE), ('Jimmy', 'Stewart', TRUE), ('Jeff', 'Jeffers', FALSE) []
}

func ExampleW() {
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
