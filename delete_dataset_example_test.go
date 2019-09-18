package goqu_test

import (
	"fmt"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/mysql"
)

func ExampleDelete() {
	ds := goqu.Delete("items")

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	// Output:
	// DELETE FROM "items" []
}

func ExampleDeleteDataset_Executor() {
	db := getDb()

	de := db.Delete("goqu_user").
		Where(goqu.Ex{"first_name": "Bob"}).
		Executor()
	if r, err := de.Exec(); err != nil {
		fmt.Println(err.Error())
	} else {
		c, _ := r.RowsAffected()
		fmt.Printf("Deleted %d users", c)
	}

	// Output:
	// Deleted 1 users
}

func ExampleDeleteDataset_Executor_returning() {
	db := getDb()

	de := db.Delete("goqu_user").
		Where(goqu.C("last_name").Eq("Yukon")).
		Returning(goqu.C("id")).
		Executor()

	var ids []int64
	if err := de.ScanVals(&ids); err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Printf("Deleted users [ids:=%+v]", ids)
	}

	// Output:
	// Deleted users [ids:=[1 2 3]]
}

func ExampleDeleteDataset_With() {

	sql, _, _ := goqu.Delete("test").
		With("check_vals(val)", goqu.From().Select(goqu.L("123"))).
		Where(goqu.C("val").Eq(goqu.From("check_vals").Select("val"))).
		ToSQL()
	fmt.Println(sql)

	// Output:
	// WITH check_vals(val) AS (SELECT 123) DELETE FROM "test" WHERE ("val" IN (SELECT "val" FROM "check_vals"))
}

func ExampleDeleteDataset_WithRecursive() {
	sql, _, _ := goqu.Delete("nums").
		WithRecursive("nums(x)",
			goqu.From().Select(goqu.L("1")).
				UnionAll(goqu.From("nums").
					Select(goqu.L("x+1")).Where(goqu.C("x").Lt(5)))).
		ToSQL()
	fmt.Println(sql)
	// Output:
	// WITH RECURSIVE nums(x) AS (SELECT 1 UNION ALL (SELECT x+1 FROM "nums" WHERE ("x" < 5))) DELETE FROM "nums"
}

func ExampleDeleteDataset_Where() {
	// By default everything is anded together
	sql, _, _ := goqu.Delete("test").Where(goqu.Ex{
		"a": goqu.Op{"gt": 10},
		"b": goqu.Op{"lt": 10},
		"c": nil,
		"d": []string{"a", "b", "c"},
	}).ToSQL()
	fmt.Println(sql)
	// You can use ExOr to get ORed expressions together
	sql, _, _ = goqu.Delete("test").Where(goqu.ExOr{
		"a": goqu.Op{"gt": 10},
		"b": goqu.Op{"lt": 10},
		"c": nil,
		"d": []string{"a", "b", "c"},
	}).ToSQL()
	fmt.Println(sql)
	// You can use Or with Ex to Or multiple Ex maps together
	sql, _, _ = goqu.Delete("test").Where(
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
	sql, _, _ = goqu.Delete("test").Where(
		goqu.C("a").Gt(10),
		goqu.C("b").Lt(10),
		goqu.C("c").IsNull(),
		goqu.C("d").In("a", "b", "c"),
	).ToSQL()
	fmt.Println(sql)
	// You can use a combination of Ors and Ands
	sql, _, _ = goqu.Delete("test").Where(
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
	// DELETE FROM "test" WHERE (("a" > 10) AND ("b" < 10) AND ("c" IS NULL) AND ("d" IN ('a', 'b', 'c')))
	// DELETE FROM "test" WHERE (("a" > 10) OR ("b" < 10) OR ("c" IS NULL) OR ("d" IN ('a', 'b', 'c')))
	// DELETE FROM "test" WHERE ((("a" > 10) AND ("b" < 10)) OR (("c" IS NULL) AND ("d" IN ('a', 'b', 'c'))))
	// DELETE FROM "test" WHERE (("a" > 10) AND ("b" < 10) AND ("c" IS NULL) AND ("d" IN ('a', 'b', 'c')))
	// DELETE FROM "test" WHERE (("a" > 10) OR (("b" < 10) AND ("c" IS NULL)))
}

func ExampleDeleteDataset_Where_prepared() {
	// By default everything is anded together
	sql, args, _ := goqu.Delete("test").Prepared(true).Where(goqu.Ex{
		"a": goqu.Op{"gt": 10},
		"b": goqu.Op{"lt": 10},
		"c": nil,
		"d": []string{"a", "b", "c"},
	}).ToSQL()
	fmt.Println(sql, args)
	// You can use ExOr to get ORed expressions together
	sql, args, _ = goqu.Delete("test").Prepared(true).Where(goqu.ExOr{
		"a": goqu.Op{"gt": 10},
		"b": goqu.Op{"lt": 10},
		"c": nil,
		"d": []string{"a", "b", "c"},
	}).ToSQL()
	fmt.Println(sql, args)
	// You can use Or with Ex to Or multiple Ex maps together
	sql, args, _ = goqu.Delete("test").Prepared(true).Where(
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
	sql, args, _ = goqu.Delete("test").Prepared(true).Where(
		goqu.C("a").Gt(10),
		goqu.C("b").Lt(10),
		goqu.C("c").IsNull(),
		goqu.C("d").In("a", "b", "c"),
	).ToSQL()
	fmt.Println(sql, args)
	// You can use a combination of Ors and Ands
	sql, args, _ = goqu.Delete("test").Prepared(true).Where(
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
	// DELETE FROM "test" WHERE (("a" > ?) AND ("b" < ?) AND ("c" IS ?) AND ("d" IN (?, ?, ?))) [10 10 <nil> a b c]
	// DELETE FROM "test" WHERE (("a" > ?) OR ("b" < ?) OR ("c" IS ?) OR ("d" IN (?, ?, ?))) [10 10 <nil> a b c]
	// DELETE FROM "test" WHERE ((("a" > ?) AND ("b" < ?)) OR (("c" IS ?) AND ("d" IN (?, ?, ?)))) [10 10 <nil> a b c]
	// DELETE FROM "test" WHERE (("a" > ?) AND ("b" < ?) AND ("c" IS ?) AND ("d" IN (?, ?, ?))) [10 10 <nil> a b c]
	// DELETE FROM "test" WHERE (("a" > ?) OR (("b" < ?) AND ("c" IS ?))) [10 10 <nil>]
}

func ExampleDeleteDataset_ClearWhere() {
	ds := goqu.Delete("test").Where(
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
	// DELETE FROM "test"
}

func ExampleDeleteDataset_Limit() {
	ds := goqu.Dialect("mysql").Delete("test").Limit(10)
	sql, _, _ := ds.ToSQL()
	fmt.Println(sql)
	// Output:
	// DELETE FROM `test` LIMIT 10
}

func ExampleDeleteDataset_LimitAll() {
	// Using mysql dialect because it supports limit on delete
	ds := goqu.Dialect("mysql").Delete("test").LimitAll()
	sql, _, _ := ds.ToSQL()
	fmt.Println(sql)
	// Output:
	// DELETE FROM `test` LIMIT ALL
}

func ExampleDeleteDataset_ClearLimit() {
	// Using mysql dialect because it supports limit on delete
	ds := goqu.Dialect("mysql").Delete("test").Limit(10)
	sql, _, _ := ds.ClearLimit().ToSQL()
	fmt.Println(sql)
	// Output:
	// DELETE FROM `test`
}

func ExampleDeleteDataset_Order() {
	// use mysql dialect because it supports order by on deletes
	ds := goqu.Dialect("mysql").Delete("test").Order(goqu.C("a").Asc())
	sql, _, _ := ds.ToSQL()
	fmt.Println(sql)
	// Output:
	// DELETE FROM `test` ORDER BY `a` ASC
}

func ExampleDeleteDataset_OrderAppend() {
	// use mysql dialect because it supports order by on deletes
	ds := goqu.Dialect("mysql").Delete("test").Order(goqu.C("a").Asc())
	sql, _, _ := ds.OrderAppend(goqu.C("b").Desc().NullsLast()).ToSQL()
	fmt.Println(sql)
	// Output:
	// DELETE FROM `test` ORDER BY `a` ASC, `b` DESC NULLS LAST
}

func ExampleDeleteDataset_OrderPrepend() {
	// use mysql dialect because it supports order by on deletes
	ds := goqu.Dialect("mysql").Delete("test").Order(goqu.C("a").Asc())
	sql, _, _ := ds.OrderPrepend(goqu.C("b").Desc().NullsLast()).ToSQL()
	fmt.Println(sql)
	// Output:
	// DELETE FROM `test` ORDER BY `b` DESC NULLS LAST, `a` ASC
}

func ExampleDeleteDataset_ClearOrder() {
	ds := goqu.Delete("test").Order(goqu.C("a").Asc())
	sql, _, _ := ds.ClearOrder().ToSQL()
	fmt.Println(sql)
	// Output:
	// DELETE FROM "test"
}

func ExampleDeleteDataset_ToSQL() {
	sql, args, _ := goqu.Delete("items").ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = goqu.Delete("items").
		Where(goqu.Ex{"id": goqu.Op{"gt": 10}}).
		ToSQL()
	fmt.Println(sql, args)

	// Output:
	// DELETE FROM "items" []
	// DELETE FROM "items" WHERE ("id" > 10) []
}

func ExampleDeleteDataset_Prepared() {
	sql, args, _ := goqu.Delete("items").Prepared(true).ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = goqu.Delete("items").
		Prepared(true).
		Where(goqu.Ex{"id": goqu.Op{"gt": 10}}).
		ToSQL()
	fmt.Println(sql, args)

	// Output:
	// DELETE FROM "items" []
	// DELETE FROM "items" WHERE ("id" > ?) [10]
}

func ExampleDeleteDataset_Returning() {
	ds := goqu.Delete("items")
	sql, args, _ := ds.Returning("id").ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Returning("id").Where(goqu.C("id").IsNotNull()).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// DELETE FROM "items" RETURNING "id" []
	// DELETE FROM "items" WHERE ("id" IS NOT NULL) RETURNING "id" []
}
