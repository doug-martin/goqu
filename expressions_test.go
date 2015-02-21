package gql

import "fmt"

func ExampleOr() {
	sql, _ := From("test").Where(Or(
		I("a").Gt(10),
		I("a").Lt(5),
	)).Sql()
	fmt.Printf(sql)
}

func ExampleOr_withAnd() {
	sql, _ := From("items").Where(Or(
		I("a").Gt(10),
		And(
			I("b").Eq(100),
			I("c").Neq("test"),
		),
	)).Sql()
	fmt.Printf(sql)
}

func ExampleAnd() {
	//by default Where assumes an And
	sql, _ := From("test").Where(
		I("a").Gt(10),
		I("b").Lt(5),
	).Sql()
	fmt.Printf(sql)
}

func ExampleAnd_withOr() {
	sql, _ := From("test").Where(
		I("a").Gt(10),
		Or(
			I("b").Lt(5),
			I("c").In([]string{"hello", "world"}),
		),
	).Sql()
	fmt.Printf(sql)
}

func ExampleOn() {
	sql, _ := From("test").Join(
		I("test2"),
		On(I("test2.test_id").Eq(I("test.id"))),
	).Sql()
	fmt.Printf(sql)
}

func ExampleUsing() {
	sql, _ := From("test").Join(
		I("test2"),
		Using("name"),
	).Sql()
	fmt.Printf(sql)
}

func ExampleI() {
	sql, _ := From("test").Where(
		I("a").Eq(10),
		I("b").Lt(10),
		I("d").IsTrue(),
	).Sql()
	fmt.Printf(sql)

	//qualify with schema
	sql, _ = From(I("test").Schema("my_schema")).Sql()
	fmt.Printf(sql)

	sql, _ = From(I("mychema.test")).Where(
		//qualify with schema, table, and col
		I("my_schema.test.a").Eq(10),
	).Sql()
	fmt.Printf(sql)

	//* will be taken literally and no quoted
	sql, _ = From(I("test")).Select(I("test.*")).Sql()
	fmt.Printf(sql)

	fmt.Printf(sql)
}
