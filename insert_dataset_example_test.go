// nolint:lll
package goqu_test

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/postgres"
)

func ExampleInsert_goquRecord() {
	ds := goqu.Insert("user").Rows(
		goqu.Record{"first_name": "Greg", "last_name": "Farley"},
		goqu.Record{"first_name": "Jimmy", "last_name": "Stewart"},
		goqu.Record{"first_name": "Jeff", "last_name": "Jeffers"},
	)
	insertSQL, args, _ := ds.ToSQL()
	fmt.Println(insertSQL, args)

	// Output:
	// INSERT INTO "user" ("first_name", "last_name") VALUES ('Greg', 'Farley'), ('Jimmy', 'Stewart'), ('Jeff', 'Jeffers') []
}

func ExampleInsert_map() {
	ds := goqu.Insert("user").Rows(
		map[string]interface{}{"first_name": "Greg", "last_name": "Farley"},
		map[string]interface{}{"first_name": "Jimmy", "last_name": "Stewart"},
		map[string]interface{}{"first_name": "Jeff", "last_name": "Jeffers"},
	)
	insertSQL, args, _ := ds.ToSQL()
	fmt.Println(insertSQL, args)

	// Output:
	// INSERT INTO "user" ("first_name", "last_name") VALUES ('Greg', 'Farley'), ('Jimmy', 'Stewart'), ('Jeff', 'Jeffers') []
}

func ExampleInsert_struct() {
	type User struct {
		FirstName string `db:"first_name"`
		LastName  string `db:"last_name"`
	}
	ds := goqu.Insert("user").Rows(
		User{FirstName: "Greg", LastName: "Farley"},
		User{FirstName: "Jimmy", LastName: "Stewart"},
		User{FirstName: "Jeff", LastName: "Jeffers"},
	)
	insertSQL, args, _ := ds.ToSQL()
	fmt.Println(insertSQL, args)

	// Output:
	// INSERT INTO "user" ("first_name", "last_name") VALUES ('Greg', 'Farley'), ('Jimmy', 'Stewart'), ('Jeff', 'Jeffers') []
}

func ExampleInsert_prepared() {
	ds := goqu.Insert("user").Prepared(true).Rows(
		goqu.Record{"first_name": "Greg", "last_name": "Farley"},
		goqu.Record{"first_name": "Jimmy", "last_name": "Stewart"},
		goqu.Record{"first_name": "Jeff", "last_name": "Jeffers"},
	)
	insertSQL, args, _ := ds.ToSQL()
	fmt.Println(insertSQL, args)

	// Output:
	// INSERT INTO "user" ("first_name", "last_name") VALUES (?, ?), (?, ?), (?, ?) [Greg Farley Jimmy Stewart Jeff Jeffers]
}

func ExampleInsert_fromQuery() {
	ds := goqu.Insert("user").Prepared(true).
		FromQuery(goqu.From("other_table"))
	insertSQL, args, _ := ds.ToSQL()
	fmt.Println(insertSQL, args)

	// Output:
	// INSERT INTO "user" SELECT * FROM "other_table" []
}

func ExampleInsert_fromQueryWithCols() {
	ds := goqu.Insert("user").Prepared(true).
		Cols("first_name", "last_name").
		FromQuery(goqu.From("other_table").Select("fn", "ln"))
	insertSQL, args, _ := ds.ToSQL()
	fmt.Println(insertSQL, args)

	// Output:
	// INSERT INTO "user" ("first_name", "last_name") SELECT "fn", "ln" FROM "other_table" []
}

func ExampleInsert_colsAndVals() {
	ds := goqu.Insert("user").
		Cols("first_name", "last_name").
		Vals(
			goqu.Vals{"Greg", "Farley"},
			goqu.Vals{"Jimmy", "Stewart"},
			goqu.Vals{"Jeff", "Jeffers"},
		)
	insertSQL, args, _ := ds.ToSQL()
	fmt.Println(insertSQL, args)

	// Output:
	// INSERT INTO "user" ("first_name", "last_name") VALUES ('Greg', 'Farley'), ('Jimmy', 'Stewart'), ('Jeff', 'Jeffers') []
}

func ExampleInsertDataset_Executor_withRecord() {
	db := getDb()
	insert := db.Insert("goqu_user").Rows(
		goqu.Record{"first_name": "Jed", "last_name": "Riley", "created": time.Now()},
	).Executor()
	if _, err := insert.Exec(); err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("Inserted 1 user")
	}

	users := []goqu.Record{
		{"first_name": "Greg", "last_name": "Farley", "created": time.Now()},
		{"first_name": "Jimmy", "last_name": "Stewart", "created": time.Now()},
		{"first_name": "Jeff", "last_name": "Jeffers", "created": time.Now()},
	}
	if _, err := db.Insert("goqu_user").Rows(users).Executor().Exec(); err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Printf("Inserted %d users", len(users))
	}

	// Output:
	// Inserted 1 user
	// Inserted 3 users
}

func ExampleInsertDataset_Executor_recordReturning() {
	db := getDb()

	type User struct {
		ID        sql.NullInt64 `db:"id"`
		FirstName string        `db:"first_name"`
		LastName  string        `db:"last_name"`
		Created   time.Time     `db:"created"`
	}

	insert := db.Insert("goqu_user").Returning(goqu.C("id")).Rows(
		goqu.Record{"first_name": "Jed", "last_name": "Riley", "created": time.Now()},
	).Executor()
	var id int64
	if _, err := insert.ScanVal(&id); err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Printf("Inserted 1 user id:=%d\n", id)
	}

	insert = db.Insert("goqu_user").Returning(goqu.Star()).Rows([]goqu.Record{
		{"first_name": "Greg", "last_name": "Farley", "created": time.Now()},
		{"first_name": "Jimmy", "last_name": "Stewart", "created": time.Now()},
		{"first_name": "Jeff", "last_name": "Jeffers", "created": time.Now()},
	}).Executor()
	var insertedUsers []User
	if err := insert.ScanStructs(&insertedUsers); err != nil {
		fmt.Println(err.Error())
	} else {
		for _, u := range insertedUsers {
			fmt.Printf("Inserted user: [ID=%d], [FirstName=%+s] [LastName=%s]\n", u.ID.Int64, u.FirstName, u.LastName)
		}

	}

	// Output:
	// Inserted 1 user id:=5
	// Inserted user: [ID=6], [FirstName=Greg] [LastName=Farley]
	// Inserted user: [ID=7], [FirstName=Jimmy] [LastName=Stewart]
	// Inserted user: [ID=8], [FirstName=Jeff] [LastName=Jeffers]
}

func ExampleInsertDataset_Executor_scanStructs() {
	db := getDb()

	type User struct {
		ID        sql.NullInt64 `db:"id" goqu:"skipinsert"`
		FirstName string        `db:"first_name"`
		LastName  string        `db:"last_name"`
		Created   time.Time     `db:"created"`
	}

	insert := db.Insert("goqu_user").Returning("id").Rows(
		User{FirstName: "Jed", LastName: "Riley"},
	).Executor()
	var id int64
	if _, err := insert.ScanVal(&id); err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Printf("Inserted 1 user id:=%d\n", id)
	}

	insert = db.Insert("goqu_user").Returning(goqu.Star()).Rows([]User{
		{FirstName: "Greg", LastName: "Farley", Created: time.Now()},
		{FirstName: "Jimmy", LastName: "Stewart", Created: time.Now()},
		{FirstName: "Jeff", LastName: "Jeffers", Created: time.Now()},
	}).Executor()
	var insertedUsers []User
	if err := insert.ScanStructs(&insertedUsers); err != nil {
		fmt.Println(err.Error())
	} else {
		for _, u := range insertedUsers {
			fmt.Printf("Inserted user: [ID=%d], [FirstName=%+s] [LastName=%s]\n", u.ID.Int64, u.FirstName, u.LastName)
		}

	}

	// Output:
	// Inserted 1 user id:=5
	// Inserted user: [ID=6], [FirstName=Greg] [LastName=Farley]
	// Inserted user: [ID=7], [FirstName=Jimmy] [LastName=Stewart]
	// Inserted user: [ID=8], [FirstName=Jeff] [LastName=Jeffers]
}

func ExampleInsertDataset_FromQuery() {
	insertSQL, _, _ := goqu.Insert("test").
		FromQuery(goqu.From("test2").Where(goqu.C("age").Gt(10))).
		ToSQL()
	fmt.Println(insertSQL)
	// Output:
	// INSERT INTO "test" SELECT * FROM "test2" WHERE ("age" > 10)
}

func ExampleInsertDataset_ToSQL() {
	type item struct {
		ID      uint32 `db:"id" goqu:"skipinsert"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	insertSQL, args, _ := goqu.Insert("items").Rows(
		item{Name: "Test1", Address: "111 Test Addr"},
		item{Name: "Test2", Address: "112 Test Addr"},
	).ToSQL()
	fmt.Println(insertSQL, args)

	insertSQL, args, _ = goqu.Insert("items").Rows(
		goqu.Record{"name": "Test1", "address": "111 Test Addr"},
		goqu.Record{"name": "Test2", "address": "112 Test Addr"},
	).ToSQL()
	fmt.Println(insertSQL, args)

	insertSQL, args, _ = goqu.Insert("items").Rows(
		[]item{
			{Name: "Test1", Address: "111 Test Addr"},
			{Name: "Test2", Address: "112 Test Addr"},
		}).ToSQL()
	fmt.Println(insertSQL, args)

	insertSQL, args, _ = goqu.From("items").Insert().Rows(
		[]goqu.Record{
			{"name": "Test1", "address": "111 Test Addr"},
			{"name": "Test2", "address": "112 Test Addr"},
		}).ToSQL()
	fmt.Println(insertSQL, args)
	// Output:
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') []
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') []
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') []
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') []
}

func ExampleInsertDataset_Prepared() {
	type item struct {
		ID      uint32 `db:"id" goqu:"skipinsert"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	insertSQL, args, _ := goqu.Insert("items").Prepared(true).Rows(
		item{Name: "Test1", Address: "111 Test Addr"},
		item{Name: "Test2", Address: "112 Test Addr"},
	).ToSQL()
	fmt.Println(insertSQL, args)

	insertSQL, args, _ = goqu.Insert("items").Prepared(true).Rows(
		goqu.Record{"name": "Test1", "address": "111 Test Addr"},
		goqu.Record{"name": "Test2", "address": "112 Test Addr"},
	).ToSQL()
	fmt.Println(insertSQL, args)

	insertSQL, args, _ = goqu.Insert("items").Prepared(true).Rows(
		[]item{
			{Name: "Test1", Address: "111 Test Addr"},
			{Name: "Test2", Address: "112 Test Addr"},
		}).ToSQL()
	fmt.Println(insertSQL, args)

	insertSQL, args, _ = goqu.Insert("items").Prepared(true).Rows(
		[]goqu.Record{
			{"name": "Test1", "address": "111 Test Addr"},
			{"name": "Test2", "address": "112 Test Addr"},
		}).ToSQL()
	fmt.Println(insertSQL, args)
	// Output:
	// INSERT INTO "items" ("address", "name") VALUES (?, ?), (?, ?) [111 Test Addr Test1 112 Test Addr Test2]
	// INSERT INTO "items" ("address", "name") VALUES (?, ?), (?, ?) [111 Test Addr Test1 112 Test Addr Test2]
	// INSERT INTO "items" ("address", "name") VALUES (?, ?), (?, ?) [111 Test Addr Test1 112 Test Addr Test2]
	// INSERT INTO "items" ("address", "name") VALUES (?, ?), (?, ?) [111 Test Addr Test1 112 Test Addr Test2]
}

func ExampleInsertDataset_ClearRows() {
	type item struct {
		ID      uint32 `goqu:"skipinsert"`
		Address string
		Name    string
	}
	ds := goqu.Insert("items").Rows(
		item{Name: "Test1", Address: "111 Test Addr"},
		item{Name: "Test2", Address: "112 Test Addr"},
	)
	insertSQL, args, _ := ds.ClearRows().ToSQL()
	fmt.Println(insertSQL, args)

	// Output:
	// INSERT INTO "items" DEFAULT VALUES []
}

func ExampleInsertDataset_Rows_withNoDbTag() {
	type item struct {
		ID      uint32 `goqu:"skipinsert"`
		Address string
		Name    string
	}
	insertSQL, args, _ := goqu.Insert("items").
		Rows(
			item{Name: "Test1", Address: "111 Test Addr"},
			item{Name: "Test2", Address: "112 Test Addr"},
		).
		ToSQL()
	fmt.Println(insertSQL, args)

	insertSQL, args, _ = goqu.Insert("items").
		Rows(
			item{Name: "Test1", Address: "111 Test Addr"},
			item{Name: "Test2", Address: "112 Test Addr"},
		).
		ToSQL()
	fmt.Println(insertSQL, args)

	insertSQL, args, _ = goqu.Insert("items").
		Rows([]item{
			{Name: "Test1", Address: "111 Test Addr"},
			{Name: "Test2", Address: "112 Test Addr"},
		}).
		ToSQL()
	fmt.Println(insertSQL, args)

	// Output:
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') []
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') []
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') []
}

func ExampleInsertDataset_Rows_withGoquSkipInsertTag() {
	type item struct {
		ID      uint32 `goqu:"skipinsert"`
		Address string
		Name    string `goqu:"skipinsert"`
	}
	insertSQL, args, _ := goqu.Insert("items").
		Rows(
			item{Name: "Test1", Address: "111 Test Addr"},
			item{Name: "Test2", Address: "112 Test Addr"},
		).
		ToSQL()
	fmt.Println(insertSQL, args)

	insertSQL, args, _ = goqu.Insert("items").
		Rows([]item{
			{Name: "Test1", Address: "111 Test Addr"},
			{Name: "Test2", Address: "112 Test Addr"},
		}).
		ToSQL()
	fmt.Println(insertSQL, args)

	// Output:
	// INSERT INTO "items" ("address") VALUES ('111 Test Addr'), ('112 Test Addr') []
	// INSERT INTO "items" ("address") VALUES ('111 Test Addr'), ('112 Test Addr') []
}

func ExampleInsertDataset_Rows_withGoquDefaultIfEmptyTag() {
	type item struct {
		ID      uint32 `goqu:"skipinsert"`
		Address string
		Name    string `goqu:"defaultifempty"`
	}
	insertSQL, args, _ := goqu.Insert("items").
		Rows(
			item{Name: "Test1", Address: "111 Test Addr"},
			item{Address: "112 Test Addr"},
		).
		ToSQL()
	fmt.Println(insertSQL, args)

	insertSQL, args, _ = goqu.Insert("items").
		Rows([]item{
			{Address: "111 Test Addr"},
			{Name: "Test2", Address: "112 Test Addr"},
		}).
		ToSQL()
	fmt.Println(insertSQL, args)

	// Output:
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', DEFAULT) []
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', DEFAULT), ('112 Test Addr', 'Test2') []
}

func ExampleInsertDataset_Rows_withEmbeddedStruct() {
	type Address struct {
		Street string `db:"address_street"`
		State  string `db:"address_state"`
	}
	type User struct {
		Address
		FirstName string
		LastName  string
	}
	ds := goqu.Insert("user").Rows(
		User{Address: Address{Street: "111 Street", State: "NY"}, FirstName: "Greg", LastName: "Farley"},
		User{Address: Address{Street: "211 Street", State: "NY"}, FirstName: "Jimmy", LastName: "Stewart"},
		User{Address: Address{Street: "311 Street", State: "NY"}, FirstName: "Jeff", LastName: "Jeffers"},
	)
	insertSQL, args, _ := ds.ToSQL()
	fmt.Println(insertSQL, args)

	// Output:
	// INSERT INTO "user" ("address_state", "address_street", "firstname", "lastname") VALUES ('NY', '111 Street', 'Greg', 'Farley'), ('NY', '211 Street', 'Jimmy', 'Stewart'), ('NY', '311 Street', 'Jeff', 'Jeffers') []
}

func ExampleInsertDataset_Rows_withIgnoredEmbedded() {
	type Address struct {
		Street string
		State  string
	}
	type User struct {
		Address   `db:"-"`
		FirstName string
		LastName  string
	}
	ds := goqu.Insert("user").Rows(
		User{Address: Address{Street: "111 Street", State: "NY"}, FirstName: "Greg", LastName: "Farley"},
		User{Address: Address{Street: "211 Street", State: "NY"}, FirstName: "Jimmy", LastName: "Stewart"},
		User{Address: Address{Street: "311 Street", State: "NY"}, FirstName: "Jeff", LastName: "Jeffers"},
	)
	insertSQL, args, _ := ds.ToSQL()
	fmt.Println(insertSQL, args)

	// Output:
	// INSERT INTO "user" ("firstname", "lastname") VALUES ('Greg', 'Farley'), ('Jimmy', 'Stewart'), ('Jeff', 'Jeffers') []
}

func ExampleInsertDataset_Rows_withNilEmbeddedPointer() {
	type Address struct {
		Street string
		State  string
	}
	type User struct {
		*Address
		FirstName string
		LastName  string
	}
	ds := goqu.Insert("user").Rows(
		User{FirstName: "Greg", LastName: "Farley"},
		User{FirstName: "Jimmy", LastName: "Stewart"},
		User{FirstName: "Jeff", LastName: "Jeffers"},
	)
	insertSQL, args, _ := ds.ToSQL()
	fmt.Println(insertSQL, args)

	// Output:
	// INSERT INTO "user" ("firstname", "lastname") VALUES ('Greg', 'Farley'), ('Jimmy', 'Stewart'), ('Jeff', 'Jeffers') []
}

func ExampleInsertDataset_ClearOnConflict() {
	type item struct {
		ID      uint32 `db:"id" goqu:"skipinsert"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	ds := goqu.Insert("items").OnConflict(goqu.DoNothing())
	insertSQL, args, _ := ds.ClearOnConflict().Rows(
		item{Name: "Test1", Address: "111 Test Addr"},
		item{Name: "Test2", Address: "112 Test Addr"},
	).ToSQL()
	fmt.Println(insertSQL, args)

	// Output:
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') []
}

func ExampleInsertDataset_OnConflict_doNothing() {
	type item struct {
		ID      uint32 `db:"id" goqu:"skipinsert"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	insertSQL, args, _ := goqu.Insert("items").Rows(
		item{Name: "Test1", Address: "111 Test Addr"},
		item{Name: "Test2", Address: "112 Test Addr"},
	).OnConflict(goqu.DoNothing()).ToSQL()
	fmt.Println(insertSQL, args)

	// Output:
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') ON CONFLICT DO NOTHING []
}

func ExampleInsertDataset_OnConflict_doUpdate() {
	insertSQL, args, _ := goqu.Insert("items").
		Rows(
			goqu.Record{"name": "Test1", "address": "111 Test Addr"},
			goqu.Record{"name": "Test2", "address": "112 Test Addr"},
		).
		OnConflict(goqu.DoUpdate("key", goqu.Record{"updated": goqu.L("NOW()")})).
		ToSQL()
	fmt.Println(insertSQL, args)

	// Output:
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') ON CONFLICT (key) DO UPDATE SET "updated"=NOW() []
}

func ExampleInsertDataset_OnConflict_doUpdateWithWhere() {
	type item struct {
		ID      uint32 `db:"id" goqu:"skipinsert"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	insertSQL, args, _ := goqu.Insert("items").
		Rows([]item{
			{Name: "Test1", Address: "111 Test Addr"},
			{Name: "Test2", Address: "112 Test Addr"},
		}).
		OnConflict(goqu.DoUpdate(
			"key",
			goqu.Record{"updated": goqu.L("NOW()")}).Where(goqu.C("allow_update").IsTrue()),
		).
		ToSQL()
	fmt.Println(insertSQL, args)

	// Output:
	// INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('112 Test Addr', 'Test2') ON CONFLICT (key) DO UPDATE SET "updated"=NOW() WHERE ("allow_update" IS TRUE) []
}

func ExampleInsertDataset_Returning() {
	insertSQL, _, _ := goqu.Insert("test").
		Returning("id").
		Rows(goqu.Record{"a": "a", "b": "b"}).
		ToSQL()
	fmt.Println(insertSQL)
	insertSQL, _, _ = goqu.Insert("test").
		Returning(goqu.T("test").All()).
		Rows(goqu.Record{"a": "a", "b": "b"}).
		ToSQL()
	fmt.Println(insertSQL)
	insertSQL, _, _ = goqu.Insert("test").
		Returning("a", "b").
		Rows(goqu.Record{"a": "a", "b": "b"}).
		ToSQL()
	fmt.Println(insertSQL)
	// Output:
	// INSERT INTO "test" ("a", "b") VALUES ('a', 'b') RETURNING "id"
	// INSERT INTO "test" ("a", "b") VALUES ('a', 'b') RETURNING "test".*
	// INSERT INTO "test" ("a", "b") VALUES ('a', 'b') RETURNING "a", "b"
}

func ExampleInsertDataset_With() {
	insertSQL, _, _ := goqu.Insert("foo").
		With("other", goqu.From("bar").Where(goqu.C("id").Gt(10))).
		FromQuery(goqu.From("other")).
		ToSQL()
	fmt.Println(insertSQL)

	// Output:
	// WITH other AS (SELECT * FROM "bar" WHERE ("id" > 10)) INSERT INTO "foo" SELECT * FROM "other"
}

func ExampleInsertDataset_WithRecursive() {
	insertSQL, _, _ := goqu.Insert("num_count").
		WithRecursive("nums(x)",
			goqu.From().Select(goqu.L("1")).
				UnionAll(goqu.From("nums").
					Select(goqu.L("x+1")).Where(goqu.C("x").Lt(5))),
		).
		FromQuery(goqu.From("nums")).
		ToSQL()
	fmt.Println(insertSQL)
	// Output:
	// WITH RECURSIVE nums(x) AS (SELECT 1 UNION ALL (SELECT x+1 FROM "nums" WHERE ("x" < 5))) INSERT INTO "num_count" SELECT * FROM "nums"
}

func ExampleInsertDataset_Into() {
	ds := goqu.Insert("test")
	insertSQL, _, _ := ds.Into("test2").Rows(goqu.Record{"first_name": "bob", "last_name": "yukon"}).ToSQL()
	fmt.Println(insertSQL)
	// Output:
	// INSERT INTO "test2" ("first_name", "last_name") VALUES ('bob', 'yukon')
}

func ExampleInsertDataset_Into_aliased() {
	ds := goqu.Insert("test")
	insertSQL, _, _ := ds.
		Into(goqu.T("test").As("t")).
		Rows(goqu.Record{"first_name": "bob", "last_name": "yukon"}).
		ToSQL()
	fmt.Println(insertSQL)
	// Output:
	// INSERT INTO "test" AS "t" ("first_name", "last_name") VALUES ('bob', 'yukon')
}

func ExampleInsertDataset_Cols() {
	insertSQL, _, _ := goqu.Insert("test").
		Cols("a", "b", "c").
		Vals(
			[]interface{}{"a1", "b1", "c1"},
			[]interface{}{"a2", "b1", "c1"},
			[]interface{}{"a3", "b1", "c1"},
		).
		ToSQL()
	fmt.Println(insertSQL)
	// Output:
	// INSERT INTO "test" ("a", "b", "c") VALUES ('a1', 'b1', 'c1'), ('a2', 'b1', 'c1'), ('a3', 'b1', 'c1')
}

func ExampleInsertDataset_Cols_withFromQuery() {
	insertSQL, _, _ := goqu.Insert("test").
		Cols("a", "b", "c").
		FromQuery(goqu.From("foo").Select("d", "e", "f")).
		ToSQL()
	fmt.Println(insertSQL)
	// Output:
	// INSERT INTO "test" ("a", "b", "c") SELECT "d", "e", "f" FROM "foo"
}

func ExampleInsertDataset_ColsAppend() {
	insertSQL, _, _ := goqu.Insert("test").
		Cols("a", "b").
		ColsAppend("c").
		Vals(
			[]interface{}{"a1", "b1", "c1"},
			[]interface{}{"a2", "b1", "c1"},
			[]interface{}{"a3", "b1", "c1"},
		).
		ToSQL()
	fmt.Println(insertSQL)
	// Output:
	// INSERT INTO "test" ("a", "b", "c") VALUES ('a1', 'b1', 'c1'), ('a2', 'b1', 'c1'), ('a3', 'b1', 'c1')
}

func ExampleInsertDataset_ClearCols() {
	ds := goqu.Insert("test").Cols("a", "b", "c")
	insertSQL, _, _ := ds.ClearCols().Cols("other_a", "other_b", "other_c").
		FromQuery(goqu.From("foo").Select("d", "e", "f")).
		ToSQL()
	fmt.Println(insertSQL)
	// Output:
	// INSERT INTO "test" ("other_a", "other_b", "other_c") SELECT "d", "e", "f" FROM "foo"
}

func ExampleInsertDataset_Vals() {
	insertSQL, _, _ := goqu.Insert("test").
		Cols("a", "b", "c").
		Vals(
			[]interface{}{"a1", "b1", "c1"},
			[]interface{}{"a2", "b2", "c2"},
			[]interface{}{"a3", "b3", "c3"},
		).
		ToSQL()
	fmt.Println(insertSQL)

	insertSQL, _, _ = goqu.Insert("test").
		Cols("a", "b", "c").
		Vals([]interface{}{"a1", "b1", "c1"}).
		Vals([]interface{}{"a2", "b2", "c2"}).
		Vals([]interface{}{"a3", "b3", "c3"}).
		ToSQL()
	fmt.Println(insertSQL)

	// Output:
	// INSERT INTO "test" ("a", "b", "c") VALUES ('a1', 'b1', 'c1'), ('a2', 'b2', 'c2'), ('a3', 'b3', 'c3')
	// INSERT INTO "test" ("a", "b", "c") VALUES ('a1', 'b1', 'c1'), ('a2', 'b2', 'c2'), ('a3', 'b3', 'c3')
}

func ExampleInsertDataset_ClearVals() {
	insertSQL, _, _ := goqu.Insert("test").
		Cols("a", "b", "c").
		Vals(
			[]interface{}{"a1", "b1", "c1"},
			[]interface{}{"a2", "b1", "c1"},
			[]interface{}{"a3", "b1", "c1"},
		).
		ClearVals().
		ToSQL()
	fmt.Println(insertSQL)

	insertSQL, _, _ = goqu.Insert("test").
		Cols("a", "b", "c").
		Vals([]interface{}{"a1", "b1", "c1"}).
		Vals([]interface{}{"a2", "b2", "c2"}).
		Vals([]interface{}{"a3", "b3", "c3"}).
		ClearVals().
		ToSQL()
	fmt.Println(insertSQL)
	// Output:
	// INSERT INTO "test" DEFAULT VALUES
	// INSERT INTO "test" DEFAULT VALUES
}
