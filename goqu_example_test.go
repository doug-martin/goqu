package goqu_test

import (
	"fmt"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/mysql"
	_ "github.com/doug-martin/goqu/v9/dialect/postgres"
	_ "github.com/doug-martin/goqu/v9/dialect/sqlite3"
)

// Creating a mysql dataset. Be sure to import the mysql adapter
func ExampleDialect_datasetMysql() {
	// import _ "github.com/doug-martin/goqu/v9/adapters/mysql"

	d := goqu.Dialect("mysql")
	ds := d.From("test").Where(goqu.Ex{
		"foo": "bar",
		"baz": []int64{1, 2, 3},
	}).Limit(10)

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM `test` WHERE ((`baz` IN (1, 2, 3)) AND (`foo` = 'bar')) LIMIT 10 []
	// SELECT * FROM `test` WHERE ((`baz` IN (?, ?, ?)) AND (`foo` = ?)) LIMIT ? [1 2 3 bar 10]
}

// Creating a mysql database. Be sure to import the mysql adapter
func ExampleDialect_dbMysql() {
	// import _ "github.com/doug-martin/goqu/v9/adapters/mysql"

	type item struct {
		ID      int64  `db:"id"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	// set up a mock db this would normally be
	// db, err := sql.Open("mysql", dbURI)
	// 	if err != nil {
	// 		panic(err.Error())
	// 	}
	mDb, mock, _ := sqlmock.New()

	d := goqu.Dialect("mysql")

	db := d.DB(mDb)

	// use the db.From to get a dataset to execute queries
	ds := db.From("items").Where(goqu.C("id").Eq(1))

	// set up mock for example purposes
	mock.ExpectQuery("SELECT `address`, `id`, `name` FROM `items` WHERE \\(`id` = 1\\) LIMIT 1").
		WillReturnRows(
			sqlmock.NewRows([]string{"id", "address", "name"}).
				FromCSVString("1, 111 Test Addr,Test1"),
		)
	var it item
	found, err := ds.ScanStruct(&it)
	fmt.Println(it, found, err)

	// set up mock for example purposes
	mock.ExpectQuery("SELECT `address`, `id`, `name` FROM `items` WHERE \\(`id` = \\?\\) LIMIT \\?").
		WithArgs(1, 1).
		WillReturnRows(
			sqlmock.NewRows([]string{"id", "address", "name"}).
				FromCSVString("1, 111 Test Addr,Test1"),
		)

	found, err = ds.Prepared(true).ScanStruct(&it)
	fmt.Println(it, found, err)

	// Output:
	// {1 111 Test Addr Test1} true <nil>
	// {1 111 Test Addr Test1} true <nil>
}

// Creating a mysql dataset. Be sure to import the postgres adapter
func ExampleDialect_datasetPostgres() {
	// import _ "github.com/doug-martin/goqu/v9/adapters/postgres"

	d := goqu.Dialect("postgres")
	ds := d.From("test").Where(goqu.Ex{
		"foo": "bar",
		"baz": []int64{1, 2, 3},
	}).Limit(10)

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM "test" WHERE (("baz" IN (1, 2, 3)) AND ("foo" = 'bar')) LIMIT 10 []
	// SELECT * FROM "test" WHERE (("baz" IN ($1, $2, $3)) AND ("foo" = $4)) LIMIT $5 [1 2 3 bar 10]
}

// Creating a postgres dataset. Be sure to import the postgres adapter
func ExampleDialect_dbPostgres() {
	// import _ "github.com/doug-martin/goqu/v9/adapters/postgres"

	type item struct {
		ID      int64  `db:"id"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	// set up a mock db this would normally be
	// db, err := sql.Open("postgres", dbURI)
	// 	if err != nil {
	// 		panic(err.Error())
	// 	}
	mDb, mock, _ := sqlmock.New()

	d := goqu.Dialect("postgres")

	db := d.DB(mDb)

	// use the db.From to get a dataset to execute queries
	ds := db.From("items").Where(goqu.C("id").Eq(1))

	// set up mock for example purposes
	mock.ExpectQuery(`SELECT "address", "id", "name" FROM "items" WHERE \("id" = 1\) LIMIT 1`).
		WillReturnRows(
			sqlmock.NewRows([]string{"id", "address", "name"}).
				FromCSVString("1, 111 Test Addr,Test1"),
		)
	var it item
	found, err := ds.ScanStruct(&it)
	fmt.Println(it, found, err)

	// set up mock for example purposes
	mock.ExpectQuery(`SELECT "address", "id", "name" FROM "items" WHERE \("id" = \$1\) LIMIT \$2`).
		WithArgs(1, 1).
		WillReturnRows(
			sqlmock.NewRows([]string{"id", "address", "name"}).
				FromCSVString("1, 111 Test Addr,Test1"),
		)

	found, err = ds.Prepared(true).ScanStruct(&it)
	fmt.Println(it, found, err)

	// Output:
	// {1 111 Test Addr Test1} true <nil>
	// {1 111 Test Addr Test1} true <nil>
}

// Creating a mysql dataset. Be sure to import the sqlite3 adapter
func ExampleDialect_datasetSqlite3() {
	// import _ "github.com/doug-martin/goqu/v9/adapters/sqlite3"

	d := goqu.Dialect("sqlite3")
	ds := d.From("test").Where(goqu.Ex{
		"foo": "bar",
		"baz": []int64{1, 2, 3},
	}).Limit(10)

	sql, args, _ := ds.ToSQL()
	fmt.Println(sql, args)

	sql, args, _ = ds.Prepared(true).ToSQL()
	fmt.Println(sql, args)

	// Output:
	// SELECT * FROM `test` WHERE ((`baz` IN (1, 2, 3)) AND (`foo` = 'bar')) LIMIT 10 []
	// SELECT * FROM `test` WHERE ((`baz` IN (?, ?, ?)) AND (`foo` = ?)) LIMIT ? [1 2 3 bar 10]
}

// Creating a sqlite3 database. Be sure to import the sqlite3 adapter
func ExampleDialect_dbSqlite3() {
	// import _ "github.com/doug-martin/goqu/v9/adapters/sqlite3"

	type item struct {
		ID      int64  `db:"id"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	// set up a mock db this would normally be
	// db, err := sql.Open("sqlite3", dbURI)
	// 	if err != nil {
	// 		panic(err.Error())
	// 	}
	mDb, mock, _ := sqlmock.New()

	d := goqu.Dialect("sqlite3")

	db := d.DB(mDb)

	// use the db.From to get a dataset to execute queries
	ds := db.From("items").Where(goqu.C("id").Eq(1))

	// set up mock for example purposes
	mock.ExpectQuery("SELECT `address`, `id`, `name` FROM `items` WHERE \\(`id` = 1\\) LIMIT 1").
		WillReturnRows(
			sqlmock.NewRows([]string{"id", "address", "name"}).
				FromCSVString("1, 111 Test Addr,Test1"),
		)
	var it item
	found, err := ds.ScanStruct(&it)
	fmt.Println(it, found, err)

	// set up mock for example purposes
	mock.ExpectQuery("SELECT `address`, `id`, `name` FROM `items` WHERE \\(`id` = \\?\\) LIMIT \\?").
		WithArgs(1, 1).
		WillReturnRows(
			sqlmock.NewRows([]string{"id", "address", "name"}).
				FromCSVString("1, 111 Test Addr,Test1"),
		)

	found, err = ds.Prepared(true).ScanStruct(&it)
	fmt.Println(it, found, err)

	// Output:
	// {1 111 Test Addr Test1} true <nil>
	// {1 111 Test Addr Test1} true <nil>
}
