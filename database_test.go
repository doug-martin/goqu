package gql

import (
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"testing"
)

type testActionItem struct {
	Address string `db:"address"`
	Name    string `db:"name"`
}

type databaseTest struct {
	suite.Suite
}

func (me *databaseTest) TestScanStructs() {
	t := me.T()
	mDb, err := sqlmock.New()
	assert.NoError(t, err)
	sqlmock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	sqlmock.ExpectQuery(`SELECT "test" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"test"}).FromCSVString("test1\ntest2"))

	db := New("db-mock", mDb)
	var items []testActionItem
	assert.NoError(t, db.ScanStructs(&items, `SELECT * FROM "items"`))
	assert.Len(t, items, 2)
	assert.Equal(t, items[0].Address, "111 Test Addr")
	assert.Equal(t, items[0].Name, "Test1")

	assert.Equal(t, items[1].Address, "211 Test Addr")
	assert.Equal(t, items[1].Name, "Test2")

	items = items[0:0]
	assert.EqualError(t, db.ScanStructs(items, `SELECT * FROM "items"`), "gql: Type must be a pointer to a slice when calling ScanStructs")
	assert.EqualError(t, db.ScanStructs(&testActionItem{}, `SELECT * FROM "items"`), "gql: Type must be a pointer to a slice when calling ScanStructs")
	assert.EqualError(t, db.ScanStructs(&items, `SELECT "test" FROM "items"`), `gql: Unable to find corresponding field to column "test" returned by query`)
}

func (me *databaseTest) TestScanStruct() {
	t := me.T()
	mDb, err := sqlmock.New()
	assert.NoError(t, err)
	sqlmock.ExpectQuery(`SELECT \* FROM "items" LIMIT 1`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).FromCSVString("111 Test Addr,Test1"))

	sqlmock.ExpectQuery(`SELECT "test" FROM "items" LIMIT 1`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"test"}).FromCSVString("test1\ntest2"))

	db := New("mock", mDb)
	var item testActionItem
	found, err := db.ScanStruct(&item, `SELECT * FROM "items" LIMIT 1`)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, item.Address, "111 Test Addr")
	assert.Equal(t, item.Name, "Test1")

	_, err = db.ScanStruct(item, `SELECT * FROM "items" LIMIT 1`)
	assert.EqualError(t, err, "gql: Type must be a pointer to a struct when calling ScanStruct")
	_, err = db.ScanStruct([]testActionItem{}, `SELECT * FROM "items" LIMIT 1`)
	assert.EqualError(t, err, "gql: Type must be a pointer to a struct when calling ScanStruct")
	_, err = db.ScanStruct(&item, `SELECT "test" FROM "items" LIMIT 1`)
	assert.EqualError(t, err, `gql: Unable to find corresponding field to column "test" returned by query`)
}

func (me *databaseTest) TestScanVals() {
	t := me.T()
	mDb, err := sqlmock.New()
	assert.NoError(t, err)
	sqlmock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("1\n2\n3\n4\n5"))

	db := New("mock", mDb)
	var ids []uint32
	assert.NoError(t, db.ScanVals(&ids, `SELECT "id" FROM "items"`))
	assert.Len(t, ids, 5)

	assert.EqualError(t, db.ScanVals([]uint32{}, `SELECT "id" FROM "items"`), "gql: Type must be a pointer to a slice when calling ScanVals")
	assert.EqualError(t, db.ScanVals(testActionItem{}, `SELECT "id" FROM "items"`), "gql: Type must be a pointer to a slice when calling ScanVals")
}

func (me *databaseTest) TestScanVal() {
	t := me.T()
	mDb, err := sqlmock.New()
	assert.NoError(t, err)
	sqlmock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("10"))

	db := New("mock", mDb)
	var id int64
	found, err := db.ScanVal(&id, `SELECT "id" FROM "items"`)
	assert.NoError(t, err)
	assert.Equal(t, id, 10)
	assert.True(t, found)

	found, err = db.ScanVal([]int64{}, `SELECT "id" FROM "items"`)
	assert.EqualError(t, err, "gql: Type must be a pointer calling ScanVal")
	found, err = db.ScanVal(10, `SELECT "id" FROM "items"`)
	assert.EqualError(t, err, "gql: Type must be a pointer calling ScanVal")
}

func (me *databaseTest) TestExec() {
	t := me.T()
	mDb, err := sqlmock.New()
	assert.NoError(t, err)
	sqlmock.ExpectExec(`UPDATE "items" SET "address"='111 Test Addr',"name"='Test1' WHERE \("name" IS NULL\)`).
		WithArgs().
		WillReturnResult(sqlmock.NewResult(0, 0))

	sqlmock.ExpectExec(`UPDATE "items" SET "address"='111 Test Addr',"name"='Test1' WHERE \("name" IS NULL\)`).
		WithArgs().
		WillReturnError(NewGqlError("mock error"))

	db := New("mock", mDb)
	_, err = db.Exec(`UPDATE "items" SET "address"='111 Test Addr',"name"='Test1' WHERE ("name" IS NULL)`)
	assert.NoError(t, err)
	_, err = db.Exec(`UPDATE "items" SET "address"='111 Test Addr',"name"='Test1' WHERE ("name" IS NULL)`)
	assert.EqualError(t, err, "gql: mock error")
}

func (me *databaseTest) TestQuery() {
	t := me.T()
	mDb, err := sqlmock.New()
	assert.NoError(t, err)
	sqlmock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	sqlmock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnError(NewGqlError("mock error"))

	db := New("mock", mDb)
	_, err = db.Query(`SELECT * FROM "items"`)
	assert.NoError(t, err, "gql - mock error")

	_, err = db.Query(`SELECT * FROM "items"`)
	assert.EqualError(t, err, "gql: mock error")
}

func (me *databaseTest) TestQueryRow() {
	t := me.T()
	mDb, err := sqlmock.New()
	assert.NoError(t, err)
	sqlmock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	sqlmock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnError(NewGqlError("mock error"))

	db := New("mock", mDb)
	rows := db.QueryRow(`SELECT * FROM "items"`)
	var address string
	var name string
	assert.NoError(t, rows.Scan(&address, &name))

	rows = db.QueryRow(`SELECT * FROM "items"`)
	assert.EqualError(t, rows.Scan(&address, &name), "gql: mock error")
}

func (me *databaseTest) TestBegin() {
	t := me.T()
	mDb, err := sqlmock.New()
	assert.NoError(t, err)
	sqlmock.ExpectBegin()
	db := New("mock", mDb)
	tx, err := db.Begin()
	assert.NoError(t, err)
	assert.Equal(t, tx.Dialect, "mock")
}

func TestDatabaseSuite(t *testing.T) {
	suite.Run(t, new(databaseTest))
}

type txDatabaseTest struct {
	suite.Suite
}

func (me *databaseTest) TestCommit() {
	t := me.T()
	mDb, err := sqlmock.New()
	assert.NoError(t, err)
	sqlmock.ExpectBegin()
	sqlmock.ExpectCommit()
	db := New("mock", mDb)
	tx, err := db.Begin()
	assert.NoError(t, err)
	assert.NoError(t, tx.Commit())
}

func (me *databaseTest) TestRollback() {
	t := me.T()
	mDb, err := sqlmock.New()
	assert.NoError(t, err)
	sqlmock.ExpectBegin()
	sqlmock.ExpectRollback()
	db := New("mock", mDb)
	tx, err := db.Begin()
	assert.NoError(t, err)
	assert.NoError(t, tx.Rollback())
}

func (me *txDatabaseTest) TestScanStructs() {
	t := me.T()
	mDb, err := sqlmock.New()
	assert.NoError(t, err)
	sqlmock.ExpectBegin()
	sqlmock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	sqlmock.ExpectQuery(`SELECT "test" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"test"}).FromCSVString("test1\ntest2"))
	sqlmock.ExpectCommit()
	tx, err := New("db-mock", mDb).Begin()
	assert.NoError(t, err)
	var items []testActionItem
	assert.NoError(t, tx.ScanStructs(&items, `SELECT * FROM "items"`))
	assert.Len(t, items, 2)
	assert.Equal(t, items[0].Address, "111 Test Addr")
	assert.Equal(t, items[0].Name, "Test1")

	assert.Equal(t, items[1].Address, "211 Test Addr")
	assert.Equal(t, items[1].Name, "Test2")

	items = items[0:0]
	assert.EqualError(t, tx.ScanStructs(items, `SELECT * FROM "items"`), "gql: Type must be a pointer to a slice when calling ScanStructs")
	assert.EqualError(t, tx.ScanStructs(&testActionItem{}, `SELECT * FROM "items"`), "gql: Type must be a pointer to a slice when calling ScanStructs")
	assert.EqualError(t, tx.ScanStructs(&items, `SELECT "test" FROM "items"`), `gql: Unable to find corresponding field to column "test" returned by query`)
	assert.NoError(t, tx.Commit())
}

func (me *txDatabaseTest) TestScanStruct() {
	t := me.T()
	mDb, err := sqlmock.New()
	assert.NoError(t, err)
	sqlmock.ExpectBegin()
	sqlmock.ExpectQuery(`SELECT \* FROM "items" LIMIT 1`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).FromCSVString("111 Test Addr,Test1"))

	sqlmock.ExpectQuery(`SELECT "test" FROM "items" LIMIT 1`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"test"}).FromCSVString("test1\ntest2"))
	sqlmock.ExpectCommit()
	tx, err := New("mock", mDb).Begin()
	assert.NoError(t, err)
	var item testActionItem
	found, err := tx.ScanStruct(&item, `SELECT * FROM "items" LIMIT 1`)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, item.Address, "111 Test Addr")
	assert.Equal(t, item.Name, "Test1")

	_, err = tx.ScanStruct(item, `SELECT * FROM "items" LIMIT 1`)
	assert.EqualError(t, err, "gql: Type must be a pointer to a struct when calling ScanStruct")
	_, err = tx.ScanStruct([]testActionItem{}, `SELECT * FROM "items" LIMIT 1`)
	assert.EqualError(t, err, "gql: Type must be a pointer to a struct when calling ScanStruct")
	_, err = tx.ScanStruct(&item, `SELECT "test" FROM "items" LIMIT 1`)
	assert.EqualError(t, err, `gql: Unable to find corresponding field to column "test" returned by query`)
	assert.NoError(t, tx.Commit())
}

func (me *txDatabaseTest) TestScanVals() {
	t := me.T()
	mDb, err := sqlmock.New()
	assert.NoError(t, err)
	sqlmock.ExpectBegin()
	sqlmock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("1\n2\n3\n4\n5"))
	sqlmock.ExpectCommit()
	tx, err := New("mock", mDb).Begin()
	assert.NoError(t, err)
	var ids []uint32
	assert.NoError(t, tx.ScanVals(&ids, `SELECT "id" FROM "items"`))
	assert.Len(t, ids, 5)

	assert.EqualError(t, tx.ScanVals([]uint32{}, `SELECT "id" FROM "items"`), "gql: Type must be a pointer to a slice when calling ScanVals")
	assert.EqualError(t, tx.ScanVals(testActionItem{}, `SELECT "id" FROM "items"`), "gql: Type must be a pointer to a slice when calling ScanVals")
	assert.NoError(t, tx.Commit())
}

func (me *txDatabaseTest) TestScanVal() {
	t := me.T()
	mDb, err := sqlmock.New()
	assert.NoError(t, err)
	sqlmock.ExpectBegin()
	sqlmock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("10"))
	sqlmock.ExpectCommit()
	tx, err := New("mock", mDb).Begin()
	assert.NoError(t, err)
	var id int64
	found, err := tx.ScanVal(&id, `SELECT "id" FROM "items"`)
	assert.NoError(t, err)
	assert.Equal(t, id, 10)
	assert.True(t, found)

	found, err = tx.ScanVal([]int64{}, `SELECT "id" FROM "items"`)
	assert.EqualError(t, err, "gql: Type must be a pointer calling ScanVal")
	found, err = tx.ScanVal(10, `SELECT "id" FROM "items"`)
	assert.EqualError(t, err, "gql: Type must be a pointer calling ScanVal")
	assert.NoError(t, tx.Commit())
}

func (me *txDatabaseTest) TestExec() {
	t := me.T()
	mDb, err := sqlmock.New()
	assert.NoError(t, err)
	sqlmock.ExpectBegin()
	sqlmock.ExpectExec(`UPDATE "items" SET "address"='111 Test Addr',"name"='Test1' WHERE \("name" IS NULL\)`).
		WithArgs().
		WillReturnResult(sqlmock.NewResult(0, 0))

	sqlmock.ExpectExec(`UPDATE "items" SET "address"='111 Test Addr',"name"='Test1' WHERE \("name" IS NULL\)`).
		WithArgs().
		WillReturnError(NewGqlError("mock error"))
	sqlmock.ExpectCommit()
	tx, err := New("mock", mDb).Begin()
	assert.NoError(t, err)
	_, err = tx.Exec(`UPDATE "items" SET "address"='111 Test Addr',"name"='Test1' WHERE ("name" IS NULL)`)
	assert.NoError(t, err)
	_, err = tx.Exec(`UPDATE "items" SET "address"='111 Test Addr',"name"='Test1' WHERE ("name" IS NULL)`)
	assert.EqualError(t, err, "gql: mock error")
	assert.NoError(t, tx.Commit())
}

func (me *txDatabaseTest) TestQuery() {
	t := me.T()
	mDb, err := sqlmock.New()
	assert.NoError(t, err)
	sqlmock.ExpectBegin()
	sqlmock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	sqlmock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnError(NewGqlError("mock error"))
	sqlmock.ExpectCommit()
	tx, err := New("mock", mDb).Begin()
	assert.NoError(t, err)
	_, err = tx.Query(`SELECT * FROM "items"`)
	assert.NoError(t, err, "gql - mock error")

	_, err = tx.Query(`SELECT * FROM "items"`)
	assert.EqualError(t, err, "gql: mock error")
	assert.NoError(t, tx.Commit())
}

func (me *txDatabaseTest) TestQueryRow() {
	t := me.T()
	mDb, err := sqlmock.New()
	assert.NoError(t, err)
	sqlmock.ExpectBegin()
	sqlmock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	sqlmock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnError(NewGqlError("mock error"))
	sqlmock.ExpectCommit()
	tx, err := New("mock", mDb).Begin()
	assert.NoError(t, err)
	rows := tx.QueryRow(`SELECT * FROM "items"`)
	var address string
	var name string
	assert.NoError(t, rows.Scan(&address, &name))

	rows = tx.QueryRow(`SELECT * FROM "items"`)
	assert.EqualError(t, rows.Scan(&address, &name), "gql: mock error")
	assert.NoError(t, tx.Commit())
}

func TestTxDatabaseSuite(t *testing.T) {
	suite.Run(t, new(txDatabaseTest))
}
