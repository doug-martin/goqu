package goqu

import (
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

type dsTestActionItem struct {
	Address string `db:"address"`
	Name    string `db:"name"`
}

func (me *datasetTest) TestScanStructs() {
	t := me.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectQuery(`SELECT "address", "name" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	mock.ExpectQuery(`SELECT DISTINCT "name" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	mock.ExpectQuery(`SELECT "test" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"test"}).FromCSVString("test1\ntest2"))

	db := New("mock", mDb)
	var items []dsTestActionItem
	assert.NoError(t, db.From("items").ScanStructs(&items))
	assert.Len(t, items, 2)
	assert.Equal(t, items[0].Address, "111 Test Addr")
	assert.Equal(t, items[0].Name, "Test1")

	assert.Equal(t, items[1].Address, "211 Test Addr")
	assert.Equal(t, items[1].Name, "Test2")

	items = items[0:0]
	assert.NoError(t, db.From("items").SelectDistinct("name").ScanStructs(&items))
	assert.Len(t, items, 2)
	assert.Equal(t, items[0].Address, "111 Test Addr")
	assert.Equal(t, items[0].Name, "Test1")

	assert.Equal(t, items[1].Address, "211 Test Addr")
	assert.Equal(t, items[1].Name, "Test2")

	items = items[0:0]
	assert.EqualError(t, db.From("items").ScanStructs(items), "goqu: Type must be a pointer to a slice when calling ScanStructs")
	assert.EqualError(t, db.From("items").ScanStructs(&dsTestActionItem{}), "goqu: Type must be a pointer to a slice when calling ScanStructs")
	assert.EqualError(t, db.From("items").Select("test").ScanStructs(&items), `goqu: Unable to find corresponding field to column "test" returned by query`)
}

func (me *datasetTest) TestScanStructs_WithPreparedStatements() {
	t := me.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectQuery(`SELECT "address", "name" FROM "items" WHERE \(\("address" = \?\) AND \("name" IN \(\?, \?, \?\)\)\)`).
		WithArgs("111 Test Addr", "Bob", "Sally", "Billy").
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	mock.ExpectQuery(`SELECT "test" FROM "items" WHERE \(\("address" = \?\) AND \("name" IN \(\?, \?, \?\)\)\)`).
		WithArgs("111 Test Addr", "Bob", "Sally", "Billy").
		WillReturnRows(sqlmock.NewRows([]string{"test"}).FromCSVString("test1\ntest2"))

	db := New("mock", mDb)
	var items []dsTestActionItem
	assert.NoError(t, db.From("items").Prepared(true).Where(Ex{"name": []string{"Bob", "Sally", "Billy"}, "address": "111 Test Addr"}).ScanStructs(&items))
	assert.Len(t, items, 2)
	assert.Equal(t, items[0].Address, "111 Test Addr")
	assert.Equal(t, items[0].Name, "Test1")

	assert.Equal(t, items[1].Address, "211 Test Addr")
	assert.Equal(t, items[1].Name, "Test2")

	items = items[0:0]
	assert.EqualError(t, db.From("items").ScanStructs(items), "goqu: Type must be a pointer to a slice when calling ScanStructs")
	assert.EqualError(t, db.From("items").ScanStructs(&dsTestActionItem{}), "goqu: Type must be a pointer to a slice when calling ScanStructs")
	assert.EqualError(t, db.From("items").
		Prepared(true).
		Select("test").
		Where(Ex{"name": []string{"Bob", "Sally", "Billy"}, "address": "111 Test Addr"}).
		ScanStructs(&items), `goqu: Unable to find corresponding field to column "test" returned by query`)
}

func (me *datasetTest) TestScanStruct() {
	t := me.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectQuery(`SELECT "address", "name" FROM "items" LIMIT 1`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).FromCSVString("111 Test Addr,Test1"))

	mock.ExpectQuery(`SELECT DISTINCT "name" FROM "items" LIMIT 1`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).FromCSVString("111 Test Addr,Test1"))

	mock.ExpectQuery(`SELECT "test" FROM "items" LIMIT 1`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"test"}).FromCSVString("test1\ntest2"))

	db := New("mock", mDb)
	var item dsTestActionItem
	found, err := db.From("items").ScanStruct(&item)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, item.Address, "111 Test Addr")
	assert.Equal(t, item.Name, "Test1")

	item = dsTestActionItem{}
	found, err = db.From("items").SelectDistinct("name").ScanStruct(&item)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, item.Address, "111 Test Addr")
	assert.Equal(t, item.Name, "Test1")

	_, err = db.From("items").ScanStruct(item)
	assert.EqualError(t, err, "goqu: Type must be a pointer to a struct when calling ScanStruct")
	_, err = db.From("items").ScanStruct([]dsTestActionItem{})
	assert.EqualError(t, err, "goqu: Type must be a pointer to a struct when calling ScanStruct")
	_, err = db.From("items").Select("test").ScanStruct(&item)
	assert.EqualError(t, err, `goqu: Unable to find corresponding field to column "test" returned by query`)
}

func (me *datasetTest) TestScanStruct_WithPreparedStatements() {
	t := me.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectQuery(`SELECT "address", "name" FROM "items" WHERE \(\("address" = \?\) AND \("name" IN \(\?, \?, \?\)\)\) LIMIT \?`).
		WithArgs("111 Test Addr", "Bob", "Sally", "Billy", 1).
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).FromCSVString("111 Test Addr,Test1"))

	mock.ExpectQuery(`SELECT "test" FROM "items" WHERE \(\("address" = \?\) AND \("name" IN \(\?, \?, \?\)\)\) LIMIT \?`).
		WithArgs("111 Test Addr", "Bob", "Sally", "Billy", 1).
		WillReturnRows(sqlmock.NewRows([]string{"test"}).FromCSVString("test1\ntest2"))

	db := New("mock", mDb)
	var item dsTestActionItem
	found, err := db.From("items").Prepared(true).Where(Ex{"name": []string{"Bob", "Sally", "Billy"}, "address": "111 Test Addr"}).ScanStruct(&item)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, item.Address, "111 Test Addr")
	assert.Equal(t, item.Name, "Test1")

	_, err = db.From("items").ScanStruct(item)
	assert.EqualError(t, err, "goqu: Type must be a pointer to a struct when calling ScanStruct")
	_, err = db.From("items").ScanStruct([]dsTestActionItem{})
	assert.EqualError(t, err, "goqu: Type must be a pointer to a struct when calling ScanStruct")
	_, err = db.From("items").
		Prepared(true).
		Select("test").
		Where(Ex{"name": []string{"Bob", "Sally", "Billy"}, "address": "111 Test Addr"}).
		ScanStruct(&item)
	assert.EqualError(t, err, `goqu: Unable to find corresponding field to column "test" returned by query`)
}

func (me *datasetTest) TestScanVals() {
	t := me.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("1\n2\n3\n4\n5"))

	db := New("mock", mDb)
	var ids []uint32
	assert.NoError(t, db.From("items").Select("id").ScanVals(&ids))
	assert.Len(t, ids, 5)

	assert.EqualError(t, db.From("items").ScanVals([]uint32{}), "goqu: Type must be a pointer to a slice when calling ScanVals")
	assert.EqualError(t, db.From("items").ScanVals(dsTestActionItem{}), "goqu: Type must be a pointer to a slice when calling ScanVals")
}

func (me *datasetTest) TestScanVals_WithPreparedStatment() {
	t := me.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectQuery(`SELECT "id" FROM "items" WHERE \(\("address" = \?\) AND \("name" IN \(\?, \?, \?\)\)\)`).
		WithArgs("111 Test Addr", "Bob", "Sally", "Billy").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("1\n2\n3\n4\n5"))

	db := New("mock", mDb)
	var ids []uint32
	assert.NoError(t, db.From("items").
		Prepared(true).
		Select("id").
		Where(Ex{"name": []string{"Bob", "Sally", "Billy"}, "address": "111 Test Addr"}).
		ScanVals(&ids))
	assert.Len(t, ids, 5)

	assert.EqualError(t, db.From("items").ScanVals([]uint32{}), "goqu: Type must be a pointer to a slice when calling ScanVals")
	assert.EqualError(t, db.From("items").ScanVals(dsTestActionItem{}), "goqu: Type must be a pointer to a slice when calling ScanVals")
}

func (me *datasetTest) TestScanVal() {
	t := me.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectQuery(`SELECT "id" FROM "items" LIMIT 1`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("10"))

	db := New("mock", mDb)
	var id int64
	found, err := db.From("items").Select("id").ScanVal(&id)
	assert.NoError(t, err)
	assert.Equal(t, id, int64(10))
	assert.True(t, found)

	found, err = db.From("items").ScanVal([]int64{})
	assert.EqualError(t, err, "goqu: Type must be a pointer when calling ScanVal")
	found, err = db.From("items").ScanVal(10)
	assert.EqualError(t, err, "goqu: Type must be a pointer when calling ScanVal")
}

func (me *datasetTest) TestScanVal_WithPreparedStatement() {
	t := me.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectQuery(`SELECT "id" FROM "items" WHERE \(\("address" = \?\) AND \("name" IN \(\?, \?, \?\)\)\) LIMIT ?`).
		WithArgs("111 Test Addr", "Bob", "Sally", "Billy", 1).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("10"))

	db := New("mock", mDb)
	var id int64
	found, err := db.From("items").
		Prepared(true).
		Select("id").
		Where(Ex{"name": []string{"Bob", "Sally", "Billy"}, "address": "111 Test Addr"}).
		ScanVal(&id)
	assert.NoError(t, err)
	assert.Equal(t, id, int64(10))
	assert.True(t, found)

	found, err = db.From("items").ScanVal([]int64{})
	assert.EqualError(t, err, "goqu: Type must be a pointer when calling ScanVal")
	found, err = db.From("items").ScanVal(10)
	assert.EqualError(t, err, "goqu: Type must be a pointer when calling ScanVal")
}

func (me *datasetTest) TestCount() {
	t := me.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectQuery(`SELECT COUNT\(\*\) AS "count" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"count"}).FromCSVString("10"))

	db := New("mock", mDb)
	count, err := db.From("items").Count()
	assert.NoError(t, err)
	assert.Equal(t, count, int64(10))
}

func (me *datasetTest) TestCount_WithPreparedStatement() {
	t := me.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectQuery(`SELECT COUNT\(\*\) AS "count" FROM "items" WHERE \(\("address" = \?\) AND \("name" IN \(\?, \?, \?\)\)\)`).
		WithArgs("111 Test Addr", "Bob", "Sally", "Billy", 1).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).FromCSVString("10"))

	db := New("mock", mDb)
	count, err := db.From("items").
		Prepared(true).
		Where(Ex{"name": []string{"Bob", "Sally", "Billy"}, "address": "111 Test Addr"}).
		Count()
	assert.NoError(t, err)
	assert.Equal(t, count, int64(10))
}

func (me *datasetTest) TestPluck() {
	t := me.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectQuery(`SELECT "name" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"name"}).FromCSVString("test1\ntest2\ntest3\ntest4\ntest5"))

	db := New("mock", mDb)
	var names []string
	assert.NoError(t, db.From("items").Pluck(&names, "name"))
	assert.Equal(t, names, []string{"test1", "test2", "test3", "test4", "test5"})
}

func (me *datasetTest) TestPluck_WithPreparedStatement() {
	t := me.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectQuery(`SELECT "name" FROM "items" WHERE \(\("address" = \?\) AND \("name" IN \(\?, \?, \?\)\)\)`).
		WithArgs("111 Test Addr", "Bob", "Sally", "Billy").
		WillReturnRows(sqlmock.NewRows([]string{"name"}).FromCSVString("Bob\nSally\nBilly"))

	db := New("mock", mDb)
	var names []string
	assert.NoError(t, db.From("items").
		Prepared(true).
		Where(Ex{"name": []string{"Bob", "Sally", "Billy"}, "address": "111 Test Addr"}).
		Pluck(&names, "name"))
	assert.Equal(t, names, []string{"Bob", "Sally", "Billy"})
}

func (me *datasetTest) TestUpdate() {
	t := me.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectExec(`UPDATE "items" SET "address"='111 Test Addr',"name"='Test1' WHERE \("name" IS NULL\)`).
		WithArgs().
		WillReturnResult(sqlmock.NewResult(0, 0))

	db := New("mock", mDb)
	_, err = db.From("items").Where(I("name").IsNull()).Update(Record{"address": "111 Test Addr", "name": "Test1"}).Exec()
	assert.NoError(t, err)
}

func (me *datasetTest) TestUpdate_WithPreparedStatement() {
	t := me.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectExec(`UPDATE "items" SET "address"=\?,"name"=\? WHERE \(\("address" = \?\) AND \("name" IN \(\?, \?, \?\)\)\)`).
		WithArgs("112 Test Addr", "Test1", "111 Test Addr", "Bob", "Sally", "Billy").
		WillReturnResult(sqlmock.NewResult(0, 0))

	db := New("mock", mDb)
	_, err = db.From("items").
		Prepared(true).
		Where(Ex{"name": []string{"Bob", "Sally", "Billy"}, "address": "111 Test Addr"}).
		Update(Record{"address": "112 Test Addr", "name": "Test1"}).
		Exec()
	assert.NoError(t, err)
}

func (me *datasetTest) TestInsert() {
	t := me.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectExec(`INSERT INTO "items" \("address", "name"\) VALUES \('111 Test Addr', 'Test1'\)`).
		WithArgs().
		WillReturnResult(sqlmock.NewResult(0, 0))

	db := New("mock", mDb)
	_, err = db.From("items").Insert(Record{"address": "111 Test Addr", "name": "Test1"}).Exec()
	assert.NoError(t, err)
}

func (me *datasetTest) TestInsert_WithPreparedStatment() {
	t := me.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectExec(`INSERT INTO "items" \("address", "name"\) VALUES \(\?, \?\), \(\?, \?\)`).
		WithArgs("111 Test Addr", "Test1", "112 Test Addr", "Test2").
		WillReturnResult(sqlmock.NewResult(0, 0))

	db := New("mock", mDb)
	_, err = db.From("items").
		Prepared(true).
		Insert(
			Record{"address": "111 Test Addr", "name": "Test1"},
			Record{"address": "112 Test Addr", "name": "Test2"},
		).
		Exec()
	assert.NoError(t, err)
}

func (me *datasetTest) TestDelete() {
	t := me.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectExec(`DELETE FROM "items" WHERE \("id" > 10\)`).
		WithArgs().
		WillReturnResult(sqlmock.NewResult(0, 0))

	db := New("mock", mDb)
	_, err = db.From("items").Where(I("id").Gt(10)).Delete().Exec()
	assert.NoError(t, err)
}

func (me *datasetTest) TestDelete_WithPreparedStatment() {
	t := me.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectExec(`DELETE FROM "items" WHERE \("id" > \?\)`).
		WithArgs(10).
		WillReturnResult(sqlmock.NewResult(0, 0))

	db := New("mock", mDb)
	_, err = db.From("items").Prepared(true).Where(Ex{"id": Op{"gt": 10}}).Delete().Exec()
	assert.NoError(t, err)
}
