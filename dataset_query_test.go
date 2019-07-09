package goqu

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/doug-martin/goqu/v7/exec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type dsTestActionItem struct {
	Address string `db:"address"`
	Name    string `db:"name"`
}

type datasetQuerySuite struct {
	suite.Suite
}

func (dqs *datasetQuerySuite) queryFactory(db exec.DbExecutor) exec.QueryFactory {
	return exec.NewQueryFactory(db)
}

func (dqs *datasetQuerySuite) TestScanStructs() {
	t := dqs.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectQuery(`SELECT "address", "name" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	mock.ExpectQuery(`SELECT DISTINCT "name" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	mock.ExpectQuery(`SELECT "test" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"test"}).FromCSVString("test1\ntest2"))

	qf := dqs.queryFactory(mDb)
	ds := newDataset("mock", qf)
	var items []dsTestActionItem
	assert.NoError(t, ds.From("items").ScanStructs(&items))
	assert.Equal(t, items, []dsTestActionItem{
		{Address: "111 Test Addr", Name: "Test1"},
		{Address: "211 Test Addr", Name: "Test2"},
	})

	items = items[0:0]
	assert.NoError(t, ds.From("items").SelectDistinct("name").ScanStructs(&items))
	assert.Equal(t, items, []dsTestActionItem{
		{Address: "111 Test Addr", Name: "Test1"},
		{Address: "211 Test Addr", Name: "Test2"},
	})

	items = items[0:0]
	assert.EqualError(t, ds.From("items").ScanStructs(items),
		"goqu: type must be a pointer to a slice when scanning into structs")
	assert.EqualError(t, ds.From("items").ScanStructs(&dsTestActionItem{}),
		"goqu: type must be a pointer to a slice when scanning into structs")
	assert.EqualError(t, ds.From("items").Select("test").ScanStructs(&items),
		`goqu: unable to find corresponding field to column "test" returned by query`)
}

func (dqs *datasetQuerySuite) TestScanStructs_WithPreparedStatements() {
	t := dqs.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectQuery(
		`SELECT "address", "name" FROM "items" WHERE \(\("address" = \?\) AND \("name" IN \(\?, \?, \?\)\)\)`,
	).
		WithArgs("111 Test Addr", "Bob", "Sally", "Billy").
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	mock.ExpectQuery(
		`SELECT "test" FROM "items" WHERE \(\("address" = \?\) AND \("name" IN \(\?, \?, \?\)\)\)`,
	).
		WithArgs("111 Test Addr", "Bob", "Sally", "Billy").
		WillReturnRows(sqlmock.NewRows([]string{"test"}).FromCSVString("test1\ntest2"))

	qf := dqs.queryFactory(mDb)
	ds := newDataset("mock", qf)
	var items []dsTestActionItem
	assert.NoError(t, ds.From("items").Prepared(true).Where(Ex{
		"name":    []string{"Bob", "Sally", "Billy"},
		"address": "111 Test Addr",
	}).ScanStructs(&items))
	assert.Equal(t, items, []dsTestActionItem{
		{Address: "111 Test Addr", Name: "Test1"},
		{Address: "211 Test Addr", Name: "Test2"},
	})

	items = items[0:0]
	assert.EqualError(t, ds.From("items").ScanStructs(items),
		"goqu: type must be a pointer to a slice when scanning into structs")
	assert.EqualError(t, ds.From("items").ScanStructs(&dsTestActionItem{}),
		"goqu: type must be a pointer to a slice when scanning into structs")
	assert.EqualError(t, ds.From("items").
		Prepared(true).
		Select("test").
		Where(Ex{"name": []string{"Bob", "Sally", "Billy"}, "address": "111 Test Addr"}).
		ScanStructs(&items), `goqu: unable to find corresponding field to column "test" returned by query`)
}

func (dqs *datasetQuerySuite) TestScanStruct() {
	t := dqs.T()
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

	qf := dqs.queryFactory(mDb)
	ds := newDataset("mock", qf)
	var item dsTestActionItem
	found, err := ds.From("items").ScanStruct(&item)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, item.Address, "111 Test Addr")
	assert.Equal(t, item.Name, "Test1")

	item = dsTestActionItem{}
	found, err = ds.From("items").SelectDistinct("name").ScanStruct(&item)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, item.Address, "111 Test Addr")
	assert.Equal(t, item.Name, "Test1")

	_, err = ds.From("items").ScanStruct(item)
	assert.EqualError(t, err, "goqu: type must be a pointer to a struct when scanning into a struct")
	_, err = ds.From("items").ScanStruct([]dsTestActionItem{})
	assert.EqualError(t, err, "goqu: type must be a pointer to a struct when scanning into a struct")
	_, err = ds.From("items").Select("test").ScanStruct(&item)
	assert.EqualError(t, err, `goqu: unable to find corresponding field to column "test" returned by query`)
}

func (dqs *datasetQuerySuite) TestScanStruct_WithPreparedStatements() {
	t := dqs.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectQuery(
		`SELECT "address", "name" FROM "items" WHERE \(\("address" = \?\) AND \("name" IN \(\?, \?, \?\)\)\) LIMIT \?`,
	).
		WithArgs("111 Test Addr", "Bob", "Sally", "Billy", 1).
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).FromCSVString("111 Test Addr,Test1"))

	mock.ExpectQuery(`SELECT "test" FROM "items" WHERE \(\("address" = \?\) AND \("name" IN \(\?, \?, \?\)\)\) LIMIT \?`).
		WithArgs("111 Test Addr", "Bob", "Sally", "Billy", 1).
		WillReturnRows(sqlmock.NewRows([]string{"test"}).FromCSVString("test1\ntest2"))

	qf := dqs.queryFactory(mDb)
	ds := newDataset("mock", qf)
	var item dsTestActionItem
	found, err := ds.From("items").Prepared(true).Where(Ex{
		"name":    []string{"Bob", "Sally", "Billy"},
		"address": "111 Test Addr",
	}).ScanStruct(&item)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, item.Address, "111 Test Addr")
	assert.Equal(t, item.Name, "Test1")

	_, err = ds.From("items").ScanStruct(item)
	assert.EqualError(t, err, "goqu: type must be a pointer to a struct when scanning into a struct")
	_, err = ds.From("items").ScanStruct([]dsTestActionItem{})
	assert.EqualError(t, err, "goqu: type must be a pointer to a struct when scanning into a struct")
	_, err = ds.From("items").
		Prepared(true).
		Select("test").
		Where(Ex{"name": []string{"Bob", "Sally", "Billy"}, "address": "111 Test Addr"}).
		ScanStruct(&item)
	assert.EqualError(t, err, `goqu: unable to find corresponding field to column "test" returned by query`)
}

func (dqs *datasetQuerySuite) TestScanVals() {
	t := dqs.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("1\n2\n3\n4\n5"))

	qf := dqs.queryFactory(mDb)
	ds := newDataset("mock", qf)
	var ids []uint32
	assert.NoError(t, ds.From("items").Select("id").ScanVals(&ids))
	assert.Equal(t, ids, []uint32{1, 2, 3, 4, 5})

	assert.EqualError(t, ds.From("items").ScanVals([]uint32{}),
		"goqu: type must be a pointer to a slice when scanning into vals")
	assert.EqualError(t, ds.From("items").ScanVals(dsTestActionItem{}),
		"goqu: type must be a pointer to a slice when scanning into vals")
}

func (dqs *datasetQuerySuite) TestScanVals_WithPreparedStatment() {
	t := dqs.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectQuery(
		`SELECT "id" FROM "items" WHERE \(\("address" = \?\) AND \("name" IN \(\?, \?, \?\)\)\)`,
	).
		WithArgs("111 Test Addr", "Bob", "Sally", "Billy").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("1\n2\n3\n4\n5"))

	qf := dqs.queryFactory(mDb)
	ds := newDataset("mock", qf)
	var ids []uint32
	assert.NoError(t, ds.From("items").
		Prepared(true).
		Select("id").
		Where(Ex{"name": []string{"Bob", "Sally", "Billy"}, "address": "111 Test Addr"}).
		ScanVals(&ids))
	assert.Equal(t, ids, []uint32{1, 2, 3, 4, 5})

	assert.EqualError(t, ds.From("items").ScanVals([]uint32{}),
		"goqu: type must be a pointer to a slice when scanning into vals")
	assert.EqualError(t, ds.From("items").ScanVals(dsTestActionItem{}),
		"goqu: type must be a pointer to a slice when scanning into vals")
}

func (dqs *datasetQuerySuite) TestScanVal() {
	t := dqs.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectQuery(`SELECT "id" FROM "items" LIMIT 1`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("10"))

	qf := dqs.queryFactory(mDb)
	ds := newDataset("mock", qf)
	var id int64
	found, err := ds.From("items").Select("id").ScanVal(&id)
	assert.NoError(t, err)
	assert.Equal(t, id, int64(10))
	assert.True(t, found)

	found, err = ds.From("items").ScanVal([]int64{})
	assert.False(t, found)
	assert.EqualError(t, err, "goqu: type must be a pointer when scanning into val")
	found, err = ds.From("items").ScanVal(10)
	assert.False(t, found)
	assert.EqualError(t, err, "goqu: type must be a pointer when scanning into val")
}

func (dqs *datasetQuerySuite) TestScanVal_WithPreparedStatement() {
	t := dqs.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectQuery(
		`SELECT "id" FROM "items" WHERE \(\("address" = \?\) AND \("name" IN \(\?, \?, \?\)\)\) LIMIT ?`,
	).
		WithArgs("111 Test Addr", "Bob", "Sally", "Billy", 1).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("10"))

	qf := dqs.queryFactory(mDb)
	ds := newDataset("mock", qf)
	var id int64
	found, err := ds.From("items").
		Prepared(true).
		Select("id").
		Where(Ex{"name": []string{"Bob", "Sally", "Billy"}, "address": "111 Test Addr"}).
		ScanVal(&id)
	assert.NoError(t, err)
	assert.Equal(t, id, int64(10))
	assert.True(t, found)

	found, err = ds.From("items").ScanVal([]int64{})
	assert.False(t, found)
	assert.EqualError(t, err, "goqu: type must be a pointer when scanning into val")
	found, err = ds.From("items").ScanVal(10)
	assert.False(t, found)
	assert.EqualError(t, err, "goqu: type must be a pointer when scanning into val")
}

func (dqs *datasetQuerySuite) TestCount() {
	t := dqs.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectQuery(`SELECT COUNT\(\*\) AS "count" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"count"}).FromCSVString("10"))

	qf := dqs.queryFactory(mDb)
	ds := newDataset("mock", qf)
	count, err := ds.From("items").Count()
	assert.NoError(t, err)
	assert.Equal(t, count, int64(10))
}

func (dqs *datasetQuerySuite) TestCount_WithPreparedStatement() {
	t := dqs.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectQuery(
		`SELECT COUNT\(\*\) AS "count" FROM "items" WHERE \(\("address" = \?\) AND \("name" IN \(\?, \?, \?\)\)\)`,
	).
		WithArgs("111 Test Addr", "Bob", "Sally", "Billy", 1).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).FromCSVString("10"))

	qf := dqs.queryFactory(mDb)
	ds := newDataset("mock", qf)
	count, err := ds.From("items").
		Prepared(true).
		Where(Ex{"name": []string{"Bob", "Sally", "Billy"}, "address": "111 Test Addr"}).
		Count()
	assert.NoError(t, err)
	assert.Equal(t, count, int64(10))
}

func (dqs *datasetQuerySuite) TestPluck() {
	t := dqs.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectQuery(`SELECT "name" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"name"}).FromCSVString("test1\ntest2\ntest3\ntest4\ntest5"))

	qf := dqs.queryFactory(mDb)
	ds := newDataset("mock", qf)
	var names []string
	assert.NoError(t, ds.From("items").Pluck(&names, "name"))
	assert.Equal(t, names, []string{"test1", "test2", "test3", "test4", "test5"})
}

func (dqs *datasetQuerySuite) TestPluck_WithPreparedStatement() {
	t := dqs.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectQuery(
		`SELECT "name" FROM "items" WHERE \(\("address" = \?\) AND \("name" IN \(\?, \?, \?\)\)\)`,
	).
		WithArgs("111 Test Addr", "Bob", "Sally", "Billy").
		WillReturnRows(sqlmock.NewRows([]string{"name"}).FromCSVString("Bob\nSally\nBilly"))

	qf := dqs.queryFactory(mDb)
	ds := newDataset("mock", qf)
	var names []string
	assert.NoError(t, ds.From("items").
		Prepared(true).
		Where(Ex{"name": []string{"Bob", "Sally", "Billy"}, "address": "111 Test Addr"}).
		Pluck(&names, "name"))
	assert.Equal(t, names, []string{"Bob", "Sally", "Billy"})
}

func (dqs *datasetQuerySuite) TestUpdate() {
	t := dqs.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectExec(`UPDATE "items" SET "address"='111 Test Addr',"name"='Test1' WHERE \("name" IS NULL\)`).
		WithArgs().
		WillReturnResult(sqlmock.NewResult(0, 0))

	qf := dqs.queryFactory(mDb)
	ds := newDataset("mock", qf)
	_, err = ds.From("items").Where(C("name").IsNull()).Update(Record{
		"address": "111 Test Addr",
		"name":    "Test1",
	}).Exec()
	assert.NoError(t, err)
}

func (dqs *datasetQuerySuite) TestUpdate_WithPreparedStatement() {
	t := dqs.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectExec(
		`UPDATE "items" SET "address"=\?,"name"=\? WHERE \(\("address" = \?\) AND \("name" IN \(\?, \?, \?\)\)\)`,
	).
		WithArgs("112 Test Addr", "Test1", "111 Test Addr", "Bob", "Sally", "Billy").
		WillReturnResult(sqlmock.NewResult(0, 0))

	qf := dqs.queryFactory(mDb)
	ds := newDataset("mock", qf)
	_, err = ds.From("items").
		Prepared(true).
		Where(Ex{"name": []string{"Bob", "Sally", "Billy"}, "address": "111 Test Addr"}).
		Update(Record{"address": "112 Test Addr", "name": "Test1"}).
		Exec()
	assert.NoError(t, err)
}

func (dqs *datasetQuerySuite) TestInsert() {
	t := dqs.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectExec(`INSERT INTO "items" \("address", "name"\) VALUES \('111 Test Addr', 'Test1'\)`).
		WithArgs().
		WillReturnResult(sqlmock.NewResult(0, 0))

	qf := dqs.queryFactory(mDb)
	ds := newDataset("mock", qf)
	_, err = ds.From("items").Insert(Record{"address": "111 Test Addr", "name": "Test1"}).Exec()
	assert.NoError(t, err)
}

func (dqs *datasetQuerySuite) TestInsert_WithPreparedStatment() {
	t := dqs.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectExec(`INSERT INTO "items" \("address", "name"\) VALUES \(\?, \?\), \(\?, \?\)`).
		WithArgs("111 Test Addr", "Test1", "112 Test Addr", "Test2").
		WillReturnResult(sqlmock.NewResult(0, 0))

	qf := dqs.queryFactory(mDb)
	ds := newDataset("mock", qf)
	_, err = ds.From("items").
		Prepared(true).
		Insert(
			Record{"address": "111 Test Addr", "name": "Test1"},
			Record{"address": "112 Test Addr", "name": "Test2"},
		).
		Exec()
	assert.NoError(t, err)
}

func (dqs *datasetQuerySuite) TestDelete() {
	t := dqs.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectExec(`DELETE FROM "items" WHERE \("id" > 10\)`).
		WithArgs().
		WillReturnResult(sqlmock.NewResult(0, 0))

	qf := dqs.queryFactory(mDb)
	ds := newDataset("mock", qf)
	_, err = ds.From("items").Where(C("id").Gt(10)).Delete().Exec()
	assert.NoError(t, err)
}

func (dqs *datasetQuerySuite) TestDelete_WithPreparedStatment() {
	t := dqs.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectExec(`DELETE FROM "items" WHERE \("id" > \?\)`).
		WithArgs(10).
		WillReturnResult(sqlmock.NewResult(0, 0))

	qf := dqs.queryFactory(mDb)
	ds := newDataset("mock", qf)
	_, err = ds.From("items").Prepared(true).Where(Ex{"id": Op{"gt": 10}}).Delete().Exec()
	assert.NoError(t, err)
}

func TestDatasetQuerySuite(t *testing.T) {
	suite.Run(t, new(datasetQuerySuite))
}
