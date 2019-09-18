package goqu

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/doug-martin/goqu/v9/internal/errors"
	"github.com/stretchr/testify/suite"
)

type testActionItem struct {
	Address string `db:"address"`
	Name    string `db:"name"`
}

type dbTestMockLogger struct {
	Messages []string
}

func (dtml *dbTestMockLogger) Printf(format string, v ...interface{}) {
	dtml.Messages = append(dtml.Messages, fmt.Sprintf(format, v...))
}

func (dtml *dbTestMockLogger) Reset(format string, v ...interface{}) {
	dtml.Messages = dtml.Messages[0:0]
}

type databaseSuite struct {
	suite.Suite
}

func (ds *databaseSuite) TestLogger() {
	mDb, mock, err := sqlmock.New()
	ds.NoError(err)
	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	mock.ExpectExec(`SELECT \* FROM "items" WHERE "id" = ?`).
		WithArgs(1).
		WillReturnResult(sqlmock.NewResult(0, 0))

	db := New("db-mock", mDb)
	logger := new(dbTestMockLogger)
	db.Logger(logger)
	var items []testActionItem
	ds.NoError(db.ScanStructs(&items, `SELECT * FROM "items"`))
	_, err = db.Exec(`SELECT * FROM "items" WHERE "id" = ?`, 1)
	ds.NoError(err)
	db.Trace("TEST", "")
	ds.Equal([]string{
		"[goqu] QUERY [query:=`SELECT * FROM \"items\"`]",
		"[goqu] EXEC [query:=`SELECT * FROM \"items\" WHERE \"id\" = ?` args:=[1]]",
		"[goqu] TEST",
	}, logger.Messages)
}

func (ds *databaseSuite) TestScanStructs() {
	mDb, mock, err := sqlmock.New()
	ds.NoError(err)
	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	mock.ExpectQuery(`SELECT "test" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"test"}).FromCSVString("test1\ntest2"))

	db := New("db-mock", mDb)
	var items []testActionItem
	ds.NoError(db.ScanStructs(&items, `SELECT * FROM "items"`))
	ds.Len(items, 2)
	ds.Equal("111 Test Addr", items[0].Address)
	ds.Equal("Test1", items[0].Name)

	ds.Equal("211 Test Addr", items[1].Address)
	ds.Equal("Test2", items[1].Name)

	items = items[0:0]
	ds.EqualError(db.ScanStructs(items, `SELECT * FROM "items"`),
		"goqu: type must be a pointer to a slice when scanning into structs")
	ds.EqualError(db.ScanStructs(&testActionItem{}, `SELECT * FROM "items"`),
		"goqu: type must be a pointer to a slice when scanning into structs")
	ds.EqualError(db.ScanStructs(&items, `SELECT "test" FROM "items"`),
		`goqu: unable to find corresponding field to column "test" returned by query`)
}

func (ds *databaseSuite) TestScanStruct() {
	mDb, mock, err := sqlmock.New()
	ds.NoError(err)
	mock.ExpectQuery(`SELECT \* FROM "items" LIMIT 1`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).FromCSVString("111 Test Addr,Test1"))

	mock.ExpectQuery(`SELECT "test" FROM "items" LIMIT 1`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"test"}).FromCSVString("test1\ntest2"))

	db := New("mock", mDb)
	var item testActionItem
	found, err := db.ScanStruct(&item, `SELECT * FROM "items" LIMIT 1`)
	ds.NoError(err)
	ds.True(found)
	ds.Equal("111 Test Addr", item.Address)
	ds.Equal("Test1", item.Name)

	_, err = db.ScanStruct(item, `SELECT * FROM "items" LIMIT 1`)
	ds.EqualError(err, "goqu: type must be a pointer to a struct when scanning into a struct")
	_, err = db.ScanStruct([]testActionItem{}, `SELECT * FROM "items" LIMIT 1`)
	ds.EqualError(err, "goqu: type must be a pointer to a struct when scanning into a struct")
	_, err = db.ScanStruct(&item, `SELECT "test" FROM "items" LIMIT 1`)
	ds.EqualError(err, `goqu: unable to find corresponding field to column "test" returned by query`)
}

func (ds *databaseSuite) TestScanVals() {
	mDb, mock, err := sqlmock.New()
	ds.NoError(err)
	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("1\n2\n3\n4\n5"))

	db := New("mock", mDb)
	var ids []uint32
	ds.NoError(db.ScanVals(&ids, `SELECT "id" FROM "items"`))
	ds.Len(ids, 5)

	ds.EqualError(db.ScanVals([]uint32{}, `SELECT "id" FROM "items"`),
		"goqu: type must be a pointer to a slice when scanning into vals")
	ds.EqualError(db.ScanVals(testActionItem{}, `SELECT "id" FROM "items"`),
		"goqu: type must be a pointer to a slice when scanning into vals")
}

func (ds *databaseSuite) TestScanVal() {
	mDb, mock, err := sqlmock.New()
	ds.NoError(err)
	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("10"))

	db := New("mock", mDb)
	var id int64
	found, err := db.ScanVal(&id, `SELECT "id" FROM "items"`)
	ds.NoError(err)
	ds.Equal(int64(10), id)
	ds.True(found)

	found, err = db.ScanVal([]int64{}, `SELECT "id" FROM "items"`)
	ds.False(found)
	ds.EqualError(err, "goqu: type must be a pointer when scanning into val")
	found, err = db.ScanVal(10, `SELECT "id" FROM "items"`)
	ds.False(found)
	ds.EqualError(err, "goqu: type must be a pointer when scanning into val")
}

func (ds *databaseSuite) TestExec() {
	mDb, mock, err := sqlmock.New()
	ds.NoError(err)
	mock.ExpectExec(`UPDATE "items" SET "address"='111 Test Addr',"name"='Test1' WHERE \("name" IS NULL\)`).
		WithArgs().
		WillReturnResult(sqlmock.NewResult(0, 0))

	mock.ExpectExec(`UPDATE "items" SET "address"='111 Test Addr',"name"='Test1' WHERE \("name" IS NULL\)`).
		WithArgs().
		WillReturnError(errors.New("mock error"))

	db := New("mock", mDb)
	_, err = db.Exec(`UPDATE "items" SET "address"='111 Test Addr',"name"='Test1' WHERE ("name" IS NULL)`)
	ds.NoError(err)
	_, err = db.Exec(`UPDATE "items" SET "address"='111 Test Addr',"name"='Test1' WHERE ("name" IS NULL)`)
	ds.EqualError(err, "goqu: mock error")
}

func (ds *databaseSuite) TestQuery() {
	mDb, mock, err := sqlmock.New()
	ds.NoError(err)
	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnError(errors.New("mock error"))

	db := New("mock", mDb)
	_, err = db.Query(`SELECT * FROM "items"`)
	ds.NoError(err, "goqu - mock error")

	_, err = db.Query(`SELECT * FROM "items"`)
	ds.EqualError(err, "goqu: mock error")
}

func (ds *databaseSuite) TestQueryRow() {
	mDb, mock, err := sqlmock.New()
	ds.NoError(err)
	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnError(errors.New("mock error"))

	db := New("mock", mDb)
	rows := db.QueryRow(`SELECT * FROM "items"`)
	var address string
	var name string
	ds.NoError(rows.Scan(&address, &name))

	rows = db.QueryRow(`SELECT * FROM "items"`)
	ds.EqualError(rows.Scan(&address, &name), "goqu: mock error")
}

func (ds *databaseSuite) TestPrepare() {
	mDb, mock, err := sqlmock.New()
	ds.NoError(err)
	mock.ExpectPrepare("SELECT \\* FROM test WHERE id = \\?")
	db := New("mock", mDb)
	stmt, err := db.Prepare("SELECT * FROM test WHERE id = ?")
	ds.NoError(err)
	ds.NotNil(stmt)
}

func (ds *databaseSuite) TestBegin() {
	mDb, mock, err := sqlmock.New()
	ds.NoError(err)
	mock.ExpectBegin()
	mock.ExpectBegin().WillReturnError(errors.New("transaction error"))
	db := New("mock", mDb)
	tx, err := db.Begin()
	ds.NoError(err)
	ds.Equal("mock", tx.Dialect())

	_, err = db.Begin()
	ds.EqualError(err, "goqu: transaction error")
}

func (ds *databaseSuite) TestBeginTx() {
	ctx := context.Background()
	mDb, mock, err := sqlmock.New()
	ds.NoError(err)
	mock.ExpectBegin()
	mock.ExpectBegin().WillReturnError(errors.New("transaction error"))
	db := New("mock", mDb)
	tx, err := db.BeginTx(ctx, nil)
	ds.NoError(err)
	ds.Equal("mock", tx.Dialect())

	_, err = db.BeginTx(ctx, nil)
	ds.EqualError(err, "goqu: transaction error")
}

func (ds *databaseSuite) TestWithTx() {
	mDb, mock, err := sqlmock.New()
	ds.NoError(err)

	db := newDatabase("mock", mDb)

	cases := []struct {
		expectf func(sqlmock.Sqlmock)
		f       func(*TxDatabase) error
		wantErr bool
		errStr  string
	}{
		{
			expectf: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectCommit()
			},
			f:       func(_ *TxDatabase) error { return nil },
			wantErr: false,
		},
		{
			expectf: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin().WillReturnError(errors.New("transaction begin error"))
			},
			f:       func(_ *TxDatabase) error { return nil },
			wantErr: true,
			errStr:  "goqu: transaction begin error",
		},
		{
			expectf: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectRollback()
			},
			f:       func(_ *TxDatabase) error { return errors.New("transaction error") },
			wantErr: true,
			errStr:  "goqu: transaction error",
		},
		{
			expectf: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectRollback().WillReturnError(errors.New("transaction rollback error"))
			},
			f:       func(_ *TxDatabase) error { return errors.New("something wrong") },
			wantErr: true,
			errStr:  "goqu: transaction rollback error",
		},
	}
	for _, c := range cases {
		c.expectf(mock)
		err := db.WithTx(c.f)
		if c.wantErr {
			ds.EqualError(err, c.errStr)
		} else {
			ds.NoError(err)
		}
	}
}

func (ds *databaseSuite) TestDataRace() {
	mDb, mock, err := sqlmock.New()
	ds.NoError(err)
	db := newDatabase("mock", mDb)

	const concurrency = 10

	for i := 0; i < concurrency; i++ {
		mock.ExpectQuery(`SELECT "address", "name" FROM "items"`).
			WithArgs().
			WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
				FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))
	}

	wg := sync.WaitGroup{}
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			sql := db.From("items").Limit(1)
			var item testActionItem
			found, err := sql.ScanStruct(&item)
			ds.NoError(err)
			ds.True(found)
			ds.Equal(item.Address, "111 Test Addr")
			ds.Equal(item.Name, "Test1")
		}()
	}

	wg.Wait()
}

func TestDatabaseSuite(t *testing.T) {
	suite.Run(t, new(databaseSuite))
}

type txdatabaseSuite struct {
	suite.Suite
}

func (tds *txdatabaseSuite) TestLogger() {
	mDb, mock, err := sqlmock.New()
	tds.NoError(err)
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	mock.ExpectExec(`SELECT \* FROM "items" WHERE "id" = ?`).
		WithArgs(1).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	tx, err := newDatabase("db-mock", mDb).Begin()
	tds.NoError(err)
	logger := new(dbTestMockLogger)
	tx.Logger(logger)
	var items []testActionItem
	tds.NoError(tx.ScanStructs(&items, `SELECT * FROM "items"`))
	_, err = tx.Exec(`SELECT * FROM "items" WHERE "id" = ?`, 1)
	tds.NoError(err)
	tds.NoError(tx.Commit())
	tds.Equal([]string{
		"[goqu - transaction] QUERY [query:=`SELECT * FROM \"items\"`] ",
		"[goqu - transaction] EXEC [query:=`SELECT * FROM \"items\" WHERE \"id\" = ?` args:=[1]] ",
		"[goqu - transaction] COMMIT",
	}, logger.Messages)
}

func (tds *txdatabaseSuite) TestLogger_FromDb() {
	mDb, mock, err := sqlmock.New()
	tds.NoError(err)
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	mock.ExpectExec(`SELECT \* FROM "items" WHERE "id" = ?`).
		WithArgs(1).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	db := New("db-mock", mDb)
	logger := new(dbTestMockLogger)
	db.Logger(logger)
	tx, err := db.Begin()
	tds.NoError(err)

	var items []testActionItem
	tds.NoError(tx.ScanStructs(&items, `SELECT * FROM "items"`))
	_, err = tx.Exec(`SELECT * FROM "items" WHERE "id" = ?`, 1)
	tds.NoError(err)
	tds.NoError(tx.Commit())
	tds.Equal([]string{
		"[goqu - transaction] QUERY [query:=`SELECT * FROM \"items\"`] ",
		"[goqu - transaction] EXEC [query:=`SELECT * FROM \"items\" WHERE \"id\" = ?` args:=[1]] ",
		"[goqu - transaction] COMMIT",
	}, logger.Messages)
}

func (tds *txdatabaseSuite) TestCommit() {
	mDb, mock, err := sqlmock.New()
	tds.NoError(err)
	mock.ExpectBegin()
	mock.ExpectCommit()
	db := newDatabase("mock", mDb)
	tx, err := db.Begin()
	tds.NoError(err)
	tds.NoError(tx.Commit())
}

func (tds *txdatabaseSuite) TestRollback() {
	mDb, mock, err := sqlmock.New()
	tds.NoError(err)
	mock.ExpectBegin()
	mock.ExpectRollback()
	db := newDatabase("mock", mDb)
	tx, err := db.Begin()
	tds.NoError(err)
	tds.NoError(tx.Rollback())
}

func (tds *txdatabaseSuite) TestFrom() {
	mDb, mock, err := sqlmock.New()
	tds.NoError(err)
	mock.ExpectBegin()
	mock.ExpectCommit()
	db := newDatabase("mock", mDb)
	tx, err := db.Begin()
	tds.NoError(err)
	tds.NotNil(From("test"))
	tds.NoError(tx.Commit())
}

func (tds *txdatabaseSuite) TestScanStructs() {
	mDb, mock, err := sqlmock.New()
	tds.NoError(err)
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	mock.ExpectQuery(`SELECT "test" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"test"}).FromCSVString("test1\ntest2"))
	mock.ExpectCommit()
	db := newDatabase("mock", mDb)
	tx, err := db.Begin()
	tds.NoError(err)
	var items []testActionItem
	tds.NoError(tx.ScanStructs(&items, `SELECT * FROM "items"`))
	tds.Len(items, 2)
	tds.Equal("111 Test Addr", items[0].Address)
	tds.Equal("Test1", items[0].Name)

	tds.Equal("211 Test Addr", items[1].Address)
	tds.Equal("Test2", items[1].Name)

	items = items[0:0]
	tds.EqualError(tx.ScanStructs(items, `SELECT * FROM "items"`),
		"goqu: type must be a pointer to a slice when scanning into structs")
	tds.EqualError(tx.ScanStructs(&testActionItem{}, `SELECT * FROM "items"`),
		"goqu: type must be a pointer to a slice when scanning into structs")
	tds.EqualError(tx.ScanStructs(&items, `SELECT "test" FROM "items"`),
		`goqu: unable to find corresponding field to column "test" returned by query`)
	tds.NoError(tx.Commit())
}

func (tds *txdatabaseSuite) TestScanStruct() {
	mDb, mock, err := sqlmock.New()
	tds.NoError(err)
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT \* FROM "items" LIMIT 1`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).FromCSVString("111 Test Addr,Test1"))

	mock.ExpectQuery(`SELECT "test" FROM "items" LIMIT 1`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"test"}).FromCSVString("test1\ntest2"))
	mock.ExpectCommit()
	db := newDatabase("mock", mDb)
	tx, err := db.Begin()
	tds.NoError(err)
	var item testActionItem
	found, err := tx.ScanStruct(&item, `SELECT * FROM "items" LIMIT 1`)
	tds.NoError(err)
	tds.True(found)
	tds.Equal("111 Test Addr", item.Address)
	tds.Equal("Test1", item.Name)

	_, err = tx.ScanStruct(item, `SELECT * FROM "items" LIMIT 1`)
	tds.EqualError(err, "goqu: type must be a pointer to a struct when scanning into a struct")
	_, err = tx.ScanStruct([]testActionItem{}, `SELECT * FROM "items" LIMIT 1`)
	tds.EqualError(err, "goqu: type must be a pointer to a struct when scanning into a struct")
	_, err = tx.ScanStruct(&item, `SELECT "test" FROM "items" LIMIT 1`)
	tds.EqualError(err, `goqu: unable to find corresponding field to column "test" returned by query`)
	tds.NoError(tx.Commit())
}

func (tds *txdatabaseSuite) TestScanVals() {
	mDb, mock, err := sqlmock.New()
	tds.NoError(err)
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("1\n2\n3\n4\n5"))
	mock.ExpectCommit()
	db := newDatabase("mock", mDb)
	tx, err := db.Begin()
	tds.NoError(err)
	var ids []uint32
	tds.NoError(tx.ScanVals(&ids, `SELECT "id" FROM "items"`))
	tds.Len(ids, 5)

	tds.EqualError(tx.ScanVals([]uint32{}, `SELECT "id" FROM "items"`),
		"goqu: type must be a pointer to a slice when scanning into vals")
	tds.EqualError(tx.ScanVals(testActionItem{}, `SELECT "id" FROM "items"`),
		"goqu: type must be a pointer to a slice when scanning into vals")
	tds.NoError(tx.Commit())
}

func (tds *txdatabaseSuite) TestScanVal() {
	mDb, mock, err := sqlmock.New()
	tds.NoError(err)
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("10"))
	mock.ExpectCommit()
	db := newDatabase("mock", mDb)
	tx, err := db.Begin()
	tds.NoError(err)
	var id int64
	found, err := tx.ScanVal(&id, `SELECT "id" FROM "items"`)
	tds.NoError(err)
	tds.Equal(int64(10), id)
	tds.True(found)

	found, err = tx.ScanVal([]int64{}, `SELECT "id" FROM "items"`)
	tds.False(found)
	tds.EqualError(err, "goqu: type must be a pointer when scanning into val")
	found, err = tx.ScanVal(10, `SELECT "id" FROM "items"`)
	tds.False(found)
	tds.EqualError(err, "goqu: type must be a pointer when scanning into val")
	tds.NoError(tx.Commit())
}

func (tds *txdatabaseSuite) TestExec() {
	mDb, mock, err := sqlmock.New()
	tds.NoError(err)
	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE "items" SET "address"='111 Test Addr',"name"='Test1' WHERE \("name" IS NULL\)`).
		WithArgs().
		WillReturnResult(sqlmock.NewResult(0, 0))

	mock.ExpectExec(`UPDATE "items" SET "address"='111 Test Addr',"name"='Test1' WHERE \("name" IS NULL\)`).
		WithArgs().
		WillReturnError(errors.New("mock error"))
	mock.ExpectCommit()
	db := newDatabase("mock", mDb)
	tx, err := db.Begin()
	tds.NoError(err)
	_, err = tx.Exec(`UPDATE "items" SET "address"='111 Test Addr',"name"='Test1' WHERE ("name" IS NULL)`)
	tds.NoError(err)
	_, err = tx.Exec(`UPDATE "items" SET "address"='111 Test Addr',"name"='Test1' WHERE ("name" IS NULL)`)
	tds.EqualError(err, "goqu: mock error")
	tds.NoError(tx.Commit())
}

func (tds *txdatabaseSuite) TestQuery() {
	mDb, mock, err := sqlmock.New()
	tds.NoError(err)
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnError(errors.New("mock error"))
	mock.ExpectCommit()
	db := newDatabase("mock", mDb)
	tx, err := db.Begin()
	tds.NoError(err)
	_, err = tx.Query(`SELECT * FROM "items"`)
	tds.NoError(err, "goqu - mock error")

	_, err = tx.Query(`SELECT * FROM "items"`)
	tds.EqualError(err, "goqu: mock error")
	tds.NoError(tx.Commit())
}

func (tds *txdatabaseSuite) TestQueryRow() {
	mDb, mock, err := sqlmock.New()
	tds.NoError(err)
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnError(errors.New("mock error"))
	mock.ExpectCommit()
	db := newDatabase("mock", mDb)
	tx, err := db.Begin()
	tds.NoError(err)
	rows := tx.QueryRow(`SELECT * FROM "items"`)
	var address string
	var name string
	tds.NoError(rows.Scan(&address, &name))

	rows = tx.QueryRow(`SELECT * FROM "items"`)
	tds.EqualError(rows.Scan(&address, &name), "goqu: mock error")
	tds.NoError(tx.Commit())
}

func (tds *txdatabaseSuite) TestWrap() {
	mDb, mock, err := sqlmock.New()
	tds.NoError(err)
	mock.ExpectBegin()
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectRollback()
	db := newDatabase("mock", mDb)
	tx, err := db.Begin()
	tds.NoError(err)
	tds.NoError(tx.Wrap(func() error {
		return nil
	}))
	tx, err = db.Begin()
	tds.NoError(err)
	tds.EqualError(tx.Wrap(func() error {
		return errors.New("tx error")
	}), "goqu: tx error")
}

func (tds *txdatabaseSuite) TestDataRace() {
	mDb, mock, err := sqlmock.New()
	tds.NoError(err)
	mock.ExpectBegin()
	db := newDatabase("mock", mDb)
	tx, err := db.Begin()
	tds.NoError(err)

	const concurrency = 10

	for i := 0; i < concurrency; i++ {
		mock.ExpectQuery(`SELECT "address", "name" FROM "items"`).
			WithArgs().
			WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
				FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))
	}

	wg := sync.WaitGroup{}
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			sql := tx.From("items").Limit(1)
			var item testActionItem
			found, err := sql.ScanStruct(&item)
			tds.NoError(err)
			tds.True(found)
			tds.Equal(item.Address, "111 Test Addr")
			tds.Equal(item.Name, "Test1")
		}()
	}

	wg.Wait()
	mock.ExpectCommit()
	tds.NoError(tx.Commit())
}

func TestTxDatabaseSuite(t *testing.T) {
	suite.Run(t, new(txdatabaseSuite))
}
