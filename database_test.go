package goqu

import (
	"context"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/doug-martin/goqu/v7/internal/errors"
	"github.com/stretchr/testify/assert"
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

type databaseTest struct {
	suite.Suite
}

func (dt *databaseTest) TestLogger() {
	t := dt.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
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
	assert.NoError(t, db.ScanStructs(&items, `SELECT * FROM "items"`))
	_, err = db.Exec(`SELECT * FROM "items" WHERE "id" = ?`, 1)
	assert.NoError(t, err)
	db.Trace("TEST", "")
	assert.Equal(t, logger.Messages, []string{
		"[goqu] QUERY [query:=`SELECT * FROM \"items\"`]",
		"[goqu] EXEC [query:=`SELECT * FROM \"items\" WHERE \"id\" = ?` args:=[1]]",
		"[goqu] TEST",
	})
}

func (dt *databaseTest) TestScanStructs() {
	t := dt.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	mock.ExpectQuery(`SELECT "test" FROM "items"`).
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
	assert.EqualError(t, db.ScanStructs(items, `SELECT * FROM "items"`),
		"goqu: type must be a pointer to a slice when scanning into structs")
	assert.EqualError(t, db.ScanStructs(&testActionItem{}, `SELECT * FROM "items"`),
		"goqu: type must be a pointer to a slice when scanning into structs")
	assert.EqualError(t, db.ScanStructs(&items, `SELECT "test" FROM "items"`),
		`goqu: unable to find corresponding field to column "test" returned by query`)
}

func (dt *databaseTest) TestScanStruct() {
	t := dt.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectQuery(`SELECT \* FROM "items" LIMIT 1`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).FromCSVString("111 Test Addr,Test1"))

	mock.ExpectQuery(`SELECT "test" FROM "items" LIMIT 1`).
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
	assert.EqualError(t, err, "goqu: type must be a pointer to a struct when scanning into a struct")
	_, err = db.ScanStruct([]testActionItem{}, `SELECT * FROM "items" LIMIT 1`)
	assert.EqualError(t, err, "goqu: type must be a pointer to a struct when scanning into a struct")
	_, err = db.ScanStruct(&item, `SELECT "test" FROM "items" LIMIT 1`)
	assert.EqualError(t, err, `goqu: unable to find corresponding field to column "test" returned by query`)
}

func (dt *databaseTest) TestScanVals() {
	t := dt.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("1\n2\n3\n4\n5"))

	db := New("mock", mDb)
	var ids []uint32
	assert.NoError(t, db.ScanVals(&ids, `SELECT "id" FROM "items"`))
	assert.Len(t, ids, 5)

	assert.EqualError(t, db.ScanVals([]uint32{}, `SELECT "id" FROM "items"`),
		"goqu: type must be a pointer to a slice when scanning into vals")
	assert.EqualError(t, db.ScanVals(testActionItem{}, `SELECT "id" FROM "items"`),
		"goqu: type must be a pointer to a slice when scanning into vals")
}

func (dt *databaseTest) TestScanVal() {
	t := dt.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("10"))

	db := New("mock", mDb)
	var id int64
	found, err := db.ScanVal(&id, `SELECT "id" FROM "items"`)
	assert.NoError(t, err)
	assert.Equal(t, id, int64(10))
	assert.True(t, found)

	found, err = db.ScanVal([]int64{}, `SELECT "id" FROM "items"`)
	assert.False(t, found)
	assert.EqualError(t, err, "goqu: type must be a pointer when scanning into val")
	found, err = db.ScanVal(10, `SELECT "id" FROM "items"`)
	assert.False(t, found)
	assert.EqualError(t, err, "goqu: type must be a pointer when scanning into val")
}

func (dt *databaseTest) TestExec() {
	t := dt.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectExec(`UPDATE "items" SET "address"='111 Test Addr',"name"='Test1' WHERE \("name" IS NULL\)`).
		WithArgs().
		WillReturnResult(sqlmock.NewResult(0, 0))

	mock.ExpectExec(`UPDATE "items" SET "address"='111 Test Addr',"name"='Test1' WHERE \("name" IS NULL\)`).
		WithArgs().
		WillReturnError(errors.New("mock error"))

	db := New("mock", mDb)
	_, err = db.Exec(`UPDATE "items" SET "address"='111 Test Addr',"name"='Test1' WHERE ("name" IS NULL)`)
	assert.NoError(t, err)
	_, err = db.Exec(`UPDATE "items" SET "address"='111 Test Addr',"name"='Test1' WHERE ("name" IS NULL)`)
	assert.EqualError(t, err, "goqu: mock error")
}

func (dt *databaseTest) TestQuery() {
	t := dt.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnError(errors.New("mock error"))

	db := New("mock", mDb)
	_, err = db.Query(`SELECT * FROM "items"`)
	assert.NoError(t, err, "goqu - mock error")

	_, err = db.Query(`SELECT * FROM "items"`)
	assert.EqualError(t, err, "goqu: mock error")
}

func (dt *databaseTest) TestQueryRow() {
	t := dt.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
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
	assert.NoError(t, rows.Scan(&address, &name))

	rows = db.QueryRow(`SELECT * FROM "items"`)
	assert.EqualError(t, rows.Scan(&address, &name), "goqu: mock error")
}

func (dt *databaseTest) TestPrepare() {
	t := dt.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectPrepare("SELECT \\* FROM test WHERE id = \\?")
	db := New("mock", mDb)
	stmt, err := db.Prepare("SELECT * FROM test WHERE id = ?")
	assert.NoError(t, err)
	assert.NotNil(t, stmt)
}

func (dt *databaseTest) TestBegin() {
	t := dt.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectBegin()
	mock.ExpectBegin().WillReturnError(errors.New("transaction error"))
	db := New("mock", mDb)
	tx, err := db.Begin()
	assert.NoError(t, err)
	assert.Equal(t, tx.Dialect(), "mock")

	_, err = db.Begin()
	assert.EqualError(t, err, "goqu: transaction error")
}

func (dt *databaseTest) TestBeginTx() {
	t := dt.T()
	ctx := context.Background()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectBegin()
	mock.ExpectBegin().WillReturnError(errors.New("transaction error"))
	db := New("mock", mDb)
	tx, err := db.BeginTx(ctx, nil)
	assert.NoError(t, err)
	assert.Equal(t, tx.Dialect(), "mock")

	_, err = db.BeginTx(ctx, nil)
	assert.EqualError(t, err, "goqu: transaction error")
}

func (dt *databaseTest) TestWithTx() {
	t := dt.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)

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
			assert.EqualError(t, err, c.errStr)
		} else {
			assert.NoError(t, err)
		}
	}
}

func TestDatabaseSuite(t *testing.T) {
	suite.Run(t, new(databaseTest))
}

type txDatabaseTest struct {
	suite.Suite
}

func (tdt *txDatabaseTest) TestLogger() {
	t := tdt.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
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
	assert.NoError(t, err)
	logger := new(dbTestMockLogger)
	tx.Logger(logger)
	var items []testActionItem
	assert.NoError(t, tx.ScanStructs(&items, `SELECT * FROM "items"`))
	_, err = tx.Exec(`SELECT * FROM "items" WHERE "id" = ?`, 1)
	assert.NoError(t, err)
	assert.NoError(t, tx.Commit())
	assert.Equal(t, logger.Messages, []string{
		"[goqu - transaction] QUERY [query:=`SELECT * FROM \"items\"`] ",
		"[goqu - transaction] EXEC [query:=`SELECT * FROM \"items\" WHERE \"id\" = ?` args:=[1]] ",
		"[goqu - transaction] COMMIT",
	})
}

func (tdt *txDatabaseTest) TestLogger_FromDb() {
	t := tdt.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
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
	assert.NoError(t, err)

	var items []testActionItem
	assert.NoError(t, tx.ScanStructs(&items, `SELECT * FROM "items"`))
	_, err = tx.Exec(`SELECT * FROM "items" WHERE "id" = ?`, 1)
	assert.NoError(t, err)
	assert.NoError(t, tx.Commit())
	assert.Equal(t, logger.Messages, []string{
		"[goqu - transaction] QUERY [query:=`SELECT * FROM \"items\"`] ",
		"[goqu - transaction] EXEC [query:=`SELECT * FROM \"items\" WHERE \"id\" = ?` args:=[1]] ",
		"[goqu - transaction] COMMIT",
	})
}

func (tdt *txDatabaseTest) TestCommit() {
	t := tdt.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectBegin()
	mock.ExpectCommit()
	db := newDatabase("mock", mDb)
	tx, err := db.Begin()
	assert.NoError(t, err)
	assert.NoError(t, tx.Commit())
}

func (tdt *txDatabaseTest) TestRollback() {
	t := tdt.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectBegin()
	mock.ExpectRollback()
	db := newDatabase("mock", mDb)
	tx, err := db.Begin()
	assert.NoError(t, err)
	assert.NoError(t, tx.Rollback())
}

func (tdt *txDatabaseTest) TestFrom() {
	t := tdt.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectBegin()
	mock.ExpectCommit()
	db := newDatabase("mock", mDb)
	tx, err := db.Begin()
	assert.NoError(t, err)
	assert.NotNil(t, From("test"))
	assert.NoError(t, tx.Commit())
}

func (tdt *txDatabaseTest) TestScanStructs() {
	t := tdt.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
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
	assert.NoError(t, err)
	var items []testActionItem
	assert.NoError(t, tx.ScanStructs(&items, `SELECT * FROM "items"`))
	assert.Len(t, items, 2)
	assert.Equal(t, items[0].Address, "111 Test Addr")
	assert.Equal(t, items[0].Name, "Test1")

	assert.Equal(t, items[1].Address, "211 Test Addr")
	assert.Equal(t, items[1].Name, "Test2")

	items = items[0:0]
	assert.EqualError(t, tx.ScanStructs(items, `SELECT * FROM "items"`),
		"goqu: type must be a pointer to a slice when scanning into structs")
	assert.EqualError(t, tx.ScanStructs(&testActionItem{}, `SELECT * FROM "items"`),
		"goqu: type must be a pointer to a slice when scanning into structs")
	assert.EqualError(t, tx.ScanStructs(&items, `SELECT "test" FROM "items"`),
		`goqu: unable to find corresponding field to column "test" returned by query`)
	assert.NoError(t, tx.Commit())
}

func (tdt *txDatabaseTest) TestScanStruct() {
	t := tdt.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
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
	assert.NoError(t, err)
	var item testActionItem
	found, err := tx.ScanStruct(&item, `SELECT * FROM "items" LIMIT 1`)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, item.Address, "111 Test Addr")
	assert.Equal(t, item.Name, "Test1")

	_, err = tx.ScanStruct(item, `SELECT * FROM "items" LIMIT 1`)
	assert.EqualError(t, err, "goqu: type must be a pointer to a struct when scanning into a struct")
	_, err = tx.ScanStruct([]testActionItem{}, `SELECT * FROM "items" LIMIT 1`)
	assert.EqualError(t, err, "goqu: type must be a pointer to a struct when scanning into a struct")
	_, err = tx.ScanStruct(&item, `SELECT "test" FROM "items" LIMIT 1`)
	assert.EqualError(t, err, `goqu: unable to find corresponding field to column "test" returned by query`)
	assert.NoError(t, tx.Commit())
}

func (tdt *txDatabaseTest) TestScanVals() {
	t := tdt.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("1\n2\n3\n4\n5"))
	mock.ExpectCommit()
	db := newDatabase("mock", mDb)
	tx, err := db.Begin()
	assert.NoError(t, err)
	var ids []uint32
	assert.NoError(t, tx.ScanVals(&ids, `SELECT "id" FROM "items"`))
	assert.Len(t, ids, 5)

	assert.EqualError(t, tx.ScanVals([]uint32{}, `SELECT "id" FROM "items"`),
		"goqu: type must be a pointer to a slice when scanning into vals")
	assert.EqualError(t, tx.ScanVals(testActionItem{}, `SELECT "id" FROM "items"`),
		"goqu: type must be a pointer to a slice when scanning into vals")
	assert.NoError(t, tx.Commit())
}

func (tdt *txDatabaseTest) TestScanVal() {
	t := tdt.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("10"))
	mock.ExpectCommit()
	db := newDatabase("mock", mDb)
	tx, err := db.Begin()
	assert.NoError(t, err)
	var id int64
	found, err := tx.ScanVal(&id, `SELECT "id" FROM "items"`)
	assert.NoError(t, err)
	assert.Equal(t, id, int64(10))
	assert.True(t, found)

	found, err = tx.ScanVal([]int64{}, `SELECT "id" FROM "items"`)
	assert.False(t, found)
	assert.EqualError(t, err, "goqu: type must be a pointer when scanning into val")
	found, err = tx.ScanVal(10, `SELECT "id" FROM "items"`)
	assert.False(t, found)
	assert.EqualError(t, err, "goqu: type must be a pointer when scanning into val")
	assert.NoError(t, tx.Commit())
}

func (tdt *txDatabaseTest) TestExec() {
	t := tdt.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
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
	assert.NoError(t, err)
	_, err = tx.Exec(`UPDATE "items" SET "address"='111 Test Addr',"name"='Test1' WHERE ("name" IS NULL)`)
	assert.NoError(t, err)
	_, err = tx.Exec(`UPDATE "items" SET "address"='111 Test Addr',"name"='Test1' WHERE ("name" IS NULL)`)
	assert.EqualError(t, err, "goqu: mock error")
	assert.NoError(t, tx.Commit())
}

func (tdt *txDatabaseTest) TestQuery() {
	t := tdt.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
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
	assert.NoError(t, err)
	_, err = tx.Query(`SELECT * FROM "items"`)
	assert.NoError(t, err, "goqu - mock error")

	_, err = tx.Query(`SELECT * FROM "items"`)
	assert.EqualError(t, err, "goqu: mock error")
	assert.NoError(t, tx.Commit())
}

func (tdt *txDatabaseTest) TestQueryRow() {
	t := tdt.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
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
	assert.NoError(t, err)
	rows := tx.QueryRow(`SELECT * FROM "items"`)
	var address string
	var name string
	assert.NoError(t, rows.Scan(&address, &name))

	rows = tx.QueryRow(`SELECT * FROM "items"`)
	assert.EqualError(t, rows.Scan(&address, &name), "goqu: mock error")
	assert.NoError(t, tx.Commit())
}

func (tdt *txDatabaseTest) TestWrap() {
	t := tdt.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)
	mock.ExpectBegin()
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectRollback()
	db := newDatabase("mock", mDb)
	tx, err := db.Begin()
	assert.NoError(t, err)
	assert.NoError(t, tx.Wrap(func() error {
		return nil
	}))
	tx, err = db.Begin()
	assert.NoError(t, err)
	assert.EqualError(t, tx.Wrap(func() error {
		return errors.New("tx error")
	}), "goqu: tx error")
}

func TestTxDatabaseSuite(t *testing.T) {
	suite.Run(t, new(txDatabaseTest))
}
