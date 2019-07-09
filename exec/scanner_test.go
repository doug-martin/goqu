package exec

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type TestCrudActionItem struct {
	Address string `db:"address"`
	Name    string `db:"name"`
}

type TestCrudActionNoTagsItem struct {
	Address string
	Name    string
}

type TestCrudActionWithPointerField struct {
	Address *string
	Name    *string
}

type TestComposedCrudActionItem struct {
	TestCrudActionItem
	PhoneNumber string `db:"phone_number"`
	Age         int64  `db:"age"`
}

type TestEmbeddedPtrCrudActionItem struct {
	*TestCrudActionItem
	PhoneNumber string `db:"phone_number"`
	Age         int64  `db:"age"`
}

var (
	testAddr1 = "111 Test Addr"
	testAddr2 = "211 Test Addr"
	testName1 = "Test1"
	testName2 = "Test2"
)

type mockDB struct {
	db *sql.DB
}

func newMockDb(db *sql.DB) DbExecutor {
	return &mockDB{db: db}
}

func (m *mockDB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return m.db.ExecContext(ctx, query, args...)
}
func (m *mockDB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return m.db.QueryContext(ctx, query, args...)
}

type crudExecTest struct {
	suite.Suite
}

func (cet *crudExecTest) TestWithError() {
	t := cet.T()
	ctx := context.Background()
	mDb, _, err := sqlmock.New()
	assert.NoError(t, err)
	db := newMockDb(mDb)
	expectedErr := fmt.Errorf("crud exec error")
	e := newQueryExecutor(db, expectedErr, `SELECT * FROM "items"`)
	var items []TestCrudActionItem
	assert.EqualError(t, e.ScanStructs(&items), expectedErr.Error())
	assert.EqualError(t, e.ScanStructsContext(ctx, &items), expectedErr.Error())
	found, err := e.ScanStruct(&TestCrudActionItem{})
	assert.EqualError(t, err, expectedErr.Error())
	assert.False(t, found)
	found, err = e.ScanStructContext(ctx, &TestCrudActionItem{})
	assert.EqualError(t, err, expectedErr.Error())
	assert.False(t, found)
	var vals []string
	assert.EqualError(t, e.ScanVals(&vals), expectedErr.Error())
	assert.EqualError(t, e.ScanValsContext(ctx, &vals), expectedErr.Error())
	var val string
	found, err = e.ScanVal(&val)
	assert.EqualError(t, err, expectedErr.Error())
	assert.False(t, found)
	found, err = e.ScanValContext(ctx, &val)
	assert.EqualError(t, err, expectedErr.Error())
	assert.False(t, found)
}

func (cet *crudExecTest) TestScanStructs_withTaggedFields() {
	t := cet.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	db := newMockDb(mDb)
	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []TestCrudActionItem
	assert.NoError(t, e.ScanStructs(&items))
	assert.Equal(t, []TestCrudActionItem{
		{Address: "111 Test Addr", Name: "Test1"},
		{Address: "211 Test Addr", Name: "Test2"},
	}, items)
}

func (cet *crudExecTest) TestScanStructs_withUntaggedFields() {
	t := cet.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	db := newMockDb(mDb)
	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []TestCrudActionNoTagsItem
	assert.NoError(t, e.ScanStructs(&items))
	assert.Equal(t, []TestCrudActionNoTagsItem{
		{Address: "111 Test Addr", Name: "Test1"},
		{Address: "211 Test Addr", Name: "Test2"},
	}, items)
}

func (cet *crudExecTest) TestScanStructs_withPointerFields() {
	t := cet.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	db := newMockDb(mDb)
	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []TestCrudActionWithPointerField
	assert.NoError(t, e.ScanStructs(&items))
	assert.Equal(t, []TestCrudActionWithPointerField{
		{Address: &testAddr1, Name: &testName1},
		{Address: &testAddr2, Name: &testName2},
	}, items)
}

func (cet *crudExecTest) TestScanStructs_pointers() {
	t := cet.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	db := newMockDb(mDb)
	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []*TestCrudActionItem
	assert.NoError(t, e.ScanStructs(&items))
	assert.Equal(t, []*TestCrudActionItem{
		{Address: "111 Test Addr", Name: "Test1"},
		{Address: "211 Test Addr", Name: "Test2"},
	}, items)
}

func (cet *crudExecTest) TestScanStructs_withEmbeddedStruct() {
	t := cet.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "phone_number", "age"}).
			FromCSVString("111 Test Addr,Test1,111-111-1111,20\n211 Test Addr,Test2,222-222-2222,30"))

	db := newMockDb(mDb)
	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []TestComposedCrudActionItem
	assert.NoError(t, e.ScanStructs(&composed))
	assert.Equal(t, []TestComposedCrudActionItem{
		{TestCrudActionItem: TestCrudActionItem{Address: "111 Test Addr", Name: "Test1"}, PhoneNumber: "111-111-1111", Age: 20},
		{TestCrudActionItem: TestCrudActionItem{Address: "211 Test Addr", Name: "Test2"}, PhoneNumber: "222-222-2222", Age: 30},
	}, composed)
}

func (cet *crudExecTest) TestScanStructs_pointersWithEmbeddedStruct() {
	t := cet.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "phone_number", "age"}).
			FromCSVString("111 Test Addr,Test1,111-111-1111,20\n211 Test Addr,Test2,222-222-2222,30"))

	db := newMockDb(mDb)
	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []*TestComposedCrudActionItem
	assert.NoError(t, e.ScanStructs(&composed))
	assert.Equal(t, []*TestComposedCrudActionItem{
		{TestCrudActionItem: TestCrudActionItem{Address: "111 Test Addr", Name: "Test1"}, PhoneNumber: "111-111-1111", Age: 20},
		{TestCrudActionItem: TestCrudActionItem{Address: "211 Test Addr", Name: "Test2"}, PhoneNumber: "222-222-2222", Age: 30},
	}, composed)
}

func (cet *crudExecTest) TestScanStructs_withEmbeddedStructPointer() {
	t := cet.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "phone_number", "age"}).
			FromCSVString("111 Test Addr,Test1,111-111-1111,20\n211 Test Addr,Test2,222-222-2222,30"))

	db := newMockDb(mDb)
	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []TestEmbeddedPtrCrudActionItem
	assert.NoError(t, e.ScanStructs(&composed))
	assert.Equal(t, []TestEmbeddedPtrCrudActionItem{
		{TestCrudActionItem: &TestCrudActionItem{Address: "111 Test Addr", Name: "Test1"}, PhoneNumber: "111-111-1111", Age: 20},
		{TestCrudActionItem: &TestCrudActionItem{Address: "211 Test Addr", Name: "Test2"}, PhoneNumber: "222-222-2222", Age: 30},
	}, composed)
}

func (cet *crudExecTest) TestScanStructs_pointersWithEmbeddedStructPointer() {
	t := cet.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "phone_number", "age"}).
			FromCSVString("111 Test Addr,Test1,111-111-1111,20\n211 Test Addr,Test2,222-222-2222,30"))

	db := newMockDb(mDb)
	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []*TestEmbeddedPtrCrudActionItem
	assert.NoError(t, e.ScanStructs(&composed))
	assert.Equal(t, []*TestEmbeddedPtrCrudActionItem{
		{TestCrudActionItem: &TestCrudActionItem{Address: "111 Test Addr", Name: "Test1"}, PhoneNumber: "111-111-1111", Age: 20},
		{TestCrudActionItem: &TestCrudActionItem{Address: "211 Test Addr", Name: "Test2"}, PhoneNumber: "222-222-2222", Age: 30},
	}, composed)
}

func (cet *crudExecTest) TestScanStructs_badValue() {
	t := cet.T()
	mDb, _, err := sqlmock.New()
	assert.NoError(t, err)

	db := newMockDb(mDb)
	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []TestCrudActionItem
	assert.Equal(t, errUnsupportedScanStructsType, e.ScanStructs(items))
	assert.Equal(t, errUnsupportedScanStructsType, e.ScanStructs(&TestCrudActionItem{}))
}

func (cet *crudExecTest) TestScanStructs_queryError() {
	t := cet.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WillReturnError(fmt.Errorf("queryExecutor error"))

	db := newMockDb(mDb)
	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []TestCrudActionItem
	assert.EqualError(t, e.ScanStructs(&items), "queryExecutor error")
}

func (cet *crudExecTest) TestScanStructsContext_withTaggedFields() {
	t := cet.T()
	ctx := context.Background()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	db := newMockDb(mDb)
	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []TestCrudActionItem
	assert.NoError(t, e.ScanStructsContext(ctx, &items))
	assert.Equal(t, []TestCrudActionItem{
		{Address: "111 Test Addr", Name: "Test1"},
		{Address: "211 Test Addr", Name: "Test2"},
	}, items)
}

func (cet *crudExecTest) TestScanStructsContext_withUntaggedFields() {
	t := cet.T()
	ctx := context.Background()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	db := newMockDb(mDb)
	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []TestCrudActionNoTagsItem
	assert.NoError(t, e.ScanStructsContext(ctx, &items))
	assert.Equal(t, []TestCrudActionNoTagsItem{
		{Address: "111 Test Addr", Name: "Test1"},
		{Address: "211 Test Addr", Name: "Test2"},
	}, items)
}

func (cet *crudExecTest) TestScanStructsContext_withPointerFields() {
	t := cet.T()
	ctx := context.Background()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	db := newMockDb(mDb)
	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []TestCrudActionWithPointerField
	assert.NoError(t, e.ScanStructsContext(ctx, &items))
	assert.Equal(t, []TestCrudActionWithPointerField{
		{Address: &testAddr1, Name: &testName1},
		{Address: &testAddr2, Name: &testName2},
	}, items)
}

func (cet *crudExecTest) TestScanStructsContext_pointers() {
	t := cet.T()
	ctx := context.Background()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	db := newMockDb(mDb)
	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []*TestCrudActionItem
	assert.NoError(t, e.ScanStructsContext(ctx, &items))
	assert.Equal(t, []*TestCrudActionItem{
		{Address: "111 Test Addr", Name: "Test1"},
		{Address: "211 Test Addr", Name: "Test2"},
	}, items)
}

func (cet *crudExecTest) TestScanStructsContext_withEmbeddedStruct() {
	t := cet.T()
	ctx := context.Background()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "phone_number", "age"}).
			FromCSVString("111 Test Addr,Test1,111-111-1111,20\n211 Test Addr,Test2,222-222-2222,30"))

	db := newMockDb(mDb)
	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []TestComposedCrudActionItem
	assert.NoError(t, e.ScanStructsContext(ctx, &composed))
	assert.Equal(t, []TestComposedCrudActionItem{
		{TestCrudActionItem: TestCrudActionItem{Address: "111 Test Addr", Name: "Test1"}, PhoneNumber: "111-111-1111", Age: 20},
		{TestCrudActionItem: TestCrudActionItem{Address: "211 Test Addr", Name: "Test2"}, PhoneNumber: "222-222-2222", Age: 30},
	}, composed)
}

func (cet *crudExecTest) TestScanStructsContext_pointersWithEmbeddedStruct() {
	t := cet.T()
	ctx := context.Background()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "phone_number", "age"}).
			FromCSVString("111 Test Addr,Test1,111-111-1111,20\n211 Test Addr,Test2,222-222-2222,30"))

	db := newMockDb(mDb)
	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []*TestComposedCrudActionItem
	assert.NoError(t, e.ScanStructsContext(ctx, &composed))
	assert.Equal(t, []*TestComposedCrudActionItem{
		{TestCrudActionItem: TestCrudActionItem{Address: "111 Test Addr", Name: "Test1"}, PhoneNumber: "111-111-1111", Age: 20},
		{TestCrudActionItem: TestCrudActionItem{Address: "211 Test Addr", Name: "Test2"}, PhoneNumber: "222-222-2222", Age: 30},
	}, composed)
}

func (cet *crudExecTest) TestScanStructsContext_withEmbeddedStructPointer() {
	t := cet.T()
	ctx := context.Background()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "phone_number", "age"}).
			FromCSVString("111 Test Addr,Test1,111-111-1111,20\n211 Test Addr,Test2,222-222-2222,30"))

	db := newMockDb(mDb)
	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []TestEmbeddedPtrCrudActionItem
	assert.NoError(t, e.ScanStructsContext(ctx, &composed))
	assert.Equal(t, []TestEmbeddedPtrCrudActionItem{
		{TestCrudActionItem: &TestCrudActionItem{Address: "111 Test Addr", Name: "Test1"}, PhoneNumber: "111-111-1111", Age: 20},
		{TestCrudActionItem: &TestCrudActionItem{Address: "211 Test Addr", Name: "Test2"}, PhoneNumber: "222-222-2222", Age: 30},
	}, composed)
}

func (cet *crudExecTest) TestScanStructsContext_pointersWithEmbeddedStructPointer() {
	t := cet.T()
	ctx := context.Background()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "phone_number", "age"}).
			FromCSVString("111 Test Addr,Test1,111-111-1111,20\n211 Test Addr,Test2,222-222-2222,30"))

	db := newMockDb(mDb)
	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []*TestEmbeddedPtrCrudActionItem
	assert.NoError(t, e.ScanStructsContext(ctx, &composed))
	assert.Equal(t, []*TestEmbeddedPtrCrudActionItem{
		{TestCrudActionItem: &TestCrudActionItem{Address: "111 Test Addr", Name: "Test1"}, PhoneNumber: "111-111-1111", Age: 20},
		{TestCrudActionItem: &TestCrudActionItem{Address: "211 Test Addr", Name: "Test2"}, PhoneNumber: "222-222-2222", Age: 30},
	}, composed)
}

func (cet *crudExecTest) TestScanStructsContext_badValue() {
	t := cet.T()
	ctx := context.Background()
	mDb, _, err := sqlmock.New()
	assert.NoError(t, err)

	db := newMockDb(mDb)
	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []TestCrudActionItem
	assert.Equal(t, errUnsupportedScanStructsType, e.ScanStructsContext(ctx, items))
	assert.Equal(t, errUnsupportedScanStructsType, e.ScanStructsContext(ctx, &TestCrudActionItem{}))
}

func (cet *crudExecTest) TestScanStructsContext_queryError() {
	t := cet.T()
	ctx := context.Background()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WillReturnError(fmt.Errorf("queryExecutor error"))

	db := newMockDb(mDb)
	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []TestCrudActionItem
	assert.EqualError(t, e.ScanStructsContext(ctx, &items), "queryExecutor error")
}

func (cet *crudExecTest) TestScanStruct() {
	t := cet.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WillReturnError(fmt.Errorf("queryExecutor error"))

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).FromCSVString("111 Test Addr,Test1"))

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "phone_number", "age"}).
			FromCSVString("111 Test Addr,Test1,111-111-1111,20"))

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "phone_number", "age"}).
			FromCSVString("111 Test Addr,Test1,111-111-1111,20"))

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).FromCSVString("111 Test Addr,Test1"))

	db := newMockDb(mDb)
	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var slicePtr []TestCrudActionItem
	var item TestCrudActionItem
	found, err := e.ScanStruct(item)
	assert.Equal(t, errUnsupportedScanStructType, err)
	assert.False(t, found)
	found, err = e.ScanStruct(&slicePtr)
	assert.Equal(t, errUnsupportedScanStructType, err)
	assert.False(t, found)
	found, err = e.ScanStruct(&item)
	assert.EqualError(t, err, "queryExecutor error")
	assert.False(t, found)

	found, err = e.ScanStruct(&item)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, item.Address, "111 Test Addr")
	assert.Equal(t, item.Name, "Test1")

	var composed TestComposedCrudActionItem
	found, err = e.ScanStruct(&composed)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, composed.Address, "111 Test Addr")
	assert.Equal(t, composed.Name, "Test1")
	assert.Equal(t, composed.PhoneNumber, "111-111-1111")
	assert.Equal(t, composed.Age, int64(20))

	var embeddedPtr TestEmbeddedPtrCrudActionItem
	found, err = e.ScanStruct(&embeddedPtr)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, embeddedPtr.Address, "111 Test Addr")
	assert.Equal(t, embeddedPtr.Name, "Test1")
	assert.Equal(t, embeddedPtr.PhoneNumber, "111-111-1111")
	assert.Equal(t, embeddedPtr.Age, int64(20))

	var noTag TestCrudActionNoTagsItem
	found, err = e.ScanStruct(&noTag)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, noTag.Address, "111 Test Addr")
	assert.Equal(t, noTag.Name, "Test1")
}

func (cet *crudExecTest) TestScanVals() {
	t := cet.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)

	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WillReturnError(fmt.Errorf("queryExecutor error"))

	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("1\n2"))

	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("1\n2"))

	db := newMockDb(mDb)
	e := newQueryExecutor(db, nil, `SELECT "id" FROM "items"`)

	var id int64
	var ids []int64
	assert.Equal(t, errUnsupportedScanValsType, e.ScanVals(ids))
	assert.Equal(t, errUnsupportedScanValsType, e.ScanVals(&id))
	assert.EqualError(t, e.ScanVals(&ids), "queryExecutor error")

	assert.NoError(t, e.ScanVals(&ids))
	assert.Equal(t, ids, []int64{1, 2})

	var pointers []*int64
	assert.NoError(t, e.ScanVals(&pointers))
	assert.Len(t, pointers, 2)
	assert.Equal(t, *pointers[0], int64(1))
	assert.Equal(t, *pointers[1], int64(2))
}

func (cet *crudExecTest) TestScanVal() {
	t := cet.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)

	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WillReturnError(fmt.Errorf("queryExecutor error"))

	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("1"))

	db := newMockDb(mDb)
	e := newQueryExecutor(db, nil, `SELECT "id" FROM "items"`)

	var id int64
	var ids []int64
	found, err := e.ScanVal(id)
	assert.Equal(t, errScanValPointer, err)
	assert.False(t, found)
	found, err = e.ScanVal(&ids)
	assert.Equal(t, errScanValNonSlice, err)
	assert.False(t, found)
	found, err = e.ScanVal(&id)
	assert.EqualError(t, err, "queryExecutor error")
	assert.False(t, found)

	var ptrID int64
	found, err = e.ScanVal(&ptrID)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, ptrID, int64(1))
}

func TestCrudExecSuite(t *testing.T) {
	suite.Run(t, new(crudExecTest))
}
