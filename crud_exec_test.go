package goqu

import (
	"context"
	"fmt"
	"strings"
	"sync"
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

type crudExecTest struct {
	suite.Suite
}

func (me *crudExecTest) TestWithError() {
	t := me.T()
	ctx := context.Background()
	mDb, _, err := sqlmock.New()
	assert.NoError(t, err)
	db := New("db-mock", mDb)
	expectedErr := fmt.Errorf("crud exec error")
	exec := newCrudExec(db, expectedErr, `SELECT * FROM "items"`)
	var items []TestCrudActionItem
	assert.EqualError(t, exec.ScanStructs(&items), expectedErr.Error())
	assert.EqualError(t, exec.ScanStructsContext(ctx, &items), expectedErr.Error())
	found, err := exec.ScanStruct(&TestCrudActionItem{})
	assert.EqualError(t, err, expectedErr.Error())
	assert.False(t, found)
	found, err = exec.ScanStructContext(ctx, &TestCrudActionItem{})
	assert.EqualError(t, err, expectedErr.Error())
	assert.False(t, found)
	var vals []string
	assert.EqualError(t, exec.ScanVals(&vals), expectedErr.Error())
	assert.EqualError(t, exec.ScanValsContext(ctx, &vals), expectedErr.Error())
	var val string
	found, err = exec.ScanVal(&val)
	assert.EqualError(t, err, expectedErr.Error())
	assert.False(t, found)
	found, err = exec.ScanValContext(ctx, &val)
	assert.EqualError(t, err, expectedErr.Error())
	assert.False(t, found)
}

func (me *crudExecTest) TestScanStructs() {
	t := me.T()
	ctx := context.Background()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WillReturnError(fmt.Errorf("query error"))
	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WillReturnError(fmt.Errorf("query error"))

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))
	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "phone_number", "age"}).FromCSVString("111 Test Addr,Test1,111-111-1111,20\n211 Test Addr,Test2,222-222-2222,30"))
	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "phone_number", "age"}).FromCSVString("111 Test Addr,Test1,111-111-1111,20\n211 Test Addr,Test2,222-222-2222,30"))

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))
	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "phone_number", "age"}).FromCSVString("111 Test Addr,Test1,111-111-1111,20\n211 Test Addr,Test2,222-222-2222,30"))
	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "phone_number", "age"}).FromCSVString("111 Test Addr,Test1,111-111-1111,20\n211 Test Addr,Test2,222-222-2222,30"))

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "phone_number", "age"}).FromCSVString("111 Test Addr,Test1,111-111-1111,20\n211 Test Addr,Test2,222-222-2222,30"))
	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "phone_number", "age"}).FromCSVString("111 Test Addr,Test1,111-111-1111,20\n211 Test Addr,Test2,222-222-2222,30"))

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))
	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	db := New("db-mock", mDb)
	exec := newCrudExec(db, nil, `SELECT * FROM "items"`)

	var items []TestCrudActionItem
	assert.EqualError(t, exec.ScanStructs(items), "goqu: Type must be a pointer to a slice when calling ScanStructs")
	assert.EqualError(t, exec.ScanStructsContext(ctx, items), "goqu: Type must be a pointer to a slice when calling ScanStructs")
	assert.EqualError(t, exec.ScanStructs(&TestCrudActionItem{}), "goqu: Type must be a pointer to a slice when calling ScanStructs")
	assert.EqualError(t, exec.ScanStructsContext(ctx, &TestCrudActionItem{}), "goqu: Type must be a pointer to a slice when calling ScanStructs")
	assert.EqualError(t, exec.ScanStructs(&items), "query error")
	assert.EqualError(t, exec.ScanStructsContext(ctx, &items), "query error")

	assert.NoError(t, exec.ScanStructs(&items))
	assert.Len(t, items, 2)
	assert.Equal(t, items[0].Address, "111 Test Addr")
	assert.Equal(t, items[0].Name, "Test1")

	assert.Equal(t, items[1].Address, "211 Test Addr")
	assert.Equal(t, items[1].Name, "Test2")

	items = nil
	assert.NoError(t, exec.ScanStructsContext(ctx, &items))
	assert.Len(t, items, 2)
	assert.Equal(t, items[0].Address, "111 Test Addr")
	assert.Equal(t, items[0].Name, "Test1")

	assert.Equal(t, items[1].Address, "211 Test Addr")
	assert.Equal(t, items[1].Name, "Test2")

	var composed []TestComposedCrudActionItem
	assert.NoError(t, exec.ScanStructs(&composed))
	assert.Len(t, composed, 2)
	assert.Equal(t, composed[0].Address, "111 Test Addr")
	assert.Equal(t, composed[0].Name, "Test1")
	assert.Equal(t, composed[0].PhoneNumber, "111-111-1111")
	assert.Equal(t, composed[0].Age, int64(20))

	assert.Equal(t, composed[1].Address, "211 Test Addr")
	assert.Equal(t, composed[1].Name, "Test2")
	assert.Equal(t, composed[1].PhoneNumber, "222-222-2222")
	assert.Equal(t, composed[1].Age, int64(30))

	composed = nil
	assert.NoError(t, exec.ScanStructsContext(ctx, &composed))
	assert.Len(t, composed, 2)
	assert.Equal(t, composed[0].Address, "111 Test Addr")
	assert.Equal(t, composed[0].Name, "Test1")
	assert.Equal(t, composed[0].PhoneNumber, "111-111-1111")
	assert.Equal(t, composed[0].Age, int64(20))

	assert.Equal(t, composed[1].Address, "211 Test Addr")
	assert.Equal(t, composed[1].Name, "Test2")
	assert.Equal(t, composed[1].PhoneNumber, "222-222-2222")
	assert.Equal(t, composed[1].Age, int64(30))

	var pointers []*TestCrudActionItem
	assert.NoError(t, exec.ScanStructs(&pointers))
	assert.Len(t, pointers, 2)
	assert.Equal(t, pointers[0].Address, "111 Test Addr")
	assert.Equal(t, pointers[0].Name, "Test1")

	assert.Equal(t, pointers[1].Address, "211 Test Addr")
	assert.Equal(t, pointers[1].Name, "Test2")

	pointers = nil
	assert.NoError(t, exec.ScanStructsContext(ctx, &pointers))
	assert.Len(t, pointers, 2)
	assert.Equal(t, pointers[0].Address, "111 Test Addr")
	assert.Equal(t, pointers[0].Name, "Test1")

	assert.Equal(t, pointers[1].Address, "211 Test Addr")
	assert.Equal(t, pointers[1].Name, "Test2")

	var composedPointers []*TestComposedCrudActionItem
	assert.NoError(t, exec.ScanStructs(&composedPointers))
	assert.Len(t, composedPointers, 2)
	assert.Equal(t, composedPointers[0].Address, "111 Test Addr")
	assert.Equal(t, composedPointers[0].Name, "Test1")
	assert.Equal(t, composedPointers[0].PhoneNumber, "111-111-1111")
	assert.Equal(t, composedPointers[0].Age, int64(20))

	assert.Equal(t, composedPointers[1].Address, "211 Test Addr")
	assert.Equal(t, composedPointers[1].Name, "Test2")
	assert.Equal(t, composedPointers[1].PhoneNumber, "222-222-2222")
	assert.Equal(t, composedPointers[1].Age, int64(30))

	composedPointers = nil
	assert.NoError(t, exec.ScanStructsContext(ctx, &composedPointers))
	assert.Len(t, composedPointers, 2)
	assert.Equal(t, composedPointers[0].Address, "111 Test Addr")
	assert.Equal(t, composedPointers[0].Name, "Test1")
	assert.Equal(t, composedPointers[0].PhoneNumber, "111-111-1111")
	assert.Equal(t, composedPointers[0].Age, int64(20))

	assert.Equal(t, composedPointers[1].Address, "211 Test Addr")
	assert.Equal(t, composedPointers[1].Name, "Test2")
	assert.Equal(t, composedPointers[1].PhoneNumber, "222-222-2222")
	assert.Equal(t, composedPointers[1].Age, int64(30))

	var embeddedPtrs []*TestEmbeddedPtrCrudActionItem
	assert.NoError(t, exec.ScanStructs(&embeddedPtrs))
	assert.Len(t, embeddedPtrs, 2)
	assert.Equal(t, embeddedPtrs[0].Address, "111 Test Addr")
	assert.Equal(t, embeddedPtrs[0].Name, "Test1")
	assert.Equal(t, embeddedPtrs[0].PhoneNumber, "111-111-1111")
	assert.Equal(t, embeddedPtrs[0].Age, int64(20))

	assert.Equal(t, embeddedPtrs[1].Address, "211 Test Addr")
	assert.Equal(t, embeddedPtrs[1].Name, "Test2")
	assert.Equal(t, embeddedPtrs[1].PhoneNumber, "222-222-2222")
	assert.Equal(t, embeddedPtrs[1].Age, int64(30))

	embeddedPtrs = nil
	assert.NoError(t, exec.ScanStructsContext(ctx, &embeddedPtrs))
	assert.Len(t, embeddedPtrs, 2)
	assert.Equal(t, embeddedPtrs[0].Address, "111 Test Addr")
	assert.Equal(t, embeddedPtrs[0].Name, "Test1")
	assert.Equal(t, embeddedPtrs[0].PhoneNumber, "111-111-1111")
	assert.Equal(t, embeddedPtrs[0].Age, int64(20))

	assert.Equal(t, embeddedPtrs[1].Address, "211 Test Addr")
	assert.Equal(t, embeddedPtrs[1].Name, "Test2")
	assert.Equal(t, embeddedPtrs[1].PhoneNumber, "222-222-2222")
	assert.Equal(t, embeddedPtrs[1].Age, int64(30))

	var noTags []TestCrudActionNoTagsItem
	assert.NoError(t, exec.ScanStructs(&noTags))
	assert.Len(t, noTags, 2)
	assert.Equal(t, noTags[0].Address, "111 Test Addr")
	assert.Equal(t, noTags[0].Name, "Test1")

	assert.Equal(t, noTags[1].Address, "211 Test Addr")
	assert.Equal(t, noTags[1].Name, "Test2")

	noTags = nil
	assert.NoError(t, exec.ScanStructsContext(ctx, &noTags))
	assert.Len(t, noTags, 2)
	assert.Equal(t, noTags[0].Address, "111 Test Addr")
	assert.Equal(t, noTags[0].Name, "Test1")

	assert.Equal(t, noTags[1].Address, "211 Test Addr")
	assert.Equal(t, noTags[1].Name, "Test2")
}

func (me *crudExecTest) TestColumnRename() {
	t := me.T()
	// different key names are used each time to circumvent the caching that happens
	// it seems like a solid assumption that when people use this feature,
	// they would simply set a renaming function once at startup,
	// and not change between requests like this
	lowerAnon := struct {
		FirstLower string
		LastLower  string
	}{}
	lowerColumnMap, lowerErr := getColumnMap(&lowerAnon)
	assert.NoError(t, lowerErr)

	var lowerKeys []string
	for key := range lowerColumnMap {
		lowerKeys = append(lowerKeys, key)
	}
	assert.Contains(t, lowerKeys, "firstlower")
	assert.Contains(t, lowerKeys, "lastlower")

	// changing rename function
	SetColumnRenameFunction(strings.ToUpper)

	upperAnon := struct {
		FirstUpper string
		LastUpper  string
	}{}
	upperColumnMap, upperErr := getColumnMap(&upperAnon)
	assert.NoError(t, upperErr)

	var upperKeys []string
	for key := range upperColumnMap {
		upperKeys = append(upperKeys, key)
	}
	assert.Contains(t, upperKeys, "FIRSTUPPER")
	assert.Contains(t, upperKeys, "LASTUPPER")

	SetColumnRenameFunction(defaultColumnRenameFunction)
}

func (me *crudExecTest) TestScanStruct() {
	t := me.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WillReturnError(fmt.Errorf("query error"))

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).FromCSVString("111 Test Addr,Test1"))

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "phone_number", "age"}).FromCSVString("111 Test Addr,Test1,111-111-1111,20"))

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "phone_number", "age"}).FromCSVString("111 Test Addr,Test1,111-111-1111,20"))

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).FromCSVString("111 Test Addr,Test1"))

	db := New("db-mock", mDb)
	exec := newCrudExec(db, nil, `SELECT * FROM "items"`)

	var slicePtr []TestCrudActionItem
	var item TestCrudActionItem
	found, err := exec.ScanStruct(item)
	assert.EqualError(t, err, "goqu: Type must be a pointer to a struct when calling ScanStruct")
	assert.False(t, found)
	found, err = exec.ScanStruct(&slicePtr)
	assert.EqualError(t, err, "goqu: Type must be a pointer to a struct when calling ScanStruct")
	assert.False(t, found)
	found, err = exec.ScanStruct(&item)
	assert.EqualError(t, err, "query error")
	assert.False(t, found)

	found, err = exec.ScanStruct(&item)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, item.Address, "111 Test Addr")
	assert.Equal(t, item.Name, "Test1")

	var composed TestComposedCrudActionItem
	found, err = exec.ScanStruct(&composed)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, composed.Address, "111 Test Addr")
	assert.Equal(t, composed.Name, "Test1")
	assert.Equal(t, composed.PhoneNumber, "111-111-1111")
	assert.Equal(t, composed.Age, int64(20))

	var embeddedPtr TestEmbeddedPtrCrudActionItem
	found, err = exec.ScanStruct(&embeddedPtr)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, embeddedPtr.Address, "111 Test Addr")
	assert.Equal(t, embeddedPtr.Name, "Test1")
	assert.Equal(t, embeddedPtr.PhoneNumber, "111-111-1111")
	assert.Equal(t, embeddedPtr.Age, int64(20))

	var noTag TestCrudActionNoTagsItem
	found, err = exec.ScanStruct(&noTag)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, noTag.Address, "111 Test Addr")
	assert.Equal(t, noTag.Name, "Test1")
}

func (me *crudExecTest) TestScanVals() {
	t := me.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)

	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WillReturnError(fmt.Errorf("query error"))

	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("1\n2"))

	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("1\n2"))

	db := New("db-mock", mDb)
	exec := newCrudExec(db, nil, `SELECT "id" FROM "items"`)

	var id int64
	var ids []int64
	assert.EqualError(t, exec.ScanVals(ids), "goqu: Type must be a pointer to a slice when calling ScanVals")
	assert.EqualError(t, exec.ScanVals(&id), "goqu: Type must be a pointer to a slice when calling ScanVals")
	assert.EqualError(t, exec.ScanVals(&ids), "query error")

	assert.NoError(t, exec.ScanVals(&ids))
	assert.Equal(t, ids, []int64{1, 2})

	var pointers []*int64
	assert.NoError(t, exec.ScanVals(&pointers))
	assert.Len(t, pointers, 2)
	assert.Equal(t, *pointers[0], int64(1))
	assert.Equal(t, *pointers[1], int64(2))
}

func (me *crudExecTest) TestScanVal() {
	t := me.T()
	mDb, mock, err := sqlmock.New()
	assert.NoError(t, err)

	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WillReturnError(fmt.Errorf("query error"))

	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("1"))

	db := New("db-mock", mDb)
	exec := newCrudExec(db, nil, `SELECT "id" FROM "items"`)

	var id int64
	var ids []int64
	found, err := exec.ScanVal(id)
	assert.EqualError(t, err, "goqu: Type must be a pointer when calling ScanVal")
	assert.False(t, found)
	found, err = exec.ScanVal(&ids)
	assert.EqualError(t, err, "goqu: Cannot scan into a slice when calling ScanVal")
	assert.False(t, found)
	found, err = exec.ScanVal(&id)
	assert.EqualError(t, err, "query error")
	assert.False(t, found)

	var ptrId int64
	found, err = exec.ScanVal(&ptrId)
	assert.NoError(t, err)
	assert.Equal(t, ptrId, int64(1))
}

func (me *crudExecTest) TestParallelGetColumnMap() {
	t := me.T()

	type item struct {
		id   uint
		name string
	}

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		i := item{}
		m, err := getColumnMap(i)
		assert.NoError(t, err)
		assert.NotNil(t, m)
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		i := item{}
		m, err := getColumnMap(i)
		assert.NoError(t, err)
		assert.NotNil(t, m)
		wg.Done()
	}()

	wg.Wait()
}

func TestCrudExecSuite(t *testing.T) {
	suite.Run(t, new(crudExecTest))
}
