package exec

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/suite"
)

var (
	testAddr1                  = "111 Test Addr"
	testAddr2                  = "211 Test Addr"
	testName1                  = "Test1"
	testName2                  = "Test2"
	testPhone1                 = "111-111-1111"
	testPhone2                 = "222-222-2222"
	testAge1             int64 = 10
	testAge2             int64 = 20
	testByteSliceContent       = "byte slice result"
)

type crudExecTest struct {
	suite.Suite
}

func (cet *crudExecTest) TestWithError() {
	type StructWithTags struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	ctx := context.Background()
	db, _, err := sqlmock.New()
	cet.NoError(err)
	expectedErr := fmt.Errorf("crud exec error")
	e := newQueryExecutor(db, expectedErr, `SELECT * FROM "items"`)
	var items []StructWithTags
	cet.EqualError(e.ScanStructs(&items), expectedErr.Error())
	cet.EqualError(e.ScanStructsContext(ctx, &items), expectedErr.Error())
	found, err := e.ScanStruct(&StructWithTags{})
	cet.EqualError(err, expectedErr.Error())
	cet.False(found)
	found, err = e.ScanStructContext(ctx, &StructWithTags{})
	cet.EqualError(err, expectedErr.Error())
	cet.False(found)
	var vals []string
	cet.EqualError(e.ScanVals(&vals), expectedErr.Error())
	cet.EqualError(e.ScanValsContext(ctx, &vals), expectedErr.Error())
	var val string
	found, err = e.ScanVal(&val)
	cet.EqualError(err, expectedErr.Error())
	cet.False(found)
	found, err = e.ScanValContext(ctx, &val)
	cet.EqualError(err, expectedErr.Error())
	cet.False(found)
}

func (cet *crudExecTest) TestScanStructs_withTaggedFields() {
	type StructWithTags struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	db, mock, err := sqlmock.New()
	cet.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			AddRow(testAddr1, testName1).
			AddRow(testAddr2, testName2),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []StructWithTags
	cet.NoError(e.ScanStructs(&items))
	cet.Equal([]StructWithTags{
		{Address: testAddr1, Name: testName1},
		{Address: testAddr2, Name: testName2},
	}, items)
}

func (cet *crudExecTest) TestScanStructs_withUntaggedFields() {
	type StructWithNoTags struct {
		Address string
		Name    string
	}
	db, mock, err := sqlmock.New()
	cet.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			AddRow(testAddr1, testName1).
			AddRow(testAddr2, testName2))

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []StructWithNoTags
	cet.NoError(e.ScanStructs(&items))
	cet.Equal([]StructWithNoTags{
		{Address: testAddr1, Name: testName1},
		{Address: testAddr2, Name: testName2},
	}, items)
}

func (cet *crudExecTest) TestScanStructs_withPointerFields() {
	type StructWithPointerFields struct {
		Str   *string
		Time  *time.Time
		Bool  *bool
		Int   *int64
		Float *float64
	}
	db, mock, err := sqlmock.New()
	cet.NoError(err)
	now := time.Now()
	str1, str2 := "str1", "str2"
	t := true
	var i1, i2 int64 = 1, 2
	var f1, f2 = 1.1, 2.1
	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"str", "time", "bool", "int", "float"}).
			AddRow(str1, now, true, i1, f1).
			AddRow(str2, now, true, i2, f2).
			AddRow(nil, nil, nil, nil, nil),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []StructWithPointerFields
	cet.NoError(e.ScanStructs(&items))
	cet.Equal([]StructWithPointerFields{
		{Str: &str1, Time: &now, Bool: &t, Int: &i1, Float: &f1},
		{Str: &str2, Time: &now, Bool: &t, Int: &i2, Float: &f2},
		{},
	}, items)
}

func (cet *crudExecTest) TestScanStructs_withPrivateFields() {
	type StructWithPrivateTags struct {
		private string // nolint:structcheck,unused
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	db, mock, err := sqlmock.New()
	cet.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			AddRow(testAddr1, testName1).
			AddRow(testAddr2, testName2),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []StructWithPrivateTags
	cet.NoError(e.ScanStructs(&items))
	cet.Equal([]StructWithPrivateTags{
		{Address: testAddr1, Name: testName1},
		{Address: testAddr2, Name: testName2},
	}, items)
}

func (cet *crudExecTest) TestScanStructs_pointers() {
	type StructWithTags struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	db, mock, err := sqlmock.New()
	cet.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			AddRow(testAddr1, testName1).
			AddRow(testAddr2, testName2),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []*StructWithTags
	cet.NoError(e.ScanStructs(&items))
	cet.Equal([]*StructWithTags{
		{Address: testAddr1, Name: testName1},
		{Address: testAddr2, Name: testName2},
	}, items)
}

func (cet *crudExecTest) TestScanStructs_withIgnoredEmbeddedStruct() {
	type StructWithTags struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	type ComposedIgnoredStruct struct {
		StructWithTags `db:"-"`
		PhoneNumber    string `db:"phone_number"`
		Age            int64  `db:"age"`
	}

	db, mock, err := sqlmock.New()
	cet.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"phone_number", "age"}).
			AddRow(testPhone1, testAge1).AddRow(testPhone2, testAge2),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []ComposedIgnoredStruct
	cet.NoError(e.ScanStructs(&composed))
	cet.Equal([]ComposedIgnoredStruct{
		{StructWithTags: StructWithTags{}, PhoneNumber: testPhone1, Age: testAge1},
		{StructWithTags: StructWithTags{}, PhoneNumber: testPhone2, Age: testAge2},
	}, composed)
}

func (cet *crudExecTest) TestScanStructs_withEmbeddedStruct() {
	type StructWithTags struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	type ComposedStruct struct {
		StructWithTags
		PhoneNumber string `db:"phone_number"`
		Age         int64  `db:"age"`
	}
	db, mock, err := sqlmock.New()
	cet.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "phone_number", "age"}).
			AddRow(testAddr1, testName1, testPhone1, testAge1).
			AddRow(testAddr2, testName2, testPhone2, testAge2),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []ComposedStruct
	cet.NoError(e.ScanStructs(&composed))
	cet.Equal([]ComposedStruct{
		{StructWithTags: StructWithTags{Address: testAddr1, Name: testName1}, PhoneNumber: testPhone1, Age: testAge1},
		{StructWithTags: StructWithTags{Address: testAddr2, Name: testName2}, PhoneNumber: testPhone2, Age: testAge2},
	}, composed)
}

func (cet *crudExecTest) TestScanStructs_pointersWithEmbeddedStruct() {
	type StructWithTags struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	type ComposedStruct struct {
		StructWithTags
		PhoneNumber string `db:"phone_number"`
		Age         int64  `db:"age"`
	}

	db, mock, err := sqlmock.New()
	cet.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "phone_number", "age"}).
			AddRow(testAddr1, testName1, testPhone1, testAge1).
			AddRow(testAddr2, testName2, testPhone2, testAge2),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []*ComposedStruct
	cet.NoError(e.ScanStructs(&composed))
	cet.Equal([]*ComposedStruct{
		{StructWithTags: StructWithTags{Address: testAddr1, Name: testName1}, PhoneNumber: testPhone1, Age: testAge1},
		{StructWithTags: StructWithTags{Address: testAddr2, Name: testName2}, PhoneNumber: testPhone2, Age: testAge2},
	}, composed)
}

func (cet *crudExecTest) TestScanStructs_pointersWithEmbeddedStructDuplicateFields() {
	type StructWithTags struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	type ComposedStructWithDuplicateFields struct {
		StructWithTags
		Address string `db:"other_address"`
		Name    string `db:"other_name"`
	}

	var otherAddr1, otherAddr2 = "111 Test Addr Other", "211 Test Addr Other"
	var otherName1, otherName2 = "Test1 Other", "Test2 Other"

	db, mock, err := sqlmock.New()
	cet.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "other_address", "other_name"}).
			AddRow(testAddr1, testName1, otherAddr1, otherName1).
			AddRow(testAddr2, testName2, otherAddr2, otherName2),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []*ComposedStructWithDuplicateFields
	cet.NoError(e.ScanStructs(&composed))
	cet.Equal([]*ComposedStructWithDuplicateFields{
		{
			StructWithTags: StructWithTags{Address: testAddr1, Name: testName1},
			Address:        otherAddr1,
			Name:           otherName1,
		},
		{
			StructWithTags: StructWithTags{Address: testAddr2, Name: testName2},
			Address:        otherAddr2,
			Name:           otherName2,
		},
	}, composed)
}

func (cet *crudExecTest) TestScanStructs_pointersWithEmbeddedPointerDuplicateFields() {
	type StructWithTags struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	type ComposedWithWithPointerWithDuplicateFields struct {
		*StructWithTags
		Address string `db:"other_address"`
		Name    string `db:"other_name"`
	}

	var otherAddr1, otherAddr2 = "111 Test Addr Other", "211 Test Addr Other"
	var otherName1, otherName2 = "Test1 Other", "Test2 Other"

	db, mock, err := sqlmock.New()
	cet.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "other_address", "other_name"}).
			AddRow(testAddr1, testName1, otherAddr1, otherName1).
			AddRow(testAddr2, testName2, otherAddr2, otherName2),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []*ComposedWithWithPointerWithDuplicateFields
	cet.NoError(e.ScanStructs(&composed))
	cet.Equal([]*ComposedWithWithPointerWithDuplicateFields{
		{
			StructWithTags: &StructWithTags{Address: testAddr1, Name: testName1},
			Address:        otherAddr1,
			Name:           otherName1,
		},
		{
			StructWithTags: &StructWithTags{Address: testAddr2, Name: testName2},
			Address:        otherAddr2,
			Name:           otherName2,
		},
	}, composed)
}

func (cet *crudExecTest) TestScanStructs_withIgnoredEmbeddedPointerStruct() {
	type StructWithTags struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	type ComposedIgnoredPointerStruct struct {
		*StructWithTags `db:"-"`
		PhoneNumber     string `db:"phone_number"`
		Age             int64  `db:"age"`
	}

	db, mock, err := sqlmock.New()
	cet.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"phone_number", "age"}).
			AddRow(testPhone1, testAge1).
			AddRow(testPhone2, testAge2),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []ComposedIgnoredPointerStruct
	cet.NoError(e.ScanStructs(&composed))
	cet.Equal([]ComposedIgnoredPointerStruct{
		{StructWithTags: &StructWithTags{}, PhoneNumber: testPhone1, Age: testAge1},
		{StructWithTags: &StructWithTags{}, PhoneNumber: testPhone2, Age: testAge2},
	}, composed)
}

func (cet *crudExecTest) TestScanStructs_withEmbeddedStructPointer() {
	type StructWithTags struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	type ComposedWithPointerStruct struct {
		*StructWithTags
		PhoneNumber string `db:"phone_number"`
		Age         int64  `db:"age"`
	}

	db, mock, err := sqlmock.New()
	cet.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "phone_number", "age"}).
			AddRow(testAddr1, testName1, testPhone1, testAge1).
			AddRow(testAddr2, testName2, testPhone2, testAge2),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []ComposedWithPointerStruct
	cet.NoError(e.ScanStructs(&composed))
	cet.Equal([]ComposedWithPointerStruct{
		{StructWithTags: &StructWithTags{Address: testAddr1, Name: testName1}, PhoneNumber: testPhone1, Age: testAge1},
		{StructWithTags: &StructWithTags{Address: testAddr2, Name: testName2}, PhoneNumber: testPhone2, Age: testAge2},
	}, composed)
}

func (cet *crudExecTest) TestScanStructs_pointersWithEmbeddedStructPointer() {
	type StructWithTags struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	type ComposedWithPointerStruct struct {
		*StructWithTags
		PhoneNumber string `db:"phone_number"`
		Age         int64  `db:"age"`
	}
	db, mock, err := sqlmock.New()
	cet.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "phone_number", "age"}).
			AddRow(testAddr1, testName1, testPhone1, testAge1).
			AddRow(testAddr2, testName2, testPhone2, testAge2),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []*ComposedWithPointerStruct
	cet.NoError(e.ScanStructs(&composed))
	cet.Equal([]*ComposedWithPointerStruct{
		{StructWithTags: &StructWithTags{Address: testAddr1, Name: testName1}, PhoneNumber: testPhone1, Age: testAge1},
		{StructWithTags: &StructWithTags{Address: testAddr2, Name: testName2}, PhoneNumber: testPhone2, Age: testAge2},
	}, composed)
}

func (cet *crudExecTest) TestScanStructs_badValue() {
	type StructWithTags struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	db, _, err := sqlmock.New()
	cet.NoError(err)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []StructWithTags
	cet.Equal(errUnsupportedScanStructsType, e.ScanStructs(items))
	cet.Equal(errUnsupportedScanStructsType, e.ScanStructs(&StructWithTags{}))
}

func (cet *crudExecTest) TestScanStructs_queryError() {
	type StructWithTags struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	db, mock, err := sqlmock.New()
	cet.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WillReturnError(fmt.Errorf("queryExecutor error"))

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []StructWithTags
	cet.EqualError(e.ScanStructs(&items), "queryExecutor error")
}

func (cet *crudExecTest) TestScanStructsContext_withTaggedFields() {
	type StructWithTags struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	cet.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			AddRow(testAddr1, testName1).
			AddRow(testAddr2, testName2),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []StructWithTags
	cet.NoError(e.ScanStructsContext(ctx, &items))
	cet.Equal([]StructWithTags{
		{Address: testAddr1, Name: testName1},
		{Address: testAddr2, Name: testName2},
	}, items)
}

func (cet *crudExecTest) TestScanStructsContext_withUntaggedFields() {
	type StructWithNoTags struct {
		Address string
		Name    string
	}

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	cet.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			AddRow(testAddr1, testName1).
			AddRow(testAddr2, testName2),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []StructWithNoTags
	cet.NoError(e.ScanStructsContext(ctx, &items))
	cet.Equal([]StructWithNoTags{
		{Address: testAddr1, Name: testName1},
		{Address: testAddr2, Name: testName2},
	}, items)
}

func (cet *crudExecTest) TestScanStructsContext_withPointerFields() {
	type StructWithPointerFields struct {
		Address *string
		Name    *string
	}
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	cet.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			AddRow(testAddr1, testName1).
			AddRow(testAddr2, testName2),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []StructWithPointerFields
	cet.NoError(e.ScanStructsContext(ctx, &items))
	cet.Equal([]StructWithPointerFields{
		{Address: &testAddr1, Name: &testName1},
		{Address: &testAddr2, Name: &testName2},
	}, items)
}

func (cet *crudExecTest) TestScanStructsContext_pointers() {
	type StructWithTags struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	cet.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			AddRow(testAddr1, testName1).
			AddRow(testAddr2, testName2),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []*StructWithTags
	cet.NoError(e.ScanStructsContext(ctx, &items))
	cet.Equal([]*StructWithTags{
		{Address: testAddr1, Name: testName1},
		{Address: testAddr2, Name: testName2},
	}, items)
}

func (cet *crudExecTest) TestScanStructsContext_withEmbeddedStruct() {
	type StructWithTags struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	type ComposedStruct struct {
		StructWithTags
		PhoneNumber string `db:"phone_number"`
		Age         int64  `db:"age"`
	}
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	cet.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "phone_number", "age"}).
			AddRow(testAddr1, testName1, testPhone1, testAge1).
			AddRow(testAddr2, testName2, testPhone2, testAge2),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []ComposedStruct
	cet.NoError(e.ScanStructsContext(ctx, &composed))
	cet.Equal([]ComposedStruct{
		{StructWithTags: StructWithTags{Address: testAddr1, Name: testName1}, PhoneNumber: testPhone1, Age: testAge1},
		{StructWithTags: StructWithTags{Address: testAddr2, Name: testName2}, PhoneNumber: testPhone2, Age: testAge2},
	}, composed)
}

func (cet *crudExecTest) TestScanStructsContext_withIgnoredEmbeddedStruct() {
	type StructWithTags struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	type ComposedIgnoredStruct struct {
		StructWithTags `db:"-"`
		PhoneNumber    string `db:"phone_number"`
		Age            int64  `db:"age"`
	}

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	cet.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"phone_number", "age"}).
			AddRow(testPhone1, testAge1).
			AddRow(testPhone2, testAge2),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []ComposedIgnoredStruct
	cet.NoError(e.ScanStructsContext(ctx, &composed))
	cet.Equal([]ComposedIgnoredStruct{
		{StructWithTags: StructWithTags{}, PhoneNumber: testPhone1, Age: testAge1},
		{StructWithTags: StructWithTags{}, PhoneNumber: testPhone2, Age: testAge2},
	}, composed)
}

func (cet *crudExecTest) TestScanStructsContext_pointersWithEmbeddedStruct() {
	type StructWithTags struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	type ComposedStruct struct {
		StructWithTags
		PhoneNumber string `db:"phone_number"`
		Age         int64  `db:"age"`
	}
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	cet.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "phone_number", "age"}).
			AddRow(testAddr1, testName1, testPhone1, testAge1).
			AddRow(testAddr2, testName2, testPhone2, testAge2),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []*ComposedStruct
	cet.NoError(e.ScanStructsContext(ctx, &composed))
	cet.Equal([]*ComposedStruct{
		{StructWithTags: StructWithTags{Address: testAddr1, Name: testName1}, PhoneNumber: testPhone1, Age: testAge1},
		{StructWithTags: StructWithTags{Address: testAddr2, Name: testName2}, PhoneNumber: testPhone2, Age: testAge2},
	}, composed)
}

func (cet *crudExecTest) TestScanStructsContext_withEmbeddedStructPointer() {
	type StructWithTags struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	type ComposedWithPointerStruct struct {
		*StructWithTags
		PhoneNumber string `db:"phone_number"`
		Age         int64  `db:"age"`
	}

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	cet.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "phone_number", "age"}).
			AddRow(testAddr1, testName1, testPhone1, testAge1).
			AddRow(testAddr2, testName2, testPhone2, testAge2),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []ComposedWithPointerStruct
	cet.NoError(e.ScanStructsContext(ctx, &composed))
	cet.Equal([]ComposedWithPointerStruct{
		{StructWithTags: &StructWithTags{Address: testAddr1, Name: testName1}, PhoneNumber: testPhone1, Age: testAge1},
		{StructWithTags: &StructWithTags{Address: testAddr2, Name: testName2}, PhoneNumber: testPhone2, Age: testAge2},
	}, composed)
}

func (cet *crudExecTest) TestScanStructsContext_pointersWithEmbeddedStructPointer() {
	type StructWithTags struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	type ComposedWithPointerStruct struct {
		*StructWithTags
		PhoneNumber string `db:"phone_number"`
		Age         int64  `db:"age"`
	}

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	cet.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "phone_number", "age"}).
			AddRow(testAddr1, testName1, testPhone1, testAge1).
			AddRow(testAddr2, testName2, testPhone2, testAge2),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []*ComposedWithPointerStruct
	cet.NoError(e.ScanStructsContext(ctx, &composed))
	cet.Equal([]*ComposedWithPointerStruct{
		{StructWithTags: &StructWithTags{Address: testAddr1, Name: testName1}, PhoneNumber: testPhone1, Age: testAge1},
		{StructWithTags: &StructWithTags{Address: testAddr2, Name: testName2}, PhoneNumber: testPhone2, Age: testAge2},
	}, composed)
}

func (cet *crudExecTest) TestScanStructsContext_badValue() {
	type StructWithTags struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	ctx := context.Background()
	db, _, err := sqlmock.New()
	cet.NoError(err)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []StructWithTags
	cet.Equal(errUnsupportedScanStructsType, e.ScanStructsContext(ctx, items))
	cet.Equal(errUnsupportedScanStructsType, e.ScanStructsContext(ctx, &StructWithTags{}))
}

func (cet *crudExecTest) TestScanStructsContext_queryError() {
	type StructWithTags struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	cet.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WillReturnError(fmt.Errorf("queryExecutor error"))

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []StructWithTags
	cet.EqualError(e.ScanStructsContext(ctx, &items), "queryExecutor error")
}

func (cet *crudExecTest) TestScanStruct() {
	type StructWithNoTags struct {
		Address string
		Name    string
	}

	type StructWithTags struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	type ComposedStruct struct {
		StructWithTags
		PhoneNumber string `db:"phone_number"`
		Age         int64  `db:"age"`
	}
	type ComposedWithPointerStruct struct {
		*StructWithTags
		PhoneNumber string `db:"phone_number"`
		Age         int64  `db:"age"`
	}

	db, mock, err := sqlmock.New()
	cet.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WillReturnError(fmt.Errorf("queryExecutor error"))

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			AddRow(testAddr1, testName1),
		)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "phone_number", "age"}).
			AddRow(testAddr1, testName1, testPhone1, testAge1),
		)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "phone_number", "age"}).
			AddRow(testAddr1, testName1, testPhone1, testAge1),
		)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).AddRow(testAddr1, testName1))

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var slicePtr []StructWithTags
	var item StructWithTags
	found, err := e.ScanStruct(item)
	cet.Equal(errUnsupportedScanStructType, err)
	cet.False(found)
	found, err = e.ScanStruct(&slicePtr)
	cet.Equal(errUnsupportedScanStructType, err)
	cet.False(found)
	found, err = e.ScanStruct(&item)
	cet.EqualError(err, "queryExecutor error")
	cet.False(found)

	found, err = e.ScanStruct(&item)
	cet.NoError(err)
	cet.True(found)
	cet.Equal(StructWithTags{
		Address: testAddr1,
		Name:    testName1,
	}, item)

	var composed ComposedStruct
	found, err = e.ScanStruct(&composed)
	cet.NoError(err)
	cet.True(found)
	cet.Equal(ComposedStruct{
		StructWithTags: StructWithTags{Address: testAddr1, Name: testName1},
		PhoneNumber:    testPhone1,
		Age:            testAge1,
	}, composed)

	var embeddedPtr ComposedWithPointerStruct
	found, err = e.ScanStruct(&embeddedPtr)
	cet.NoError(err)
	cet.True(found)
	cet.Equal(ComposedWithPointerStruct{
		StructWithTags: &StructWithTags{
			Address: testAddr1,
			Name:    testName1,
		},
		PhoneNumber: testPhone1,
		Age:         testAge1,
	}, embeddedPtr)

	var noTag StructWithNoTags
	found, err = e.ScanStruct(&noTag)
	cet.NoError(err)
	cet.True(found)
	cet.Equal(StructWithNoTags{
		Address: testAddr1,
		Name:    testName1,
	}, noTag)
}

func (cet *crudExecTest) TestScanVals() {
	db, mock, err := sqlmock.New()
	cet.NoError(err)

	var id1, id2 int64 = 1, 2

	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WillReturnError(fmt.Errorf("queryExecutor error"))

	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(id1).AddRow(id2))

	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(id1).AddRow(id2))

	e := newQueryExecutor(db, nil, `SELECT "id" FROM "items"`)

	var id int64
	var ids []int64
	cet.Equal(errUnsupportedScanValsType, e.ScanVals(ids))
	cet.Equal(errUnsupportedScanValsType, e.ScanVals(&id))
	cet.EqualError(e.ScanVals(&ids), "queryExecutor error")

	cet.NoError(e.ScanVals(&ids))
	cet.Equal(ids, []int64{id1, id2})

	var pointers []*int64
	cet.NoError(e.ScanVals(&pointers))
	cet.Len(pointers, 2)
	cet.Equal(&id1, pointers[0])
	cet.Equal(&id2, pointers[1])
}

func (cet *crudExecTest) TestScanVal() {
	db, mock, err := sqlmock.New()
	cet.NoError(err)

	id1 := int64(1)
	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WillReturnError(fmt.Errorf("queryExecutor error"))

	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(id1))

	e := newQueryExecutor(db, nil, `SELECT "id" FROM "items"`)

	var id int64
	var ids []int64
	found, err := e.ScanVal(id)
	cet.Equal(errScanValPointer, err)
	cet.False(found)
	found, err = e.ScanVal(&ids)
	cet.Equal(errScanValNonSlice, err)
	cet.False(found)
	found, err = e.ScanVal(&id)
	cet.EqualError(err, "queryExecutor error")
	cet.False(found)

	var ptrID *int64
	found, err = e.ScanVal(&ptrID)
	cet.NoError(err)
	cet.True(found)
	cet.Equal(&id1, ptrID)
}

func (cet *crudExecTest) TestScanVal_withByteSlice() {
	db, mock, err := sqlmock.New()
	cet.NoError(err)

	mock.ExpectQuery(`SELECT "name" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow(testByteSliceContent))

	e := newQueryExecutor(db, nil, `SELECT "name" FROM "items"`)

	var bytes []byte
	found, err := e.ScanVal(bytes)
	cet.Equal(errScanValPointer, err)
	cet.False(found)

	found, err = e.ScanVal(&bytes)
	cet.NoError(err)
	cet.True(found)
	cet.Equal([]byte(testByteSliceContent), bytes)
}

func (cet *crudExecTest) TestScanVal_withRawBytes() {
	db, mock, err := sqlmock.New()
	cet.NoError(err)

	mock.ExpectQuery(`SELECT "name" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow(testByteSliceContent))

	e := newQueryExecutor(db, nil, `SELECT "name" FROM "items"`)

	var bytes sql.RawBytes
	found, err := e.ScanVal(bytes)
	cet.Equal(errScanValPointer, err)
	cet.False(found)

	found, err = e.ScanVal(&bytes)
	cet.NoError(err)
	cet.True(found)
	cet.Equal(sql.RawBytes(testByteSliceContent), bytes)
}

type JSONBoolArray []bool

func (b *JSONBoolArray) Scan(src interface{}) error {
	return json.Unmarshal(src.([]byte), b)
}

func (cet *crudExecTest) TestScanVal_withValuerSlice() {
	db, mock, err := sqlmock.New()
	cet.NoError(err)

	mock.ExpectQuery(`SELECT "bools" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"bools"}).FromCSVString(`"[true, false, true]"`))

	e := newQueryExecutor(db, nil, `SELECT "bools" FROM "items"`)

	var bools JSONBoolArray
	found, err := e.ScanVal(bools)
	cet.Equal(errScanValPointer, err)
	cet.False(found)

	found, err = e.ScanVal(&bools)
	cet.NoError(err)
	cet.True(found)
	cet.Equal(JSONBoolArray{true, false, true}, bools)
}

func TestCrudExecSuite(t *testing.T) {
	suite.Run(t, new(crudExecTest))
}
