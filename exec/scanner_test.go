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
	testAddr1 = "111 Test Addr"
	testAddr2 = "211 Test Addr"
	testName1 = "Test1"
	testName2 = "Test2"
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
			FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []StructWithTags
	cet.NoError(e.ScanStructs(&items))
	cet.Equal([]StructWithTags{
		{Address: "111 Test Addr", Name: "Test1"},
		{Address: "211 Test Addr", Name: "Test2"},
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
			FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []StructWithNoTags
	cet.NoError(e.ScanStructs(&items))
	cet.Equal([]StructWithNoTags{
		{Address: "111 Test Addr", Name: "Test1"},
		{Address: "211 Test Addr", Name: "Test2"},
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
	var f1, f2 float64 = 1.1, 2.1
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
			FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

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
			FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []*StructWithTags
	cet.NoError(e.ScanStructs(&items))
	cet.Equal([]*StructWithTags{
		{Address: "111 Test Addr", Name: "Test1"},
		{Address: "211 Test Addr", Name: "Test2"},
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
			FromCSVString("111-111-1111,20\n222-222-2222,30"))

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []ComposedIgnoredStruct
	cet.NoError(e.ScanStructs(&composed))
	cet.Equal([]ComposedIgnoredStruct{
		{StructWithTags: StructWithTags{}, PhoneNumber: "111-111-1111", Age: 20},
		{StructWithTags: StructWithTags{}, PhoneNumber: "222-222-2222", Age: 30},
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
			FromCSVString("111 Test Addr,Test1,111-111-1111,20\n211 Test Addr,Test2,222-222-2222,30"))

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []ComposedStruct
	cet.NoError(e.ScanStructs(&composed))
	cet.Equal([]ComposedStruct{
		{StructWithTags: StructWithTags{Address: "111 Test Addr", Name: "Test1"}, PhoneNumber: "111-111-1111", Age: 20},
		{StructWithTags: StructWithTags{Address: "211 Test Addr", Name: "Test2"}, PhoneNumber: "222-222-2222", Age: 30},
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
			FromCSVString("111 Test Addr,Test1,111-111-1111,20\n211 Test Addr,Test2,222-222-2222,30"))

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []*ComposedStruct
	cet.NoError(e.ScanStructs(&composed))
	cet.Equal([]*ComposedStruct{
		{StructWithTags: StructWithTags{Address: "111 Test Addr", Name: "Test1"}, PhoneNumber: "111-111-1111", Age: 20},
		{StructWithTags: StructWithTags{Address: "211 Test Addr", Name: "Test2"}, PhoneNumber: "222-222-2222", Age: 30},
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

	db, mock, err := sqlmock.New()
	cet.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "other_address", "other_name"}).
			FromCSVString("111 Test Addr,Test1,111 Test Addr Other,Test1 Other\n211 Test Addr,Test2,211 Test Addr Other,Test2 Other"))

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []*ComposedStructWithDuplicateFields
	cet.NoError(e.ScanStructs(&composed))
	cet.Equal([]*ComposedStructWithDuplicateFields{
		{
			StructWithTags: StructWithTags{Address: "111 Test Addr", Name: "Test1"},
			Address:        "111 Test Addr Other",
			Name:           "Test1 Other",
		},
		{
			StructWithTags: StructWithTags{Address: "211 Test Addr", Name: "Test2"},
			Address:        "211 Test Addr Other",
			Name:           "Test2 Other",
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

	db, mock, err := sqlmock.New()
	cet.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "other_address", "other_name"}).
			FromCSVString("111 Test Addr,Test1,111 Test Addr Other,Test1 Other\n211 Test Addr,Test2,211 Test Addr Other,Test2 Other"))

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []*ComposedWithWithPointerWithDuplicateFields
	cet.NoError(e.ScanStructs(&composed))
	cet.Equal([]*ComposedWithWithPointerWithDuplicateFields{
		{
			StructWithTags: &StructWithTags{Address: "111 Test Addr", Name: "Test1"},
			Address:        "111 Test Addr Other",
			Name:           "Test1 Other",
		},
		{
			StructWithTags: &StructWithTags{Address: "211 Test Addr", Name: "Test2"},
			Address:        "211 Test Addr Other",
			Name:           "Test2 Other",
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
			FromCSVString("111-111-1111,20\n222-222-2222,30"))

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []ComposedIgnoredPointerStruct
	cet.NoError(e.ScanStructs(&composed))
	cet.Equal([]ComposedIgnoredPointerStruct{
		{StructWithTags: &StructWithTags{}, PhoneNumber: "111-111-1111", Age: 20},
		{StructWithTags: &StructWithTags{}, PhoneNumber: "222-222-2222", Age: 30},
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
			FromCSVString("111 Test Addr,Test1,111-111-1111,20\n211 Test Addr,Test2,222-222-2222,30"))

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []ComposedWithPointerStruct
	cet.NoError(e.ScanStructs(&composed))
	cet.Equal([]ComposedWithPointerStruct{
		{StructWithTags: &StructWithTags{Address: "111 Test Addr", Name: "Test1"}, PhoneNumber: "111-111-1111", Age: 20},
		{StructWithTags: &StructWithTags{Address: "211 Test Addr", Name: "Test2"}, PhoneNumber: "222-222-2222", Age: 30},
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
			FromCSVString("111 Test Addr,Test1,111-111-1111,20\n211 Test Addr,Test2,222-222-2222,30"))

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []*ComposedWithPointerStruct
	cet.NoError(e.ScanStructs(&composed))
	cet.Equal([]*ComposedWithPointerStruct{
		{StructWithTags: &StructWithTags{Address: "111 Test Addr", Name: "Test1"}, PhoneNumber: "111-111-1111", Age: 20},
		{StructWithTags: &StructWithTags{Address: "211 Test Addr", Name: "Test2"}, PhoneNumber: "222-222-2222", Age: 30},
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
			FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []StructWithTags
	cet.NoError(e.ScanStructsContext(ctx, &items))
	cet.Equal([]StructWithTags{
		{Address: "111 Test Addr", Name: "Test1"},
		{Address: "211 Test Addr", Name: "Test2"},
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
			FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []StructWithNoTags
	cet.NoError(e.ScanStructsContext(ctx, &items))
	cet.Equal([]StructWithNoTags{
		{Address: "111 Test Addr", Name: "Test1"},
		{Address: "211 Test Addr", Name: "Test2"},
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
			FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

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
			FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []*StructWithTags
	cet.NoError(e.ScanStructsContext(ctx, &items))
	cet.Equal([]*StructWithTags{
		{Address: "111 Test Addr", Name: "Test1"},
		{Address: "211 Test Addr", Name: "Test2"},
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
			FromCSVString("111 Test Addr,Test1,111-111-1111,20\n211 Test Addr,Test2,222-222-2222,30"))

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []ComposedStruct
	cet.NoError(e.ScanStructsContext(ctx, &composed))
	cet.Equal([]ComposedStruct{
		{StructWithTags: StructWithTags{Address: "111 Test Addr", Name: "Test1"}, PhoneNumber: "111-111-1111", Age: 20},
		{StructWithTags: StructWithTags{Address: "211 Test Addr", Name: "Test2"}, PhoneNumber: "222-222-2222", Age: 30},
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
			FromCSVString("111-111-1111,20\n222-222-2222,30"))

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []ComposedIgnoredStruct
	cet.NoError(e.ScanStructsContext(ctx, &composed))
	cet.Equal([]ComposedIgnoredStruct{
		{StructWithTags: StructWithTags{}, PhoneNumber: "111-111-1111", Age: 20},
		{StructWithTags: StructWithTags{}, PhoneNumber: "222-222-2222", Age: 30},
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
			FromCSVString("111 Test Addr,Test1,111-111-1111,20\n211 Test Addr,Test2,222-222-2222,30"))

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []*ComposedStruct
	cet.NoError(e.ScanStructsContext(ctx, &composed))
	cet.Equal([]*ComposedStruct{
		{StructWithTags: StructWithTags{Address: "111 Test Addr", Name: "Test1"}, PhoneNumber: "111-111-1111", Age: 20},
		{StructWithTags: StructWithTags{Address: "211 Test Addr", Name: "Test2"}, PhoneNumber: "222-222-2222", Age: 30},
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
			FromCSVString("111 Test Addr,Test1,111-111-1111,20\n211 Test Addr,Test2,222-222-2222,30"))

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []ComposedWithPointerStruct
	cet.NoError(e.ScanStructsContext(ctx, &composed))
	cet.Equal([]ComposedWithPointerStruct{
		{StructWithTags: &StructWithTags{Address: "111 Test Addr", Name: "Test1"}, PhoneNumber: "111-111-1111", Age: 20},
		{StructWithTags: &StructWithTags{Address: "211 Test Addr", Name: "Test2"}, PhoneNumber: "222-222-2222", Age: 30},
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
			FromCSVString("111 Test Addr,Test1,111-111-1111,20\n211 Test Addr,Test2,222-222-2222,30"))

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []*ComposedWithPointerStruct
	cet.NoError(e.ScanStructsContext(ctx, &composed))
	cet.Equal([]*ComposedWithPointerStruct{
		{StructWithTags: &StructWithTags{Address: "111 Test Addr", Name: "Test1"}, PhoneNumber: "111-111-1111", Age: 20},
		{StructWithTags: &StructWithTags{Address: "211 Test Addr", Name: "Test2"}, PhoneNumber: "222-222-2222", Age: 30},
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
	cet.Equal(item.Address, "111 Test Addr")
	cet.Equal(item.Name, "Test1")

	var composed ComposedStruct
	found, err = e.ScanStruct(&composed)
	cet.NoError(err)
	cet.True(found)
	cet.Equal(composed.Address, "111 Test Addr")
	cet.Equal(composed.Name, "Test1")
	cet.Equal(composed.PhoneNumber, "111-111-1111")
	cet.Equal(composed.Age, int64(20))

	var embeddedPtr ComposedWithPointerStruct
	found, err = e.ScanStruct(&embeddedPtr)
	cet.NoError(err)
	cet.True(found)
	cet.Equal(embeddedPtr.Address, "111 Test Addr")
	cet.Equal(embeddedPtr.Name, "Test1")
	cet.Equal(embeddedPtr.PhoneNumber, "111-111-1111")
	cet.Equal(embeddedPtr.Age, int64(20))

	var noTag StructWithNoTags
	found, err = e.ScanStruct(&noTag)
	cet.NoError(err)
	cet.True(found)
	cet.Equal(noTag.Address, "111 Test Addr")
	cet.Equal(noTag.Name, "Test1")
}

func (cet *crudExecTest) TestScanVals() {
	db, mock, err := sqlmock.New()
	cet.NoError(err)

	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WillReturnError(fmt.Errorf("queryExecutor error"))

	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("1\n2"))

	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("1\n2"))

	e := newQueryExecutor(db, nil, `SELECT "id" FROM "items"`)

	var id int64
	var ids []int64
	cet.Equal(errUnsupportedScanValsType, e.ScanVals(ids))
	cet.Equal(errUnsupportedScanValsType, e.ScanVals(&id))
	cet.EqualError(e.ScanVals(&ids), "queryExecutor error")

	cet.NoError(e.ScanVals(&ids))
	cet.Equal(ids, []int64{1, 2})

	var pointers []*int64
	cet.NoError(e.ScanVals(&pointers))
	cet.Len(pointers, 2)
	cet.Equal(*pointers[0], int64(1))
	cet.Equal(*pointers[1], int64(2))
}

func (cet *crudExecTest) TestScanVal() {
	db, mock, err := sqlmock.New()
	cet.NoError(err)

	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WillReturnError(fmt.Errorf("queryExecutor error"))

	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("1"))

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

	var ptrID int64
	found, err = e.ScanVal(&ptrID)
	cet.NoError(err)
	cet.True(found)
	cet.Equal(ptrID, int64(1))
}

func (cet *crudExecTest) TestScanVal_withByteSlice() {
	db, mock, err := sqlmock.New()
	cet.NoError(err)

	mock.ExpectQuery(`SELECT "name" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"name"}).FromCSVString("byte slice result"))

	e := newQueryExecutor(db, nil, `SELECT "name" FROM "items"`)

	var bytes []byte
	found, err := e.ScanVal(bytes)
	cet.Equal(errScanValPointer, err)
	cet.False(found)

	found, err = e.ScanVal(&bytes)
	cet.NoError(err)
	cet.True(found)
	cet.Equal([]byte("byte slice result"), bytes)
}

func (cet *crudExecTest) TestScanVal_withRawBytes() {
	db, mock, err := sqlmock.New()
	cet.NoError(err)

	mock.ExpectQuery(`SELECT "name" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"name"}).FromCSVString("byte slice result"))

	e := newQueryExecutor(db, nil, `SELECT "name" FROM "items"`)

	var bytes sql.RawBytes
	found, err := e.ScanVal(bytes)
	cet.Equal(errScanValPointer, err)
	cet.False(found)

	found, err = e.ScanVal(&bytes)
	cet.NoError(err)
	cet.True(found)
	cet.Equal(sql.RawBytes("byte slice result"), bytes)
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
