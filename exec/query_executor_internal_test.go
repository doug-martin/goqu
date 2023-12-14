package exec

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
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
	otherAddr1                 = "111 Test Addr Other"
	otherAddr2                 = "211 Test Addr Other"
	otherName1                 = "Test1 Other"
	otherName2                 = "Test2 Other"
)

type queryExecutorSuite struct {
	suite.Suite
}

func (qes *queryExecutorSuite) TestWithError() {
	type StructWithTags struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	ctx := context.Background()
	db, _, err := sqlmock.New()
	qes.NoError(err)
	expectedErr := fmt.Errorf("crud exec error")
	e := newQueryExecutor(db, expectedErr, `SELECT * FROM "items"`)
	var items []StructWithTags
	qes.EqualError(e.ScanStructs(&items), expectedErr.Error())
	qes.EqualError(e.ScanStructsContext(ctx, &items), expectedErr.Error())
	found, err := e.ScanStruct(&StructWithTags{})
	qes.EqualError(err, expectedErr.Error())
	qes.False(found)
	found, err = e.ScanStructContext(ctx, &StructWithTags{})
	qes.EqualError(err, expectedErr.Error())
	qes.False(found)
	var vals []string
	qes.EqualError(e.ScanVals(&vals), expectedErr.Error())
	qes.EqualError(e.ScanValsContext(ctx, &vals), expectedErr.Error())
	var val string
	found, err = e.ScanVal(&val)
	qes.EqualError(err, expectedErr.Error())
	qes.False(found)
	found, err = e.ScanValContext(ctx, &val)
	qes.EqualError(err, expectedErr.Error())
	qes.False(found)
}

func (qes *queryExecutorSuite) TestToSQL() {
	db, _, err := sqlmock.New()
	qes.NoError(err)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)
	query, args, err := e.ToSQL()
	qes.NoError(err)
	qes.Equal(`SELECT * FROM "items"`, query)
	qes.Empty(args)
}

func (qes *queryExecutorSuite) TestScanStructs_withTaggedFields() {
	type StructWithTags struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	db, mock, err := sqlmock.New()
	qes.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			AddRow(testAddr1, testName1).
			AddRow(testAddr2, testName2),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []StructWithTags
	qes.NoError(e.ScanStructs(&items))
	qes.Equal([]StructWithTags{
		{Address: testAddr1, Name: testName1},
		{Address: testAddr2, Name: testName2},
	}, items)
}

func (qes *queryExecutorSuite) TestScanStructs_withUntaggedFields() {
	type StructWithNoTags struct {
		Address string
		Name    string
	}
	db, mock, err := sqlmock.New()
	qes.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			AddRow(testAddr1, testName1).
			AddRow(testAddr2, testName2))

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []StructWithNoTags
	qes.NoError(e.ScanStructs(&items))
	qes.Equal([]StructWithNoTags{
		{Address: testAddr1, Name: testName1},
		{Address: testAddr2, Name: testName2},
	}, items)
}

func (qes *queryExecutorSuite) TestScanStructs_withPointerFields() {
	type StructWithPointerFields struct {
		Str   *string
		Time  *time.Time
		Bool  *bool
		Int   *int64
		Float *float64
	}
	db, mock, err := sqlmock.New()
	qes.NoError(err)
	now := time.Now()
	str1, str2 := "str1", "str2"
	t := true
	var i1, i2 int64 = 1, 2
	f1, f2 := 1.1, 2.1
	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"str", "time", "bool", "int", "float"}).
			AddRow(str1, now, true, i1, f1).
			AddRow(str2, now, true, i2, f2).
			AddRow(nil, nil, nil, nil, nil),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []StructWithPointerFields
	qes.NoError(e.ScanStructs(&items))
	qes.Equal([]StructWithPointerFields{
		{Str: &str1, Time: &now, Bool: &t, Int: &i1, Float: &f1},
		{Str: &str2, Time: &now, Bool: &t, Int: &i2, Float: &f2},
		{},
	}, items)
}

func (qes *queryExecutorSuite) TestScanStructs_withPrivateFields() {
	type StructWithPrivateTags struct {
		private string //nolint:structcheck,unused // need for test
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	db, mock, err := sqlmock.New()
	qes.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			AddRow(testAddr1, testName1).
			AddRow(testAddr2, testName2),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []StructWithPrivateTags
	qes.NoError(e.ScanStructs(&items))
	qes.Equal([]StructWithPrivateTags{
		{Address: testAddr1, Name: testName1},
		{Address: testAddr2, Name: testName2},
	}, items)
}

func (qes *queryExecutorSuite) TestScanStructs_pointers() {
	type StructWithTags struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	db, mock, err := sqlmock.New()
	qes.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			AddRow(testAddr1, testName1).
			AddRow(testAddr2, testName2),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []*StructWithTags
	qes.NoError(e.ScanStructs(&items))
	qes.Equal([]*StructWithTags{
		{Address: testAddr1, Name: testName1},
		{Address: testAddr2, Name: testName2},
	}, items)
}

func (qes *queryExecutorSuite) TestScanStructs_withIgnoredEmbeddedStruct() {
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
	qes.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"phone_number", "age"}).
			AddRow(testPhone1, testAge1).AddRow(testPhone2, testAge2),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []ComposedIgnoredStruct
	qes.NoError(e.ScanStructs(&composed))
	qes.Equal([]ComposedIgnoredStruct{
		{StructWithTags: StructWithTags{}, PhoneNumber: testPhone1, Age: testAge1},
		{StructWithTags: StructWithTags{}, PhoneNumber: testPhone2, Age: testAge2},
	}, composed)
}

func (qes *queryExecutorSuite) TestScanStructs_withEmbeddedStruct() {
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
	qes.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "phone_number", "age"}).
			AddRow(testAddr1, testName1, testPhone1, testAge1).
			AddRow(testAddr2, testName2, testPhone2, testAge2),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []ComposedStruct
	qes.NoError(e.ScanStructs(&composed))
	qes.Equal([]ComposedStruct{
		{StructWithTags: StructWithTags{Address: testAddr1, Name: testName1}, PhoneNumber: testPhone1, Age: testAge1},
		{StructWithTags: StructWithTags{Address: testAddr2, Name: testName2}, PhoneNumber: testPhone2, Age: testAge2},
	}, composed)
}

func (qes *queryExecutorSuite) TestScanStructs_pointersWithEmbeddedStruct() {
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
	qes.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "phone_number", "age"}).
			AddRow(testAddr1, testName1, testPhone1, testAge1).
			AddRow(testAddr2, testName2, testPhone2, testAge2),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []*ComposedStruct
	qes.NoError(e.ScanStructs(&composed))
	qes.Equal([]*ComposedStruct{
		{StructWithTags: StructWithTags{Address: testAddr1, Name: testName1}, PhoneNumber: testPhone1, Age: testAge1},
		{StructWithTags: StructWithTags{Address: testAddr2, Name: testName2}, PhoneNumber: testPhone2, Age: testAge2},
	}, composed)
}

func (qes *queryExecutorSuite) TestScanStructs_pointersWithEmbeddedStructDuplicateFields() {
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
	qes.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "other_address", "other_name"}).
			AddRow(testAddr1, testName1, otherAddr1, otherName1).
			AddRow(testAddr2, testName2, otherAddr2, otherName2),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []*ComposedStructWithDuplicateFields
	qes.NoError(e.ScanStructs(&composed))
	qes.Equal([]*ComposedStructWithDuplicateFields{
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

func (qes *queryExecutorSuite) TestScanStructs_pointersWithEmbeddedPointerDuplicateFields() {
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
	qes.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "other_address", "other_name"}).
			AddRow(testAddr1, testName1, otherAddr1, otherName1).
			AddRow(testAddr2, testName2, otherAddr2, otherName2),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []*ComposedWithWithPointerWithDuplicateFields
	qes.NoError(e.ScanStructs(&composed))
	qes.Equal([]*ComposedWithWithPointerWithDuplicateFields{
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

func (qes *queryExecutorSuite) TestScanStructs_withIgnoredEmbeddedPointerStruct() {
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
	qes.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"phone_number", "age"}).
			AddRow(testPhone1, testAge1).
			AddRow(testPhone2, testAge2),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []ComposedIgnoredPointerStruct
	qes.NoError(e.ScanStructs(&composed))
	qes.Equal([]ComposedIgnoredPointerStruct{
		{PhoneNumber: testPhone1, Age: testAge1},
		{PhoneNumber: testPhone2, Age: testAge2},
	}, composed)
}

func (qes *queryExecutorSuite) TestScanStructs_withEmbeddedStructPointer() {
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
	qes.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "phone_number", "age"}).
			AddRow(testAddr1, testName1, testPhone1, testAge1).
			AddRow(testAddr2, testName2, testPhone2, testAge2),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []ComposedWithPointerStruct
	qes.NoError(e.ScanStructs(&composed))
	qes.Equal([]ComposedWithPointerStruct{
		{StructWithTags: &StructWithTags{Address: testAddr1, Name: testName1}, PhoneNumber: testPhone1, Age: testAge1},
		{StructWithTags: &StructWithTags{Address: testAddr2, Name: testName2}, PhoneNumber: testPhone2, Age: testAge2},
	}, composed)
}

func (qes *queryExecutorSuite) TestScanStructs_pointersWithEmbeddedStructPointer() {
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
	qes.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "phone_number", "age"}).
			AddRow(testAddr1, testName1, testPhone1, testAge1).
			AddRow(testAddr2, testName2, testPhone2, testAge2),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []*ComposedWithPointerStruct
	qes.NoError(e.ScanStructs(&composed))
	qes.Equal([]*ComposedWithPointerStruct{
		{StructWithTags: &StructWithTags{Address: testAddr1, Name: testName1}, PhoneNumber: testPhone1, Age: testAge1},
		{StructWithTags: &StructWithTags{Address: testAddr2, Name: testName2}, PhoneNumber: testPhone2, Age: testAge2},
	}, composed)
}

func (qes *queryExecutorSuite) TestScanStructs_badValue() {
	type StructWithTags struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	tests := []struct {
		name  string
		items interface{}
	}{
		{
			name:  "non-pointer items",
			items: []StructWithTags{},
		},
		{
			name:  "non-slice items",
			items: &StructWithTags{},
		},
	}
	for i := range tests {
		test := tests[i]
		qes.Run(test.name, func() {
			db, mock, err := sqlmock.New()
			qes.NoError(err)
			mock.ExpectQuery(`SELECT \* FROM "items"`).
				WithArgs().
				WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
					AddRow(testAddr1, testName1).AddRow(testAddr2, testName2),
				)
			e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)
			qes.Equal(errUnsupportedScanStructsType, e.ScanStructs(test.items))
		})
	}
}

func (qes *queryExecutorSuite) TestScanStructs_queryError() {
	type StructWithTags struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	db, mock, err := sqlmock.New()
	qes.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WillReturnError(fmt.Errorf("queryExecutor error"))

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []StructWithTags
	qes.EqualError(e.ScanStructs(&items), "queryExecutor error")
}

func (qes *queryExecutorSuite) TestScanStructsContext_withTaggedFields() {
	type StructWithTags struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	qes.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			AddRow(testAddr1, testName1).
			AddRow(testAddr2, testName2),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []StructWithTags
	qes.NoError(e.ScanStructsContext(ctx, &items))
	qes.Equal([]StructWithTags{
		{Address: testAddr1, Name: testName1},
		{Address: testAddr2, Name: testName2},
	}, items)
}

func (qes *queryExecutorSuite) TestScanStructsContext_withUntaggedFields() {
	type StructWithNoTags struct {
		Address string
		Name    string
	}

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	qes.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			AddRow(testAddr1, testName1).
			AddRow(testAddr2, testName2),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []StructWithNoTags
	qes.NoError(e.ScanStructsContext(ctx, &items))
	qes.Equal([]StructWithNoTags{
		{Address: testAddr1, Name: testName1},
		{Address: testAddr2, Name: testName2},
	}, items)
}

func (qes *queryExecutorSuite) TestScanStructsContext_withPointerFields() {
	type StructWithPointerFields struct {
		Address *string
		Name    *string
	}
	ctx := context.Background()
	db, mock, err := sqlmock.New()
	qes.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			AddRow(testAddr1, testName1).
			AddRow(testAddr2, testName2),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []StructWithPointerFields
	qes.NoError(e.ScanStructsContext(ctx, &items))
	qes.Equal([]StructWithPointerFields{
		{Address: &testAddr1, Name: &testName1},
		{Address: &testAddr2, Name: &testName2},
	}, items)
}

func (qes *queryExecutorSuite) TestScanStructsContext_pointers() {
	type StructWithTags struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	qes.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			AddRow(testAddr1, testName1).
			AddRow(testAddr2, testName2),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []*StructWithTags
	qes.NoError(e.ScanStructsContext(ctx, &items))
	qes.Equal([]*StructWithTags{
		{Address: testAddr1, Name: testName1},
		{Address: testAddr2, Name: testName2},
	}, items)
}

func (qes *queryExecutorSuite) TestScanStructsContext_withEmbeddedStruct() {
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
	qes.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "phone_number", "age"}).
			AddRow(testAddr1, testName1, testPhone1, testAge1).
			AddRow(testAddr2, testName2, testPhone2, testAge2),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []ComposedStruct
	qes.NoError(e.ScanStructsContext(ctx, &composed))
	qes.Equal([]ComposedStruct{
		{StructWithTags: StructWithTags{Address: testAddr1, Name: testName1}, PhoneNumber: testPhone1, Age: testAge1},
		{StructWithTags: StructWithTags{Address: testAddr2, Name: testName2}, PhoneNumber: testPhone2, Age: testAge2},
	}, composed)
}

func (qes *queryExecutorSuite) TestScanStructsContext_withIgnoredEmbeddedStruct() {
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
	qes.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"phone_number", "age"}).
			AddRow(testPhone1, testAge1).
			AddRow(testPhone2, testAge2),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []ComposedIgnoredStruct
	qes.NoError(e.ScanStructsContext(ctx, &composed))
	qes.Equal([]ComposedIgnoredStruct{
		{StructWithTags: StructWithTags{}, PhoneNumber: testPhone1, Age: testAge1},
		{StructWithTags: StructWithTags{}, PhoneNumber: testPhone2, Age: testAge2},
	}, composed)
}

func (qes *queryExecutorSuite) TestScanStructsContext_pointersWithEmbeddedStruct() {
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
	qes.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "phone_number", "age"}).
			AddRow(testAddr1, testName1, testPhone1, testAge1).
			AddRow(testAddr2, testName2, testPhone2, testAge2),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []*ComposedStruct
	qes.NoError(e.ScanStructsContext(ctx, &composed))
	qes.Equal([]*ComposedStruct{
		{StructWithTags: StructWithTags{Address: testAddr1, Name: testName1}, PhoneNumber: testPhone1, Age: testAge1},
		{StructWithTags: StructWithTags{Address: testAddr2, Name: testName2}, PhoneNumber: testPhone2, Age: testAge2},
	}, composed)
}

func (qes *queryExecutorSuite) TestScanStructsContext_withEmbeddedStructPointer() {
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
	qes.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "phone_number", "age"}).
			AddRow(testAddr1, testName1, testPhone1, testAge1).
			AddRow(testAddr2, testName2, testPhone2, testAge2),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []ComposedWithPointerStruct
	qes.NoError(e.ScanStructsContext(ctx, &composed))
	qes.Equal([]ComposedWithPointerStruct{
		{StructWithTags: &StructWithTags{Address: testAddr1, Name: testName1}, PhoneNumber: testPhone1, Age: testAge1},
		{StructWithTags: &StructWithTags{Address: testAddr2, Name: testName2}, PhoneNumber: testPhone2, Age: testAge2},
	}, composed)
}

func (qes *queryExecutorSuite) TestScanStructsContext_pointersWithEmbeddedStructPointer() {
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
	qes.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name", "phone_number", "age"}).
			AddRow(testAddr1, testName1, testPhone1, testAge1).
			AddRow(testAddr2, testName2, testPhone2, testAge2),
		)

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var composed []*ComposedWithPointerStruct
	qes.NoError(e.ScanStructsContext(ctx, &composed))
	qes.Equal([]*ComposedWithPointerStruct{
		{StructWithTags: &StructWithTags{Address: testAddr1, Name: testName1}, PhoneNumber: testPhone1, Age: testAge1},
		{StructWithTags: &StructWithTags{Address: testAddr2, Name: testName2}, PhoneNumber: testPhone2, Age: testAge2},
	}, composed)
}

func (qes *queryExecutorSuite) TestScanStructsContext_badValue() {
	type StructWithTags struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	tests := []struct {
		name  string
		items interface{}
	}{
		{
			name:  "non-pointer items",
			items: []StructWithTags{},
		},
		{
			name:  "non-slice items",
			items: &StructWithTags{},
		},
	}
	for i := range tests {
		test := tests[i]
		qes.Run(test.name, func() {
			db, mock, err := sqlmock.New()
			qes.NoError(err)
			mock.ExpectQuery(`SELECT \* FROM "items"`).
				WithArgs().
				WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
					AddRow(testAddr1, testName1).AddRow(testAddr2, testName2),
				)
			e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)
			qes.Equal(errUnsupportedScanStructsType, e.ScanStructsContext(context.Background(), test.items))
		})
	}
}

func (qes *queryExecutorSuite) TestScanStructsContext_queryError() {
	type StructWithTags struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	ctx := context.Background()
	db, mock, err := sqlmock.New()
	qes.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WillReturnError(fmt.Errorf("queryExecutor error"))

	e := newQueryExecutor(db, nil, `SELECT * FROM "items"`)

	var items []StructWithTags
	qes.EqualError(e.ScanStructsContext(ctx, &items), "queryExecutor error")
}

func (qes *queryExecutorSuite) TestScanStruct() {
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
	qes.NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WillReturnError(fmt.Errorf("queryExecutor error"))

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			AddRow(nil, nil),
		)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}))

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
	qes.Equal(errUnsupportedScanStructType, err)
	qes.False(found)
	found, err = e.ScanStruct(&slicePtr)
	qes.Equal(errUnsupportedScanStructType, err)
	qes.False(found)
	found, err = e.ScanStruct(&item)
	qes.EqualError(err, "queryExecutor error")
	qes.False(found)

	found, err = e.ScanStruct(&item)
	qes.Error(err)
	qes.False(found)

	found, err = e.ScanStruct(&item)
	qes.NoError(err)
	qes.False(found)

	found, err = e.ScanStruct(&item)
	qes.NoError(err)
	qes.True(found)
	qes.Equal(StructWithTags{
		Address: testAddr1,
		Name:    testName1,
	}, item)

	var composed ComposedStruct
	found, err = e.ScanStruct(&composed)
	qes.NoError(err)
	qes.True(found)
	qes.Equal(ComposedStruct{
		StructWithTags: StructWithTags{Address: testAddr1, Name: testName1},
		PhoneNumber:    testPhone1,
		Age:            testAge1,
	}, composed)

	var embeddedPtr ComposedWithPointerStruct
	found, err = e.ScanStruct(&embeddedPtr)
	qes.NoError(err)
	qes.True(found)
	qes.Equal(ComposedWithPointerStruct{
		StructWithTags: &StructWithTags{
			Address: testAddr1,
			Name:    testName1,
		},
		PhoneNumber: testPhone1,
		Age:         testAge1,
	}, embeddedPtr)

	var noTag StructWithNoTags
	found, err = e.ScanStruct(&noTag)
	qes.NoError(err)
	qes.True(found)
	qes.Equal(StructWithNoTags{
		Address: testAddr1,
		Name:    testName1,
	}, noTag)
}

func (qes *queryExecutorSuite) TestScanStruct_taggedStructs() {
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

	type StructWithTaggedStructs struct {
		NoTags          StructWithNoTags          `db:"notags"`
		Tags            StructWithTags            `db:"tags"`
		Composed        ComposedStruct            `db:"composedstruct"`
		ComposedPointer ComposedWithPointerStruct `db:"composedptrstruct"`
	}

	db, mock, err := sqlmock.New()
	qes.NoError(err)

	cols := []string{
		"notags.address", "notags.name",
		"tags.address", "tags.name",
		"composedstruct.address", "composedstruct.name", "composedstruct.phone_number", "composedstruct.age",
		"composedptrstruct.address", "composedptrstruct.name", "composedptrstruct.phone_number", "composedptrstruct.age",
	}

	q := `SELECT` + strings.Join(cols, ", ") + ` FROM "items"`

	mock.ExpectQuery(q).
		WithArgs().
		WillReturnRows(sqlmock.NewRows(cols).AddRow(
			testAddr1, testName1,
			testAddr2, testName2,
			testAddr1, testName1, testPhone1, testAge1,
			testAddr2, testName2, testPhone2, testAge2,
		))

	e := newQueryExecutor(db, nil, q)

	var item StructWithTaggedStructs
	found, err := e.ScanStruct(&item)
	qes.NoError(err)
	qes.True(found)
	qes.Equal(StructWithTaggedStructs{
		NoTags: StructWithNoTags{Address: testAddr1, Name: testName1},
		Tags:   StructWithTags{Address: testAddr2, Name: testName2},
		Composed: ComposedStruct{
			StructWithTags: StructWithTags{Address: testAddr1, Name: testName1},
			PhoneNumber:    testPhone1,
			Age:            testAge1,
		},
		ComposedPointer: ComposedWithPointerStruct{
			StructWithTags: &StructWithTags{Address: testAddr2, Name: testName2},
			PhoneNumber:    testPhone2,
			Age:            testAge2,
		},
	}, item)
}

func (qes *queryExecutorSuite) TestScanVals() {
	db, mock, err := sqlmock.New()
	qes.NoError(err)

	var id1, id2 int64 = 1, 2

	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WillReturnError(fmt.Errorf("queryExecutor error"))

	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(id1).RowError(0, fmt.Errorf("row error")))

	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(id1).AddRow("a"))

	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(id1).AddRow(id2))

	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(id1).AddRow(id2))

	e := newQueryExecutor(db, nil, `SELECT "id" FROM "items"`)

	var ids []int64
	qes.EqualError(e.ScanVals(&ids), "queryExecutor error")
	qes.EqualError(e.ScanVals(&ids), "row error")
	qes.Error(e.ScanVals(&ids))

	ids = ids[0:0]
	qes.NoError(e.ScanVals(&ids))
	qes.Equal(ids, []int64{id1, id2})

	var pointers []*int64
	qes.NoError(e.ScanVals(&pointers))
	qes.Len(pointers, 2)
	qes.Equal(&id1, pointers[0])
	qes.Equal(&id2, pointers[1])
}

func (qes *queryExecutorSuite) TestScanValsError() {
	var id int64

	tests := []struct {
		name  string
		items interface{}
	}{
		{
			name:  "non-pointer items",
			items: []int64{},
		},
		{
			name:  "non-slice items",
			items: &id,
		},
	}
	for i := range tests {
		test := tests[i]
		qes.Run(test.name, func() {
			db, mock, err := sqlmock.New()
			qes.NoError(err)
			mock.ExpectQuery(`SELECT "id" FROM "items"`).
				WithArgs().
				WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1).AddRow(2))

			e := newQueryExecutor(db, nil, `SELECT "id" FROM "items"`)
			qes.Equal(errUnsupportedScanValsType, e.ScanVals(test.items))
		})
	}
}

func (qes *queryExecutorSuite) TestScanVal() {
	db, mock, err := sqlmock.New()
	qes.NoError(err)

	id1 := int64(1)
	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WillReturnError(fmt.Errorf("queryExecutor error"))

	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).RowError(0, fmt.Errorf("row error")).AddRow(id1))

	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("c"))

	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(id1))

	e := newQueryExecutor(db, nil, `SELECT "id" FROM "items"`)

	var id int64
	var ids []int64
	found, err := e.ScanVal(id)
	qes.Equal(errScanValPointer, err)
	qes.False(found)
	found, err = e.ScanVal(&ids)
	qes.Equal(errScanValNonSlice, err)
	qes.False(found)
	found, err = e.ScanVal(&id)
	qes.EqualError(err, "queryExecutor error")
	qes.False(found)

	found, err = e.ScanVal(&id)
	qes.EqualError(err, "row error")
	qes.False(found)

	found, err = e.ScanVal(&id)
	qes.Error(err)
	qes.False(found)

	var ptrID *int64
	found, err = e.ScanVal(&ptrID)
	qes.NoError(err)
	qes.True(found)
	qes.Equal(&id1, ptrID)
}

func (qes *queryExecutorSuite) TestScanVal_withByteSlice() {
	db, mock, err := sqlmock.New()
	qes.NoError(err)

	mock.ExpectQuery(`SELECT "name" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow(testByteSliceContent))

	e := newQueryExecutor(db, nil, `SELECT "name" FROM "items"`)

	var bytes []byte
	found, err := e.ScanVal(bytes)
	qes.Equal(errScanValPointer, err)
	qes.False(found)

	found, err = e.ScanVal(&bytes)
	qes.NoError(err)
	qes.True(found)
	qes.Equal([]byte(testByteSliceContent), bytes)
}

func (qes *queryExecutorSuite) TestScanVal_withRawBytes() {
	db, mock, err := sqlmock.New()
	qes.NoError(err)

	mock.ExpectQuery(`SELECT "name" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"name"}).AddRow(testByteSliceContent))

	e := newQueryExecutor(db, nil, `SELECT "name" FROM "items"`)

	var bytes sql.RawBytes
	found, err := e.ScanVal(bytes)
	qes.Equal(errScanValPointer, err)
	qes.False(found)

	found, err = e.ScanVal(&bytes)
	qes.NoError(err)
	qes.True(found)
	qes.Equal(sql.RawBytes(testByteSliceContent), bytes)
}

type JSONBoolArray []bool

func (b *JSONBoolArray) Scan(src interface{}) error {
	return json.Unmarshal(src.([]byte), b)
}

func (qes *queryExecutorSuite) TestScanVal_withValuerSlice() {
	db, mock, err := sqlmock.New()
	qes.NoError(err)

	mock.ExpectQuery(`SELECT "bools" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"bools"}).FromCSVString(`"[true, false, true]"`))

	e := newQueryExecutor(db, nil, `SELECT "bools" FROM "items"`)

	var bools JSONBoolArray
	found, err := e.ScanVal(bools)
	qes.Equal(errScanValPointer, err)
	qes.False(found)

	found, err = e.ScanVal(&bools)
	qes.NoError(err)
	qes.True(found)
	qes.Equal(JSONBoolArray{true, false, true}, bools)
}

func TestQueryExecutorSuite(t *testing.T) {
	suite.Run(t, new(queryExecutorSuite))
}
