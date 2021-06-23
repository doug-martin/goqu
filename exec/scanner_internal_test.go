package exec

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/suite"
)

type scannerSuite struct {
	suite.Suite
}

func TestScanner(t *testing.T) {
	suite.Run(t, &scannerSuite{})
}

func (s *scannerSuite) TestScanStructs() {
	type StructWithTags struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	db, mock, err := sqlmock.New()
	s.Require().NoError(err)

	mock.ExpectQuery(`SELECT \* FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			AddRow(testAddr1, testName1).
			AddRow(testAddr2, testName2),
		)
	rows, err := db.Query(`SELECT * FROM "items"`)
	s.Require().NoError(err)

	sc := NewScanner(rows)

	result := make([]StructWithTags, 0)
	err = sc.ScanStructs(result)
	s.Require().EqualError(err, errUnsupportedScanStructsType.Error())

	err = sc.ScanStructs(&result)
	s.Require().NoError(err)
	s.Require().ElementsMatch(
		[]StructWithTags{{Address: testAddr1, Name: testName1}, {Address: testAddr2, Name: testName2}},
		result,
	)
}

func (s *scannerSuite) TestScanVals() {
	db, mock, err := sqlmock.New()
	s.Require().NoError(err)

	mock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1).AddRow(2))

	rows, err := db.Query(`SELECT "id" FROM "items"`)
	s.Require().NoError(err)

	sc := NewScanner(rows)

	result := make([]int, 0)
	err = sc.ScanVals(result)
	s.Require().EqualError(err, errUnsupportedScanValsType.Error())

	err = sc.ScanVals(&result)
	s.Require().NoError(err)
	s.Require().ElementsMatch([]int{1, 2}, result)
}
