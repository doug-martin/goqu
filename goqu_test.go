package goqu

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/suite"
)

type (
	dialectWrapperSuite struct {
		suite.Suite
	}
)

func (dws *dialectWrapperSuite) SetupSuite() {
	testDialect := DefaultDialectOptions()
	// override to some value to ensure correct dialect is set
	RegisterDialect("test", testDialect)
}

func (dws *dialectWrapperSuite) TearDownSuite() {
	DeregisterDialect("test")
}

func (dws *dialectWrapperSuite) TestFrom() {
	dw := Dialect("test")
	dws.Equal(From("table").WithDialect("test"), dw.From("table"))
}

func (dws *dialectWrapperSuite) TestSelect() {
	dw := Dialect("test")
	dws.Equal(Select("col").WithDialect("test"), dw.Select("col"))
}

func (dws *dialectWrapperSuite) TestInsert() {
	dw := Dialect("test")
	dws.Equal(Insert("table").WithDialect("test"), dw.Insert("table"))
}

func (dws *dialectWrapperSuite) TestDelete() {
	dw := Dialect("test")
	dws.Equal(Delete("table").WithDialect("test"), dw.Delete("table"))
}

func (dws *dialectWrapperSuite) TestTruncate() {
	dw := Dialect("test")
	dws.Equal(Truncate("table").WithDialect("test"), dw.Truncate("table"))
}

func (dws *dialectWrapperSuite) TestDB() {
	mDb, _, err := sqlmock.New()
	dws.Require().NoError(err)
	dw := Dialect("test")
	dws.Equal(New("test", mDb), dw.DB(mDb))
}

func TestDialectWrapper(t *testing.T) {
	suite.Run(t, new(dialectWrapperSuite))
}
