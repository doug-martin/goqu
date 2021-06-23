package goqu_test

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/doug-martin/goqu/v9"
	"github.com/stretchr/testify/suite"
)

type (
	dialectWrapperSuite struct {
		suite.Suite
	}
)

func (dws *dialectWrapperSuite) SetupSuite() {
	testDialect := goqu.DefaultDialectOptions()
	// override to some value to ensure correct dialect is set
	goqu.RegisterDialect("test", testDialect)
}

func (dws *dialectWrapperSuite) TearDownSuite() {
	goqu.DeregisterDialect("test")
}

func (dws *dialectWrapperSuite) TestFrom() {
	dw := goqu.Dialect("test")
	dws.Equal(goqu.From("table").WithDialect("test"), dw.From("table"))
}

func (dws *dialectWrapperSuite) TestSelect() {
	dw := goqu.Dialect("test")
	dws.Equal(goqu.Select("col").WithDialect("test"), dw.Select("col"))
}

func (dws *dialectWrapperSuite) TestInsert() {
	dw := goqu.Dialect("test")
	dws.Equal(goqu.Insert("table").WithDialect("test"), dw.Insert("table"))
}

func (dws *dialectWrapperSuite) TestDelete() {
	dw := goqu.Dialect("test")
	dws.Equal(goqu.Delete("table").WithDialect("test"), dw.Delete("table"))
}

func (dws *dialectWrapperSuite) TestTruncate() {
	dw := goqu.Dialect("test")
	dws.Equal(goqu.Truncate("table").WithDialect("test"), dw.Truncate("table"))
}

func (dws *dialectWrapperSuite) TestDB() {
	mDB, _, err := sqlmock.New()
	dws.Require().NoError(err)
	dw := goqu.Dialect("test")
	dws.Equal(goqu.New("test", mDB), dw.DB(mDB))
}

func TestDialectWrapper(t *testing.T) {
	suite.Run(t, new(dialectWrapperSuite))
}
