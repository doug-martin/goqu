package goqu

import (
	"testing"

	"github.com/doug-martin/goqu/v9/exp"
	"github.com/doug-martin/goqu/v9/internal/sb"
	"github.com/doug-martin/goqu/v9/sqlgen/mocks"
	"github.com/stretchr/testify/suite"
)

type dialectTestSuite struct {
	suite.Suite
}

func (dts *dialectTestSuite) TestDialect() {
	opts := DefaultDialectOptions()
	sm := new(mocks.SelectSQLGenerator)
	d := sqlDialect{dialect: "test", dialectOptions: opts, selectGen: sm}

	dts.Equal("test", d.Dialect())
}

func (dts *dialectTestSuite) TestToSelectSQL() {
	opts := DefaultDialectOptions()
	sm := new(mocks.SelectSQLGenerator)
	d := sqlDialect{dialect: "test", dialectOptions: opts, selectGen: sm}

	b := sb.NewSQLBuilder(true)
	sc := exp.NewSelectClauses()
	sm.On("Generate", b, sc).Return(nil).Once()

	d.ToSelectSQL(b, sc)
	sm.AssertExpectations(dts.T())
}

func (dts *dialectTestSuite) TestToUpdateSQL() {
	opts := DefaultDialectOptions()
	um := new(mocks.UpdateSQLGenerator)
	d := sqlDialect{dialect: "test", dialectOptions: opts, updateGen: um}

	b := sb.NewSQLBuilder(true)
	uc := exp.NewUpdateClauses()
	um.On("Generate", b, uc).Return(nil).Once()

	d.ToUpdateSQL(b, uc)
	um.AssertExpectations(dts.T())

}

func (dts *dialectTestSuite) TestToInsertSQL() {
	opts := DefaultDialectOptions()
	im := new(mocks.InsertSQLGenerator)
	d := sqlDialect{dialect: "test", dialectOptions: opts, insertGen: im}

	b := sb.NewSQLBuilder(true)
	ic := exp.NewInsertClauses()
	im.On("Generate", b, ic).Return(nil).Once()

	d.ToInsertSQL(b, ic)
	im.AssertExpectations(dts.T())
}

func (dts *dialectTestSuite) TestToDeleteSQL() {
	opts := DefaultDialectOptions()
	dm := new(mocks.DeleteSQLGenerator)
	d := sqlDialect{dialect: "test", dialectOptions: opts, deleteGen: dm}

	b := sb.NewSQLBuilder(true)
	dc := exp.NewDeleteClauses()
	dm.On("Generate", b, dc).Return(nil).Once()

	d.ToDeleteSQL(b, dc)
	dm.AssertExpectations(dts.T())
}

func (dts *dialectTestSuite) TestToTruncateSQL() {
	opts := DefaultDialectOptions()
	tm := new(mocks.TruncateSQLGenerator)
	d := sqlDialect{dialect: "test", dialectOptions: opts, truncateGen: tm}

	b := sb.NewSQLBuilder(true)
	tc := exp.NewTruncateClauses()
	tm.On("Generate", b, tc).Return(nil).Once()

	d.ToTruncateSQL(b, tc)
	tm.AssertExpectations(dts.T())
}

func TestSQLDialect(t *testing.T) {
	suite.Run(t, new(dialectTestSuite))
}
