package goqu

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/doug-martin/goqu/v8/exec"
	"github.com/doug-martin/goqu/v8/exp"
	"github.com/doug-martin/goqu/v8/internal/errors"
	"github.com/doug-martin/goqu/v8/internal/sb"
	"github.com/doug-martin/goqu/v8/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type truncateDatasetSuite struct {
	suite.Suite
}

func (tds *truncateDatasetSuite) TestClone() {
	t := tds.T()
	ds := Truncate("test")
	assert.Equal(t, ds.Clone(), ds)
}

func (tds *truncateDatasetSuite) TestExpression() {
	t := tds.T()
	ds := Truncate("test")
	assert.Equal(t, ds.Expression(), ds)
}

func (tds *truncateDatasetSuite) TestDialect() {
	t := tds.T()
	ds := Truncate("test")
	assert.NotNil(t, ds.Dialect())
}

func (tds *truncateDatasetSuite) TestWithDialect() {
	t := tds.T()
	ds := Truncate("test")
	dialect := GetDialect("default")
	ds.WithDialect("default")
	assert.Equal(t, ds.Dialect(), dialect)
}

func (tds *truncateDatasetSuite) TestPrepared() {
	t := tds.T()
	ds := Truncate("test")
	preparedDs := ds.Prepared(true)
	assert.True(t, preparedDs.IsPrepared())
	assert.False(t, ds.IsPrepared())
	// should apply the prepared to any datasets created from the root
	assert.True(t, preparedDs.Restrict().IsPrepared())
}

func (tds *truncateDatasetSuite) TestGetClauses() {
	t := tds.T()
	ds := Truncate("test")
	ce := exp.NewTruncateClauses().SetTable(exp.NewColumnListExpression(I("test")))
	assert.Equal(t, ce, ds.GetClauses())
}

func (tds *truncateDatasetSuite) TestTable(from ...interface{}) {
	t := tds.T()
	ds := Truncate("test")
	dsc := ds.GetClauses()
	ec := dsc.SetTable(exp.NewColumnListExpression(T("t")))
	assert.Equal(t, ec, ds.Table(T("t")).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (tds *truncateDatasetSuite) TestCascade() {
	t := tds.T()
	ds := Truncate("test")
	dsc := ds.GetClauses()
	ec := dsc.SetOptions(exp.TruncateOptions{Cascade: true})
	assert.Equal(t, ec, ds.Cascade().GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (tds *truncateDatasetSuite) TestCascade_ToSQL() {
	t := tds.T()
	ds1 := Truncate("items")
	tsql, _, err := ds1.Cascade().ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, tsql, `TRUNCATE "items" CASCADE`)
}

func (tds *truncateDatasetSuite) TestNoCascade() {
	t := tds.T()
	ds := Truncate("test").Cascade()
	dsc := ds.GetClauses()
	ec := dsc.SetOptions(exp.TruncateOptions{Cascade: false})
	assert.Equal(t, ec, ds.NoCascade().GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (tds *truncateDatasetSuite) TestRestrict() {
	t := tds.T()
	ds := Truncate("test")
	dsc := ds.GetClauses()
	ec := dsc.SetOptions(exp.TruncateOptions{Restrict: true})
	assert.Equal(t, ec, ds.Restrict().GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (tds *truncateDatasetSuite) TestRestrict_ToSQL() {
	t := tds.T()
	ds1 := Truncate("items")
	tsql, _, err := ds1.Restrict().ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, tsql, `TRUNCATE "items" RESTRICT`)
}

func (tds *truncateDatasetSuite) TestNoRestrict() {
	t := tds.T()
	ds := Truncate("test").Restrict()
	dsc := ds.GetClauses()
	ec := dsc.SetOptions(exp.TruncateOptions{Restrict: false})
	assert.Equal(t, ec, ds.NoRestrict().GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (tds *truncateDatasetSuite) TestIdentity() {
	t := tds.T()
	ds := Truncate("test")
	dsc := ds.GetClauses()
	ec := dsc.SetOptions(exp.TruncateOptions{Identity: "RESTART"})
	assert.Equal(t, ec, ds.Identity("RESTART").GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (tds *truncateDatasetSuite) TestIdentity_ToSQL() {
	t := tds.T()
	ds1 := Truncate("items")

	tsql, _, err := ds1.Identity("restart").ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, tsql, `TRUNCATE "items" RESTART IDENTITY`)

	tsql, _, err = ds1.Identity("continue").ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, tsql, `TRUNCATE "items" CONTINUE IDENTITY`)
}

func (tds *truncateDatasetSuite) TestToSQL() {
	t := tds.T()
	md := new(mocks.SQLDialect)
	ds := Truncate("test").SetDialect(md)
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(false)
	md.On("ToTruncateSQL", sqlB, c).Return(nil).Once()

	sql, args, err := ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, sql)
	assert.Empty(t, args)
	md.AssertExpectations(t)
}

func (tds *truncateDatasetSuite) TestToSQL__withPrepared() {
	t := tds.T()
	md := new(mocks.SQLDialect)
	ds := Truncate("test").Prepared(true).SetDialect(md)
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(true)
	md.On("ToTruncateSQL", sqlB, c).Return(nil).Once()

	sql, args, err := ds.ToSQL()
	assert.Empty(t, sql)
	assert.Empty(t, args)
	assert.Nil(t, err)
	md.AssertExpectations(t)
}

func (tds *truncateDatasetSuite) TestToSQL_withError() {
	t := tds.T()
	md := new(mocks.SQLDialect)
	ds := Truncate("test").SetDialect(md)
	c := ds.GetClauses()
	ee := errors.New("expected error")
	sqlB := sb.NewSQLBuilder(false)
	md.On("ToTruncateSQL", sqlB, c).Run(func(args mock.Arguments) {
		args.Get(0).(sb.SQLBuilder).SetError(ee)
	}).Once()

	sql, args, err := ds.ToSQL()
	assert.Empty(t, sql)
	assert.Empty(t, args)
	assert.Equal(t, ee, err)
	md.AssertExpectations(t)
}

func (tds *truncateDatasetSuite) TestExecutor() {
	t := tds.T()
	mDb, _, err := sqlmock.New()
	assert.NoError(t, err)

	ds := newTruncateDataset("mock", exec.NewQueryFactory(mDb)).
		Table("table1", "table2")

	tsql, args, err := ds.Executor().ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `TRUNCATE "table1", "table2"`, tsql)

	tsql, args, err = ds.Prepared(true).Executor().ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `TRUNCATE "table1", "table2"`, tsql)
}

func TestTruncateDataset(t *testing.T) {
	suite.Run(t, new(truncateDatasetSuite))
}
