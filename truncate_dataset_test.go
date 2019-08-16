package goqu

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/doug-martin/goqu/v8/exec"
	"github.com/doug-martin/goqu/v8/exp"
	"github.com/doug-martin/goqu/v8/internal/errors"
	"github.com/doug-martin/goqu/v8/internal/sb"
	"github.com/doug-martin/goqu/v8/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type truncateDatasetSuite struct {
	suite.Suite
}

func (tds *truncateDatasetSuite) TestClone() {
	ds := Truncate("test")
	tds.Equal(ds, ds.Clone())
}

func (tds *truncateDatasetSuite) TestExpression() {
	ds := Truncate("test")
	tds.Equal(ds, ds.Expression())
}

func (tds *truncateDatasetSuite) TestDialect() {
	ds := Truncate("test")
	tds.NotNil(ds.Dialect())
}

func (tds *truncateDatasetSuite) TestWithDialect() {
	ds := Truncate("test")
	md := new(mocks.SQLDialect)
	ds = ds.SetDialect(md)

	dialect := GetDialect("default")
	dialectDs := ds.WithDialect("default")
	tds.Equal(md, ds.Dialect())
	tds.Equal(dialect, dialectDs.Dialect())
}

func (tds *truncateDatasetSuite) TestPrepared() {
	ds := Truncate("test")
	preparedDs := ds.Prepared(true)
	tds.True(preparedDs.IsPrepared())
	tds.False(ds.IsPrepared())
	// should apply the prepared to any datasets created from the root
	tds.True(preparedDs.Restrict().IsPrepared())
}

func (tds *truncateDatasetSuite) TestGetClauses() {
	ds := Truncate("test")
	ce := exp.NewTruncateClauses().SetTable(exp.NewColumnListExpression(I("test")))
	tds.Equal(ce, ds.GetClauses())
}

func (tds *truncateDatasetSuite) TestTable(from ...interface{}) {
	ds := Truncate("test")
	dsc := ds.GetClauses()
	ec := dsc.SetTable(exp.NewColumnListExpression(T("t")))
	tds.Equal(ec, ds.Table(T("t")).GetClauses())
	tds.Equal(dsc, ds.GetClauses())
}

func (tds *truncateDatasetSuite) TestCascade() {
	ds := Truncate("test")
	dsc := ds.GetClauses()
	ec := dsc.SetOptions(exp.TruncateOptions{Cascade: true})
	tds.Equal(ec, ds.Cascade().GetClauses())
	tds.Equal(dsc, ds.GetClauses())
}

func (tds *truncateDatasetSuite) TestCascade_ToSQL() {
	ds1 := Truncate("items")
	tsql, _, err := ds1.Cascade().ToSQL()
	tds.NoError(err)
	tds.Equal(`TRUNCATE "items" CASCADE`, tsql)
}

func (tds *truncateDatasetSuite) TestNoCascade() {
	ds := Truncate("test").Cascade()
	dsc := ds.GetClauses()
	ec := dsc.SetOptions(exp.TruncateOptions{Cascade: false})
	tds.Equal(ec, ds.NoCascade().GetClauses())
	tds.Equal(dsc, ds.GetClauses())
}

func (tds *truncateDatasetSuite) TestRestrict() {
	ds := Truncate("test")
	dsc := ds.GetClauses()
	ec := dsc.SetOptions(exp.TruncateOptions{Restrict: true})
	tds.Equal(ec, ds.Restrict().GetClauses())
	tds.Equal(dsc, ds.GetClauses())
}

func (tds *truncateDatasetSuite) TestRestrict_ToSQL() {
	ds1 := Truncate("items")
	tsql, _, err := ds1.Restrict().ToSQL()
	tds.NoError(err)
	tds.Equal(`TRUNCATE "items" RESTRICT`, tsql)
}

func (tds *truncateDatasetSuite) TestNoRestrict() {
	ds := Truncate("test").Restrict()
	dsc := ds.GetClauses()
	ec := dsc.SetOptions(exp.TruncateOptions{Restrict: false})
	tds.Equal(ec, ds.NoRestrict().GetClauses())
	tds.Equal(dsc, ds.GetClauses())
}

func (tds *truncateDatasetSuite) TestIdentity() {
	ds := Truncate("test")
	dsc := ds.GetClauses()
	ec := dsc.SetOptions(exp.TruncateOptions{Identity: "RESTART"})
	tds.Equal(ec, ds.Identity("RESTART").GetClauses())
	tds.Equal(dsc, ds.GetClauses())
}

func (tds *truncateDatasetSuite) TestIdentity_ToSQL() {
	ds1 := Truncate("items")

	tsql, _, err := ds1.Identity("restart").ToSQL()
	tds.NoError(err)
	tds.Equal(`TRUNCATE "items" RESTART IDENTITY`, tsql)

	tsql, _, err = ds1.Identity("continue").ToSQL()
	tds.NoError(err)
	tds.Equal(`TRUNCATE "items" CONTINUE IDENTITY`, tsql)
}

func (tds *truncateDatasetSuite) TestToSQL() {
	md := new(mocks.SQLDialect)
	ds := Truncate("test").SetDialect(md)
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(false)
	md.On("ToTruncateSQL", sqlB, c).Return(nil).Once()

	sql, args, err := ds.ToSQL()
	tds.NoError(err)
	tds.Empty(sql)
	tds.Empty(args)
	md.AssertExpectations(tds.T())
}

func (tds *truncateDatasetSuite) TestToSQL__withPrepared() {
	md := new(mocks.SQLDialect)
	ds := Truncate("test").Prepared(true).SetDialect(md)
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(true)
	md.On("ToTruncateSQL", sqlB, c).Return(nil).Once()

	sql, args, err := ds.ToSQL()
	tds.Empty(sql)
	tds.Empty(args)
	tds.Nil(err)
	md.AssertExpectations(tds.T())
}

func (tds *truncateDatasetSuite) TestToSQL_withError() {
	md := new(mocks.SQLDialect)
	ds := Truncate("test").SetDialect(md)
	c := ds.GetClauses()
	ee := errors.New("expected error")
	sqlB := sb.NewSQLBuilder(false)
	md.On("ToTruncateSQL", sqlB, c).Run(func(args mock.Arguments) {
		args.Get(0).(sb.SQLBuilder).SetError(ee)
	}).Once()

	sql, args, err := ds.ToSQL()
	tds.Empty(sql)
	tds.Empty(args)
	tds.Equal(ee, err)
	md.AssertExpectations(tds.T())
}

func (tds *truncateDatasetSuite) TestExecutor() {
	mDb, _, err := sqlmock.New()
	tds.NoError(err)

	ds := newTruncateDataset("mock", exec.NewQueryFactory(mDb)).
		Table("table1", "table2")

	tsql, args, err := ds.Executor().ToSQL()
	tds.NoError(err)
	tds.Empty(args)
	tds.Equal(`TRUNCATE "table1", "table2"`, tsql)

	tsql, args, err = ds.Prepared(true).Executor().ToSQL()
	tds.NoError(err)
	tds.Empty(args)
	tds.Equal(`TRUNCATE "table1", "table2"`, tsql)
}

func TestTruncateDataset(t *testing.T) {
	suite.Run(t, new(truncateDatasetSuite))
}
