package goqu

import (
	"testing"

	"github.com/doug-martin/goqu/v7/exp"
	"github.com/doug-martin/goqu/v7/internal/errors"
	"github.com/doug-martin/goqu/v7/internal/sb"
	"github.com/doug-martin/goqu/v7/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type datasetTest struct {
	suite.Suite
}

func (dt *datasetTest) TestClone() {
	t := dt.T()
	ds := From("test")
	assert.Equal(t, ds.Clone(), ds)
}

func (dt *datasetTest) TestExpression() {
	t := dt.T()
	ds := From("test")
	assert.Equal(t, ds.Expression(), ds)
}

func (dt *datasetTest) TestDialect() {
	t := dt.T()
	ds := From("test")
	assert.NotNil(t, ds.Dialect())
}

func (dt *datasetTest) TestWithDialect() {
	t := dt.T()
	ds := From("test")
	dialect := GetDialect("default")
	ds.WithDialect("default")
	assert.Equal(t, ds.Dialect(), dialect)
}

func (dt *datasetTest) TestPrepared() {
	t := dt.T()
	ds := From("test")
	preparedDs := ds.Prepared(true)
	assert.True(t, preparedDs.IsPrepared())
	assert.False(t, ds.IsPrepared())
	// should apply the prepared to any datasets created from the root
	assert.True(t, preparedDs.Where(Ex{"a": 1}).IsPrepared())
}

func (dt *datasetTest) TestGetClauses() {
	t := dt.T()
	ds := From("test")
	ce := exp.NewClauses().SetFrom(exp.NewColumnListExpression(I("test")))
	assert.Equal(t, ce, ds.GetClauses())
}

func (dt *datasetTest) TestWith() {
	t := dt.T()
	from := From("cte")
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.CommonTablesAppend(exp.NewCommonTableExpression(false, "test-cte", from))
	assert.Equal(t, ec, ds.With("test-cte", from).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestWithRecursive() {
	t := dt.T()
	from := From("cte")
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.CommonTablesAppend(exp.NewCommonTableExpression(true, "test-cte", from))
	assert.Equal(t, ec, ds.WithRecursive("test-cte", from).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestSelect(selects ...interface{}) {
	t := dt.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetSelect(exp.NewColumnListExpression(C("a")))
	assert.Equal(t, ec, ds.Select(C("a")).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestSelectDistinct(selects ...interface{}) {
	t := dt.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetSelectDistinct(exp.NewColumnListExpression(C("a")))
	assert.Equal(t, ec, ds.SelectDistinct(C("a")).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestClearSelect() {
	t := dt.T()
	ds := From("test").Select(C("a"))
	dsc := ds.GetClauses()
	ec := dsc.SetSelect(exp.NewColumnListExpression(Star()))
	assert.Equal(t, ec, ds.ClearSelect().GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestSelectAppend(selects ...interface{}) {
	t := dt.T()
	ds := From("test").Select(C("a"))
	dsc := ds.GetClauses()
	ec := dsc.SelectAppend(exp.NewColumnListExpression(C("b")))
	assert.Equal(t, ec, ds.SelectAppend(C("b")).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestFrom(from ...interface{}) {
	t := dt.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetFrom(exp.NewColumnListExpression(T("t")))
	assert.Equal(t, ec, ds.From(T("t")).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestFromSelf() {
	t := dt.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetFrom(exp.NewColumnListExpression(ds.As("t1")))
	assert.Equal(t, ec, ds.FromSelf().GetClauses())

	ec2 := dsc.SetFrom(exp.NewColumnListExpression(ds.As("test")))
	assert.Equal(t, ec2, ds.As("test").FromSelf().GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestCompoundFromSelf() {
	t := dt.T()
	ds := From("test")
	dsc := ds.GetClauses()
	assert.Equal(t, dsc, ds.CompoundFromSelf().GetClauses())

	ds2 := ds.Limit(1)
	dsc2 := exp.NewClauses().SetFrom(exp.NewColumnListExpression(ds2.As("t1")))
	assert.Equal(t, dsc2, ds2.CompoundFromSelf().GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestJoin() {
	t := dt.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.JoinsAppend(
		exp.NewConditionedJoinExpression(exp.InnerJoinType, T("foo"), On(C("a").IsNull())),
	)
	assert.Equal(t, ec, ds.Join(T("foo"), On(C("a").IsNull())).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestInnerJoin() {
	t := dt.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.JoinsAppend(
		exp.NewConditionedJoinExpression(exp.InnerJoinType, T("foo"), On(C("a").IsNull())),
	)
	assert.Equal(t, ec, ds.InnerJoin(T("foo"), On(C("a").IsNull())).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestFullOuterJoin() {
	t := dt.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.JoinsAppend(
		exp.NewConditionedJoinExpression(exp.FullOuterJoinType, T("foo"), On(C("a").IsNull())),
	)
	assert.Equal(t, ec, ds.FullOuterJoin(T("foo"), On(C("a").IsNull())).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestRightOuterJoin() {
	t := dt.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.JoinsAppend(
		exp.NewConditionedJoinExpression(exp.RightOuterJoinType, T("foo"), On(C("a").IsNull())),
	)
	assert.Equal(t, ec, ds.RightOuterJoin(T("foo"), On(C("a").IsNull())).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestLeftOuterJoin() {
	t := dt.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.JoinsAppend(
		exp.NewConditionedJoinExpression(exp.LeftOuterJoinType, T("foo"), On(C("a").IsNull())),
	)
	assert.Equal(t, ec, ds.LeftOuterJoin(T("foo"), On(C("a").IsNull())).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestFullJoin() {
	t := dt.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.JoinsAppend(
		exp.NewConditionedJoinExpression(exp.FullJoinType, T("foo"), On(C("a").IsNull())),
	)
	assert.Equal(t, ec, ds.FullJoin(T("foo"), On(C("a").IsNull())).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestRightJoin() {
	t := dt.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.JoinsAppend(
		exp.NewConditionedJoinExpression(exp.RightJoinType, T("foo"), On(C("a").IsNull())),
	)
	assert.Equal(t, ec, ds.RightJoin(T("foo"), On(C("a").IsNull())).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestLeftJoin() {
	t := dt.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.JoinsAppend(
		exp.NewConditionedJoinExpression(exp.LeftJoinType, T("foo"), On(C("a").IsNull())),
	)
	assert.Equal(t, ec, ds.LeftJoin(T("foo"), On(C("a").IsNull())).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestNaturalJoin() {
	t := dt.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.JoinsAppend(
		exp.NewUnConditionedJoinExpression(exp.NaturalJoinType, T("foo")),
	)
	assert.Equal(t, ec, ds.NaturalJoin(T("foo")).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestNaturalLeftJoin() {
	t := dt.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.JoinsAppend(
		exp.NewUnConditionedJoinExpression(exp.NaturalLeftJoinType, T("foo")),
	)
	assert.Equal(t, ec, ds.NaturalLeftJoin(T("foo")).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestNaturalRightJoin() {
	t := dt.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.JoinsAppend(
		exp.NewUnConditionedJoinExpression(exp.NaturalRightJoinType, T("foo")),
	)
	assert.Equal(t, ec, ds.NaturalRightJoin(T("foo")).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}
func (dt *datasetTest) TestNaturalFullJoin() {
	t := dt.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.JoinsAppend(
		exp.NewUnConditionedJoinExpression(exp.NaturalFullJoinType, T("foo")),
	)
	assert.Equal(t, ec, ds.NaturalFullJoin(T("foo")).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestCrossJoin() {
	t := dt.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.JoinsAppend(
		exp.NewUnConditionedJoinExpression(exp.CrossJoinType, T("foo")),
	)
	assert.Equal(t, ec, ds.CrossJoin(T("foo")).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestWhere() {
	t := dt.T()
	ds := From("test")
	dsc := ds.GetClauses()
	w := Ex{
		"a": 1,
	}
	ec := dsc.WhereAppend(w)
	assert.Equal(t, ec, ds.Where(w).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestClearWhere() {
	t := dt.T()
	w := Ex{
		"a": 1,
	}
	ds := From("test").Where(w)
	dsc := ds.GetClauses()
	ec := dsc.ClearWhere()
	assert.Equal(t, ec, ds.ClearWhere().GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestForUpdate() {
	t := dt.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetLock(exp.NewLock(exp.ForUpdate, NoWait))
	assert.Equal(t, ec, ds.ForUpdate(NoWait).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestForNoKeyUpdate() {
	t := dt.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetLock(exp.NewLock(exp.ForNoKeyUpdate, NoWait))
	assert.Equal(t, ec, ds.ForNoKeyUpdate(NoWait).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestForKeyShare() {
	t := dt.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetLock(exp.NewLock(exp.ForKeyShare, NoWait))
	assert.Equal(t, ec, ds.ForKeyShare(NoWait).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestForShare() {
	t := dt.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetLock(exp.NewLock(exp.ForShare, NoWait))
	assert.Equal(t, ec, ds.ForShare(NoWait).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestGroupBy() {
	t := dt.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetGroupBy(exp.NewColumnListExpression(C("a")))
	assert.Equal(t, ec, ds.GroupBy("a").GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestHaving() {
	t := dt.T()
	ds := From("test")
	dsc := ds.GetClauses()
	h := C("a").Gt(1)
	ec := dsc.HavingAppend(h)
	assert.Equal(t, ec, ds.Having(h).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestOrder() {
	t := dt.T()
	ds := From("test")
	dsc := ds.GetClauses()
	o := C("a").Desc()
	ec := dsc.SetOrder(o)
	assert.Equal(t, ec, ds.Order(o).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestOrderAppend() {
	t := dt.T()
	ds := From("test").Order(C("a").Desc())
	dsc := ds.GetClauses()
	o := C("b").Desc()
	ec := dsc.OrderAppend(o)
	assert.Equal(t, ec, ds.OrderAppend(o).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestClearOrder() {
	t := dt.T()
	ds := From("test").Order(C("a").Desc())
	dsc := ds.GetClauses()
	ec := dsc.ClearOrder()
	assert.Equal(t, ec, ds.ClearOrder().GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestLimit() {
	t := dt.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetLimit(uint(1))
	assert.Equal(t, ec, ds.Limit(1).GetClauses())
	assert.Equal(t, dsc, ds.Limit(0).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestLimitAll() {
	t := dt.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetLimit(L("ALL"))
	assert.Equal(t, ec, ds.LimitAll().GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestClearLimit() {
	t := dt.T()
	ds := From("test").Limit(1)
	dsc := ds.GetClauses()
	ec := dsc.ClearLimit()
	assert.Equal(t, ec, ds.ClearLimit().GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestOffset() {
	t := dt.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetOffset(1)
	assert.Equal(t, ec, ds.Offset(1).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestClearOffset() {
	t := dt.T()
	ds := From("test").Offset(1)
	dsc := ds.GetClauses()
	ec := dsc.ClearOffset()
	assert.Equal(t, ec, ds.ClearOffset().GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestUnion() {
	t := dt.T()
	uds := From("union_test")
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.CompoundsAppend(exp.NewCompoundExpression(exp.UnionCompoundType, uds))
	assert.Equal(t, ec, ds.Union(uds).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestUnionAll() {
	t := dt.T()
	uds := From("union_test")
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.CompoundsAppend(exp.NewCompoundExpression(exp.UnionAllCompoundType, uds))
	assert.Equal(t, ec, ds.UnionAll(uds).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestIntersect() {
	t := dt.T()
	uds := From("union_test")
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.CompoundsAppend(exp.NewCompoundExpression(exp.IntersectCompoundType, uds))
	assert.Equal(t, ec, ds.Intersect(uds).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}
func (dt *datasetTest) TestIntersectAll() {
	t := dt.T()
	uds := From("union_test")
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.CompoundsAppend(exp.NewCompoundExpression(exp.IntersectAllCompoundType, uds))
	assert.Equal(t, ec, ds.IntersectAll(uds).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestReturning() {
	t := dt.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetReturning(exp.NewColumnListExpression(C("a")))
	assert.Equal(t, ec, ds.Returning("a").GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestAs() {
	t := dt.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetAlias(T("a"))
	assert.Equal(t, ec, ds.As("a").GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dt *datasetTest) TestToSQL() {
	t := dt.T()
	md := new(mocks.SQLDialect)
	ds := From("test").SetDialect(md)
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(false)
	md.On("ToSelectSQL", sqlB, c).Return(nil).Once()
	sql, args, err := ds.ToSQL()
	assert.Empty(t, sql)
	assert.Empty(t, args)
	assert.Nil(t, err)
	md.AssertExpectations(t)
}

func (dt *datasetTest) TestToSQL_ReturnedError() {
	t := dt.T()
	md := new(mocks.SQLDialect)
	ds := From("test").SetDialect(md)
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(false)
	ee := errors.New("expected error")
	md.On("ToSelectSQL", sqlB, c).Run(func(args mock.Arguments) {
		args.Get(0).(sb.SQLBuilder).SetError(ee)
	}).Once()

	sql, args, err := ds.ToSQL()
	assert.Empty(t, sql)
	assert.Empty(t, args)
	assert.Equal(t, ee, err)
	md.AssertExpectations(t)
}

func (dt *datasetTest) TestAppendSQL() {
	t := dt.T()
	md := new(mocks.SQLDialect)
	ds := From("test").SetDialect(md)
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(false)
	md.On("ToSelectSQL", sqlB, c).Return(nil).Once()
	ds.AppendSQL(sqlB)
	assert.NoError(t, sqlB.Error())
	md.AssertExpectations(t)
}

func (dt *datasetTest) TestToInsertSQL_WithNoArgs() {
	t := dt.T()
	md := new(mocks.SQLDialect)
	ds := From("test").SetDialect(md)
	c := ds.GetClauses()
	eie, err := exp.NewInsertExpression()
	assert.NoError(t, err)
	sqlB := sb.NewSQLBuilder(false)
	md.On("ToInsertSQL", sqlB, c, eie).Return(nil).Once()
	sql, args, err := ds.ToInsertSQL()
	assert.Empty(t, sql)
	assert.Empty(t, args)
	assert.Nil(t, err)
	md.AssertExpectations(t)
}

func (dt *datasetTest) TestToInsertSQL_WithReturnedError() {
	t := dt.T()
	md := new(mocks.SQLDialect)
	ds := From("test").SetDialect(md)
	c := ds.GetClauses()
	rows := []interface{}{
		Record{"c": "a"},
		Record{"c": "b"},
	}
	eie, err := exp.NewInsertExpression(rows...)
	assert.NoError(t, err)

	sqlB := sb.NewSQLBuilder(false)
	ee := errors.New("test")
	md.On("ToInsertSQL", sqlB, c, eie).Run(func(args mock.Arguments) {
		args.Get(0).(sb.SQLBuilder).SetError(ee)
	}).Once()
	sql, args, err := ds.ToInsertSQL(rows...)
	assert.Empty(t, sql)
	assert.Empty(t, args)
	assert.Equal(t, ee, err)
	md.AssertExpectations(t)
}

func (dt *datasetTest) TestToInsertIgnoreSQL() {
	t := dt.T()
	md := new(mocks.SQLDialect)
	ds := From("test").SetDialect(md)
	c := ds.GetClauses()
	rows := []interface{}{
		Record{"c": "a"},
		Record{"c": "b"},
	}
	eie, err := exp.NewInsertExpression(rows...)
	assert.NoError(t, err)
	eie = eie.DoNothing()
	sqlB := sb.NewSQLBuilder(false)
	md.On("ToInsertSQL", sqlB, c, eie).Return(nil).Once()
	sql, args, err := ds.ToInsertIgnoreSQL(rows...)
	assert.Empty(t, sql)
	assert.Empty(t, args)
	assert.Nil(t, err)
	md.AssertExpectations(t)
}

func (dt *datasetTest) TestToInsertConflictSQL() {
	t := dt.T()
	md := new(mocks.SQLDialect)
	ds := From("test").SetDialect(md)
	c := ds.GetClauses()
	ce := DoUpdate("a", "b")
	rows := []interface{}{
		Record{"c": "a"},
		Record{"c": "b"},
	}
	eie, err := exp.NewInsertExpression(rows...)
	assert.NoError(t, err)
	sqlB := sb.NewSQLBuilder(false)
	eie = eie.SetOnConflict(ce)
	md.On("ToInsertSQL", sqlB, c, eie).Return(nil).Once()
	sql, args, err := ds.ToInsertConflictSQL(ce, rows...)
	assert.Empty(t, sql)
	assert.Empty(t, args)
	assert.Nil(t, err)
	md.AssertExpectations(t)
}

func (dt *datasetTest) TestToUpdateSQL() {
	t := dt.T()
	md := new(mocks.SQLDialect)
	ds := From("test").SetDialect(md)
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(false)
	r := Record{"c": "a"}
	md.On("ToUpdateSQL", sqlB, c, r).Return(nil).Once()
	sql, args, err := ds.ToUpdateSQL(Record{"c": "a"})
	assert.Empty(t, sql)
	assert.Empty(t, args)
	assert.Nil(t, err)
	md.AssertExpectations(t)
}

func (dt *datasetTest) TestToUpdateSQL_Prepared() {
	t := dt.T()
	md := new(mocks.SQLDialect)
	ds := From("test").Prepared(true).SetDialect(md)
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(true)
	r := Record{"c": "a"}
	md.On("ToUpdateSQL", sqlB, c, r).Return(nil).Once()
	sql, args, err := ds.ToUpdateSQL(Record{"c": "a"})
	assert.Empty(t, sql)
	assert.Empty(t, args)
	assert.Nil(t, err)
	md.AssertExpectations(t)
}

func (dt *datasetTest) TestToUpdateSQL_WithError() {
	t := dt.T()
	md := new(mocks.SQLDialect)
	ds := From("test").SetDialect(md)
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(false)
	r := Record{"c": "a"}
	ee := errors.New("expected error")
	md.On("ToUpdateSQL", sqlB, c, r).Run(func(args mock.Arguments) {
		args.Get(0).(sb.SQLBuilder).SetError(ee)
	}).Once()

	sql, args, err := ds.ToUpdateSQL(Record{"c": "a"})
	assert.Empty(t, sql)
	assert.Empty(t, args)
	assert.Equal(t, ee, err)
	md.AssertExpectations(t)
}

func (dt *datasetTest) TestToDeleteSQL() {
	t := dt.T()
	md := new(mocks.SQLDialect)
	ds := From("test").SetDialect(md)
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(false)
	md.On("ToDeleteSQL", sqlB, c).Return(nil).Once()

	sql, args, err := ds.ToDeleteSQL()
	assert.Empty(t, sql)
	assert.Empty(t, args)
	assert.Nil(t, err)
	md.AssertExpectations(t)
}

func (dt *datasetTest) TestToDeleteSQL_Prepared() {
	t := dt.T()
	md := new(mocks.SQLDialect)
	ds := From("test").Prepared(true).SetDialect(md)
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(true)
	md.On("ToDeleteSQL", sqlB, c).Return(nil).Once()

	sql, args, err := ds.ToDeleteSQL()
	assert.Empty(t, sql)
	assert.Empty(t, args)
	assert.Nil(t, err)
	md.AssertExpectations(t)
}

func (dt *datasetTest) TestToDeleteSQL_WithError() {
	t := dt.T()
	md := new(mocks.SQLDialect)
	ds := From("test").SetDialect(md)
	c := ds.GetClauses()
	ee := errors.New("expected error")
	sqlB := sb.NewSQLBuilder(false)
	md.On("ToDeleteSQL", sqlB, c).Run(func(args mock.Arguments) {
		args.Get(0).(sb.SQLBuilder).SetError(ee)
	}).Once()

	sql, args, err := ds.ToDeleteSQL()
	assert.Empty(t, sql)
	assert.Empty(t, args)
	assert.Equal(t, ee, err)
	md.AssertExpectations(t)
}

func (dt *datasetTest) TestToTruncateSQL() {
	t := dt.T()
	md := new(mocks.SQLDialect)
	ds := From("test").SetDialect(md)
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(false)
	md.On("ToTruncateSQL", sqlB, c, TruncateOptions{}).Return(nil).Once()

	sql, args, err := ds.ToTruncateSQL()
	assert.Empty(t, sql)
	assert.Empty(t, args)
	assert.Nil(t, err)
	md.AssertExpectations(t)
}

func (dt *datasetTest) TestToTruncateSQL__Prepared() {
	t := dt.T()
	md := new(mocks.SQLDialect)
	ds := From("test").Prepared(true).SetDialect(md)
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(true)
	md.On("ToTruncateSQL", sqlB, c, TruncateOptions{}).Return(nil).Once()

	sql, args, err := ds.ToTruncateSQL()
	assert.Empty(t, sql)
	assert.Empty(t, args)
	assert.Nil(t, err)
	md.AssertExpectations(t)
}

func (dt *datasetTest) TestToTruncateSQL_WithError() {
	t := dt.T()
	md := new(mocks.SQLDialect)
	ds := From("test").SetDialect(md)
	c := ds.GetClauses()
	ee := errors.New("expected error")
	sqlB := sb.NewSQLBuilder(false)
	md.On("ToTruncateSQL", sqlB, c, TruncateOptions{}).Run(func(args mock.Arguments) {
		args.Get(0).(sb.SQLBuilder).SetError(ee)
	}).Once()

	sql, args, err := ds.ToTruncateSQL()
	assert.Empty(t, sql)
	assert.Empty(t, args)
	assert.Equal(t, ee, err)
	md.AssertExpectations(t)
}

func (dt *datasetTest) TestToTruncateWithOptsSQL() {
	t := dt.T()
	md := new(mocks.SQLDialect)
	ds := From("test").SetDialect(md)
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(false)
	to := TruncateOptions{Cascade: true}
	md.On("ToTruncateSQL", sqlB, c, to).Return(nil).Once()

	sql, args, err := ds.ToTruncateWithOptsSQL(to)
	assert.Empty(t, sql)
	assert.Empty(t, args)
	assert.Nil(t, err)
	md.AssertExpectations(t)
}

func (dt *datasetTest) TestToTruncateWithOptsSQL_Prepared() {
	t := dt.T()
	md := new(mocks.SQLDialect)
	ds := From("test").Prepared(true).SetDialect(md)
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(true)
	to := TruncateOptions{Cascade: true}
	md.On("ToTruncateSQL", sqlB, c, to).Return(nil).Once()

	sql, args, err := ds.ToTruncateWithOptsSQL(to)
	assert.Empty(t, sql)
	assert.Empty(t, args)
	assert.Nil(t, err)
	md.AssertExpectations(t)
}

func (dt *datasetTest) TestToTruncateWithOptsSQL_WithError() {
	t := dt.T()
	md := new(mocks.SQLDialect)
	ds := From("test").SetDialect(md)
	c := ds.GetClauses()
	ee := errors.New("expected error")
	to := TruncateOptions{Cascade: true}
	sqlB := sb.NewSQLBuilder(false)
	md.On("ToTruncateSQL", sqlB, c, to).Run(func(args mock.Arguments) {
		args.Get(0).(sb.SQLBuilder).SetError(ee)
	}).Once()

	sql, args, err := ds.ToTruncateWithOptsSQL(to)
	assert.Empty(t, sql)
	assert.Empty(t, args)
	assert.Equal(t, ee, err)
	md.AssertExpectations(t)
}

func TestDatasetSuite(t *testing.T) {
	suite.Run(t, new(datasetTest))
}
