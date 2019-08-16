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

type deleteDatasetSuite struct {
	suite.Suite
}

func (dds *deleteDatasetSuite) SetupSuite() {
	noReturn := DefaultDialectOptions()
	noReturn.SupportsReturn = false
	RegisterDialect("no-return", noReturn)

	limitOnDelete := DefaultDialectOptions()
	limitOnDelete.SupportsLimitOnDelete = true
	RegisterDialect("limit-on-delete", limitOnDelete)

	orderOnDelete := DefaultDialectOptions()
	orderOnDelete.SupportsOrderByOnDelete = true
	RegisterDialect("order-on-delete", orderOnDelete)
}

func (dds *deleteDatasetSuite) TearDownSuite() {
	DeregisterDialect("no-return")
	DeregisterDialect("limit-on-delete")
	DeregisterDialect("order-on-delete")
}

func (dds *deleteDatasetSuite) TestDialect() {
	t := dds.T()
	ds := Delete("test")
	assert.NotNil(t, ds.Dialect())
}

func (dds *deleteDatasetSuite) TestWithDialect() {
	t := dds.T()
	ds := Delete("test")
	md := new(mocks.SQLDialect)
	ds = ds.SetDialect(md)

	dialect := GetDialect("default")
	ds = ds.WithDialect("default")
	assert.Equal(t, ds.Dialect(), dialect)
}

func (dds *deleteDatasetSuite) TestPrepared() {
	t := dds.T()
	ds := Delete("test")
	preparedDs := ds.Prepared(true)
	assert.True(t, preparedDs.IsPrepared())
	assert.False(t, ds.IsPrepared())
	// should apply the prepared to any datasets created from the root
	assert.True(t, preparedDs.Where(Ex{"a": 1}).IsPrepared())
}

func (dds *deleteDatasetSuite) TestPrepared_ToSQL() {
	t := dds.T()
	ds1 := Delete("items")
	dsql, args, err := ds1.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{})
	assert.Equal(t, `DELETE FROM "items"`, dsql)

	dsql, args, err = ds1.Where(I("id").Eq(1)).Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1)})
	assert.Equal(t, `DELETE FROM "items" WHERE ("id" = ?)`, dsql)

	dsql, args, err = ds1.Returning("id").Where(I("id").Eq(1)).Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1)})
	assert.Equal(t, `DELETE FROM "items" WHERE ("id" = ?) RETURNING "id"`, dsql)
}

func (dds *deleteDatasetSuite) TestGetClauses() {
	t := dds.T()
	ds := Delete("test")
	ce := exp.NewDeleteClauses().SetFrom(I("test"))
	assert.Equal(t, ce, ds.GetClauses())
}

func (dds *deleteDatasetSuite) TestWith() {
	t := dds.T()
	from := From("cte")
	ds := Delete("test")
	dsc := ds.GetClauses()
	ec := dsc.CommonTablesAppend(exp.NewCommonTableExpression(false, "test-cte", from))
	assert.Equal(t, ec, ds.With("test-cte", from).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dds *deleteDatasetSuite) TestWithRecursive() {
	t := dds.T()
	from := From("cte")
	ds := Delete("test")
	dsc := ds.GetClauses()
	ec := dsc.CommonTablesAppend(exp.NewCommonTableExpression(true, "test-cte", from))
	assert.Equal(t, ec, ds.WithRecursive("test-cte", from).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dds *deleteDatasetSuite) TestFrom() {
	t := dds.T()
	ds := Delete("test")
	dsc := ds.GetClauses()
	ec := dsc.SetFrom(T("t"))
	assert.Equal(t, ec, ds.From(T("t")).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dds *deleteDatasetSuite) TestFrom_ToSQL() {
	t := dds.T()
	ds1 := Delete("test")

	deleteSQL, _, err := ds1.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, deleteSQL, `DELETE FROM "test"`)

	ds2 := ds1.From("test2")
	deleteSQL, _, err = ds2.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, deleteSQL, `DELETE FROM "test2"`)

	// original should not change
	deleteSQL, _, err = ds1.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, deleteSQL, `DELETE FROM "test"`)

}

func (dds *deleteDatasetSuite) TestWhere() {
	t := dds.T()
	ds := Delete("test")
	dsc := ds.GetClauses()
	w := Ex{
		"a": 1,
	}
	ec := dsc.WhereAppend(w)
	assert.Equal(t, ec, ds.Where(w).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dds *deleteDatasetSuite) TestWhere_ToSQL() {
	t := dds.T()
	ds1 := Delete("test")

	b := ds1.Where(
		C("a").Eq(true),
		C("a").Neq(true),
		C("a").Eq(false),
		C("a").Neq(false),
	)
	deleteSQL, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, deleteSQL, `DELETE FROM "test" `+
		`WHERE (("a" IS TRUE) AND ("a" IS NOT TRUE) AND ("a" IS FALSE) AND ("a" IS NOT FALSE))`)

	deleteSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, deleteSQL, `DELETE FROM "test" `+
		`WHERE (("a" IS TRUE) AND ("a" IS NOT TRUE) AND ("a" IS FALSE) AND ("a" IS NOT FALSE))`)

	b = ds1.Where(
		C("a").Eq("a"),
		C("b").Neq("b"),
		C("c").Gt("c"),
		C("d").Gte("d"),
		C("e").Lt("e"),
		C("f").Lte("f"),
	)
	deleteSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, deleteSQL, `DELETE FROM "test" `+
		`WHERE (("a" = 'a') AND ("b" != 'b') AND ("c" > 'c') AND ("d" >= 'd') AND ("e" < 'e') AND ("f" <= 'f'))`)

	deleteSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{"a", "b", "c", "d", "e", "f"}, args)
	assert.Equal(t, deleteSQL, `DELETE FROM "test" `+
		`WHERE (("a" = ?) AND ("b" != ?) AND ("c" > ?) AND ("d" >= ?) AND ("e" < ?) AND ("f" <= ?))`)

	b = ds1.Where(
		C("a").Eq(From("test2").Select("id")),
	)
	deleteSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, deleteSQL, `DELETE FROM "test" WHERE ("a" IN (SELECT "id" FROM "test2"))`)

	deleteSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, deleteSQL, `DELETE FROM "test" WHERE ("a" IN (SELECT "id" FROM "test2"))`)

	b = ds1.Where(Ex{
		"a": "a",
		"b": Op{"neq": "b"},
		"c": Op{"gt": "c"},
		"d": Op{"gte": "d"},
		"e": Op{"lt": "e"},
		"f": Op{"lte": "f"},
	})
	deleteSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, deleteSQL, `DELETE FROM "test" `+
		`WHERE (("a" = 'a') AND ("b" != 'b') AND ("c" > 'c') AND ("d" >= 'd') AND ("e" < 'e') AND ("f" <= 'f'))`)

	deleteSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{"a", "b", "c", "d", "e", "f"}, args)
	assert.Equal(t, deleteSQL, `DELETE FROM "test" `+
		`WHERE (("a" = ?) AND ("b" != ?) AND ("c" > ?) AND ("d" >= ?) AND ("e" < ?) AND ("f" <= ?))`)

	b = ds1.Where(Ex{
		"a": From("test2").Select("id"),
	})
	deleteSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, deleteSQL, `DELETE FROM "test" WHERE ("a" IN (SELECT "id" FROM "test2"))`)
	deleteSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, deleteSQL, `DELETE FROM "test" WHERE ("a" IN (SELECT "id" FROM "test2"))`)
}

func (dds *deleteDatasetSuite) TestWhere_chainToSQL() {
	t := dds.T()
	ds1 := Delete("test").Where(
		C("x").Eq(0),
		C("y").Eq(1),
	)

	ds2 := ds1.Where(
		C("z").Eq(2),
	)

	a := ds2.Where(
		C("a").Eq("A"),
	)
	b := ds2.Where(
		C("b").Eq("B"),
	)
	deleteSQL, _, err := a.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, deleteSQL, `DELETE FROM "test" `+
		`WHERE (("x" = 0) AND ("y" = 1) AND ("z" = 2) AND ("a" = 'A'))`)
	deleteSQL, _, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, deleteSQL, `DELETE FROM "test" `+
		`WHERE (("x" = 0) AND ("y" = 1) AND ("z" = 2) AND ("b" = 'B'))`)
}

func (dds *deleteDatasetSuite) TestWhere_emptyToSQL() {
	t := dds.T()
	ds1 := Delete("test")

	b := ds1.Where()
	deleteSQL, _, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, deleteSQL, `DELETE FROM "test"`)
}

func (dds *deleteDatasetSuite) TestClearWhere() {
	t := dds.T()
	w := Ex{
		"a": 1,
	}
	ds := Delete("test").Where(w)
	dsc := ds.GetClauses()
	ec := dsc.ClearWhere()
	assert.Equal(t, ec, ds.ClearWhere().GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dds *deleteDatasetSuite) TestClearWhere_ToSQL() {
	t := dds.T()
	ds1 := Delete("test")

	b := ds1.Where(
		C("a").Eq(1),
	).ClearWhere()
	deleteSQL, _, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, deleteSQL, `DELETE FROM "test"`)
}

func (dds *deleteDatasetSuite) TestOrder() {
	t := dds.T()
	ds := Delete("test")
	dsc := ds.GetClauses()
	o := C("a").Desc()
	ec := dsc.SetOrder(o)
	assert.Equal(t, ec, ds.Order(o).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}
func (dds *deleteDatasetSuite) TestOrder_ToSQL() {
	t := dds.T()

	ds1 := Delete("test").WithDialect("order-on-delete")

	b := ds1.Order(C("a").Asc(), L(`("a" + "b" > 2)`).Asc())
	deleteSQL, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, deleteSQL, `DELETE FROM "test" ORDER BY "a" ASC, ("a" + "b" > 2) ASC`)

	deleteSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, deleteSQL, `DELETE FROM "test" ORDER BY "a" ASC, ("a" + "b" > 2) ASC`)
}

func (dds *deleteDatasetSuite) TestOrderAppend() {
	t := dds.T()
	ds := Delete("test").Order(C("a").Desc())
	dsc := ds.GetClauses()
	o := C("b").Desc()
	ec := dsc.OrderAppend(o)
	assert.Equal(t, ec, ds.OrderAppend(o).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dds *deleteDatasetSuite) TestOrderAppend_ToSQL() {
	t := dds.T()
	ds := Delete("test").WithDialect("order-on-delete")
	b := ds.Order(C("a").Asc().NullsFirst()).OrderAppend(C("b").Desc().NullsLast())
	deleteSQL, _, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, deleteSQL, `DELETE FROM "test" ORDER BY "a" ASC NULLS FIRST, "b" DESC NULLS LAST`)

	b = ds.OrderAppend(C("a").Asc().NullsFirst()).OrderAppend(C("b").Desc().NullsLast())
	deleteSQL, _, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, deleteSQL, `DELETE FROM "test" ORDER BY "a" ASC NULLS FIRST, "b" DESC NULLS LAST`)

}

func (dds *deleteDatasetSuite) TestClearOrder() {
	t := dds.T()
	ds := Delete("test").Order(C("a").Desc())
	dsc := ds.GetClauses()
	ec := dsc.ClearOrder()
	assert.Equal(t, ec, ds.ClearOrder().GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dds *deleteDatasetSuite) TestClearOrder_ToSQL() {
	t := dds.T()
	ds := Delete("test").WithDialect("order-on-delete")
	b := ds.Order(C("a").Asc().NullsFirst()).ClearOrder()
	deleteSQL, _, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, deleteSQL, `DELETE FROM "test"`)
}

func (dds *deleteDatasetSuite) TestLimit() {
	t := dds.T()
	ds := Delete("test")
	dsc := ds.GetClauses()
	ec := dsc.SetLimit(uint(1))
	assert.Equal(t, ec, ds.Limit(1).GetClauses())
	assert.Equal(t, dsc, ds.Limit(0).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dds *deleteDatasetSuite) TestLimit_ToSQL() {
	t := dds.T()
	ds1 := Delete("test").WithDialect("limit-on-delete")

	b := ds1.Where(C("a").Gt(1)).Limit(10)
	deleteSQL, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, deleteSQL, `DELETE FROM "test" WHERE ("a" > 1) LIMIT 10`)

	deleteSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(1), int64(10)}, args)
	assert.Equal(t, deleteSQL, `DELETE FROM "test" WHERE ("a" > ?) LIMIT ?`)

	b = ds1.Where(C("a").Gt(1)).Limit(0)
	deleteSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, deleteSQL, `DELETE FROM "test" WHERE ("a" > 1)`)

	deleteSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(1)}, args)
	assert.Equal(t, deleteSQL, `DELETE FROM "test" WHERE ("a" > ?)`)
}

func (dds *deleteDatasetSuite) TestLimitAll() {
	t := dds.T()
	ds := Delete("test")
	dsc := ds.GetClauses()
	ec := dsc.SetLimit(L("ALL"))
	assert.Equal(t, ec, ds.LimitAll().GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dds *deleteDatasetSuite) TestLimitAll_ToSQL() {
	t := dds.T()
	ds1 := Delete("test").WithDialect("limit-on-delete")

	b := ds1.Where(C("a").Gt(1)).LimitAll()

	deleteSQL, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, deleteSQL, `DELETE FROM "test" WHERE ("a" > 1) LIMIT ALL`)

	deleteSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(1)}, args)
	assert.Equal(t, deleteSQL, `DELETE FROM "test" WHERE ("a" > ?) LIMIT ALL`)

	b = ds1.Where(C("a").Gt(1)).Limit(0).LimitAll()
	deleteSQL, _, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, deleteSQL, `DELETE FROM "test" WHERE ("a" > 1) LIMIT ALL`)

	deleteSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(1)}, args)
	assert.Equal(t, deleteSQL, `DELETE FROM "test" WHERE ("a" > ?) LIMIT ALL`)
}

func (dds *deleteDatasetSuite) TestClearLimit() {
	t := dds.T()
	ds := Delete("test").Limit(1)
	dsc := ds.GetClauses()
	ec := dsc.ClearLimit()
	assert.Equal(t, ec, ds.ClearLimit().GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dds *deleteDatasetSuite) TestClearLimit_ToSQL() {
	t := dds.T()
	ds1 := Delete("test")

	b := ds1.Where(C("a").Gt(1)).LimitAll().ClearLimit()
	deleteSQL, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, deleteSQL, `DELETE FROM "test" WHERE ("a" > 1)`)

	deleteSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(1)}, args)
	assert.Equal(t, deleteSQL, `DELETE FROM "test" WHERE ("a" > ?)`)

	b = ds1.Where(C("a").Gt(1)).Limit(10).ClearLimit()
	deleteSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, deleteSQL, `DELETE FROM "test" WHERE ("a" > 1)`)
	deleteSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(1)}, args)
	assert.Equal(t, deleteSQL, `DELETE FROM "test" WHERE ("a" > ?)`)
}

func (dds *deleteDatasetSuite) TestReturning() {
	t := dds.T()
	ds := Delete("test")
	dsc := ds.GetClauses()
	ec := dsc.SetReturning(exp.NewColumnListExpression(C("a")))
	assert.Equal(t, ec, ds.Returning("a").GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (dds *deleteDatasetSuite) TestReturning_ToSQL() {
	t := dds.T()
	ds := Delete("test")
	b := ds.Returning("a")

	deleteSQL, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, deleteSQL, `DELETE FROM "test" RETURNING "a"`)

	deleteSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, deleteSQL, `DELETE FROM "test" RETURNING "a"`)
}

func (dds *deleteDatasetSuite) TestToSQL() {
	t := dds.T()
	md := new(mocks.SQLDialect)
	ds := Delete("test").SetDialect(md)
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(false)
	md.On("ToDeleteSQL", sqlB, c).Return(nil).Once()

	sql, args, err := ds.ToSQL()
	assert.Empty(t, sql)
	assert.Empty(t, args)
	assert.Nil(t, err)
	md.AssertExpectations(t)
}

func (dds *deleteDatasetSuite) TestToSQL_Prepared() {
	t := dds.T()
	md := new(mocks.SQLDialect)
	ds := Delete("test").Prepared(true).SetDialect(md)
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(true)
	md.On("ToDeleteSQL", sqlB, c).Return(nil).Once()

	sql, args, err := ds.ToSQL()
	assert.Empty(t, sql)
	assert.Empty(t, args)
	assert.Nil(t, err)
	md.AssertExpectations(t)
}

func (dds *deleteDatasetSuite) TestToSQL_WithError() {
	t := dds.T()
	md := new(mocks.SQLDialect)
	ds := Delete("test").SetDialect(md)
	c := ds.GetClauses()
	ee := errors.New("expected error")
	sqlB := sb.NewSQLBuilder(false)
	md.On("ToDeleteSQL", sqlB, c).Run(func(args mock.Arguments) {
		args.Get(0).(sb.SQLBuilder).SetError(ee)
	}).Once()

	sql, args, err := ds.ToSQL()
	assert.Empty(t, sql)
	assert.Empty(t, args)
	assert.Equal(t, ee, err)
	md.AssertExpectations(t)
}

func (dds *deleteDatasetSuite) TestExecutor() {
	t := dds.T()
	mDb, _, err := sqlmock.New()
	assert.NoError(t, err)

	qf := exec.NewQueryFactory(mDb)
	ds := newDeleteDataset("mock", qf).From("items").Where(Ex{"id": Op{"gt": 10}})

	dsql, args, err := ds.Executor().ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `DELETE FROM "items" WHERE ("id" > 10)`, dsql)

	dsql, args, err = ds.Prepared(true).Executor().ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(10)}, args)
	assert.Equal(t, `DELETE FROM "items" WHERE ("id" > ?)`, dsql)
}

func TestDeleteDataset(t *testing.T) {
	suite.Run(t, new(deleteDatasetSuite))
}
