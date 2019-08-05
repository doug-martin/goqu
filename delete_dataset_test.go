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
	ds := Delete("test")
	dds.NotNil(ds.Dialect())
}

func (dds *deleteDatasetSuite) TestWithDialect() {
	ds := Delete("test")
	dialect := GetDialect("default")
	ds.WithDialect("default")
	dds.Equal(dialect, ds.Dialect())
}

func (dds *deleteDatasetSuite) TestPrepared() {
	ds := Delete("test")
	preparedDs := ds.Prepared(true)
	dds.True(preparedDs.IsPrepared())
	dds.False(ds.IsPrepared())
	// should apply the prepared to any datasets created from the root
	dds.True(preparedDs.Where(Ex{"a": 1}).IsPrepared())
}

func (dds *deleteDatasetSuite) TestPrepared_ToSQL() {
	ds1 := Delete("items")
	dsql, args, err := ds1.Prepared(true).ToSQL()
	dds.NoError(err)
	dds.Empty(args)
	dds.Equal(`DELETE FROM "items"`, dsql)

	dsql, args, err = ds1.Where(I("id").Eq(1)).Prepared(true).ToSQL()
	dds.NoError(err)
	dds.Equal([]interface{}{int64(1)}, args)
	dds.Equal(`DELETE FROM "items" WHERE ("id" = ?)`, dsql)

	dsql, args, err = ds1.Returning("id").Where(I("id").Eq(1)).Prepared(true).ToSQL()
	dds.NoError(err)
	dds.Equal([]interface{}{int64(1)}, args)
	dds.Equal(`DELETE FROM "items" WHERE ("id" = ?) RETURNING "id"`, dsql)
}

func (dds *deleteDatasetSuite) TestGetClauses() {
	ds := Delete("test")
	ce := exp.NewDeleteClauses().SetFrom(I("test"))
	dds.Equal(ce, ds.GetClauses())
}

func (dds *deleteDatasetSuite) TestWith() {
	from := From("cte")
	ds := Delete("test")
	dsc := ds.GetClauses()
	ec := dsc.CommonTablesAppend(exp.NewCommonTableExpression(false, "test-cte", from))
	dds.Equal(ec, ds.With("test-cte", from).GetClauses())
	dds.Equal(dsc, ds.GetClauses())
}

func (dds *deleteDatasetSuite) TestWithRecursive() {
	from := From("cte")
	ds := Delete("test")
	dsc := ds.GetClauses()
	ec := dsc.CommonTablesAppend(exp.NewCommonTableExpression(true, "test-cte", from))
	dds.Equal(ec, ds.WithRecursive("test-cte", from).GetClauses())
	dds.Equal(dsc, ds.GetClauses())
}

func (dds *deleteDatasetSuite) TestFrom() {
	ds := Delete("test")
	dsc := ds.GetClauses()
	ec := dsc.SetFrom(T("t"))
	dds.Equal(ec, ds.From(T("t")).GetClauses())
	dds.Equal(dsc, ds.GetClauses())
}

func (dds *deleteDatasetSuite) TestFrom_ToSQL() {
	ds1 := Delete("test")

	deleteSQL, _, err := ds1.ToSQL()
	dds.NoError(err)
	dds.Equal(`DELETE FROM "test"`, deleteSQL)

	ds2 := ds1.From("test2")
	deleteSQL, _, err = ds2.ToSQL()
	dds.NoError(err)
	dds.Equal(`DELETE FROM "test2"`, deleteSQL)

	// original should not change
	deleteSQL, _, err = ds1.ToSQL()
	dds.NoError(err)
	dds.Equal(`DELETE FROM "test"`, deleteSQL)

}

func (dds *deleteDatasetSuite) TestWhere() {
	ds := Delete("test")
	dsc := ds.GetClauses()
	w := Ex{
		"a": 1,
	}
	ec := dsc.WhereAppend(w)
	dds.Equal(ec, ds.Where(w).GetClauses())
	dds.Equal(dsc, ds.GetClauses())
}

func (dds *deleteDatasetSuite) TestWhere_ToSQL() {
	ds1 := Delete("test")

	b := ds1.Where(
		C("a").Eq(true),
		C("a").Neq(true),
		C("a").Eq(false),
		C("a").Neq(false),
	)
	deleteSQL, args, err := b.ToSQL()
	dds.NoError(err)
	dds.Empty(args)
	dds.Equal(
		`DELETE FROM "test" WHERE (("a" IS TRUE) AND ("a" IS NOT TRUE) AND ("a" IS FALSE) AND ("a" IS NOT FALSE))`,
		deleteSQL,
	)

	deleteSQL, args, err = b.Prepared(true).ToSQL()
	dds.NoError(err)
	dds.Empty(args)
	dds.Equal(
		`DELETE FROM "test" WHERE (("a" IS TRUE) AND ("a" IS NOT TRUE) AND ("a" IS FALSE) AND ("a" IS NOT FALSE))`,
		deleteSQL,
	)

	b = ds1.Where(
		C("a").Eq("a"),
		C("b").Neq("b"),
		C("c").Gt("c"),
		C("d").Gte("d"),
		C("e").Lt("e"),
		C("f").Lte("f"),
	)
	deleteSQL, args, err = b.ToSQL()
	dds.NoError(err)
	dds.Empty(args)
	dds.Equal(
		`DELETE FROM "test" `+
			`WHERE (("a" = 'a') AND ("b" != 'b') AND ("c" > 'c') AND ("d" >= 'd') AND ("e" < 'e') AND ("f" <= 'f'))`,
		deleteSQL,
	)

	deleteSQL, args, err = b.Prepared(true).ToSQL()
	dds.NoError(err)
	dds.Equal([]interface{}{"a", "b", "c", "d", "e", "f"}, args)
	dds.Equal(
		`DELETE FROM "test" `+
			`WHERE (("a" = ?) AND ("b" != ?) AND ("c" > ?) AND ("d" >= ?) AND ("e" < ?) AND ("f" <= ?))`,
		deleteSQL,
	)

	b = ds1.Where(
		C("a").Eq(From("test2").Select("id")),
	)
	deleteSQL, args, err = b.ToSQL()
	dds.NoError(err)
	dds.Empty(args)
	dds.Equal(
		`DELETE FROM "test" WHERE ("a" IN (SELECT "id" FROM "test2"))`,
		deleteSQL,
	)

	deleteSQL, args, err = b.Prepared(true).ToSQL()
	dds.NoError(err)
	dds.Empty(args)
	dds.Equal(
		`DELETE FROM "test" WHERE ("a" IN (SELECT "id" FROM "test2"))`,
		deleteSQL,
	)

	b = ds1.Where(Ex{
		"a": "a",
		"b": Op{"neq": "b"},
		"c": Op{"gt": "c"},
		"d": Op{"gte": "d"},
		"e": Op{"lt": "e"},
		"f": Op{"lte": "f"},
	})
	deleteSQL, args, err = b.ToSQL()
	dds.NoError(err)
	dds.Empty(args)
	dds.Equal(`DELETE FROM "test" `+
		`WHERE (("a" = 'a') AND ("b" != 'b') AND ("c" > 'c') AND ("d" >= 'd') AND ("e" < 'e') AND ("f" <= 'f'))`,
		deleteSQL,
	)

	deleteSQL, args, err = b.Prepared(true).ToSQL()
	dds.NoError(err)
	dds.Equal([]interface{}{"a", "b", "c", "d", "e", "f"}, args)
	dds.Equal(
		`DELETE FROM "test" `+
			`WHERE (("a" = ?) AND ("b" != ?) AND ("c" > ?) AND ("d" >= ?) AND ("e" < ?) AND ("f" <= ?))`,
		deleteSQL,
	)

	b = ds1.Where(Ex{
		"a": From("test2").Select("id"),
	})
	deleteSQL, args, err = b.ToSQL()
	dds.NoError(err)
	dds.Empty(args)
	dds.Equal(`DELETE FROM "test" WHERE ("a" IN (SELECT "id" FROM "test2"))`, deleteSQL)
	deleteSQL, args, err = b.Prepared(true).ToSQL()
	dds.NoError(err)
	dds.Empty(args)
	dds.Equal(`DELETE FROM "test" WHERE ("a" IN (SELECT "id" FROM "test2"))`, deleteSQL)
}

func (dds *deleteDatasetSuite) TestWhere_chainToSQL() {
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
	dds.NoError(err)
	dds.Equal(
		`DELETE FROM "test" WHERE (("x" = 0) AND ("y" = 1) AND ("z" = 2) AND ("a" = 'A'))`,
		deleteSQL,
	)
	deleteSQL, _, err = b.ToSQL()
	dds.NoError(err)
	dds.Equal(
		`DELETE FROM "test" WHERE (("x" = 0) AND ("y" = 1) AND ("z" = 2) AND ("b" = 'B'))`,
		deleteSQL,
	)
}

func (dds *deleteDatasetSuite) TestWhere_emptyToSQL() {
	ds1 := Delete("test")

	b := ds1.Where()
	deleteSQL, _, err := b.ToSQL()
	dds.NoError(err)
	dds.Equal(`DELETE FROM "test"`, deleteSQL)
}

func (dds *deleteDatasetSuite) TestClearWhere() {
	w := Ex{
		"a": 1,
	}
	ds := Delete("test").Where(w)
	dsc := ds.GetClauses()
	ec := dsc.ClearWhere()
	dds.Equal(ec, ds.ClearWhere().GetClauses())
	dds.Equal(dsc, ds.GetClauses())
}

func (dds *deleteDatasetSuite) TestClearWhere_ToSQL() {
	ds1 := Delete("test")

	b := ds1.Where(
		C("a").Eq(1),
	).ClearWhere()
	deleteSQL, _, err := b.ToSQL()
	dds.NoError(err)
	dds.Equal(`DELETE FROM "test"`, deleteSQL)
}

func (dds *deleteDatasetSuite) TestOrder() {
	ds := Delete("test")
	dsc := ds.GetClauses()
	o := C("a").Desc()
	ec := dsc.SetOrder(o)
	dds.Equal(ec, ds.Order(o).GetClauses())
	dds.Equal(dsc, ds.GetClauses())
}
func (dds *deleteDatasetSuite) TestOrder_ToSQL() {

	ds1 := Delete("test").WithDialect("order-on-delete")

	b := ds1.Order(C("a").Asc(), L(`("a" + "b" > 2)`).Asc())
	deleteSQL, args, err := b.ToSQL()
	dds.NoError(err)
	dds.Empty(args)
	dds.Equal(`DELETE FROM "test" ORDER BY "a" ASC, ("a" + "b" > 2) ASC`, deleteSQL)

	deleteSQL, args, err = b.Prepared(true).ToSQL()
	dds.NoError(err)
	dds.Empty(args)
	dds.Equal(`DELETE FROM "test" ORDER BY "a" ASC, ("a" + "b" > 2) ASC`, deleteSQL)
}

func (dds *deleteDatasetSuite) TestOrderAppend() {
	ds := Delete("test").Order(C("a").Desc())
	dsc := ds.GetClauses()
	o := C("b").Desc()
	ec := dsc.OrderAppend(o)
	dds.Equal(ec, ds.OrderAppend(o).GetClauses())
	dds.Equal(dsc, ds.GetClauses())
}

func (dds *deleteDatasetSuite) TestOrderAppend_ToSQL() {
	ds := Delete("test").WithDialect("order-on-delete")
	b := ds.Order(C("a").Asc().NullsFirst()).OrderAppend(C("b").Desc().NullsLast())
	deleteSQL, _, err := b.ToSQL()
	dds.NoError(err)
	dds.Equal(`DELETE FROM "test" ORDER BY "a" ASC NULLS FIRST, "b" DESC NULLS LAST`, deleteSQL)

	b = ds.OrderAppend(C("a").Asc().NullsFirst()).OrderAppend(C("b").Desc().NullsLast())
	deleteSQL, _, err = b.ToSQL()
	dds.NoError(err)
	dds.Equal(`DELETE FROM "test" ORDER BY "a" ASC NULLS FIRST, "b" DESC NULLS LAST`, deleteSQL)

}

func (dds *deleteDatasetSuite) TestClearOrder() {
	ds := Delete("test").Order(C("a").Desc())
	dsc := ds.GetClauses()
	ec := dsc.ClearOrder()
	dds.Equal(ec, ds.ClearOrder().GetClauses())
	dds.Equal(dsc, ds.GetClauses())
}

func (dds *deleteDatasetSuite) TestClearOrder_ToSQL() {
	ds := Delete("test").WithDialect("order-on-delete")
	b := ds.Order(C("a").Asc().NullsFirst()).ClearOrder()
	deleteSQL, _, err := b.ToSQL()
	dds.NoError(err)
	dds.Equal(`DELETE FROM "test"`, deleteSQL)
}

func (dds *deleteDatasetSuite) TestLimit() {
	ds := Delete("test")
	dsc := ds.GetClauses()
	ec := dsc.SetLimit(uint(1))
	dds.Equal(ec, ds.Limit(1).GetClauses())
	dds.Equal(dsc, ds.Limit(0).GetClauses())
	dds.Equal(dsc, ds.GetClauses())
}

func (dds *deleteDatasetSuite) TestLimit_ToSQL() {
	ds1 := Delete("test").WithDialect("limit-on-delete")

	b := ds1.Where(C("a").Gt(1)).Limit(10)
	deleteSQL, args, err := b.ToSQL()
	dds.NoError(err)
	dds.Empty(args)
	dds.Equal(`DELETE FROM "test" WHERE ("a" > 1) LIMIT 10`, deleteSQL)

	deleteSQL, args, err = b.Prepared(true).ToSQL()
	dds.NoError(err)
	dds.Equal([]interface{}{int64(1), int64(10)}, args)
	dds.Equal(`DELETE FROM "test" WHERE ("a" > ?) LIMIT ?`, deleteSQL)

	b = ds1.Where(C("a").Gt(1)).Limit(0)
	deleteSQL, args, err = b.ToSQL()
	dds.NoError(err)
	dds.Empty(args)
	dds.Equal(`DELETE FROM "test" WHERE ("a" > 1)`, deleteSQL)

	deleteSQL, args, err = b.Prepared(true).ToSQL()
	dds.NoError(err)
	dds.Equal([]interface{}{int64(1)}, args)
	dds.Equal(`DELETE FROM "test" WHERE ("a" > ?)`, deleteSQL)
}

func (dds *deleteDatasetSuite) TestLimitAll() {
	ds := Delete("test")
	dsc := ds.GetClauses()
	ec := dsc.SetLimit(L("ALL"))
	dds.Equal(ec, ds.LimitAll().GetClauses())
	dds.Equal(dsc, ds.GetClauses())
}

func (dds *deleteDatasetSuite) TestLimitAll_ToSQL() {
	ds1 := Delete("test").WithDialect("limit-on-delete")

	b := ds1.Where(C("a").Gt(1)).LimitAll()

	deleteSQL, args, err := b.ToSQL()
	dds.NoError(err)
	dds.Empty(args)
	dds.Equal(`DELETE FROM "test" WHERE ("a" > 1) LIMIT ALL`, deleteSQL)

	deleteSQL, args, err = b.Prepared(true).ToSQL()
	dds.NoError(err)
	dds.Equal([]interface{}{int64(1)}, args)
	dds.Equal(`DELETE FROM "test" WHERE ("a" > ?) LIMIT ALL`, deleteSQL)

	b = ds1.Where(C("a").Gt(1)).Limit(0).LimitAll()
	deleteSQL, _, err = b.ToSQL()
	dds.NoError(err)
	dds.Equal(`DELETE FROM "test" WHERE ("a" > 1) LIMIT ALL`, deleteSQL)

	deleteSQL, args, err = b.Prepared(true).ToSQL()
	dds.NoError(err)
	dds.Equal([]interface{}{int64(1)}, args)
	dds.Equal(`DELETE FROM "test" WHERE ("a" > ?) LIMIT ALL`, deleteSQL)
}

func (dds *deleteDatasetSuite) TestClearLimit() {
	ds := Delete("test").Limit(1)
	dsc := ds.GetClauses()
	ec := dsc.ClearLimit()
	dds.Equal(ec, ds.ClearLimit().GetClauses())
	dds.Equal(dsc, ds.GetClauses())
}

func (dds *deleteDatasetSuite) TestClearLimit_ToSQL() {
	ds1 := Delete("test")

	b := ds1.Where(C("a").Gt(1)).LimitAll().ClearLimit()
	deleteSQL, args, err := b.ToSQL()
	dds.NoError(err)
	dds.Empty(args)
	dds.Equal(`DELETE FROM "test" WHERE ("a" > 1)`, deleteSQL)

	deleteSQL, args, err = b.Prepared(true).ToSQL()
	dds.NoError(err)
	dds.Equal([]interface{}{int64(1)}, args)
	dds.Equal(`DELETE FROM "test" WHERE ("a" > ?)`, deleteSQL)

	b = ds1.Where(C("a").Gt(1)).Limit(10).ClearLimit()
	deleteSQL, args, err = b.ToSQL()
	dds.NoError(err)
	dds.Empty(args)
	dds.Equal(`DELETE FROM "test" WHERE ("a" > 1)`, deleteSQL)
	deleteSQL, args, err = b.Prepared(true).ToSQL()
	dds.NoError(err)
	dds.Equal([]interface{}{int64(1)}, args)
	dds.Equal(`DELETE FROM "test" WHERE ("a" > ?)`, deleteSQL)
}

func (dds *deleteDatasetSuite) TestReturning() {
	ds := Delete("test")
	dsc := ds.GetClauses()
	ec := dsc.SetReturning(exp.NewColumnListExpression(C("a")))
	dds.Equal(ec, ds.Returning("a").GetClauses())
	dds.Equal(dsc, ds.GetClauses())
}

func (dds *deleteDatasetSuite) TestReturning_ToSQL() {
	ds := Delete("test")
	b := ds.Returning("a")

	deleteSQL, args, err := b.ToSQL()
	dds.NoError(err)
	dds.Empty(args)
	dds.Equal(`DELETE FROM "test" RETURNING "a"`, deleteSQL)

	deleteSQL, args, err = b.Prepared(true).ToSQL()
	dds.NoError(err)
	dds.Empty(args)
	dds.Equal(`DELETE FROM "test" RETURNING "a"`, deleteSQL)
}

func (dds *deleteDatasetSuite) TestToSQL() {
	md := new(mocks.SQLDialect)
	ds := Delete("test").SetDialect(md)
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(false)
	md.On("ToDeleteSQL", sqlB, c).Return(nil).Once()

	sql, args, err := ds.ToSQL()
	dds.Empty(sql)
	dds.Empty(args)
	dds.Nil(err)
	md.AssertExpectations(dds.T())
}

func (dds *deleteDatasetSuite) TestToSQL_Prepared() {
	md := new(mocks.SQLDialect)
	ds := Delete("test").Prepared(true).SetDialect(md)
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(true)
	md.On("ToDeleteSQL", sqlB, c).Return(nil).Once()

	sql, args, err := ds.ToSQL()
	dds.Empty(sql)
	dds.Empty(args)
	dds.Nil(err)
	md.AssertExpectations(dds.T())
}

func (dds *deleteDatasetSuite) TestToSQL_WithError() {
	md := new(mocks.SQLDialect)
	ds := Delete("test").SetDialect(md)
	c := ds.GetClauses()
	ee := errors.New("expected error")
	sqlB := sb.NewSQLBuilder(false)
	md.On("ToDeleteSQL", sqlB, c).Run(func(args mock.Arguments) {
		args.Get(0).(sb.SQLBuilder).SetError(ee)
	}).Once()

	sql, args, err := ds.ToSQL()
	dds.Empty(sql)
	dds.Empty(args)
	dds.Equal(ee, err)
	md.AssertExpectations(dds.T())
}

func (dds *deleteDatasetSuite) TestExecutor() {
	mDb, _, err := sqlmock.New()
	dds.NoError(err)

	qf := exec.NewQueryFactory(mDb)
	ds := newDeleteDataset("mock", qf).From("items").Where(Ex{"id": Op{"gt": 10}})

	dsql, args, err := ds.Executor().ToSQL()
	dds.NoError(err)
	dds.Empty(args)
	dds.Equal(`DELETE FROM "items" WHERE ("id" > 10)`, dsql)

	dsql, args, err = ds.Prepared(true).Executor().ToSQL()
	dds.NoError(err)
	dds.Equal([]interface{}{int64(10)}, args)
	dds.Equal(`DELETE FROM "items" WHERE ("id" > ?)`, dsql)
}

func TestDeleteDataset(t *testing.T) {
	suite.Run(t, new(deleteDatasetSuite))
}
