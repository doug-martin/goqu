package goqu

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/doug-martin/goqu/v9/exec"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/doug-martin/goqu/v9/internal/errors"
	"github.com/doug-martin/goqu/v9/internal/sb"
	"github.com/doug-martin/goqu/v9/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type (
	deleteTestCase struct {
		ds      *DeleteDataset
		clauses exp.DeleteClauses
	}
	deleteDatasetSuite struct {
		suite.Suite
	}
)

func (dds *deleteDatasetSuite) assertCases(cases ...deleteTestCase) {
	for _, s := range cases {
		dds.Equal(s.clauses, s.ds.GetClauses())
	}
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
	md := new(mocks.SQLDialect)
	ds = ds.SetDialect(md)

	dialect := GetDialect("default")
	dialectDs := ds.WithDialect("default")
	dds.Equal(md, ds.Dialect())
	dds.Equal(dialect, dialectDs.Dialect())
}

func (dds *deleteDatasetSuite) TestPrepared() {
	ds := Delete("test")
	preparedDs := ds.Prepared(true)
	dds.True(preparedDs.IsPrepared())
	dds.False(ds.IsPrepared())
	// should apply the prepared to any datasets created from the root
	dds.True(preparedDs.Where(Ex{"a": 1}).IsPrepared())
}

func (dds *deleteDatasetSuite) TestGetClauses() {
	ds := Delete("test")
	ce := exp.NewDeleteClauses().SetFrom(I("test"))
	dds.Equal(ce, ds.GetClauses())
}

func (dds *deleteDatasetSuite) TestWith() {
	from := From("cte")
	bd := Delete("items")
	dds.assertCases(
		deleteTestCase{
			ds: bd.With("test-cte", from),
			clauses: exp.NewDeleteClauses().SetFrom(C("items")).
				CommonTablesAppend(exp.NewCommonTableExpression(false, "test-cte", from)),
		},
		deleteTestCase{
			ds:      bd,
			clauses: exp.NewDeleteClauses().SetFrom(C("items")),
		},
	)
}

func (dds *deleteDatasetSuite) TestWithRecursive() {
	from := From("cte")
	bd := Delete("items")
	dds.assertCases(
		deleteTestCase{
			ds: bd.WithRecursive("test-cte", from),
			clauses: exp.NewDeleteClauses().SetFrom(C("items")).
				CommonTablesAppend(exp.NewCommonTableExpression(true, "test-cte", from)),
		},
		deleteTestCase{
			ds:      bd,
			clauses: exp.NewDeleteClauses().SetFrom(C("items")),
		},
	)
}

func (dds *deleteDatasetSuite) TestFrom_withIdentifier() {
	bd := Delete("items")
	dds.assertCases(
		deleteTestCase{
			ds:      bd.From("items2"),
			clauses: exp.NewDeleteClauses().SetFrom(C("items2")),
		},
		deleteTestCase{
			ds:      bd.From(C("items2")),
			clauses: exp.NewDeleteClauses().SetFrom(C("items2")),
		},
		deleteTestCase{
			ds:      bd.From(T("items2")),
			clauses: exp.NewDeleteClauses().SetFrom(T("items2")),
		},
		deleteTestCase{
			ds:      bd.From("schema.table"),
			clauses: exp.NewDeleteClauses().SetFrom(I("schema.table")),
		},
		deleteTestCase{
			ds:      bd,
			clauses: exp.NewDeleteClauses().SetFrom(C("items")),
		},
	)

	dds.PanicsWithValue(errBadFromArgument, func() {
		Delete("test").From(true)
	})
}

func (dds *deleteDatasetSuite) TestWhere() {
	bd := Delete("items")
	dds.assertCases(
		deleteTestCase{
			ds: bd.Where(Ex{"a": 1}),
			clauses: exp.NewDeleteClauses().
				SetFrom(C("items")).
				WhereAppend(Ex{"a": 1}),
		},
		deleteTestCase{
			ds: bd.Where(Ex{"a": 1}).Where(C("b").Eq("c")),
			clauses: exp.NewDeleteClauses().
				SetFrom(C("items")).
				WhereAppend(Ex{"a": 1}).
				WhereAppend(C("b").Eq("c")),
		},
		deleteTestCase{
			ds:      bd,
			clauses: exp.NewDeleteClauses().SetFrom(C("items")),
		},
	)
}

func (dds *deleteDatasetSuite) TestClearWhere() {
	bd := Delete("items").Where(Ex{"a": 1})
	dds.assertCases(
		deleteTestCase{
			ds: bd.ClearWhere(),
			clauses: exp.NewDeleteClauses().
				SetFrom(C("items")),
		},
		deleteTestCase{
			ds: bd,
			clauses: exp.NewDeleteClauses().
				SetFrom(C("items")).
				WhereAppend(Ex{"a": 1}),
		},
	)
}

func (dds *deleteDatasetSuite) TestOrder() {
	bd := Delete("items")
	dds.assertCases(
		deleteTestCase{
			ds: bd.Order(C("a").Asc()),
			clauses: exp.NewDeleteClauses().
				SetFrom(C("items")).
				SetOrder(C("a").Asc()),
		},
		deleteTestCase{
			ds: bd.Order(C("a").Asc()).Order(C("b").Desc()),
			clauses: exp.NewDeleteClauses().
				SetFrom(C("items")).
				SetOrder(C("b").Desc()),
		},
		deleteTestCase{
			ds: bd.Order(C("a").Asc(), C("b").Desc()),
			clauses: exp.NewDeleteClauses().
				SetFrom(C("items")).
				SetOrder(C("a").Asc(), C("b").Desc()),
		},
		deleteTestCase{
			ds:      bd,
			clauses: exp.NewDeleteClauses().SetFrom(C("items")),
		},
	)
}

func (dds *deleteDatasetSuite) TestOrderAppend() {
	bd := Delete("items").Order(C("a").Asc())
	dds.assertCases(
		deleteTestCase{
			ds: bd.OrderAppend(C("b").Desc()),
			clauses: exp.NewDeleteClauses().
				SetFrom(C("items")).
				SetOrder(C("a").Asc(), C("b").Desc()),
		},
		deleteTestCase{
			ds: bd,
			clauses: exp.NewDeleteClauses().
				SetFrom(C("items")).
				SetOrder(C("a").Asc()),
		},
	)
}

func (dds *deleteDatasetSuite) TestOrderPrepend() {
	bd := Delete("items").Order(C("a").Asc())
	dds.assertCases(
		deleteTestCase{
			ds: bd.OrderPrepend(C("b").Desc()),
			clauses: exp.NewDeleteClauses().
				SetFrom(C("items")).
				SetOrder(C("b").Desc(), C("a").Asc()),
		},
		deleteTestCase{
			ds: bd,
			clauses: exp.NewDeleteClauses().
				SetFrom(C("items")).
				SetOrder(C("a").Asc()),
		},
	)
}

func (dds *deleteDatasetSuite) TestClearOrder() {
	bd := Delete("items").Order(C("a").Asc())
	dds.assertCases(
		deleteTestCase{
			ds:      bd.ClearOrder(),
			clauses: exp.NewDeleteClauses().SetFrom(C("items")),
		},
		deleteTestCase{
			ds: bd,
			clauses: exp.NewDeleteClauses().
				SetFrom(C("items")).
				SetOrder(C("a").Asc()),
		},
	)
}

func (dds *deleteDatasetSuite) TestLimit() {
	bd := Delete("test")
	dds.assertCases(
		deleteTestCase{
			ds: bd.Limit(10),
			clauses: exp.NewDeleteClauses().
				SetFrom(C("test")).
				SetLimit(uint(10)),
		},
		deleteTestCase{
			ds:      bd.Limit(0),
			clauses: exp.NewDeleteClauses().SetFrom(C("test")),
		},
		deleteTestCase{
			ds: bd.Limit(10).Limit(2),
			clauses: exp.NewDeleteClauses().
				SetFrom(C("test")).
				SetLimit(uint(2)),
		},
		deleteTestCase{
			ds:      bd.Limit(10).Limit(0),
			clauses: exp.NewDeleteClauses().SetFrom(C("test")),
		},
		deleteTestCase{
			ds:      bd,
			clauses: exp.NewDeleteClauses().SetFrom(C("test")),
		},
	)
}

func (dds *deleteDatasetSuite) TestLimitAll() {
	bd := Delete("test")
	dds.assertCases(
		deleteTestCase{
			ds: bd.LimitAll(),
			clauses: exp.NewDeleteClauses().
				SetFrom(C("test")).
				SetLimit(L("ALL")),
		},
		deleteTestCase{
			ds: bd.Limit(10).LimitAll(),
			clauses: exp.NewDeleteClauses().
				SetFrom(C("test")).
				SetLimit(L("ALL")),
		},
		deleteTestCase{
			ds:      bd,
			clauses: exp.NewDeleteClauses().SetFrom(C("test")),
		},
	)
}

func (dds *deleteDatasetSuite) TestClearLimit() {
	bd := Delete("test").Limit(10)
	dds.assertCases(
		deleteTestCase{
			ds:      bd.ClearLimit(),
			clauses: exp.NewDeleteClauses().SetFrom(C("test")),
		},
		deleteTestCase{
			ds:      bd,
			clauses: exp.NewDeleteClauses().SetFrom(C("test")).SetLimit(uint(10)),
		},
	)
}

func (dds *deleteDatasetSuite) TestReturning() {
	bd := Delete("items")
	dds.assertCases(
		deleteTestCase{
			ds: bd.Returning("a"),
			clauses: exp.NewDeleteClauses().
				SetFrom(C("items")).
				SetReturning(exp.NewColumnListExpression("a")),
		},
		deleteTestCase{
			ds: bd.Returning(),
			clauses: exp.NewDeleteClauses().
				SetFrom(C("items")).
				SetReturning(exp.NewColumnListExpression()),
		},
		deleteTestCase{
			ds: bd.Returning(nil),
			clauses: exp.NewDeleteClauses().
				SetFrom(C("items")).
				SetReturning(exp.NewColumnListExpression()),
		},
		deleteTestCase{
			ds: bd.Returning("a").Returning("b"),
			clauses: exp.NewDeleteClauses().
				SetFrom(C("items")).
				SetReturning(exp.NewColumnListExpression("b")),
		},
		deleteTestCase{
			ds:      bd,
			clauses: exp.NewDeleteClauses().SetFrom(C("items")),
		},
	)
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

func (dds *deleteDatasetSuite) TestSetError() {

	err1 := errors.New("error #1")
	err2 := errors.New("error #2")
	err3 := errors.New("error #3")

	// Verify initial error set/get works properly
	md := new(mocks.SQLDialect)
	ds := Delete("test").SetDialect(md)
	ds = ds.SetError(err1)
	dds.Equal(err1, ds.Error())
	sql, args, err := ds.ToSQL()
	dds.Empty(sql)
	dds.Empty(args)
	dds.Equal(err1, err)

	// Repeated SetError calls on Dataset should not overwrite the original error
	ds = ds.SetError(err2)
	dds.Equal(err1, ds.Error())
	sql, args, err = ds.ToSQL()
	dds.Empty(sql)
	dds.Empty(args)
	dds.Equal(err1, err)

	// Builder functions should not lose the error
	ds = ds.ClearLimit()
	dds.Equal(err1, ds.Error())
	sql, args, err = ds.ToSQL()
	dds.Empty(sql)
	dds.Empty(args)
	dds.Equal(err1, err)

	// Deeper errors inside SQL generation should still return original error
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(false)
	md.On("ToDeleteSQL", sqlB, c).Run(func(args mock.Arguments) {
		args.Get(0).(sb.SQLBuilder).SetError(err3)
	}).Once()

	sql, args, err = ds.ToSQL()
	dds.Empty(sql)
	dds.Empty(args)
	dds.Equal(err1, err)
}

func TestDeleteDataset(t *testing.T) {
	suite.Run(t, new(deleteDatasetSuite))
}
