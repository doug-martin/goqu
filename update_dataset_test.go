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

type (
	updateTestCase struct {
		ds      *UpdateDataset
		clauses exp.UpdateClauses
	}
	updateDatasetSuite struct {
		suite.Suite
	}
)

func (uds *updateDatasetSuite) assertCases(cases ...updateTestCase) {
	for _, s := range cases {
		uds.Equal(s.clauses, s.ds.GetClauses())
	}
}

func (uds *updateDatasetSuite) TestClone() {
	ds := Update("test")
	uds.Equal(ds, ds.Clone())
}

func (uds *updateDatasetSuite) TestExpression() {
	ds := Update("test")
	uds.Equal(ds, ds.Expression())
}

func (uds *updateDatasetSuite) TestDialect() {
	ds := Update("test")
	uds.NotNil(ds.Dialect())
}

func (uds *updateDatasetSuite) TestWithDialect() {
	ds := Update("test")
	md := new(mocks.SQLDialect)
	ds = ds.SetDialect(md)

	dialect := GetDialect("default")
	dialectDs := ds.WithDialect("default")
	uds.Equal(md, ds.Dialect())
	uds.Equal(dialect, dialectDs.Dialect())
}

func (uds *updateDatasetSuite) TestPrepared() {
	ds := Update("test")
	preparedDs := ds.Prepared(true)
	uds.True(preparedDs.IsPrepared())
	uds.False(ds.IsPrepared())
	// should apply the prepared to any datasets created from the root
	uds.True(preparedDs.Where(Ex{"a": 1}).IsPrepared())
}

func (uds *updateDatasetSuite) TestGetClauses() {
	ds := Update("test")
	ce := exp.NewUpdateClauses().SetTable(I("test"))
	uds.Equal(ce, ds.GetClauses())
}

func (uds *updateDatasetSuite) TestWith() {
	from := Update("cte")
	bd := Update("items")
	uds.assertCases(
		updateTestCase{
			ds: bd.With("test-cte", from),
			clauses: exp.NewUpdateClauses().
				SetTable(C("items")).
				CommonTablesAppend(exp.NewCommonTableExpression(false, "test-cte", from)),
		},
		updateTestCase{
			ds:      bd,
			clauses: exp.NewUpdateClauses().SetTable(C("items")),
		},
	)
}

func (uds *updateDatasetSuite) TestWithRecursive() {
	from := Update("cte")
	bd := Update("items")
	uds.assertCases(
		updateTestCase{
			ds: bd.WithRecursive("test-cte", from),
			clauses: exp.NewUpdateClauses().
				SetTable(C("items")).
				CommonTablesAppend(exp.NewCommonTableExpression(true, "test-cte", from)),
		},
		updateTestCase{
			ds:      bd,
			clauses: exp.NewUpdateClauses().SetTable(C("items")),
		},
	)
}

func (uds *updateDatasetSuite) TestTable() {
	bd := Update("items")
	uds.assertCases(
		updateTestCase{
			ds:      bd.Table("items2"),
			clauses: exp.NewUpdateClauses().SetTable(C("items2")),
		},
		updateTestCase{
			ds:      bd.Table(L("literal_table")),
			clauses: exp.NewUpdateClauses().SetTable(L("literal_table")),
		},
		updateTestCase{
			ds:      bd,
			clauses: exp.NewUpdateClauses().SetTable(C("items")),
		},
	)
	uds.PanicsWithValue(errUnsupportedUpdateTableType, func() {
		bd.Table(true)
	})
}

func (uds *updateDatasetSuite) TestSet() {
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	bd := Update("items")
	uds.assertCases(
		updateTestCase{
			ds: bd.Set(item{Name: "Test", Address: "111 Test Addr"}),
			clauses: exp.NewUpdateClauses().
				SetTable(C("items")).
				SetSetValues(item{Name: "Test", Address: "111 Test Addr"}),
		},
		updateTestCase{
			ds: bd.Set(Record{"name": "Test", "address": "111 Test Addr"}),
			clauses: exp.NewUpdateClauses().
				SetTable(C("items")).
				SetSetValues(Record{"name": "Test", "address": "111 Test Addr"}),
		},
		updateTestCase{
			ds: bd,
			clauses: exp.NewUpdateClauses().
				SetTable(C("items")),
		},
	)
}

func (uds *updateDatasetSuite) TestFrom() {
	bd := Update("items")
	uds.assertCases(
		updateTestCase{
			ds: bd.From("other"),
			clauses: exp.NewUpdateClauses().
				SetTable(C("items")).
				SetFrom(exp.NewColumnListExpression("other")),
		},
		updateTestCase{
			ds: bd.From("other").From("other2"),
			clauses: exp.NewUpdateClauses().
				SetTable(C("items")).
				SetFrom(exp.NewColumnListExpression("other2")),
		},
		updateTestCase{
			ds: bd,
			clauses: exp.NewUpdateClauses().
				SetTable(C("items")),
		},
	)
}

func (uds *updateDatasetSuite) TestWhere() {
	bd := Update("items")
	uds.assertCases(
		updateTestCase{
			ds: bd.Where(Ex{"a": 1}),
			clauses: exp.NewUpdateClauses().
				SetTable(C("items")).
				WhereAppend(Ex{"a": 1}),
		},
		updateTestCase{
			ds: bd.Where(Ex{"a": 1}).Where(C("b").Eq("c")),
			clauses: exp.NewUpdateClauses().
				SetTable(C("items")).
				WhereAppend(Ex{"a": 1}).WhereAppend(C("b").Eq("c")),
		},
		updateTestCase{
			ds: bd,
			clauses: exp.NewUpdateClauses().
				SetTable(C("items")),
		},
	)
}

func (uds *updateDatasetSuite) TestClearWhere() {
	bd := Update("items").Where(Ex{"a": 1})
	uds.assertCases(
		updateTestCase{
			ds:      bd.ClearWhere(),
			clauses: exp.NewUpdateClauses().SetTable(C("items")),
		},
		updateTestCase{
			ds: bd,
			clauses: exp.NewUpdateClauses().
				SetTable(C("items")).
				WhereAppend(Ex{"a": 1}),
		},
	)
}

func (uds *updateDatasetSuite) TestOrder() {
	bd := Update("items")
	uds.assertCases(
		updateTestCase{
			ds: bd.Order(C("a").Desc()),
			clauses: exp.NewUpdateClauses().
				SetTable(C("items")).OrderAppend(C("a").Desc()),
		},
		updateTestCase{
			ds: bd.Order(C("a").Desc()).Order(C("b").Asc()),
			clauses: exp.NewUpdateClauses().
				SetTable(C("items")).
				OrderAppend(C("b").Asc()),
		},
		updateTestCase{
			ds:      bd,
			clauses: exp.NewUpdateClauses().SetTable(C("items")),
		},
	)
}

func (uds *updateDatasetSuite) TestOrderAppend() {
	bd := Update("items").Order(C("a").Desc())
	uds.assertCases(
		updateTestCase{
			ds: bd.OrderAppend(C("b").Asc()),
			clauses: exp.NewUpdateClauses().
				SetTable(C("items")).
				OrderAppend(C("a").Desc()).
				OrderAppend(C("b").Asc()),
		},
		updateTestCase{
			ds: bd,
			clauses: exp.NewUpdateClauses().
				SetTable(C("items")).
				OrderAppend(C("a").Desc()),
		},
	)
}
func (uds *updateDatasetSuite) TestOrderPrepend() {
	bd := Update("items").Order(C("a").Desc())
	uds.assertCases(
		updateTestCase{
			ds: bd.OrderPrepend(C("b").Asc()),
			clauses: exp.NewUpdateClauses().
				SetTable(C("items")).
				OrderAppend(C("b").Asc()).
				OrderAppend(C("a").Desc()),
		},
		updateTestCase{
			ds: bd,
			clauses: exp.NewUpdateClauses().
				SetTable(C("items")).
				OrderAppend(C("a").Desc()),
		},
	)
}

func (uds *updateDatasetSuite) TestClearOrder() {
	bd := Update("items").Order(C("a").Desc())
	uds.assertCases(
		updateTestCase{
			ds:      bd.ClearOrder(),
			clauses: exp.NewUpdateClauses().SetTable(C("items")),
		},
		updateTestCase{
			ds: bd,
			clauses: exp.NewUpdateClauses().
				SetTable(C("items")).
				OrderAppend(C("a").Desc()),
		},
	)
}

func (uds *updateDatasetSuite) TestLimit() {
	bd := Update("items")
	uds.assertCases(
		updateTestCase{
			ds:      bd.Limit(10),
			clauses: exp.NewUpdateClauses().SetTable(C("items")).SetLimit(uint(10)),
		},
		updateTestCase{
			ds:      bd.Limit(0),
			clauses: exp.NewUpdateClauses().SetTable(C("items")),
		},
		updateTestCase{
			ds:      bd,
			clauses: exp.NewUpdateClauses().SetTable(C("items")),
		},
	)
}

func (uds *updateDatasetSuite) TestLimitAll() {
	bd := Update("items")
	uds.assertCases(
		updateTestCase{
			ds:      bd.LimitAll(),
			clauses: exp.NewUpdateClauses().SetTable(C("items")).SetLimit(L("ALL")),
		},
		updateTestCase{
			ds:      bd,
			clauses: exp.NewUpdateClauses().SetTable(C("items")),
		},
	)
}
func (uds *updateDatasetSuite) TestClearLimit() {
	bd := Update("items")
	uds.assertCases(
		updateTestCase{
			ds:      bd.LimitAll().ClearLimit(),
			clauses: exp.NewUpdateClauses().SetTable(C("items")),
		},
		updateTestCase{
			ds:      bd.Limit(10).ClearLimit(),
			clauses: exp.NewUpdateClauses().SetTable(C("items")),
		},
		updateTestCase{
			ds:      bd,
			clauses: exp.NewUpdateClauses().SetTable(C("items")),
		},
	)
}

func (uds *updateDatasetSuite) TestReturning() {
	bd := Update("items")
	uds.assertCases(
		updateTestCase{
			ds: bd.Returning("a", "b"),
			clauses: exp.NewUpdateClauses().
				SetTable(C("items")).
				SetReturning(exp.NewColumnListExpression("a", "b")),
		},
		updateTestCase{
			ds: bd.Returning(),
			clauses: exp.NewUpdateClauses().
				SetTable(C("items")).
				SetReturning(exp.NewColumnListExpression()),
		},
		updateTestCase{
			ds: bd.Returning(nil),
			clauses: exp.NewUpdateClauses().
				SetTable(C("items")).
				SetReturning(exp.NewColumnListExpression()),
		},
		updateTestCase{
			ds: bd.Returning("a", "b").Returning("c"),
			clauses: exp.NewUpdateClauses().
				SetTable(C("items")).
				SetReturning(exp.NewColumnListExpression("c")),
		},
		updateTestCase{
			ds:      bd,
			clauses: exp.NewUpdateClauses().SetTable(C("items")),
		},
	)
}

func (uds *updateDatasetSuite) TestToSQL() {
	md := new(mocks.SQLDialect)
	ds := Update("test").SetDialect(md)
	r := Record{"c": "a"}
	c := ds.GetClauses().SetSetValues(r)
	sqlB := sb.NewSQLBuilder(false)
	md.On("ToUpdateSQL", sqlB, c).Return(nil).Once()
	updateSQL, args, err := ds.Set(r).ToSQL()
	uds.Empty(updateSQL)
	uds.Empty(args)
	uds.Nil(err)
	md.AssertExpectations(uds.T())
}

func (uds *updateDatasetSuite) TestToSQL_Prepared() {
	md := new(mocks.SQLDialect)
	ds := Update("test").Prepared(true).SetDialect(md)
	r := Record{"c": "a"}
	c := ds.GetClauses().SetSetValues(r)
	sqlB := sb.NewSQLBuilder(true)
	md.On("ToUpdateSQL", sqlB, c).Return(nil).Once()
	updateSQL, args, err := ds.Set(Record{"c": "a"}).ToSQL()
	uds.Empty(updateSQL)
	uds.Empty(args)
	uds.Nil(err)
	md.AssertExpectations(uds.T())
}

func (uds *updateDatasetSuite) TestToSQL_WithError() {
	md := new(mocks.SQLDialect)
	ds := Update("test").SetDialect(md)
	r := Record{"c": "a"}
	c := ds.GetClauses().SetSetValues(r)
	sqlB := sb.NewSQLBuilder(false)
	ee := errors.New("expected error")
	md.On("ToUpdateSQL", sqlB, c).Run(func(args mock.Arguments) {
		args.Get(0).(sb.SQLBuilder).SetError(ee)
	}).Once()

	updateSQL, args, err := ds.Set(Record{"c": "a"}).ToSQL()
	uds.Empty(updateSQL)
	uds.Empty(args)
	uds.Equal(ee, err)
	md.AssertExpectations(uds.T())
}

func (uds *updateDatasetSuite) TestExecutor() {
	mDb, _, err := sqlmock.New()
	uds.NoError(err)
	ds := newUpdateDataset("mock", exec.NewQueryFactory(mDb)).
		Table("items").
		Set(Record{"address": "111 Test Addr", "name": "Test1"}).
		Where(C("name").IsNull())

	updateSQL, args, err := ds.Executor().ToSQL()
	uds.NoError(err)
	uds.Empty(args)
	uds.Equal(`UPDATE "items" SET "address"='111 Test Addr',"name"='Test1' WHERE ("name" IS NULL)`, updateSQL)

	updateSQL, args, err = ds.Prepared(true).Executor().ToSQL()
	uds.NoError(err)
	uds.Equal([]interface{}{"111 Test Addr", "Test1", nil}, args)
	uds.Equal(`UPDATE "items" SET "address"=?,"name"=? WHERE ("name" IS ?)`, updateSQL)
}

func (uds *updateDatasetSuite) TestSetError() {

	err1 := errors.New("error #1")
	err2 := errors.New("error #2")
	err3 := errors.New("error #3")

	// Verify initial error set/get works properly
	md := new(mocks.SQLDialect)
	ds := Update("test").SetDialect(md)
	ds = ds.SetError(err1)
	uds.Equal(err1, ds.Error())
	sql, args, err := ds.ToSQL()
	uds.Empty(sql)
	uds.Empty(args)
	uds.Equal(err1, err)

	// Repeated SetError calls on Dataset should not overwrite the original error
	ds = ds.SetError(err2)
	uds.Equal(err1, ds.Error())
	sql, args, err = ds.ToSQL()
	uds.Empty(sql)
	uds.Empty(args)
	uds.Equal(err1, err)

	// Builder functions should not lose the error
	ds = ds.ClearLimit()
	uds.Equal(err1, ds.Error())
	sql, args, err = ds.ToSQL()
	uds.Empty(sql)
	uds.Empty(args)
	uds.Equal(err1, err)

	// Deeper errors inside SQL generation should still return original error
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(false)
	md.On("ToUpdateSQL", sqlB, c).Run(func(args mock.Arguments) {
		args.Get(0).(sb.SQLBuilder).SetError(err3)
	}).Once()

	sql, args, err = ds.ToSQL()
	uds.Empty(sql)
	uds.Empty(args)
	uds.Equal(err1, err)
}

func TestUpdateDataset(t *testing.T) {
	suite.Run(t, new(updateDatasetSuite))
}
