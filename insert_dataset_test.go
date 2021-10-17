package goqu_test

import (
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/doug-martin/goqu/v9/internal/errors"
	"github.com/doug-martin/goqu/v9/internal/sb"
	"github.com/doug-martin/goqu/v9/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type (
	insertTestCase struct {
		ds      *goqu.InsertDataset
		clauses exp.InsertClauses
	}
	insertDatasetSuite struct {
		suite.Suite
	}
)

func (ids *insertDatasetSuite) assertCases(cases ...insertTestCase) {
	for _, s := range cases {
		ids.Equal(s.clauses, s.ds.GetClauses())
	}
}

func (ids *insertDatasetSuite) TestInsert() {
	ds := goqu.Insert("test")
	ids.IsType(&goqu.InsertDataset{}, ds)
	ids.Implements((*exp.Expression)(nil), ds)
	ids.Implements((*exp.AppendableExpression)(nil), ds)
}

func (ids *insertDatasetSuite) TestClone() {
	ds := goqu.Insert("test")
	ids.Equal(ds.Clone(), ds)
}

func (ids *insertDatasetSuite) TestExpression() {
	ds := goqu.Insert("test")
	ids.Equal(ds.Expression(), ds)
}

func (ids *insertDatasetSuite) TestDialect() {
	ds := goqu.Insert("test")
	ids.NotNil(ds.Dialect())
}

func (ids *insertDatasetSuite) TestWithDialect() {
	ds := goqu.Insert("test")
	md := new(mocks.SQLDialect)
	ds = ds.SetDialect(md)

	dialect := goqu.GetDialect("default")
	dialectDs := ds.WithDialect("default")
	ids.Equal(md, ds.Dialect())
	ids.Equal(dialect, dialectDs.Dialect())
}

func (ids *insertDatasetSuite) TestPrepared() {
	ds := goqu.Insert("test")
	preparedDs := ds.Prepared(true)
	ids.True(preparedDs.IsPrepared())
	ids.False(ds.IsPrepared())
	// should apply the prepared to any datasets created from the root
	ids.True(preparedDs.Returning(goqu.C("col")).IsPrepared())

	defer goqu.SetDefaultPrepared(false)
	goqu.SetDefaultPrepared(true)

	// should be prepared by default
	ds = goqu.Insert("test")
	ids.True(ds.IsPrepared())
}

func (ids *insertDatasetSuite) TestGetClauses() {
	ds := goqu.Insert("test")
	ce := exp.NewInsertClauses().SetInto(goqu.I("test"))
	ids.Equal(ce, ds.GetClauses())
}

func (ids *insertDatasetSuite) TestWith() {
	from := goqu.From("cte")
	bd := goqu.Insert("items")
	ids.assertCases(
		insertTestCase{
			ds: bd.With("test-cte", from),
			clauses: exp.NewInsertClauses().
				SetInto(goqu.C("items")).
				CommonTablesAppend(exp.NewCommonTableExpression(false, "test-cte", from)),
		},
		insertTestCase{
			ds:      bd,
			clauses: exp.NewInsertClauses().SetInto(goqu.C("items")),
		},
	)
}

func (ids *insertDatasetSuite) TestWithRecursive() {
	from := goqu.From("cte")
	bd := goqu.Insert("items")
	ids.assertCases(
		insertTestCase{
			ds: bd.WithRecursive("test-cte", from),
			clauses: exp.NewInsertClauses().
				SetInto(goqu.C("items")).
				CommonTablesAppend(exp.NewCommonTableExpression(true, "test-cte", from)),
		},
		insertTestCase{
			ds:      bd,
			clauses: exp.NewInsertClauses().SetInto(goqu.C("items")),
		},
	)
}

func (ids *insertDatasetSuite) TestInto() {
	bd := goqu.Insert("items")
	ids.assertCases(
		insertTestCase{
			ds:      bd.Into("items2"),
			clauses: exp.NewInsertClauses().SetInto(goqu.C("items2")),
		},
		insertTestCase{
			ds:      bd.Into(goqu.L("items2")),
			clauses: exp.NewInsertClauses().SetInto(goqu.L("items2")),
		},
		insertTestCase{
			ds:      bd,
			clauses: exp.NewInsertClauses().SetInto(goqu.C("items")),
		},
	)

	ids.PanicsWithValue(goqu.ErrUnsupportedIntoType, func() {
		bd.Into(true)
	})
}

func (ids *insertDatasetSuite) TestCols() {
	bd := goqu.Insert("items")
	ids.assertCases(
		insertTestCase{
			ds: bd.Cols("a", "b"),
			clauses: exp.NewInsertClauses().
				SetInto(goqu.C("items")).
				SetCols(exp.NewColumnListExpression("a", "b")),
		},
		insertTestCase{
			ds: bd.Cols("a", "b").Cols("c", "d"),
			clauses: exp.NewInsertClauses().
				SetInto(goqu.C("items")).
				SetCols(exp.NewColumnListExpression("c", "d")),
		},
		insertTestCase{
			ds:      bd,
			clauses: exp.NewInsertClauses().SetInto(goqu.C("items")),
		},
	)
}

func (ids *insertDatasetSuite) TestClearCols() {
	bd := goqu.Insert("items").Cols("a", "b")
	ids.assertCases(
		insertTestCase{
			ds:      bd.ClearCols(),
			clauses: exp.NewInsertClauses().SetInto(goqu.C("items")),
		},
		insertTestCase{
			ds:      bd,
			clauses: exp.NewInsertClauses().SetInto(goqu.C("items")).SetCols(exp.NewColumnListExpression("a", "b")),
		},
	)
}

func (ids *insertDatasetSuite) TestColsAppend() {
	bd := goqu.Insert("items").Cols("a")
	ids.assertCases(
		insertTestCase{
			ds:      bd.ColsAppend("b"),
			clauses: exp.NewInsertClauses().SetInto(goqu.C("items")).SetCols(exp.NewColumnListExpression("a", "b")),
		},
		insertTestCase{
			ds:      bd,
			clauses: exp.NewInsertClauses().SetInto(goqu.C("items")).SetCols(exp.NewColumnListExpression("a")),
		},
	)
}

func (ids *insertDatasetSuite) TestFromQuery() {
	bd := goqu.Insert("items")
	ids.assertCases(
		insertTestCase{
			ds: bd.FromQuery(goqu.From("other_items").Where(goqu.C("b").Gt(10))),
			clauses: exp.NewInsertClauses().
				SetInto(goqu.C("items")).
				SetFrom(goqu.From("other_items").Where(goqu.C("b").Gt(10))),
		},
		insertTestCase{
			ds: bd.FromQuery(goqu.From("other_items").Where(goqu.C("b").Gt(10))).Cols("a", "b"),
			clauses: exp.NewInsertClauses().
				SetInto(goqu.C("items")).
				SetCols(exp.NewColumnListExpression("a", "b")).
				SetFrom(goqu.From("other_items").Where(goqu.C("b").Gt(10))),
		},
		insertTestCase{
			ds:      bd,
			clauses: exp.NewInsertClauses().SetInto(goqu.C("items")),
		},
	)
}

func (ids *insertDatasetSuite) TestFromQueryDialectInheritance() {
	md := new(mocks.SQLDialect)
	md.On("Dialect").Return("dialect")

	ids.Run("ok, default dialect is replaced with insert dialect", func() {
		bd := goqu.Insert("items").SetDialect(md).FromQuery(goqu.From("other_items"))
		ids.Require().Equal(md, bd.GetClauses().From().(*goqu.SelectDataset).Dialect())
	})

	ids.Run("ok, insert and select dialects coincide", func() {
		bd := goqu.Insert("items").SetDialect(md).FromQuery(goqu.From("other_items").SetDialect(md))
		ids.Require().Equal(md, bd.GetClauses().From().(*goqu.SelectDataset).Dialect())
	})

	ids.Run("ok, insert and select dialects are default", func() {
		bd := goqu.Insert("items").FromQuery(goqu.From("other_items"))
		ids.Require().Equal(goqu.GetDialect("default"), bd.GetClauses().From().(*goqu.SelectDataset).Dialect())
	})

	ids.Run("panic, insert and select dialects are different", func() {
		defer func() {
			r := recover()
			if r == nil {
				ids.Fail("there should be a panic")
			}
			ids.Require().Equal(
				"incompatible dialects for INSERT (\"dialect\") and SELECT (\"other_dialect\")",
				r.(error).Error(),
			)
		}()

		otherDialect := new(mocks.SQLDialect)
		otherDialect.On("Dialect").Return("other_dialect")
		goqu.Insert("items").SetDialect(md).FromQuery(goqu.From("otherItems").SetDialect(otherDialect))
	})
}

func (ids *insertDatasetSuite) TestVals() {
	val1 := []interface{}{
		"a", "b",
	}
	val2 := []interface{}{
		"c", "d",
	}

	bd := goqu.Insert("items")
	ids.assertCases(
		insertTestCase{
			ds: bd.Vals(val1),
			clauses: exp.NewInsertClauses().
				SetInto(goqu.C("items")).
				SetVals([][]interface{}{val1}),
		},
		insertTestCase{
			ds: bd.Vals(val1, val2),
			clauses: exp.NewInsertClauses().
				SetInto(goqu.C("items")).
				SetVals([][]interface{}{val1, val2}),
		},
		insertTestCase{
			ds: bd.Vals(val1).Vals(val2),
			clauses: exp.NewInsertClauses().
				SetInto(goqu.C("items")).
				SetVals([][]interface{}{val1, val2}),
		},
		insertTestCase{
			ds:      bd,
			clauses: exp.NewInsertClauses().SetInto(goqu.C("items")),
		},
	)
}

func (ids *insertDatasetSuite) TestClearVals() {
	val := []interface{}{
		"a", "b",
	}
	bd := goqu.Insert("items").Vals(val)
	ids.assertCases(
		insertTestCase{
			ds:      bd.ClearVals(),
			clauses: exp.NewInsertClauses().SetInto(goqu.C("items")),
		},
		insertTestCase{
			ds:      bd,
			clauses: exp.NewInsertClauses().SetInto(goqu.C("items")).SetVals([][]interface{}{val}),
		},
	)
}

func (ids *insertDatasetSuite) TestRows() {
	type item struct {
		CreatedAt *time.Time `db:"created_at"`
	}
	n := time.Now()
	r := item{CreatedAt: nil}
	r2 := item{CreatedAt: &n}
	bd := goqu.Insert("items")
	ids.assertCases(
		insertTestCase{
			ds:      bd.Rows(r),
			clauses: exp.NewInsertClauses().SetInto(goqu.C("items")).SetRows([]interface{}{r}),
		},
		insertTestCase{
			ds:      bd.Rows(r).Rows(r2),
			clauses: exp.NewInsertClauses().SetInto(goqu.C("items")).SetRows([]interface{}{r2}),
		},
		insertTestCase{
			ds:      bd,
			clauses: exp.NewInsertClauses().SetInto(goqu.C("items")),
		},
	)
}

func (ids *insertDatasetSuite) TestClearRows() {
	type item struct {
		CreatedAt *time.Time `db:"created_at"`
	}
	r := item{CreatedAt: nil}
	bd := goqu.Insert("items").Rows(r)
	ids.assertCases(
		insertTestCase{
			ds:      bd.ClearRows(),
			clauses: exp.NewInsertClauses().SetInto(goqu.C("items")),
		},
		insertTestCase{
			ds:      bd,
			clauses: exp.NewInsertClauses().SetInto(goqu.C("items")).SetRows([]interface{}{r}),
		},
	)
}

func (ids *insertDatasetSuite) TestOnConflict() {
	du := goqu.DoUpdate("other_items", goqu.Record{"a": 1})

	bd := goqu.Insert("items")
	ids.assertCases(
		insertTestCase{
			ds:      bd.OnConflict(nil),
			clauses: exp.NewInsertClauses().SetInto(goqu.C("items")),
		},
		insertTestCase{
			ds:      bd.OnConflict(goqu.DoNothing()),
			clauses: exp.NewInsertClauses().SetInto(goqu.C("items")).SetOnConflict(goqu.DoNothing()),
		},
		insertTestCase{
			ds:      bd.OnConflict(du),
			clauses: exp.NewInsertClauses().SetInto(goqu.C("items")).SetOnConflict(du),
		},
		insertTestCase{
			ds:      bd,
			clauses: exp.NewInsertClauses().SetInto(goqu.C("items")),
		},
	)
}

func (ids *insertDatasetSuite) TestAs() {
	du := goqu.DoUpdate("other_items", goqu.Record{"new.a": 1})

	bd := goqu.Insert("items").As("new")
	ids.assertCases(
		insertTestCase{
			ds: bd.OnConflict(nil),
			clauses: exp.NewInsertClauses().SetInto(goqu.C("items")).
				SetAlias(exp.NewIdentifierExpression("", "new", "")),
		},
		insertTestCase{
			ds: bd.OnConflict(goqu.DoNothing()),
			clauses: exp.NewInsertClauses().SetInto(goqu.C("items")).
				SetAlias(exp.NewIdentifierExpression("", "new", "")).
				SetOnConflict(goqu.DoNothing()),
		},
		insertTestCase{
			ds: bd.OnConflict(du),
			clauses: exp.NewInsertClauses().
				SetAlias(exp.NewIdentifierExpression("", "new", "")).
				SetInto(goqu.C("items")).SetOnConflict(du),
		},
		insertTestCase{
			ds: bd,
			clauses: exp.NewInsertClauses().
				SetAlias(exp.NewIdentifierExpression("", "new", "")).
				SetInto(goqu.C("items")),
		},
	)
}

func (ids *insertDatasetSuite) TestClearOnConflict() {
	du := goqu.DoUpdate("other_items", goqu.Record{"a": 1})

	bd := goqu.Insert("items").OnConflict(du)
	ids.assertCases(
		insertTestCase{
			ds:      bd.ClearOnConflict(),
			clauses: exp.NewInsertClauses().SetInto(goqu.C("items")),
		},
		insertTestCase{
			ds:      bd,
			clauses: exp.NewInsertClauses().SetInto(goqu.C("items")).SetOnConflict(du),
		},
	)
}

func (ids *insertDatasetSuite) TestReturning() {
	bd := goqu.Insert("items")
	ids.assertCases(
		insertTestCase{
			ds: bd.Returning("a"),
			clauses: exp.NewInsertClauses().
				SetInto(goqu.C("items")).
				SetReturning(exp.NewColumnListExpression("a")),
		},
		insertTestCase{
			ds: bd.Returning(),
			clauses: exp.NewInsertClauses().
				SetInto(goqu.C("items")).
				SetReturning(exp.NewColumnListExpression()),
		},
		insertTestCase{
			ds: bd.Returning(nil),
			clauses: exp.NewInsertClauses().
				SetInto(goqu.C("items")).
				SetReturning(exp.NewColumnListExpression()),
		},
		insertTestCase{
			ds: bd.Returning(),
			clauses: exp.NewInsertClauses().
				SetInto(goqu.C("items")).
				SetReturning(exp.NewColumnListExpression()),
		},
		insertTestCase{
			ds: bd.Returning("a").Returning("b"),
			clauses: exp.NewInsertClauses().
				SetInto(goqu.C("items")).
				SetReturning(exp.NewColumnListExpression("b")),
		},
		insertTestCase{
			ds:      bd,
			clauses: exp.NewInsertClauses().SetInto(goqu.C("items")),
		},
	)
}

func (ids *insertDatasetSuite) TestReturnsColumns() {
	ds := goqu.Insert("test")
	ids.False(ds.ReturnsColumns())
	ids.True(ds.Returning("foo", "bar").ReturnsColumns())
}

func (ids *insertDatasetSuite) TestExecutor() {
	mDB, _, err := sqlmock.New()
	ids.NoError(err)

	ds := goqu.New("mock", mDB).Insert("items").
		Rows(goqu.Record{"address": "111 Test Addr", "name": "Test1"})

	isql, args, err := ds.Executor().ToSQL()
	ids.NoError(err)
	ids.Empty(args)
	ids.Equal(`INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1')`, isql)

	isql, args, err = ds.Prepared(true).Executor().ToSQL()
	ids.NoError(err)
	ids.Equal([]interface{}{"111 Test Addr", "Test1"}, args)
	ids.Equal(`INSERT INTO "items" ("address", "name") VALUES (?, ?)`, isql)

	defer goqu.SetDefaultPrepared(false)
	goqu.SetDefaultPrepared(true)

	isql, args, err = ds.Executor().ToSQL()
	ids.NoError(err)
	ids.Equal([]interface{}{"111 Test Addr", "Test1"}, args)
	ids.Equal(`INSERT INTO "items" ("address", "name") VALUES (?, ?)`, isql)
}

func (ids *insertDatasetSuite) TestInsertStruct() {
	defer goqu.SetIgnoreUntaggedFields(false)

	mDB, _, err := sqlmock.New()
	ids.NoError(err)

	item := dsUntaggedTestActionItem{
		Address:  "111 Test Addr",
		Name:     "Test1",
		Untagged: "Test2",
	}

	ds := goqu.New("mock", mDB).Insert("items").
		Rows(item)

	isql, args, err := ds.Executor().ToSQL()
	ids.NoError(err)
	ids.Empty(args)
	ids.Equal(`INSERT INTO "items" ("address", "name", "untagged") VALUES ('111 Test Addr', 'Test1', 'Test2')`, isql)

	isql, args, err = ds.Prepared(true).Executor().ToSQL()
	ids.NoError(err)
	ids.Equal([]interface{}{"111 Test Addr", "Test1", "Test2"}, args)
	ids.Equal(`INSERT INTO "items" ("address", "name", "untagged") VALUES (?, ?, ?)`, isql)

	goqu.SetIgnoreUntaggedFields(true)

	isql, args, err = ds.Executor().ToSQL()
	ids.NoError(err)
	ids.Empty(args)
	ids.Equal(`INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1')`, isql)

	isql, args, err = ds.Prepared(true).Executor().ToSQL()
	ids.NoError(err)
	ids.Equal([]interface{}{"111 Test Addr", "Test1"}, args)
	ids.Equal(`INSERT INTO "items" ("address", "name") VALUES (?, ?)`, isql)
}

func (ids *insertDatasetSuite) TestToSQL() {
	md := new(mocks.SQLDialect)
	ds := goqu.Insert("test").SetDialect(md)
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(false)
	md.On("ToInsertSQL", sqlB, c).Return(nil).Once()
	insertSQL, args, err := ds.ToSQL()
	ids.Empty(insertSQL)
	ids.Empty(args)
	ids.Nil(err)
	md.AssertExpectations(ids.T())
}

func (ids *insertDatasetSuite) TestToSQL_Prepared() {
	md := new(mocks.SQLDialect)
	ds := goqu.Insert("test").SetDialect(md).Prepared(true)
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(true)
	md.On("ToInsertSQL", sqlB, c).Return(nil).Once()
	insertSQL, args, err := ds.ToSQL()
	ids.Empty(insertSQL)
	ids.Empty(args)
	ids.Nil(err)
	md.AssertExpectations(ids.T())
}

func (ids *insertDatasetSuite) TestToSQL_ReturnedError() {
	md := new(mocks.SQLDialect)
	ds := goqu.Insert("test").SetDialect(md)
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(false)
	ee := errors.New("expected error")
	md.On("ToInsertSQL", sqlB, c).Run(func(args mock.Arguments) {
		args.Get(0).(sb.SQLBuilder).SetError(ee)
	}).Once()

	insertSQL, args, err := ds.ToSQL()
	ids.Empty(insertSQL)
	ids.Empty(args)
	ids.Equal(ee, err)
	md.AssertExpectations(ids.T())
}

func (ids *insertDatasetSuite) TestSetError() {
	err1 := errors.New("error #1")
	err2 := errors.New("error #2")
	err3 := errors.New("error #3")

	// Verify initial error set/get works properly
	md := new(mocks.SQLDialect)
	ds := goqu.Insert("test").SetDialect(md)
	ds = ds.SetError(err1)
	ids.Equal(err1, ds.Error())
	sql, args, err := ds.ToSQL()
	ids.Empty(sql)
	ids.Empty(args)
	ids.Equal(err1, err)

	// Repeated SetError calls on Dataset should not overwrite the original error
	ds = ds.SetError(err2)
	ids.Equal(err1, ds.Error())
	sql, args, err = ds.ToSQL()
	ids.Empty(sql)
	ids.Empty(args)
	ids.Equal(err1, err)

	// Builder functions should not lose the error
	ds = ds.Cols("a", "b")
	ids.Equal(err1, ds.Error())
	sql, args, err = ds.ToSQL()
	ids.Empty(sql)
	ids.Empty(args)
	ids.Equal(err1, err)

	// Deeper errors inside SQL generation should still return original error
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(false)
	md.On("ToInsertSQL", sqlB, c).Run(func(args mock.Arguments) {
		args.Get(0).(sb.SQLBuilder).SetError(err3)
	}).Once()

	sql, args, err = ds.ToSQL()
	ids.Empty(sql)
	ids.Empty(args)
	ids.Equal(err1, err)
}

func TestInsertDataset(t *testing.T) {
	suite.Run(t, new(insertDatasetSuite))
}
