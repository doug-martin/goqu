package goqu

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"testing"
	"time"

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

type updateDatasetSuite struct {
	suite.Suite
}

func (uds *updateDatasetSuite) SetupSuite() {
	noReturn := DefaultDialectOptions()
	noReturn.SupportsReturn = false
	RegisterDialect("no-return", noReturn)

	limitOnUpdate := DefaultDialectOptions()
	limitOnUpdate.SupportsLimitOnUpdate = true
	RegisterDialect("limit-on-update", limitOnUpdate)

	orderOnUpdate := DefaultDialectOptions()
	orderOnUpdate.SupportsOrderByOnUpdate = true
	RegisterDialect("order-on-update", orderOnUpdate)
}

func (uds *updateDatasetSuite) TearDownSuite() {
	DeregisterDialect("no-return")
	DeregisterDialect("limit-on-update")
	DeregisterDialect("order-on-update")
}

func (uds *updateDatasetSuite) TestClone() {
	t := uds.T()
	ds := Update("test")
	assert.Equal(t, ds.Clone(), ds)
}

func (uds *updateDatasetSuite) TestExpression() {
	t := uds.T()
	ds := Update("test")
	assert.Equal(t, ds.Expression(), ds)
}

func (uds *updateDatasetSuite) TestDialect() {
	t := uds.T()
	ds := Update("test")
	assert.NotNil(t, ds.Dialect())
}

func (uds *updateDatasetSuite) TestWithDialect() {
	t := uds.T()
	ds := Update("test")
	dialect := GetDialect("default")
	ds.WithDialect("default")
	assert.Equal(t, ds.Dialect(), dialect)
}

func (uds *updateDatasetSuite) TestPrepared() {
	t := uds.T()
	ds := Update("test")
	preparedDs := ds.Prepared(true)
	assert.True(t, preparedDs.IsPrepared())
	assert.False(t, ds.IsPrepared())
	// should apply the prepared to any datasets created from the root
	assert.True(t, preparedDs.Where(Ex{"a": 1}).IsPrepared())
}

func (uds *updateDatasetSuite) TestGetClauses() {
	t := uds.T()
	ds := Update("test")
	ce := exp.NewUpdateClauses().SetTable(I("test"))
	assert.Equal(t, ce, ds.GetClauses())
}

func (uds *updateDatasetSuite) TestWith() {
	t := uds.T()
	from := Update("cte")
	ds := Update("test")
	dsc := ds.GetClauses()
	ec := dsc.CommonTablesAppend(exp.NewCommonTableExpression(false, "test-cte", from))
	assert.Equal(t, ec, ds.With("test-cte", from).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (uds *updateDatasetSuite) TestWithRecursive() {
	t := uds.T()
	from := Update("cte")
	ds := Update("test")
	dsc := ds.GetClauses()
	ec := dsc.CommonTablesAppend(exp.NewCommonTableExpression(true, "test-cte", from))
	assert.Equal(t, ec, ds.WithRecursive("test-cte", from).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (uds *updateDatasetSuite) TestTable() {
	t := uds.T()
	ds := Update("test")
	dsc := ds.GetClauses()
	ec := dsc.SetTable(T("t"))
	assert.Equal(t, ec, ds.Table(T("t")).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (uds *updateDatasetSuite) TestTable_ToSQL() {
	t := uds.T()
	ds1 := Update("test").Set(C("a").Set("a1"))

	updateSQL, _, err := ds1.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, updateSQL, `UPDATE "test" SET "a"='a1'`)

	ds2 := ds1.Table("test2")
	updateSQL, _, err = ds2.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, updateSQL, `UPDATE "test2" SET "a"='a1'`)

	// should not change original
	updateSQL, _, err = ds1.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, updateSQL, `UPDATE "test" SET "a"='a1'`)
}

func (uds *updateDatasetSuite) TestSet_ToSQLWithStructs() {
	t := uds.T()
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	ds1 := Update("items").Set(item{Name: "Test", Address: "111 Test Addr"})
	updateSQL, args, err := ds1.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `UPDATE "items" SET "address"='111 Test Addr',"name"='Test'`, updateSQL)

	updateSQL, args, err = ds1.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test"})
	assert.Equal(t, `UPDATE "items" SET "address"=?,"name"=?`, updateSQL)
}

func (uds *updateDatasetSuite) TestSet_ToSQLWithMaps() {
	t := uds.T()
	ds1 := Update("items").Set(Record{"name": "Test", "address": "111 Test Addr"})
	updateSQL, args, err := ds1.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `UPDATE "items" SET "address"='111 Test Addr',"name"='Test'`, updateSQL)

	updateSQL, args, err = ds1.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test"})
	assert.Equal(t, `UPDATE "items" SET "address"=?,"name"=?`, updateSQL)

}

func (uds *updateDatasetSuite) TestSet_ToSQLWithByteSlice() {
	t := uds.T()
	type item struct {
		Name string `db:"name"`
		Data []byte `db:"data"`
	}
	ds1 := Update("items").Set(item{Name: "Test", Data: []byte(`{"someJson":"data"}`)})
	updateSQL, args, err := ds1.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `UPDATE "items" SET "data"='{"someJson":"data"}',"name"='Test'`, updateSQL)

	updateSQL, args, err = ds1.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `UPDATE "items" SET "data"=?,"name"=?`, updateSQL)
	assert.Equal(t, args, []interface{}{[]byte(`{"someJson":"data"}`), "Test"})
}

type valuerType []byte

func (j valuerType) Value() (driver.Value, error) {
	return []byte(fmt.Sprintf("%s World", string(j))), nil
}

func (uds *updateDatasetSuite) TestSet_ToSQLWithCustomValuer() {
	t := uds.T()
	type item struct {
		Name string     `db:"name"`
		Data valuerType `db:"data"`
	}
	ds1 := Update("items").Set(item{Name: "Test", Data: []byte(`Hello`)})
	updateSQL, args, err := ds1.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `UPDATE "items" SET "data"='Hello World',"name"='Test'`, updateSQL)

	updateSQL, args, err = ds1.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{[]byte("Hello World"), "Test"})
	assert.Equal(t, `UPDATE "items" SET "data"=?,"name"=?`, updateSQL)
}

func (uds *updateDatasetSuite) TestSet_ToSQLWithValuer() {
	t := uds.T()
	type item struct {
		Name string         `db:"name"`
		Data sql.NullString `db:"data"`
	}
	ds1 := Update("items").Set(item{Name: "Test", Data: sql.NullString{String: "Hello World", Valid: true}})

	updateSQL, args, err := ds1.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `UPDATE "items" SET "data"='Hello World',"name"='Test'`, updateSQL)

	updateSQL, args, err = ds1.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"Hello World", "Test"})
	assert.Equal(t, `UPDATE "items" SET "data"=?,"name"=?`, updateSQL)
}

func (uds *updateDatasetSuite) TestSet_ToSQLWithValuerNull() {
	t := uds.T()
	type item struct {
		Name string         `db:"name"`
		Data sql.NullString `db:"data"`
	}
	ds1 := Update("items").Set(item{Name: "Test"})
	updateSQL, args, err := ds1.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `UPDATE "items" SET "data"=NULL,"name"='Test'`, updateSQL)

	updateSQL, args, err = ds1.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{"Test"}, args)
	assert.Equal(t, `UPDATE "items" SET "data"=NULL,"name"=?`, updateSQL)
}

func (uds *updateDatasetSuite) TestSet_ToSQLWithEmbeddedStruct() {
	t := uds.T()
	type Phone struct {
		Primary string    `db:"primary_phone"`
		Home    string    `db:"home_phone"`
		Created time.Time `db:"phone_created"`
	}
	type item struct {
		Phone
		Address    string      `db:"address" goqu:"skipupdate"`
		Name       string      `db:"name"`
		Created    time.Time   `db:"created"`
		NilPointer interface{} `db:"nil_pointer"`
	}
	created, _ := time.Parse("2006-01-02", "2015-01-01")
	ds1 := Update("items").Set(item{
		Name:    "Test",
		Address: "111 Test Addr",
		Created: created,
		Phone: Phone{
			Home:    "123123",
			Primary: "456456",
			Created: created,
		},
	})
	updateSQL, args, err := ds1.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `UPDATE "items" SET `+
		`"created"='2015-01-01T00:00:00Z',`+
		`"home_phone"='123123',`+
		`"name"='Test',`+
		`"nil_pointer"=NULL,`+
		`"phone_created"='2015-01-01T00:00:00Z',`+
		`"primary_phone"='456456'`, updateSQL)

	updateSQL, args, err = ds1.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `UPDATE "items" SET `+
		`"created"=?,"home_phone"=?,"name"=?,"nil_pointer"=NULL,"phone_created"=?,"primary_phone"=?`, updateSQL)
	assert.Equal(t, []interface{}{created, "123123", "Test", created, "456456"}, args)
}

func (uds *updateDatasetSuite) TestSet_ToSQLWithEmbeddedStructPtr() {
	t := uds.T()
	type Phone struct {
		Primary string    `db:"primary_phone"`
		Home    string    `db:"home_phone"`
		Created time.Time `db:"phone_created"`
	}
	type item struct {
		*Phone
		Address string    `db:"address" goqu:"skipupdate"`
		Name    string    `db:"name"`
		Created time.Time `db:"created"`
	}
	created, _ := time.Parse("2006-01-02", "2015-01-01")

	ds1 := Update("items").Set(item{
		Name:    "Test",
		Address: "111 Test Addr",
		Created: created,
		Phone: &Phone{
			Home:    "123123",
			Primary: "456456",
			Created: created,
		},
	})

	updateSQL, args, err := ds1.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{})
	assert.Equal(t, `UPDATE "items" SET `+
		`"created"='2015-01-01T00:00:00Z',`+
		`"home_phone"='123123',`+
		`"name"='Test',`+
		`"phone_created"='2015-01-01T00:00:00Z',`+
		`"primary_phone"='456456'`, updateSQL)

	updateSQL, args, err = ds1.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `UPDATE "items" `+
		`SET "created"=?,"home_phone"=?,"name"=?,"phone_created"=?,"primary_phone"=?`, updateSQL)
	assert.Equal(t, []interface{}{created, "123123", "Test", created, "456456"}, args)
}

func (uds *updateDatasetSuite) TestSet_ToSQLWithUnsupportedType() {
	t := uds.T()
	ds1 := Update("items").Set([]string{"HELLO"})

	_, _, err := ds1.ToSQL()
	assert.EqualError(t, err, "goqu: unsupported update interface type []string")

	_, _, err = ds1.Prepared(true).ToSQL()
	assert.EqualError(t, err, "goqu: unsupported update interface type []string")
}

func (uds *updateDatasetSuite) TestSet_ToSQLWithSkipupdateTag() {
	t := uds.T()
	type item struct {
		Address string `db:"address" goqu:"skipupdate"`
		Name    string `db:"name"`
	}
	ds1 := Update("items").Set(item{Name: "Test", Address: "111 Test Addr"})

	updateSQL, args, err := ds1.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `UPDATE "items" SET "name"='Test'`, updateSQL)

	updateSQL, args, err = ds1.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{"Test"}, args)
	assert.Equal(t, `UPDATE "items" SET "name"=?`, updateSQL)
}

func (uds *updateDatasetSuite) TestFrom() {
	ds := Update("test")
	dsc := ds.GetClauses()
	ec := dsc.SetFrom(exp.NewColumnListExpression("other"))
	uds.Equal(ec, ds.From("other").GetClauses())
	uds.Equal(dsc, ds.GetClauses())
}

func (uds *updateDatasetSuite) TestFrom_ToSQL() {
	ds1 := Update("test").Set(C("a").Set("a1")).From("other_table").Where(Ex{
		"test.name": T("other_test").Col("name"),
	})

	updateSQL, args, err := ds1.ToSQL()
	uds.NoError(err)
	uds.Empty(args)
	uds.Equal(`UPDATE "test" SET "a"='a1' FROM "other_table" WHERE ("test"."name" = "other_test"."name")`, updateSQL)

	updateSQL, args, err = ds1.Prepared(true).ToSQL()
	uds.NoError(err)
	uds.Equal([]interface{}{"a1"}, args)
	uds.Equal(`UPDATE "test" SET "a"=? FROM "other_table" WHERE ("test"."name" = "other_test"."name")`, updateSQL)
}

func (uds *updateDatasetSuite) TestWhere() {
	t := uds.T()
	ds := Update("test")
	dsc := ds.GetClauses()
	w := Ex{
		"a": 1,
	}
	ec := dsc.WhereAppend(w)
	assert.Equal(t, ec, ds.Where(w).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (uds *updateDatasetSuite) TestWhere_ToSQL() {
	t := uds.T()
	ds1 := Update("test").Set(C("a").Set("a1"))

	b := ds1.Where(
		C("a").Eq(true),
		C("a").Neq(true),
		C("a").Eq(false),
		C("a").Neq(false),
	)
	updateSQL, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, updateSQL, `UPDATE "test" SET "a"='a1' `+
		`WHERE (("a" IS TRUE) AND ("a" IS NOT TRUE) AND ("a" IS FALSE) AND ("a" IS NOT FALSE))`)

	updateSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{"a1"}, args)
	assert.Equal(t, updateSQL, `UPDATE "test" SET "a"=? `+
		`WHERE (("a" IS TRUE) AND ("a" IS NOT TRUE) AND ("a" IS FALSE) AND ("a" IS NOT FALSE))`)

	b = ds1.Where(
		C("a").Eq("a"),
		C("b").Neq("b"),
		C("c").Gt("c"),
		C("d").Gte("d"),
		C("e").Lt("e"),
		C("f").Lte("f"),
	)
	updateSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, updateSQL, `UPDATE "test" SET "a"='a1' `+
		`WHERE (("a" = 'a') AND ("b" != 'b') AND ("c" > 'c') AND ("d" >= 'd') AND ("e" < 'e') AND ("f" <= 'f'))`)

	updateSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{"a1", "a", "b", "c", "d", "e", "f"}, args)
	assert.Equal(t, updateSQL, `UPDATE "test" SET "a"=? `+
		`WHERE (("a" = ?) AND ("b" != ?) AND ("c" > ?) AND ("d" >= ?) AND ("e" < ?) AND ("f" <= ?))`)

	b = ds1.Where(
		C("a").Eq(From("test2").Select("id")),
	)
	updateSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, updateSQL, `UPDATE "test" SET "a"='a1' WHERE ("a" IN (SELECT "id" FROM "test2"))`)

	updateSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{"a1"}, args)
	assert.Equal(t, updateSQL, `UPDATE "test" SET "a"=? WHERE ("a" IN (SELECT "id" FROM "test2"))`)

	b = ds1.Where(Ex{
		"a": "a",
		"b": Op{"neq": "b"},
		"c": Op{"gt": "c"},
		"d": Op{"gte": "d"},
		"e": Op{"lt": "e"},
		"f": Op{"lte": "f"},
	})
	updateSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, updateSQL, `UPDATE "test" SET "a"='a1' `+
		`WHERE (("a" = 'a') AND ("b" != 'b') AND ("c" > 'c') AND ("d" >= 'd') AND ("e" < 'e') AND ("f" <= 'f'))`)

	updateSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{"a1", "a", "b", "c", "d", "e", "f"}, args)
	assert.Equal(t, updateSQL, `UPDATE "test" SET "a"=? `+
		`WHERE (("a" = ?) AND ("b" != ?) AND ("c" > ?) AND ("d" >= ?) AND ("e" < ?) AND ("f" <= ?))`)

	b = ds1.Where(Ex{
		"a": From("test2").Select("id"),
	})
	updateSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, updateSQL, `UPDATE "test" SET "a"='a1' WHERE ("a" IN (SELECT "id" FROM "test2"))`)

	updateSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{"a1"}, args)
	assert.Equal(t, updateSQL, `UPDATE "test" SET "a"=? WHERE ("a" IN (SELECT "id" FROM "test2"))`)
}

func (uds *updateDatasetSuite) TestWhere_ToSQLEmpty() {
	t := uds.T()
	ds1 := Update("test").Set(C("a").Set("a1"))

	b := ds1.Where()
	updateSQL, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, updateSQL, `UPDATE "test" SET "a"='a1'`)

	updateSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{"a1"}, args)
	assert.Equal(t, updateSQL, `UPDATE "test" SET "a"=?`)
}

func (uds *updateDatasetSuite) TestWhere_ToSQLWithChain() {
	t := uds.T()
	ds1 := Update("test").Set(C("a").Set("a1")).Where(
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
	updateSQL, args, err := a.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, updateSQL, `UPDATE "test" SET "a"='a1' `+
		`WHERE (("x" = 0) AND ("y" = 1) AND ("z" = 2) AND ("a" = 'A'))`)

	updateSQL, args, err = a.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{"a1", int64(0), int64(1), int64(2), "A"}, args)
	assert.Equal(t, updateSQL, `UPDATE "test" SET "a"=? `+
		`WHERE (("x" = ?) AND ("y" = ?) AND ("z" = ?) AND ("a" = ?))`)

	updateSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, updateSQL, `UPDATE "test" SET "a"='a1' `+
		`WHERE (("x" = 0) AND ("y" = 1) AND ("z" = 2) AND ("b" = 'B'))`)

	updateSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{"a1", int64(0), int64(1), int64(2), "B"}, args)
	assert.Equal(t, updateSQL, `UPDATE "test" SET "a"=? `+
		`WHERE (("x" = ?) AND ("y" = ?) AND ("z" = ?) AND ("b" = ?))`)
}

func (uds *updateDatasetSuite) TestClearWhere() {
	t := uds.T()
	w := Ex{
		"a": 1,
	}
	ds := Update("test").Where(w)
	dsc := ds.GetClauses()
	ec := dsc.ClearWhere()
	assert.Equal(t, ec, ds.ClearWhere().GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (uds *updateDatasetSuite) TestClearWhere_ToSQL() {
	t := uds.T()
	ds1 := Update("test").Set(C("a").Set("a1"))

	b := ds1.Where(
		C("a").Eq(1),
	).ClearWhere()
	updateSQL, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, updateSQL, `UPDATE "test" SET "a"='a1'`)

	updateSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{"a1"}, args)
	assert.Equal(t, updateSQL, `UPDATE "test" SET "a"=?`)
}

func (uds *updateDatasetSuite) TestOrder() {
	t := uds.T()
	ds := Update("test")
	dsc := ds.GetClauses()
	o := C("a").Desc()
	ec := dsc.SetOrder(o)
	assert.Equal(t, ec, ds.Order(o).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (uds *updateDatasetSuite) TestOrder_ToSQL() {
	t := uds.T()

	ds1 := Update("test").WithDialect("order-on-update").Set(C("a").Set("a1"))

	b := ds1.Order(C("a").Asc(), L(`("a" + "b" > 2)`).Asc())

	updateSQL, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, updateSQL, `UPDATE "test" SET "a"='a1' ORDER BY "a" ASC, ("a" + "b" > 2) ASC`)

	updateSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{"a1"}, args)
	assert.Equal(t, updateSQL, `UPDATE "test" SET "a"=? ORDER BY "a" ASC, ("a" + "b" > 2) ASC`)
}

func (uds *updateDatasetSuite) TestOrderAppend() {
	t := uds.T()
	ds := Update("test").Order(C("a").Desc())
	dsc := ds.GetClauses()
	o := C("b").Desc()
	ec := dsc.OrderAppend(o)
	assert.Equal(t, ec, ds.OrderAppend(o).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (uds *updateDatasetSuite) TestOrderAppend_ToSQL() {
	t := uds.T()
	ds := Update("test").WithDialect("order-on-update").Set(C("a").Set("a1"))

	b := ds.Order(C("a").Asc().NullsFirst()).OrderAppend(C("b").Desc().NullsLast())
	updateSQL, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, updateSQL, `UPDATE "test" SET "a"='a1' ORDER BY "a" ASC NULLS FIRST, "b" DESC NULLS LAST`)

	updateSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{"a1"}, args)
	assert.Equal(t, updateSQL, `UPDATE "test" SET "a"=? ORDER BY "a" ASC NULLS FIRST, "b" DESC NULLS LAST`)

	b = ds.OrderAppend(C("a").Asc().NullsFirst()).OrderAppend(C("b").Desc().NullsLast())
	updateSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, updateSQL, `UPDATE "test" SET "a"='a1' ORDER BY "a" ASC NULLS FIRST, "b" DESC NULLS LAST`)

	updateSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{"a1"}, args)
	assert.Equal(t, updateSQL, `UPDATE "test" SET "a"=? ORDER BY "a" ASC NULLS FIRST, "b" DESC NULLS LAST`)
}

func (uds *updateDatasetSuite) TestClearOrder() {
	t := uds.T()
	ds := Update("test").Order(C("a").Desc())
	dsc := ds.GetClauses()
	ec := dsc.ClearOrder()
	assert.Equal(t, ec, ds.ClearOrder().GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (uds *updateDatasetSuite) TestClearOrder_ToSQL() {
	t := uds.T()
	b := Update("test").
		WithDialect("order-on-update").
		Set(C("a").Set("a1")).
		Order(C("a").Asc().NullsFirst()).
		ClearOrder()

	updateSQL, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, updateSQL, `UPDATE "test" SET "a"='a1'`)

	updateSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{"a1"}, args)
	assert.Equal(t, updateSQL, `UPDATE "test" SET "a"=?`)
}

func (uds *updateDatasetSuite) TestLimit() {
	t := uds.T()
	ds := Update("test")
	dsc := ds.GetClauses()
	ec := dsc.SetLimit(uint(1))
	assert.Equal(t, ec, ds.Limit(1).GetClauses())
	assert.Equal(t, dsc, ds.Limit(0).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (uds *updateDatasetSuite) TestLimit_ToSQL() {
	t := uds.T()
	ds1 := Update("test").WithDialect("limit-on-update").Set(C("a").Set("a1"))

	b := ds1.Limit(10)

	updateSQL, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, updateSQL, `UPDATE "test" SET "a"='a1' LIMIT 10`)

	updateSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{"a1", int64(10)}, args)
	assert.Equal(t, updateSQL, `UPDATE "test" SET "a"=? LIMIT ?`)

	b = ds1.Limit(0)
	updateSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, updateSQL, `UPDATE "test" SET "a"='a1'`)

	updateSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{"a1"}, args)
	assert.Equal(t, updateSQL, `UPDATE "test" SET "a"=?`)
}

func (uds *updateDatasetSuite) TestLimitAll() {
	t := uds.T()
	ds := Update("test")
	dsc := ds.GetClauses()
	ec := dsc.SetLimit(L("ALL"))
	assert.Equal(t, ec, ds.LimitAll().GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (uds *updateDatasetSuite) TestLimitAll_ToSQL() {
	t := uds.T()
	ds1 := Update("test").WithDialect("limit-on-update").Set(C("a").Set("a1"))

	b := ds1.LimitAll()
	updateSQL, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, updateSQL, `UPDATE "test" SET "a"='a1' LIMIT ALL`)

	updateSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{"a1"}, args)
	assert.Equal(t, updateSQL, `UPDATE "test" SET "a"=? LIMIT ALL`)

	b = ds1.Limit(0).LimitAll()
	updateSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, updateSQL, `UPDATE "test" SET "a"='a1' LIMIT ALL`)

	updateSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{"a1"}, args)
	assert.Equal(t, updateSQL, `UPDATE "test" SET "a"=? LIMIT ALL`)
}

func (uds *updateDatasetSuite) TestClearLimit() {
	t := uds.T()
	ds := Update("test").Limit(1)
	dsc := ds.GetClauses()
	ec := dsc.ClearLimit()
	assert.Equal(t, ec, ds.ClearLimit().GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (uds *updateDatasetSuite) TestClearLimit_ToSQL() {
	t := uds.T()
	ds1 := Update("test").WithDialect("limit-on-update").Set(C("a").Set("a1"))

	b := ds1.LimitAll().ClearLimit()
	updateSQL, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, updateSQL, `UPDATE "test" SET "a"='a1'`)

	updateSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{"a1"}, args)
	assert.Equal(t, updateSQL, `UPDATE "test" SET "a"=?`)

	b = ds1.Limit(10).ClearLimit()
	updateSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, updateSQL, `UPDATE "test" SET "a"='a1'`)

	updateSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{"a1"}, args)
	assert.Equal(t, updateSQL, `UPDATE "test" SET "a"=?`)
}

func (uds *updateDatasetSuite) TestReturning() {
	t := uds.T()
	ds := Update("test")
	dsc := ds.GetClauses()
	ec := dsc.SetReturning(exp.NewColumnListExpression(C("a")))
	assert.Equal(t, ec, ds.Returning("a").GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (uds *updateDatasetSuite) TestReturning_ToSQL() {
	t := uds.T()
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	ds := Update("items")
	ds1 := ds.Set(item{Name: "Test", Address: "111 Test Addr"}).Returning(T("items").All())
	ds2 := ds.Set(Record{"name": "Test", "address": "111 Test Addr"}).Returning(T("items").All())

	updateSQL, args, err := ds1.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `UPDATE "items" SET "address"='111 Test Addr',"name"='Test' RETURNING "items".*`, updateSQL)

	updateSQL, args, err = ds1.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{"111 Test Addr", "Test"}, args)
	assert.Equal(t, `UPDATE "items" SET "address"=?,"name"=? RETURNING "items".*`, updateSQL)

	updateSQL, args, err = ds2.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `UPDATE "items" SET "address"='111 Test Addr',"name"='Test' RETURNING "items".*`, updateSQL)

	updateSQL, args, err = ds2.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{"111 Test Addr", "Test"}, args)
	assert.Equal(t, `UPDATE "items" SET "address"=?,"name"=? RETURNING "items".*`, updateSQL)
}

func (uds *updateDatasetSuite) TestToSQL() {
	t := uds.T()
	md := new(mocks.SQLDialect)
	ds := Update("test").SetDialect(md)
	r := Record{"c": "a"}
	c := ds.GetClauses().SetSetValues(r)
	sqlB := sb.NewSQLBuilder(false)
	md.On("ToUpdateSQL", sqlB, c).Return(nil).Once()
	updateSQL, args, err := ds.Set(r).ToSQL()
	assert.Empty(t, updateSQL)
	assert.Empty(t, args)
	assert.Nil(t, err)
	md.AssertExpectations(t)
}

func (uds *updateDatasetSuite) TestToSQL_Prepared() {
	t := uds.T()
	md := new(mocks.SQLDialect)
	ds := Update("test").Prepared(true).SetDialect(md)
	r := Record{"c": "a"}
	c := ds.GetClauses().SetSetValues(r)
	sqlB := sb.NewSQLBuilder(true)
	md.On("ToUpdateSQL", sqlB, c).Return(nil).Once()
	updateSQL, args, err := ds.Set(Record{"c": "a"}).ToSQL()
	assert.Empty(t, updateSQL)
	assert.Empty(t, args)
	assert.Nil(t, err)
	md.AssertExpectations(t)
}

func (uds *updateDatasetSuite) TestToSQL_withNoSources() {
	t := uds.T()
	ds1 := newUpdateDataset("test", nil)
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	_, _, err := ds1.Set(item{Name: "Test", Address: "111 Test Addr"}).ToSQL()
	assert.EqualError(t, err, "goqu: no source found when generating update sql")
}

func (uds *updateDatasetSuite) TestToSQL_withReturnNotSupported() {
	t := uds.T()
	ds1 := New("no-return", nil).Update("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	_, _, err := ds1.Set(item{Name: "Test", Address: "111 Test Addr"}).Returning("id").ToSQL()
	assert.EqualError(t, err, "goqu: adapter does not support RETURNING clause")
}

func (uds *updateDatasetSuite) TestToSQL_WithError() {
	t := uds.T()
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
	assert.Empty(t, updateSQL)
	assert.Empty(t, args)
	assert.Equal(t, ee, err)
	md.AssertExpectations(t)
}

func (uds *updateDatasetSuite) TestExecutor() {
	t := uds.T()
	mDb, _, err := sqlmock.New()
	assert.NoError(t, err)
	ds := newUpdateDataset("mock", exec.NewQueryFactory(mDb)).
		Table("items").
		Set(Record{"address": "111 Test Addr", "name": "Test1"}).
		Where(C("name").IsNull())

	updateSQL, args, err := ds.Executor().ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `UPDATE "items" SET "address"='111 Test Addr',"name"='Test1' WHERE ("name" IS NULL)`, updateSQL)

	updateSQL, args, err = ds.Prepared(true).Executor().ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{"111 Test Addr", "Test1"}, args)
	assert.Equal(t, `UPDATE "items" SET "address"=?,"name"=? WHERE ("name" IS NULL)`, updateSQL)
}

func TestUpdateDataset(t *testing.T) {
	suite.Run(t, new(updateDatasetSuite))
}
