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
	dialect := GetDialect("default")
	ds.WithDialect("default")
	uds.Equal(dialect, ds.Dialect())
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
	ds := Update("test")
	dsc := ds.GetClauses()
	ec := dsc.CommonTablesAppend(exp.NewCommonTableExpression(false, "test-cte", from))
	uds.Equal(ec, ds.With("test-cte", from).GetClauses())
	uds.Equal(dsc, ds.GetClauses())
}

func (uds *updateDatasetSuite) TestWithRecursive() {
	from := Update("cte")
	ds := Update("test")
	dsc := ds.GetClauses()
	ec := dsc.CommonTablesAppend(exp.NewCommonTableExpression(true, "test-cte", from))
	uds.Equal(ec, ds.WithRecursive("test-cte", from).GetClauses())
	uds.Equal(dsc, ds.GetClauses())
}

func (uds *updateDatasetSuite) TestTable() {
	ds := Update("test")
	dsc := ds.GetClauses()
	ec := dsc.SetTable(T("t"))
	uds.Equal(ec, ds.Table(T("t")).GetClauses())
	uds.Equal(dsc, ds.GetClauses())
}

func (uds *updateDatasetSuite) TestTable_ToSQL() {
	ds1 := Update("test").Set(C("a").Set("a1"))

	updateSQL, _, err := ds1.ToSQL()
	uds.NoError(err)
	uds.Equal(`UPDATE "test" SET "a"='a1'`, updateSQL)

	ds2 := ds1.Table("test2")
	updateSQL, _, err = ds2.ToSQL()
	uds.NoError(err)
	uds.Equal(`UPDATE "test2" SET "a"='a1'`, updateSQL)

	// should not change original
	updateSQL, _, err = ds1.ToSQL()
	uds.NoError(err)
	uds.Equal(`UPDATE "test" SET "a"='a1'`, updateSQL)
}

func (uds *updateDatasetSuite) TestSet_ToSQLWithStructs() {
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	ds1 := Update("items").Set(item{Name: "Test", Address: "111 Test Addr"})
	updateSQL, args, err := ds1.ToSQL()
	uds.NoError(err)
	uds.Empty(args)
	uds.Equal(`UPDATE "items" SET "address"='111 Test Addr',"name"='Test'`, updateSQL)

	updateSQL, args, err = ds1.Prepared(true).ToSQL()
	uds.NoError(err)
	uds.Equal(args, []interface{}{"111 Test Addr", "Test"})
	uds.Equal(`UPDATE "items" SET "address"=?,"name"=?`, updateSQL)
}

func (uds *updateDatasetSuite) TestSet_ToSQLWithMaps() {
	ds1 := Update("items").Set(Record{"name": "Test", "address": "111 Test Addr"})
	updateSQL, args, err := ds1.ToSQL()
	uds.NoError(err)
	uds.Empty(args)
	uds.Equal(`UPDATE "items" SET "address"='111 Test Addr',"name"='Test'`, updateSQL)

	updateSQL, args, err = ds1.Prepared(true).ToSQL()
	uds.NoError(err)
	uds.Equal([]interface{}{"111 Test Addr", "Test"}, args)
	uds.Equal(`UPDATE "items" SET "address"=?,"name"=?`, updateSQL)

}

func (uds *updateDatasetSuite) TestSet_ToSQLWithByteSlice() {
	type item struct {
		Name string `db:"name"`
		Data []byte `db:"data"`
	}
	ds1 := Update("items").Set(item{Name: "Test", Data: []byte(`{"someJson":"data"}`)})
	updateSQL, args, err := ds1.ToSQL()
	uds.NoError(err)
	uds.Empty(args)
	uds.Equal(`UPDATE "items" SET "data"='{"someJson":"data"}',"name"='Test'`, updateSQL)

	updateSQL, args, err = ds1.Prepared(true).ToSQL()
	uds.NoError(err)
	uds.Equal(`UPDATE "items" SET "data"=?,"name"=?`, updateSQL)
	uds.Equal(args, []interface{}{[]byte(`{"someJson":"data"}`), "Test"})
}

type valuerType []byte

func (j valuerType) Value() (driver.Value, error) {
	return []byte(fmt.Sprintf("%s World", string(j))), nil
}

func (uds *updateDatasetSuite) TestSet_ToSQLWithCustomValuer() {
	type item struct {
		Name string     `db:"name"`
		Data valuerType `db:"data"`
	}
	ds1 := Update("items").Set(item{Name: "Test", Data: []byte(`Hello`)})
	updateSQL, args, err := ds1.ToSQL()
	uds.NoError(err)
	uds.Empty(args)
	uds.Equal(`UPDATE "items" SET "data"='Hello World',"name"='Test'`, updateSQL)

	updateSQL, args, err = ds1.Prepared(true).ToSQL()
	uds.NoError(err)
	uds.Equal([]interface{}{[]byte("Hello World"), "Test"}, args)
	uds.Equal(`UPDATE "items" SET "data"=?,"name"=?`, updateSQL)
}

func (uds *updateDatasetSuite) TestSet_ToSQLWithValuer() {
	type item struct {
		Name string         `db:"name"`
		Data sql.NullString `db:"data"`
	}
	ds1 := Update("items").Set(item{Name: "Test", Data: sql.NullString{String: "Hello World", Valid: true}})

	updateSQL, args, err := ds1.ToSQL()
	uds.NoError(err)
	uds.Empty(args)
	uds.Equal(`UPDATE "items" SET "data"='Hello World',"name"='Test'`, updateSQL)

	updateSQL, args, err = ds1.Prepared(true).ToSQL()
	uds.NoError(err)
	uds.Equal(args, []interface{}{"Hello World", "Test"})
	uds.Equal(`UPDATE "items" SET "data"=?,"name"=?`, updateSQL)
}

func (uds *updateDatasetSuite) TestSet_ToSQLWithValuerNull() {
	type item struct {
		Name string         `db:"name"`
		Data sql.NullString `db:"data"`
	}
	ds1 := Update("items").Set(item{Name: "Test"})
	updateSQL, args, err := ds1.ToSQL()
	uds.NoError(err)
	uds.Empty(args)
	uds.Equal(`UPDATE "items" SET "data"=NULL,"name"='Test'`, updateSQL)

	updateSQL, args, err = ds1.Prepared(true).ToSQL()
	uds.NoError(err)
	uds.Equal([]interface{}{"Test"}, args)
	uds.Equal(`UPDATE "items" SET "data"=NULL,"name"=?`, updateSQL)
}

func (uds *updateDatasetSuite) TestSet_ToSQLWithEmbeddedStruct() {
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
	uds.NoError(err)
	uds.Empty(args)
	uds.Equal(`UPDATE "items" SET `+
		`"created"='2015-01-01T00:00:00Z',`+
		`"home_phone"='123123',`+
		`"name"='Test',`+
		`"nil_pointer"=NULL,`+
		`"phone_created"='2015-01-01T00:00:00Z',`+
		`"primary_phone"='456456'`, updateSQL)

	updateSQL, args, err = ds1.Prepared(true).ToSQL()
	uds.NoError(err)
	uds.Equal(`UPDATE "items" SET `+
		`"created"=?,"home_phone"=?,"name"=?,"nil_pointer"=NULL,"phone_created"=?,"primary_phone"=?`, updateSQL)
	uds.Equal([]interface{}{created, "123123", "Test", created, "456456"}, args)
}

func (uds *updateDatasetSuite) TestSet_ToSQLWithEmbeddedStructPtr() {
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
	uds.NoError(err)
	uds.Empty(args)
	uds.Equal(`UPDATE "items" SET `+
		`"created"='2015-01-01T00:00:00Z',`+
		`"home_phone"='123123',`+
		`"name"='Test',`+
		`"phone_created"='2015-01-01T00:00:00Z',`+
		`"primary_phone"='456456'`, updateSQL)

	updateSQL, args, err = ds1.Prepared(true).ToSQL()
	uds.NoError(err)
	uds.Equal(`UPDATE "items" `+
		`SET "created"=?,"home_phone"=?,"name"=?,"phone_created"=?,"primary_phone"=?`, updateSQL)
	uds.Equal([]interface{}{created, "123123", "Test", created, "456456"}, args)
}

func (uds *updateDatasetSuite) TestSet_ToSQLWithUnsupportedType() {
	ds1 := Update("items").Set([]string{"HELLO"})

	_, _, err := ds1.ToSQL()
	uds.EqualError(err, "goqu: unsupported update interface type []string")

	_, _, err = ds1.Prepared(true).ToSQL()
	uds.EqualError(err, "goqu: unsupported update interface type []string")
}

func (uds *updateDatasetSuite) TestSet_ToSQLWithSkipupdateTag() {
	type item struct {
		Address string `db:"address" goqu:"skipupdate"`
		Name    string `db:"name"`
	}
	ds1 := Update("items").Set(item{Name: "Test", Address: "111 Test Addr"})

	updateSQL, args, err := ds1.ToSQL()
	uds.NoError(err)
	uds.Empty(args)
	uds.Equal(`UPDATE "items" SET "name"='Test'`, updateSQL)

	updateSQL, args, err = ds1.Prepared(true).ToSQL()
	uds.NoError(err)
	uds.Equal([]interface{}{"Test"}, args)
	uds.Equal(`UPDATE "items" SET "name"=?`, updateSQL)
}

func (uds *updateDatasetSuite) TestSet_ToSQLWithDefaultIfEmptyTag() {
	type item struct {
		Address string  `db:"address" goqu:"skipupdate, defaultifempty"`
		Name    string  `db:"name" goqu:"defaultifempty"`
		Alias   *string `db:"alias" goqu:"defaultifempty"`
	}
	ds := Update("items").Set(item{Name: "Test", Address: "111 Test Addr"})

	updateSQL, args, err := ds.ToSQL()
	uds.NoError(err)
	uds.Empty(args)
	uds.Equal(`UPDATE "items" SET "alias"=DEFAULT,"name"='Test'`, updateSQL)

	updateSQL, args, err = ds.Prepared(true).ToSQL()
	uds.NoError(err)
	uds.Equal([]interface{}{"Test"}, args)
	uds.Equal(`UPDATE "items" SET "alias"=DEFAULT,"name"=?`, updateSQL)

	var alias = ""
	ds = ds.Set(item{Alias: &alias})

	updateSQL, args, err = ds.ToSQL()
	uds.NoError(err)
	uds.Empty(args)
	uds.Equal(`UPDATE "items" SET "alias"='',"name"=DEFAULT`, updateSQL)

	updateSQL, args, err = ds.Prepared(true).ToSQL()
	uds.NoError(err)
	uds.Equal([]interface{}{""}, args)
	uds.Equal(`UPDATE "items" SET "alias"=?,"name"=DEFAULT`, updateSQL)
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
	ds := Update("test")
	dsc := ds.GetClauses()
	w := Ex{
		"a": 1,
	}
	ec := dsc.WhereAppend(w)
	uds.Equal(ec, ds.Where(w).GetClauses())
	uds.Equal(dsc, ds.GetClauses())
}

func (uds *updateDatasetSuite) TestWhere_ToSQL() {
	ds1 := Update("test").Set(C("a").Set("a1"))

	b := ds1.Where(
		C("a").Eq(true),
		C("a").Neq(true),
		C("a").Eq(false),
		C("a").Neq(false),
	)
	updateSQL, args, err := b.ToSQL()
	uds.NoError(err)
	uds.Empty(args)
	uds.Equal(
		`UPDATE "test" SET "a"='a1' `+
			`WHERE (("a" IS TRUE) AND ("a" IS NOT TRUE) AND ("a" IS FALSE) AND ("a" IS NOT FALSE))`,
		updateSQL,
	)

	updateSQL, args, err = b.Prepared(true).ToSQL()
	uds.NoError(err)
	uds.Equal([]interface{}{"a1"}, args)
	uds.Equal(
		`UPDATE "test" SET "a"=? `+
			`WHERE (("a" IS TRUE) AND ("a" IS NOT TRUE) AND ("a" IS FALSE) AND ("a" IS NOT FALSE))`,
		updateSQL,
	)

	b = ds1.Where(
		C("a").Eq("a"),
		C("b").Neq("b"),
		C("c").Gt("c"),
		C("d").Gte("d"),
		C("e").Lt("e"),
		C("f").Lte("f"),
	)
	updateSQL, args, err = b.ToSQL()
	uds.NoError(err)
	uds.Empty(args)
	uds.Equal(
		`UPDATE "test" SET "a"='a1' `+
			`WHERE (("a" = 'a') AND ("b" != 'b') AND ("c" > 'c') AND ("d" >= 'd') AND ("e" < 'e') AND ("f" <= 'f'))`,
		updateSQL,
	)

	updateSQL, args, err = b.Prepared(true).ToSQL()
	uds.NoError(err)
	uds.Equal([]interface{}{"a1", "a", "b", "c", "d", "e", "f"}, args)
	uds.Equal(
		`UPDATE "test" SET "a"=? `+
			`WHERE (("a" = ?) AND ("b" != ?) AND ("c" > ?) AND ("d" >= ?) AND ("e" < ?) AND ("f" <= ?))`,
		updateSQL,
	)

	b = ds1.Where(
		C("a").Eq(From("test2").Select("id")),
	)
	updateSQL, args, err = b.ToSQL()
	uds.NoError(err)
	uds.Empty(args)
	uds.Equal(`UPDATE "test" SET "a"='a1' WHERE ("a" IN (SELECT "id" FROM "test2"))`, updateSQL)

	updateSQL, args, err = b.Prepared(true).ToSQL()
	uds.NoError(err)
	uds.Equal([]interface{}{"a1"}, args)
	uds.Equal(`UPDATE "test" SET "a"=? WHERE ("a" IN (SELECT "id" FROM "test2"))`, updateSQL)

	b = ds1.Where(Ex{
		"a": "a",
		"b": Op{"neq": "b"},
		"c": Op{"gt": "c"},
		"d": Op{"gte": "d"},
		"e": Op{"lt": "e"},
		"f": Op{"lte": "f"},
	})
	updateSQL, args, err = b.ToSQL()
	uds.NoError(err)
	uds.Empty(args)
	uds.Equal(
		`UPDATE "test" SET "a"='a1' `+
			`WHERE (("a" = 'a') AND ("b" != 'b') AND ("c" > 'c') AND ("d" >= 'd') AND ("e" < 'e') AND ("f" <= 'f'))`,
		updateSQL,
	)

	updateSQL, args, err = b.Prepared(true).ToSQL()
	uds.NoError(err)
	uds.Equal([]interface{}{"a1", "a", "b", "c", "d", "e", "f"}, args)
	uds.Equal(
		`UPDATE "test" SET "a"=? `+
			`WHERE (("a" = ?) AND ("b" != ?) AND ("c" > ?) AND ("d" >= ?) AND ("e" < ?) AND ("f" <= ?))`,
		updateSQL,
	)

	b = ds1.Where(Ex{
		"a": From("test2").Select("id"),
	})
	updateSQL, args, err = b.ToSQL()
	uds.NoError(err)
	uds.Empty(args)
	uds.Equal(`UPDATE "test" SET "a"='a1' WHERE ("a" IN (SELECT "id" FROM "test2"))`, updateSQL)

	updateSQL, args, err = b.Prepared(true).ToSQL()
	uds.NoError(err)
	uds.Equal([]interface{}{"a1"}, args)
	uds.Equal(`UPDATE "test" SET "a"=? WHERE ("a" IN (SELECT "id" FROM "test2"))`, updateSQL)
}

func (uds *updateDatasetSuite) TestWhere_ToSQLEmpty() {
	ds1 := Update("test").Set(C("a").Set("a1"))

	b := ds1.Where()
	updateSQL, args, err := b.ToSQL()
	uds.NoError(err)
	uds.Empty(args)
	uds.Equal(`UPDATE "test" SET "a"='a1'`, updateSQL)

	updateSQL, args, err = b.Prepared(true).ToSQL()
	uds.NoError(err)
	uds.Equal([]interface{}{"a1"}, args)
	uds.Equal(`UPDATE "test" SET "a"=?`, updateSQL)
}

func (uds *updateDatasetSuite) TestWhere_ToSQLWithChain() {
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
	uds.NoError(err)
	uds.Empty(args)
	uds.Equal(
		`UPDATE "test" SET "a"='a1' WHERE (("x" = 0) AND ("y" = 1) AND ("z" = 2) AND ("a" = 'A'))`,
		updateSQL,
	)

	updateSQL, args, err = a.Prepared(true).ToSQL()
	uds.NoError(err)
	uds.Equal([]interface{}{"a1", int64(0), int64(1), int64(2), "A"}, args)
	uds.Equal(
		`UPDATE "test" SET "a"=? WHERE (("x" = ?) AND ("y" = ?) AND ("z" = ?) AND ("a" = ?))`,
		updateSQL,
	)

	updateSQL, args, err = b.ToSQL()
	uds.NoError(err)
	uds.Empty(args)
	uds.Equal(
		`UPDATE "test" SET "a"='a1' WHERE (("x" = 0) AND ("y" = 1) AND ("z" = 2) AND ("b" = 'B'))`,
		updateSQL,
	)

	updateSQL, args, err = b.Prepared(true).ToSQL()
	uds.NoError(err)
	uds.Equal([]interface{}{"a1", int64(0), int64(1), int64(2), "B"}, args)
	uds.Equal(
		`UPDATE "test" SET "a"=? WHERE (("x" = ?) AND ("y" = ?) AND ("z" = ?) AND ("b" = ?))`,
		updateSQL,
	)
}

func (uds *updateDatasetSuite) TestClearWhere() {
	w := Ex{
		"a": 1,
	}
	ds := Update("test").Where(w)
	dsc := ds.GetClauses()
	ec := dsc.ClearWhere()
	uds.Equal(ec, ds.ClearWhere().GetClauses())
	uds.Equal(dsc, ds.GetClauses())
}

func (uds *updateDatasetSuite) TestClearWhere_ToSQL() {
	ds1 := Update("test").Set(C("a").Set("a1"))

	b := ds1.Where(
		C("a").Eq(1),
	).ClearWhere()
	updateSQL, args, err := b.ToSQL()
	uds.NoError(err)
	uds.Empty(args)
	uds.Equal(`UPDATE "test" SET "a"='a1'`, updateSQL)

	updateSQL, args, err = b.Prepared(true).ToSQL()
	uds.NoError(err)
	uds.Equal([]interface{}{"a1"}, args)
	uds.Equal(`UPDATE "test" SET "a"=?`, updateSQL)
}

func (uds *updateDatasetSuite) TestOrder() {
	ds := Update("test")
	dsc := ds.GetClauses()
	o := C("a").Desc()
	ec := dsc.SetOrder(o)
	uds.Equal(ec, ds.Order(o).GetClauses())
	uds.Equal(dsc, ds.GetClauses())
}

func (uds *updateDatasetSuite) TestOrder_ToSQL() {

	ds1 := Update("test").WithDialect("order-on-update").Set(C("a").Set("a1"))

	b := ds1.Order(C("a").Asc(), L(`("a" + "b" > 2)`).Asc())

	updateSQL, args, err := b.ToSQL()
	uds.NoError(err)
	uds.Empty(args)
	uds.Equal(`UPDATE "test" SET "a"='a1' ORDER BY "a" ASC, ("a" + "b" > 2) ASC`, updateSQL)

	updateSQL, args, err = b.Prepared(true).ToSQL()
	uds.NoError(err)
	uds.Equal([]interface{}{"a1"}, args)
	uds.Equal(`UPDATE "test" SET "a"=? ORDER BY "a" ASC, ("a" + "b" > 2) ASC`, updateSQL)
}

func (uds *updateDatasetSuite) TestOrderAppend() {
	ds := Update("test").Order(C("a").Desc())
	dsc := ds.GetClauses()
	o := C("b").Desc()
	ec := dsc.OrderAppend(o)
	uds.Equal(ec, ds.OrderAppend(o).GetClauses())
	uds.Equal(dsc, ds.GetClauses())
}

func (uds *updateDatasetSuite) TestOrderAppend_ToSQL() {
	ds := Update("test").WithDialect("order-on-update").Set(C("a").Set("a1"))

	b := ds.Order(C("a").Asc().NullsFirst()).OrderAppend(C("b").Desc().NullsLast())
	updateSQL, args, err := b.ToSQL()
	uds.NoError(err)
	uds.Empty(args)
	uds.Equal(`UPDATE "test" SET "a"='a1' ORDER BY "a" ASC NULLS FIRST, "b" DESC NULLS LAST`, updateSQL)

	updateSQL, args, err = b.Prepared(true).ToSQL()
	uds.NoError(err)
	uds.Equal([]interface{}{"a1"}, args)
	uds.Equal(updateSQL, `UPDATE "test" SET "a"=? ORDER BY "a" ASC NULLS FIRST, "b" DESC NULLS LAST`)

	b = ds.OrderAppend(C("a").Asc().NullsFirst()).OrderAppend(C("b").Desc().NullsLast())
	updateSQL, args, err = b.ToSQL()
	uds.NoError(err)
	uds.Empty(args)
	uds.Equal(`UPDATE "test" SET "a"='a1' ORDER BY "a" ASC NULLS FIRST, "b" DESC NULLS LAST`, updateSQL)

	updateSQL, args, err = b.Prepared(true).ToSQL()
	uds.NoError(err)
	uds.Equal([]interface{}{"a1"}, args)
	uds.Equal(`UPDATE "test" SET "a"=? ORDER BY "a" ASC NULLS FIRST, "b" DESC NULLS LAST`, updateSQL)
}

func (uds *updateDatasetSuite) TestClearOrder() {
	ds := Update("test").Order(C("a").Desc())
	dsc := ds.GetClauses()
	ec := dsc.ClearOrder()
	uds.Equal(ec, ds.ClearOrder().GetClauses())
	uds.Equal(dsc, ds.GetClauses())
}

func (uds *updateDatasetSuite) TestClearOrder_ToSQL() {
	b := Update("test").
		WithDialect("order-on-update").
		Set(C("a").Set("a1")).
		Order(C("a").Asc().NullsFirst()).
		ClearOrder()

	updateSQL, args, err := b.ToSQL()
	uds.NoError(err)
	uds.Empty(args)
	uds.Equal(`UPDATE "test" SET "a"='a1'`, updateSQL)

	updateSQL, args, err = b.Prepared(true).ToSQL()
	uds.NoError(err)
	uds.Equal([]interface{}{"a1"}, args)
	uds.Equal(`UPDATE "test" SET "a"=?`, updateSQL)
}

func (uds *updateDatasetSuite) TestLimit() {
	ds := Update("test")
	dsc := ds.GetClauses()
	ec := dsc.SetLimit(uint(1))
	uds.Equal(ec, ds.Limit(1).GetClauses())
	uds.Equal(dsc, ds.Limit(0).GetClauses())
	uds.Equal(dsc, ds.GetClauses())
}

func (uds *updateDatasetSuite) TestLimit_ToSQL() {
	ds1 := Update("test").WithDialect("limit-on-update").Set(C("a").Set("a1"))

	b := ds1.Limit(10)

	updateSQL, args, err := b.ToSQL()
	uds.NoError(err)
	uds.Empty(args)
	uds.Equal(`UPDATE "test" SET "a"='a1' LIMIT 10`, updateSQL)

	updateSQL, args, err = b.Prepared(true).ToSQL()
	uds.NoError(err)
	uds.Equal([]interface{}{"a1", int64(10)}, args)
	uds.Equal(`UPDATE "test" SET "a"=? LIMIT ?`, updateSQL)

	b = ds1.Limit(0)
	updateSQL, args, err = b.ToSQL()
	uds.NoError(err)
	uds.Empty(args)
	uds.Equal(`UPDATE "test" SET "a"='a1'`, updateSQL)

	updateSQL, args, err = b.Prepared(true).ToSQL()
	uds.NoError(err)
	uds.Equal([]interface{}{"a1"}, args)
	uds.Equal(`UPDATE "test" SET "a"=?`, updateSQL)
}

func (uds *updateDatasetSuite) TestLimitAll() {
	ds := Update("test")
	dsc := ds.GetClauses()
	ec := dsc.SetLimit(L("ALL"))
	uds.Equal(ec, ds.LimitAll().GetClauses())
	uds.Equal(dsc, ds.GetClauses())
}

func (uds *updateDatasetSuite) TestLimitAll_ToSQL() {
	ds1 := Update("test").WithDialect("limit-on-update").Set(C("a").Set("a1"))

	b := ds1.LimitAll()
	updateSQL, args, err := b.ToSQL()
	uds.NoError(err)
	uds.Empty(args)
	uds.Equal(`UPDATE "test" SET "a"='a1' LIMIT ALL`, updateSQL)

	updateSQL, args, err = b.Prepared(true).ToSQL()
	uds.NoError(err)
	uds.Equal([]interface{}{"a1"}, args)
	uds.Equal(`UPDATE "test" SET "a"=? LIMIT ALL`, updateSQL)

	b = ds1.Limit(0).LimitAll()
	updateSQL, args, err = b.ToSQL()
	uds.NoError(err)
	uds.Empty(args)
	uds.Equal(`UPDATE "test" SET "a"='a1' LIMIT ALL`, updateSQL)

	updateSQL, args, err = b.Prepared(true).ToSQL()
	uds.NoError(err)
	uds.Equal([]interface{}{"a1"}, args)
	uds.Equal(`UPDATE "test" SET "a"=? LIMIT ALL`, updateSQL)
}

func (uds *updateDatasetSuite) TestClearLimit() {
	ds := Update("test").Limit(1)
	dsc := ds.GetClauses()
	ec := dsc.ClearLimit()
	uds.Equal(ec, ds.ClearLimit().GetClauses())
	uds.Equal(dsc, ds.GetClauses())
}

func (uds *updateDatasetSuite) TestClearLimit_ToSQL() {
	ds1 := Update("test").WithDialect("limit-on-update").Set(C("a").Set("a1"))

	b := ds1.LimitAll().ClearLimit()
	updateSQL, args, err := b.ToSQL()
	uds.NoError(err)
	uds.Empty(args)
	uds.Equal(`UPDATE "test" SET "a"='a1'`, updateSQL)

	updateSQL, args, err = b.Prepared(true).ToSQL()
	uds.NoError(err)
	uds.Equal([]interface{}{"a1"}, args)
	uds.Equal(`UPDATE "test" SET "a"=?`, updateSQL)

	b = ds1.Limit(10).ClearLimit()
	updateSQL, args, err = b.ToSQL()
	uds.NoError(err)
	uds.Empty(args)
	uds.Equal(`UPDATE "test" SET "a"='a1'`, updateSQL)

	updateSQL, args, err = b.Prepared(true).ToSQL()
	uds.NoError(err)
	uds.Equal([]interface{}{"a1"}, args)
	uds.Equal(`UPDATE "test" SET "a"=?`, updateSQL)
}

func (uds *updateDatasetSuite) TestReturning() {
	ds := Update("test")
	dsc := ds.GetClauses()
	ec := dsc.SetReturning(exp.NewColumnListExpression(C("a")))
	uds.Equal(ec, ds.Returning("a").GetClauses())
	uds.Equal(dsc, ds.GetClauses())
}

func (uds *updateDatasetSuite) TestReturning_ToSQL() {
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	ds := Update("items")
	ds1 := ds.Set(item{Name: "Test", Address: "111 Test Addr"}).Returning(T("items").All())
	ds2 := ds.Set(Record{"name": "Test", "address": "111 Test Addr"}).Returning(T("items").All())

	updateSQL, args, err := ds1.ToSQL()
	uds.NoError(err)
	uds.Empty(args)
	uds.Equal(`UPDATE "items" SET "address"='111 Test Addr',"name"='Test' RETURNING "items".*`, updateSQL)

	updateSQL, args, err = ds1.Prepared(true).ToSQL()
	uds.NoError(err)
	uds.Equal([]interface{}{"111 Test Addr", "Test"}, args)
	uds.Equal(`UPDATE "items" SET "address"=?,"name"=? RETURNING "items".*`, updateSQL)

	updateSQL, args, err = ds2.ToSQL()
	uds.NoError(err)
	uds.Empty(args)
	uds.Equal(`UPDATE "items" SET "address"='111 Test Addr',"name"='Test' RETURNING "items".*`, updateSQL)

	updateSQL, args, err = ds2.Prepared(true).ToSQL()
	uds.NoError(err)
	uds.Equal([]interface{}{"111 Test Addr", "Test"}, args)
	uds.Equal(`UPDATE "items" SET "address"=?,"name"=? RETURNING "items".*`, updateSQL)
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

func (uds *updateDatasetSuite) TestToSQL_withNoSources() {
	ds1 := newUpdateDataset("test", nil)
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	_, _, err := ds1.Set(item{Name: "Test", Address: "111 Test Addr"}).ToSQL()
	uds.EqualError(err, "goqu: no source found when generating update sql")
}

func (uds *updateDatasetSuite) TestToSQL_withReturnNotSupported() {
	ds1 := New("no-return", nil).Update("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	_, _, err := ds1.Set(item{Name: "Test", Address: "111 Test Addr"}).Returning("id").ToSQL()
	uds.EqualError(err, "goqu: dialect does not support RETURNING clause [dialect=no-return]")
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
	uds.Equal([]interface{}{"111 Test Addr", "Test1"}, args)
	uds.Equal(`UPDATE "items" SET "address"=?,"name"=? WHERE ("name" IS NULL)`, updateSQL)
}

func TestUpdateDataset(t *testing.T) {
	suite.Run(t, new(updateDatasetSuite))
}
