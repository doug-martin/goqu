package goqu

import (
	"database/sql"
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

type insertDatasetSuite struct {
	suite.Suite
}

func (ids *insertDatasetSuite) SetupSuite() {
	noReturn := DefaultDialectOptions()
	noReturn.SupportsReturn = false
	RegisterDialect("no-return", noReturn)
}

func (ids *insertDatasetSuite) TearDownSuite() {
	DeregisterDialect("no-return")
}

func (ids *insertDatasetSuite) TestClone() {
	ds := Insert("test")
	ids.Equal(ds.Clone(), ds)
}

func (ids *insertDatasetSuite) TestExpression() {
	ds := Insert("test")
	ids.Equal(ds.Expression(), ds)
}

func (ids *insertDatasetSuite) TestDialect() {
	ds := Insert("test")
	ids.NotNil(ds.Dialect())
}

func (ids *insertDatasetSuite) TestWithDialect() {
	ds := Insert("test")
	md := new(mocks.SQLDialect)
	ds = ds.SetDialect(md)

	dialect := GetDialect("default")
	dialectDs := ds.WithDialect("default")
	ids.Equal(md, ds.Dialect())
	ids.Equal(dialect, dialectDs.Dialect())
}

func (ids *insertDatasetSuite) TestPrepared() {
	ds := Insert("test")
	preparedDs := ds.Prepared(true)
	ids.True(preparedDs.IsPrepared())
	ids.False(ds.IsPrepared())
	// should apply the prepared to any datasets created from the root
	ids.True(preparedDs.Returning(C("col")).IsPrepared())
}

func (ids *insertDatasetSuite) TestGetClauses() {
	ds := Insert("test")
	ce := exp.NewInsertClauses().SetInto(I("test"))
	ids.Equal(ce, ds.GetClauses())
}

func (ids *insertDatasetSuite) TestWith() {
	from := Insert("cte")
	ds := Insert("test")
	dsc := ds.GetClauses()
	ec := dsc.CommonTablesAppend(exp.NewCommonTableExpression(false, "test-cte", from))
	ids.Equal(ec, ds.With("test-cte", from).GetClauses())
	ids.Equal(dsc, ds.GetClauses())
}

func (ids *insertDatasetSuite) TestWithRecursive() {
	from := Insert("cte")
	ds := Insert("test")
	dsc := ds.GetClauses()
	ec := dsc.CommonTablesAppend(exp.NewCommonTableExpression(true, "test-cte", from))
	ids.Equal(ec, ds.WithRecursive("test-cte", from).GetClauses())
	ids.Equal(dsc, ds.GetClauses())
}

func (ids *insertDatasetSuite) TestRows_ToSQLWithNullTimeField() {
	type item struct {
		CreatedAt *time.Time `db:"created_at"`
	}
	ds := Insert("items").Rows(item{CreatedAt: nil})
	insertSQL, args, err := ds.ToSQL()
	ids.NoError(err)
	ids.Empty(args)
	ids.Equal(`INSERT INTO "items" ("created_at") VALUES (NULL)`, insertSQL)

	insertSQL, args, err = ds.Prepared(true).ToSQL()
	ids.NoError(err)
	ids.Empty(args)
	ids.Equal(`INSERT INTO "items" ("created_at") VALUES (NULL)`, insertSQL)
}

func (ids *insertDatasetSuite) TestRows_ToSQLWithInvalidValue() {
	ds := Insert("test").Rows(true)
	_, _, err := ds.ToSQL()
	ids.EqualError(err, "goqu: unsupported insert must be map, goqu.Record, or struct type got: bool")

	_, _, err = ds.Prepared(true).ToSQL()
	ids.EqualError(err, "goqu: unsupported insert must be map, goqu.Record, or struct type got: bool")
}

func (ids *insertDatasetSuite) TestRows_ToSQLWithStructs() {
	type item struct {
		Address string    `db:"address"`
		Name    string    `db:"name"`
		Created time.Time `db:"created"`
	}
	ds := Insert("items")
	created, _ := time.Parse("2006-01-02", "2015-01-01")
	ds1 := ds.Rows(item{Name: "Test", Address: "111 Test Addr", Created: created})

	insertSQL, args, err := ds1.ToSQL()
	ids.NoError(err)
	ids.Empty(args)
	ids.Equal(
		`INSERT INTO "items" ("address", "created", "name") VALUES ('111 Test Addr', '`+created.Format(time.RFC3339Nano)+`', 'Test')`,
		insertSQL,
	) // #nosec

	insertSQL, args, err = ds1.Prepared(true).ToSQL()
	ids.NoError(err)
	ids.Equal([]interface{}{"111 Test Addr", created, "Test"}, args)
	ids.Equal(`INSERT INTO "items" ("address", "created", "name") VALUES (?, ?, ?)`, insertSQL)

	ds2 := ds1.Rows(
		item{Address: "111 Test Addr", Name: "Test1", Created: created},
		item{Address: "211 Test Addr", Name: "Test2", Created: created},
		item{Address: "311 Test Addr", Name: "Test3", Created: created},
		item{Address: "411 Test Addr", Name: "Test4", Created: created},
	)

	insertSQL, args, err = ds2.ToSQL()
	ids.NoError(err)
	ids.Empty(args)
	ids.Equal(
		`INSERT INTO "items" ("address", "created", "name") VALUES `+
			`('111 Test Addr', '`+created.Format(time.RFC3339Nano)+`', 'Test1'), `+
			`('211 Test Addr', '`+created.Format(time.RFC3339Nano)+`', 'Test2'), `+
			`('311 Test Addr', '`+created.Format(time.RFC3339Nano)+`', 'Test3'), `+
			`('411 Test Addr', '`+created.Format(time.RFC3339Nano)+`', 'Test4')`,
		insertSQL,
	)

	insertSQL, args, err = ds2.Prepared(true).ToSQL()
	ids.NoError(err)
	ids.Equal([]interface{}{
		"111 Test Addr", created, "Test1",
		"211 Test Addr", created, "Test2",
		"311 Test Addr", created, "Test3",
		"411 Test Addr", created, "Test4",
	}, args)
	ids.Equal(
		`INSERT INTO "items" ("address", "created", "name") VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?)`,
		insertSQL,
	)
}

func (ids *insertDatasetSuite) TestRows_ToSQLWithEmbeddedStruct() {
	type Phone struct {
		Primary string `db:"primary_phone"`
		Home    string `db:"home_phone"`
	}
	type item struct {
		Phone
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	bd := Insert("items")
	ds := bd.Rows(item{
		Name:    "Test",
		Address: "111 Test Addr",
		Phone: Phone{
			Home:    "123123",
			Primary: "456456",
		},
	})

	insertSQL, args, err := ds.ToSQL()
	ids.NoError(err)
	ids.Empty(args)
	ids.Equal(`INSERT INTO "items" ("address", "home_phone", "name", "primary_phone") VALUES `+
		`('111 Test Addr', '123123', 'Test', '456456')`, insertSQL)

	insertSQL, args, err = ds.Prepared(true).ToSQL()
	ids.NoError(err)
	ids.Equal([]interface{}{"111 Test Addr", "123123", "Test", "456456"}, args)
	ids.Equal(
		`INSERT INTO "items" ("address", "home_phone", "name", "primary_phone") VALUES (?, ?, ?, ?)`,
		insertSQL,
	)

	ds = bd.Rows(
		item{Address: "111 Test Addr", Name: "Test1", Phone: Phone{Home: "123123", Primary: "456456"}},
		item{Address: "211 Test Addr", Name: "Test2", Phone: Phone{Home: "123123", Primary: "456456"}},
		item{Address: "311 Test Addr", Name: "Test3", Phone: Phone{Home: "123123", Primary: "456456"}},
		item{Address: "411 Test Addr", Name: "Test4", Phone: Phone{Home: "123123", Primary: "456456"}},
	)
	insertSQL, args, err = ds.ToSQL()
	ids.NoError(err)
	ids.Empty(args)
	ids.Equal(`INSERT INTO "items" ("address", "home_phone", "name", "primary_phone") VALUES `+
		`('111 Test Addr', '123123', 'Test1', '456456'), `+
		`('211 Test Addr', '123123', 'Test2', '456456'), `+
		`('311 Test Addr', '123123', 'Test3', '456456'), `+
		`('411 Test Addr', '123123', 'Test4', '456456')`, insertSQL)

	insertSQL, args, err = ds.Prepared(true).ToSQL()
	ids.NoError(err)
	ids.Equal([]interface{}{
		"111 Test Addr", "123123", "Test1", "456456",
		"211 Test Addr", "123123", "Test2", "456456",
		"311 Test Addr", "123123", "Test3", "456456",
		"411 Test Addr", "123123", "Test4", "456456",
	}, args)
	ids.Equal(`INSERT INTO "items" ("address", "home_phone", "name", "primary_phone") VALUES `+
		`(?, ?, ?, ?), `+
		`(?, ?, ?, ?), `+
		`(?, ?, ?, ?), `+
		`(?, ?, ?, ?)`, insertSQL)
}

func (ids *insertDatasetSuite) TestRows_ToSQLWithEmbeddedStructPtr() {
	type Phone struct {
		Primary string `db:"primary_phone"`
		Home    string `db:"home_phone"`
	}
	type item struct {
		*Phone
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	bd := Insert("items")
	ds := bd.Rows(item{
		Name:    "Test",
		Address: "111 Test Addr",
		Phone: &Phone{
			Home:    "123123",
			Primary: "456456",
		},
	})

	insertSQL, args, err := ds.ToSQL()
	ids.NoError(err)
	ids.Empty(args)
	ids.Equal(`INSERT INTO "items" ("address", "home_phone", "name", "primary_phone") VALUES `+
		`('111 Test Addr', '123123', 'Test', '456456')`, insertSQL)

	insertSQL, args, err = ds.Prepared(true).ToSQL()
	ids.NoError(err)
	ids.Equal([]interface{}{"111 Test Addr", "123123", "Test", "456456"}, args)
	ids.Equal(
		insertSQL,
		`INSERT INTO "items" ("address", "home_phone", "name", "primary_phone") VALUES (?, ?, ?, ?)`,
	)

	ds = bd.Rows(
		item{Address: "111 Test Addr", Name: "Test1", Phone: &Phone{Home: "123123", Primary: "456456"}},
		item{Address: "211 Test Addr", Name: "Test2", Phone: &Phone{Home: "123123", Primary: "456456"}},
		item{Address: "311 Test Addr", Name: "Test3", Phone: &Phone{Home: "123123", Primary: "456456"}},
		item{Address: "411 Test Addr", Name: "Test4", Phone: &Phone{Home: "123123", Primary: "456456"}},
	)
	insertSQL, args, err = ds.ToSQL()
	ids.NoError(err)
	ids.Empty(args)
	ids.Equal(`INSERT INTO "items" ("address", "home_phone", "name", "primary_phone") VALUES `+
		`('111 Test Addr', '123123', 'Test1', '456456'), `+
		`('211 Test Addr', '123123', 'Test2', '456456'), `+
		`('311 Test Addr', '123123', 'Test3', '456456'), `+
		`('411 Test Addr', '123123', 'Test4', '456456')`, insertSQL)

	insertSQL, args, err = ds.Prepared(true).ToSQL()
	ids.NoError(err)
	ids.Equal([]interface{}{
		"111 Test Addr", "123123", "Test1", "456456",
		"211 Test Addr", "123123", "Test2", "456456",
		"311 Test Addr", "123123", "Test3", "456456",
		"411 Test Addr", "123123", "Test4", "456456",
	}, args)
	ids.Equal(`INSERT INTO "items" ("address", "home_phone", "name", "primary_phone") VALUES `+
		`(?, ?, ?, ?), `+
		`(?, ?, ?, ?), `+
		`(?, ?, ?, ?), `+
		`(?, ?, ?, ?)`, insertSQL)
}

func (ids *insertDatasetSuite) TestRows_ToSQLWithValuer() {
	type item struct {
		Address string        `db:"address"`
		Name    string        `db:"name"`
		Valuer  sql.NullInt64 `db:"valuer"`
	}

	bd := Insert("items")
	ds := bd.Rows(item{Name: "Test", Address: "111 Test Addr", Valuer: sql.NullInt64{Int64: 10, Valid: true}})
	insertSQL, args, err := ds.ToSQL()
	ids.NoError(err)
	ids.Empty(args)
	ids.Equal(`INSERT INTO "items" ("address", "name", "valuer") VALUES ('111 Test Addr', 'Test', 10)`, insertSQL)

	insertSQL, args, err = ds.Prepared(true).ToSQL()
	ids.NoError(err)
	ids.Equal([]interface{}{"111 Test Addr", "Test", int64(10)}, args)
	ids.Equal(`INSERT INTO "items" ("address", "name", "valuer") VALUES (?, ?, ?)`, insertSQL)

	ds = bd.Rows(
		item{Address: "111 Test Addr", Name: "Test1", Valuer: sql.NullInt64{Int64: 10, Valid: true}},
		item{Address: "211 Test Addr", Name: "Test2", Valuer: sql.NullInt64{Int64: 20, Valid: true}},
		item{Address: "311 Test Addr", Name: "Test3", Valuer: sql.NullInt64{Int64: 30, Valid: true}},
		item{Address: "411 Test Addr", Name: "Test4", Valuer: sql.NullInt64{Int64: 40, Valid: true}},
	)
	insertSQL, args, err = ds.ToSQL()
	ids.NoError(err)
	ids.Empty(args)
	ids.Equal(`INSERT INTO "items" ("address", "name", "valuer") VALUES `+
		`('111 Test Addr', 'Test1', 10), `+
		`('211 Test Addr', 'Test2', 20), `+
		`('311 Test Addr', 'Test3', 30), `+
		`('411 Test Addr', 'Test4', 40)`, insertSQL)

	insertSQL, args, err = ds.Prepared(true).ToSQL()
	ids.NoError(err)
	ids.Equal([]interface{}{
		"111 Test Addr", "Test1", int64(10),
		"211 Test Addr", "Test2", int64(20),
		"311 Test Addr", "Test3", int64(30),
		"411 Test Addr", "Test4", int64(40),
	}, args)
	ids.Equal(`INSERT INTO "items" ("address", "name", "valuer") VALUES `+
		`(?, ?, ?), `+
		`(?, ?, ?), `+
		`(?, ?, ?), `+
		`(?, ?, ?)`, insertSQL)
}

func (ids *insertDatasetSuite) TestRows_ToSQLWithValuerNull() {
	type item struct {
		Address string        `db:"address"`
		Name    string        `db:"name"`
		Valuer  sql.NullInt64 `db:"valuer"`
	}

	bd := Insert("items")
	ds := bd.Rows(item{Name: "Test", Address: "111 Test Addr"})
	insertSQL, args, err := ds.ToSQL()
	ids.NoError(err)
	ids.Empty(args)
	ids.Equal(`INSERT INTO "items" ("address", "name", "valuer") VALUES ('111 Test Addr', 'Test', NULL)`, insertSQL)

	insertSQL, args, err = ds.Prepared(true).ToSQL()
	ids.NoError(err)
	ids.Equal([]interface{}{"111 Test Addr", "Test"}, args)
	ids.Equal(`INSERT INTO "items" ("address", "name", "valuer") VALUES (?, ?, NULL)`, insertSQL)

	ds = bd.Rows(
		item{Address: "111 Test Addr", Name: "Test1"},
		item{Address: "211 Test Addr", Name: "Test2"},
		item{Address: "311 Test Addr", Name: "Test3"},
		item{Address: "411 Test Addr", Name: "Test4"},
	)
	insertSQL, args, err = ds.ToSQL()
	ids.NoError(err)
	ids.Empty(args)
	ids.Equal(`INSERT INTO "items" ("address", "name", "valuer") VALUES `+
		`('111 Test Addr', 'Test1', NULL), `+
		`('211 Test Addr', 'Test2', NULL), `+
		`('311 Test Addr', 'Test3', NULL), `+
		`('411 Test Addr', 'Test4', NULL)`,
		insertSQL,
	)

	insertSQL, args, err = ds.Prepared(true).ToSQL()
	ids.NoError(err)
	ids.Equal([]interface{}{
		"111 Test Addr", "Test1",
		"211 Test Addr", "Test2",
		"311 Test Addr", "Test3",
		"411 Test Addr", "Test4",
	}, args)
	ids.Equal(`INSERT INTO "items" ("address", "name", "valuer") VALUES `+
		`(?, ?, NULL), `+
		`(?, ?, NULL), `+
		`(?, ?, NULL), `+
		`(?, ?, NULL)`,
		insertSQL,
	)
}

func (ids *insertDatasetSuite) TestRows_ToSQLWithMaps() {
	ds := Insert("items")

	ds1 := ds.Rows(map[string]interface{}{"name": "Test", "address": "111 Test Addr"})
	insertSQL, args, err := ds1.ToSQL()
	ids.NoError(err)
	ids.Empty(args)
	ids.Equal(`INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test')`, insertSQL)

	insertSQL, args, err = ds1.Prepared(true).ToSQL()
	ids.NoError(err)
	ids.Equal([]interface{}{"111 Test Addr", "Test"}, args)
	ids.Equal(`INSERT INTO "items" ("address", "name") VALUES (?, ?)`, insertSQL)

	ds1 = ds.Rows(
		map[string]interface{}{"address": "111 Test Addr", "name": "Test1"},
		map[string]interface{}{"address": "211 Test Addr", "name": "Test2"},
		map[string]interface{}{"address": "311 Test Addr", "name": "Test3"},
		map[string]interface{}{"address": "411 Test Addr", "name": "Test4"},
	)
	insertSQL, _, err = ds1.ToSQL()
	ids.NoError(err)
	ids.Equal(`INSERT INTO "items" ("address", "name") VALUES `+
		`('111 Test Addr', 'Test1'), `+
		`('211 Test Addr', 'Test2'), `+
		`('311 Test Addr', 'Test3'), `+
		`('411 Test Addr', 'Test4')`,
		insertSQL,
	)

	insertSQL, args, err = ds1.Prepared(true).ToSQL()
	ids.NoError(err)
	ids.Equal([]interface{}{
		"111 Test Addr", "Test1",
		"211 Test Addr", "Test2",
		"311 Test Addr", "Test3",
		"411 Test Addr", "Test4",
	}, args)
	ids.Equal(`INSERT INTO "items" ("address", "name") VALUES (?, ?), (?, ?), (?, ?), (?, ?)`, insertSQL)
}

func (ids *insertDatasetSuite) TestRows_ToSQLWithSQLBuilder() {
	ds := Insert("items")

	ds1 := ds.Rows(From("other_items").Where(C("b").Gt(10)))

	insertSQL, args, err := ds1.ToSQL()
	ids.NoError(err)
	ids.Empty(args)
	ids.Equal(`INSERT INTO "items" SELECT * FROM "other_items" WHERE ("b" > 10)`, insertSQL)

	insertSQL, args, err = ds1.Prepared(true).ToSQL()
	ids.NoError(err)
	ids.Equal([]interface{}{int64(10)}, args)
	ids.Equal(`INSERT INTO "items" SELECT * FROM "other_items" WHERE ("b" > ?)`, insertSQL)
}

func (ids *insertDatasetSuite) TestRows_ToSQLWithMapsWithDifferentLengths() {
	ds1 := Insert("items").Rows(
		map[string]interface{}{"address": "111 Test Addr", "name": "Test1"},
		map[string]interface{}{"address": "211 Test Addr"},
		map[string]interface{}{"address": "311 Test Addr", "name": "Test3"},
		map[string]interface{}{"address": "411 Test Addr", "name": "Test4"},
	)
	_, _, err := ds1.ToSQL()
	ids.EqualError(err, "goqu: rows with different value length expected 2 got 1")
	_, _, err = ds1.Prepared(true).ToSQL()
	ids.EqualError(err, "goqu: rows with different value length expected 2 got 1")
}

func (ids *insertDatasetSuite) TestRows_ToSQLWitDifferentKeys() {
	ds := Insert("items").Rows(
		map[string]interface{}{"address": "111 Test Addr", "name": "test"},
		map[string]interface{}{"phoneNumber": 10, "address": "111 Test Addr"},
	)
	_, _, err := ds.ToSQL()
	ids.EqualError(err, `goqu: rows with different keys expected ["address","name"] got ["address","phoneNumber"]`)

	_, _, err = ds.Prepared(true).ToSQL()
	ids.EqualError(err, `goqu: rows with different keys expected ["address","name"] got ["address","phoneNumber"]`)
}

func (ids *insertDatasetSuite) TestRows_ToSQLDifferentTypes() {
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	type item2 struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	bd := Insert("items")
	ds := bd.Rows(
		item{Address: "111 Test Addr", Name: "Test1"},
		item2{Address: "211 Test Addr", Name: "Test2"},
		item{Address: "311 Test Addr", Name: "Test3"},
		item2{Address: "411 Test Addr", Name: "Test4"},
	)
	_, _, err := ds.ToSQL()
	ids.EqualError(err, "goqu: rows must be all the same type expected goqu.item got goqu.item2")
	_, _, err = ds.Prepared(true).ToSQL()
	ids.EqualError(err, "goqu: rows must be all the same type expected goqu.item got goqu.item2")

	ds = bd.Rows(
		item{Address: "111 Test Addr", Name: "Test1"},
		map[string]interface{}{"address": "211 Test Addr", "name": "Test2"},
		item{Address: "311 Test Addr", Name: "Test3"},
		map[string]interface{}{"address": "411 Test Addr", "name": "Test4"},
	)
	_, _, err = ds.ToSQL()
	ids.EqualError(err, "goqu: rows must be all the same type expected goqu.item got map[string]interface {}")

	_, _, err = ds.Prepared(true).ToSQL()
	ids.EqualError(err, "goqu: rows must be all the same type expected goqu.item got map[string]interface {}")
}

func (ids *insertDatasetSuite) TestRows_ToSQLWithGoquSkipInsertTagSQL() {
	type item struct {
		ID      uint32 `db:"id" goqu:"skipinsert"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	ds := Insert("items")

	ds1 := ds.Rows(item{Name: "Test", Address: "111 Test Addr"})

	insertSQL, args, err := ds1.ToSQL()
	ids.NoError(err)
	ids.Empty(args)
	ids.Equal(`INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test')`, insertSQL)

	insertSQL, args, err = ds1.Prepared(true).ToSQL()
	ids.NoError(err)
	ids.Equal([]interface{}{"111 Test Addr", "Test"}, args)
	ids.Equal(`INSERT INTO "items" ("address", "name") VALUES (?, ?)`, insertSQL)

	ds1 = ds.Rows(
		item{Name: "Test1", Address: "111 Test Addr"},
		item{Name: "Test2", Address: "211 Test Addr"},
		item{Name: "Test3", Address: "311 Test Addr"},
		item{Name: "Test4", Address: "411 Test Addr"},
	)

	insertSQL, args, err = ds1.ToSQL()
	ids.NoError(err)
	ids.Empty(args)
	ids.Equal(`INSERT INTO "items" ("address", "name") VALUES `+
		`('111 Test Addr', 'Test1'), `+
		`('211 Test Addr', 'Test2'), `+
		`('311 Test Addr', 'Test3'), `+
		`('411 Test Addr', 'Test4')`,
		insertSQL,
	)

	insertSQL, args, err = ds1.Prepared(true).ToSQL()
	ids.NoError(err)
	ids.Equal([]interface{}{
		"111 Test Addr", "Test1",
		"211 Test Addr", "Test2",
		"311 Test Addr", "Test3",
		"411 Test Addr", "Test4",
	}, args)
	ids.Equal(`INSERT INTO "items" ("address", "name") VALUES (?, ?), (?, ?), (?, ?), (?, ?)`, insertSQL)
}

func (ids *insertDatasetSuite) TestRows_ToSQLWithGoquDefaultIfEmptyTag() {
	type item struct {
		ID      uint32 `db:"id" goqu:"skipinsert"`
		Address string `db:"address" goqu:"defaultifempty"`
		Name    string `db:"name" goqu:"defaultifempty"`
		Bool    bool   `db:"bool" goqu:"skipinsert,defaultifempty"`
	}
	ds := Insert("items")

	ds1 := ds.Rows(item{Name: "Test", Address: "111 Test Addr"})

	insertSQL, args, err := ds1.ToSQL()
	ids.NoError(err)
	ids.Empty(args)
	ids.Equal(`INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test')`, insertSQL)

	insertSQL, args, err = ds1.Prepared(true).ToSQL()
	ids.NoError(err)
	ids.Equal([]interface{}{"111 Test Addr", "Test"}, args)
	ids.Equal(`INSERT INTO "items" ("address", "name") VALUES (?, ?)`, insertSQL)

	ds1 = ds.Rows(item{})

	insertSQL, args, err = ds1.ToSQL()
	ids.NoError(err)
	ids.Empty(args)
	ids.Equal(`INSERT INTO "items" ("address", "name") VALUES (DEFAULT, DEFAULT)`, insertSQL)

	insertSQL, args, err = ds1.Prepared(true).ToSQL()
	ids.NoError(err)
	ids.Empty(args)
	ids.Equal(`INSERT INTO "items" ("address", "name") VALUES (DEFAULT, DEFAULT)`, insertSQL)
}

func (ids *insertDatasetSuite) TestRows_ToSQLWithDefaultValues() {
	ds := Insert("items")
	ds1 := ds.Rows()

	insertSQL, args, err := ds1.ToSQL()
	ids.NoError(err)
	ids.Empty(args)
	ids.Equal(`INSERT INTO "items" DEFAULT VALUES`, insertSQL)

	insertSQL, args, err = ds1.Prepared(true).ToSQL()
	ids.NoError(err)
	ids.Empty(args)
	ids.Equal(`INSERT INTO "items" DEFAULT VALUES`, insertSQL)

	ds1 = ds.Rows(map[string]interface{}{"name": Default(), "address": Default()})
	insertSQL, args, err = ds1.ToSQL()
	ids.NoError(err)
	ids.Empty(args)
	ids.Equal(`INSERT INTO "items" ("address", "name") VALUES (DEFAULT, DEFAULT)`, insertSQL)

	insertSQL, _, err = ds1.ToSQL()
	ids.NoError(err)
	ids.Empty(args)
	ids.Equal(`INSERT INTO "items" ("address", "name") VALUES (DEFAULT, DEFAULT)`, insertSQL)
}

func (ids *insertDatasetSuite) TestToSQL() {
	md := new(mocks.SQLDialect)
	ds := Insert("test").SetDialect(md)
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(false)
	md.On("ToInsertSQL", sqlB, c).Return(nil).Once()
	insertSQL, args, err := ds.ToSQL()
	ids.Empty(insertSQL)
	ids.Empty(args)
	ids.Nil(err)
	md.AssertExpectations(ids.T())
}

func (ids *insertDatasetSuite) TestToSQL_WithNoInto() {
	ds1 := newInsertDataset("test", nil).Rows(map[string]interface{}{
		"address": "111 Test Addr", "name": "Test1",
	})
	_, _, err := ds1.ToSQL()
	ids.EqualError(err, "goqu: no source found when generating insert sql")
	_, _, err = ds1.Prepared(true).ToSQL()
	ids.EqualError(err, "goqu: no source found when generating insert sql")
}

func (ids *insertDatasetSuite) TestToSQL_ReturnedError() {
	md := new(mocks.SQLDialect)
	ds := Insert("test").SetDialect(md)
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

func (ids *insertDatasetSuite) TestFromQuery_ToSQL() {
	bd := Insert("items")

	ds := bd.FromQuery(From("other_items").Where(C("b").Gt(10)))

	insertSQL, args, err := ds.ToSQL()
	ids.NoError(err)
	ids.Empty(args)
	ids.Equal(`INSERT INTO "items" SELECT * FROM "other_items" WHERE ("b" > 10)`, insertSQL)

	insertSQL, args, err = ds.Prepared(true).ToSQL()
	ids.NoError(err)
	ids.Equal([]interface{}{int64(10)}, args)
	ids.Equal(`INSERT INTO "items" SELECT * FROM "other_items" WHERE ("b" > ?)`, insertSQL)
}

func (ids *insertDatasetSuite) TestFromQuery_ToSQLWithCols() {
	bd := Insert("items")

	ds := bd.Cols("a", "b").FromQuery(From("other_items").Select("c", "d").Where(C("b").Gt(10)))

	insertSQL, args, err := ds.ToSQL()
	ids.NoError(err)
	ids.Empty(args)
	ids.Equal(`INSERT INTO "items" ("a", "b") SELECT "c", "d" FROM "other_items" WHERE ("b" > 10)`, insertSQL)

	insertSQL, args, err = ds.Prepared(true).ToSQL()
	ids.NoError(err)
	ids.Equal([]interface{}{int64(10)}, args)
	ids.Equal(`INSERT INTO "items" ("a", "b") SELECT "c", "d" FROM "other_items" WHERE ("b" > ?)`, insertSQL)
}

func (ids *insertDatasetSuite) TestOnConflict__ToSQLNilConflictExpression() {
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	ds := Insert("items").Rows(item{Name: "Test", Address: "111 Test Addr"}).OnConflict(nil)
	insertSQL, args, err := ds.ToSQL()
	ids.NoError(err)
	ids.Empty(args)
	ids.Equal(`INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test')`, insertSQL)

	insertSQL, args, err = ds.Prepared(true).ToSQL()
	ids.NoError(err)
	ids.Equal([]interface{}{"111 Test Addr", "Test"}, args)
	ids.Equal(`INSERT INTO "items" ("address", "name") VALUES (?, ?)`, insertSQL)
}

func (ids *insertDatasetSuite) TestOnConflict__ToSQLDoUpdate() {
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	i := item{Name: "Test", Address: "111 Test Addr"}
	ds := Insert("items").Rows(i).OnConflict(
		DoUpdate("name", Record{"address": L("excluded.address")}),
	)
	insertSQL, args, err := ds.ToSQL()
	ids.NoError(err)
	ids.Empty(args)
	ids.Equal(`INSERT INTO "items" ("address", "name") VALUES `+
		`('111 Test Addr', 'Test') `+
		`ON CONFLICT (name) `+
		`DO UPDATE `+
		`SET "address"=excluded.address`, insertSQL)

	insertSQL, args, err = ds.Prepared(true).ToSQL()
	ids.NoError(err)
	ids.Equal([]interface{}{"111 Test Addr", "Test"}, args)
	ids.Equal(
		`INSERT INTO "items" ("address", "name") VALUES (?, ?) ON CONFLICT (name) DO UPDATE SET "address"=excluded.address`,
		insertSQL,
	)
}

func (ids *insertDatasetSuite) TestOnConflict__ToSQLDoUpdateWhere() {
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	i := item{Name: "Test", Address: "111 Test Addr"}
	ds := Insert("items").Rows(i).OnConflict(
		DoUpdate("name", Record{"address": L("excluded.address")}).
			Where(C("name").Eq("Test")),
	)

	insertSQL, args, err := ds.ToSQL()
	ids.NoError(err)
	ids.Empty(args)
	ids.Equal(`INSERT INTO "items" ("address", "name") VALUES `+
		`('111 Test Addr', 'Test') `+
		`ON CONFLICT (name) `+
		`DO UPDATE `+
		`SET "address"=excluded.address WHERE ("name" = 'Test')`, insertSQL)

	insertSQL, args, err = ds.Prepared(true).ToSQL()
	ids.NoError(err)
	ids.Equal([]interface{}{"111 Test Addr", "Test", "Test"}, args)
	ids.Equal(`INSERT INTO "items" ("address", "name") VALUES `+
		`(?, ?) `+
		`ON CONFLICT (name) `+
		`DO UPDATE `+
		`SET "address"=excluded.address WHERE ("name" = ?)`, insertSQL)
}

func (ids *insertDatasetSuite) TestOnConflict__ToSQLWithDatasetDoUpdateWhere() {
	fromDs := From("ds2")
	ds := Insert("items").
		FromQuery(fromDs).
		OnConflict(
			DoUpdate("name", Record{"address": L("excluded.address")}).Where(C("name").Eq("Test")),
		)

	insertSQL, args, err := ds.ToSQL()
	ids.NoError(err)
	ids.Empty(args)
	ids.Equal(`INSERT INTO "items" `+
		`SELECT * FROM "ds2" `+
		`ON CONFLICT (name) `+
		`DO UPDATE `+
		`SET "address"=excluded.address WHERE ("name" = 'Test')`, insertSQL)

	insertSQL, args, err = ds.Prepared(true).ToSQL()
	ids.NoError(err)
	ids.Equal([]interface{}{"Test"}, args)
	ids.Equal(`INSERT INTO "items" `+
		`SELECT * FROM "ds2" `+
		`ON CONFLICT (name) `+
		`DO UPDATE `+
		`SET "address"=excluded.address WHERE ("name" = ?)`, insertSQL)
}

func (ids *insertDatasetSuite) TestOnConflict_ToSQLDoNothing() {
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	ds := Insert("items").Rows(item{Name: "Test", Address: "111 Test Addr"}).OnConflict(DoNothing())
	insertSQL, args, err := ds.ToSQL()
	ids.NoError(err)
	ids.Empty(args)
	ids.Equal(`INSERT INTO "items" ("address", "name") VALUES `+
		`('111 Test Addr', 'Test') `+
		`ON CONFLICT DO NOTHING`, insertSQL)

	insertSQL, args, err = ds.Prepared(true).ToSQL()
	ids.NoError(err)
	ids.Equal([]interface{}{"111 Test Addr", "Test"}, args)
	ids.Equal(`INSERT INTO "items" ("address", "name") VALUES (?, ?) ON CONFLICT DO NOTHING`, insertSQL)
}

func (ids *insertDatasetSuite) TestReturning() {
	ds := Insert("test")
	dsc := ds.GetClauses()
	ec := dsc.SetReturning(exp.NewColumnListExpression(C("a")))
	ids.Equal(ec, ds.Returning("a").GetClauses())
	ids.Equal(dsc, ds.GetClauses())
}

func (ids *insertDatasetSuite) TestReturning_ToSQL() {
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	bd := Insert("items").Returning("id")

	ds := bd.FromQuery(From("other_items").Where(C("b").Gt(10)))

	insertSQL, args, err := ds.ToSQL()
	ids.NoError(err)
	ids.Empty(args)
	ids.Equal(`INSERT INTO "items" SELECT * FROM "other_items" WHERE ("b" > 10) RETURNING "id"`, insertSQL)

	insertSQL, args, err = ds.Prepared(true).ToSQL()
	ids.NoError(err)
	ids.Equal([]interface{}{int64(10)}, args)
	ids.Equal(`INSERT INTO "items" SELECT * FROM "other_items" WHERE ("b" > ?) RETURNING "id"`, insertSQL)

	ds = bd.Rows(map[string]interface{}{"name": "Test", "address": "111 Test Addr"})

	insertSQL, args, err = ds.ToSQL()
	ids.NoError(err)
	ids.Empty(args)
	ids.Equal(
		`INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test') RETURNING "id"`,
		insertSQL,
	)

	insertSQL, args, err = ds.Prepared(true).ToSQL()
	ids.NoError(err)
	ids.Equal([]interface{}{"111 Test Addr", "Test"}, args)
	ids.Equal(`INSERT INTO "items" ("address", "name") VALUES (?, ?) RETURNING "id"`, insertSQL)

	ds = bd.Rows(item{Name: "Test", Address: "111 Test Addr"})

	insertSQL, _, err = ds.ToSQL()
	ids.NoError(err)
	ids.Equal(
		`INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test') RETURNING "id"`,
		insertSQL,
	)

	insertSQL, args, err = ds.Prepared(true).ToSQL()
	ids.NoError(err)
	ids.Equal([]interface{}{"111 Test Addr", "Test"}, args)
	ids.Equal(`INSERT INTO "items" ("address", "name") VALUES (?, ?) RETURNING "id"`, insertSQL)
}

func (ids *insertDatasetSuite) TestReturning_ToSQLReturnNotSupported() {
	ds1 := New("no-return", nil).Insert("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	_, _, err := ds1.Returning("id").Rows(item{Name: "Test", Address: "111 Test Addr"}).ToSQL()
	ids.EqualError(err, "goqu: dialect does not support RETURNING clause [dialect=no-return]")

	_, _, err = ds1.Returning("id").Rows(From("test2")).ToSQL()
	ids.EqualError(err, "goqu: dialect does not support RETURNING clause [dialect=no-return]")
}

func (ids *insertDatasetSuite) TestExecutor() {
	mDb, _, err := sqlmock.New()
	ids.NoError(err)

	ds := newInsertDataset("mock", exec.NewQueryFactory(mDb)).
		Into("items").
		Rows(Record{"address": "111 Test Addr", "name": "Test1"})

	isql, args, err := ds.Executor().ToSQL()
	ids.NoError(err)
	ids.Empty(args)
	ids.Equal(`INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1')`, isql)

	isql, args, err = ds.Prepared(true).Executor().ToSQL()
	ids.NoError(err)
	ids.Equal([]interface{}{"111 Test Addr", "Test1"}, args)
	ids.Equal(`INSERT INTO "items" ("address", "name") VALUES (?, ?)`, isql)
}

func TestInsertDataset(t *testing.T) {
	suite.Run(t, new(insertDatasetSuite))
}
