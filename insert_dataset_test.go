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
	"github.com/stretchr/testify/assert"
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
	t := ids.T()
	ds := Insert("test")
	assert.Equal(t, ds.Clone(), ds)
}

func (ids *insertDatasetSuite) TestExpression() {
	t := ids.T()
	ds := Insert("test")
	assert.Equal(t, ds.Expression(), ds)
}

func (ids *insertDatasetSuite) TestDialect() {
	t := ids.T()
	ds := Insert("test")
	assert.NotNil(t, ds.Dialect())
}

func (ids *insertDatasetSuite) TestWithDialect() {
	t := ids.T()
	ds := Insert("test")
	md := new(mocks.SQLDialect)
	ds = ds.SetDialect(md)

	dialect := GetDialect("default")
	ds = ds.WithDialect("default")
	assert.Equal(t, ds.Dialect(), dialect)
}

func (ids *insertDatasetSuite) TestPrepared() {
	t := ids.T()
	ds := Insert("test")
	preparedDs := ds.Prepared(true)
	assert.True(t, preparedDs.IsPrepared())
	assert.False(t, ds.IsPrepared())
	// should apply the prepared to any datasets created from the root
	assert.True(t, preparedDs.Returning(C("col")).IsPrepared())
}

func (ids *insertDatasetSuite) TestGetClauses() {
	t := ids.T()
	ds := Insert("test")
	ce := exp.NewInsertClauses().SetInto(I("test"))
	assert.Equal(t, ce, ds.GetClauses())
}

func (ids *insertDatasetSuite) TestWith() {
	t := ids.T()
	from := Insert("cte")
	ds := Insert("test")
	dsc := ds.GetClauses()
	ec := dsc.CommonTablesAppend(exp.NewCommonTableExpression(false, "test-cte", from))
	assert.Equal(t, ec, ds.With("test-cte", from).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (ids *insertDatasetSuite) TestWithRecursive() {
	t := ids.T()
	from := Insert("cte")
	ds := Insert("test")
	dsc := ds.GetClauses()
	ec := dsc.CommonTablesAppend(exp.NewCommonTableExpression(true, "test-cte", from))
	assert.Equal(t, ec, ds.WithRecursive("test-cte", from).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (ids *insertDatasetSuite) TestRows_ToSQLWithNullTimeField() {
	t := ids.T()
	type item struct {
		CreatedAt *time.Time `db:"created_at"`
	}
	ds := Insert("items").Rows(item{CreatedAt: nil})
	insertSQL, args, err := ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `INSERT INTO "items" ("created_at") VALUES (NULL)`, insertSQL)

	insertSQL, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `INSERT INTO "items" ("created_at") VALUES (NULL)`, insertSQL)
}

func (ids *insertDatasetSuite) TestRows_ToSQLWithInvalidValue() {
	t := ids.T()
	ds := Insert("test").Rows(true)
	_, _, err := ds.ToSQL()
	assert.EqualError(t, err, "goqu: unsupported insert must be map, goqu.Record, or struct type got: bool")

	_, _, err = ds.Prepared(true).ToSQL()
	assert.EqualError(t, err, "goqu: unsupported insert must be map, goqu.Record, or struct type got: bool")
}

func (ids *insertDatasetSuite) TestRows_ToSQLWithStructs() {
	t := ids.T()
	type item struct {
		Address string    `db:"address"`
		Name    string    `db:"name"`
		Created time.Time `db:"created"`
	}
	ds := Insert("items")
	created, _ := time.Parse("2006-01-02", "2015-01-01")
	ds1 := ds.Rows(item{Name: "Test", Address: "111 Test Addr", Created: created})

	insertSQL, args, err := ds1.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t,
		`INSERT INTO "items" ("address", "created", "name") VALUES ('111 Test Addr', '`+created.Format(time.RFC3339Nano)+`', 'Test')`,
		insertSQL,
	) // #nosec

	insertSQL, args, err = ds1.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", created, "Test"})
	assert.Equal(t, `INSERT INTO "items" ("address", "created", "name") VALUES (?, ?, ?)`, insertSQL)

	ds2 := ds1.Rows(
		item{Address: "111 Test Addr", Name: "Test1", Created: created},
		item{Address: "211 Test Addr", Name: "Test2", Created: created},
		item{Address: "311 Test Addr", Name: "Test3", Created: created},
		item{Address: "411 Test Addr", Name: "Test4", Created: created},
	)

	insertSQL, args, err = ds2.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t,
		`INSERT INTO "items" ("address", "created", "name") VALUES `+
			`('111 Test Addr', '`+created.Format(time.RFC3339Nano)+`', 'Test1'), `+
			`('211 Test Addr', '`+created.Format(time.RFC3339Nano)+`', 'Test2'), `+
			`('311 Test Addr', '`+created.Format(time.RFC3339Nano)+`', 'Test3'), `+
			`('411 Test Addr', '`+created.Format(time.RFC3339Nano)+`', 'Test4')`,
		insertSQL,
	)

	insertSQL, args, err = ds2.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{
		"111 Test Addr", created, "Test1",
		"211 Test Addr", created, "Test2",
		"311 Test Addr", created, "Test3",
		"411 Test Addr", created, "Test4",
	})
	assert.Equal(t,
		`INSERT INTO "items" ("address", "created", "name") VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?)`,
		insertSQL,
	)
}

func (ids *insertDatasetSuite) TestRows_ToSQLWithEmbeddedStruct() {
	t := ids.T()
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
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `INSERT INTO "items" ("address", "home_phone", "name", "primary_phone") VALUES `+
		`('111 Test Addr', '123123', 'Test', '456456')`, insertSQL)

	insertSQL, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "123123", "Test", "456456"})
	assert.Equal(
		t,
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
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `INSERT INTO "items" ("address", "home_phone", "name", "primary_phone") VALUES `+
		`('111 Test Addr', '123123', 'Test1', '456456'), `+
		`('211 Test Addr', '123123', 'Test2', '456456'), `+
		`('311 Test Addr', '123123', 'Test3', '456456'), `+
		`('411 Test Addr', '123123', 'Test4', '456456')`, insertSQL)

	insertSQL, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{
		"111 Test Addr", "123123", "Test1", "456456",
		"211 Test Addr", "123123", "Test2", "456456",
		"311 Test Addr", "123123", "Test3", "456456",
		"411 Test Addr", "123123", "Test4", "456456",
	})
	assert.Equal(t, `INSERT INTO "items" ("address", "home_phone", "name", "primary_phone") VALUES `+
		`(?, ?, ?, ?), `+
		`(?, ?, ?, ?), `+
		`(?, ?, ?, ?), `+
		`(?, ?, ?, ?)`, insertSQL)
}

func (ids *insertDatasetSuite) TestRows_ToSQLWithEmbeddedStructPtr() {
	t := ids.T()
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
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `INSERT INTO "items" ("address", "home_phone", "name", "primary_phone") VALUES `+
		`('111 Test Addr', '123123', 'Test', '456456')`, insertSQL)

	insertSQL, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "123123", "Test", "456456"})
	assert.Equal(
		t,
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
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `INSERT INTO "items" ("address", "home_phone", "name", "primary_phone") VALUES `+
		`('111 Test Addr', '123123', 'Test1', '456456'), `+
		`('211 Test Addr', '123123', 'Test2', '456456'), `+
		`('311 Test Addr', '123123', 'Test3', '456456'), `+
		`('411 Test Addr', '123123', 'Test4', '456456')`, insertSQL)

	insertSQL, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{
		"111 Test Addr", "123123", "Test1", "456456",
		"211 Test Addr", "123123", "Test2", "456456",
		"311 Test Addr", "123123", "Test3", "456456",
		"411 Test Addr", "123123", "Test4", "456456",
	})
	assert.Equal(t, `INSERT INTO "items" ("address", "home_phone", "name", "primary_phone") VALUES `+
		`(?, ?, ?, ?), `+
		`(?, ?, ?, ?), `+
		`(?, ?, ?, ?), `+
		`(?, ?, ?, ?)`, insertSQL)
}

func (ids *insertDatasetSuite) TestRows_ToSQLWithValuer() {
	t := ids.T()
	type item struct {
		Address string        `db:"address"`
		Name    string        `db:"name"`
		Valuer  sql.NullInt64 `db:"valuer"`
	}

	bd := Insert("items")
	ds := bd.Rows(item{Name: "Test", Address: "111 Test Addr", Valuer: sql.NullInt64{Int64: 10, Valid: true}})
	insertSQL, args, err := ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `INSERT INTO "items" ("address", "name", "valuer") VALUES ('111 Test Addr', 'Test', 10)`, insertSQL)

	insertSQL, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test", int64(10)})
	assert.Equal(t, `INSERT INTO "items" ("address", "name", "valuer") VALUES (?, ?, ?)`, insertSQL)

	ds = bd.Rows(
		item{Address: "111 Test Addr", Name: "Test1", Valuer: sql.NullInt64{Int64: 10, Valid: true}},
		item{Address: "211 Test Addr", Name: "Test2", Valuer: sql.NullInt64{Int64: 20, Valid: true}},
		item{Address: "311 Test Addr", Name: "Test3", Valuer: sql.NullInt64{Int64: 30, Valid: true}},
		item{Address: "411 Test Addr", Name: "Test4", Valuer: sql.NullInt64{Int64: 40, Valid: true}},
	)
	insertSQL, args, err = ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `INSERT INTO "items" ("address", "name", "valuer") VALUES `+
		`('111 Test Addr', 'Test1', 10), `+
		`('211 Test Addr', 'Test2', 20), `+
		`('311 Test Addr', 'Test3', 30), `+
		`('411 Test Addr', 'Test4', 40)`, insertSQL)

	insertSQL, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{
		"111 Test Addr", "Test1", int64(10),
		"211 Test Addr", "Test2", int64(20),
		"311 Test Addr", "Test3", int64(30),
		"411 Test Addr", "Test4", int64(40),
	})
	assert.Equal(t, `INSERT INTO "items" ("address", "name", "valuer") VALUES `+
		`(?, ?, ?), `+
		`(?, ?, ?), `+
		`(?, ?, ?), `+
		`(?, ?, ?)`, insertSQL)
}

func (ids *insertDatasetSuite) TestRows_ToSQLWithValuerNull() {
	t := ids.T()
	type item struct {
		Address string        `db:"address"`
		Name    string        `db:"name"`
		Valuer  sql.NullInt64 `db:"valuer"`
	}

	bd := Insert("items")
	ds := bd.Rows(item{Name: "Test", Address: "111 Test Addr"})
	insertSQL, args, err := ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `INSERT INTO "items" ("address", "name", "valuer") VALUES ('111 Test Addr', 'Test', NULL)`, insertSQL)

	insertSQL, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test"})
	assert.Equal(t, `INSERT INTO "items" ("address", "name", "valuer") VALUES (?, ?, NULL)`, insertSQL)

	ds = bd.Rows(
		item{Address: "111 Test Addr", Name: "Test1"},
		item{Address: "211 Test Addr", Name: "Test2"},
		item{Address: "311 Test Addr", Name: "Test3"},
		item{Address: "411 Test Addr", Name: "Test4"},
	)
	insertSQL, args, err = ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `INSERT INTO "items" ("address", "name", "valuer") VALUES `+
		`('111 Test Addr', 'Test1', NULL), `+
		`('211 Test Addr', 'Test2', NULL), `+
		`('311 Test Addr', 'Test3', NULL), `+
		`('411 Test Addr', 'Test4', NULL)`,
		insertSQL,
	)

	insertSQL, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{
		"111 Test Addr", "Test1",
		"211 Test Addr", "Test2",
		"311 Test Addr", "Test3",
		"411 Test Addr", "Test4",
	})
	assert.Equal(t, `INSERT INTO "items" ("address", "name", "valuer") VALUES `+
		`(?, ?, NULL), `+
		`(?, ?, NULL), `+
		`(?, ?, NULL), `+
		`(?, ?, NULL)`,
		insertSQL,
	)
}

func (ids *insertDatasetSuite) TestRows_ToSQLWithMaps() {
	t := ids.T()
	ds := Insert("items")

	ds1 := ds.Rows(map[string]interface{}{"name": "Test", "address": "111 Test Addr"})
	insertSQL, args, err := ds1.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test')`, insertSQL)

	insertSQL, args, err = ds1.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test"})
	assert.Equal(t, `INSERT INTO "items" ("address", "name") VALUES (?, ?)`, insertSQL)

	ds1 = ds.Rows(
		map[string]interface{}{"address": "111 Test Addr", "name": "Test1"},
		map[string]interface{}{"address": "211 Test Addr", "name": "Test2"},
		map[string]interface{}{"address": "311 Test Addr", "name": "Test3"},
		map[string]interface{}{"address": "411 Test Addr", "name": "Test4"},
	)
	insertSQL, _, err = ds1.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `INSERT INTO "items" ("address", "name") VALUES `+
		`('111 Test Addr', 'Test1'), `+
		`('211 Test Addr', 'Test2'), `+
		`('311 Test Addr', 'Test3'), `+
		`('411 Test Addr', 'Test4')`,
		insertSQL,
	)

	insertSQL, args, err = ds1.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{
		"111 Test Addr", "Test1",
		"211 Test Addr", "Test2",
		"311 Test Addr", "Test3",
		"411 Test Addr", "Test4",
	})
	assert.Equal(t, `INSERT INTO "items" ("address", "name") VALUES (?, ?), (?, ?), (?, ?), (?, ?)`, insertSQL)
}

func (ids *insertDatasetSuite) TestRows_ToSQLWithSQLBuilder() {
	t := ids.T()
	ds := Insert("items")

	ds1 := ds.Rows(From("other_items").Where(C("b").Gt(10)))

	insertSQL, args, err := ds1.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `INSERT INTO "items" SELECT * FROM "other_items" WHERE ("b" > 10)`, insertSQL)

	insertSQL, args, err = ds1.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(10)})
	assert.Equal(t, `INSERT INTO "items" SELECT * FROM "other_items" WHERE ("b" > ?)`, insertSQL)
}

func (ids *insertDatasetSuite) TestRows_ToSQLWithMapsWithDifferentLengths() {
	t := ids.T()
	ds1 := Insert("items").Rows(
		map[string]interface{}{"address": "111 Test Addr", "name": "Test1"},
		map[string]interface{}{"address": "211 Test Addr"},
		map[string]interface{}{"address": "311 Test Addr", "name": "Test3"},
		map[string]interface{}{"address": "411 Test Addr", "name": "Test4"},
	)
	_, _, err := ds1.ToSQL()
	assert.EqualError(t, err, "goqu: rows with different value length expected 2 got 1")
	_, _, err = ds1.Prepared(true).ToSQL()
	assert.EqualError(t, err, "goqu: rows with different value length expected 2 got 1")
}

func (ids *insertDatasetSuite) TestRows_ToSQLWitDifferentKeys() {
	t := ids.T()
	ds := Insert("items").Rows(
		map[string]interface{}{"address": "111 Test Addr", "name": "test"},
		map[string]interface{}{"phoneNumber": 10, "address": "111 Test Addr"},
	)
	_, _, err := ds.ToSQL()
	assert.EqualError(
		t,
		err,
		`goqu: rows with different keys expected ["address","name"] got ["address","phoneNumber"]`,
	)

	_, _, err = ds.Prepared(true).ToSQL()
	assert.EqualError(
		t,
		err,
		`goqu: rows with different keys expected ["address","name"] got ["address","phoneNumber"]`,
	)
}

func (ids *insertDatasetSuite) TestRows_ToSQLDifferentTypes() {
	t := ids.T()
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
	assert.EqualError(t, err, "goqu: rows must be all the same type expected goqu.item got goqu.item2")
	_, _, err = ds.Prepared(true).ToSQL()
	assert.EqualError(t, err, "goqu: rows must be all the same type expected goqu.item got goqu.item2")

	ds = bd.Rows(
		item{Address: "111 Test Addr", Name: "Test1"},
		map[string]interface{}{"address": "211 Test Addr", "name": "Test2"},
		item{Address: "311 Test Addr", Name: "Test3"},
		map[string]interface{}{"address": "411 Test Addr", "name": "Test4"},
	)
	_, _, err = ds.ToSQL()
	assert.EqualError(
		t,
		err,
		"goqu: rows must be all the same type expected goqu.item got map[string]interface {}",
	)

	_, _, err = ds.Prepared(true).ToSQL()
	assert.EqualError(
		t,
		err,
		"goqu: rows must be all the same type expected goqu.item got map[string]interface {}",
	)
}

func (ids *insertDatasetSuite) TestRows_ToSQLWithGoquSkipInsertTagSQL() {
	t := ids.T()
	type item struct {
		ID      uint32 `db:"id" goqu:"skipinsert"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	ds := Insert("items")

	ds1 := ds.Rows(item{Name: "Test", Address: "111 Test Addr"})

	insertSQL, args, err := ds1.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test')`, insertSQL)

	insertSQL, args, err = ds1.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test"})
	assert.Equal(t, `INSERT INTO "items" ("address", "name") VALUES (?, ?)`, insertSQL)

	ds1 = ds.Rows(
		item{Name: "Test1", Address: "111 Test Addr"},
		item{Name: "Test2", Address: "211 Test Addr"},
		item{Name: "Test3", Address: "311 Test Addr"},
		item{Name: "Test4", Address: "411 Test Addr"},
	)

	insertSQL, args, err = ds1.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `INSERT INTO "items" ("address", "name") VALUES `+
		`('111 Test Addr', 'Test1'), `+
		`('211 Test Addr', 'Test2'), `+
		`('311 Test Addr', 'Test3'), `+
		`('411 Test Addr', 'Test4')`,
		insertSQL,
	)

	insertSQL, args, err = ds1.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{
		"111 Test Addr", "Test1",
		"211 Test Addr", "Test2",
		"311 Test Addr", "Test3",
		"411 Test Addr", "Test4",
	})
	assert.Equal(t, `INSERT INTO "items" ("address", "name") VALUES (?, ?), (?, ?), (?, ?), (?, ?)`, insertSQL)
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
	ids.Equal(insertSQL, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test')`)

	insertSQL, args, err = ds1.Prepared(true).ToSQL()
	ids.NoError(err)
	ids.Equal([]interface{}{"111 Test Addr", "Test"}, args)
	ids.Equal(insertSQL, `INSERT INTO "items" ("address", "name") VALUES (?, ?)`)

	ds1 = ds.Rows(item{})

	insertSQL, args, err = ds1.ToSQL()
	ids.NoError(err)
	ids.Empty(args)
	ids.Equal(insertSQL, `INSERT INTO "items" ("address", "name") VALUES (DEFAULT, DEFAULT)`)

	insertSQL, args, err = ds1.Prepared(true).ToSQL()
	ids.NoError(err)
	ids.Empty(args)
	ids.Equal(insertSQL, `INSERT INTO "items" ("address", "name") VALUES (DEFAULT, DEFAULT)`)
}

func (ids *insertDatasetSuite) TestRows_ToSQLWithDefaultValues() {
	t := ids.T()
	ds := Insert("items")
	ds1 := ds.Rows()

	insertSQL, args, err := ds1.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `INSERT INTO "items" DEFAULT VALUES`, insertSQL)

	insertSQL, args, err = ds1.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `INSERT INTO "items" DEFAULT VALUES`, insertSQL)

	ds1 = ds.Rows(map[string]interface{}{"name": Default(), "address": Default()})
	insertSQL, args, err = ds1.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `INSERT INTO "items" ("address", "name") VALUES (DEFAULT, DEFAULT)`, insertSQL)

	insertSQL, _, err = ds1.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `INSERT INTO "items" ("address", "name") VALUES (DEFAULT, DEFAULT)`, insertSQL)
}

func (ids *insertDatasetSuite) TestToSQL() {
	t := ids.T()
	md := new(mocks.SQLDialect)
	ds := Insert("test").SetDialect(md)
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(false)
	md.On("ToInsertSQL", sqlB, c).Return(nil).Once()
	insertSQL, args, err := ds.ToSQL()
	assert.Empty(t, insertSQL)
	assert.Empty(t, args)
	assert.Nil(t, err)
	md.AssertExpectations(t)
}

func (ids *insertDatasetSuite) TestToSQL_WithNoInto() {
	t := ids.T()
	ds1 := newInsertDataset("test", nil).Rows(map[string]interface{}{
		"address": "111 Test Addr", "name": "Test1",
	})
	_, _, err := ds1.ToSQL()
	assert.EqualError(t, err, "goqu: no source found when generating insert sql")
	_, _, err = ds1.Prepared(true).ToSQL()
	assert.EqualError(t, err, "goqu: no source found when generating insert sql")
}

func (ids *insertDatasetSuite) TestToSQL_ReturnedError() {
	t := ids.T()
	md := new(mocks.SQLDialect)
	ds := Insert("test").SetDialect(md)
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(false)
	ee := errors.New("expected error")
	md.On("ToInsertSQL", sqlB, c).Run(func(args mock.Arguments) {
		args.Get(0).(sb.SQLBuilder).SetError(ee)
	}).Once()

	insertSQL, args, err := ds.ToSQL()
	assert.Empty(t, insertSQL)
	assert.Empty(t, args)
	assert.Equal(t, ee, err)
	md.AssertExpectations(t)
}

func (ids *insertDatasetSuite) TestFromQuery_ToSQL() {
	t := ids.T()
	bd := Insert("items")

	ds := bd.FromQuery(From("other_items").Where(C("b").Gt(10)))

	insertSQL, args, err := ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `INSERT INTO "items" SELECT * FROM "other_items" WHERE ("b" > 10)`, insertSQL)

	insertSQL, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(10)})
	assert.Equal(t, `INSERT INTO "items" SELECT * FROM "other_items" WHERE ("b" > ?)`, insertSQL)
}

func (ids *insertDatasetSuite) TestFromQuery_ToSQLWithCols() {
	t := ids.T()
	bd := Insert("items")

	ds := bd.Cols("a", "b").FromQuery(From("other_items").Select("c", "d").Where(C("b").Gt(10)))

	insertSQL, args, err := ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `INSERT INTO "items" ("a", "b") SELECT "c", "d" FROM "other_items" WHERE ("b" > 10)`, insertSQL)

	insertSQL, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(10)})
	assert.Equal(t, `INSERT INTO "items" ("a", "b") SELECT "c", "d" FROM "other_items" WHERE ("b" > ?)`, insertSQL)
}

func (ids *insertDatasetSuite) TestOnConflict__ToSQLNilConflictExpression() {
	t := ids.T()
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	ds := Insert("items").Rows(item{Name: "Test", Address: "111 Test Addr"}).OnConflict(nil)
	insertSQL, args, err := ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test')`, insertSQL)

	insertSQL, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{"111 Test Addr", "Test"}, args)
	assert.Equal(t, `INSERT INTO "items" ("address", "name") VALUES (?, ?)`, insertSQL)
}

func (ids *insertDatasetSuite) TestOnConflict__ToSQLDoUpdate() {
	t := ids.T()
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	i := item{Name: "Test", Address: "111 Test Addr"}
	ds := Insert("items").Rows(i).OnConflict(
		DoUpdate("name", Record{"address": L("excluded.address")}),
	)
	insertSQL, args, err := ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `INSERT INTO "items" ("address", "name") VALUES `+
		`('111 Test Addr', 'Test') `+
		`ON CONFLICT (name) `+
		`DO UPDATE `+
		`SET "address"=excluded.address`, insertSQL)

	insertSQL, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{"111 Test Addr", "Test"}, args)
	assert.Equal(
		t,
		`INSERT INTO "items" ("address", "name") VALUES (?, ?) ON CONFLICT (name) DO UPDATE SET "address"=excluded.address`,
		insertSQL,
	)
}

func (ids *insertDatasetSuite) TestOnConflict__ToSQLDoUpdateWhere() {
	t := ids.T()
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
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `INSERT INTO "items" ("address", "name") VALUES `+
		`('111 Test Addr', 'Test') `+
		`ON CONFLICT (name) `+
		`DO UPDATE `+
		`SET "address"=excluded.address WHERE ("name" = 'Test')`, insertSQL)

	insertSQL, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{"111 Test Addr", "Test", "Test"}, args)
	assert.Equal(t, `INSERT INTO "items" ("address", "name") VALUES `+
		`(?, ?) `+
		`ON CONFLICT (name) `+
		`DO UPDATE `+
		`SET "address"=excluded.address WHERE ("name" = ?)`, insertSQL)
}

func (ids *insertDatasetSuite) TestOnConflict__ToSQLWithDatasetDoUpdateWhere() {
	t := ids.T()
	fromDs := From("ds2")
	ds := Insert("items").
		FromQuery(fromDs).
		OnConflict(
			DoUpdate("name", Record{"address": L("excluded.address")}).Where(C("name").Eq("Test")),
		)

	insertSQL, args, err := ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `INSERT INTO "items" `+
		`SELECT * FROM "ds2" `+
		`ON CONFLICT (name) `+
		`DO UPDATE `+
		`SET "address"=excluded.address WHERE ("name" = 'Test')`, insertSQL)

	insertSQL, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{"Test"}, args)
	assert.Equal(t, `INSERT INTO "items" `+
		`SELECT * FROM "ds2" `+
		`ON CONFLICT (name) `+
		`DO UPDATE `+
		`SET "address"=excluded.address WHERE ("name" = ?)`, insertSQL)
}

func (ids *insertDatasetSuite) TestOnConflict_ToSQLDoNothing() {
	t := ids.T()
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	ds := Insert("items").Rows(item{Name: "Test", Address: "111 Test Addr"}).OnConflict(DoNothing())
	insertSQL, args, err := ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `INSERT INTO "items" ("address", "name") VALUES `+
		`('111 Test Addr', 'Test') `+
		`ON CONFLICT DO NOTHING`, insertSQL)

	insertSQL, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{"111 Test Addr", "Test"}, args)
	assert.Equal(t, `INSERT INTO "items" ("address", "name") VALUES (?, ?) ON CONFLICT DO NOTHING`, insertSQL)
}

func (ids *insertDatasetSuite) TestReturning() {
	t := ids.T()
	ds := Insert("test")
	dsc := ds.GetClauses()
	ec := dsc.SetReturning(exp.NewColumnListExpression(C("a")))
	assert.Equal(t, ec, ds.Returning("a").GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (ids *insertDatasetSuite) TestReturning_ToSQL() {
	t := ids.T()
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	bd := Insert("items").Returning("id")

	ds := bd.FromQuery(From("other_items").Where(C("b").Gt(10)))

	insertSQL, args, err := ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `INSERT INTO "items" SELECT * FROM "other_items" WHERE ("b" > 10) RETURNING "id"`, insertSQL)

	insertSQL, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(10)})
	assert.Equal(t, `INSERT INTO "items" SELECT * FROM "other_items" WHERE ("b" > ?) RETURNING "id"`, insertSQL)

	ds = bd.Rows(map[string]interface{}{"name": "Test", "address": "111 Test Addr"})

	insertSQL, args, err = ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(
		t,
		insertSQL,
		`INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test') RETURNING "id"`,
	)

	insertSQL, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test"})
	assert.Equal(t, `INSERT INTO "items" ("address", "name") VALUES (?, ?) RETURNING "id"`, insertSQL)

	ds = bd.Rows(item{Name: "Test", Address: "111 Test Addr"})

	insertSQL, _, err = ds.ToSQL()
	assert.NoError(t, err)
	assert.Equal(
		t,
		`INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test') RETURNING "id"`,
		insertSQL,
	)

	insertSQL, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test"})
	assert.Equal(t, `INSERT INTO "items" ("address", "name") VALUES (?, ?) RETURNING "id"`, insertSQL)
}

func (ids *insertDatasetSuite) TestReturning_ToSQLReturnNotSupported() {
	t := ids.T()
	ds1 := New("no-return", nil).Insert("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	_, _, err := ds1.Returning("id").Rows(item{Name: "Test", Address: "111 Test Addr"}).ToSQL()
	assert.EqualError(t, err, "goqu: adapter does not support RETURNING clause")

	_, _, err = ds1.Returning("id").Rows(From("test2")).ToSQL()
	assert.EqualError(t, err, "goqu: adapter does not support RETURNING clause")
}

func (ids *insertDatasetSuite) TestExecutor() {
	t := ids.T()
	mDb, _, err := sqlmock.New()
	assert.NoError(t, err)

	ds := newInsertDataset("mock", exec.NewQueryFactory(mDb)).
		Into("items").
		Rows(Record{"address": "111 Test Addr", "name": "Test1"})

	isql, args, err := ds.Executor().ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1')`, isql)

	isql, args, err = ds.Prepared(true).Executor().ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{"111 Test Addr", "Test1"}, args)
	assert.Equal(t, `INSERT INTO "items" ("address", "name") VALUES (?, ?)`, isql)
}

func TestInsertDataset(t *testing.T) {
	suite.Run(t, new(insertDatasetSuite))
}
