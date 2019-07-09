package goqu

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"testing"
	"time"

	"github.com/doug-martin/goqu/v7/exp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type datasetIntegrationTest struct {
	suite.Suite
}

func (dit *datasetIntegrationTest) SetupSuite() {
	noReturn := DefaultDialectOptions()
	noReturn.SupportsReturn = false
	RegisterDialect("no-return", noReturn)

	limitOnDelete := DefaultDialectOptions()
	limitOnDelete.SupportsLimitOnDelete = true
	RegisterDialect("limit-on-delete", limitOnDelete)

	orderOnDelete := DefaultDialectOptions()
	orderOnDelete.SupportsOrderByOnDelete = true
	RegisterDialect("order-on-delete", orderOnDelete)

	limitOnUpdate := DefaultDialectOptions()
	limitOnUpdate.SupportsLimitOnUpdate = true
	RegisterDialect("limit-on-update", limitOnUpdate)

	orderOnUpdate := DefaultDialectOptions()
	orderOnUpdate.SupportsOrderByOnUpdate = true
	RegisterDialect("order-on-update", orderOnUpdate)
}

func (dit *datasetIntegrationTest) TearDownSuite() {
	DeregisterDialect("no-return")
	DeregisterDialect("limit-on-delete")
	DeregisterDialect("order-on-delete")
	DeregisterDialect("limit-on-update")
	DeregisterDialect("order-on-update")
}

func (dit *datasetIntegrationTest) TestToDeleteSQLNoReturning() {
	t := dit.T()
	ds1 := New("no-return", nil).From("items")
	_, _, err := ds1.Returning("id").ToDeleteSQL()
	assert.EqualError(t, err, "goqu: adapter does not support RETURNING clause")
}

func (dit *datasetIntegrationTest) TestToDeleteSQLWithLimit() {
	t := dit.T()
	ds1 := New("limit-on-delete", nil).From("items")
	dsql, _, err := ds1.Limit(10).ToDeleteSQL()
	assert.Nil(t, err)
	assert.Equal(t, dsql, `DELETE FROM "items" LIMIT 10`)
}

func (dit *datasetIntegrationTest) TestToDeleteSQLWithOrder() {
	t := dit.T()
	ds1 := New("order-on-delete", nil).From("items")
	dsql, _, err := ds1.Order(C("name").Desc()).ToDeleteSQL()
	assert.Nil(t, err)
	assert.Equal(t, dsql, `DELETE FROM "items" ORDER BY "name" DESC`)
}

func (dit *datasetIntegrationTest) TestToDeleteSQLNoSources() {
	t := dit.T()
	ds1 := From("items")
	_, _, err := ds1.From().ToDeleteSQL()
	assert.EqualError(t, err, "goqu: no source found when generating delete sql")
}

func (dit *datasetIntegrationTest) TestPreparedToDeleteSQL() {
	t := dit.T()
	ds1 := From("items")
	dsql, args, err := ds1.Prepared(true).ToDeleteSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{})
	assert.Equal(t, dsql, `DELETE FROM "items"`)

	dsql, args, err = ds1.Where(I("id").Eq(1)).Prepared(true).ToDeleteSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1)})
	assert.Equal(t, dsql, `DELETE FROM "items" WHERE ("id" = ?)`)

	dsql, args, err = ds1.Returning("id").Where(I("id").Eq(1)).Prepared(true).ToDeleteSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1)})
	assert.Equal(t, dsql, `DELETE FROM "items" WHERE ("id" = ?) RETURNING "id"`)
}

func (dit *datasetIntegrationTest) TestToTruncateSQL() {
	t := dit.T()
	ds1 := From("items")
	tsql, _, err := ds1.ToTruncateSQL()
	assert.NoError(t, err)
	assert.Equal(t, tsql, `TRUNCATE "items"`)
}

func (dit *datasetIntegrationTest) TestToTruncateSQLNoSources() {
	t := dit.T()
	ds1 := From("items")
	_, _, err := ds1.From().ToTruncateSQL()
	assert.EqualError(t, err, "goqu: no source found when generating truncate sql")
}

func (dit *datasetIntegrationTest) TestToTruncateSQLWithOpts() {
	t := dit.T()
	ds1 := From("items")
	tsql, _, err := ds1.ToTruncateWithOptsSQL(TruncateOptions{Cascade: true})
	assert.NoError(t, err)
	assert.Equal(t, tsql, `TRUNCATE "items" CASCADE`)

	tsql, _, err = ds1.ToTruncateWithOptsSQL(TruncateOptions{Restrict: true})
	assert.NoError(t, err)
	assert.Equal(t, tsql, `TRUNCATE "items" RESTRICT`)

	tsql, _, err = ds1.ToTruncateWithOptsSQL(TruncateOptions{Identity: "restart"})
	assert.NoError(t, err)
	assert.Equal(t, tsql, `TRUNCATE "items" RESTART IDENTITY`)

	tsql, _, err = ds1.ToTruncateWithOptsSQL(TruncateOptions{Identity: "continue"})
	assert.NoError(t, err)
	assert.Equal(t, tsql, `TRUNCATE "items" CONTINUE IDENTITY`)
}

func (dit *datasetIntegrationTest) TestPreparedToTruncateSQL() {
	t := dit.T()
	ds1 := From("items")
	tsql, args, err := ds1.ToTruncateSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{})
	assert.Equal(t, tsql, `TRUNCATE "items"`)
}

func (dit *datasetIntegrationTest) TestInsertNullTime() {
	t := dit.T()
	ds1 := From("items")
	type item struct {
		CreatedAt *time.Time `db:"created_at"`
	}
	insertSQL, _, err := ds1.ToInsertSQL(item{CreatedAt: nil})
	assert.NoError(t, err)
	assert.Equal(t, insertSQL, `INSERT INTO "items" ("created_at") VALUES (NULL)`)
}

func (dit *datasetIntegrationTest) TestToInsertSQLNoReturning() {
	t := dit.T()
	ds1 := New("no-return", nil).From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	_, _, err := ds1.Returning("id").ToInsertSQL(item{Name: "Test", Address: "111 Test Addr"})
	assert.EqualError(t, err, "goqu: adapter does not support RETURNING clause")

	_, _, err = ds1.Returning("id").ToInsertSQL(From("test2"))
	assert.EqualError(t, err, "goqu: adapter does not support RETURNING clause")
}

func (dit *datasetIntegrationTest) TestInsert_InvalidValue() {
	t := dit.T()
	ds1 := From("no-return").From("items")
	_, _, err := ds1.ToInsertSQL(true)
	assert.EqualError(t, err, "goqu: unsupported insert must be map, goqu.Record, or struct type got: bool")
}

func (dit *datasetIntegrationTest) TestToInsertSQLWithStructs() {
	t := dit.T()
	ds1 := From("items")
	type item struct {
		Address string    `db:"address"`
		Name    string    `db:"name"`
		Created time.Time `db:"created"`
	}
	created, _ := time.Parse("2006-01-02", "2015-01-01")
	insertSQL, _, err := ds1.ToInsertSQL(item{Name: "Test", Address: "111 Test Addr", Created: created})
	assert.NoError(t, err)
	assert.Equal(t, insertSQL,
		`INSERT INTO "items" ("address", "name", "created") VALUES ('111 Test Addr', 'Test', '`+created.Format(time.RFC3339Nano)+`')`,
	) // #nosec

	insertSQL, _, err = ds1.ToInsertSQL(
		item{Address: "111 Test Addr", Name: "Test1", Created: created},
		item{Address: "211 Test Addr", Name: "Test2", Created: created},
		item{Address: "311 Test Addr", Name: "Test3", Created: created},
		item{Address: "411 Test Addr", Name: "Test4", Created: created},
	)
	assert.NoError(t, err)
	assert.Equal(t, insertSQL,
		`INSERT INTO "items" ("address", "name", "created") VALUES `+
			`('111 Test Addr', 'Test1', '`+created.Format(time.RFC3339Nano)+`'), `+
			`('211 Test Addr', 'Test2', '`+created.Format(time.RFC3339Nano)+`'), `+
			`('311 Test Addr', 'Test3', '`+created.Format(time.RFC3339Nano)+`'), `+
			`('411 Test Addr', 'Test4', '`+created.Format(time.RFC3339Nano)+`')`,
	)
}

func (dit *datasetIntegrationTest) TestToInsertSQLWithEmbeddedStruct() {
	t := dit.T()
	ds1 := From("items")
	type Phone struct {
		Primary string `db:"primary_phone"`
		Home    string `db:"home_phone"`
	}
	type item struct {
		Phone
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	insertSQL, _, err := ds1.ToInsertSQL(item{
		Name:    "Test",
		Address: "111 Test Addr",
		Phone: Phone{
			Home:    "123123",
			Primary: "456456",
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, insertSQL, `INSERT INTO "items" ("primary_phone", "home_phone", "address", "name") VALUES `+
		`('456456', '123123', '111 Test Addr', 'Test')`)

	insertSQL, _, err = ds1.ToInsertSQL(
		item{Address: "111 Test Addr", Name: "Test1", Phone: Phone{Home: "123123", Primary: "456456"}},
		item{Address: "211 Test Addr", Name: "Test2", Phone: Phone{Home: "123123", Primary: "456456"}},
		item{Address: "311 Test Addr", Name: "Test3", Phone: Phone{Home: "123123", Primary: "456456"}},
		item{Address: "411 Test Addr", Name: "Test4", Phone: Phone{Home: "123123", Primary: "456456"}},
	)
	assert.NoError(t, err)
	assert.Equal(t, insertSQL, `INSERT INTO "items" ("primary_phone", "home_phone", "address", "name") VALUES `+
		`('456456', '123123', '111 Test Addr', 'Test1'), `+
		`('456456', '123123', '211 Test Addr', 'Test2'), `+
		`('456456', '123123', '311 Test Addr', 'Test3'), `+
		`('456456', '123123', '411 Test Addr', 'Test4')`)
}

func (dit *datasetIntegrationTest) TestToInsertSQLWithEmbeddedStructPtr() {
	t := dit.T()
	ds1 := From("items")
	type Phone struct {
		Primary string `db:"primary_phone"`
		Home    string `db:"home_phone"`
	}
	type item struct {
		*Phone
		Address string        `db:"address"`
		Name    string        `db:"name"`
		Valuer  sql.NullInt64 `db:"valuer"`
	}
	insertSQL, _, err := ds1.ToInsertSQL(item{
		Name:    "Test",
		Address: "111 Test Addr",
		Valuer:  sql.NullInt64{Int64: 10, Valid: true},
		Phone:   &Phone{Home: "123123", Primary: "456456"},
	})
	assert.NoError(t, err)
	assert.Equal(t, insertSQL, `INSERT INTO "items" `+
		`("primary_phone", "home_phone", "address", "name", "valuer")`+
		` VALUES ('456456', '123123', '111 Test Addr', 'Test', 10)`)

	insertSQL, _, err = ds1.ToInsertSQL(
		item{Address: "111 Test Addr", Name: "Test1", Phone: &Phone{Home: "123123", Primary: "456456"}},
		item{Address: "211 Test Addr", Name: "Test2", Phone: &Phone{Home: "123123", Primary: "456456"}},
		item{Address: "311 Test Addr", Name: "Test3", Phone: &Phone{Home: "123123", Primary: "456456"}},
		item{Address: "411 Test Addr", Name: "Test4", Phone: &Phone{Home: "123123", Primary: "456456"}},
	)
	assert.NoError(t, err)
	assert.Equal(t, insertSQL,
		`INSERT INTO "items" ("primary_phone", "home_phone", "address", "name", "valuer") VALUES `+
			`('456456', '123123', '111 Test Addr', 'Test1', NULL), `+
			`('456456', '123123', '211 Test Addr', 'Test2', NULL), `+
			`('456456', '123123', '311 Test Addr', 'Test3', NULL), `+
			`('456456', '123123', '411 Test Addr', 'Test4', NULL)`)
}

func (dit *datasetIntegrationTest) TestToInsertSQLWithValuer() {
	t := dit.T()
	ds1 := From("items")

	type item struct {
		Address string        `db:"address"`
		Name    string        `db:"name"`
		Valuer  sql.NullInt64 `db:"valuer"`
	}
	sqlString, _, err := ds1.ToInsertSQL(item{Name: "Test", Address: "111 Test Addr", Valuer: sql.NullInt64{Int64: 10, Valid: true}})
	assert.NoError(t, err)
	assert.Equal(t, sqlString, `INSERT INTO "items" ("address", "name", "valuer") VALUES ('111 Test Addr', 'Test', 10)`)

	sqlString, _, err = ds1.ToInsertSQL(
		item{Address: "111 Test Addr", Name: "Test1", Valuer: sql.NullInt64{Int64: 10, Valid: true}},
		item{Address: "211 Test Addr", Name: "Test2", Valuer: sql.NullInt64{Int64: 10, Valid: true}},
		item{Address: "311 Test Addr", Name: "Test3", Valuer: sql.NullInt64{Int64: 10, Valid: true}},
		item{Address: "411 Test Addr", Name: "Test4", Valuer: sql.NullInt64{Int64: 10, Valid: true}},
	)
	assert.NoError(t, err)
	assert.Equal(t, sqlString, `INSERT INTO "items" ("address", "name", "valuer") VALUES `+
		`('111 Test Addr', 'Test1', 10), `+
		`('211 Test Addr', 'Test2', 10), `+
		`('311 Test Addr', 'Test3', 10), `+
		`('411 Test Addr', 'Test4', 10)`)
}

func (dit *datasetIntegrationTest) TestToInsertSQLWithValuerNull() {
	t := dit.T()
	ds1 := From("items")

	type item struct {
		Address string        `db:"address"`
		Name    string        `db:"name"`
		Valuer  sql.NullInt64 `db:"valuer"`
	}
	sqlString, _, err := ds1.ToInsertSQL(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(
		t,
		sqlString,
		`INSERT INTO "items" ("address", "name", "valuer") VALUES ('111 Test Addr', 'Test', NULL)`,
	)

	sqlString, _, err = ds1.ToInsertSQL(
		item{Address: "111 Test Addr", Name: "Test1"},
		item{Address: "211 Test Addr", Name: "Test2"},
		item{Address: "311 Test Addr", Name: "Test3"},
		item{Address: "411 Test Addr", Name: "Test4"},
	)
	assert.NoError(t, err)
	assert.Equal(t, sqlString, `INSERT INTO "items" ("address", "name", "valuer") VALUES `+
		`('111 Test Addr', 'Test1', NULL), `+
		`('211 Test Addr', 'Test2', NULL), `+
		`('311 Test Addr', 'Test3', NULL), `+
		`('411 Test Addr', 'Test4', NULL)`)
}

func (dit *datasetIntegrationTest) TestToInsertSQLWithMaps() {
	t := dit.T()
	ds1 := From("items")

	insertSQL, _, err := ds1.ToInsertSQL(map[string]interface{}{"name": "Test", "address": "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, insertSQL, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test')`)

	insertSQL, _, err = ds1.ToInsertSQL(
		map[string]interface{}{"address": "111 Test Addr", "name": "Test1"},
		map[string]interface{}{"address": "211 Test Addr", "name": "Test2"},
		map[string]interface{}{"address": "311 Test Addr", "name": "Test3"},
		map[string]interface{}{"address": "411 Test Addr", "name": "Test4"},
	)
	assert.NoError(t, err)
	assert.Equal(t, insertSQL, `INSERT INTO "items" ("address", "name") VALUES `+
		`('111 Test Addr', 'Test1'), `+
		`('211 Test Addr', 'Test2'), `+
		`('311 Test Addr', 'Test3'), `+
		`('411 Test Addr', 'Test4')`)

	_, _, err = ds1.ToInsertSQL(
		map[string]interface{}{"address": "111 Test Addr", "name": "Test1"},
		map[string]interface{}{"address": "211 Test Addr"},
		map[string]interface{}{"address": "311 Test Addr", "name": "Test3"},
		map[string]interface{}{"address": "411 Test Addr", "name": "Test4"},
	)
	assert.EqualError(t, err, "goqu: rows with different value length expected 2 got 1")
}

func (dit *datasetIntegrationTest) TestToInsertSQLWitSQLBuilder() {
	t := dit.T()
	ds1 := From("items")

	insertSQL, _, err := ds1.ToInsertSQL(From("other_items"))
	assert.NoError(t, err)
	assert.Equal(t, insertSQL, `INSERT INTO "items" SELECT * FROM "other_items"`)
}

func (dit *datasetIntegrationTest) TestInsertReturning() {
	t := dit.T()
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	ds1 := From("items").Returning("id")

	insertSQL, _, err := ds1.Returning("id").ToInsertSQL(From("other_items"))
	assert.NoError(t, err)
	assert.Equal(t, insertSQL, `INSERT INTO "items" SELECT * FROM "other_items" RETURNING "id"`)

	insertSQL, _, err = ds1.ToInsertSQL(map[string]interface{}{"name": "Test", "address": "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(
		t,
		insertSQL,
		`INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test') RETURNING "id"`,
	)

	insertSQL, _, err = ds1.ToInsertSQL(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(
		t,
		insertSQL,
		`INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test') RETURNING "id"`,
	)
}

func (dit *datasetIntegrationTest) TestToInsertSQLWithNoFrom() {
	t := dit.T()
	ds1 := From("test").From()
	_, _, err := ds1.ToInsertSQL(map[string]interface{}{"address": "111 Test Addr", "name": "Test1"})
	assert.EqualError(t, err, "goqu: no source found when generating insert sql")
}

func (dit *datasetIntegrationTest) TestToInsertSQLWithMapsWithDifferentLengths() {
	t := dit.T()
	ds1 := From("items")
	_, _, err := ds1.ToInsertSQL(
		map[string]interface{}{"address": "111 Test Addr", "name": "Test1"},
		map[string]interface{}{"address": "211 Test Addr"},
		map[string]interface{}{"address": "311 Test Addr", "name": "Test3"},
		map[string]interface{}{"address": "411 Test Addr", "name": "Test4"},
	)
	assert.EqualError(t, err, "goqu: rows with different value length expected 2 got 1")
}

func (dit *datasetIntegrationTest) TestToInsertSQLWitDifferentKeys() {
	t := dit.T()
	ds1 := From("items")
	_, _, err := ds1.ToInsertSQL(
		map[string]interface{}{"address": "111 Test Addr", "name": "test"},
		map[string]interface{}{"phoneNumber": 10, "address": "111 Test Addr"},
	)
	assert.EqualError(
		t,
		err,
		`goqu: rows with different keys expected ["address","name"] got ["address","phoneNumber"]`,
	)
}

func (dit *datasetIntegrationTest) TestToInsertSQLDifferentTypes() {
	t := dit.T()
	ds1 := From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	type item2 struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	_, _, err := ds1.ToInsertSQL(
		item{Address: "111 Test Addr", Name: "Test1"},
		item2{Address: "211 Test Addr", Name: "Test2"},
		item{Address: "311 Test Addr", Name: "Test3"},
		item2{Address: "411 Test Addr", Name: "Test4"},
	)
	assert.EqualError(t, err, "goqu: rows must be all the same type expected goqu.item got goqu.item2")

	_, _, err = ds1.ToInsertSQL(
		item{Address: "111 Test Addr", Name: "Test1"},
		map[string]interface{}{"address": "211 Test Addr", "name": "Test2"},
		item{Address: "311 Test Addr", Name: "Test3"},
		map[string]interface{}{"address": "411 Test Addr", "name": "Test4"},
	)
	assert.EqualError(
		t,
		err,
		"goqu: rows must be all the same type expected goqu.item got map[string]interface {}",
	)
}

func (dit *datasetIntegrationTest) TestInsertWithGoquPkTagSQL() {
	t := dit.T()
	ds1 := From("items")
	type item struct {
		ID      uint32 `db:"id" goqu:"pk,skipinsert"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	insertSQL, _, err := ds1.ToInsertSQL(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, insertSQL, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test')`)

	insertSQL, _, err = ds1.ToInsertSQL(map[string]interface{}{"name": "Test", "address": "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, insertSQL, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test')`)

	insertSQL, _, err = ds1.ToInsertSQL(
		item{Name: "Test1", Address: "111 Test Addr"},
		item{Name: "Test2", Address: "211 Test Addr"},
		item{Name: "Test3", Address: "311 Test Addr"},
		item{Name: "Test4", Address: "411 Test Addr"},
	)
	assert.NoError(t, err)
	assert.Equal(t, insertSQL, `INSERT INTO "items" ("address", "name") VALUES `+
		`('111 Test Addr', 'Test1'), `+
		`('211 Test Addr', 'Test2'), `+
		`('311 Test Addr', 'Test3'), `+
		`('411 Test Addr', 'Test4')`)
}

func (dit *datasetIntegrationTest) TestInsertWithGoquSkipInsertTagSQL() {
	t := dit.T()
	ds1 := From("items")
	type item struct {
		ID      uint32 `db:"id" goqu:"skipinsert"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	insertSQL, _, err := ds1.ToInsertSQL(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, insertSQL, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test')`)

	insertSQL, _, err = ds1.ToInsertSQL(
		item{Name: "Test1", Address: "111 Test Addr"},
		item{Name: "Test2", Address: "211 Test Addr"},
		item{Name: "Test3", Address: "311 Test Addr"},
		item{Name: "Test4", Address: "411 Test Addr"},
	)
	assert.NoError(t, err)
	assert.Equal(t, insertSQL, `INSERT INTO "items" ("address", "name") VALUES `+
		`('111 Test Addr', 'Test1'), `+
		`('211 Test Addr', 'Test2'), `+
		`('311 Test Addr', 'Test3'), `+
		`('411 Test Addr', 'Test4')`)
}

func (dit *datasetIntegrationTest) TestInsertDefaultValues() {
	t := dit.T()
	ds1 := From("items")

	insertSQL, _, err := ds1.ToInsertSQL()
	assert.NoError(t, err)
	assert.Equal(t, insertSQL, `INSERT INTO "items" DEFAULT VALUES`)

	insertSQL, _, err = ds1.ToInsertSQL(map[string]interface{}{"name": Default(), "address": Default()})
	assert.NoError(t, err)
	assert.Equal(t, insertSQL, `INSERT INTO "items" ("address", "name") VALUES (DEFAULT, DEFAULT)`)

}

func (dit *datasetIntegrationTest) TestPreparedToInsertSQLWithStructs() {
	t := dit.T()
	ds1 := From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	insertSQL, args, err := ds1.Prepared(true).ToInsertSQL(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test"})
	assert.Equal(t, insertSQL, `INSERT INTO "items" ("address", "name") VALUES (?, ?)`)

	insertSQL, args, err = ds1.Prepared(true).ToInsertSQL(
		item{Address: "111 Test Addr", Name: "Test1"},
		item{Address: "211 Test Addr", Name: "Test2"},
		item{Address: "311 Test Addr", Name: "Test3"},
		item{Address: "411 Test Addr", Name: "Test4"},
	)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{
		"111 Test Addr",
		"Test1",
		"211 Test Addr",
		"Test2",
		"311 Test Addr",
		"Test3",
		"411 Test Addr",
		"Test4",
	})
	assert.Equal(t, insertSQL, `INSERT INTO "items" ("address", "name") VALUES (?, ?), (?, ?), (?, ?), (?, ?)`)
}

func (dit *datasetIntegrationTest) TestPreparedToInsertSQLWithMaps() {
	t := dit.T()
	ds1 := From("items")

	insertSQL, args, err := ds1.Prepared(true).ToInsertSQL(map[string]interface{}{
		"name":    "Test",
		"address": "111 Test Addr",
	})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test"})
	assert.Equal(t, insertSQL, `INSERT INTO "items" ("address", "name") VALUES (?, ?)`)

	insertSQL, args, err = ds1.Prepared(true).ToInsertSQL(
		map[string]interface{}{"address": "111 Test Addr", "name": "Test1"},
		map[string]interface{}{"address": "211 Test Addr", "name": "Test2"},
		map[string]interface{}{"address": "311 Test Addr", "name": "Test3"},
		map[string]interface{}{"address": "411 Test Addr", "name": "Test4"},
	)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{
		"111 Test Addr",
		"Test1",
		"211 Test Addr",
		"Test2",
		"311 Test Addr",
		"Test3",
		"411 Test Addr",
		"Test4",
	})
	assert.Equal(t, insertSQL, `INSERT INTO "items" ("address", "name") VALUES (?, ?), (?, ?), (?, ?), (?, ?)`)
}

func (dit *datasetIntegrationTest) TestPreparedToInsertSQLWitSQLBuilder() {
	t := dit.T()
	ds1 := From("items")

	insertSQL, args, err := ds1.
		Prepared(true).
		ToInsertSQL(
			From("other_items").Where(C("b").Gt(10)),
		)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(10)})
	assert.Equal(t, insertSQL, `INSERT INTO "items" SELECT * FROM "other_items" WHERE ("b" > ?)`)
}

func (dit *datasetIntegrationTest) TestPreparedInsertReturning() {
	t := dit.T()
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	ds1 := From("items").Returning("id")

	insertSQL, args, err := ds1.
		Returning("id").
		Prepared(true).
		ToInsertSQL(From("other_items").Where(C("b").Gt(10)))
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(10)})
	assert.Equal(t, insertSQL, `INSERT INTO "items" SELECT * FROM "other_items" WHERE ("b" > ?) RETURNING "id"`)

	insertSQL, args, err = ds1.Prepared(true).ToInsertSQL(map[string]interface{}{
		"name":    "Test",
		"address": "111 Test Addr",
	})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test"})
	assert.Equal(t, insertSQL, `INSERT INTO "items" ("address", "name") VALUES (?, ?) RETURNING "id"`)

	insertSQL, args, err = ds1.Prepared(true).ToInsertSQL(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test"})
	assert.Equal(t, insertSQL, `INSERT INTO "items" ("address", "name") VALUES (?, ?) RETURNING "id"`)
}

func (dit *datasetIntegrationTest) TestPreparedInsertWithGoquPkTagSQL() {
	t := dit.T()
	ds1 := From("items")
	type item struct {
		ID      uint32 `db:"id" goqu:"pk,skipinsert"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	insertSQL, args, err := ds1.Prepared(true).ToInsertSQL(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test"})
	assert.Equal(t, insertSQL, `INSERT INTO "items" ("address", "name") VALUES (?, ?)`)

	insertSQL, args, err = ds1.Prepared(true).ToInsertSQL(map[string]interface{}{
		"name":    "Test",
		"address": "111 Test Addr",
	})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test"})
	assert.Equal(t, insertSQL, `INSERT INTO "items" ("address", "name") VALUES (?, ?)`)

	insertSQL, args, err = ds1.Prepared(true).ToInsertSQL(
		item{Name: "Test1", Address: "111 Test Addr"},
		item{Name: "Test2", Address: "211 Test Addr"},
		item{Name: "Test3", Address: "311 Test Addr"},
		item{Name: "Test4", Address: "411 Test Addr"},
	)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{
		"111 Test Addr",
		"Test1",
		"211 Test Addr",
		"Test2",
		"311 Test Addr",
		"Test3",
		"411 Test Addr",
		"Test4",
	})
	assert.Equal(t, insertSQL, `INSERT INTO "items" ("address", "name") VALUES (?, ?), (?, ?), (?, ?), (?, ?)`)
}

func (dit *datasetIntegrationTest) TestPreparedInsertWithGoquSkipInsertTagSQL() {
	t := dit.T()
	ds1 := From("items")
	type item struct {
		ID      uint32 `db:"id" goqu:"skipinsert"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	insertSQL, args, err := ds1.Prepared(true).ToInsertSQL(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test"})
	assert.Equal(t, insertSQL, `INSERT INTO "items" ("address", "name") VALUES (?, ?)`)

	insertSQL, args, err = ds1.Prepared(true).ToInsertSQL(
		item{Name: "Test1", Address: "111 Test Addr"},
		item{Name: "Test2", Address: "211 Test Addr"},
		item{Name: "Test3", Address: "311 Test Addr"},
		item{Name: "Test4", Address: "411 Test Addr"},
	)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{
		"111 Test Addr",
		"Test1",
		"211 Test Addr",
		"Test2",
		"311 Test Addr",
		"Test3",
		"411 Test Addr",
		"Test4",
	})
	assert.Equal(t, insertSQL, `INSERT INTO "items" ("address", "name") VALUES (?, ?), (?, ?), (?, ?), (?, ?)`)
}

func (dit *datasetIntegrationTest) TestPreparedInsertDefaultValues() {
	t := dit.T()
	ds1 := From("items")

	insertSQL, args, err := ds1.Prepared(true).ToInsertSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{})
	assert.Equal(t, insertSQL, `INSERT INTO "items" DEFAULT VALUES`)

	insertSQL, args, err = ds1.Prepared(true).ToInsertSQL(map[string]interface{}{
		"name":    Default(),
		"address": Default(),
	})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{})
	assert.Equal(t, insertSQL, `INSERT INTO "items" ("address", "name") VALUES (DEFAULT, DEFAULT)`)

}

func (dit *datasetIntegrationTest) TestPreparedToInsertSQLWithEmbeddedStruct() {
	t := dit.T()
	ds1 := From("items")
	type Phone struct {
		Primary string `db:"primary_phone"`
		Home    string `db:"home_phone"`
	}
	type item struct {
		Phone
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	insertSQL, args, err := ds1.Prepared(true).ToInsertSQL(item{
		Name:    "Test",
		Address: "111 Test Addr",
		Phone: Phone{
			Home:    "123123",
			Primary: "456456",
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"456456", "123123", "111 Test Addr", "Test"})
	assert.Equal(
		t,
		insertSQL,
		`INSERT INTO "items" ("primary_phone", "home_phone", "address", "name") VALUES (?, ?, ?, ?)`,
	)

	insertSQL, args, err = ds1.Prepared(true).ToInsertSQL(
		item{Address: "111 Test Addr", Name: "Test1", Phone: Phone{Home: "123123", Primary: "456456"}},
		item{Address: "211 Test Addr", Name: "Test2", Phone: Phone{Home: "123123", Primary: "456456"}},
		item{Address: "311 Test Addr", Name: "Test3", Phone: Phone{Home: "123123", Primary: "456456"}},
		item{Address: "411 Test Addr", Name: "Test4", Phone: Phone{Home: "123123", Primary: "456456"}},
	)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{
		"456456", "123123", "111 Test Addr", "Test1",
		"456456", "123123", "211 Test Addr", "Test2",
		"456456", "123123", "311 Test Addr", "Test3",
		"456456", "123123", "411 Test Addr", "Test4",
	})
	assert.Equal(t, insertSQL, `INSERT INTO "items" ("primary_phone", "home_phone", "address", "name") VALUES `+
		`(?, ?, ?, ?), `+
		`(?, ?, ?, ?), `+
		`(?, ?, ?, ?), `+
		`(?, ?, ?, ?)`)
}

func (dit *datasetIntegrationTest) TestPreparedToInsertSQLWithEmbeddedStructPtr() {
	t := dit.T()
	ds1 := From("items")
	type Phone struct {
		Primary string `db:"primary_phone"`
		Home    string `db:"home_phone"`
	}
	type item struct {
		*Phone
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	insertSQL, args, err := ds1.Prepared(true).ToInsertSQL(item{
		Name:    "Test",
		Address: "111 Test Addr",
		Phone: &Phone{
			Home:    "123123",
			Primary: "456456",
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"456456", "123123", "111 Test Addr", "Test"})
	assert.Equal(
		t,
		insertSQL,
		`INSERT INTO "items" ("primary_phone", "home_phone", "address", "name") VALUES (?, ?, ?, ?)`,
	)

	insertSQL, args, err = ds1.Prepared(true).ToInsertSQL(
		item{Address: "111 Test Addr", Name: "Test1", Phone: &Phone{Home: "123123", Primary: "456456"}},
		item{Address: "211 Test Addr", Name: "Test2", Phone: &Phone{Home: "123123", Primary: "456456"}},
		item{Address: "311 Test Addr", Name: "Test3", Phone: &Phone{Home: "123123", Primary: "456456"}},
		item{Address: "411 Test Addr", Name: "Test4", Phone: &Phone{Home: "123123", Primary: "456456"}},
	)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{
		"456456", "123123", "111 Test Addr", "Test1",
		"456456", "123123", "211 Test Addr", "Test2",
		"456456", "123123", "311 Test Addr", "Test3",
		"456456", "123123", "411 Test Addr", "Test4",
	})
	assert.Equal(t, insertSQL, `INSERT INTO "items" ("primary_phone", "home_phone", "address", "name") VALUES `+
		`(?, ?, ?, ?), `+
		`(?, ?, ?, ?), `+
		`(?, ?, ?, ?), `+
		`(?, ?, ?, ?)`)
}

func (dit *datasetIntegrationTest) TestPreparedToInsertSQLWithValuer() {
	t := dit.T()
	ds1 := From("items")

	type item struct {
		Address string        `db:"address"`
		Name    string        `db:"name"`
		Valuer  sql.NullInt64 `db:"valuer"`
	}
	sqlString, args, err := ds1.Prepared(true).ToInsertSQL(item{
		Name:    "Test",
		Address: "111 Test Addr",
		Valuer:  sql.NullInt64{Int64: 10, Valid: true},
	})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{
		"111 Test Addr", "Test", int64(10),
	})
	assert.Equal(t, sqlString, `INSERT INTO "items" ("address", "name", "valuer") VALUES (?, ?, ?)`)

	sqlString, args, err = ds1.Prepared(true).ToInsertSQL(
		item{Address: "111 Test Addr", Name: "Test1", Valuer: sql.NullInt64{Int64: 10, Valid: true}},
		item{Address: "211 Test Addr", Name: "Test2", Valuer: sql.NullInt64{Int64: 20, Valid: true}},
		item{Address: "311 Test Addr", Name: "Test3", Valuer: sql.NullInt64{Int64: 30, Valid: true}},
		item{Address: "411 Test Addr", Name: "Test4", Valuer: sql.NullInt64{Int64: 40, Valid: true}},
	)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{
		"111 Test Addr", "Test1", int64(10),
		"211 Test Addr", "Test2", int64(20),
		"311 Test Addr", "Test3", int64(30),
		"411 Test Addr", "Test4", int64(40),
	})
	assert.Equal(t, sqlString, `INSERT INTO "items" ("address", "name", "valuer") VALUES `+
		`(?, ?, ?), `+
		`(?, ?, ?), `+
		`(?, ?, ?), `+
		`(?, ?, ?)`)
}

func (dit *datasetIntegrationTest) TestPreparedToInsertSQLWithValuerNull() {
	t := dit.T()
	ds1 := From("items")

	type item struct {
		Address string        `db:"address"`
		Name    string        `db:"name"`
		Valuer  sql.NullInt64 `db:"valuer"`
	}
	sqlString, args, err := ds1.Prepared(true).ToInsertSQL(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{
		"111 Test Addr", "Test",
	})
	assert.Equal(t, sqlString, `INSERT INTO "items" ("address", "name", "valuer") VALUES (?, ?, NULL)`)

	sqlString, args, err = ds1.Prepared(true).ToInsertSQL(
		item{Address: "111 Test Addr", Name: "Test1"},
		item{Address: "211 Test Addr", Name: "Test2"},
		item{Address: "311 Test Addr", Name: "Test3"},
		item{Address: "411 Test Addr", Name: "Test4"},
	)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{
		"111 Test Addr", "Test1",
		"211 Test Addr", "Test2",
		"311 Test Addr", "Test3",
		"411 Test Addr", "Test4",
	})
	assert.Equal(t, sqlString, `INSERT INTO "items" ("address", "name", "valuer") VALUES `+
		`(?, ?, NULL), `+
		`(?, ?, NULL), `+
		`(?, ?, NULL), `+
		`(?, ?, NULL)`)
}

func (dit *datasetIntegrationTest) TestToInsertConflictSQL__OnConflictIsNil() {
	t := dit.T()
	ds1 := From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	insertSQL, _, err := ds1.ToInsertConflictSQL(nil, item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test')`, insertSQL)
}

func (dit *datasetIntegrationTest) TestToInsertConflictSQL__OnConflictexpDoUpdate() {
	t := dit.T()
	ds1 := From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	i := item{Name: "Test", Address: "111 Test Addr"}
	insertSQL, _, err := ds1.ToInsertConflictSQL(
		DoUpdate("name", Record{"address": L("excluded.address")}),
		i,
	)
	assert.NoError(t, err)
	assert.Equal(t, `INSERT INTO "items" ("address", "name") VALUES `+
		`('111 Test Addr', 'Test') `+
		`ON CONFLICT (name) `+
		`DO UPDATE `+
		`SET "address"=excluded.address`, insertSQL)
}

func (dit *datasetIntegrationTest) TestToInsertConflictSQL__OnConflictDoUpdateWhere() {
	t := dit.T()
	ds1 := From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	i := item{Name: "Test", Address: "111 Test Addr"}

	insertSQL, _, err := ds1.ToInsertConflictSQL(
		DoUpdate("name", Record{"address": L("excluded.address")}).
			Where(C("name").Eq("Test")),
		i,
	)
	assert.NoError(t, err)
	assert.Equal(t, `INSERT INTO "items" ("address", "name") VALUES `+
		`('111 Test Addr', 'Test') `+
		`ON CONFLICT (name) `+
		`DO UPDATE `+
		`SET "address"=excluded.address WHERE ("name" = 'Test')`, insertSQL)
}

func (dit *datasetIntegrationTest) TestToInsertConflictSQLWithDataset__OnConflictDoUpdateWhere() {
	t := dit.T()
	ds1 := From("items")
	ds2 := From("ds2")

	insertSQL, _, err := ds1.ToInsertConflictSQL(
		DoUpdate("name", Record{"address": L("excluded.address")}).
			Where(C("name").Eq("Test")),
		ds2,
	)
	assert.NoError(t, err)
	assert.Equal(t, `INSERT INTO "items" `+
		`SELECT * FROM "ds2" `+
		`ON CONFLICT (name) `+
		`DO UPDATE `+
		`SET "address"=excluded.address WHERE ("name" = 'Test')`, insertSQL)
}

func (dit *datasetIntegrationTest) TestInsertConflict__ImplementsConflictExpressionInterface() {
	t := dit.T()
	assert.Implements(t, (*exp.ConflictExpression)(nil), DoNothing())
	assert.Implements(t, (*exp.ConflictExpression)(nil), DoUpdate("", nil))
}

func (dit *datasetIntegrationTest) TestToInsertIgnoreSQL() {
	t := dit.T()
	ds1 := From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	insertSQL, _, err := ds1.ToInsertIgnoreSQL(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, `INSERT INTO "items" ("address", "name") VALUES `+
		`('111 Test Addr', 'Test') `+
		`ON CONFLICT DO NOTHING`, insertSQL)
}
func (dit *datasetIntegrationTest) TestToUpdateSQLWithNoSources() {
	t := dit.T()
	ds1 := From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	_, _, err := ds1.From().ToUpdateSQL(item{Name: "Test", Address: "111 Test Addr"})
	assert.EqualError(t, err, "goqu: no source found when generating update sql")
}

func (dit *datasetIntegrationTest) TestToUpdateSQLNoReturning() {
	t := dit.T()
	ds1 := New("no-return", nil).From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	_, _, err := ds1.Returning("id").ToUpdateSQL(item{Name: "Test", Address: "111 Test Addr"})
	assert.EqualError(t, err, "goqu: adapter does not support RETURNING clause")
}

func (dit *datasetIntegrationTest) TestToUpdateSQLWithLimit() {
	t := dit.T()
	ds1 := New("limit-on-update", nil).From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	updateSQL, _, err := ds1.Limit(10).ToUpdateSQL(item{Name: "Test", Address: "111 Test Addr"})
	assert.Nil(t, err)
	assert.Equal(t, updateSQL, `UPDATE "items" SET "address"='111 Test Addr',"name"='Test' LIMIT 10`)
}

func (dit *datasetIntegrationTest) TestToUpdateSQLWithOrder() {
	t := dit.T()
	ds1 := New("order-on-update", nil).From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	updateSQL, _, err := ds1.Order(C("name").Desc()).ToUpdateSQL(item{Name: "Test", Address: "111 Test Addr"})
	assert.Nil(t, err)
	assert.Equal(t, `UPDATE "items" SET "address"='111 Test Addr',"name"='Test' ORDER BY "name" DESC`, updateSQL)
}

func (dit *datasetIntegrationTest) TestToUpdateSQLWithStructs() {
	t := dit.T()
	ds1 := From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	updateSQL, _, err := ds1.ToUpdateSQL(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, `UPDATE "items" SET "address"='111 Test Addr',"name"='Test'`, updateSQL)
}

func (dit *datasetIntegrationTest) TestToUpdateSQLWithMaps() {
	t := dit.T()
	ds1 := From("items")
	updateSQL, _, err := ds1.ToUpdateSQL(Record{"name": "Test", "address": "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, `UPDATE "items" SET "address"='111 Test Addr',"name"='Test'`, updateSQL)

}

func (dit *datasetIntegrationTest) TestToUpdateSQLWithByteSlice() {
	t := dit.T()
	ds1 := From("items")
	type item struct {
		Name string `db:"name"`
		Data []byte `db:"data"`
	}
	updateSQL, _, err := ds1.
		Returning(T("items").All()).
		ToUpdateSQL(item{Name: "Test", Data: []byte(`{"someJson":"data"}`)})
	assert.NoError(t, err)
	assert.Equal(t, `UPDATE "items" SET "name"='Test',"data"='{"someJson":"data"}' RETURNING "items".*`, updateSQL)
}

type valuerType []byte

func (j valuerType) Value() (driver.Value, error) {
	return []byte(fmt.Sprintf("%s World", string(j))), nil
}

func (dit *datasetIntegrationTest) TestToUpdateSQLWithCustomValuer() {
	t := dit.T()
	ds1 := From("items")
	type item struct {
		Name string     `db:"name"`
		Data valuerType `db:"data"`
	}
	updateSQL, _, err := ds1.
		Returning(T("items").All()).
		ToUpdateSQL(item{Name: "Test", Data: []byte(`Hello`)})
	assert.NoError(t, err)
	assert.Equal(t, `UPDATE "items" SET "name"='Test',"data"='Hello World' RETURNING "items".*`, updateSQL)
}

func (dit *datasetIntegrationTest) TestToUpdateSQLWithValuer() {
	t := dit.T()
	ds1 := From("items")
	type item struct {
		Name string         `db:"name"`
		Data sql.NullString `db:"data"`
	}

	updateSQL, _, err := ds1.
		Returning(T("items").All()).
		ToUpdateSQL(item{Name: "Test", Data: sql.NullString{String: "Hello World", Valid: true}})
	assert.NoError(t, err)
	assert.Equal(t, `UPDATE "items" SET "name"='Test',"data"='Hello World' RETURNING "items".*`, updateSQL)
}

func (dit *datasetIntegrationTest) TestToUpdateSQLWithValuerNull() {
	t := dit.T()
	ds1 := From("items")
	type item struct {
		Name string         `db:"name"`
		Data sql.NullString `db:"data"`
	}
	updateSQL, _, err := ds1.Returning(T("items").All()).ToUpdateSQL(item{Name: "Test"})
	assert.NoError(t, err)
	assert.Equal(t, `UPDATE "items" SET "name"='Test',"data"=NULL RETURNING "items".*`, updateSQL)
}

func (dit *datasetIntegrationTest) TestToUpdateSQLWithEmbeddedStruct() {
	t := dit.T()
	ds1 := From("items")
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

	updateSQL, args, err := ds1.ToUpdateSQL(item{
		Name:    "Test",
		Address: "111 Test Addr",
		Created: created,
		Phone: Phone{
			Home:    "123123",
			Primary: "456456",
			Created: created,
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{})
	assert.Equal(t, `UPDATE "items" SET `+
		`"primary_phone"='456456',`+
		`"home_phone"='123123',`+
		`"phone_created"='2015-01-01T00:00:00Z',`+
		`"name"='Test',`+
		`"created"='2015-01-01T00:00:00Z',`+
		`"nil_pointer"=NULL`, updateSQL)
}

func (dit *datasetIntegrationTest) TestToUpdateSQLWithEmbeddedStructPtr() {
	t := dit.T()
	ds1 := From("items")
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

	updateSQL, args, err := ds1.ToUpdateSQL(item{
		Name:    "Test",
		Address: "111 Test Addr",
		Created: created,
		Phone: &Phone{
			Home:    "123123",
			Primary: "456456",
			Created: created,
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{})
	assert.Equal(t, `UPDATE "items" SET `+
		`"primary_phone"='456456',`+
		`"home_phone"='123123',`+
		`"phone_created"='2015-01-01T00:00:00Z',`+
		`"name"='Test',`+
		`"created"='2015-01-01T00:00:00Z'`, updateSQL)
}

func (dit *datasetIntegrationTest) TestToUpdateSQLWithUnsupportedType() {
	t := dit.T()
	ds1 := From("items")
	_, _, err := ds1.ToUpdateSQL([]string{"HELLO"})
	assert.EqualError(t, err, "goqu: unsupported update interface type []string")
}

func (dit *datasetIntegrationTest) TestToUpdateSQLWithSkipupdateTag() {
	t := dit.T()
	ds1 := From("items")
	type item struct {
		Address string `db:"address" goqu:"skipupdate"`
		Name    string `db:"name"`
	}
	updateSQL, _, err := ds1.ToUpdateSQL(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, `UPDATE "items" SET "name"='Test'`, updateSQL)
}

func (dit *datasetIntegrationTest) TestToUpdateSQLWithWhere() {
	t := dit.T()
	ds1 := From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	updateSQL, _, err := ds1.
		Where(C("name").IsNull()).
		ToUpdateSQL(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, `UPDATE "items" SET "address"='111 Test Addr',"name"='Test' WHERE ("name" IS NULL)`, updateSQL)

	updateSQL, _, err = ds1.
		Where(C("name").IsNull()).
		ToUpdateSQL(Record{"name": "Test", "address": "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, `UPDATE "items" SET "address"='111 Test Addr',"name"='Test' WHERE ("name" IS NULL)`, updateSQL)
}

func (dit *datasetIntegrationTest) TestToUpdateSQLWithReturning() {
	t := dit.T()
	ds1 := From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	updateSQL, _, err := ds1.
		Returning(T("items").All()).
		ToUpdateSQL(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, `UPDATE "items" SET "address"='111 Test Addr',"name"='Test' RETURNING "items".*`, updateSQL)

	updateSQL, _, err = ds1.
		Where(C("name").IsNull()).
		Returning(L(`"items".*`)).
		ToUpdateSQL(Record{"name": "Test", "address": "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, `UPDATE "items" SET "address"='111 Test Addr',"name"='Test' WHERE ("name" IS NULL) RETURNING "items".*`, updateSQL)
}

func (dit *datasetIntegrationTest) TestPreparedToUpdateSQLWithStructs() {
	t := dit.T()
	ds1 := From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	updateSQL, args, err := ds1.
		Prepared(true).
		ToUpdateSQL(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test"})
	assert.Equal(t, `UPDATE "items" SET "address"=?,"name"=?`, updateSQL)
}

func (dit *datasetIntegrationTest) TestPreparedToUpdateSQLWithMaps() {
	t := dit.T()
	ds1 := From("items")
	updateSQL, args, err := ds1.
		Prepared(true).
		ToUpdateSQL(Record{"name": "Test", "address": "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test"})
	assert.Equal(t, `UPDATE "items" SET "address"=?,"name"=?`, updateSQL)

}

func (dit *datasetIntegrationTest) TestPreparedToUpdateSQLWithByteSlice() {
	t := dit.T()
	ds1 := From("items")
	type item struct {
		Name string `db:"name"`
		Data []byte `db:"data"`
	}
	updateSQL, args, err := ds1.
		Returning(T("items").All()).
		Prepared(true).
		ToUpdateSQL(item{Name: "Test", Data: []byte(`{"someJson":"data"}`)})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"Test", []byte(`{"someJson":"data"}`)})
	assert.Equal(t, `UPDATE "items" SET "name"=?,"data"=? RETURNING "items".*`, updateSQL)
}

func (dit *datasetIntegrationTest) TestPreparedToUpdateSQLWithCustomValuer() {
	t := dit.T()
	ds1 := From("items")
	type item struct {
		Name string     `db:"name"`
		Data valuerType `db:"data"`
	}
	updateSQL, args, err := ds1.
		Returning(T("items").All()).
		Prepared(true).
		ToUpdateSQL(item{Name: "Test", Data: []byte(`Hello`)})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"Test", []byte("Hello World")})
	assert.Equal(t, `UPDATE "items" SET "name"=?,"data"=? RETURNING "items".*`, updateSQL)
}

func (dit *datasetIntegrationTest) TestPreparedToUpdateSQLWithValuer() {
	t := dit.T()
	ds1 := From("items")
	type item struct {
		Name string         `db:"name"`
		Data sql.NullString `db:"data"`
	}
	updateSQL, args, err := ds1.
		Returning(T("items").All()).
		Prepared(true).
		ToUpdateSQL(item{Name: "Test", Data: sql.NullString{String: "Hello World", Valid: true}})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"Test", "Hello World"})
	assert.Equal(t, `UPDATE "items" SET "name"=?,"data"=? RETURNING "items".*`, updateSQL)
}

func (dit *datasetIntegrationTest) TestPreparedToUpdateSQLWithSkipupdateTag() {
	t := dit.T()
	ds1 := From("items")
	type item struct {
		Address string `db:"address" goqu:"skipupdate"`
		Name    string `db:"name"`
	}
	updateSQL, args, err := ds1.Prepared(true).ToUpdateSQL(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"Test"})
	assert.Equal(t, `UPDATE "items" SET "name"=?`, updateSQL)
}

func (dit *datasetIntegrationTest) TestPreparedToUpdateSQLWithEmbeddedStruct() {
	t := dit.T()
	ds1 := From("items")
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

	updateSQL, args, err := ds1.Prepared(true).ToUpdateSQL(item{
		Name:    "Test",
		Address: "111 Test Addr",
		Created: created,
		Phone: Phone{
			Home:    "123123",
			Primary: "456456",
			Created: created,
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"456456", "123123", created, "Test", created})
	assert.Equal(t, `UPDATE "items" `+
		`SET "primary_phone"=?,"home_phone"=?,"phone_created"=?,"name"=?,"created"=?,"nil_pointer"=NULL`, updateSQL)
}

func (dit *datasetIntegrationTest) TestPreparedToUpdateSQLWithEmbeddedStructPtr() {
	t := dit.T()
	ds1 := From("items")
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

	updateSQL, args, err := ds1.Prepared(true).ToUpdateSQL(item{
		Name:    "Test",
		Address: "111 Test Addr",
		Created: created,
		Phone: &Phone{
			Home:    "123123",
			Primary: "456456",
			Created: created,
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"456456", "123123", created, "Test", created})
	assert.Equal(t,
		`UPDATE "items" SET "primary_phone"=?,"home_phone"=?,"phone_created"=?,"name"=?,"created"=?`,
		updateSQL,
	)
}

func (dit *datasetIntegrationTest) TestPreparedToUpdateSQLWithWhere() {
	t := dit.T()
	ds1 := From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	updateSQL, args, err := ds1.
		Where(C("name").IsNull()).
		Prepared(true).
		ToUpdateSQL(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test"})
	assert.Equal(t, `UPDATE "items" SET "address"=?,"name"=? WHERE ("name" IS NULL)`, updateSQL)

	updateSQL, args, err = ds1.
		Where(C("name").IsNull()).
		Prepared(true).
		ToUpdateSQL(Record{"name": "Test", "address": "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test"})
	assert.Equal(t, `UPDATE "items" SET "address"=?,"name"=? WHERE ("name" IS NULL)`, updateSQL)
}

func (dit *datasetIntegrationTest) TestPreparedToUpdateSQLWithReturning() {
	t := dit.T()
	ds1 := From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	updateSQL, args, err := ds1.
		Returning(T("items").All()).
		Prepared(true).
		ToUpdateSQL(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test"})
	assert.Equal(t, `UPDATE "items" SET "address"=?,"name"=? RETURNING "items".*`, updateSQL)

	updateSQL, args, err = ds1.
		Where(C("name").IsNull()).
		Returning(L(`"items".*`)).
		Prepared(true).
		ToUpdateSQL(Record{"name": "Test", "address": "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test"})
	assert.Equal(t, `UPDATE "items" SET "address"=?,"name"=? WHERE ("name" IS NULL) RETURNING "items".*`, updateSQL)
}

func (dit *datasetIntegrationTest) TestSelect() {
	t := dit.T()
	ds1 := From("test")

	selectSQL, _, err := ds1.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test"`)

	selectSQL, _, err = ds1.Select().ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test"`)

	selectSQL, _, err = ds1.Select("id").ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT "id" FROM "test"`)

	selectSQL, _, err = ds1.Select("id", "name").ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT "id", "name" FROM "test"`)

	selectSQL, _, err = ds1.Select(L("COUNT(*)").As("count")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT COUNT(*) AS "count" FROM "test"`)

	selectSQL, _, err = ds1.Select(C("id").As("other_id"), L("COUNT(*)").As("count")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT "id" AS "other_id", COUNT(*) AS "count" FROM "test"`)

	selectSQL, _, err = ds1.From().Select(ds1.From("test_1").Select("id")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT (SELECT "id" FROM "test_1")`)

	selectSQL, _, err = ds1.From().Select(ds1.From("test_1").Select("id").As("test_id")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT (SELECT "id" FROM "test_1") AS "test_id"`)

	selectSQL, _, err = ds1.From().
		Select(
			DISTINCT("a").As("distinct"),
			COUNT("a").As("count"),
			L("CASE WHEN ? THEN ? ELSE ? END", MIN("a").Eq(10), true, false),
			L("CASE WHEN ? THEN ? ELSE ? END", AVG("a").Neq(10), true, false),
			L("CASE WHEN ? THEN ? ELSE ? END", FIRST("a").Gt(10), true, false),
			L("CASE WHEN ? THEN ? ELSE ? END", FIRST("a").Gte(10), true, false),
			L("CASE WHEN ? THEN ? ELSE ? END", LAST("a").Lt(10), true, false),
			L("CASE WHEN ? THEN ? ELSE ? END", LAST("a").Lte(10), true, false),
			SUM("a").As("sum"),
			COALESCE(C("a"), "a").As("colaseced"),
		).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT `+
		`DISTINCT("a") AS "distinct", `+
		`COUNT("a") AS "count", `+
		`CASE WHEN (MIN("a") = 10) THEN TRUE ELSE FALSE END, `+
		`CASE WHEN (AVG("a") != 10) THEN TRUE ELSE FALSE END, `+
		`CASE WHEN (FIRST("a") > 10) THEN TRUE ELSE FALSE END, `+
		`CASE WHEN (FIRST("a") >= 10) THEN TRUE ELSE FALSE END,`+
		` CASE WHEN (LAST("a") < 10) THEN TRUE ELSE FALSE END, `+
		`CASE WHEN (LAST("a") <= 10) THEN TRUE ELSE FALSE END, `+
		`SUM("a") AS "sum", `+
		`COALESCE("a", 'a') AS "colaseced"`)

	type MyStruct struct {
		Name         string
		Address      string `db:"address"`
		EmailAddress string `db:"email_address"`
		FakeCol      string `db:"-"`
	}
	selectSQL, _, err = ds1.Select(&MyStruct{}).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT "address", "email_address", "name" FROM "test"`)

	selectSQL, _, err = ds1.Select(MyStruct{}).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT "address", "email_address", "name" FROM "test"`)

	type myStruct2 struct {
		MyStruct
		Zipcode string `db:"zipcode"`
	}

	selectSQL, _, err = ds1.Select(&myStruct2{}).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT "address", "email_address", "name", "zipcode" FROM "test"`)

	selectSQL, _, err = ds1.Select(myStruct2{}).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT "address", "email_address", "name", "zipcode" FROM "test"`)

	var myStructs []MyStruct
	selectSQL, _, err = ds1.Select(&myStructs).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT "address", "email_address", "name" FROM "test"`)

	selectSQL, _, err = ds1.Select(myStructs).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT "address", "email_address", "name" FROM "test"`)
	// should not change original
	selectSQL, _, err = ds1.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test"`)
}

func (dit *datasetIntegrationTest) TestSelectDistinct() {
	t := dit.T()
	ds1 := From("test")

	selectSQL, _, err := ds1.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test"`)

	selectSQL, _, err = ds1.SelectDistinct("id").ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT DISTINCT "id" FROM "test"`)

	selectSQL, _, err = ds1.SelectDistinct("id", "name").ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT DISTINCT "id", "name" FROM "test"`)

	selectSQL, _, err = ds1.SelectDistinct(L("COUNT(*)").As("count")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT DISTINCT COUNT(*) AS "count" FROM "test"`)

	selectSQL, _, err = ds1.SelectDistinct(C("id").As("other_id"), L("COUNT(*)").As("count")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT DISTINCT "id" AS "other_id", COUNT(*) AS "count" FROM "test"`)

	type MyStruct struct {
		Name         string
		Address      string `db:"address"`
		EmailAddress string `db:"email_address"`
		FakeCol      string `db:"-"`
	}
	selectSQL, _, err = ds1.SelectDistinct(&MyStruct{}).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT DISTINCT "address", "email_address", "name" FROM "test"`)

	selectSQL, _, err = ds1.SelectDistinct(MyStruct{}).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT DISTINCT "address", "email_address", "name" FROM "test"`)

	type myStruct2 struct {
		MyStruct
		Zipcode string `db:"zipcode"`
	}

	selectSQL, _, err = ds1.SelectDistinct(&myStruct2{}).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT DISTINCT "address", "email_address", "name", "zipcode" FROM "test"`)

	selectSQL, _, err = ds1.SelectDistinct(myStruct2{}).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT DISTINCT "address", "email_address", "name", "zipcode" FROM "test"`)

	var myStructs []MyStruct
	selectSQL, _, err = ds1.SelectDistinct(&myStructs).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT DISTINCT "address", "email_address", "name" FROM "test"`)

	selectSQL, _, err = ds1.SelectDistinct(myStructs).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT DISTINCT "address", "email_address", "name" FROM "test"`)
	// should not change original
	selectSQL, _, err = ds1.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test"`)
	// should not change original
	selectSQL, _, err = ds1.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test"`)
}

func (dit *datasetIntegrationTest) TestClearSelect() {
	t := dit.T()
	ds1 := From("test")

	selectSQL, _, err := ds1.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test"`)

	b := ds1.Select("a").ClearSelect()
	selectSQL, _, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test"`)
}

func (dit *datasetIntegrationTest) TestSelectAppend() {
	t := dit.T()
	ds1 := From("test")

	selectSQL, _, err := ds1.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test"`)

	b := ds1.Select("a").SelectAppend("b").SelectAppend("c")
	selectSQL, _, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT "a", "b", "c" FROM "test"`)
}

func (dit *datasetIntegrationTest) TestFrom() {
	t := dit.T()
	ds1 := From("test")

	selectSQL, _, err := ds1.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test"`)

	ds2 := ds1.From("test2")
	selectSQL, _, err = ds2.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test2"`)

	ds2 = ds1.From("test2", "test3")
	selectSQL, _, err = ds2.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test2", "test3"`)

	ds2 = ds1.From(T("test2").As("test_2"), "test3")
	selectSQL, _, err = ds2.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test2" AS "test_2", "test3"`)

	ds2 = ds1.From(ds1.From("test2"), "test3")
	selectSQL, _, err = ds2.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM (SELECT * FROM "test2") AS "t1", "test3"`)

	ds2 = ds1.From(ds1.From("test2").As("test_2"), "test3")
	selectSQL, _, err = ds2.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM (SELECT * FROM "test2") AS "test_2", "test3"`)
	// should not change original
	selectSQL, _, err = ds1.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test"`)
}

func (dit *datasetIntegrationTest) TestEmptyWhere() {
	t := dit.T()
	ds1 := From("test")

	b := ds1.Where()
	selectSQL, _, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test"`)
}

func (dit *datasetIntegrationTest) TestWhere() {
	t := dit.T()
	ds1 := From("test")

	b := ds1.Where(
		C("a").Eq(true),
		C("a").Neq(true),
		C("a").Eq(false),
		C("a").Neq(false),
	)
	selectSQL, _, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" `+
		`WHERE (("a" IS TRUE) AND ("a" IS NOT TRUE) AND ("a" IS FALSE) AND ("a" IS NOT FALSE))`)

	b = ds1.Where(
		C("a").Eq("a"),
		C("b").Neq("b"),
		C("c").Gt("c"),
		C("d").Gte("d"),
		C("e").Lt("e"),
		C("f").Lte("f"),
	)
	selectSQL, _, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" `+
		`WHERE (("a" = 'a') AND ("b" != 'b') AND ("c" > 'c') AND ("d" >= 'd') AND ("e" < 'e') AND ("f" <= 'f'))`)

	b = ds1.Where(
		C("a").Eq(From("test2").Select("id")),
	)
	selectSQL, _, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" IN (SELECT "id" FROM "test2"))`)

	b = ds1.Where(Ex{
		"a": "a",
		"b": Op{"neq": "b"},
		"c": Op{"gt": "c"},
		"d": Op{"gte": "d"},
		"e": Op{"lt": "e"},
		"f": Op{"lte": "f"},
	})
	selectSQL, _, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" `+
		`WHERE (("a" = 'a') AND ("b" != 'b') AND ("c" > 'c') AND ("d" >= 'd') AND ("e" < 'e') AND ("f" <= 'f'))`)

	b = ds1.Where(Ex{
		"a": From("test2").Select("id"),
	})
	selectSQL, _, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" IN (SELECT "id" FROM "test2"))`)
}

func (dit *datasetIntegrationTest) TestWhereChain() {
	t := dit.T()
	ds1 := From("test").Where(
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
	selectSQL, _, err := a.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" `+
		`WHERE (("x" = 0) AND ("y" = 1) AND ("z" = 2) AND ("a" = 'A'))`)
	selectSQL, _, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" `+
		`WHERE (("x" = 0) AND ("y" = 1) AND ("z" = 2) AND ("b" = 'B'))`)
}

func (dit *datasetIntegrationTest) TestClearWhere() {
	t := dit.T()
	ds1 := From("test")

	b := ds1.Where(
		C("a").Eq(1),
	).ClearWhere()
	selectSQL, _, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test"`)
}

func (dit *datasetIntegrationTest) TestLimit() {
	t := dit.T()
	ds1 := From("test")

	b := ds1.Where(
		C("a").Gt(1),
	).Limit(10)
	selectSQL, _, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1) LIMIT 10`)

	b = ds1.Where(
		C("a").Gt(1),
	).Limit(0)
	selectSQL, _, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1)`)
}

func (dit *datasetIntegrationTest) TestLimitAll() {
	t := dit.T()
	ds1 := From("test")

	b := ds1.Where(
		C("a").Gt(1),
	).LimitAll()
	selectSQL, _, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1) LIMIT ALL`)

	b = ds1.Where(
		C("a").Gt(1),
	).Limit(0).LimitAll()
	selectSQL, _, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1) LIMIT ALL`)
}

func (dit *datasetIntegrationTest) TestClearLimit() {
	t := dit.T()
	ds1 := From("test")

	b := ds1.Where(
		C("a").Gt(1),
	).LimitAll().ClearLimit()
	selectSQL, _, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1)`)

	b = ds1.Where(
		C("a").Gt(1),
	).Limit(10).ClearLimit()
	selectSQL, _, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1)`)
}

func (dit *datasetIntegrationTest) TestOffset() {
	t := dit.T()
	ds1 := From("test")

	b := ds1.Where(
		C("a").Gt(1),
	).Offset(10)
	selectSQL, _, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1) OFFSET 10`)

	b = ds1.Where(
		C("a").Gt(1),
	).Offset(0)
	selectSQL, _, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1)`)
}

func (dit *datasetIntegrationTest) TestClearOffset() {
	t := dit.T()
	ds1 := From("test")

	b := ds1.Where(
		C("a").Gt(1),
	).Offset(10).ClearOffset()
	selectSQL, _, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1)`)
}

func (dit *datasetIntegrationTest) TestForUpdate() {
	t := dit.T()
	ds1 := From("test")

	b := ds1.Where(
		C("a").Gt(1),
	).ForUpdate(Wait)
	selectSQL, _, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1) FOR UPDATE `)

	b = ds1.Where(
		C("a").Gt(1),
	).ForUpdate(NoWait)
	selectSQL, _, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1) FOR UPDATE NOWAIT`)

	b = ds1.Where(
		C("a").Gt(1),
	).ForUpdate(SkipLocked)
	selectSQL, _, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1) FOR UPDATE SKIP LOCKED`)
}

func (dit *datasetIntegrationTest) TestForNoKeyUpdate() {
	t := dit.T()
	ds1 := From("test")

	b := ds1.Where(
		C("a").Gt(1),
	).ForNoKeyUpdate(Wait)
	selectSQL, _, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1) FOR NO KEY UPDATE `)

	b = ds1.Where(
		C("a").Gt(1),
	).ForNoKeyUpdate(NoWait)
	selectSQL, _, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1) FOR NO KEY UPDATE NOWAIT`)

	b = ds1.Where(
		C("a").Gt(1),
	).ForNoKeyUpdate(SkipLocked)
	selectSQL, _, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1) FOR NO KEY UPDATE SKIP LOCKED`)
}

func (dit *datasetIntegrationTest) TestForKeyShare() {
	t := dit.T()
	ds1 := From("test")

	b := ds1.Where(
		C("a").Gt(1),
	).ForKeyShare(Wait)
	selectSQL, _, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1) FOR KEY SHARE `)

	b = ds1.Where(
		C("a").Gt(1),
	).ForKeyShare(NoWait)
	selectSQL, _, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1) FOR KEY SHARE NOWAIT`)

	b = ds1.Where(
		C("a").Gt(1),
	).ForKeyShare(SkipLocked)
	selectSQL, _, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1) FOR KEY SHARE SKIP LOCKED`)
}

func (dit *datasetIntegrationTest) TestForShare() {
	t := dit.T()
	ds1 := From("test")

	b := ds1.Where(
		C("a").Gt(1),
	).ForShare(Wait)
	selectSQL, _, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1) FOR SHARE `)

	b = ds1.Where(
		C("a").Gt(1),
	).ForShare(NoWait)
	selectSQL, _, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1) FOR SHARE NOWAIT`)

	b = ds1.Where(
		C("a").Gt(1),
	).ForShare(SkipLocked)
	selectSQL, _, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1) FOR SHARE SKIP LOCKED`)
}

func (dit *datasetIntegrationTest) TestGroupBy() {
	t := dit.T()
	ds1 := From("test")

	b := ds1.Where(
		C("a").Gt(1),
	).GroupBy("created")
	selectSQL, _, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1) GROUP BY "created"`)

	b = ds1.Where(
		C("a").Gt(1),
	).GroupBy(L("created::DATE"))
	selectSQL, _, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1) GROUP BY created::DATE`)

	b = ds1.Where(
		C("a").Gt(1),
	).GroupBy("name", L("created::DATE"))
	selectSQL, _, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1) GROUP BY "name", created::DATE`)
}

func (dit *datasetIntegrationTest) TestHaving() {
	t := dit.T()
	ds1 := From("test")

	b := ds1.Having(Ex{
		"a": Op{"gt": 1},
	}).GroupBy("created")
	selectSQL, _, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" GROUP BY "created" HAVING ("a" > 1)`)

	b = ds1.Where(Ex{"b": true}).
		Having(Ex{"a": Op{"gt": 1}}).
		GroupBy("created")
	selectSQL, _, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("b" IS TRUE) GROUP BY "created" HAVING ("a" > 1)`)

	b = ds1.Having(Ex{"a": Op{"gt": 1}})
	selectSQL, _, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" HAVING ("a" > 1)`)

	b = ds1.Having(Ex{"a": Op{"gt": 1}}).Having(Ex{"b": 2})
	selectSQL, _, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" HAVING (("a" > 1) AND ("b" = 2))`)
}

func (dit *datasetIntegrationTest) TestOrder() {
	t := dit.T()

	ds1 := From("test")

	b := ds1.Order(C("a").Asc(), L(`("a" + "b" > 2)`).Asc())
	selectSQL, _, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" ORDER BY "a" ASC, ("a" + "b" > 2) ASC`)
}

func (dit *datasetIntegrationTest) TestOrderAppend() {
	t := dit.T()
	b := From("test").Order(C("a").Asc().NullsFirst()).OrderAppend(C("b").Desc().NullsLast())
	selectSQL, _, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" ORDER BY "a" ASC NULLS FIRST, "b" DESC NULLS LAST`)

	b = From("test").OrderAppend(C("a").Asc().NullsFirst()).OrderAppend(C("b").Desc().NullsLast())
	selectSQL, _, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" ORDER BY "a" ASC NULLS FIRST, "b" DESC NULLS LAST`)

}

func (dit *datasetIntegrationTest) TestClearOrder() {
	t := dit.T()
	b := From("test").Order(C("a").Asc().NullsFirst()).ClearOrder()
	selectSQL, _, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test"`)
}

func (dit *datasetIntegrationTest) TestJoin() {
	t := dit.T()
	ds1 := From("items")

	selectSQL, _, err := ds1.Join(T("players").As("p"), On(Ex{"p.id": I("items.playerId")})).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" `+
		`INNER JOIN "players" AS "p" ON ("p"."id" = "items"."playerId")`)

	selectSQL, _, err = ds1.Join(ds1.From("players").As("p"), On(Ex{"p.id": I("items.playerId")})).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" `+
		`INNER JOIN (SELECT * FROM "players") AS "p" ON ("p"."id" = "items"."playerId")`)

	selectSQL, _, err = ds1.Join(S("v1").Table("test"), On(Ex{"v1.test.id": I("items.playerId")})).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" `+
		`INNER JOIN "v1"."test" ON ("v1"."test"."id" = "items"."playerId")`)

	selectSQL, _, err = ds1.Join(T("test"), Using(C("name"), C("common_id"))).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" INNER JOIN "test" USING ("name", "common_id")`)

	selectSQL, _, err = ds1.Join(T("test"), Using("name", "common_id")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" INNER JOIN "test" USING ("name", "common_id")`)

}

func (dit *datasetIntegrationTest) TestLeftOuterJoin() {
	t := dit.T()
	ds1 := From("items")

	selectSQL, _, err := ds1.LeftOuterJoin(T("categories"), On(Ex{
		"categories.categoryId": I("items.id"),
	})).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" `+
		`LEFT OUTER JOIN "categories" ON ("categories"."categoryId" = "items"."id")`)

	selectSQL, _, err = ds1.
		LeftOuterJoin(
			T("categories"),
			On(
				I("categories.categoryId").Eq(I("items.id")),
				I("categories.categoryId").In(1, 2, 3)),
		).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `SELECT * FROM "items" `+
		`LEFT OUTER JOIN "categories" `+
		`ON (("categories"."categoryId" = "items"."id") AND ("categories"."categoryId" IN (1, 2, 3)))`, selectSQL)

}

func (dit *datasetIntegrationTest) TestFullOuterJoin() {
	t := dit.T()
	ds1 := From("items")
	selectSQL, _, err := ds1.
		FullOuterJoin(T("categories"), On(Ex{"categories.categoryId": I("items.id")})).
		Order(C("stamp").Asc()).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" `+
		`FULL OUTER JOIN "categories" ON ("categories"."categoryId" = "items"."id") ORDER BY "stamp" ASC`)

	selectSQL, _, err = ds1.FullOuterJoin(
		T("categories"),
		On(Ex{"categories.categoryId": I("items.id")}),
	).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" `+
		`FULL OUTER JOIN "categories" ON ("categories"."categoryId" = "items"."id")`)
}

func (dit *datasetIntegrationTest) TestInnerJoin() {
	t := dit.T()
	ds1 := From("items")
	selectSQL, _, err := ds1.
		InnerJoin(T("b"), On(Ex{"b.itemsId": I("items.id")})).
		LeftOuterJoin(T("c"), On(Ex{"c.b_id": I("b.id")})).
		ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" `+
		`INNER JOIN "b" ON ("b"."itemsId" = "items"."id") `+
		`LEFT OUTER JOIN "c" ON ("c"."b_id" = "b"."id")`)

	selectSQL, _, err = ds1.
		InnerJoin(T("b"), On(Ex{"b.itemsId": I("items.id")})).
		LeftOuterJoin(T("c"), On(Ex{"c.b_id": I("b.id")})).
		ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" `+
		`INNER JOIN "b" ON ("b"."itemsId" = "items"."id") `+
		`LEFT OUTER JOIN "c" ON ("c"."b_id" = "b"."id")`)

	selectSQL, _, err = ds1.InnerJoin(
		T("categories"),
		On(Ex{"categories.categoryId": I("items.id")}),
	).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" `+
		`INNER JOIN "categories" ON ("categories"."categoryId" = "items"."id")`)
}

func (dit *datasetIntegrationTest) TestRightOuterJoin() {
	t := dit.T()
	ds1 := From("items")
	selectSQL, _, err := ds1.RightOuterJoin(
		T("categories"),
		On(Ex{"categories.categoryId": I("items.id")}),
	).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" `+
		`RIGHT OUTER JOIN "categories" ON ("categories"."categoryId" = "items"."id")`)
}

func (dit *datasetIntegrationTest) TestLeftJoin() {
	t := dit.T()
	ds1 := From("items")
	selectSQL, _, err := ds1.LeftJoin(
		T("categories"),
		On(Ex{"categories.categoryId": I("items.id")}),
	).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" `+
		`LEFT JOIN "categories" ON ("categories"."categoryId" = "items"."id")`)
}

func (dit *datasetIntegrationTest) TestRightJoin() {
	t := dit.T()
	ds1 := From("items")
	selectSQL, _, err := ds1.RightJoin(
		T("categories"),
		On(Ex{"categories.categoryId": I("items.id")}),
	).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" `+
		`RIGHT JOIN "categories" ON ("categories"."categoryId" = "items"."id")`)
}

func (dit *datasetIntegrationTest) TestFullJoin() {
	t := dit.T()
	ds1 := From("items")
	selectSQL, _, err := ds1.FullJoin(
		T("categories"),
		On(Ex{"categories.categoryId": I("items.id")}),
	).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" `+
		`FULL JOIN "categories" ON ("categories"."categoryId" = "items"."id")`)
}

func (dit *datasetIntegrationTest) TestNaturalJoin() {
	t := dit.T()
	ds1 := From("items")
	selectSQL, _, err := ds1.NaturalJoin(T("categories")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" NATURAL JOIN "categories"`)
}

func (dit *datasetIntegrationTest) TestNaturalLeftJoin() {
	t := dit.T()
	ds1 := From("items")
	selectSQL, _, err := ds1.NaturalLeftJoin(T("categories")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" NATURAL LEFT JOIN "categories"`)

}

func (dit *datasetIntegrationTest) TestNaturalRightJoin() {
	t := dit.T()
	ds1 := From("items")
	selectSQL, _, err := ds1.NaturalRightJoin(T("categories")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" NATURAL RIGHT JOIN "categories"`)
}

func (dit *datasetIntegrationTest) TestNaturalFullJoin() {
	t := dit.T()
	ds1 := From("items")
	selectSQL, _, err := ds1.NaturalFullJoin(T("categories")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" NATURAL FULL JOIN "categories"`)
}

func (dit *datasetIntegrationTest) TestCrossJoin() {
	t := dit.T()
	selectSQL, _, err := From("items").CrossJoin(T("categories")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" CROSS JOIN "categories"`)
}

func (dit *datasetIntegrationTest) TestSQLFunctionExpressionsInHaving() {
	t := dit.T()
	ds1 := From("items")
	selectSQL, _, err := ds1.GroupBy("name").Having(SUM("amount").Gt(0)).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" GROUP BY "name" HAVING (SUM("amount") > 0)`)
}

func (dit *datasetIntegrationTest) TestUnion() {
	t := dit.T()
	a := From("invoice").Select("id", "amount").Where(C("amount").Gt(1000))
	b := From("invoice").Select("id", "amount").Where(C("amount").Lt(10))

	selectSQL, _, err := a.Union(b).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
		`UNION (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	selectSQL, _, err = a.Limit(1).Union(b).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM (`+
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) LIMIT 1) AS "t1" `+
		`UNION (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	selectSQL, _, err = a.Order(C("id").Asc()).Union(b).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM `+
		`(SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) ORDER BY "id" ASC) AS "t1" `+
		`UNION (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	selectSQL, _, err = a.Union(b.Limit(1)).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
		`UNION (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) LIMIT 1) AS "t1")`)

	selectSQL, _, err = a.Union(b.Order(C("id").Desc())).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
		`UNION (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) ORDER BY "id" DESC) AS "t1")`)

	selectSQL, _, err = a.Limit(1).Union(b.Order(C("id").Desc())).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM (`+
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) LIMIT 1) AS "t1" `+
		`UNION (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) ORDER BY "id" DESC) AS "t1")`)

	selectSQL, _, err = a.Union(b).Union(b.Where(C("id").Lt(50))).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
		`UNION (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10)) `+
		`UNION (SELECT "id", "amount" FROM "invoice" WHERE (("amount" < 10) AND ("id" < 50)))`)

}

func (dit *datasetIntegrationTest) TestUnionAll() {
	t := dit.T()
	a := From("invoice").Select("id", "amount").Where(C("amount").Gt(1000))
	b := From("invoice").Select("id", "amount").Where(C("amount").Lt(10))

	selectSQL, _, err := a.UnionAll(b).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
		`UNION ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	selectSQL, _, err = a.Limit(1).UnionAll(b).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM `+
		`(SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) LIMIT 1) AS "t1" `+
		`UNION ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	selectSQL, _, err = a.Order(C("id").Asc()).UnionAll(b).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM `+
		`(SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) ORDER BY "id" ASC) AS "t1" `+
		`UNION ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	selectSQL, _, err = a.UnionAll(b.Limit(1)).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
		`UNION ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) LIMIT 1) AS "t1")`)

	selectSQL, _, err = a.UnionAll(b.Order(C("id").Desc())).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
		`UNION ALL `+
		`(SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) ORDER BY "id" DESC) AS "t1")`)

	selectSQL, _, err = a.Limit(1).UnionAll(b.Order(C("id").Desc())).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM (`+
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) LIMIT 1) AS "t1" `+
		`UNION ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) ORDER BY "id" DESC) AS "t1"`+
		`)`)
}

func (dit *datasetIntegrationTest) TestIntersect() {
	t := dit.T()
	a := From("invoice").Select("id", "amount").Where(C("amount").Gt(1000))
	b := From("invoice").Select("id", "amount").Where(C("amount").Lt(10))

	selectSQL, _, err := a.Intersect(b).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
		`INTERSECT (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	selectSQL, _, err = a.Limit(1).Intersect(b).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM (`+
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) LIMIT 1) AS "t1" `+
		`INTERSECT (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10)`+
		`)`)

	selectSQL, _, err = a.Order(C("id").Asc()).Intersect(b).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM (`+
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) ORDER BY "id" ASC) AS "t1" `+
		`INTERSECT (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10)`+
		`)`)

	selectSQL, _, err = a.Intersect(b.Limit(1)).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
		`INTERSECT (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) LIMIT 1) AS "t1")`)

	selectSQL, _, err = a.Intersect(b.Order(C("id").Desc())).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
		`INTERSECT (`+
		`SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) ORDER BY "id" DESC) AS "t1"`+
		`)`)

	selectSQL, _, err = a.Limit(1).Intersect(b.Order(C("id").Desc())).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM (`+
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) LIMIT 1) AS "t1" `+
		`INTERSECT (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) ORDER BY "id" DESC) AS "t1"`+
		`)`)
}

func (dit *datasetIntegrationTest) TestIntersectAll() {
	t := dit.T()
	a := From("invoice").Select("id", "amount").Where(C("amount").Gt(1000))
	b := From("invoice").Select("id", "amount").Where(C("amount").Lt(10))

	selectSQL, _, err := a.IntersectAll(b).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
		`INTERSECT ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	selectSQL, _, err = a.Limit(1).IntersectAll(b).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM (`+
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) LIMIT 1) AS "t1" `+
		`INTERSECT ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10)`+
		`)`)

	selectSQL, _, err = a.Order(C("id").Asc()).IntersectAll(b).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM (`+
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) ORDER BY "id" ASC) AS "t1" `+
		`INTERSECT ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10)`+
		`)`)

	selectSQL, _, err = a.IntersectAll(b.Limit(1)).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
		`INTERSECT ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) LIMIT 1) AS "t1")`)

	selectSQL, _, err = a.IntersectAll(b.Order(C("id").Desc())).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
		`INTERSECT ALL `+
		`(SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) ORDER BY "id" DESC) AS "t1")`)

	selectSQL, _, err = a.Limit(1).IntersectAll(b.Order(C("id").Desc())).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM (`+
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) LIMIT 1) AS "t1" `+
		`INTERSECT ALL `+
		`(SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) ORDER BY "id" DESC) AS "t1")`)
}

// TO PREPARED

func (dit *datasetIntegrationTest) TestPreparedWhere() {
	t := dit.T()
	ds1 := From("test")

	b := ds1.Where(Ex{
		"a": true,
		"b": Op{"neq": true},
		"c": false,
		"d": Op{"neq": false},
		"e": nil,
	})
	selectSQL, args, err := b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{})
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE (`+
		`("a" IS TRUE) `+
		`AND ("b" IS NOT TRUE) `+
		`AND ("c" IS FALSE) `+
		`AND ("d" IS NOT FALSE) `+
		`AND ("e" IS NULL)`+
		`)`)

	b = ds1.Where(Ex{
		"a": "a",
		"b": Op{"neq": "b"},
		"c": Op{"gt": "c"},
		"d": Op{"gte": "d"},
		"e": Op{"lt": "e"},
		"f": Op{"lte": "f"},
		"g": Op{"is": nil},
		"h": Op{"isnot": nil},
	})
	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"a", "b", "c", "d", "e", "f"})
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE (`+
		`("a" = ?) `+
		`AND ("b" != ?) `+
		`AND ("c" > ?) `+
		`AND ("d" >= ?) `+
		`AND ("e" < ?) `+
		`AND ("f" <= ?) `+
		`AND ("g" IS NULL) `+
		`AND ("h" IS NOT NULL)`+
		`)`)
}

func (dit *datasetIntegrationTest) TestPreparedLimit() {
	t := dit.T()
	ds1 := From("test")

	b := ds1.Where(C("a").Gt(1)).Limit(10)
	selectSQL, args, err := b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1), int64(10)})
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > ?) LIMIT ?`)

	b = ds1.Where(C("a").Gt(1)).Limit(0)
	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1)})
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > ?)`)
}

func (dit *datasetIntegrationTest) TestPreparedLimitAll() {
	t := dit.T()
	ds1 := From("test")

	b := ds1.Where(C("a").Gt(1)).LimitAll()
	selectSQL, args, err := b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1)})
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > ?) LIMIT ALL`)

	b = ds1.Where(C("a").Gt(1)).Limit(0).LimitAll()
	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1)})
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > ?) LIMIT ALL`)
}

func (dit *datasetIntegrationTest) TestPreparedClearLimit() {
	t := dit.T()
	ds1 := From("test")

	b := ds1.Where(C("a").Gt(1)).LimitAll().ClearLimit()
	selectSQL, args, err := b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1)})
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > ?)`)

	b = ds1.Where(C("a").Gt(1)).Limit(10).ClearLimit()
	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1)})
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > ?)`)
}

func (dit *datasetIntegrationTest) TestPreparedOffset() {
	t := dit.T()
	ds1 := From("test")

	b := ds1.Where(C("a").Gt(1)).Offset(10)
	selectSQL, args, err := b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1), int64(10)})
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > ?) OFFSET ?`)

	b = ds1.Where(C("a").Gt(1)).Offset(0)
	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1)})
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > ?)`)
}

func (dit *datasetIntegrationTest) TestPreparedClearOffset() {
	t := dit.T()
	ds1 := From("test")

	b := ds1.Where(C("a").Gt(1)).Offset(10).ClearOffset()
	selectSQL, args, err := b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1)})
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > ?)`)
}

func (dit *datasetIntegrationTest) TestPreparedGroupBy() {
	t := dit.T()
	ds1 := From("test")

	b := ds1.Where(C("a").Gt(1)).GroupBy("created")
	selectSQL, args, err := b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1)})
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > ?) GROUP BY "created"`)

	b = ds1.Where(C("a").Gt(1)).GroupBy(L("created::DATE"))
	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1)})
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > ?) GROUP BY created::DATE`)

	b = ds1.Where(C("a").Gt(1)).GroupBy("name", L("created::DATE"))
	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1)})
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > ?) GROUP BY "name", created::DATE`)
}

func (dit *datasetIntegrationTest) TestPreparedHaving() {
	t := dit.T()
	ds1 := From("test")

	b := ds1.Having(C("a").Gt(1)).GroupBy("created")
	selectSQL, args, err := b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1)})
	assert.Equal(t, selectSQL, `SELECT * FROM "test" GROUP BY "created" HAVING ("a" > ?)`)

	b = ds1.
		Where(C("b").IsTrue()).
		Having(C("a").Gt(1)).
		GroupBy("created")
	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1)})
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("b" IS TRUE) GROUP BY "created" HAVING ("a" > ?)`)

	b = ds1.Having(C("a").Gt(1))
	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1)})
	assert.Equal(t, selectSQL, `SELECT * FROM "test" HAVING ("a" > ?)`)
}

func (dit *datasetIntegrationTest) TestPreparedJoin() {
	t := dit.T()
	ds1 := From("items")

	selectSQL, args, err := ds1.Join(
		T("players").As("p"),
		On(I("p.id").Eq(I("items.playerId"))),
	).Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{})
	assert.Equal(t, selectSQL, `SELECT * FROM "items" `+
		`INNER JOIN "players" AS "p" ON ("p"."id" = "items"."playerId")`)

	selectSQL, args, err = ds1.Join(
		ds1.From("players").As("p"),
		On(I("p.id").Eq(I("items.playerId"))),
	).Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{})
	assert.Equal(t, selectSQL, `SELECT * FROM "items" `+
		`INNER JOIN (SELECT * FROM "players") AS "p" ON ("p"."id" = "items"."playerId")`)

	selectSQL, args, err = ds1.Join(
		S("v1").Table("test"),
		On(I("v1.test.id").Eq(I("items.playerId"))),
	).Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{})
	assert.Equal(t, selectSQL, `SELECT * FROM "items" `+
		`INNER JOIN "v1"."test" ON ("v1"."test"."id" = "items"."playerId")`)

	selectSQL, args, err = ds1.Join(
		T("test"),
		Using(I("name"), I("common_id")),
	).Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{})
	assert.Equal(t, selectSQL, `SELECT * FROM "items" INNER JOIN "test" USING ("name", "common_id")`)

	selectSQL, args, err = ds1.Join(
		T("test"),
		Using("name", "common_id"),
	).Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{})
	assert.Equal(t, selectSQL, `SELECT * FROM "items" INNER JOIN "test" USING ("name", "common_id")`)

	selectSQL, args, err = ds1.Join(
		T("categories"),
		On(
			I("categories.categoryId").Eq(I("items.id")),
			I("categories.categoryId").In(1, 2, 3),
		),
	).Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1), int64(2), int64(3)})
	assert.Equal(t, selectSQL, `SELECT * FROM "items" `+
		`INNER JOIN "categories" ON (`+
		`("categories"."categoryId" = "items"."id") AND ("categories"."categoryId" IN (?, ?, ?))`+
		`)`)

}

func (dit *datasetIntegrationTest) TestPreparedFunctionExpressionsInHaving() {
	t := dit.T()
	ds1 := From("items")
	selectSQL, args, err := ds1.
		GroupBy("name").
		Having(SUM("amount").Gt(0)).
		Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(0)})
	assert.Equal(t, selectSQL, `SELECT * FROM "items" GROUP BY "name" HAVING (SUM("amount") > ?)`)
}

func (dit *datasetIntegrationTest) TestPreparedUnion() {
	t := dit.T()
	a := From("invoice").Select("id", "amount").Where(C("amount").Gt(1000))
	b := From("invoice").Select("id", "amount").Where(C("amount").Lt(10))

	selectSQL, args, err := a.Union(b).Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(10)})
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?)`+
		` UNION (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?))`)

	selectSQL, args, err = a.Limit(1).Union(b).Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(1), int64(10)})
	assert.Equal(t, selectSQL, `SELECT * FROM (`+
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) LIMIT ?) AS "t1" `+
		`UNION (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?)`+
		`)`)

	selectSQL, args, err = a.Union(b.Limit(1)).Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(10), int64(1)})
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) `+
		`UNION (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?) LIMIT ?) AS "t1")`)

	selectSQL, args, err = a.Union(b).Union(b.Where(C("id").Lt(50))).Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(10), int64(10), int64(50)})
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) `+
		`UNION (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?)) `+
		`UNION (SELECT "id", "amount" FROM "invoice" WHERE (("amount" < ?) AND ("id" < ?)))`)

}

func (dit *datasetIntegrationTest) TestPreparedUnionAll() {
	t := dit.T()
	a := From("invoice").Select("id", "amount").Where(C("amount").Gt(1000))
	b := From("invoice").Select("id", "amount").Where(C("amount").Lt(10))

	selectSQL, args, err := a.UnionAll(b).Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(10)})
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) `+
		`UNION ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?))`)

	selectSQL, args, err = a.Limit(1).UnionAll(b).Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(1), int64(10)})
	assert.Equal(t, selectSQL, `SELECT * FROM (`+
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) LIMIT ?) AS "t1" `+
		`UNION ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?)`+
		`)`)

	selectSQL, args, err = a.UnionAll(b.Limit(1)).Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(10), int64(1)})
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) `+
		`UNION ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?) LIMIT ?) AS "t1")`)

	selectSQL, args, err = a.UnionAll(b).UnionAll(b.Where(C("id").Lt(50))).Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(10), int64(10), int64(50)})
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) `+
		`UNION ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?)) `+
		`UNION ALL (SELECT "id", "amount" FROM "invoice" WHERE (("amount" < ?) AND ("id" < ?)))`)
}

func (dit *datasetIntegrationTest) TestPreparedIntersect() {
	t := dit.T()
	a := From("invoice").Select("id", "amount").Where(C("amount").Gt(1000))
	b := From("invoice").Select("id", "amount").Where(C("amount").Lt(10))

	selectSQL, args, err := a.Intersect(b).Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(10)})
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) `+
		`INTERSECT (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?))`)

	selectSQL, args, err = a.Limit(1).Intersect(b).Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(1), int64(10)})
	assert.Equal(t, selectSQL, `SELECT * FROM (`+
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) LIMIT ?) AS "t1" `+
		`INTERSECT (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?)`+
		`)`)

	selectSQL, args, err = a.Intersect(b.Limit(1)).Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(10), int64(1)})
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) `+
		`INTERSECT (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?) LIMIT ?) AS "t1")`)

}

func (dit *datasetIntegrationTest) TestPreparedIntersectAll() {
	t := dit.T()
	a := From("invoice").Select("id", "amount").Where(C("amount").Gt(1000))
	b := From("invoice").Select("id", "amount").Where(C("amount").Lt(10))

	selectSQL, args, err := a.IntersectAll(b).Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(10)})
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) `+
		`INTERSECT ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?))`)

	selectSQL, args, err = a.Limit(1).IntersectAll(b).Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(1), int64(10)})
	assert.Equal(t, selectSQL, `SELECT * FROM (`+
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) LIMIT ?) AS "t1" `+
		`INTERSECT ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?)`+
		`)`)

	selectSQL, args, err = a.IntersectAll(b.Limit(1)).Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(10), int64(1)})
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?)`+
		` INTERSECT ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?) LIMIT ?) AS "t1")`)

}

func TestDatasetIntegrationSuite(t *testing.T) {
	suite.Run(t, new(datasetIntegrationTest))
}
