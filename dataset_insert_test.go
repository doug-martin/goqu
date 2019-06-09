package goqu

import (
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"

	"database/sql"
	"time"
)

func (me *datasetTest) TestInsertNullTime() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		CreatedAt *time.Time `db:"created_at"`
	}
	sql, _, err := ds1.ToInsertSql(item{CreatedAt: nil})
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" ("created_at") VALUES (NULL)`)
}

func (me *datasetTest) TestInsertSqlNoReturning() {
	t := me.T()
	mDb, _, _ := sqlmock.New()
	ds1 := New("no-return", mDb).From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	_, _, err := ds1.Returning("id").ToInsertSql(item{Name: "Test", Address: "111 Test Addr"})
	assert.EqualError(t, err, "goqu: Adapter does not support RETURNING clause")

	_, _, err = ds1.Returning("id").ToInsertSql(From("test2"))
	assert.EqualError(t, err, "goqu: Adapter does not support RETURNING clause")
}

func (me *datasetTest) TestInsert_InvalidValue() {
	t := me.T()
	mDb, _, _ := sqlmock.New()
	ds1 := New("no-return", mDb).From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	_, _, err := ds1.ToInsertSql(true)
	assert.EqualError(t, err, "goqu: Unsupported insert must be map, goqu.Record, or struct type got: bool")
}

func (me *datasetTest) TestInsertSqlWithStructs() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Address string    `db:"address"`
		Name    string    `db:"name"`
		Created time.Time `db:"created"`
	}
	created, _ := time.Parse("2006-01-02", "2015-01-01")
	sql, _, err := ds1.ToInsertSql(item{Name: "Test", Address: "111 Test Addr", Created: created})
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name", "created") VALUES ('111 Test Addr', 'Test', '`+created.Format(time.RFC3339Nano)+`')`)

	sql, _, err = ds1.ToInsertSql(
		item{Address: "111 Test Addr", Name: "Test1", Created: created},
		item{Address: "211 Test Addr", Name: "Test2", Created: created},
		item{Address: "311 Test Addr", Name: "Test3", Created: created},
		item{Address: "411 Test Addr", Name: "Test4", Created: created},
	)
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name", "created") VALUES ('111 Test Addr', 'Test1', '`+created.Format(time.RFC3339Nano)+`'), ('211 Test Addr', 'Test2', '`+created.Format(time.RFC3339Nano)+`'), ('311 Test Addr', 'Test3', '`+created.Format(time.RFC3339Nano)+`'), ('411 Test Addr', 'Test4', '`+created.Format(time.RFC3339Nano)+`')`)
}

func (me *datasetTest) TestInsertSqlWithEmbeddedStruct() {
	t := me.T()
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
	sql, _, err := ds1.ToInsertSql(item{Name: "Test", Address: "111 Test Addr", Phone: Phone{Home: "123123", Primary: "456456"}})
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" ("primary_phone", "home_phone", "address", "name") VALUES ('456456', '123123', '111 Test Addr', 'Test')`)

	sql, _, err = ds1.ToInsertSql(
		item{Address: "111 Test Addr", Name: "Test1", Phone: Phone{Home: "123123", Primary: "456456"}},
		item{Address: "211 Test Addr", Name: "Test2", Phone: Phone{Home: "123123", Primary: "456456"}},
		item{Address: "311 Test Addr", Name: "Test3", Phone: Phone{Home: "123123", Primary: "456456"}},
		item{Address: "411 Test Addr", Name: "Test4", Phone: Phone{Home: "123123", Primary: "456456"}},
	)
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" ("primary_phone", "home_phone", "address", "name") VALUES ('456456', '123123', '111 Test Addr', 'Test1'), ('456456', '123123', '211 Test Addr', 'Test2'), ('456456', '123123', '311 Test Addr', 'Test3'), ('456456', '123123', '411 Test Addr', 'Test4')`)
}

func (me *datasetTest) TestInsertSqlWithEmbeddedStructPtr() {
	t := me.T()
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
	sql, _, err := ds1.ToInsertSql(item{Name: "Test", Address: "111 Test Addr", Valuer: sql.NullInt64{Int64: 10, Valid: true}, Phone: &Phone{Home: "123123", Primary: "456456"}})
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" ("primary_phone", "home_phone", "address", "name", "valuer") VALUES ('456456', '123123', '111 Test Addr', 'Test', 10)`)

	sql, _, err = ds1.ToInsertSql(
		item{Address: "111 Test Addr", Name: "Test1", Phone: &Phone{Home: "123123", Primary: "456456"}},
		item{Address: "211 Test Addr", Name: "Test2", Phone: &Phone{Home: "123123", Primary: "456456"}},
		item{Address: "311 Test Addr", Name: "Test3", Phone: &Phone{Home: "123123", Primary: "456456"}},
		item{Address: "411 Test Addr", Name: "Test4", Phone: &Phone{Home: "123123", Primary: "456456"}},
	)
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" ("primary_phone", "home_phone", "address", "name", "valuer") VALUES ('456456', '123123', '111 Test Addr', 'Test1', NULL), ('456456', '123123', '211 Test Addr', 'Test2', NULL), ('456456', '123123', '311 Test Addr', 'Test3', NULL), ('456456', '123123', '411 Test Addr', 'Test4', NULL)`)
}

func (me *datasetTest) TestInsertSqlWithValuer() {
	t := me.T()
	ds1 := From("items")

	type item struct {
		Address string        `db:"address"`
		Name    string        `db:"name"`
		Valuer  sql.NullInt64 `db:"valuer"`
	}
	sqlString, _, err := ds1.ToInsertSql(item{Name: "Test", Address: "111 Test Addr", Valuer: sql.NullInt64{Int64: 10, Valid: true}})
	assert.NoError(t, err)
	assert.Equal(t, sqlString, `INSERT INTO "items" ("address", "name", "valuer") VALUES ('111 Test Addr', 'Test', 10)`)

	sqlString, _, err = ds1.ToInsertSql(
		item{Address: "111 Test Addr", Name: "Test1", Valuer: sql.NullInt64{Int64: 10, Valid: true}},
		item{Address: "211 Test Addr", Name: "Test2", Valuer: sql.NullInt64{Int64: 10, Valid: true}},
		item{Address: "311 Test Addr", Name: "Test3", Valuer: sql.NullInt64{Int64: 10, Valid: true}},
		item{Address: "411 Test Addr", Name: "Test4", Valuer: sql.NullInt64{Int64: 10, Valid: true}},
	)
	assert.NoError(t, err)
	assert.Equal(t, sqlString, `INSERT INTO "items" ("address", "name", "valuer") VALUES ('111 Test Addr', 'Test1', 10), ('211 Test Addr', 'Test2', 10), ('311 Test Addr', 'Test3', 10), ('411 Test Addr', 'Test4', 10)`)
}

func (me *datasetTest) TestInsertSqlWithValuerNull() {
	t := me.T()
	ds1 := From("items")

	type item struct {
		Address string        `db:"address"`
		Name    string        `db:"name"`
		Valuer  sql.NullInt64 `db:"valuer"`
	}
	sqlString, _, err := ds1.ToInsertSql(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, sqlString, `INSERT INTO "items" ("address", "name", "valuer") VALUES ('111 Test Addr', 'Test', NULL)`)

	sqlString, _, err = ds1.ToInsertSql(
		item{Address: "111 Test Addr", Name: "Test1"},
		item{Address: "211 Test Addr", Name: "Test2"},
		item{Address: "311 Test Addr", Name: "Test3"},
		item{Address: "411 Test Addr", Name: "Test4"},
	)
	assert.NoError(t, err)
	assert.Equal(t, sqlString, `INSERT INTO "items" ("address", "name", "valuer") VALUES ('111 Test Addr', 'Test1', NULL), ('211 Test Addr', 'Test2', NULL), ('311 Test Addr', 'Test3', NULL), ('411 Test Addr', 'Test4', NULL)`)
}

func (me *datasetTest) TestInsertSqlWithMaps() {
	t := me.T()
	ds1 := From("items")

	sql, _, err := ds1.ToInsertSql(map[string]interface{}{"name": "Test", "address": "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test')`)

	sql, _, err = ds1.ToInsertSql(
		map[string]interface{}{"address": "111 Test Addr", "name": "Test1"},
		map[string]interface{}{"address": "211 Test Addr", "name": "Test2"},
		map[string]interface{}{"address": "311 Test Addr", "name": "Test3"},
		map[string]interface{}{"address": "411 Test Addr", "name": "Test4"},
	)
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('211 Test Addr', 'Test2'), ('311 Test Addr', 'Test3'), ('411 Test Addr', 'Test4')`)

	_, _, err = ds1.ToInsertSql(
		map[string]interface{}{"address": "111 Test Addr", "name": "Test1"},
		map[string]interface{}{"address": "211 Test Addr"},
		map[string]interface{}{"address": "311 Test Addr", "name": "Test3"},
		map[string]interface{}{"address": "411 Test Addr", "name": "Test4"},
	)
	assert.EqualError(t, err, "goqu: Rows with different value length expected 2 got 1")
}

func (me *datasetTest) TestInsertSqlWitSqlBuilder() {
	t := me.T()
	ds1 := From("items")

	sql, _, err := ds1.ToInsertSql(From("other_items"))
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" SELECT * FROM "other_items"`)
}

func (me *datasetTest) TestInsertReturning() {
	t := me.T()
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	ds1 := From("items").Returning("id")

	sql, _, err := ds1.Returning("id").ToInsertSql(From("other_items"))
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" SELECT * FROM "other_items" RETURNING "id"`)

	sql, _, err = ds1.ToInsertSql(map[string]interface{}{"name": "Test", "address": "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test') RETURNING "id"`)

	sql, _, err = ds1.ToInsertSql(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test') RETURNING "id"`)
}

func (me *datasetTest) TestInsertSqlWithNoFrom() {
	t := me.T()
	ds1 := From("test").From()
	_, _, err := ds1.ToInsertSql(map[string]interface{}{"address": "111 Test Addr", "name": "Test1"})
	assert.EqualError(t, err, "goqu: No source found when generating insert sql")
}

func (me *datasetTest) TestInsertSqlWithMapsWithDifferentLengths() {
	t := me.T()
	ds1 := From("items")
	_, _, err := ds1.ToInsertSql(
		map[string]interface{}{"address": "111 Test Addr", "name": "Test1"},
		map[string]interface{}{"address": "211 Test Addr"},
		map[string]interface{}{"address": "311 Test Addr", "name": "Test3"},
		map[string]interface{}{"address": "411 Test Addr", "name": "Test4"},
	)
	assert.EqualError(t, err, "goqu: Rows with different value length expected 2 got 1")
}

func (me *datasetTest) TestInsertSqlWitDifferentKeys() {
	t := me.T()
	ds1 := From("items")
	_, _, err := ds1.ToInsertSql(
		map[string]interface{}{"address": "111 Test Addr", "name": "test"},
		map[string]interface{}{"phoneNumber": 10, "address": "111 Test Addr"},
	)
	assert.EqualError(t, err, `goqu: Rows with different keys expected ["address","name"] got ["address","phoneNumber"]`)
}

func (me *datasetTest) TestInsertSqlDifferentTypes() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	type item2 struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	_, _, err := ds1.ToInsertSql(
		item{Address: "111 Test Addr", Name: "Test1"},
		item2{Address: "211 Test Addr", Name: "Test2"},
		item{Address: "311 Test Addr", Name: "Test3"},
		item2{Address: "411 Test Addr", Name: "Test4"},
	)
	assert.EqualError(t, err, "goqu: Rows must be all the same type expected goqu.item got goqu.item2")

	_, _, err = ds1.ToInsertSql(
		item{Address: "111 Test Addr", Name: "Test1"},
		map[string]interface{}{"address": "211 Test Addr", "name": "Test2"},
		item{Address: "311 Test Addr", Name: "Test3"},
		map[string]interface{}{"address": "411 Test Addr", "name": "Test4"},
	)
	assert.EqualError(t, err, "goqu: Rows must be all the same type expected goqu.item got map[string]interface {}")
}

func (me *datasetTest) TestInsertWithGoquPkTagSql() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Id      uint32 `db:"id" goqu:"pk,skipinsert"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, _, err := ds1.ToInsertSql(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test')`)

	sql, _, err = ds1.ToInsertSql(map[string]interface{}{"name": "Test", "address": "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test')`)

	sql, _, err = ds1.ToInsertSql(
		item{Name: "Test1", Address: "111 Test Addr"},
		item{Name: "Test2", Address: "211 Test Addr"},
		item{Name: "Test3", Address: "311 Test Addr"},
		item{Name: "Test4", Address: "411 Test Addr"},
	)
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('211 Test Addr', 'Test2'), ('311 Test Addr', 'Test3'), ('411 Test Addr', 'Test4')`)
}

func (me *datasetTest) TestInsertWithGoquSkipInsertTagSql() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Id      uint32 `db:"id" goqu:"skipinsert"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, _, err := ds1.ToInsertSql(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test')`)

	sql, _, err = ds1.ToInsertSql(
		item{Name: "Test1", Address: "111 Test Addr"},
		item{Name: "Test2", Address: "211 Test Addr"},
		item{Name: "Test3", Address: "311 Test Addr"},
		item{Name: "Test4", Address: "411 Test Addr"},
	)
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('211 Test Addr', 'Test2'), ('311 Test Addr', 'Test3'), ('411 Test Addr', 'Test4')`)
}

func (me *datasetTest) TestInsertDefaultValues() {
	t := me.T()
	ds1 := From("items")

	sql, _, err := ds1.ToInsertSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" DEFAULT VALUES`)

	sql, _, err = ds1.ToInsertSql(map[string]interface{}{"name": Default(), "address": Default()})
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES (DEFAULT, DEFAULT)`)

}

func (me *datasetTest) TestPreparedInsertSqlWithStructs() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, args, err := ds1.Prepared(true).ToInsertSql(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test"})
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES (?, ?)`)

	sql, args, err = ds1.Prepared(true).ToInsertSql(
		item{Address: "111 Test Addr", Name: "Test1"},
		item{Address: "211 Test Addr", Name: "Test2"},
		item{Address: "311 Test Addr", Name: "Test3"},
		item{Address: "411 Test Addr", Name: "Test4"},
	)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test1", "211 Test Addr", "Test2", "311 Test Addr", "Test3", "411 Test Addr", "Test4"})
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES (?, ?), (?, ?), (?, ?), (?, ?)`)
}

func (me *datasetTest) TestPreparedInsertSqlWithMaps() {
	t := me.T()
	ds1 := From("items")

	sql, args, err := ds1.Prepared(true).ToInsertSql(map[string]interface{}{"name": "Test", "address": "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test"})
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES (?, ?)`)

	sql, args, err = ds1.Prepared(true).ToInsertSql(
		map[string]interface{}{"address": "111 Test Addr", "name": "Test1"},
		map[string]interface{}{"address": "211 Test Addr", "name": "Test2"},
		map[string]interface{}{"address": "311 Test Addr", "name": "Test3"},
		map[string]interface{}{"address": "411 Test Addr", "name": "Test4"},
	)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test1", "211 Test Addr", "Test2", "311 Test Addr", "Test3", "411 Test Addr", "Test4"})
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES (?, ?), (?, ?), (?, ?), (?, ?)`)
}

func (me *datasetTest) TestPreparedInsertSqlWitSqlBuilder() {
	t := me.T()
	ds1 := From("items")

	sql, args, err := ds1.Prepared(true).ToInsertSql(From("other_items").Where(I("b").Gt(10)))
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(10)})
	assert.Equal(t, sql, `INSERT INTO "items" SELECT * FROM "other_items" WHERE ("b" > ?)`)
}

func (me *datasetTest) TestPreparedInsertReturning() {
	t := me.T()
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	ds1 := From("items").Returning("id")

	sql, args, err := ds1.Returning("id").Prepared(true).ToInsertSql(From("other_items").Where(I("b").Gt(10)))
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(10)})
	assert.Equal(t, sql, `INSERT INTO "items" SELECT * FROM "other_items" WHERE ("b" > ?) RETURNING "id"`)

	sql, args, err = ds1.Prepared(true).ToInsertSql(map[string]interface{}{"name": "Test", "address": "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test"})
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES (?, ?) RETURNING "id"`)

	sql, args, err = ds1.Prepared(true).ToInsertSql(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test"})
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES (?, ?) RETURNING "id"`)
}

func (me *datasetTest) TestPreparedInsertWithGoquPkTagSql() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Id      uint32 `db:"id" goqu:"pk,skipinsert"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, args, err := ds1.Prepared(true).ToInsertSql(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test"})
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES (?, ?)`)

	sql, args, err = ds1.Prepared(true).ToInsertSql(map[string]interface{}{"name": "Test", "address": "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test"})
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES (?, ?)`)

	sql, args, err = ds1.Prepared(true).ToInsertSql(
		item{Name: "Test1", Address: "111 Test Addr"},
		item{Name: "Test2", Address: "211 Test Addr"},
		item{Name: "Test3", Address: "311 Test Addr"},
		item{Name: "Test4", Address: "411 Test Addr"},
	)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test1", "211 Test Addr", "Test2", "311 Test Addr", "Test3", "411 Test Addr", "Test4"})
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES (?, ?), (?, ?), (?, ?), (?, ?)`)
}

func (me *datasetTest) TestPreparedInsertWithGoquSkipInsertTagSql() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Id      uint32 `db:"id" goqu:"skipinsert"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, args, err := ds1.Prepared(true).ToInsertSql(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test"})
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES (?, ?)`)

	sql, args, err = ds1.Prepared(true).ToInsertSql(
		item{Name: "Test1", Address: "111 Test Addr"},
		item{Name: "Test2", Address: "211 Test Addr"},
		item{Name: "Test3", Address: "311 Test Addr"},
		item{Name: "Test4", Address: "411 Test Addr"},
	)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test1", "211 Test Addr", "Test2", "311 Test Addr", "Test3", "411 Test Addr", "Test4"})
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES (?, ?), (?, ?), (?, ?), (?, ?)`)
}

func (me *datasetTest) TestPreparedInsertDefaultValues() {
	t := me.T()
	ds1 := From("items")

	sql, args, err := ds1.Prepared(true).ToInsertSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{})
	assert.Equal(t, sql, `INSERT INTO "items" DEFAULT VALUES`)

	sql, args, err = ds1.Prepared(true).ToInsertSql(map[string]interface{}{"name": Default(), "address": Default()})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{})
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES (DEFAULT, DEFAULT)`)

}

func (me *datasetTest) TestPreparedInsertSqlWithEmbeddedStruct() {
	t := me.T()
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
	sql, args, err := ds1.Prepared(true).ToInsertSql(item{Name: "Test", Address: "111 Test Addr", Phone: Phone{Home: "123123", Primary: "456456"}})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"456456", "123123", "111 Test Addr", "Test"})
	assert.Equal(t, sql, `INSERT INTO "items" ("primary_phone", "home_phone", "address", "name") VALUES (?, ?, ?, ?)`)

	sql, args, err = ds1.Prepared(true).ToInsertSql(
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
	assert.Equal(t, sql, `INSERT INTO "items" ("primary_phone", "home_phone", "address", "name") VALUES (?, ?, ?, ?), (?, ?, ?, ?), (?, ?, ?, ?), (?, ?, ?, ?)`)
}

func (me *datasetTest) TestPreparedInsertSqlWithEmbeddedStructPtr() {
	t := me.T()
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
	sql, args, err := ds1.Prepared(true).ToInsertSql(item{Name: "Test", Address: "111 Test Addr", Phone: &Phone{Home: "123123", Primary: "456456"}})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"456456", "123123", "111 Test Addr", "Test"})
	assert.Equal(t, sql, `INSERT INTO "items" ("primary_phone", "home_phone", "address", "name") VALUES (?, ?, ?, ?)`)

	sql, args, err = ds1.Prepared(true).ToInsertSql(
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
	assert.Equal(t, sql, `INSERT INTO "items" ("primary_phone", "home_phone", "address", "name") VALUES (?, ?, ?, ?), (?, ?, ?, ?), (?, ?, ?, ?), (?, ?, ?, ?)`)
}

func (me *datasetTest) TestPreparedInsertSqlWithValuer() {
	t := me.T()
	ds1 := From("items")

	type item struct {
		Address string        `db:"address"`
		Name    string        `db:"name"`
		Valuer  sql.NullInt64 `db:"valuer"`
	}
	sqlString, args, err := ds1.Prepared(true).ToInsertSql(item{Name: "Test", Address: "111 Test Addr", Valuer: sql.NullInt64{Int64: 10, Valid: true}})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{
		"111 Test Addr", "Test", int64(10),
	})
	assert.Equal(t, sqlString, `INSERT INTO "items" ("address", "name", "valuer") VALUES (?, ?, ?)`)

	sqlString, args, err = ds1.Prepared(true).ToInsertSql(
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
	assert.Equal(t, sqlString, `INSERT INTO "items" ("address", "name", "valuer") VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?)`)
}

func (me *datasetTest) TestPreparedInsertSqlWithValuerNull() {
	t := me.T()
	ds1 := From("items")

	type item struct {
		Address string        `db:"address"`
		Name    string        `db:"name"`
		Valuer  sql.NullInt64 `db:"valuer"`
	}
	sqlString, args, err := ds1.Prepared(true).ToInsertSql(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{
		"111 Test Addr", "Test",
	})
	assert.Equal(t, sqlString, `INSERT INTO "items" ("address", "name", "valuer") VALUES (?, ?, NULL)`)

	sqlString, args, err = ds1.Prepared(true).ToInsertSql(
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
	assert.Equal(t, sqlString, `INSERT INTO "items" ("address", "name", "valuer") VALUES (?, ?, NULL), (?, ?, NULL), (?, ?, NULL), (?, ?, NULL)`)
}

func (me *datasetTest) TestInsertConflictSql__OnConflictIsNil() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, _, err := ds1.ToInsertConflictSql(nil, item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test')`, sql)
}

func (me *datasetTest) TestInsertConflictSql__OnConflictDoUpdate() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	i := item{Name: "Test", Address: "111 Test Addr"}
	sql, _, err := ds1.ToInsertConflictSql(DoUpdate("name", Record{"address": L("excluded.address")}), i)
	assert.NoError(t, err)
	assert.Equal(t, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test') ON CONFLICT (name) DO UPDATE SET "address"=excluded.address`, sql)
}

func (me *datasetTest) TestInsertConflictSql__OnConflictDoUpdateWhere() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	i := item{Name: "Test", Address: "111 Test Addr"}

	sql, _, err := ds1.ToInsertConflictSql(DoUpdate("name", Record{"address": L("excluded.address")}).Where(I("name").Eq("Test")), i)
	assert.NoError(t, err)
	assert.Equal(t, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test') ON CONFLICT (name) DO UPDATE SET "address"=excluded.address WHERE ("name" = 'Test')`, sql)
}

func (me *datasetTest) TestInsertConflictSqlWithDataset__OnConflictDoUpdateWhere() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}

	ds2 := From("ds2")

	sql, _, err := ds1.ToInsertConflictSql(DoUpdate("name", Record{"address": L("excluded.address")}).Where(I("name").Eq("Test")), ds2)
	assert.NoError(t, err)
	assert.Equal(t, `INSERT INTO "items" SELECT * FROM "ds2" ON CONFLICT (name) DO UPDATE SET "address"=excluded.address WHERE ("name" = 'Test')`, sql)
}

func (me *datasetTest) TestInsertIgnoreSql() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, _, err := ds1.ToInsertIgnoreSql(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test') ON CONFLICT DO NOTHING`, sql)
}

func (me *datasetTest) TestInsertConflict__ImplementsConflictExpressionInterface() {
	t := me.T()
	assert.Implements(t, (*ConflictExpression)(nil), DoNothing())
	assert.Implements(t, (*ConflictExpression)(nil), DoUpdate("", nil))
}
