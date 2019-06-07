package goqu

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func (me *datasetTest) TestUpdateSqlWithNoSources() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	_, _, err := ds1.From().ToUpdateSql(item{Name: "Test", Address: "111 Test Addr"})
	assert.EqualError(t, err, "goqu: No source found when generating update sql")
}

func (me *datasetTest) TestUpdateSqlNoReturning() {
	t := me.T()
	mDb, _, _ := sqlmock.New()
	ds1 := New("no-return", mDb).From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	_, _, err := ds1.Returning("id").ToUpdateSql(item{Name: "Test", Address: "111 Test Addr"})
	assert.EqualError(t, err, "goqu: Adapter does not support RETURNING clause")
}

func (me *datasetTest) TestUpdateSqlWithLimit() {
	t := me.T()
	mDb, _, _ := sqlmock.New()
	ds1 := New("limit", mDb).From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, _, err := ds1.Limit(10).ToUpdateSql(item{Name: "Test", Address: "111 Test Addr"})
	assert.Nil(t, err)
	assert.Equal(t, sql, `UPDATE "items" SET "address"='111 Test Addr',"name"='Test' LIMIT 10`)
}

func (me *datasetTest) TestUpdateSqlWithOrder() {
	t := me.T()
	mDb, _, _ := sqlmock.New()
	ds1 := New("order", mDb).From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, _, err := ds1.Order(I("name").Desc()).ToUpdateSql(item{Name: "Test", Address: "111 Test Addr"})
	assert.Nil(t, err)
	assert.Equal(t, sql, `UPDATE "items" SET "address"='111 Test Addr',"name"='Test' ORDER BY "name" DESC`)
}

func (me *datasetTest) TestUpdateSqlWithStructs() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, _, err := ds1.ToUpdateSql(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, sql, `UPDATE "items" SET "address"='111 Test Addr',"name"='Test'`)
}

func (me *datasetTest) TestUpdateSqlWithMaps() {
	t := me.T()
	ds1 := From("items")
	sql, _, err := ds1.ToUpdateSql(Record{"name": "Test", "address": "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, sql, `UPDATE "items" SET "address"='111 Test Addr',"name"='Test'`)

}

func (me *datasetTest) TestUpdateSqlWithByteSlice() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Name string `db:"name"`
		Data []byte `db:"data"`
	}
	sql, _, err := ds1.Returning(I("items").All()).ToUpdateSql(item{Name: "Test", Data: []byte(`{"someJson":"data"}`)})
	assert.NoError(t, err)
	assert.Equal(t, sql, `UPDATE "items" SET "name"='Test',"data"='{"someJson":"data"}' RETURNING "items".*`)
}

type valuerType []byte

func (j valuerType) Value() (driver.Value, error) {
	return []byte(fmt.Sprintf("%s World", string(j))), nil
}

func (me *datasetTest) TestUpdateSqlWithCustomValuer() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Name string     `db:"name"`
		Data valuerType `db:"data"`
	}
	sql, _, err := ds1.Returning(I("items").All()).ToUpdateSql(item{Name: "Test", Data: []byte(`Hello`)})
	assert.NoError(t, err)
	assert.Equal(t, sql, `UPDATE "items" SET "name"='Test',"data"='Hello World' RETURNING "items".*`)
}

func (me *datasetTest) TestUpdateSqlWithValuer() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Name string         `db:"name"`
		Data sql.NullString `db:"data"`
	}

	sql, _, err := ds1.Returning(I("items").All()).ToUpdateSql(item{Name: "Test", Data: sql.NullString{String: "Hello World", Valid: true}})
	assert.NoError(t, err)
	assert.Equal(t, sql, `UPDATE "items" SET "name"='Test',"data"='Hello World' RETURNING "items".*`)
}

func (me *datasetTest) TestUpdateSqlWithValuerNull() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Name string         `db:"name"`
		Data sql.NullString `db:"data"`
	}
	sql, _, err := ds1.Returning(I("items").All()).ToUpdateSql(item{Name: "Test"})
	assert.NoError(t, err)
	assert.Equal(t, sql, `UPDATE "items" SET "name"='Test',"data"=NULL RETURNING "items".*`)
}

func (me *datasetTest) TestUpdateSqlWithEmbeddedStruct() {
	t := me.T()
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

	sql, args, err := ds1.ToUpdateSql(item{Name: "Test", Address: "111 Test Addr", Created: created, Phone: Phone{
		Home:    "123123",
		Primary: "456456",
		Created: created,
	}})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{})
	assert.Equal(t, sql, `UPDATE "items" SET "primary_phone"='456456',"home_phone"='123123',"phone_created"='2015-01-01T00:00:00Z',"name"='Test',"created"='2015-01-01T00:00:00Z',"nil_pointer"=NULL`)
}

func (me *datasetTest) TestUpdateSqlWithEmbeddedStructPtr() {
	t := me.T()
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

	sql, args, err := ds1.ToUpdateSql(item{Name: "Test", Address: "111 Test Addr", Created: created, Phone: &Phone{
		Home:    "123123",
		Primary: "456456",
		Created: created,
	}})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{})
	assert.Equal(t, sql, `UPDATE "items" SET "primary_phone"='456456',"home_phone"='123123',"phone_created"='2015-01-01T00:00:00Z',"name"='Test',"created"='2015-01-01T00:00:00Z'`)
}

func (me *datasetTest) TestUpdateSqlWithUnsupportedType() {
	t := me.T()
	ds1 := From("items")
	_, _, err := ds1.ToUpdateSql([]string{"HELLO"})
	assert.EqualError(t, err, "goqu: Unsupported update interface type []string")
}

func (me *datasetTest) TestUpdateSqlWithSkipupdateTag() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Address string `db:"address" goqu:"skipupdate"`
		Name    string `db:"name"`
	}
	sql, _, err := ds1.ToUpdateSql(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, sql, `UPDATE "items" SET "name"='Test'`)
}

func (me *datasetTest) TestUpdateSqlWithWhere() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, _, err := ds1.Where(I("name").IsNull()).ToUpdateSql(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, sql, `UPDATE "items" SET "address"='111 Test Addr',"name"='Test' WHERE ("name" IS NULL)`)

	sql, _, err = ds1.Where(I("name").IsNull()).ToUpdateSql(Record{"name": "Test", "address": "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, sql, `UPDATE "items" SET "address"='111 Test Addr',"name"='Test' WHERE ("name" IS NULL)`)
}

func (me *datasetTest) TestUpdateSqlWithReturning() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, _, err := ds1.Returning(I("items").All()).ToUpdateSql(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, sql, `UPDATE "items" SET "address"='111 Test Addr',"name"='Test' RETURNING "items".*`)

	sql, _, err = ds1.Where(I("name").IsNull()).Returning(Literal(`"items".*`)).ToUpdateSql(Record{"name": "Test", "address": "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, sql, `UPDATE "items" SET "address"='111 Test Addr',"name"='Test' WHERE ("name" IS NULL) RETURNING "items".*`)
}

func (me *datasetTest) TestPreparedUpdateSqlWithStructs() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, args, err := ds1.Prepared(true).ToUpdateSql(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test"})
	assert.Equal(t, sql, `UPDATE "items" SET "address"=?,"name"=?`)
}

func (me *datasetTest) TestPreparedUpdateSqlWithMaps() {
	t := me.T()
	ds1 := From("items")
	sql, args, err := ds1.Prepared(true).ToUpdateSql(Record{"name": "Test", "address": "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test"})
	assert.Equal(t, sql, `UPDATE "items" SET "address"=?,"name"=?`)

}

func (me *datasetTest) TestPreparedUpdateSqlWithByteSlice() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Name string `db:"name"`
		Data []byte `db:"data"`
	}
	sql, args, err := ds1.Returning(I("items").All()).Prepared(true).ToUpdateSql(item{Name: "Test", Data: []byte(`{"someJson":"data"}`)})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"Test", []byte(`{"someJson":"data"}`)})
	assert.Equal(t, sql, `UPDATE "items" SET "name"=?,"data"=? RETURNING "items".*`)
}

func (me *datasetTest) TestPreparedUpdateSqlWithCustomValuer() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Name string     `db:"name"`
		Data valuerType `db:"data"`
	}
	sql, args, err := ds1.Returning(I("items").All()).Prepared(true).ToUpdateSql(item{Name: "Test", Data: []byte(`Hello`)})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"Test", []byte("Hello World")})
	assert.Equal(t, sql, `UPDATE "items" SET "name"=?,"data"=? RETURNING "items".*`)
}

func (me *datasetTest) TestPreparedUpdateSqlWithValuer() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Name string         `db:"name"`
		Data sql.NullString `db:"data"`
	}
	sql, args, err := ds1.Returning(I("items").All()).Prepared(true).ToUpdateSql(item{Name: "Test", Data: sql.NullString{String: "Hello World", Valid: true}})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"Test", "Hello World"})
	assert.Equal(t, sql, `UPDATE "items" SET "name"=?,"data"=? RETURNING "items".*`)
}

func (me *datasetTest) TestPreparedUpdateSqlWithSkipupdateTag() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Address string `db:"address" goqu:"skipupdate"`
		Name    string `db:"name"`
	}
	sql, args, err := ds1.Prepared(true).ToUpdateSql(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"Test"})
	assert.Equal(t, sql, `UPDATE "items" SET "name"=?`)
}

func (me *datasetTest) TestPreparedUpdateSqlWithEmbeddedStruct() {
	t := me.T()
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

	sql, args, err := ds1.Prepared(true).ToUpdateSql(item{Name: "Test", Address: "111 Test Addr", Created: created, Phone: Phone{
		Home:    "123123",
		Primary: "456456",
		Created: created,
	}})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"456456", "123123", created, "Test", created})
	assert.Equal(t, sql, `UPDATE "items" SET "primary_phone"=?,"home_phone"=?,"phone_created"=?,"name"=?,"created"=?,"nil_pointer"=NULL`)
}

func (me *datasetTest) TestPreparedUpdateSqlWithEmbeddedStructPtr() {
	t := me.T()
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

	sql, args, err := ds1.Prepared(true).ToUpdateSql(item{Name: "Test", Address: "111 Test Addr", Created: created, Phone: &Phone{
		Home:    "123123",
		Primary: "456456",
		Created: created,
	}})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"456456", "123123", created, "Test", created})
	assert.Equal(t, sql, `UPDATE "items" SET "primary_phone"=?,"home_phone"=?,"phone_created"=?,"name"=?,"created"=?`)
}

func (me *datasetTest) TestPreparedUpdateSqlWithWhere() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, args, err := ds1.Where(I("name").IsNull()).Prepared(true).ToUpdateSql(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test"})
	assert.Equal(t, sql, `UPDATE "items" SET "address"=?,"name"=? WHERE ("name" IS NULL)`)

	sql, args, err = ds1.Where(I("name").IsNull()).Prepared(true).ToUpdateSql(Record{"name": "Test", "address": "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test"})
	assert.Equal(t, sql, `UPDATE "items" SET "address"=?,"name"=? WHERE ("name" IS NULL)`)
}

func (me *datasetTest) TestPreparedUpdateSqlWithReturning() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, args, err := ds1.Returning(I("items").All()).Prepared(true).ToUpdateSql(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test"})
	assert.Equal(t, sql, `UPDATE "items" SET "address"=?,"name"=? RETURNING "items".*`)

	sql, args, err = ds1.Where(I("name").IsNull()).Returning(Literal(`"items".*`)).Prepared(true).ToUpdateSql(Record{"name": "Test", "address": "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test"})
	assert.Equal(t, sql, `UPDATE "items" SET "address"=?,"name"=? WHERE ("name" IS NULL) RETURNING "items".*`)
}
