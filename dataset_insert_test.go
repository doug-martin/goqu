package gql

import "github.com/stretchr/testify/assert"

func (me *datasetTest) TestInsertSqlWithStructs() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, err := ds1.InsertSql(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test')`)

	sql, err = ds1.InsertSql(
		item{Address: "111 Test Addr", Name: "Test1"},
		item{Address: "211 Test Addr", Name: "Test2"},
		item{Address: "311 Test Addr", Name: "Test3"},
		item{Address: "411 Test Addr", Name: "Test4"},
	)
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('211 Test Addr', 'Test2'), ('311 Test Addr', 'Test3'), ('411 Test Addr', 'Test4')`)
}

func (me *datasetTest) TestInsertSqlWithMaps() {
	t := me.T()
	ds1 := From("items")

	sql, err := ds1.InsertSql(map[string]interface{}{"name": "Test", "address": "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test')`)

	sql, err = ds1.InsertSql(
		map[string]interface{}{"address": "111 Test Addr", "name": "Test1"},
		map[string]interface{}{"address": "211 Test Addr", "name": "Test2"},
		map[string]interface{}{"address": "311 Test Addr", "name": "Test3"},
		map[string]interface{}{"address": "411 Test Addr", "name": "Test4"},
	)
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('211 Test Addr', 'Test2'), ('311 Test Addr', 'Test3'), ('411 Test Addr', 'Test4')`)

	_, err = ds1.InsertSql(
		map[string]interface{}{"address": "111 Test Addr", "name": "Test1"},
		map[string]interface{}{"address": "211 Test Addr"},
		map[string]interface{}{"address": "311 Test Addr", "name": "Test3"},
		map[string]interface{}{"address": "411 Test Addr", "name": "Test4"},
	)
	assert.EqualError(t, err, "gql: Rows with different value length expected 2 got 1")
}

func (me *datasetTest) TestInsertSqlWitSqlBuilder() {
	t := me.T()
	ds1 := From("items")

	sql, err := ds1.InsertSql(From("other_items"))
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

	sql, err := ds1.Returning("id").InsertSql(From("other_items"))
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" SELECT * FROM "other_items" RETURNING "id"`)

	sql, err = ds1.InsertSql(map[string]interface{}{"name": "Test", "address": "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test') RETURNING "id"`)

	sql, err = ds1.InsertSql(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test') RETURNING "id"`)
}

func (me *datasetTest) TestInsertSqlWithNoFrom() {
	t := me.T()
	ds1 := From("test").From()
	_, err := ds1.InsertSql(map[string]interface{}{"address": "111 Test Addr", "name": "Test1"})
	assert.EqualError(t, err, "gql: No source found when generating insert sql")
}

func (me *datasetTest) TestInsertSqlWithMapsWithDifferentLengths() {
	t := me.T()
	ds1 := From("items")
	_, err := ds1.InsertSql(
		map[string]interface{}{"address": "111 Test Addr", "name": "Test1"},
		map[string]interface{}{"address": "211 Test Addr"},
		map[string]interface{}{"address": "311 Test Addr", "name": "Test3"},
		map[string]interface{}{"address": "411 Test Addr", "name": "Test4"},
	)
	assert.EqualError(t, err, "gql: Rows with different value length expected 2 got 1")
}

func (me *datasetTest) TestInsertSqlWitDifferentKeys() {
	t := me.T()
	ds1 := From("items")
	_, err := ds1.InsertSql(
		map[string]interface{}{"address": "111 Test Addr", "name": "test"},
		map[string]interface{}{"phoneNumber": 10, "address": "111 Test Addr"},
	)
	assert.EqualError(t, err, `gql: Rows with different keys expected ["address","name"] got ["address","phoneNumber"]`)
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
	_, err := ds1.InsertSql(
		item{Address: "111 Test Addr", Name: "Test1"},
		item2{Address: "211 Test Addr", Name: "Test2"},
		item{Address: "311 Test Addr", Name: "Test3"},
		item2{Address: "411 Test Addr", Name: "Test4"},
	)
	assert.EqualError(t, err, "gql: Rows must be all the same type expected gql.item got gql.item2")

	_, err = ds1.InsertSql(
		item{Address: "111 Test Addr", Name: "Test1"},
		map[string]interface{}{"address": "211 Test Addr", "name": "Test2"},
		item{Address: "311 Test Addr", Name: "Test3"},
		map[string]interface{}{"address": "411 Test Addr", "name": "Test4"},
	)
	assert.EqualError(t, err, "gql: Rows must be all the same type expected gql.item got map[string]interface {}")
}

func (me *datasetTest) TestInsertWithGqlPkTagSql() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Id      uint32 `db:"id" gql:"pk,skipinsert"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, err := ds1.InsertSql(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test')`)

	sql, err = ds1.InsertSql(map[string]interface{}{"name": "Test", "address": "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test')`)

	sql, err = ds1.InsertSql(
		item{Name: "Test1", Address: "111 Test Addr"},
		item{Name: "Test2", Address: "211 Test Addr"},
		item{Name: "Test3", Address: "311 Test Addr"},
		item{Name: "Test4", Address: "411 Test Addr"},
	)
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test1'), ('211 Test Addr', 'Test2'), ('311 Test Addr', 'Test3'), ('411 Test Addr', 'Test4')`)
}

func (me *datasetTest) TestInsertWithGqlSkipInsertTagSql() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Id      uint32 `db:"id" gql:"skipinsert"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, err := ds1.InsertSql(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test')`)

	sql, err = ds1.InsertSql(
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

	sql, err := ds1.InsertSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `INSERT INTO "items" DEFAULT VALUES`)

	sql, err = ds1.InsertSql(map[string]interface{}{"name": Default(), "address": Default()})
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
	sql, args, err := ds1.PreparedInsertSql(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test"})
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES (?, ?)`)

	sql, args, err = ds1.PreparedInsertSql(
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

	sql, args, err := ds1.PreparedInsertSql(map[string]interface{}{"name": "Test", "address": "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test"})
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES (?, ?)`)

	sql, args, err = ds1.PreparedInsertSql(
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

	sql, args, err := ds1.PreparedInsertSql(From("other_items").Where(I("b").Gt(10)))
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{10})
	assert.Equal(t, sql, `INSERT INTO "items" SELECT * FROM "other_items" WHERE ("b" > ?)`)
}

func (me *datasetTest) TestPreparedInsertReturning() {
	t := me.T()
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	ds1 := From("items").Returning("id")

	sql, args, err := ds1.Returning("id").PreparedInsertSql(From("other_items").Where(I("b").Gt(10)))
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{10})
	assert.Equal(t, sql, `INSERT INTO "items" SELECT * FROM "other_items" WHERE ("b" > ?) RETURNING "id"`)

	sql, args, err = ds1.PreparedInsertSql(map[string]interface{}{"name": "Test", "address": "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test"})
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES (?, ?) RETURNING "id"`)

	sql, args, err = ds1.PreparedInsertSql(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test"})
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES (?, ?) RETURNING "id"`)
}

func (me *datasetTest) TestPreparedInsertWithGqlPkTagSql() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Id      uint32 `db:"id" gql:"pk,skipinsert"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, args, err := ds1.PreparedInsertSql(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test"})
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES (?, ?)`)

	sql, args, err = ds1.PreparedInsertSql(map[string]interface{}{"name": "Test", "address": "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test"})
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES (?, ?)`)

	sql, args, err = ds1.PreparedInsertSql(
		item{Name: "Test1", Address: "111 Test Addr"},
		item{Name: "Test2", Address: "211 Test Addr"},
		item{Name: "Test3", Address: "311 Test Addr"},
		item{Name: "Test4", Address: "411 Test Addr"},
	)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test1", "211 Test Addr", "Test2", "311 Test Addr", "Test3", "411 Test Addr", "Test4"})
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES (?, ?), (?, ?), (?, ?), (?, ?)`)
}

func (me *datasetTest) TestPreparedInsertWithGqlSkipInsertTagSql() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Id      uint32 `db:"id" gql:"skipinsert"`
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, args, err := ds1.PreparedInsertSql(item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{"111 Test Addr", "Test"})
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES (?, ?)`)

	sql, args, err = ds1.PreparedInsertSql(
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

	sql, args, err := ds1.PreparedInsertSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{})
	assert.Equal(t, sql, `INSERT INTO "items" DEFAULT VALUES`)

	sql, args, err = ds1.PreparedInsertSql(map[string]interface{}{"name": Default(), "address": Default()})
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{})
	assert.Equal(t, sql, `INSERT INTO "items" ("address", "name") VALUES (DEFAULT, DEFAULT)`)

}
