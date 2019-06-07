package goqu

import (
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func (me *datasetTest) TestDeleteSqlNoReturning() {
	t := me.T()
	mDb, _, _ := sqlmock.New()
	ds1 := New("no-return", mDb).From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	_, _, err := ds1.Returning("id").ToDeleteSql()
	assert.EqualError(t, err, "goqu: Adapter does not support RETURNING clause")
}

func (me *datasetTest) TestDeleteSqlWithLimit() {
	t := me.T()
	mDb, _, _ := sqlmock.New()
	ds1 := New("limit", mDb).From("items")
	sql, _, err := ds1.Limit(10).ToDeleteSql()
	assert.Nil(t, err)
	assert.Equal(t, sql, `DELETE FROM "items" LIMIT 10`)
}

func (me *datasetTest) TestDeleteSqlWithOrder() {
	t := me.T()
	mDb, _, _ := sqlmock.New()
	ds1 := New("order", mDb).From("items")
	sql, _, err := ds1.Order(I("name").Desc()).ToDeleteSql()
	assert.Nil(t, err)
	assert.Equal(t, sql, `DELETE FROM "items" ORDER BY "name" DESC`)
}

func (me *datasetTest) TestToDeleteSql() {
	t := me.T()
	ds1 := From("items")
	sql, _, err := ds1.ToDeleteSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `DELETE FROM "items"`)
}

func (me *datasetTest) TestDeleteSqlNoSources() {
	t := me.T()
	ds1 := From("items")
	_, _, err := ds1.From().ToDeleteSql()
	assert.EqualError(t, err, "goqu: No source found when generating delete sql")
}

func (me *datasetTest) TestDeleteSqlWithWhere() {
	t := me.T()
	ds1 := From("items")
	sql, _, err := ds1.Where(I("id").IsNotNull()).ToDeleteSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `DELETE FROM "items" WHERE ("id" IS NOT NULL)`)
}

func (me *datasetTest) TestDeleteSqlWithReturning() {
	t := me.T()
	ds1 := From("items")
	sql, _, err := ds1.Returning("id").ToDeleteSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `DELETE FROM "items" RETURNING "id"`)

	sql, _, err = ds1.Returning("id").Where(I("id").IsNotNull()).ToDeleteSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `DELETE FROM "items" WHERE ("id" IS NOT NULL) RETURNING "id"`)
}

func (me *datasetTest) TestTruncateSql() {
	t := me.T()
	ds1 := From("items")
	sql, _, err := ds1.ToTruncateSql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `TRUNCATE "items"`)
}

func (me *datasetTest) TestTruncateSqlNoSources() {
	t := me.T()
	ds1 := From("items")
	_, _, err := ds1.From().ToTruncateSql()
	assert.EqualError(t, err, "goqu: No source found when generating truncate sql")
}

func (me *datasetTest) TestTruncateSqlWithOpts() {
	t := me.T()
	ds1 := From("items")
	sql, _, err := ds1.ToTruncateWithOptsSql(TruncateOptions{Cascade: true})
	assert.NoError(t, err)
	assert.Equal(t, sql, `TRUNCATE "items" CASCADE`)

	sql, _, err = ds1.ToTruncateWithOptsSql(TruncateOptions{Restrict: true})
	assert.NoError(t, err)
	assert.Equal(t, sql, `TRUNCATE "items" RESTRICT`)

	sql, _, err = ds1.ToTruncateWithOptsSql(TruncateOptions{Identity: "restart"})
	assert.NoError(t, err)
	assert.Equal(t, sql, `TRUNCATE "items" RESTART IDENTITY`)

	sql, _, err = ds1.ToTruncateWithOptsSql(TruncateOptions{Identity: "continue"})
	assert.NoError(t, err)
	assert.Equal(t, sql, `TRUNCATE "items" CONTINUE IDENTITY`)
}

func (me *datasetTest) TestPreparedToDeleteSql() {
	t := me.T()
	ds1 := From("items")
	sql, args, err := ds1.Prepared(true).ToDeleteSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{})
	assert.Equal(t, sql, `DELETE FROM "items"`)

	sql, args, err = ds1.Where(I("id").Eq(1)).Prepared(true).ToDeleteSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1)})
	assert.Equal(t, sql, `DELETE FROM "items" WHERE ("id" = ?)`)

	sql, args, err = ds1.Returning("id").Where(I("id").Eq(1)).Prepared(true).ToDeleteSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1)})
	assert.Equal(t, sql, `DELETE FROM "items" WHERE ("id" = ?) RETURNING "id"`)
}

func (me *datasetTest) TestPreparedTruncateSql() {
	t := me.T()
	ds1 := From("items")
	sql, args, err := ds1.ToTruncateSql()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{})
	assert.Equal(t, sql, `TRUNCATE "items"`)
}
