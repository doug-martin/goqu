package goqu

import (
	"github.com/c2fo/testify/assert"
	"github.com/c2fo/testify/suite"
	"testing"
)

type upsertTest struct {
	suite.Suite
}

func (me *upsertTest) TestUpsertSql__OnConflictDoNothing() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	sql, _, err := ds1.ToUpsertSql(DoNothing(), item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test') ON CONFLICT DO NOTHING`, sql)

	sql, _, err = ds1.ToUpsertSql(nil, item{Name: "Test", Address: "111 Test Addr"})
	assert.NoError(t, err)
	assert.Equal(t, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test') ON CONFLICT DO NOTHING`, sql)
}

func (me *upsertTest) TestUpsertSql__OnConflictDoUpdate() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	i := item{Name: "Test", Address: "111 Test Addr"}
	sql, _, err := ds1.ToUpsertSql(DoUpdate("name", Record{"address": L("excluded.address")}), i)
	assert.NoError(t, err)
	assert.Equal(t, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test') ON CONFLICT (name) DO UPDATE SET "address"=excluded.address`, sql)
}

func (me *upsertTest) TestUpsertSql__OnConflictDoUpdateWhere() {
	t := me.T()
	ds1 := From("items")
	type item struct {
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	i := item{Name: "Test", Address: "111 Test Addr"}

	sql, _, err := ds1.ToUpsertSql(DoUpdate("name", Record{"address": L("excluded.address")}).Where(I("name").Eq("Test")), i)
	assert.NoError(t, err)
	assert.Equal(t, `INSERT INTO "items" ("address", "name") VALUES ('111 Test Addr', 'Test') ON CONFLICT (name) DO UPDATE SET "address"=excluded.address WHERE ("name" = 'Test')`, sql)
}


func (me *upsertTest) TestDoNothing__ImplementsConflictExpressionInterface() {
	t := me.T()
	assert.Implements(t, (*ConflictExpression)(nil), DoNothing())
	assert.Implements(t, (*ConflictExpression)(nil), DoUpdate("", nil))
}


func TestUpsertSuite(t *testing.T) {
	suite.Run(t, new(upsertTest))
}