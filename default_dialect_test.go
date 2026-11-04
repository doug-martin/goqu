package goqu_test

import (
	"testing"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/mysql"
	_ "github.com/doug-martin/goqu/v9/dialect/postgres"
	"github.com/stretchr/testify/require"
)

func resetDefaultDialect(t *testing.T) {
	t.Helper()
	goqu.SetDefaultDialect("default")
	t.Cleanup(func() { goqu.SetDefaultDialect("default") })
}

func TestSetDefaultDialect(t *testing.T) {
	resetDefaultDialect(t)

	sql, _, err := goqu.From("t").Where(goqu.C("a").Eq("x")).ToSQL()
	require.NoError(t, err)
	require.Equal(t, `SELECT * FROM "t" WHERE ("a" = 'x')`, sql)

	goqu.SetDefaultDialect("postgres")
	sql, args, err := goqu.From("t").Where(goqu.C("a").Eq("x")).Prepared(true).ToSQL()
	require.NoError(t, err)
	require.Equal(t, `SELECT * FROM "t" WHERE ("a" = $1)`, sql)
	require.Equal(t, []any{"x"}, args)

	// explicit dialects still win
	sql, _, err = goqu.Dialect("default").From("t").Where(goqu.C("a").Eq(1)).Prepared(true).ToSQL()
	require.NoError(t, err)
	require.Equal(t, `SELECT * FROM "t" WHERE ("a" = ?)`, sql)
	sql, _, err = goqu.From("t").WithDialect("mysql").Select("a").ToSQL()
	require.NoError(t, err)
	require.Equal(t, "SELECT `a` FROM `t`", sql)
}

func TestSetDefaultDialectAllStatements(t *testing.T) {
	resetDefaultDialect(t)
	goqu.SetDefaultDialect("mysql")

	tests := map[string]string{
		"select":   "SELECT `a` FROM `t`",
		"from":     "SELECT * FROM `t`",
		"insert":   "INSERT INTO `t` (`a`) VALUES (1)",
		"update":   "UPDATE `t` SET `a`=1",
		"delete":   "DELETE `t` FROM `t`",
		"truncate": "TRUNCATE `t`",
	}
	got := map[string]string{}
	var err error
	got["select"], _, err = goqu.Select("a").From("t").ToSQL()
	require.NoError(t, err)
	got["from"], _, err = goqu.From("t").ToSQL()
	require.NoError(t, err)
	got["insert"], _, err = goqu.Insert("t").Rows(goqu.Record{"a": 1}).ToSQL()
	require.NoError(t, err)
	got["update"], _, err = goqu.Update("t").Set(goqu.Record{"a": 1}).ToSQL()
	require.NoError(t, err)
	got["delete"], _, err = goqu.Delete("t").ToSQL()
	require.NoError(t, err)
	got["truncate"], _, err = goqu.Truncate("t").ToSQL()
	require.NoError(t, err)
	require.Equal(t, tests, got)
}

func TestSetDefaultDialectUnregisteredPanics(t *testing.T) {
	resetDefaultDialect(t)
	require.Panics(t, func() { goqu.SetDefaultDialect("nope") })
	sql, _, err := goqu.From("t").ToSQL()
	require.NoError(t, err)
	require.Equal(t, `SELECT * FROM "t"`, sql)
}

func TestSetDefaultDialectResetOnDeregister(t *testing.T) {
	resetDefaultDialect(t)
	goqu.RegisterDialect("tmpdialect", goqu.DefaultDialectOptions())
	goqu.SetDefaultDialect("tmpdialect")
	goqu.DeregisterDialect("tmpdialect")
	sql, _, err := goqu.From("t").ToSQL()
	require.NoError(t, err)
	require.Equal(t, `SELECT * FROM "t"`, sql)
}

func TestInsertFromQueryWithDefaultDialect(t *testing.T) {
	resetDefaultDialect(t)
	goqu.SetDefaultDialect("postgres")

	// a select from the package level functions adopts the insert's dialect
	sql, _, err := goqu.Dialect("mysql").Insert("t").FromQuery(goqu.From("o")).ToSQL()
	require.NoError(t, err)
	require.Equal(t, "INSERT INTO `t` SELECT * FROM `o`", sql)

	// a select shared between inserts is not mutated by the first one
	shared := goqu.From("o").Where(goqu.C("a").Eq("x"))
	pg := goqu.Dialect("postgres").Insert("t").FromQuery(shared)
	my := goqu.Dialect("mysql").Insert("t").FromQuery(shared)
	sql, _, err = pg.ToSQL()
	require.NoError(t, err)
	require.Equal(t, `INSERT INTO "t" SELECT * FROM "o" WHERE ("a" = 'x')`, sql)
	sql, _, err = my.ToSQL()
	require.NoError(t, err)
	require.Equal(t, "INSERT INTO `t` SELECT * FROM `o` WHERE (`a` = 'x')", sql)
	sql, _, err = pg.ToSQL()
	require.NoError(t, err)
	require.Equal(t, `INSERT INTO "t" SELECT * FROM "o" WHERE ("a" = 'x')`, sql)

	// an explicitly chosen, different dialect is still rejected
	require.Panics(t, func() {
		goqu.Dialect("mysql").Insert("t").FromQuery(goqu.Dialect("postgres").From("o"))
	})
}

func TestRequireDialect(t *testing.T) {
	dw, err := goqu.RequireDialect("postgres")
	require.NoError(t, err)
	sql, _, _ := dw.From("t").Prepared(true).Where(goqu.C("a").Eq(1)).ToSQL()
	require.Equal(t, `SELECT * FROM "t" WHERE ("a" = $1)`, sql)

	_, err = goqu.RequireDialect("nope")
	require.Error(t, err)
	require.Contains(t, err.Error(), "nope")

	require.NotPanics(t, func() { goqu.MustDialect("postgres") })
	require.Panics(t, func() { goqu.MustDialect("nope") })
}
