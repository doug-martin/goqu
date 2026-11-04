package goqu_test

import (
	"testing"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/postgres"
	"github.com/stretchr/testify/require"
)

func TestSetDefaultDialect(t *testing.T) {
	t.Cleanup(func() { goqu.SetDefaultDialect("default") })

	sql, _, err := goqu.From("t").Where(goqu.C("a").Eq("x")).ToSQL()
	require.NoError(t, err)
	require.Equal(t, `SELECT * FROM "t" WHERE ("a" = 'x')`, sql)

	goqu.SetDefaultDialect("postgres")
	sql, args, err := goqu.From("t").Where(goqu.C("a").Eq("x")).Prepared(true).ToSQL()
	require.NoError(t, err)
	require.Equal(t, `SELECT * FROM "t" WHERE ("a" = $1)`, sql)
	require.Equal(t, []any{"x"}, args)

	sql, _, err = goqu.Insert("t").Rows(goqu.Record{"a": 1}).Returning("a").ToSQL()
	require.NoError(t, err)
	require.Equal(t, `INSERT INTO "t" ("a") VALUES (1) RETURNING "a"`, sql)

	// explicit dialects still win
	require.Equal(t, goqu.GetDialect("default"), goqu.Dialect("default").From("t").Dialect())
}

func TestSetDefaultDialectUnregisteredPanics(t *testing.T) {
	require.Panics(t, func() { goqu.SetDefaultDialect("nope") })
	sql, _, err := goqu.From("t").ToSQL()
	require.NoError(t, err)
	require.Equal(t, `SELECT * FROM "t"`, sql)
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
