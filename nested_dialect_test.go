package goqu_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/mysql"
	_ "github.com/doug-martin/goqu/v9/dialect/postgres"
	_ "github.com/doug-martin/goqu/v9/dialect/sqlserver"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/doug-martin/goqu/v9/internal/sb"
	"github.com/doug-martin/goqu/v9/sqlgen"
)

// subqueries, CTEs, compounds and insert-from-select render with the dialect of the outer statement (#413, #370)
func TestNestedDatasetsUseOuterDialect(t *testing.T) {
	sub := func() *goqu.SelectDataset { return goqu.From("s").Where(goqu.C("c").Eq(1)) }

	t.Run("subquery in update set (#413)", func(t *testing.T) {
		sql, args, err := goqu.Dialect("postgres").Update("a").
			Set(goqu.Record{"sub": sub()}).Where(goqu.C("x").Eq(2)).Prepared(true).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `UPDATE "a" SET "sub"=(SELECT * FROM "s" WHERE ("c" = $1)) WHERE ("x" = $2)`, sql)
		require.Equal(t, []any{int64(1), int64(2)}, args)
	})

	t.Run("cte (#370)", func(t *testing.T) {
		sql, args, err := goqu.From("t").Where(goqu.C("f").Eq(2)).With("cte", sub()).
			WithDialect("postgres").Prepared(true).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `WITH cte AS (SELECT * FROM "s" WHERE ("c" = $1)) SELECT * FROM "t" WHERE ("f" = $2)`, sql)
		require.Equal(t, []any{int64(1), int64(2)}, args)
	})

	t.Run("compound", func(t *testing.T) {
		sql, _, err := goqu.Dialect("postgres").From("a").Union(sub()).Prepared(true).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `SELECT * FROM "a" UNION (SELECT * FROM "s" WHERE ("c" = $1))`, sql)
	})

	t.Run("insert from query", func(t *testing.T) {
		sql, _, err := goqu.Dialect("postgres").Insert("a").FromQuery(sub()).Prepared(true).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `INSERT INTO "a" SELECT * FROM "s" WHERE ("c" = $1)`, sql)
	})

	t.Run("nested update/delete/insert in a cte", func(t *testing.T) {
		ins := goqu.Insert("x").Rows(goqu.Record{"a": 1}).Returning("a")
		upd := goqu.Update("x").Set(goqu.Record{"a": 1}).Returning("a")
		del := goqu.Delete("x").Where(goqu.C("a").Eq(1)).Returning("a")
		for _, n := range []struct {
			name string
			ds   exp.AppendableExpression
		}{{"i", ins}, {"u", upd}, {"d", del}} {
			sql, _, err := goqu.Dialect("postgres").From("t").With(n.name, n.ds).Prepared(true).ToSQL()
			require.NoError(t, err)
			require.Contains(t, sql, "$1")
			require.NotContains(t, sql, "?")
		}
	})
}

func TestAppendSQLAndErrors(t *testing.T) {
	boom := errors.New("boom")
	type appender interface {
		exp.AppendableExpression
		AppendSQLWithDialect(b sb.SQLBuilder, dialect string, do *sqlgen.SQLDialectOptions)
	}
	cases := map[string]struct {
		ok  appender
		bad appender
	}{
		"select": {goqu.From("t"), goqu.From("t").SetError(boom)},
		"insert": {goqu.Insert("t").Rows(goqu.Record{"a": 1}), goqu.Insert("t").SetError(boom)},
		"update": {goqu.Update("t").Set(goqu.Record{"a": 1}), goqu.Update("t").SetError(boom)},
		"delete": {goqu.Delete("t"), goqu.Delete("t").SetError(boom)},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			// AppendSQL uses the dataset's own dialect
			b := sb.NewSQLBuilder(false)
			c.ok.AppendSQL(b)
			s, _, err := b.ToSQL()
			require.NoError(t, err)
			require.NotEmpty(t, s)

			// an unregistered dialect name with its own options still renders
			b = sb.NewSQLBuilder(false)
			c.ok.AppendSQLWithDialect(b, "not-registered", sqlgen.DefaultDialectOptions())
			s, _, err = b.ToSQL()
			require.NoError(t, err)
			require.NotEmpty(t, s)

			// errors are propagated through both paths
			b = sb.NewSQLBuilder(false)
			c.bad.AppendSQL(b)
			require.Equal(t, boom, b.Error())
			b = sb.NewSQLBuilder(false)
			c.bad.AppendSQLWithDialect(b, "default", sqlgen.DefaultDialectOptions())
			require.Equal(t, boom, b.Error())
		})
	}
}

// stubDialect is a custom SQLDialect implementation that wraps a registered dialect and marks its selects
type stubDialect struct{ goqu.SQLDialect }

func (stubDialect) ToSelectSQL(b sb.SQLBuilder, _ exp.SelectClauses) { b.Write([]byte("CUSTOM")) }

func TestNestedDatasetDialectEdgeCases(t *testing.T) {
	t.Run("custom SQLDialect on a nested dataset is kept", func(t *testing.T) {
		custom := goqu.From("s").SetDialect(stubDialect{goqu.GetDialect("default")})
		sql, _, err := goqu.Dialect("postgres").From("t").Where(goqu.C("a").In(custom)).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `SELECT * FROM "t" WHERE ("a" IN (CUSTOM))`, sql)
	})

	t.Run("identifier quoting follows the enclosing dialect", func(t *testing.T) {
		sql, _, err := goqu.Dialect("mysql").From("t").Where(goqu.C("a").In(goqu.From("s"))).ToSQL()
		require.NoError(t, err)
		require.Equal(t, "SELECT * FROM `t` WHERE (`a` IN (SELECT * FROM `s`))", sql)

		sql, _, err = goqu.Dialect("sqlserver").From("t").Where(goqu.C("a").In(goqu.From("s"))).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `SELECT * FROM "t" WHERE ("a" IN (SELECT * FROM "s"))`, sql)
	})

	t.Run("explicit nested dialect is overridden by the enclosing one", func(t *testing.T) {
		sub := goqu.Dialect("mysql").From("s").Where(goqu.C("c").Eq(1))
		sql, args, err := goqu.Dialect("postgres").From("t").Where(goqu.C("a").In(sub)).Prepared(true).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `SELECT * FROM "t" WHERE ("a" IN (SELECT * FROM "s" WHERE ("c" = $1)))`, sql)
		require.Equal(t, []any{int64(1)}, args)
	})

	t.Run("cte nested in a cte", func(t *testing.T) {
		inner := goqu.From("s").Where(goqu.C("c").Eq(1))
		outer := goqu.From("o").With("inner_cte", inner)
		sql, _, err := goqu.Dialect("postgres").From("t").With("outer_cte", outer).Prepared(true).ToSQL()
		require.NoError(t, err)
		require.Equal(t,
			`WITH outer_cte AS (WITH inner_cte AS (SELECT * FROM "s" WHERE ("c" = $1)) SELECT * FROM "o") SELECT * FROM "t"`,
			sql)
	})
}
