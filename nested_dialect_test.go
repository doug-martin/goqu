package goqu_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/postgres"
	"github.com/doug-martin/goqu/v9/exp"
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
