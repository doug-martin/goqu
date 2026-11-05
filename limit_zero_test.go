package goqu_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/mysql"
	_ "github.com/doug-martin/goqu/v9/dialect/postgres"
	_ "github.com/doug-martin/goqu/v9/dialect/sqlserver"
)

// Limit(0) clears the limit, LimitZero forces LIMIT 0 (#194)
func TestLimitZero(t *testing.T) {
	t.Run("select", func(t *testing.T) {
		sql, _, err := goqu.Dialect("postgres").From("t").LimitZero().ToSQL()
		require.NoError(t, err)
		require.Equal(t, `SELECT * FROM "t" LIMIT 0`, sql)

		sql, args, err := goqu.Dialect("postgres").From("t").LimitZero().Prepared(true).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `SELECT * FROM "t" LIMIT $1`, sql)
		require.Equal(t, []any{int64(0)}, args)

		sql, _, err = goqu.Dialect("mysql").From("t").Limit(5).LimitZero().ToSQL()
		require.NoError(t, err)
		require.Equal(t, "SELECT * FROM `t` LIMIT 0", sql)
	})

	t.Run("sqlserver", func(t *testing.T) {
		sql, _, err := goqu.Dialect("sqlserver").From("t").LimitZero().ToSQL()
		require.NoError(t, err)
		require.Equal(t, `SELECT  TOP (0) * FROM "t"`, sql)
	})

	t.Run("update and delete", func(t *testing.T) {
		sql, _, err := goqu.Dialect("mysql").Update("t").Set(goqu.Record{"a": 1}).LimitZero().ToSQL()
		require.NoError(t, err)
		require.Equal(t, "UPDATE `t` SET `a`=1 LIMIT 0", sql)

		sql, _, err = goqu.Dialect("mysql").Delete("t").LimitZero().ToSQL()
		require.NoError(t, err)
		require.Equal(t, "DELETE FROM `t` LIMIT 0", sql)
	})
}
