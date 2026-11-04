package sqlserver_test

import (
	"testing"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/sqlserver"
	"github.com/stretchr/testify/require"
)

// https://github.com/doug-martin/goqu/issues/411
func TestStringLiteralsAreNotBackslashEscaped(t *testing.T) {
	t.Parallel()

	sql, args, err := goqu.Dialect("sqlserver").Insert("t").
		Rows(goqu.Record{"m": `{"a":"it's","b":"c:\dir"}` + "\n"}).
		ToSQL()
	require.NoError(t, err)
	require.Empty(t, args)
	require.Equal(t, "INSERT INTO \"t\" (\"m\") VALUES ('{\"a\":\"it''s\",\"b\":\"c:\\dir\"}\n')", sql)
}

func TestStringLiteralsCannotBeBrokenOutOf(t *testing.T) {
	t.Parallel()

	for in, want := range map[string]string{
		"' OR 1=1 --":          "SELECT ''' OR 1=1 --'",
		`\'; DROP TABLE t; --`: `SELECT '\''; DROP TABLE t; --'`,
		"''":                   "SELECT ''''''",
	} {
		sql, args, err := goqu.Dialect("sqlserver").Select(goqu.L("?", in)).ToSQL()
		require.NoError(t, err)
		require.Empty(t, args)
		require.Equal(t, want, sql)
	}
}
