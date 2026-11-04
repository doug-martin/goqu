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

func TestUnicodeStringLiteralsGetNPrefix(t *testing.T) {
	t.Parallel()

	ds := goqu.Dialect("sqlserver")
	for in, want := range map[string]string{
		"plain":    "SELECT 'plain'",
		"héllo":    "SELECT N'héllo'",
		"日本語 it's": "SELECT N'日本語 it''s'",
	} {
		sql, _, err := ds.Select(goqu.L("?", in)).ToSQL()
		require.NoError(t, err)
		require.Equal(t, want, sql)

		sql, _, err = ds.Select(goqu.L("?", []byte(in))).ToSQL()
		require.NoError(t, err)
		require.Equal(t, want, sql)
	}
}
