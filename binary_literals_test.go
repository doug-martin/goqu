package goqu_test

import (
	"testing"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/mysql"
	_ "github.com/doug-martin/goqu/v9/dialect/postgres"
	_ "github.com/doug-martin/goqu/v9/dialect/sqlite3"
	_ "github.com/doug-martin/goqu/v9/dialect/sqlserver"
	"github.com/stretchr/testify/require"
)

// https://github.com/doug-martin/goqu/issues/335, #436, #254, #353
func TestInterpolatedBinaryData(t *testing.T) {
	t.Parallel()

	binary := []byte{40, 41, 42, 250, 0x0, 0x0}
	invalid := []byte{0xff, 0xfe}
	text := []byte("hello world")

	tests := []struct {
		dialect   string
		binary    string
		invalid   string
		text      string
		plainText string
	}{
		{"default", "X'28292afa0000'", "X'fffe'", "'hello world'", `'plain'`},
		{"postgres", "decode('28292afa0000', 'hex')", "decode('fffe', 'hex')", "'hello world'", `'plain'`},
		{"mysql", "X'28292afa0000'", "X'fffe'", "'hello world'", `'plain'`},
		{"sqlite3", "X'28292afa0000'", "X'fffe'", "'hello world'", `'plain'`},
		{"sqlserver", "0x28292afa0000", "0xfffe", "'hello world'", `'plain'`},
	}
	for _, tc := range tests {
		t.Run(tc.dialect, func(t *testing.T) {
			t.Parallel()
			ds := goqu.Dialect(tc.dialect)
			for in, want := range map[string]string{
				string(binary): tc.binary, string(invalid): tc.invalid, string(text): tc.text, `plain`: tc.plainText,
			} {
				sql, args, err := ds.Select(goqu.L("?", []byte(in))).ToSQL()
				require.NoError(t, err)
				require.Empty(t, args)
				require.Equal(t, "SELECT "+want, sql)
			}
		})
	}
}

func TestPreparedBinaryDataUnchanged(t *testing.T) {
	t.Parallel()

	binary := []byte{250, 0}
	sql, args, err := goqu.Dialect("postgres").Insert("t").Rows(goqu.Record{"b": binary}).Prepared(true).ToSQL()
	require.NoError(t, err)
	require.Equal(t, `INSERT INTO "t" ("b") VALUES ($1)`, sql)
	require.Equal(t, []any{binary}, args)
}
