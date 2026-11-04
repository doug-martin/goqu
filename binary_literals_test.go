package goqu_test

import (
	"fmt"
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

func TestInterpolatedBinaryDataWithNUL(t *testing.T) {
	t.Parallel()

	nul := string([]byte{'a', 0, 'b'})
	for dialect, want := range map[string]string{
		"postgres":  "SELECT decode('610062', 'hex')",
		"sqlserver": "SELECT 0x610062",
		// mysql and sqlite3 accept NUL in text literals and keep writing them inline
		"mysql":   "SELECT 'a\\x00b'",
		"sqlite3": "SELECT 'a\x00b'",
	} {
		sql, _, err := goqu.Dialect(dialect).Select(goqu.L("?", []byte(nul))).ToSQL()
		require.NoError(t, err)
		require.Equal(t, want, sql, dialect)
	}
}

func TestInterpolatedBinaryDataLong(t *testing.T) {
	t.Parallel()

	// longer than the internal hex chunk so that chunking is exercised
	data := make([]byte, 300)
	want := make([]byte, 0, len(data)*2)
	for i := range data {
		data[i] = byte(0x80 + i%0x7f) // never valid UTF-8 on its own
		want = append(want, fmt.Sprintf("%02x", data[i])...)
	}
	sql, _, err := goqu.Dialect("sqlserver").Select(goqu.L("?", data)).ToSQL()
	require.NoError(t, err)
	require.Equal(t, "SELECT 0x"+string(want), sql)
}

func TestInterpolatedBinaryDataCustomDialectWithoutBinaryOptions(t *testing.T) {
	t.Parallel()

	opts := goqu.DefaultDialectOptions()
	opts.BinaryLiteralPrefix, opts.BinaryLiteralSuffix = nil, nil
	goqu.RegisterDialect("binary-test-no-options", opts)

	sql, _, err := goqu.Dialect("binary-test-no-options").Select(goqu.L("?", []byte{0xff})).ToSQL()
	require.NoError(t, err)
	require.Equal(t, "SELECT X'ff'", sql)
}
