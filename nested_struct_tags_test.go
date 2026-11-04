package goqu_test

import (
	"testing"

	"github.com/doug-martin/goqu/v9"
	"github.com/stretchr/testify/require"
)

// https://github.com/doug-martin/goqu/issues/356
func TestNestedStructSkipTags(t *testing.T) {
	t.Parallel()

	type bar struct {
		B string `db:"b"`
	}
	type embedded struct {
		E string `db:"e"`
	}
	type foo struct {
		A        string `db:"a"`
		SkipBoth bar    `db:"skip_both" goqu:"skipinsert,skipupdate"`
		SkipIns  bar    `db:"skip_ins" goqu:"skipinsert"`
		SkipUpd  bar    `db:"skip_upd" goqu:"skipupdate"`
		Keep     bar    `db:"keep"`
		embedded `goqu:"skipinsert"`
	}

	row := foo{A: "a", SkipBoth: bar{"1"}, SkipIns: bar{"2"}, SkipUpd: bar{"3"}, Keep: bar{"4"}}

	insertSQL, _, err := goqu.Insert("foo").Rows(row).ToSQL()
	require.NoError(t, err)
	require.Equal(t, `INSERT INTO "foo" ("a", "keep"."b", "skip_upd"."b") VALUES ('a', '4', '3')`, insertSQL)

	updateSQL, _, err := goqu.Update("foo").Set(row).ToSQL()
	require.NoError(t, err)
	require.Equal(t, `UPDATE "foo" SET "a"='a',"e"='',"keep"."b"='4',"skip_ins"."b"='2'`, updateSQL)
}
