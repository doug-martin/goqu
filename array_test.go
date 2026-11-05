package goqu_test

import (
	"database/sql/driver"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/mysql"
	_ "github.com/doug-martin/goqu/v9/dialect/postgres"
)

type arrayValuer struct{}

func (arrayValuer) Value() (driver.Value, error) { return "{x}", nil }

type arrayRow struct {
	Name string   `db:"name"`
	Tags []string `db:"tags"`
}

// slices in insert and update values render as arrays on postgres (#387, #215)
func TestSliceAsArray(t *testing.T) {
	pg := goqu.Dialect("postgres")

	t.Run("insert record", func(t *testing.T) {
		sql, _, err := pg.Insert("posts").Rows(goqu.Record{"title": "t", "tags": []string{"a", "b"}}).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `INSERT INTO "posts" ("tags", "title") VALUES (ARRAY['a', 'b'], 't')`, sql)
	})

	t.Run("insert prepared", func(t *testing.T) {
		sql, args, err := pg.Insert("posts").Rows(goqu.Record{"tags": []int{1, 2}}).Prepared(true).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `INSERT INTO "posts" ("tags") VALUES (ARRAY[$1, $2])`, sql)
		require.Equal(t, []any{int64(1), int64(2)}, args)
	})

	t.Run("insert struct", func(t *testing.T) {
		sql, _, err := pg.Insert("posts").Rows(arrayRow{Name: "n", Tags: []string{"x"}}).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `INSERT INTO "posts" ("name", "tags") VALUES ('n', ARRAY['x'])`, sql)
	})

	t.Run("insert cols and vals", func(t *testing.T) {
		sql, _, err := pg.Insert("posts").Cols("title", "tags").Vals(goqu.Vals{"t", []string{"a"}}).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `INSERT INTO "posts" ("title", "tags") VALUES ('t', ARRAY['a'])`, sql)
	})

	t.Run("empty and nil slices", func(t *testing.T) {
		sql, _, err := pg.Insert("posts").Rows(goqu.Record{"a": []string{}, "b": []string(nil)}).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `INSERT INTO "posts" ("a", "b") VALUES ('{}', NULL)`, sql)
	})

	t.Run("nested slices", func(t *testing.T) {
		sql, _, err := pg.Insert("posts").Rows(goqu.Record{"m": [][]int{{1, 2}, {3, 4}}}).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `INSERT INTO "posts" ("m") VALUES (ARRAY[ARRAY[1, 2], ARRAY[3, 4]])`, sql)
	})

	t.Run("update", func(t *testing.T) {
		sql, _, err := pg.Update("posts").Set(goqu.Record{"tags": []string{"a"}}).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `UPDATE "posts" SET "tags"=ARRAY['a']`, sql)
	})

	t.Run("on conflict update", func(t *testing.T) {
		sql, _, err := pg.Insert("posts").Rows(goqu.Record{"id": 1}).
			OnConflict(goqu.DoUpdate("id", goqu.Record{"tags": []string{"a"}})).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `INSERT INTO "posts" ("id") VALUES (1) ON CONFLICT (id) DO UPDATE SET "tags"=ARRAY['a']`, sql)
	})

	t.Run("not converted", func(t *testing.T) {
		sql, _, err := pg.Insert("posts").Rows(goqu.Record{
			"raw":  json.RawMessage(`{"a":1}`),
			"val":  arrayValuer{},
			"lit":  goqu.L("'{}'::text[]"),
			"text": "plain",
		}).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `INSERT INTO "posts" ("lit", "raw", "text", "val") VALUES ('{}'::text[], '{"a":1}', 'plain', '{x}')`, sql)
	})

	t.Run("pointer to a slice", func(t *testing.T) {
		tags := []string{"a"}
		var none *[]string
		sql, _, err := pg.Insert("posts").Rows(goqu.Record{"a": &tags, "b": none}).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `INSERT INTO "posts" ("a", "b") VALUES (ARRAY['a'], NULL)`, sql)
	})

	t.Run("predicates keep the list form", func(t *testing.T) {
		sql, _, err := pg.From("posts").Where(goqu.C("id").In([]int{1, 2})).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `SELECT * FROM "posts" WHERE ("id" IN (1, 2))`, sql)
	})

	t.Run("other dialects keep the list form", func(t *testing.T) {
		sql, _, err := goqu.Dialect("mysql").Insert("posts").Rows(goqu.Record{"tags": []string{"a", "b"}}).ToSQL()
		require.NoError(t, err)
		require.Equal(t, "INSERT INTO `posts` (`tags`) VALUES (('a', 'b'))", sql)
	})

	t.Run("goqu.Array in a predicate", func(t *testing.T) {
		sql, _, err := pg.From("posts").Where(goqu.C("tags").Eq(goqu.Array([]string{"a"}))).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `SELECT * FROM "posts" WHERE ("tags" = ARRAY['a'])`, sql)
	})

	t.Run("goqu.Array in a dialect without SliceAsArray", func(t *testing.T) {
		sql, _, err := goqu.Dialect("mysql").Update("posts").Set(goqu.Record{"tags": goqu.Array([]int{1})}).ToSQL()
		require.NoError(t, err)
		require.Equal(t, "UPDATE `posts` SET `tags`=ARRAY[1]", sql)
	})

	t.Run("goqu.Array of a non slice is an error", func(t *testing.T) {
		_, _, err := pg.Update("posts").Set(goqu.Record{"tags": goqu.Array(1)}).ToSQL()
		require.Error(t, err)
	})

	t.Run("goqu.Array of nil is NULL", func(t *testing.T) {
		sql, _, err := pg.Update("posts").Set(goqu.Record{"tags": goqu.Array(nil)}).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `UPDATE "posts" SET "tags"=NULL`, sql)
	})

	t.Run("pointer and Go array", func(t *testing.T) {
		s := []string{"a"}
		sql, _, err := pg.Update("posts").Set(goqu.Record{"p": goqu.Array(&s), "q": goqu.Array([2]int{1, 2})}).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `UPDATE "posts" SET "p"=ARRAY['a'],"q"=ARRAY[1, 2]`, sql)
	})
}
