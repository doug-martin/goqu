package goqu_test

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/mysql"
	_ "github.com/doug-martin/goqu/v9/dialect/postgres"
	"github.com/doug-martin/goqu/v9/sqlgen"
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
		require.Equal(t, `INSERT INTO "posts" ("tags", "title") VALUES ('{"a","b"}', 't')`, sql)
	})

	t.Run("insert prepared", func(t *testing.T) {
		sql, args, err := pg.Insert("posts").Rows(goqu.Record{"tags": []int{1, 2}}).Prepared(true).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `INSERT INTO "posts" ("tags") VALUES ($1)`, sql)
		require.Equal(t, []any{"{1,2}"}, args)
	})

	t.Run("insert struct", func(t *testing.T) {
		sql, _, err := pg.Insert("posts").Rows(arrayRow{Name: "n", Tags: []string{"x"}}).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `INSERT INTO "posts" ("name", "tags") VALUES ('n', '{"x"}')`, sql)
	})

	t.Run("insert cols and vals", func(t *testing.T) {
		sql, _, err := pg.Insert("posts").Cols("title", "tags").Vals(goqu.Vals{"t", []string{"a"}}).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `INSERT INTO "posts" ("title", "tags") VALUES ('t', '{"a"}')`, sql)
	})

	t.Run("empty and nil slices", func(t *testing.T) {
		sql, _, err := pg.Insert("posts").Rows(goqu.Record{"a": []string{}, "b": []string(nil)}).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `INSERT INTO "posts" ("a", "b") VALUES ('{}', NULL)`, sql)
	})

	t.Run("nested slices", func(t *testing.T) {
		sql, _, err := pg.Insert("posts").Rows(goqu.Record{"m": [][]int{{1, 2}, {3, 4}}}).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `INSERT INTO "posts" ("m") VALUES ('{{1,2},{3,4}}')`, sql)
	})

	t.Run("update", func(t *testing.T) {
		sql, _, err := pg.Update("posts").Set(goqu.Record{"tags": []string{"a"}}).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `UPDATE "posts" SET "tags"='{"a"}'`, sql)
	})

	t.Run("on conflict update", func(t *testing.T) {
		sql, _, err := pg.Insert("posts").Rows(goqu.Record{"id": 1}).
			OnConflict(goqu.DoUpdate("id", goqu.Record{"tags": []string{"a"}})).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `INSERT INTO "posts" ("id") VALUES (1) ON CONFLICT (id) DO UPDATE SET "tags"='{"a"}'`, sql)
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
		var none *[]string
		sql, _, err := pg.Insert("posts").Rows(goqu.Record{"a": new([]string{"a"}), "b": none}).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `INSERT INTO "posts" ("a", "b") VALUES ('{"a"}', NULL)`, sql)
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
		require.Equal(t, `SELECT * FROM "posts" WHERE ("tags" = '{"a"}')`, sql)
	})

	t.Run("goqu.Array in a dialect without SliceAsArray", func(t *testing.T) {
		sql, _, err := goqu.Dialect("mysql").Update("posts").Set(goqu.Record{"tags": goqu.Array([]int{1})}).ToSQL()
		require.NoError(t, err)
		require.Equal(t, "UPDATE `posts` SET `tags`='{1}'", sql)
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
		sql, _, err := pg.Update("posts").Set(goqu.Record{"p": goqu.Array(new([]string{"a"})), "q": goqu.Array([2]int{1, 2})}).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `UPDATE "posts" SET "p"='{"a"}',"q"='{1,2}'`, sql)
	})
}

func TestSliceAsArrayElements(t *testing.T) {
	pg := goqu.Dialect("postgres")

	t.Run("element escaping", func(t *testing.T) {
		sql, _, err := pg.Update("posts").Set(goqu.Record{"tags": []string{`a"b`, `c\d`, "it's", "", "NULL"}}).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `UPDATE "posts" SET "tags"='{"a\"b","c\\d","it''s","","NULL"}'`, sql)
	})

	t.Run("element types", func(t *testing.T) {
		ts := time.Date(2024, 1, 2, 3, 4, 5, 6000, time.UTC)
		sql, _, err := pg.Update("posts").Set(goqu.Record{
			"f": []float64{1.5, math.Inf(1), math.Inf(-1)},
			"b": []bool{true, false},
			"u": []uint8{1}, // a byte slice is bytea, not an array
			"t": []time.Time{ts},
			"x": [][]byte{{0, 1}},
			"n": []*string{nil},
			"v": []any{arrayValuer{}, nil, 7, "s"},
			"i": []int8{-1},
		}).ToSQL()
		require.NoError(t, err)
		require.Contains(t, sql, `"b"='{true,false}'`)
		require.Contains(t, sql, `"f"='{1.5,Infinity,-Infinity}'`)
		require.Contains(t, sql, `"t"='{"2024-01-02T03:04:05.000006Z"}'`)
		require.Contains(t, sql, `"x"='{"\\x0001"}'`)
		require.Contains(t, sql, `"n"='{NULL}'`)
		require.Contains(t, sql, `"v"='{"{x}",NULL,7,"s"}'`)
		require.Contains(t, sql, `"i"='{-1}'`)
	})

	t.Run("expression elements use the array constructor", func(t *testing.T) {
		sql, _, err := pg.Update("posts").Set(goqu.Record{"tags": []any{goqu.L("now()"), "a"}}).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `UPDATE "posts" SET "tags"=ARRAY[now(), 'a']`, sql)

		sql, _, err = pg.Update("posts").Set(goqu.Record{"tags": [][]any{{goqu.L("a")}, {goqu.L("b")}}}).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `UPDATE "posts" SET "tags"=ARRAY[ARRAY[a], ARRAY[b]]`, sql)
	})

	t.Run("nested empty slices are an error", func(t *testing.T) {
		_, _, err := pg.Update("posts").Set(goqu.Record{"m": [][]string{{}, {}}}).ToSQL()
		require.ErrorIs(t, err, sqlgen.ErrEmptyNestedArray)

		_, _, err = pg.Update("posts").Set(goqu.Record{"m": [][]any{{goqu.L("a")}, {}}}).ToSQL()
		require.ErrorIs(t, err, sqlgen.ErrEmptyNestedArray)

		_, _, err = pg.Update("posts").Set(goqu.Record{"m": []any{goqu.L("a"), []any{}}}).ToSQL()
		require.ErrorIs(t, err, sqlgen.ErrEmptyNestedArray)
	})

	t.Run("a valuer error is returned", func(t *testing.T) {
		_, _, err := pg.Update("posts").Set(goqu.Record{"m": []any{failingValuer{}}}).ToSQL()
		require.EqualError(t, err, "goqu: boom")
	})

	t.Run("an unsupported element is an error", func(t *testing.T) {
		_, _, err := pg.Update("posts").Set(goqu.Record{"m": []any{struct{ A int }{1}}}).ToSQL()
		require.Error(t, err)
	})

	t.Run("json and text bytes are text, binary bytes are hex", func(t *testing.T) {
		sql, _, err := pg.Update("posts").Set(goqu.Record{
			"j": []json.RawMessage{json.RawMessage(`{"a":"b"}`)},
			"k": []any{jsonValuer{}},
			"u": [][2]byte{{0, 255}},
		}).ToSQL()
		require.NoError(t, err)
		require.Contains(t, sql, `"j"='{"{\"a\":\"b\"}"}'`)
		require.Contains(t, sql, `"k"='{"{\"k\":1}"}'`)
		require.Contains(t, sql, `"u"='{"\\x00ff"}'`)
	})

	t.Run("times use the time location and the dialect time format", func(t *testing.T) {
		loc := time.FixedZone("X", 3600)
		goqu.SetTimeLocation(loc)
		defer goqu.SetTimeLocation(time.UTC)
		ts := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
		sql, _, err := pg.Update("posts").Set(goqu.Record{"t": []time.Time{ts}}).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `UPDATE "posts" SET "t"='{"2024-01-02T04:04:05+01:00"}'`, sql)
	})

	t.Run("ragged and nil sub arrays are errors", func(t *testing.T) {
		_, _, err := pg.Update("posts").Set(goqu.Record{"m": [][]int{{1}, {2, 3}}}).ToSQL()
		require.ErrorIs(t, err, sqlgen.ErrRaggedArray)
		_, _, err = pg.Update("posts").Set(goqu.Record{"m": [][]int{nil, {1}}}).ToSQL()
		require.ErrorIs(t, err, sqlgen.ErrEmptyNestedArray)
		sql, _, err := pg.Update("posts").Set(goqu.Record{"m": []any{[]int{1}, []int{2}, "x"}}).ToSQL()
		require.NoError(t, err)
		require.Contains(t, sql, `{{1},{2},"x"}`)
	})

	t.Run("named byte arrays, nil raw messages and slice backed valuers", func(t *testing.T) {
		sql, _, err := pg.Update("posts").Set(goqu.Record{
			"a": goqu.Array([][2]octet{{0, 255}}),
			"b": []json.RawMessage{nil, json.RawMessage(`1`)},
			"c": []any{sliceValuer{1}, sliceValuer{1, 2}, &sliceValuer{3}},
			"d": [][]any{{sliceValuer{1}, "x"}, {sliceValuer{1, 2}, "y"}},
		}).ToSQL()
		require.NoError(t, err)
		require.Contains(t, sql, `"a"='{"\\x00ff"}'`)
		require.Contains(t, sql, `"b"='{NULL,"1"}'`)
		require.Contains(t, sql, `"c"='{"v","v","v"}'`)
		require.Contains(t, sql, `"d"='{{"v","x"},{"v","y"}}'`)
	})

	t.Run("pointer elements and named types", func(t *testing.T) {
		sql, _, err := pg.Update("posts").Set(goqu.Record{"a": []*int{new(5)}, "b": []*string{new("x")}, "c": []namedUint{1}}).ToSQL()
		require.NoError(t, err)
		require.Contains(t, sql, `"a"='{5}'`)
		require.Contains(t, sql, `"b"='{"x"}'`)
		require.Contains(t, sql, `"c"='{1}'`)
	})
}

type namedUint uint16

type octet uint8

// a Valuer backed by a slice that is a single value in an array
type sliceValuer []int

func (v sliceValuer) Value() (driver.Value, error) { return "v", nil }

type jsonValuer struct{}

func (jsonValuer) Value() (driver.Value, error) { return []byte(`{"k":1}`), nil }

type failingValuer struct{}

func (failingValuer) Value() (driver.Value, error) { return nil, errors.New("goqu: boom") }
