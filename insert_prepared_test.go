package goqu_test

import (
	"testing"

	"github.com/doug-martin/goqu/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInsertDataset_preparedBulkInsertArgs(t *testing.T) {
	ds := goqu.Insert("t").Prepared(true).Rows(
		goqu.Record{"a": 1, "b": "x"},
		goqu.Record{"a": 2, "b": "y"},
		goqu.Record{"a": 3, "b": "z"},
	)
	sql, args, err := ds.ToSQL()
	require.NoError(t, err)
	assert.Equal(t, `INSERT INTO "t" ("a", "b") VALUES (?, ?), (?, ?), (?, ?)`, sql)
	assert.Equal(t, []any{int64(1), "x", int64(2), "y", int64(3), "z"}, args)
}

func TestInsertDataset_preparedColsValsArgs(t *testing.T) {
	ds := goqu.Insert("t").Prepared(true).Cols("a", "b").Vals(
		goqu.Vals{1, "x"},
		goqu.Vals{2, "y"},
		goqu.Vals{3, "z"},
	)
	sql, args, err := ds.ToSQL()
	require.NoError(t, err)
	assert.Equal(t, `INSERT INTO "t" ("a", "b") VALUES (?, ?), (?, ?), (?, ?)`, sql)
	assert.Equal(t, []any{int64(1), "x", int64(2), "y", int64(3), "z"}, args)
}
