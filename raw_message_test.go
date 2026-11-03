package goqu_test

import (
	"encoding/json"
	"testing"

	"github.com/doug-martin/goqu/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type rawMessageRow struct {
	Data json.RawMessage `db:"data"`
}

func TestJSONRawMessage_comparisonsAreNotInLists(t *testing.T) {
	raw := json.RawMessage(`{"a":1}`)
	for name, val := range map[string]any{"value": raw, "pointer": &raw, "bytes": []byte(`{"a":1}`)} {
		t.Run(name, func(t *testing.T) {
			sql, args, err := goqu.From("t").Where(goqu.C("c").Eq(val)).ToSQL()
			require.NoError(t, err)
			assert.Equal(t, `SELECT * FROM "t" WHERE ("c" = '{"a":1}')`, sql)
			assert.Empty(t, args)

			sql, args, err = goqu.From("t").Prepared(true).Where(goqu.C("c").Neq(val)).ToSQL()
			require.NoError(t, err)
			assert.Equal(t, `SELECT * FROM "t" WHERE ("c" != ?)`, sql)
			assert.Equal(t, []any{[]byte(`{"a":1}`)}, args)
		})
	}
}

func TestJSONRawMessage_insertFromStruct(t *testing.T) {
	sql, args, err := goqu.Insert("t").Rows(rawMessageRow{Data: json.RawMessage(`{"a":1}`)}).Prepared(true).ToSQL()
	require.NoError(t, err)
	assert.Equal(t, `INSERT INTO "t" ("data") VALUES (?)`, sql)
	assert.Equal(t, []any{[]byte(`{"a":1}`)}, args)

	// an unset RawMessage is NULL, not an empty string (which is not valid JSON)
	sql, args, err = goqu.Insert("t").Rows(rawMessageRow{}).ToSQL()
	require.NoError(t, err)
	assert.Equal(t, `INSERT INTO "t" ("data") VALUES (NULL)`, sql)
	assert.Empty(t, args)
}

func TestJSONRawMessage_inAndNotIn(t *testing.T) {
	raw := json.RawMessage(`{"a":1}`)
	for name, val := range map[string]any{"value": raw, "pointer": &raw, "bytes": []byte(`{"a":1}`)} {
		t.Run(name, func(t *testing.T) {
			sql, _, err := goqu.From("t").Where(goqu.C("c").In(val)).ToSQL()
			require.NoError(t, err)
			assert.Equal(t, `SELECT * FROM "t" WHERE ("c" IN ('{"a":1}'))`, sql)

			sql, args, err := goqu.From("t").Prepared(true).Where(goqu.C("c").NotIn(val)).ToSQL()
			require.NoError(t, err)
			assert.Equal(t, `SELECT * FROM "t" WHERE ("c" NOT IN (?))`, sql)
			assert.Equal(t, []any{[]byte(`{"a":1}`)}, args)
		})
	}
}

func TestJSONRawMessage_nilComparesAsNull(t *testing.T) {
	var unset json.RawMessage
	for name, val := range map[string]any{"value": unset, "pointer": &unset} {
		t.Run(name, func(t *testing.T) {
			sql, args, err := goqu.From("t").Where(goqu.C("c").Eq(val)).ToSQL()
			require.NoError(t, err)
			assert.Equal(t, `SELECT * FROM "t" WHERE ("c" IS NULL)`, sql)
			assert.Empty(t, args)

			sql, _, err = goqu.From("t").Where(goqu.C("c").Neq(val)).ToSQL()
			require.NoError(t, err)
			assert.Equal(t, `SELECT * FROM "t" WHERE ("c" IS NOT NULL)`, sql)
		})
	}
	// an empty but set message is still a (zero length) value
	sql, _, err := goqu.From("t").Where(goqu.C("c").Eq(json.RawMessage{})).ToSQL()
	require.NoError(t, err)
	assert.Equal(t, `SELECT * FROM "t" WHERE ("c" = '')`, sql)
}
