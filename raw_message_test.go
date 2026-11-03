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
