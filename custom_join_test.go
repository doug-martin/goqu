package goqu_test

import (
	"testing"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/mysql"
	_ "github.com/doug-martin/goqu/v9/dialect/postgres"
	_ "github.com/doug-martin/goqu/v9/dialect/sqlite3"
	_ "github.com/doug-martin/goqu/v9/dialect/sqlserver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSelectDataset_CustomJoinInAllDialects(t *testing.T) {
	for _, dialect := range []string{"default", "mysql", "postgres", "sqlite3", "sqlserver"} {
		t.Run(dialect, func(t *testing.T) {
			ds := goqu.Dialect(dialect).From("t").CustomJoin(goqu.L("ARRAY JOIN tags"))
			sql, args, err := ds.ToSQL()
			require.NoError(t, err)
			assert.Contains(t, sql, " ARRAY JOIN tags")
			assert.Empty(t, args)

			sql, args, err = ds.Prepared(true).Where(goqu.C("a").Eq(1)).ToSQL()
			require.NoError(t, err)
			assert.Contains(t, sql, " ARRAY JOIN tags WHERE")
			assert.Equal(t, []any{int64(1)}, args)
		})
	}
}
