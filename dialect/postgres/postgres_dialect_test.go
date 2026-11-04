package postgres_test

import (
	"testing"

	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/stretchr/testify/assert"
)

func TestInsertOnConflictDoNothingTarget(t *testing.T) {
	ds := goqu.Dialect("postgres").Insert("test").
		Rows(goqu.Record{"a": "a"}).
		OnConflict(goqu.DoNothing().SetCols(exp.NewColumnListExpression(nil, "db", "a")))
	sql, args, err := ds.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `INSERT INTO "test" ("a") VALUES ('a') ON CONFLICT ("a") DO NOTHING`, sql)
	assert.Empty(t, args)

	sql, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `INSERT INTO "test" ("a") VALUES ($1) ON CONFLICT ("a") DO NOTHING`, sql)
	assert.Equal(t, []any{"a"}, args)
}
