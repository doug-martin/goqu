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
		OnConflict(goqu.DoNothing().SetCols(exp.NewColumnListExpression("a")))
	sql, args, err := ds.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `INSERT INTO "test" ("a") VALUES ('a') ON CONFLICT ("a") DO NOTHING`, sql)
	assert.Empty(t, args)

	sql, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `INSERT INTO "test" ("a") VALUES ($1) ON CONFLICT ("a") DO NOTHING`, sql)
	assert.Equal(t, []any{"a"}, args)
}

func TestInsertOnConflictTargets(t *testing.T) {
	ds := goqu.Dialect("postgres").Insert("test").Rows(goqu.Record{"a": "a", "b": "b"})

	sql, args, err := ds.OnConflict(goqu.DoNothing().OnConstraint("test_a_b_key")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `INSERT INTO "test" ("a", "b") VALUES ('a', 'b') ON CONFLICT ON CONSTRAINT "test_a_b_key" DO NOTHING`, sql)
	assert.Empty(t, args)

	sql, args, err = ds.OnConflict(
		goqu.DoUpdateSet(goqu.Record{"b": "u"}).
			TargetCols("a", "b").
			TargetWhere(goqu.C("kind").Eq("live")).
			Where(goqu.C("active").Eq(1)),
	).Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t,
		`INSERT INTO "test" ("a", "b") VALUES ($1, $2) ON CONFLICT ("a", "b") WHERE ("kind" = 'live') `+
			`DO UPDATE SET "b"=$3 WHERE ("active" = $4)`,
		sql,
	)
	assert.Equal(t, []any{"a", "b", "u", int64(1)}, args)
}
