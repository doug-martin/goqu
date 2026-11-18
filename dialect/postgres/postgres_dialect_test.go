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

func TestValuesTable(t *testing.T) {
	values := goqu.Values([]any{"a", goqu.Cast(goqu.V(1), "int")}).As("c", "id", "foo")
	sql, args, err := goqu.Dialect("postgres").Update("t").Set(goqu.Record{"foo": goqu.I("c.foo")}).From(values).
		Where(goqu.I("t.id").Eq(goqu.I("c.id"))).Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `UPDATE "t" SET "foo"="c"."foo" FROM (VALUES ($1, CAST($2 AS int))) AS "c"("id", "foo") `+
		`WHERE ("t"."id" = "c"."id")`, sql)
	assert.Equal(t, []any{"a", int64(1)}, args)
}

func TestValuesTableCTE(t *testing.T) {
	sql, _, err := goqu.Dialect("postgres").From("t").
		With("t(a, b)", goqu.Values([]any{1, "x"})).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `WITH t(a, b) AS (VALUES (1, 'x')) SELECT * FROM "t"`, sql)

	sql, _, err = goqu.Dialect("postgres").From("x").
		InnerJoin(goqu.Values([]any{1}).As("v", "a"), goqu.On(goqu.I("v.a").Eq(goqu.I("x.a")))).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `SELECT * FROM "x" INNER JOIN (VALUES (1)) AS "v"("a") ON ("v"."a" = "x"."a")`, sql)
}

func TestDeleteUsing(t *testing.T) {
	ds := goqu.Dialect("postgres").Delete("a").
		Using("b").
		Where(goqu.I("a.cluster").Eq(goqu.I("b.cluster")), goqu.I("a.cluster").Eq(10)).
		Returning("a.id", "b.val")

	sql, args, err := ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `DELETE FROM "a" USING "b" WHERE (("a"."cluster" = "b"."cluster") AND ("a"."cluster" = 10)) `+
		`RETURNING "a"."id", "b"."val"`, sql)

	sql, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []any{int64(10)}, args)
	assert.Equal(t, `DELETE FROM "a" USING "b" WHERE (("a"."cluster" = "b"."cluster") AND ("a"."cluster" = $1)) `+
		`RETURNING "a"."id", "b"."val"`, sql)
}
