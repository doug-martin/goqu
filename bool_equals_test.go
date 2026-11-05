package goqu_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/exp"
)

type namedBool bool

// UseEqualsForBools writes a comparison with a boolean as = instead of IS (#408)
func TestUseEqualsForBools(t *testing.T) {
	do := goqu.DefaultDialectOptions()
	do.UseEqualsForBools = true
	goqu.RegisterDialect("trino-test", do)
	defer goqu.DeregisterDialect("trino-test")
	d := goqu.Dialect("trino-test")

	t.Run("bools", func(t *testing.T) {
		yes := true
		sql, _, err := d.From("t").Where(
			goqu.C("a").Eq(true),
			goqu.C("b").Neq(true),
			goqu.C("c").IsTrue(),
			goqu.C("d").IsNotFalse(),
			goqu.C("e").Eq(namedBool(false)),
			goqu.C("f").Eq(&yes),
			goqu.C("g").Is(true),
			goqu.C("h").IsNot(false),
		).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `SELECT * FROM "t" WHERE `+
			`(COALESCE(("a" = TRUE), FALSE) AND COALESCE(("b" != TRUE), TRUE) AND COALESCE(("c" = TRUE), FALSE) AND `+
			`COALESCE(("d" != FALSE), TRUE) AND COALESCE(("e" = FALSE), FALSE) AND COALESCE(("f" = TRUE), FALSE) AND `+
			`COALESCE(("g" = TRUE), FALSE) AND COALESCE(("h" != FALSE), TRUE))`, sql)
	})

	// IS NOT TRUE matches NULL rows, a plain != would drop them, so the result must not change
	t.Run("IS NOT keeps matching NULL rows", func(t *testing.T) {
		sql, args, err := d.From("t").Where(goqu.C("a").IsNotTrue()).Prepared(true).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `SELECT * FROM "t" WHERE COALESCE(("a" != ?), TRUE)`, sql)
		require.Equal(t, []any{true}, args)
	})

	// = and != are NULL for a NULL where IS TRUE is false and IS NOT TRUE is true, which shows when the comparison is
	// negated or selected, so it is wrapped to give the same value
	t.Run("the value for a NULL is kept when negated or selected", func(t *testing.T) {
		sql, _, err := d.From("t").Select(goqu.C("a").IsTrue().As("is_true")).
			Where(goqu.L("NOT ?", goqu.C("a").IsTrue())).ToSQL()
		require.NoError(t, err)
		require.Equal(t,
			`SELECT COALESCE(("a" = TRUE), FALSE) AS "is_true" FROM "t" WHERE NOT COALESCE(("a" = TRUE), FALSE)`, sql)
	})

	// the left hand side is written once, so a function that is not deterministic is only called once
	t.Run("the left hand side is written once", func(t *testing.T) {
		sql, _, err := d.From("t").Where(goqu.Func("f", goqu.C("a")).IsNotTrue()).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `SELECT * FROM "t" WHERE COALESCE((f("a") != TRUE), TRUE)`, sql)
	})

	t.Run("Ex maps, OR lists and subqueries", func(t *testing.T) {
		sql, _, err := d.From("t").Where(
			goqu.Ex{"a": true, "b": false},
			goqu.ExOr{"c": goqu.Op{"neq": true}, "d": goqu.Op{"isnot": false}},
		).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `SELECT * FROM "t" WHERE ((COALESCE(("a" = TRUE), FALSE) AND COALESCE(("b" = FALSE), FALSE)) AND `+
			`(COALESCE(("c" != TRUE), TRUE) OR COALESCE(("d" != FALSE), TRUE)))`, sql)

		sql, _, err = d.From("t").Where(goqu.C("x").In(d.From("u").Where(goqu.C("a").IsTrue()))).ToSQL()
		require.NoError(t, err)
		require.Equal(t,
			`SELECT * FROM "t" WHERE ("x" IN (SELECT * FROM "u" WHERE COALESCE(("a" = TRUE), FALSE)))`, sql)
	})

	// the option is read from the dialect that renders the statement, a nested dataset uses the outer dialect
	t.Run("a nested dataset uses the dialect of the outer statement", func(t *testing.T) {
		sql, _, err := goqu.From("t").Where(goqu.C("x").In(d.From("u").Where(goqu.C("a").IsTrue()))).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `SELECT * FROM "t" WHERE ("x" IN (SELECT * FROM "u" WHERE ("a" IS TRUE)))`, sql)
	})

	t.Run("prepared", func(t *testing.T) {
		sql, args, err := d.From("t").Where(goqu.C("a").IsTrue()).Prepared(true).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `SELECT * FROM "t" WHERE COALESCE(("a" = ?), FALSE)`, sql)
		require.Equal(t, []any{true}, args)
	})

	t.Run("null checks and other operators are unchanged", func(t *testing.T) {
		var none *bool
		sql, _, err := d.From("t").Where(
			goqu.C("a").IsNull(),
			goqu.C("b").IsNotNull(),
			goqu.C("c").Eq(nil),
			goqu.C("d").Eq(none),
			goqu.C("e").Is(goqu.L("UNKNOWN")),
			goqu.C("f").Gt(1),
			exp.NewBooleanExpression(exp.IsOp, goqu.C("g"), "x"),
		).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `SELECT * FROM "t" WHERE `+
			`(("a" IS NULL) AND ("b" IS NOT NULL) AND ("c" IS NULL) AND ("d" IS NULL) AND `+
			`("e" IS UNKNOWN) AND ("f" > 1) AND ("g" IS 'x'))`, sql)
	})

	t.Run("off by default", func(t *testing.T) {
		sql, _, err := goqu.From("t").Where(goqu.C("a").IsTrue(), goqu.C("b").Eq(false)).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `SELECT * FROM "t" WHERE (("a" IS TRUE) AND ("b" IS FALSE))`, sql)
	})
}
