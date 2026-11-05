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
			`(("a" = TRUE) AND (("b" != TRUE) OR ("b" IS NULL)) AND ("c" = TRUE) AND `+
			`(("d" != FALSE) OR ("d" IS NULL)) AND ("e" = FALSE) AND ("f" = TRUE) AND ("g" = TRUE) AND `+
			`(("h" != FALSE) OR ("h" IS NULL)))`, sql)
	})

	// IS NOT TRUE matches NULL rows, a plain != would drop them, so the result must not change
	t.Run("IS NOT keeps matching NULL rows", func(t *testing.T) {
		sql, args, err := d.From("t").Where(goqu.C("a").IsNotTrue()).Prepared(true).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `SELECT * FROM "t" WHERE (("a" != ?) OR ("a" IS NULL))`, sql)
		require.Equal(t, []any{true}, args)
	})

	t.Run("Ex maps, OR lists and subqueries", func(t *testing.T) {
		sql, _, err := d.From("t").Where(
			goqu.Ex{"a": true, "b": false},
			goqu.ExOr{"c": goqu.Op{"neq": true}, "d": goqu.Op{"isnot": false}},
		).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `SELECT * FROM "t" WHERE ((("a" = TRUE) AND ("b" = FALSE)) AND `+
			`((("c" != TRUE) OR ("c" IS NULL)) OR (("d" != FALSE) OR ("d" IS NULL))))`, sql)

		sql, _, err = d.From("t").Where(goqu.C("x").In(d.From("u").Where(goqu.C("a").IsTrue()))).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `SELECT * FROM "t" WHERE ("x" IN (SELECT * FROM "u" WHERE ("a" = TRUE)))`, sql)
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
		require.Equal(t, `SELECT * FROM "t" WHERE ("a" = ?)`, sql)
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
