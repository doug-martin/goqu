package goqu_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/sqlgen"
)

// a dialect without LIMIT or TOP (e.g. Oracle 19c) can only paginate with OFFSET ... ROWS FETCH ... ROWS ONLY (#429)
func TestAlwaysOffsetFetch(t *testing.T) {
	newOpts := func(always bool) *sqlgen.SQLDialectOptions {
		o := sqlgen.DefaultDialectOptions()
		o.FetchFragment = []byte(" FETCH NEXT ")
		o.AlwaysOffsetFetch = always
		// same construction as the docs: the default order with the order fragment swapped
		order := make([]sqlgen.SQLFragmentType, 0, len(o.SelectSQLOrder))
		for _, f := range o.SelectSQLOrder {
			switch f {
			case sqlgen.OrderSQLFragment:
				order = append(order, sqlgen.OrderWithOffsetFetchSQLFragment)
			case sqlgen.LimitSQLFragment, sqlgen.OffsetSQLFragment:
			default:
				order = append(order, f)
			}
		}
		o.SelectSQLOrder = order
		return o
	}
	goqu.RegisterDialect("test-always-fetch", newOpts(true))
	goqu.RegisterDialect("test-default-fetch", newOpts(false))
	defer goqu.DeregisterDialect("test-always-fetch")
	defer goqu.DeregisterDialect("test-default-fetch")

	ord := goqu.C("a").Asc()
	cases := []struct {
		name         string
		build        func(d goqu.DialectWrapper) *goqu.SelectDataset
		always, base string
	}{
		{
			"limit, no offset, ordered",
			func(d goqu.DialectWrapper) *goqu.SelectDataset { return d.From("t").Order(ord).Limit(5) },
			`SELECT * FROM "t" ORDER BY "a" ASC OFFSET 0 ROWS FETCH NEXT 5 ROWS ONLY`,
			`SELECT * FROM "t" ORDER BY "a" ASC`,
		},
		{
			"limit, no offset, unordered",
			func(d goqu.DialectWrapper) *goqu.SelectDataset { return d.From("t").Limit(5) },
			`SELECT * FROM "t" OFFSET 0 ROWS FETCH NEXT 5 ROWS ONLY`,
			`SELECT * FROM "t"`,
		},
		{
			"limit and offset, ordered",
			func(d goqu.DialectWrapper) *goqu.SelectDataset { return d.From("t").Order(ord).Limit(5).Offset(3) },
			`SELECT * FROM "t" ORDER BY "a" ASC OFFSET 3 ROWS FETCH NEXT 5 ROWS ONLY`,
			`SELECT * FROM "t" ORDER BY "a" ASC OFFSET 3 ROWS FETCH NEXT 5 ROWS ONLY`,
		},
		{
			"offset only, unordered",
			func(d goqu.DialectWrapper) *goqu.SelectDataset { return d.From("t").Offset(3) },
			`SELECT * FROM "t" OFFSET 3 ROWS`,
			`SELECT * FROM "t"`,
		},
		{
			"keeps join, group by and having",
			func(d goqu.DialectWrapper) *goqu.SelectDataset {
				return d.From("t").Join(goqu.T("u"), goqu.On(goqu.I("t.a").Eq(goqu.I("u.a")))).
					Select(goqu.COUNT("*")).GroupBy("a").Having(goqu.COUNT("*").Gt(1)).Order(ord).Limit(5)
			},
			`SELECT COUNT(*) FROM "t" INNER JOIN "u" ON ("t"."a" = "u"."a") GROUP BY "a" HAVING (COUNT(*) > 1) ` +
				`ORDER BY "a" ASC OFFSET 0 ROWS FETCH NEXT 5 ROWS ONLY`,
			`SELECT COUNT(*) FROM "t" INNER JOIN "u" ON ("t"."a" = "u"."a") GROUP BY "a" HAVING (COUNT(*) > 1) ORDER BY "a" ASC`,
		},
		{
			"neither limit nor offset",
			func(d goqu.DialectWrapper) *goqu.SelectDataset { return d.From("t").Order(ord) },
			`SELECT * FROM "t" ORDER BY "a" ASC`,
			`SELECT * FROM "t" ORDER BY "a" ASC`,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sql, _, err := c.build(goqu.Dialect("test-always-fetch")).ToSQL()
			require.NoError(t, err)
			require.Equal(t, c.always, sql)

			// the default (option off) output is unchanged
			sql, _, err = c.build(goqu.Dialect("test-default-fetch")).ToSQL()
			require.NoError(t, err)
			require.Equal(t, c.base, sql)
		})
	}

	t.Run("prepared", func(t *testing.T) {
		sql, args, err := goqu.Dialect("test-always-fetch").From("t").Limit(5).Prepared(true).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `SELECT * FROM "t" OFFSET ? ROWS FETCH NEXT ? ROWS ONLY`, sql)
		require.Equal(t, []any{int64(0), int64(5)}, args)
	})
}
