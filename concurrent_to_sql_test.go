package goqu_test

import (
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/mysql"
	_ "github.com/doug-martin/goqu/v9/dialect/postgres"
	"github.com/stretchr/testify/assert"
)

// Datasets of the same dialect share one SQL generator and builders are pooled, so generating SQL from many
// goroutines must never mix up the values of different statements. Run with -race.
func TestToSQL_isSafeForConcurrentUse(t *testing.T) {
	for _, dialect := range []string{"postgres", "mysql"} {
		t.Run(dialect, func(t *testing.T) {
			d := goqu.Dialect(dialect)
			q := func(s string) string {
				if dialect == "mysql" {
					return "`" + s + "`"
				}
				return `"` + s + `"`
			}
			var wg sync.WaitGroup
			for g := 0; g < 16; g++ {
				wg.Add(1)
				go func(g int) {
					defer wg.Done()
					for i := 0; i < 1000; i++ {
						n := int64(g*1_000_000 + i)
						f := float64(n) + 0.5

						sql, args, err := d.From("t").Where(goqu.C("a").Eq(n), goqu.C("b").Eq(f)).ToSQL()
						want := fmt.Sprintf("SELECT * FROM %s WHERE ((%s = %d) AND (%s = %s))",
							q("t"), q("a"), n, q("b"), strconv.FormatFloat(f, 'f', -1, 64))
						if !assert.NoError(t, err) || !assert.Equal(t, want, sql) || !assert.Empty(t, args) {
							return
						}

						psql, pargs, err := d.From("t").Prepared(true).
							Where(goqu.C("a").In([]int64{n, n + 1}), goqu.C("b").Eq(f)).ToSQL()
						if !assert.NoError(t, err) || !assert.Equal(t, []any{n, n + 1, f}, pargs) {
							t.Logf("sql: %s", psql)
							return
						}
					}
				}(g)
			}
			wg.Wait()
		})
	}
}
