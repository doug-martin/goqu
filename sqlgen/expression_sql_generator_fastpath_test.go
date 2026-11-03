package sqlgen_test

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/doug-martin/goqu/v9/exp"
	"github.com/doug-martin/goqu/v9/internal/sb"
	"github.com/doug-martin/goqu/v9/sqlgen"
	"github.com/stretchr/testify/assert"
)

type (
	// Named slice types are not matched by the typed fast paths, so they exercise the reflection path.
	namedInt64s   []int64
	namedInts     []int
	namedStrings  []string
	namedFloats   []float64
	namedBools    []bool
	namedTimes    []time.Time
	namedAnys     []any
	fastPathCases struct {
		name  string
		fast  any
		slow  any
		count int
	}
)

func fastPathGenerators() map[string]*sqlgen.SQLDialectOptions {
	numbered := sqlgen.DefaultDialectOptions()
	numbered.PlaceHolderFragment = []byte("$")
	numbered.IncludePlaceholderNum = true
	return map[string]*sqlgen.SQLDialectOptions{
		"default":  sqlgen.DefaultDialectOptions(),
		"numbered": numbered,
	}
}

// The typed slice fast paths must generate exactly what the reflection path generates, for interpolated and prepared
// statements, with and without numbered placeholders.
func TestExpressionSQLGenerator_typedSlicesMatchReflection(t *testing.T) {
	ts := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	cases := []fastPathCases{
		{"int64", []int64{1, -2, 300}, namedInt64s{1, -2, 300}, 3},
		{"int", []int{1, -2, 300}, namedInts{1, -2, 300}, 3},
		{"string", []string{"a", "it's", ""}, namedStrings{"a", "it's", ""}, 3},
		{"float64", []float64{1.5, -2, 14000000.5}, namedFloats{1.5, -2, 14000000.5}, 3},
		{"bool", []bool{true, false}, namedBools{true, false}, 2},
		{"time", []time.Time{ts, ts.Add(time.Hour)}, namedTimes{ts, ts.Add(time.Hour)}, 2},
		{"any", []any{1, "a", nil, 2.5, true}, namedAnys{1, "a", nil, 2.5, true}, 5},
		{"empty int64", []int64{}, namedInt64s{}, 0},
		{"empty string", []string{}, namedStrings{}, 0},
		{"empty any", []any{}, namedAnys{}, 0},
		{"nil string", []string(nil), namedStrings(nil), 0},
		{"nested", []any{[]int{1, 2}, []string{"a"}}, namedAnys{namedInts{1, 2}, namedStrings{"a"}}, 3},
	}
	for optName, opts := range fastPathGenerators() {
		for _, c := range cases {
			for _, prepared := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/prepared=%v", optName, c.name, prepared), func(t *testing.T) {
					esg := sqlgen.NewExpressionSQLGenerator("test", opts)

					fastB := sb.NewSQLBuilder(prepared)
					esg.Generate(fastB, c.fast)
					fastSQL, fastArgs, fastErr := fastB.ToSQL()

					slowB := sb.NewSQLBuilder(prepared)
					esg.Generate(slowB, c.slow)
					slowSQL, slowArgs, slowErr := slowB.ToSQL()

					assert.NoError(t, fastErr)
					assert.NoError(t, slowErr)
					assert.Equal(t, slowSQL, fastSQL)
					assert.Equal(t, slowArgs, fastArgs)
					if prepared {
						assert.Len(t, fastArgs, c.count)
					}
				})
			}
		}
	}
}

func TestExpressionSQLGenerator_bytesAndCTEFastPaths(t *testing.T) {
	esg := sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions())

	b := sb.NewSQLBuilder(false)
	esg.Generate(b, []byte("abc"))
	sql, _, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `'abc'`, sql)

	b = sb.NewSQLBuilder(true)
	esg.Generate(b, []byte("abc"))
	sql, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `?`, sql)
	assert.Equal(t, []any{[]byte("abc")}, args)
}

// Literal expressions are split on '?' bytewise for valid UTF-8; invalid UTF-8 keeps its historical handling
// (invalid bytes become U+FFFD).
func TestExpressionSQLGenerator_literalExpressionText(t *testing.T) {
	esg := sqlgen.NewExpressionSQLGenerator("test", sqlgen.DefaultDialectOptions())
	gen := func(l string, args ...any) string {
		b := sb.NewSQLBuilder(false)
		esg.Generate(b, exp.NewLiteralExpression(l, args...))
		sql, _, err := b.ToSQL()
		assert.NoError(t, err)
		return sql
	}
	assert.Equal(t, `a = 1 AND b = 'x'`, gen(`a = ? AND b = ?`, 1, "x"))
	assert.Equal(t, `1? ?`, gen(`?? ?`, 1), "arguments are consumed left to right, extra ? are kept")
	assert.Equal(t, `1 ? ?`, gen(`? ? ?`, 1), "once the arguments run out ? is written as is")
	assert.Equal(t, `日本語 = 1 ü`, gen(`日本語 = ? ü`, 1), "multi byte text around placeholders is kept")
	assert.Equal(t, `1`, gen(`?`, 1))
	assert.Equal(t, `no placeholders`, gen(`no placeholders`, 1))
	assert.Equal(t, "a\uFFFD = 1", gen("a\xff = ?", 1), "invalid UTF-8 is replaced like before")
}

// Generators are shared by every dataset of a dialect, so they must not hold per call state. Run with -race.
func TestExpressionSQLGenerator_isSafeForConcurrentUse(t *testing.T) {
	numbered := sqlgen.DefaultDialectOptions()
	numbered.PlaceHolderFragment = []byte("$")
	numbered.IncludePlaceholderNum = true
	esg := sqlgen.NewExpressionSQLGenerator("test", numbered)

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				n := int64(g*1_000_000 + i)
				f := float64(n) + 0.5

				b := sb.NewSQLBuilder(false)
				esg.Generate(b, exp.NewLiteralExpression("? AND ? AND ?", n, f, []int64{n, n + 1}))
				sql, _, err := b.ToSQL()
				want := fmt.Sprintf("%d AND %s AND (%d, %d)", n, strconv.FormatFloat(f, 'f', -1, 64), n, n+1)
				if err != nil || sql != want {
					t.Errorf("unprepared: got %q, want %q (err=%v)", sql, want, err)
					return
				}

				pb := sb.NewSQLBuilder(true)
				esg.Generate(pb, exp.NewLiteralExpression("? AND ?", n, []string{"x", "y"}))
				psql, args, err := pb.ToSQL()
				if err != nil || psql != "$1 AND ($2, $3)" || len(args) != 3 || args[0] != n {
					t.Errorf("prepared: got %q %v, want $1 AND ($2, $3) (err=%v)", psql, args, err)
					return
				}
				sb.ReleaseSQLBuilder(b)
				sb.ReleaseSQLBuilder(pb)
			}
		}(g)
	}
	wg.Wait()
}
