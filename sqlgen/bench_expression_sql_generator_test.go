package sqlgen_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/doug-martin/goqu/v9/exp"
	"github.com/doug-martin/goqu/v9/internal/sb"
	"github.com/doug-martin/goqu/v9/sqlgen"
)

func newESG(opts *sqlgen.SQLDialectOptions, prepared bool) (sqlgen.ExpressionSQLGenerator, sb.SQLBuilder) {
	return sqlgen.NewExpressionSQLGenerator("bench", opts), sb.NewSQLBuilder(prepared)
}

// --- INSERT: single row, many columns; and multi-row bulk ---

func BenchmarkExpressionSQLGenerator_Insert(b *testing.B) {
	opts := sqlgen.DefaultDialectOptions()
	optsNums := sqlgen.DefaultDialectOptions()
	optsNums.IncludePlaceholderNum = true
	optsNums.PlaceHolderFragment = []byte("$")

	columns := 128
	rows := 32

	// Precompute identifiers and sample values
	colNames := make([]interface{}, columns)
	for i := 0; i < columns; i++ {
		colNames[i] = fmt.Sprintf("c_%03d", i)
	}
	colList := exp.NewColumnListExpression(colNames...)
	// Construct one row worth of mixed values
	rowVals := make([]interface{}, columns)
	for i := 0; i < columns; i++ {
		switch i % 5 {
		case 0:
			rowVals[i] = i
		case 1:
			rowVals[i] = fmt.Sprintf("val_%d", i)
		case 2:
			rowVals[i] = (i%2 == 0)
		case 3:
			rowVals[i] = time.Unix(int64(1_700_000_000+i), 0).UTC()
		default:
			rowVals[i] = []byte("xyz")
		}
	}
	// Precompute multiple rows as copies of rowVals
	rowsVals := make([][]interface{}, rows)
	for r := 0; r < rows; r++ {
		rowsVals[r] = rowVals
	}

	types := []struct {
		name     string
		prepared bool
		opts     *sqlgen.SQLDialectOptions
	}{
		{"Unprepared", false, opts},
		{"Prepared", true, opts},
		{"PreparedWithNums", true, optsNums},
	}

	for _, tt := range types {
		b.Run(tt.name+"/SingleRowManyCols", func(b *testing.B) {
			esg, _ := newESG(tt.opts, tt.prepared)
			ident := exp.NewIdentifierExpression("", "bench_insert", "")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, builder := newESG(tt.opts, tt.prepared)
				builder.WriteStrings("INSERT INTO ")
				esg.Generate(builder, ident)
				builder.WriteStrings(" (")
				esg.Generate(builder, colList)
				builder.WriteStrings(") VALUES ")
				esg.Generate(builder, rowVals)
				if _, _, err := builder.ToSQL(); err != nil {
					b.Fatalf("ToSQL error: %v", err)
				}
			}
		})

		b.Run(tt.name+"/MultiRowBulk", func(b *testing.B) {
			esg, _ := newESG(tt.opts, tt.prepared)
			ident := exp.NewIdentifierExpression("", "bench_insert_bulk", "")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, builder := newESG(tt.opts, tt.prepared)
				builder.WriteStrings("INSERT INTO ")
				esg.Generate(builder, ident)
				builder.WriteStrings(" (")
				esg.Generate(builder, colList)
				builder.WriteStrings(") VALUES ")
				for r := 0; r < rows; r++ {
					esg.Generate(builder, rowsVals[r])
					if r < rows-1 {
						builder.WriteRunes(',', ' ')
					}
				}
				if _, _, err := builder.ToSQL(); err != nil {
					b.Fatalf("ToSQL error: %v", err)
				}
				sb.ReleaseSQLBuilder(builder)
			}
		})
	}
}

// --- UPDATE: many columns with complex WHERE ---

func BenchmarkExpressionSQLGenerator_Update(b *testing.B) {
	opts := sqlgen.DefaultDialectOptions()
	optsNums := sqlgen.DefaultDialectOptions()
	optsNums.IncludePlaceholderNum = true
	optsNums.PlaceHolderFragment = []byte("$")

	setCols := 64
	inSize := 256

	// Precompute column identifiers and values
	setExprs := make([]exp.UpdateExpression, setCols)
	for i := 0; i < setCols; i++ {
		col := exp.NewIdentifierExpression("", "", fmt.Sprintf("c_%03d", i))
		setExprs[i] = col.Set(i)
	}
	// WHERE: a big AND of equality and IN clauses
	andExprs := make([]exp.Expression, 0, 64)
	for i := 0; i < 48; i++ {
		col := exp.NewIdentifierExpression("", "", fmt.Sprintf("w_%03d", i))
		andExprs = append(andExprs, col.Eq(i))
	}
	// Large IN clauses
	values := make([]int64, inSize)
	for i := range values {
		values[i] = int64(i)
	}
	andExprs = append(andExprs,
		exp.NewIdentifierExpression("", "", "user_id").In(values),
		exp.NewIdentifierExpression("", "", "status").In([]string{"new", "ok", "hold", "done"}),
	)
	where := exp.NewExpressionList(exp.AndType, andExprs...)

	types := []struct {
		name     string
		prepared bool
		opts     *sqlgen.SQLDialectOptions
	}{
		{"Unprepared", false, opts},
		{"Prepared", true, opts},
		{"PreparedWithNums", true, optsNums},
	}

	for _, tt := range types {
		b.Run(tt.name, func(b *testing.B) {
			esg, _ := newESG(tt.opts, tt.prepared)
			ident := exp.NewIdentifierExpression("", "bench_update", "")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, builder := newESG(tt.opts, tt.prepared)
				builder.WriteStrings("UPDATE ")
				esg.Generate(builder, ident)
				builder.WriteStrings(" SET ")
				for j, se := range setExprs {
					esg.Generate(builder, se)
					if j < len(setExprs)-1 {
						builder.WriteRunes(',', ' ')
					}
				}
				builder.WriteStrings(" WHERE ")
				esg.Generate(builder, where)
				if _, _, err := builder.ToSQL(); err != nil {
					b.Fatalf("ToSQL error: %v", err)
				}
			}
		})
	}
}

// --- SELECT: large WHERE with multiple IN and LIKEs ---

func BenchmarkExpressionSQLGenerator_Select(b *testing.B) {
	opts := sqlgen.DefaultDialectOptions()
	optsNums := sqlgen.DefaultDialectOptions()
	optsNums.IncludePlaceholderNum = true
	optsNums.PlaceHolderFragment = []byte("$")

	cols := 32
	preds := 200
	inSize := 512

	colNames := make([]interface{}, cols)
	for i := 0; i < cols; i++ {
		colNames[i] = fmt.Sprintf("c_%03d", i)
	}
	colList := exp.NewColumnListExpression(colNames...)

	// WHERE predicates
	ps := make([]exp.Expression, 0, preds)
	for i := 0; i < preds-3; i++ {
		c := exp.NewIdentifierExpression("", "", fmt.Sprintf("p_%03d", i))
		if i%10 == 0 {
			ps = append(ps, c.Like("prefix%"))
		} else {
			ps = append(ps, c.Eq(i))
		}
	}
	bigIn := make([]int64, inSize)
	for i := range bigIn {
		bigIn[i] = int64(i)
	}
	ps = append(ps,
		exp.NewIdentifierExpression("", "", "id").In(bigIn),
		exp.NewIdentifierExpression("", "", "kind").In([]string{"a", "b", "c", "d"}),
		exp.NewIdentifierExpression("", "", "flag").Eq(true),
	)
	where := exp.NewExpressionList(exp.AndType, ps...)

	types := []struct {
		name     string
		prepared bool
		opts     *sqlgen.SQLDialectOptions
	}{
		{"Unprepared", false, opts},
		{"Prepared", true, opts},
		{"PreparedWithNums", true, optsNums},
	}

	for _, tt := range types {
		b.Run(tt.name, func(b *testing.B) {
			esg, _ := newESG(tt.opts, tt.prepared)
			tbl := exp.NewIdentifierExpression("", "bench_select", "")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, builder := newESG(tt.opts, tt.prepared)
				builder.WriteStrings("SELECT ")
				esg.Generate(builder, colList)
				builder.WriteStrings(" FROM ")
				esg.Generate(builder, tbl)
				builder.WriteStrings(" WHERE ")
				esg.Generate(builder, where)
				if _, _, err := builder.ToSQL(); err != nil {
					b.Fatalf("ToSQL error: %v", err)
				}
			}
		})
	}
}
