package sqlgen

import (
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/doug-martin/goqu/v9/internal/errors"
	"github.com/doug-martin/goqu/v9/internal/sb"
)

type (
	// An adapter interface to be used by a Dataset to generate SQL for a specific dialect.
	// See DefaultAdapter for a concrete implementation and examples.
	DeleteSQLGenerator interface {
		Dialect() string
		Generate(b sb.SQLBuilder, clauses exp.DeleteClauses)
	}
	// The default adapter. This class should be used when building a new adapter. When creating a new adapter you can
	// either override methods, or more typically update default values.
	// See (github.com/doug-martin/goqu/adapters/postgres)
	deleteSQLGenerator struct {
		*commonSQLGenerator
	}
)

var (
	errNoSourceForDelete = errors.New("no source found when generating delete sql")
)

func NewDeleteSQLGenerator(dialect string, do *SQLDialectOptions) DeleteSQLGenerator {
	return &deleteSQLGenerator{newCommonSQLGenerator(dialect, do)}
}

func (dsg *deleteSQLGenerator) Dialect() string {
	return dsg.dialect
}

func (dsg *deleteSQLGenerator) Generate(b sb.SQLBuilder, clauses exp.DeleteClauses) {
	if !clauses.HasFrom() {
		b.SetError(errNoSourceForDelete)
		return
	}
	for _, f := range dsg.dialectOptions.DeleteSQLOrder {
		if b.Error() != nil {
			return
		}
		switch f {
		case CommonTableSQLFragment:
			dsg.esg.Generate(b, clauses.CommonTables())
		case DeleteBeginSQLFragment:
			dsg.DeleteBeginSQL(b)
		case FromSQLFragment:
			dsg.FromSQL(b, exp.NewColumnListExpression(clauses.From()))
		case WhereSQLFragment:
			dsg.WhereSQL(b, clauses.Where())
		case OrderSQLFragment:
			if dsg.dialectOptions.SupportsOrderByOnDelete {
				dsg.OrderSQL(b, clauses.Order())
			}
		case LimitSQLFragment:
			if dsg.dialectOptions.SupportsLimitOnDelete {
				dsg.LimitSQL(b, clauses.Limit())
			}
		case ReturningSQLFragment:
			dsg.ReturningSQL(b, clauses.Returning())
		default:
			b.SetError(errNotSupportedFragment("DELETE", f))
		}
	}
}

// Adds the correct fragment to being an DELETE statement
func (dsg *deleteSQLGenerator) DeleteBeginSQL(b sb.SQLBuilder) {
	b.Write(dsg.dialectOptions.DeleteClause)
}
