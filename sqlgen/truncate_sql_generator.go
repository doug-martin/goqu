package sqlgen

import (
	"strings"

	"github.com/doug-martin/goqu/v9/exp"
	"github.com/doug-martin/goqu/v9/internal/errors"
	"github.com/doug-martin/goqu/v9/internal/sb"
)

type (
	// An adapter interface to be used by a Dataset to generate SQL for a specific dialect.
	// See DefaultAdapter for a concrete implementation and examples.
	TruncateSQLGenerator interface {
		Dialect() string
		Generate(b sb.SQLBuilder, clauses exp.TruncateClauses)
	}
	// The default adapter. This class should be used when building a new adapter. When creating a new adapter you can
	// either override methods, or more typically update default values.
	// See (github.com/doug-martin/goqu/adapters/postgres)
	truncateSQLGenerator struct {
		*commonSQLGenerator
	}
)

var errNoSourceForTruncate = errors.New("no source found when generating truncate sql")

func NewTruncateSQLGenerator(dialect string, do *SQLDialectOptions) TruncateSQLGenerator {
	return &truncateSQLGenerator{newCommonSQLGenerator(dialect, do)}
}

func (tsg *truncateSQLGenerator) Dialect() string {
	return tsg.dialect
}

func (tsg *truncateSQLGenerator) Generate(b sb.SQLBuilder, clauses exp.TruncateClauses) {
	if !clauses.HasTable() {
		b.SetError(errNoSourceForTruncate)
		return
	}
	for _, f := range tsg.dialectOptions.TruncateSQLOrder {
		if b.Error() != nil {
			return
		}
		switch f {
		case TruncateSQLFragment:
			tsg.TruncateSQL(b, clauses.Table(), clauses.Options())
		default:
			b.SetError(errNotSupportedFragment("TRUNCATE", f))
		}
	}
}

// Generates a TRUNCATE statement
func (tsg *truncateSQLGenerator) TruncateSQL(b sb.SQLBuilder, from exp.ColumnListExpression, opts exp.TruncateOptions) {
	b.Write(tsg.dialectOptions.TruncateClause)
	tsg.SourcesSQL(b, from)
	if opts.Identity != tsg.dialectOptions.EmptyString {
		b.WriteRunes(tsg.dialectOptions.SpaceRune).
			WriteStrings(strings.ToUpper(opts.Identity)).
			Write(tsg.dialectOptions.IdentityFragment)
	}
	if opts.Cascade {
		b.Write(tsg.dialectOptions.CascadeFragment)
	} else if opts.Restrict {
		b.Write(tsg.dialectOptions.RestrictFragment)
	}
}
