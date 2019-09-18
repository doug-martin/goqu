package sqlgen

import (
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/doug-martin/goqu/v9/internal/errors"
	"github.com/doug-martin/goqu/v9/internal/sb"
)

type (
	// An adapter interface to be used by a Dataset to generate SQL for a specific dialect.
	// See DefaultAdapter for a concrete implementation and examples.
	SelectSQLGenerator interface {
		Dialect() string
		Generate(b sb.SQLBuilder, clauses exp.SelectClauses)
	}
	// The default adapter. This class should be used when building a new adapter. When creating a new adapter you can
	// either override methods, or more typically update default values.
	// See (github.com/doug-martin/goqu/adapters/postgres)
	selectSQLGenerator struct {
		*commonSQLGenerator
	}
)

func errNotSupportedJoinType(j exp.JoinExpression) error {
	return errors.New("dialect does not support %v", j.JoinType())
}

func errJoinConditionRequired(j exp.JoinExpression) error {
	return errors.New("join condition required for conditioned join %v", j.JoinType())
}
func errDistinctOnNotSupported(dialect string) error {
	return errors.New("dialect does not support DISTINCT ON clause [dialect=%s]", dialect)
}

func errWindowNotSupported(dialect string) error {
	return errors.New("dialect does not support WINDOW clause [dialect=%s]", dialect)
}

var errNoWindowName = errors.New("window expresion has no valid name")

func NewSelectSQLGenerator(dialect string, do *SQLDialectOptions) SelectSQLGenerator {
	return &selectSQLGenerator{newCommonSQLGenerator(dialect, do)}
}

func (ssg *selectSQLGenerator) Dialect() string {
	return ssg.dialect
}

func (ssg *selectSQLGenerator) Generate(b sb.SQLBuilder, clauses exp.SelectClauses) {
	for _, f := range ssg.dialectOptions.SelectSQLOrder {
		if b.Error() != nil {
			return
		}
		switch f {
		case CommonTableSQLFragment:
			ssg.esg.Generate(b, clauses.CommonTables())
		case SelectSQLFragment:
			ssg.SelectSQL(b, clauses)
		case FromSQLFragment:
			ssg.FromSQL(b, clauses.From())
		case JoinSQLFragment:
			ssg.JoinSQL(b, clauses.Joins())
		case WhereSQLFragment:
			ssg.WhereSQL(b, clauses.Where())
		case GroupBySQLFragment:
			ssg.GroupBySQL(b, clauses.GroupBy())
		case HavingSQLFragment:
			ssg.HavingSQL(b, clauses.Having())
		case WindowSQLFragment:
			ssg.WindowSQL(b, clauses.Windows())
		case CompoundsSQLFragment:
			ssg.CompoundsSQL(b, clauses.Compounds())
		case OrderSQLFragment:
			ssg.OrderSQL(b, clauses.Order())
		case LimitSQLFragment:
			ssg.LimitSQL(b, clauses.Limit())
		case OffsetSQLFragment:
			ssg.OffsetSQL(b, clauses.Offset())
		case ForSQLFragment:
			ssg.ForSQL(b, clauses.Lock())
		default:
			b.SetError(errNotSupportedFragment("SELECT", f))
		}
	}
}

// Adds the SELECT clause and columns to a sql statement
func (ssg *selectSQLGenerator) SelectSQL(b sb.SQLBuilder, clauses exp.SelectClauses) {
	b.Write(ssg.dialectOptions.SelectClause).
		WriteRunes(ssg.dialectOptions.SpaceRune)
	dc := clauses.Distinct()
	if dc != nil {
		b.Write(ssg.dialectOptions.DistinctFragment)
		if !dc.IsEmpty() {
			if ssg.dialectOptions.SupportsDistinctOn {
				b.Write(ssg.dialectOptions.OnFragment).WriteRunes(ssg.dialectOptions.LeftParenRune)
				ssg.esg.Generate(b, dc)
				b.WriteRunes(ssg.dialectOptions.RightParenRune, ssg.dialectOptions.SpaceRune)
			} else {
				b.SetError(errDistinctOnNotSupported(ssg.dialect))
				return
			}
		} else {
			b.WriteRunes(ssg.dialectOptions.SpaceRune)
		}
	}
	cols := clauses.Select()
	if clauses.IsDefaultSelect() || len(cols.Columns()) == 0 {
		b.WriteRunes(ssg.dialectOptions.StarRune)
	} else {
		ssg.esg.Generate(b, cols)
	}
}

// Generates the JOIN clauses for an SQL statement
func (ssg *selectSQLGenerator) JoinSQL(b sb.SQLBuilder, joins exp.JoinExpressions) {
	if len(joins) > 0 {
		for _, j := range joins {
			joinType, ok := ssg.dialectOptions.JoinTypeLookup[j.JoinType()]
			if !ok {
				b.SetError(errNotSupportedJoinType(j))
				return
			}
			b.Write(joinType)
			ssg.esg.Generate(b, j.Table())
			if t, ok := j.(exp.ConditionedJoinExpression); ok {
				if t.IsConditionEmpty() {
					b.SetError(errJoinConditionRequired(j))
					return
				}
				ssg.joinConditionSQL(b, t.Condition())
			}
		}
	}
}

// Generates the GROUP BY clause for an SQL statement
func (ssg *selectSQLGenerator) GroupBySQL(b sb.SQLBuilder, groupBy exp.ColumnListExpression) {
	if groupBy != nil && len(groupBy.Columns()) > 0 {
		b.Write(ssg.dialectOptions.GroupByFragment)
		ssg.esg.Generate(b, groupBy)
	}
}

// Generates the HAVING clause for an SQL statement
func (ssg *selectSQLGenerator) HavingSQL(b sb.SQLBuilder, having exp.ExpressionList) {
	if having != nil && len(having.Expressions()) > 0 {
		b.Write(ssg.dialectOptions.HavingFragment)
		ssg.esg.Generate(b, having)
	}
}

// Generates the OFFSET clause for an SQL statement
func (ssg *selectSQLGenerator) OffsetSQL(b sb.SQLBuilder, offset uint) {
	if offset > 0 {
		b.Write(ssg.dialectOptions.OffsetFragment)
		ssg.esg.Generate(b, offset)
	}
}

// Generates the compound sql clause for an SQL statement (e.g. UNION, INTERSECT)
func (ssg *selectSQLGenerator) CompoundsSQL(b sb.SQLBuilder, compounds []exp.CompoundExpression) {
	for _, compound := range compounds {
		ssg.esg.Generate(b, compound)
	}
}

// Generates the FOR (aka "locking") clause for an SQL statement
func (ssg *selectSQLGenerator) ForSQL(b sb.SQLBuilder, lockingClause exp.Lock) {
	if lockingClause == nil {
		return
	}
	switch lockingClause.Strength() {
	case exp.ForNolock:
		return
	case exp.ForUpdate:
		b.Write(ssg.dialectOptions.ForUpdateFragment)
	case exp.ForNoKeyUpdate:
		b.Write(ssg.dialectOptions.ForNoKeyUpdateFragment)
	case exp.ForShare:
		b.Write(ssg.dialectOptions.ForShareFragment)
	case exp.ForKeyShare:
		b.Write(ssg.dialectOptions.ForKeyShareFragment)
	}
	// the WAIT case is the default in Postgres, and is what you get if you don't specify NOWAIT or
	// SKIP LOCKED.  There's no special syntax for it in PG, so we don't do anything for it here
	switch lockingClause.WaitOption() {
	case exp.NoWait:
		b.Write(ssg.dialectOptions.NowaitFragment)
	case exp.SkipLocked:
		b.Write(ssg.dialectOptions.SkipLockedFragment)
	}
}

func (ssg *selectSQLGenerator) WindowSQL(b sb.SQLBuilder, windows []exp.WindowExpression) {
	weLen := len(windows)
	if weLen == 0 {
		return
	}
	if !ssg.dialectOptions.SupportsWindowFunction {
		b.SetError(errWindowNotSupported(ssg.dialect))
		return
	}
	b.Write(ssg.dialectOptions.WindowFragment)
	for i, we := range windows {
		if !we.HasName() {
			b.SetError(errNoWindowName)
		}
		ssg.esg.Generate(b, we)
		if i < weLen-1 {
			b.WriteRunes(ssg.dialectOptions.CommaRune, ssg.dialectOptions.SpaceRune)
		}
	}
}

func (ssg *selectSQLGenerator) joinConditionSQL(b sb.SQLBuilder, jc exp.JoinCondition) {
	switch t := jc.(type) {
	case exp.JoinOnCondition:
		ssg.joinOnConditionSQL(b, t)
	case exp.JoinUsingCondition:
		ssg.joinUsingConditionSQL(b, t)
	}
}

func (ssg *selectSQLGenerator) joinUsingConditionSQL(b sb.SQLBuilder, jc exp.JoinUsingCondition) {
	b.Write(ssg.dialectOptions.UsingFragment).
		WriteRunes(ssg.dialectOptions.LeftParenRune)
	ssg.esg.Generate(b, jc.Using())
	b.WriteRunes(ssg.dialectOptions.RightParenRune)
}

func (ssg *selectSQLGenerator) joinOnConditionSQL(b sb.SQLBuilder, jc exp.JoinOnCondition) {
	b.Write(ssg.dialectOptions.OnFragment)
	ssg.esg.Generate(b, jc.On())
}
