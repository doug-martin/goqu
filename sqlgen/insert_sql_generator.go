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
	InsertSQLGenerator interface {
		Dialect() string
		Generate(b sb.SQLBuilder, clauses exp.InsertClauses)
	}
	// The default adapter. This class should be used when building a new adapter. When creating a new adapter you can
	// either override methods, or more typically update default values.
	// See (github.com/doug-martin/goqu/dialect/postgres)
	insertSQLGenerator struct {
		CommonSQLGenerator
	}
)

var (
	ErrConflictUpdateValuesRequired     = errors.New("values are required for on conflict update expression")
	ErrNoSourceForInsert                = errors.New("no source found when generating insert sql")
	defaultConflictOnConstraintFragment = []byte(" ON CONSTRAINT ")
)

func errMisMatchedRowLength(expectedL, actualL int) error {
	return errors.New("rows with different value length expected %d got %d", expectedL, actualL)
}

func errUpsertWithWhereNotSupported(dialect string) error {
	return errors.New("dialect does not support upsert with where clause [dialect=%s]", dialect)
}

func NewInsertSQLGenerator(dialect string, do *SQLDialectOptions) InsertSQLGenerator {
	return &insertSQLGenerator{NewCommonSQLGenerator(dialect, do)}
}

func (isg *insertSQLGenerator) Generate(
	b sb.SQLBuilder,
	clauses exp.InsertClauses,
) {
	if !clauses.HasInto() {
		b.SetError(ErrNoSourceForInsert)
		return
	}
	for _, f := range isg.DialectOptions().InsertSQLOrder {
		if b.Error() != nil {
			return
		}
		switch f {
		case CommonTableSQLFragment:
			isg.ExpressionSQLGenerator().Generate(b, clauses.CommonTables())
		case InsertBeingSQLFragment:
			isg.InsertBeginSQL(b, clauses.OnConflict())
		case IntoSQLFragment:
			b.WriteRunes(isg.DialectOptions().SpaceRune)
			isg.ExpressionSQLGenerator().Generate(b, clauses.Into())
		case InsertSQLFragment:
			isg.InsertSQL(b, clauses)
		case ReturningSQLFragment:
			isg.ReturningSQL(b, clauses.Returning())
		default:
			b.SetError(ErrNotSupportedFragment("INSERT", f))
		}
	}
}

// Adds the correct fragment to being an INSERT statement
func (isg *insertSQLGenerator) InsertBeginSQL(b sb.SQLBuilder, o exp.ConflictExpression) {
	_, isNotDoNothingConflictExpression := o.(conflictUpdater)
	// A conflict target requires a real ON CONFLICT clause: INSERT IGNORE / INSERT OR IGNORE would swallow
	// violations of every constraint, not just the targeted one.
	hasTarget := false
	if dn, ok := o.(conflictDoNothing); ok {
		hasTarget = targetOf(dn).present()
	}
	if isg.DialectOptions().SupportsInsertIgnoreSyntax && o != nil && !isNotDoNothingConflictExpression && !hasTarget {
		b.Write(isg.DialectOptions().InsertIgnoreClause)
	} else {
		b.Write(isg.DialectOptions().InsertClause)
	}
}

// Adds the columns list to an insert statement
func (isg *insertSQLGenerator) InsertSQL(b sb.SQLBuilder, ic exp.InsertClauses) {
	switch {
	case ic.HasRows():
		ie, err := exp.NewInsertExpression(ic.Rows()...)
		if err != nil {
			b.SetError(err)
			return
		}
		isg.InsertExpressionSQL(b, ie)
	case ic.HasCols() && ic.HasVals():
		isg.insertColumnsSQL(b, ic.Cols())
		isg.insertValuesSQL(b, ic.Vals())
	case ic.HasCols() && ic.HasFrom():
		isg.insertColumnsSQL(b, ic.Cols())
		isg.insertFromSQL(b, ic.From())
	case ic.HasFrom():
		isg.insertFromSQL(b, ic.From())
	default:
		isg.defaultValuesSQL(b)
	}
	if ic.HasAlias() {
		b.Write(isg.DialectOptions().AsFragment)
		isg.ExpressionSQLGenerator().Generate(b, ic.Alias())
	}
	isg.onConflictSQL(b, ic.OnConflict())
}

func (isg *insertSQLGenerator) InsertExpressionSQL(b sb.SQLBuilder, ie exp.InsertExpression) {
	switch {
	case ie.IsInsertFrom():
		isg.insertFromSQL(b, ie.From())
	case ie.IsEmpty():
		isg.defaultValuesSQL(b)
	default:
		isg.insertColumnsSQL(b, ie.Cols())
		isg.insertValuesSQL(b, ie.Vals())
	}
}

// Adds the DefaultValuesFragment to an SQL statement
func (isg *insertSQLGenerator) defaultValuesSQL(b sb.SQLBuilder) {
	b.Write(isg.DialectOptions().DefaultValuesFragment)
}

func (isg *insertSQLGenerator) insertFromSQL(b sb.SQLBuilder, ae exp.AppendableExpression) {
	b.WriteRunes(isg.DialectOptions().SpaceRune)
	appendNested(b, ae, isg.Dialect(), isg.DialectOptions())
}

// Adds the columns list to an insert statement
func (isg *insertSQLGenerator) insertColumnsSQL(b sb.SQLBuilder, cols exp.ColumnListExpression) {
	b.WriteRunes(isg.DialectOptions().SpaceRune, isg.DialectOptions().LeftParenRune)
	isg.ExpressionSQLGenerator().Generate(b, cols)
	b.WriteRunes(isg.DialectOptions().RightParenRune)
}

// Adds the values clause to an SQL statement
func (isg *insertSQLGenerator) insertValuesSQL(b sb.SQLBuilder, values [][]interface{}) {
	b.Write(isg.DialectOptions().ValuesFragment)
	rowLen := len(values[0])
	valueLen := len(values)
	for i, row := range values {
		if len(row) != rowLen {
			b.SetError(errMisMatchedRowLength(rowLen, len(row)))
			return
		}
		isg.ExpressionSQLGenerator().Generate(b, isg.DialectOptions().asArrayRow(row))
		if i < valueLen-1 {
			b.WriteRunes(isg.DialectOptions().CommaRune, isg.DialectOptions().SpaceRune)
		}
	}
}

// Adds the DefaultValuesFragment to an SQL statement
func (isg *insertSQLGenerator) onConflictSQL(b sb.SQLBuilder, o exp.ConflictExpression) {
	if o == nil {
		return
	}
	b.Write(isg.DialectOptions().ConflictFragment)
	switch t := o.(type) {
	case conflictUpdater:
		isg.conflictUpdateTargetSQL(b, t)
		if b.Error() != nil {
			return
		}
		isg.onConflictDoUpdateSQL(b, t)
	case conflictDoNothing:
		isg.conflictTargetSQL(b, targetOf(t))
		if b.Error() != nil {
			return
		}
		b.Write(isg.DialectOptions().ConflictDoNothingFragment)
	default:
		b.Write(isg.DialectOptions().ConflictDoNothingFragment)
	}
}

// conflictUpdater and conflictDoNothing are the minimal contracts the generator dispatches on, so custom
// exp.ConflictExpression implementations written against the original interfaces keep working. The target
// features are discovered through optional interfaces.
type (
	conflictUpdater interface {
		exp.ConflictExpression
		TargetColumn() string
		WhereClause() exp.ExpressionList
		Update() any
	}
	conflictDoNothing interface {
		exp.ConflictExpression
		Cols() exp.ColumnListExpression
	}
	conflictTarget struct {
		cols       exp.ColumnListExpression
		constraint string
		where      exp.ExpressionList
	}
)

func targetOf(o exp.ConflictExpression) conflictTarget {
	var t conflictTarget
	if c, ok := o.(interface {
		Cols() exp.ColumnListExpression
	}); ok {
		t.cols = c.Cols()
	}
	if c, ok := o.(interface{ Constraint() string }); ok {
		t.constraint = c.Constraint()
	}
	if c, ok := o.(interface{ TargetWhereClause() exp.ExpressionList }); ok {
		t.where = c.TargetWhereClause()
	}
	return t
}

func (t conflictTarget) hasCols() bool  { return t.cols != nil && len(t.cols.Columns()) > 0 }
func (t conflictTarget) hasWhere() bool { return t.where != nil && !t.where.IsEmpty() }
func (t conflictTarget) present() bool  { return t.hasCols() || t.constraint != "" || t.hasWhere() }

func (isg *insertSQLGenerator) conflictUpdateTargetSQL(b sb.SQLBuilder, u conflictUpdater) {
	target := u.TargetColumn()
	t := targetOf(u)
	if target == "" {
		if t.present() {
			isg.conflictTargetSQL(b, t)
		}
		return
	}
	if t.hasCols() || t.constraint != "" {
		b.SetError(ErrConflictTargetAmbiguous())
		return
	}
	isRawConstraint := strings.HasPrefix(strings.ToLower(strings.TrimSpace(target)), "on constraint")
	if t.hasWhere() {
		if isRawConstraint {
			b.SetError(ErrConflictTargetWhereRequiresCols())
			return
		}
		if !isg.DialectOptions().SupportsConflictTarget {
			b.SetError(ErrConflictTargetNotSupported(isg.Dialect()))
			return
		}
		b.WriteRunes(isg.DialectOptions().SpaceRune, isg.DialectOptions().LeftParenRune).
			WriteStrings(target).
			WriteRunes(isg.DialectOptions().RightParenRune)
		isg.conflictTargetWhereSQL(b, t.where)
		return
	}
	// legacy: the target is a raw string written as is when the dialect supports conflict targets
	if isg.DialectOptions().SupportsConflictTarget {
		b.WriteRunes(isg.DialectOptions().SpaceRune)
		if isRawConstraint {
			b.WriteStrings(target)
		} else {
			b.WriteRunes(isg.DialectOptions().LeftParenRune).
				WriteStrings(target).
				WriteRunes(isg.DialectOptions().RightParenRune)
		}
	}
}

// Generates the target of an ON CONFLICT clause: (cols) [WHERE predicate] or ON CONSTRAINT name
func (isg *insertSQLGenerator) conflictTargetSQL(b sb.SQLBuilder, t conflictTarget) {
	switch {
	case t.hasCols() && t.constraint != "":
		b.SetError(ErrConflictTargetAmbiguous())
	case t.constraint != "":
		switch {
		case !isg.DialectOptions().SupportsConflictOnConstraint:
			b.SetError(ErrConflictOnConstraintNotSupported(isg.Dialect()))
		case t.hasWhere():
			b.SetError(ErrConflictTargetWhereRequiresCols())
		default:
			fragment := isg.DialectOptions().ConflictOnConstraintFragment
			if len(fragment) == 0 {
				fragment = defaultConflictOnConstraintFragment
			}
			b.Write(fragment)
			// always a quoted identifier, never a wildcard or expression
			q := string(isg.DialectOptions().QuoteRune)
			b.WriteStrings(q, strings.ReplaceAll(t.constraint, q, q+q), q)
		}
	case t.hasCols():
		isg.conflictTargetColsSQL(b, t.cols)
		if b.Error() == nil && t.hasWhere() {
			isg.conflictTargetWhereSQL(b, t.where)
		}
	case t.hasWhere():
		b.SetError(ErrConflictTargetWhereRequiresCols())
	}
}

// Writes the target columns with values interpolated, even for prepared statements: an index expression has to
// match the index definition textually, so bind parameters are not allowed.
func (isg *insertSQLGenerator) conflictTargetColsSQL(b sb.SQLBuilder, cols exp.ColumnListExpression) {
	lb := sb.NewSQLBuilder(false)
	defer sb.ReleaseSQLBuilder(lb)
	isg.ConflictTargetSQL(lb, cols)
	sql, args, err := lb.ToSQL()
	if err != nil {
		b.SetError(err)
		return
	}
	if len(args) > 0 {
		b.SetError(ErrConflictTargetBindArgs())
		return
	}
	b.WriteStrings(sql)
}

// Writes the partial index predicate of a conflict target with its values interpolated, even for prepared
// statements: neither SQLite nor Postgres (once a reused statement switches to a generic plan) can match a
// parameterized predicate to a partial index definition.
func (isg *insertSQLGenerator) conflictTargetWhereSQL(b sb.SQLBuilder, where exp.ExpressionList) {
	lb := sb.NewSQLBuilder(false)
	defer sb.ReleaseSQLBuilder(lb)
	isg.WhereSQL(lb, where)
	sql, args, err := lb.ToSQL()
	if err != nil {
		b.SetError(err)
		return
	}
	if len(args) > 0 {
		b.SetError(ErrConflictTargetWhereBindArgs())
		return
	}
	b.WriteStrings(sql)
}

func (isg *insertSQLGenerator) onConflictDoUpdateSQL(b sb.SQLBuilder, o conflictUpdater) {
	b.Write(isg.DialectOptions().ConflictDoUpdateFragment)
	update := o.Update()
	if update == nil {
		b.SetError(ErrConflictUpdateValuesRequired)
		return
	}
	ue, err := exp.NewUpdateExpressions(update)
	if err != nil {
		b.SetError(err)
		return
	}
	isg.UpdateExpressionSQL(b, ue...)
	if b.Error() == nil && o.WhereClause() != nil {
		if !isg.DialectOptions().SupportsConflictUpdateWhere {
			b.SetError(errUpsertWithWhereNotSupported(isg.Dialect()))
			return
		}
		isg.WhereSQL(b, o.WhereClause())
	}
}
