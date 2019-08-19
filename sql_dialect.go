package goqu

import (
	"database/sql/driver"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/doug-martin/goqu/v8/exp"
	"github.com/doug-martin/goqu/v8/internal/errors"
	"github.com/doug-martin/goqu/v8/internal/sb"
	"github.com/doug-martin/goqu/v8/internal/util"
)

type (
	// An adapter interface to be used by a Dataset to generate SQL for a specific dialect.
	// See DefaultAdapter for a concrete implementation and examples.
	SQLDialect interface {
		Dialect() string
		ToSelectSQL(b sb.SQLBuilder, clauses exp.SelectClauses)
		ToUpdateSQL(b sb.SQLBuilder, clauses exp.UpdateClauses)
		ToInsertSQL(b sb.SQLBuilder, clauses exp.InsertClauses)
		ToDeleteSQL(b sb.SQLBuilder, clauses exp.DeleteClauses)
		ToTruncateSQL(b sb.SQLBuilder, clauses exp.TruncateClauses)
	}
	// The default adapter. This class should be used when building a new adapter. When creating a new adapter you can
	// either override methods, or more typically update default values.
	// See (github.com/doug-martin/goqu/adapters/postgres)
	sqlDialect struct {
		dialect        string
		dialectOptions *SQLDialectOptions
	}
)

var (
	replacementRune = '?'
	dialects        = make(map[string]SQLDialect)
	TrueLiteral     = exp.NewLiteralExpression("TRUE")
	FalseLiteral    = exp.NewLiteralExpression("FALSE")

	errNoUpdatedValuesProvided      = errors.New("no update values provided")
	errConflictUpdateValuesRequired = errors.New("values are required for on conflict update expression")
	errNoSourceForUpdate            = errors.New("no source found when generating update sql")
	errNoSourceForInsert            = errors.New("no source found when generating insert sql")
	errNoSourceForDelete            = errors.New("no source found when generating delete sql")
	errNoSourceForTruncate          = errors.New("no source found when generating truncate sql")
	errNoSetValuesForUpdate         = errors.New("no set values found when generating UPDATE sql")
	errEmptyIdentifier              = errors.New(`a empty identifier was encountered, please specify a "schema", "table" or "column"`)
	errWindowFunctionNotSupported   = errors.New("adapter does not support window function clause")
	errNoWindowName                 = errors.New("window expresion has no valid name")
)

func errNotSupportedFragment(sqlType string, f SQLFragmentType) error {
	return errors.New("unsupported %s SQL fragment %s", sqlType, f)
}

func errNotSupportedJoinType(j exp.JoinExpression) error {
	return errors.New("dialect does not support %v", j.JoinType())
}

func errJoinConditionRequired(j exp.JoinExpression) error {
	return errors.New("join condition required for conditioned join %v", j.JoinType())
}

func errMisMatchedRowLength(expectedL, actualL int) error {
	return errors.New("rows with different value length expected %d got %d", expectedL, actualL)
}

func errUnsupportedExpressionType(e exp.Expression) error {
	return errors.New("unsupported expression type %T", e)
}

func errUnsupportedIdentifierExpression(t interface{}) error {
	return errors.New("unexpected col type must be string or LiteralExpression received %T", t)
}

func errUnsupportedBooleanExpressionOperator(op exp.BooleanOperation) error {
	return errors.New("boolean operator '%+v' not supported", op)
}

func errUnsupportedRangeExpressionOperator(op exp.RangeOperation) error {
	return errors.New("range operator %+v not supported", op)
}

func errCTENotSupported(dialect string) error {
	return errors.New("dialect does not support CTE WITH clause [dialect=%s]", dialect)
}
func errRecursiveCTENotSupported(dialect string) error {
	return errors.New("dialect does not support CTE WITH RECURSIVE clause [dialect=%s]", dialect)
}
func errUpsertWithWhereNotSupported(dialect string) error {
	return errors.New("dialect does not support upsert with where clause [dialect=%s]", dialect)
}
func errReturnNotSupported(dialect string) error {
	return errors.New("dialect does not support RETURNING clause [dialect=%s]", dialect)
}

func errDistinctOnNotSupported(dialect string) error {
	return errors.New("dialect does not support DISTINCT ON clause [dialect=%s]", dialect)
}

func init() {
	RegisterDialect("default", DefaultDialectOptions())
}

func RegisterDialect(name string, do *SQLDialectOptions) {
	lowerName := strings.ToLower(name)
	dialects[lowerName] = newDialect(lowerName, do)
}

func DeregisterDialect(name string) {
	delete(dialects, strings.ToLower(name))
}

func GetDialect(name string) SQLDialect {
	name = strings.ToLower(name)
	if d, ok := dialects[name]; ok {
		return d
	}
	return newDialect("default", DefaultDialectOptions())
}

func newDialect(dialect string, do *SQLDialectOptions) SQLDialect {
	return &sqlDialect{dialect: dialect, dialectOptions: do}
}

func (d *sqlDialect) Dialect() string {
	return d.dialect
}
func (d *sqlDialect) SupportsReturn() bool {
	return d.dialectOptions.SupportsReturn
}

func (d *sqlDialect) SupportsOrderByOnUpdate() bool {
	return d.dialectOptions.SupportsOrderByOnUpdate
}

func (d *sqlDialect) SupportsLimitOnUpdate() bool {
	return d.dialectOptions.SupportsLimitOnUpdate
}

func (d *sqlDialect) SupportsOrderByOnDelete() bool {
	return d.dialectOptions.SupportsOrderByOnDelete
}
func (d *sqlDialect) SupportsLimitOnDelete() bool {
	return d.dialectOptions.SupportsLimitOnDelete
}

func (d *sqlDialect) ToSelectSQL(b sb.SQLBuilder, clauses exp.SelectClauses) {
	for _, f := range d.dialectOptions.SelectSQLOrder {
		if b.Error() != nil {
			return
		}
		switch f {
		case CommonTableSQLFragment:
			d.CommonTablesSQL(b, clauses.CommonTables())
		case SelectSQLFragment:
			d.SelectSQL(b, clauses)
		case FromSQLFragment:
			d.FromSQL(b, clauses.From())
		case JoinSQLFragment:
			d.JoinSQL(b, clauses.Joins())
		case WhereSQLFragment:
			d.WhereSQL(b, clauses.Where())
		case GroupBySQLFragment:
			d.GroupBySQL(b, clauses.GroupBy())
		case HavingSQLFragment:
			d.HavingSQL(b, clauses.Having())
		case WindowSQLFragment:
			d.WindowsSQL(b, clauses.Windows()...)
		case CompoundsSQLFragment:
			d.CompoundsSQL(b, clauses.Compounds())
		case OrderSQLFragment:
			d.OrderSQL(b, clauses.Order())
		case LimitSQLFragment:
			d.LimitSQL(b, clauses.Limit())
		case OffsetSQLFragment:
			d.OffsetSQL(b, clauses.Offset())
		case ForSQLFragment:
			d.ForSQL(b, clauses.Lock())
		default:
			b.SetError(errNotSupportedFragment("SELECT", f))
		}
	}
}

func (d *sqlDialect) ToUpdateSQL(b sb.SQLBuilder, clauses exp.UpdateClauses) {
	if !clauses.HasTable() {
		b.SetError(errNoSourceForUpdate)
		return
	}
	if !clauses.HasSetValues() {
		b.SetError(errNoSetValuesForUpdate)
		return
	}
	if !d.dialectOptions.SupportsMultipleUpdateTables && clauses.HasFrom() {
		b.SetError(errors.New("%s dialect does not support multiple tables in UPDATE", d.dialect))
	}
	updates, err := exp.NewUpdateExpressions(clauses.SetValues())
	if err != nil {
		b.SetError(err)
		return
	}
	for _, f := range d.dialectOptions.UpdateSQLOrder {
		if b.Error() != nil {
			return
		}
		switch f {
		case CommonTableSQLFragment:
			d.CommonTablesSQL(b, clauses.CommonTables())
		case UpdateBeginSQLFragment:
			d.UpdateBeginSQL(b)
		case SourcesSQLFragment:
			d.updateTableSQL(b, clauses)
		case UpdateSQLFragment:
			d.UpdateExpressionsSQL(b, updates...)
		case UpdateFromSQLFragment:
			d.updateFromSQL(b, clauses.From())
		case WhereSQLFragment:
			d.WhereSQL(b, clauses.Where())
		case OrderSQLFragment:
			if d.dialectOptions.SupportsOrderByOnUpdate {
				d.OrderSQL(b, clauses.Order())
			}
		case LimitSQLFragment:
			if d.dialectOptions.SupportsLimitOnUpdate {
				d.LimitSQL(b, clauses.Limit())
			}
		case ReturningSQLFragment:
			d.ReturningSQL(b, clauses.Returning())
		default:
			b.SetError(errNotSupportedFragment("UPDATE", f))
		}
	}
}

func (d *sqlDialect) ToInsertSQL(
	b sb.SQLBuilder,
	clauses exp.InsertClauses,
) {
	if !clauses.HasInto() {
		b.SetError(errNoSourceForInsert)
		return
	}
	for _, f := range d.dialectOptions.InsertSQLOrder {
		if b.Error() != nil {
			return
		}
		switch f {
		case CommonTableSQLFragment:
			d.CommonTablesSQL(b, clauses.CommonTables())
		case InsertBeingSQLFragment:
			d.InsertBeginSQL(b, clauses.OnConflict())
		case IntoSQLFragment:
			b.WriteRunes(d.dialectOptions.SpaceRune)
			d.Literal(b, clauses.Into())
		case InsertSQLFragment:
			d.InsertSQL(b, clauses)
		case ReturningSQLFragment:
			d.ReturningSQL(b, clauses.Returning())
		default:
			b.SetError(errNotSupportedFragment("INSERT", f))
		}
	}

}

func (d *sqlDialect) ToDeleteSQL(b sb.SQLBuilder, clauses exp.DeleteClauses) {
	if !clauses.HasFrom() {
		b.SetError(errNoSourceForDelete)
		return
	}
	for _, f := range d.dialectOptions.DeleteSQLOrder {
		if b.Error() != nil {
			return
		}
		switch f {
		case CommonTableSQLFragment:
			d.CommonTablesSQL(b, clauses.CommonTables())
		case DeleteBeginSQLFragment:
			d.DeleteBeginSQL(b)
		case FromSQLFragment:
			d.FromSQL(b, exp.NewColumnListExpression(clauses.From()))
		case WhereSQLFragment:
			d.WhereSQL(b, clauses.Where())
		case OrderSQLFragment:
			if d.dialectOptions.SupportsOrderByOnDelete {
				d.OrderSQL(b, clauses.Order())
			}
		case LimitSQLFragment:
			if d.dialectOptions.SupportsLimitOnDelete {
				d.LimitSQL(b, clauses.Limit())
			}
		case ReturningSQLFragment:
			d.ReturningSQL(b, clauses.Returning())
		default:
			b.SetError(errNotSupportedFragment("DELETE", f))
		}
	}
}

func (d *sqlDialect) ToTruncateSQL(b sb.SQLBuilder, clauses exp.TruncateClauses) {
	if !clauses.HasTable() {
		b.SetError(errNoSourceForTruncate)
		return
	}
	for _, f := range d.dialectOptions.TruncateSQLOrder {
		if b.Error() != nil {
			return
		}
		switch f {
		case TruncateSQLFragment:
			d.TruncateSQL(b, clauses.Table(), clauses.Options())
		default:
			b.SetError(errNotSupportedFragment("TRUNCATE", f))
		}
	}
}

// Adds the correct fragment to being an UPDATE statement
func (d *sqlDialect) UpdateBeginSQL(b sb.SQLBuilder) {
	b.Write(d.dialectOptions.UpdateClause)
}

// Adds the correct fragment to being an INSERT statement
func (d *sqlDialect) InsertBeginSQL(b sb.SQLBuilder, o exp.ConflictExpression) {
	if d.dialectOptions.SupportsInsertIgnoreSyntax && o != nil {
		b.Write(d.dialectOptions.InsertIgnoreClause)
	} else {
		b.Write(d.dialectOptions.InsertClause)
	}
}

// Adds the correct fragment to being an DELETE statement
func (d *sqlDialect) DeleteBeginSQL(b sb.SQLBuilder) {
	b.Write(d.dialectOptions.DeleteClause)
}

// Generates a TRUNCATE statement
func (d *sqlDialect) TruncateSQL(b sb.SQLBuilder, from exp.ColumnListExpression, opts exp.TruncateOptions) {
	b.Write(d.dialectOptions.TruncateClause)
	d.SourcesSQL(b, from)
	if opts.Identity != d.dialectOptions.EmptyString {
		b.WriteRunes(d.dialectOptions.SpaceRune).
			WriteStrings(strings.ToUpper(opts.Identity)).
			Write(d.dialectOptions.IdentityFragment)
	}
	if opts.Cascade {
		b.Write(d.dialectOptions.CascadeFragment)
	} else if opts.Restrict {
		b.Write(d.dialectOptions.RestrictFragment)
	}
}

// Adds the columns list to an insert statement
func (d *sqlDialect) InsertSQL(b sb.SQLBuilder, ic exp.InsertClauses) {
	switch {
	case ic.HasRows():
		ie, err := exp.NewInsertExpression(ic.Rows()...)
		if err != nil {
			b.SetError(err)
			return
		}
		d.InsertExpressionSQL(b, ie)
	case ic.HasCols() && ic.HasVals():
		d.insertColumnsSQL(b, ic.Cols())
		d.insertValuesSQL(b, ic.Vals())
	case ic.HasCols() && ic.HasFrom():
		d.insertColumnsSQL(b, ic.Cols())
		d.insertFromSQL(b, ic.From())
	case ic.HasFrom():
		d.insertFromSQL(b, ic.From())
	default:
		d.defaultValuesSQL(b)

	}

	d.onConflictSQL(b, ic.OnConflict())
}

func (d *sqlDialect) InsertExpressionSQL(b sb.SQLBuilder, ie exp.InsertExpression) {
	switch {
	case ie.IsInsertFrom():
		d.insertFromSQL(b, ie.From())
	case ie.IsEmpty():
		d.defaultValuesSQL(b)
	default:
		d.insertColumnsSQL(b, ie.Cols())
		d.insertValuesSQL(b, ie.Vals())
	}
}

// Adds column setters in an update SET clause
func (d *sqlDialect) UpdateExpressionsSQL(b sb.SQLBuilder, updates ...exp.UpdateExpression) {
	b.Write(d.dialectOptions.SetFragment)
	d.updateValuesSQL(b, updates...)

}

// Adds the SELECT clause and columns to a sql statement
func (d *sqlDialect) SelectSQL(b sb.SQLBuilder, clauses exp.SelectClauses) {
	b.Write(d.dialectOptions.SelectClause).
		WriteRunes(d.dialectOptions.SpaceRune)
	dc := clauses.Distinct()
	if dc != nil {
		b.Write(d.dialectOptions.DistinctFragment)
		if !dc.IsEmpty() {
			if d.dialectOptions.SupportsDistinctOn {
				b.Write(d.dialectOptions.OnFragment).WriteRunes(d.dialectOptions.LeftParenRune)
				d.Literal(b, dc)
				b.WriteRunes(d.dialectOptions.RightParenRune, d.dialectOptions.SpaceRune)
			} else {
				b.SetError(errDistinctOnNotSupported(d.dialect))
				return
			}
		} else {
			b.WriteRunes(d.dialectOptions.SpaceRune)
		}
	}
	cols := clauses.Select()
	if clauses.IsDefaultSelect() || len(cols.Columns()) == 0 {
		b.WriteRunes(d.dialectOptions.StarRune)
	} else {
		d.Literal(b, cols)
	}
}

func (d *sqlDialect) ReturningSQL(b sb.SQLBuilder, returns exp.ColumnListExpression) {
	if returns != nil && len(returns.Columns()) > 0 {
		if d.SupportsReturn() {
			b.Write(d.dialectOptions.ReturningFragment)
			d.Literal(b, returns)
		} else {
			b.SetError(errReturnNotSupported(d.dialect))
		}
	}

}

// Adds the FROM clause and tables to an sql statement
func (d *sqlDialect) FromSQL(b sb.SQLBuilder, from exp.ColumnListExpression) {
	if from != nil && !from.IsEmpty() {
		b.Write(d.dialectOptions.FromFragment)
		d.SourcesSQL(b, from)
	}
}

// Adds the generates the SQL for a column list
func (d *sqlDialect) SourcesSQL(b sb.SQLBuilder, from exp.ColumnListExpression) {
	b.WriteRunes(d.dialectOptions.SpaceRune)
	d.Literal(b, from)
}

// Generates the JOIN clauses for an SQL statement
func (d *sqlDialect) JoinSQL(b sb.SQLBuilder, joins exp.JoinExpressions) {
	if len(joins) > 0 {
		for _, j := range joins {
			joinType, ok := d.dialectOptions.JoinTypeLookup[j.JoinType()]
			if !ok {
				b.SetError(errNotSupportedJoinType(j))
				return
			}
			b.Write(joinType)
			d.Literal(b, j.Table())
			if t, ok := j.(exp.ConditionedJoinExpression); ok {
				if t.IsConditionEmpty() {
					b.SetError(errJoinConditionRequired(j))
					return
				}
				d.joinConditionSQL(b, t.Condition())
			}
		}
	}
}

// Generates the WHERE clause for an SQL statement
func (d *sqlDialect) WhereSQL(b sb.SQLBuilder, where exp.ExpressionList) {
	if where != nil && !where.IsEmpty() {
		b.Write(d.dialectOptions.WhereFragment)
		d.Literal(b, where)
	}
}

// Generates the GROUP BY clause for an SQL statement
func (d *sqlDialect) GroupBySQL(b sb.SQLBuilder, groupBy exp.ColumnListExpression) {
	if groupBy != nil && len(groupBy.Columns()) > 0 {
		b.Write(d.dialectOptions.GroupByFragment)
		d.Literal(b, groupBy)
	}
}

// Generates the HAVING clause for an SQL statement
func (d *sqlDialect) HavingSQL(b sb.SQLBuilder, having exp.ExpressionList) {
	if having != nil && len(having.Expressions()) > 0 {
		b.Write(d.dialectOptions.HavingFragment)
		d.Literal(b, having)
	}
}

func (d *sqlDialect) WindowsSQL(b sb.SQLBuilder, windows ...exp.WindowExpression) {
	if b.Error() != nil {
		return
	}
	l := len(windows)
	if l == 0 {
		return
	}
	if !d.dialectOptions.SupportsWindowFunction {
		b.SetError(errWindowFunctionNotSupported)
		return
	}
	b.Write(d.dialectOptions.WindowFragment)
	d.WindowSQL(b, windows[0], true)
	for _, we := range windows[1:] {
		b.WriteRunes(d.dialectOptions.CommaRune, d.dialectOptions.SpaceRune)
		d.WindowSQL(b, we, true)
	}
}

func (d *sqlDialect) WindowSQL(b sb.SQLBuilder, we exp.WindowExpression, withName bool) {
	if b.Error() != nil {
		return
	}
	if !d.dialectOptions.SupportsWindowFunction {
		b.SetError(errWindowFunctionNotSupported)
		return
	}
	if withName {
		name := we.Name()
		if len(name) == 0 {
			b.SetError(errNoWindowName)
			return
		}
		d.Literal(b, I(name))
		b.Write(d.dialectOptions.AsFragment)
	}
	b.WriteRunes(d.dialectOptions.LeftParenRune)

	parent, partitionCols, orderCols := we.Parent(), we.PartitionCols(), we.OrderCols()
	hasParent := len(parent) > 0
	hasPartition := partitionCols != nil && !partitionCols.IsEmpty()
	hasOrder := orderCols != nil && !orderCols.IsEmpty()

	if hasParent {
		d.Literal(b, I(parent))
		if hasPartition || hasOrder {
			b.WriteRunes(d.dialectOptions.SpaceRune)
		}
	}

	if hasPartition {
		b.Write(d.dialectOptions.WindowPartitionByFragment)
		d.Literal(b, partitionCols)
		if hasOrder {
			b.WriteRunes(d.dialectOptions.SpaceRune)
		}
	}
	if hasOrder {
		b.Write(d.dialectOptions.WindowOrderByFragment)
		d.Literal(b, orderCols)
	}

	b.WriteRunes(d.dialectOptions.RightParenRune)
}

// Generates the ORDER BY clause for an SQL statement
func (d *sqlDialect) OrderSQL(b sb.SQLBuilder, order exp.ColumnListExpression) {
	if order != nil && len(order.Columns()) > 0 {
		b.Write(d.dialectOptions.OrderByFragment)
		d.Literal(b, order)
	}
}

// Generates the LIMIT clause for an SQL statement
func (d *sqlDialect) LimitSQL(b sb.SQLBuilder, limit interface{}) {
	if limit != nil {
		b.Write(d.dialectOptions.LimitFragment)
		d.Literal(b, limit)
	}
}

// Generates the OFFSET clause for an SQL statement
func (d *sqlDialect) OffsetSQL(b sb.SQLBuilder, offset uint) {
	if offset > 0 {
		b.Write(d.dialectOptions.OffsetFragment)
		d.Literal(b, offset)
	}
}

// Generates the sql for the WITH clauses for common table expressions (CTE)
func (d *sqlDialect) CommonTablesSQL(b sb.SQLBuilder, ctes []exp.CommonTableExpression) {
	if l := len(ctes); l > 0 {
		if !d.dialectOptions.SupportsWithCTE {
			b.SetError(errCTENotSupported(d.dialect))
			return
		}
		b.Write(d.dialectOptions.WithFragment)
		anyRecursive := false
		for _, cte := range ctes {
			anyRecursive = anyRecursive || cte.IsRecursive()
		}
		if anyRecursive {
			if !d.dialectOptions.SupportsWithCTERecursive {
				b.SetError(errRecursiveCTENotSupported(d.dialect))
				return
			}
			b.Write(d.dialectOptions.RecursiveFragment)
		}
		for i, cte := range ctes {
			d.Literal(b, cte)
			if i < l-1 {
				b.WriteRunes(d.dialectOptions.CommaRune, d.dialectOptions.SpaceRune)
			}
		}
		b.WriteRunes(d.dialectOptions.SpaceRune)
	}
}

// Generates the compound sql clause for an SQL statement (e.g. UNION, INTERSECT)
func (d *sqlDialect) CompoundsSQL(b sb.SQLBuilder, compounds []exp.CompoundExpression) {
	for _, compound := range compounds {
		d.Literal(b, compound)
	}
}

// Generates the FOR (aka "locking") clause for an SQL statement
func (d *sqlDialect) ForSQL(b sb.SQLBuilder, lockingClause exp.Lock) {
	if lockingClause == nil {
		return
	}
	switch lockingClause.Strength() {
	case exp.ForNolock:
		return
	case exp.ForUpdate:
		b.Write(d.dialectOptions.ForUpdateFragment)
	case exp.ForNoKeyUpdate:
		b.Write(d.dialectOptions.ForNoKeyUpdateFragment)
	case exp.ForShare:
		b.Write(d.dialectOptions.ForShareFragment)
	case exp.ForKeyShare:
		b.Write(d.dialectOptions.ForKeyShareFragment)
	}
	// the WAIT case is the default in Postgres, and is what you get if you don't specify NOWAIT or
	// SKIP LOCKED.  There's no special syntax for it in PG, so we don't do anything for it here
	switch lockingClause.WaitOption() {
	case exp.NoWait:
		b.Write(d.dialectOptions.NowaitFragment)
	case exp.SkipLocked:
		b.Write(d.dialectOptions.SkipLockedFragment)
	}
}

func (d *sqlDialect) Literal(b sb.SQLBuilder, val interface{}) {
	if b.Error() != nil {
		return
	}
	if val == nil {
		d.literalNil(b)
		return
	}
	switch v := val.(type) {
	case exp.Expression:
		d.expressionSQL(b, v)
	case int:
		d.literalInt(b, int64(v))
	case int32:
		d.literalInt(b, int64(v))
	case int64:
		d.literalInt(b, v)
	case float32:
		d.literalFloat(b, float64(v))
	case float64:
		d.literalFloat(b, v)
	case string:
		d.literalString(b, v)
	case []byte:
		d.literalBytes(b, v)
	case bool:
		d.literalBool(b, v)
	case time.Time:
		d.literalTime(b, v)
	case *time.Time:
		if v == nil {
			d.literalNil(b)
			return
		}
		d.literalTime(b, *v)
	case driver.Valuer:
		dVal, err := v.Value()
		if err != nil {
			b.SetError(errors.New(err.Error()))
			return
		}
		d.Literal(b, dVal)
	default:
		d.reflectSQL(b, val)
	}
}

// Adds the DefaultValuesFragment to an SQL statement
func (d *sqlDialect) defaultValuesSQL(b sb.SQLBuilder) {
	b.Write(d.dialectOptions.DefaultValuesFragment)
}

func (d *sqlDialect) insertFromSQL(b sb.SQLBuilder, ae exp.AppendableExpression) {
	b.WriteRunes(d.dialectOptions.SpaceRune)
	ae.AppendSQL(b)
}

// Adds the columns list to an insert statement
func (d *sqlDialect) insertColumnsSQL(b sb.SQLBuilder, cols exp.ColumnListExpression) {
	b.WriteRunes(d.dialectOptions.SpaceRune, d.dialectOptions.LeftParenRune)
	d.Literal(b, cols)
	b.WriteRunes(d.dialectOptions.RightParenRune)
}

// Adds the values clause to an SQL statement
func (d *sqlDialect) insertValuesSQL(b sb.SQLBuilder, values [][]interface{}) {
	b.Write(d.dialectOptions.ValuesFragment)
	rowLen := len(values[0])
	valueLen := len(values)
	for i, row := range values {
		if len(row) != rowLen {
			b.SetError(errMisMatchedRowLength(rowLen, len(row)))
			return
		}
		d.Literal(b, row)
		if i < valueLen-1 {
			b.WriteRunes(d.dialectOptions.CommaRune, d.dialectOptions.SpaceRune)
		}
	}
}

// Adds the DefaultValuesFragment to an SQL statement
func (d *sqlDialect) onConflictSQL(b sb.SQLBuilder, o exp.ConflictExpression) {
	if o == nil {
		return
	}
	b.Write(d.dialectOptions.ConflictFragment)
	switch t := o.(type) {
	case exp.ConflictUpdateExpression:
		target := t.TargetColumn()
		if d.dialectOptions.SupportsConflictTarget && target != "" {
			wrapParens := !strings.HasPrefix(strings.ToLower(target), "on constraint")

			b.WriteRunes(d.dialectOptions.SpaceRune)
			if wrapParens {
				b.WriteRunes(d.dialectOptions.LeftParenRune).
					WriteStrings(target).
					WriteRunes(d.dialectOptions.RightParenRune)
			} else {
				b.Write([]byte(target))
			}
		}
		d.onConflictDoUpdateSQL(b, t)
	default:
		b.Write(d.dialectOptions.ConflictDoNothingFragment)
	}
}

func (d *sqlDialect) updateTableSQL(b sb.SQLBuilder, uc exp.UpdateClauses) {
	b.WriteRunes(d.dialectOptions.SpaceRune)
	d.Literal(b, uc.Table())
	if uc.HasFrom() {
		if !d.dialectOptions.UseFromClauseForMultipleUpdateTables {
			b.WriteRunes(d.dialectOptions.CommaRune)
			d.Literal(b, uc.From())
		}
	}
}

// Adds column setters in an update SET clause
func (d *sqlDialect) updateValuesSQL(b sb.SQLBuilder, updates ...exp.UpdateExpression) {
	if len(updates) == 0 {
		b.SetError(errNoUpdatedValuesProvided)
		return
	}
	updateLen := len(updates)
	for i, update := range updates {
		d.Literal(b, update)
		if i < updateLen-1 {
			b.WriteRunes(d.dialectOptions.CommaRune)
		}
	}
}

func (d *sqlDialect) updateFromSQL(b sb.SQLBuilder, ce exp.ColumnListExpression) {
	if ce == nil || ce.IsEmpty() {
		return
	}
	if d.dialectOptions.UseFromClauseForMultipleUpdateTables {
		d.FromSQL(b, ce)
	}
}

func (d *sqlDialect) onConflictDoUpdateSQL(b sb.SQLBuilder, o exp.ConflictUpdateExpression) {
	b.Write(d.dialectOptions.ConflictDoUpdateFragment)
	update := o.Update()
	if update == nil {
		b.SetError(errConflictUpdateValuesRequired)
		return
	}
	ue, err := exp.NewUpdateExpressions(update)
	if err != nil {
		b.SetError(err)
		return
	}
	d.updateValuesSQL(b, ue...)
	if b.Error() == nil && o.WhereClause() != nil {
		if !d.dialectOptions.SupportsConflictUpdateWhere {
			b.SetError(errUpsertWithWhereNotSupported(d.dialect))
			return
		}
		d.WhereSQL(b, o.WhereClause())
	}
}

func (d *sqlDialect) joinConditionSQL(b sb.SQLBuilder, jc exp.JoinCondition) {
	switch t := jc.(type) {
	case exp.JoinOnCondition:
		d.joinOnConditionSQL(b, t)
	case exp.JoinUsingCondition:
		d.joinUsingConditionSQL(b, t)
	}
}

func (d *sqlDialect) joinUsingConditionSQL(b sb.SQLBuilder, jc exp.JoinUsingCondition) {
	b.Write(d.dialectOptions.UsingFragment).
		WriteRunes(d.dialectOptions.LeftParenRune)
	d.Literal(b, jc.Using())
	b.WriteRunes(d.dialectOptions.RightParenRune)
}

func (d *sqlDialect) joinOnConditionSQL(b sb.SQLBuilder, jc exp.JoinOnCondition) {
	b.Write(d.dialectOptions.OnFragment)
	d.Literal(b, jc.On())
}

func (d *sqlDialect) reflectSQL(b sb.SQLBuilder, val interface{}) {
	v := reflect.Indirect(reflect.ValueOf(val))
	valKind := v.Kind()
	switch {
	case util.IsInvalid(valKind):
		d.literalNil(b)
	case util.IsSlice(valKind):
		if bs, ok := val.([]byte); ok {
			d.Literal(b, bs)
			return
		}
		d.sliceValueSQL(b, v)
	case util.IsInt(valKind):
		d.Literal(b, v.Int())
	case util.IsUint(valKind):
		d.Literal(b, int64(v.Uint()))
	case util.IsFloat(valKind):
		d.Literal(b, v.Float())
	case util.IsString(valKind):
		d.Literal(b, v.String())
	case util.IsBool(valKind):
		d.Literal(b, v.Bool())
	default:
		b.SetError(errors.NewEncodeError(val))
	}
}

func (d *sqlDialect) expressionSQL(b sb.SQLBuilder, expression exp.Expression) {
	switch e := expression.(type) {
	case exp.ColumnListExpression:
		d.columnListSQL(b, e)
	case exp.ExpressionList:
		d.expressionListSQL(b, e)
	case exp.LiteralExpression:
		d.literalExpressionSQL(b, e)
	case exp.IdentifierExpression:
		d.quoteIdentifier(b, e)
	case exp.AliasedExpression:
		d.aliasedExpressionSQL(b, e)
	case exp.BooleanExpression:
		d.booleanExpressionSQL(b, e)
	case exp.RangeExpression:
		d.rangeExpressionSQL(b, e)
	case exp.OrderedExpression:
		d.orderedExpressionSQL(b, e)
	case exp.UpdateExpression:
		d.updateExpressionSQL(b, e)
	case exp.SQLFunctionExpression:
		d.sqlFunctionExpressionSQL(b, e)
	case exp.CastExpression:
		d.castExpressionSQL(b, e)
	case exp.AppendableExpression:
		d.appendableExpressionSQL(b, e)
	case exp.CommonTableExpression:
		d.commonTableExpressionSQL(b, e)
	case exp.CompoundExpression:
		d.compoundExpressionSQL(b, e)
	case exp.Ex:
		d.expressionMapSQL(b, e)
	case exp.ExOr:
		d.expressionOrMapSQL(b, e)
	default:
		b.SetError(errUnsupportedExpressionType(e))
	}
}

// Generates a placeholder (e.g. ?, $1)
func (d *sqlDialect) placeHolderSQL(b sb.SQLBuilder, i interface{}) {
	b.WriteRunes(d.dialectOptions.PlaceHolderRune)
	if d.dialectOptions.IncludePlaceholderNum {
		b.WriteStrings(strconv.FormatInt(int64(b.CurrentArgPosition()), 10))
	}
	b.WriteArg(i)
}

// Generates creates the sql for a sub select on a Dataset
func (d *sqlDialect) appendableExpressionSQL(b sb.SQLBuilder, a exp.AppendableExpression) {
	b.WriteRunes(d.dialectOptions.LeftParenRune)
	a.AppendSQL(b)
	b.WriteRunes(d.dialectOptions.RightParenRune)
	c := a.GetClauses()
	if c != nil {
		alias := c.Alias()
		if alias != nil {
			b.Write(d.dialectOptions.AsFragment)
			d.Literal(b, alias)
		}
	}
}

// Quotes an identifier (e.g. "col", "table"."col"
func (d *sqlDialect) quoteIdentifier(b sb.SQLBuilder, ident exp.IdentifierExpression) {
	if ident.IsEmpty() {
		b.SetError(errEmptyIdentifier)
		return
	}
	schema, table, col := ident.GetSchema(), ident.GetTable(), ident.GetCol()
	if schema != d.dialectOptions.EmptyString {
		b.WriteRunes(d.dialectOptions.QuoteRune).
			WriteStrings(schema).
			WriteRunes(d.dialectOptions.QuoteRune)
	}
	if table != d.dialectOptions.EmptyString {
		if schema != d.dialectOptions.EmptyString {
			b.WriteRunes(d.dialectOptions.PeriodRune)
		}
		b.WriteRunes(d.dialectOptions.QuoteRune).
			WriteStrings(table).
			WriteRunes(d.dialectOptions.QuoteRune)
	}
	switch t := col.(type) {
	case nil:
	case string:
		if col != d.dialectOptions.EmptyString {
			if table != d.dialectOptions.EmptyString || schema != d.dialectOptions.EmptyString {
				b.WriteRunes(d.dialectOptions.PeriodRune)
			}
			b.WriteRunes(d.dialectOptions.QuoteRune).
				WriteStrings(t).
				WriteRunes(d.dialectOptions.QuoteRune)
		}
	case exp.LiteralExpression:
		if table != d.dialectOptions.EmptyString || schema != d.dialectOptions.EmptyString {
			b.WriteRunes(d.dialectOptions.PeriodRune)
		}
		d.Literal(b, t)
	default:
		b.SetError(errUnsupportedIdentifierExpression(col))
	}
}

// Generates SQL NULL value
func (d *sqlDialect) literalNil(b sb.SQLBuilder) {
	b.Write(d.dialectOptions.Null)
}

// Generates SQL bool literal, (e.g. TRUE, FALSE, mysql 1, 0, sqlite3 1, 0)
func (d *sqlDialect) literalBool(b sb.SQLBuilder, bl bool) {
	if b.IsPrepared() {
		d.placeHolderSQL(b, bl)
		return
	}
	if bl {
		b.Write(d.dialectOptions.True)
	} else {
		b.Write(d.dialectOptions.False)
	}
}

// Generates SQL for a time.Time value
func (d *sqlDialect) literalTime(b sb.SQLBuilder, t time.Time) {
	if b.IsPrepared() {
		d.placeHolderSQL(b, t)
		return
	}
	d.Literal(b, t.Format(d.dialectOptions.TimeFormat))
}

// Generates SQL for a Float Value
func (d *sqlDialect) literalFloat(b sb.SQLBuilder, f float64) {
	if b.IsPrepared() {
		d.placeHolderSQL(b, f)
		return
	}
	b.WriteStrings(strconv.FormatFloat(f, 'f', -1, 64))
}

// Generates SQL for an int value
func (d *sqlDialect) literalInt(b sb.SQLBuilder, i int64) {
	if b.IsPrepared() {
		d.placeHolderSQL(b, i)
		return
	}
	b.WriteStrings(strconv.FormatInt(i, 10))
}

// Generates SQL for a string
func (d *sqlDialect) literalString(b sb.SQLBuilder, s string) {
	if b.IsPrepared() {
		d.placeHolderSQL(b, s)
		return
	}
	b.WriteRunes(d.dialectOptions.StringQuote)
	for _, char := range s {
		if e, ok := d.dialectOptions.EscapedRunes[char]; ok {
			b.Write(e)
		} else {
			b.WriteRunes(char)
		}
	}

	b.WriteRunes(d.dialectOptions.StringQuote)
}

// Generates SQL for a slice of bytes
func (d *sqlDialect) literalBytes(b sb.SQLBuilder, bs []byte) {
	if b.IsPrepared() {
		d.placeHolderSQL(b, bs)
		return
	}
	b.WriteRunes(d.dialectOptions.StringQuote)
	i := 0
	for len(bs) > 0 {
		char, l := utf8.DecodeRune(bs)
		if e, ok := d.dialectOptions.EscapedRunes[char]; ok {
			b.Write(e)
		} else {
			b.WriteRunes(char)
		}
		i++
		bs = bs[l:]
	}
	b.WriteRunes(d.dialectOptions.StringQuote)
}

// Generates SQL for a slice of values (e.g. []int64{1,2,3,4} -> (1,2,3,4)
func (d *sqlDialect) sliceValueSQL(b sb.SQLBuilder, slice reflect.Value) {
	b.WriteRunes(d.dialectOptions.LeftParenRune)
	for i, l := 0, slice.Len(); i < l; i++ {
		d.Literal(b, slice.Index(i).Interface())
		if i < l-1 {
			b.WriteRunes(d.dialectOptions.CommaRune, d.dialectOptions.SpaceRune)
		}
	}
	b.WriteRunes(d.dialectOptions.RightParenRune)
}

// Generates SQL for an AliasedExpression (e.g. I("a").As("b") -> "a" AS "b")
func (d *sqlDialect) aliasedExpressionSQL(b sb.SQLBuilder, aliased exp.AliasedExpression) {
	d.Literal(b, aliased.Aliased())
	b.Write(d.dialectOptions.AsFragment)
	d.Literal(b, aliased.GetAs())
}

// Generates SQL for a BooleanExpresion (e.g. I("a").Eq(2) -> "a" = 2)
func (d *sqlDialect) booleanExpressionSQL(b sb.SQLBuilder, operator exp.BooleanExpression) {
	b.WriteRunes(d.dialectOptions.LeftParenRune)
	d.Literal(b, operator.LHS())
	b.WriteRunes(d.dialectOptions.SpaceRune)
	operatorOp := operator.Op()
	if val, ok := d.dialectOptions.BooleanOperatorLookup[operatorOp]; ok {
		b.Write(val)
	} else {
		b.SetError(errUnsupportedBooleanExpressionOperator(operatorOp))
		return
	}
	rhs := operator.RHS()
	if (operatorOp == exp.IsOp || operatorOp == exp.IsNotOp) && d.dialectOptions.UseLiteralIsBools {
		if rhs == true {
			rhs = TrueLiteral
		} else if rhs == false {
			rhs = FalseLiteral
		}
	}
	b.WriteRunes(d.dialectOptions.SpaceRune)
	d.Literal(b, rhs)
	b.WriteRunes(d.dialectOptions.RightParenRune)
}

// Generates SQL for a RangeExpresion (e.g. I("a").Between(RangeVal{Start:2,End:5}) -> "a" BETWEEN 2 AND 5)
func (d *sqlDialect) rangeExpressionSQL(b sb.SQLBuilder, operator exp.RangeExpression) {
	b.WriteRunes(d.dialectOptions.LeftParenRune)
	d.Literal(b, operator.LHS())
	b.WriteRunes(d.dialectOptions.SpaceRune)
	operatorOp := operator.Op()
	if val, ok := d.dialectOptions.RangeOperatorLookup[operatorOp]; ok {
		b.Write(val)
	} else {
		b.SetError(errUnsupportedRangeExpressionOperator(operatorOp))
		return
	}
	rhs := operator.RHS()
	b.WriteRunes(d.dialectOptions.SpaceRune)
	d.Literal(b, rhs.Start())
	b.Write(d.dialectOptions.AndFragment)
	d.Literal(b, rhs.End())
	b.WriteRunes(d.dialectOptions.RightParenRune)
}

// Generates SQL for an OrderedExpression (e.g. I("a").Asc() -> "a" ASC)
func (d *sqlDialect) orderedExpressionSQL(b sb.SQLBuilder, order exp.OrderedExpression) {
	d.Literal(b, order.SortExpression())
	if order.IsAsc() {
		b.Write(d.dialectOptions.AscFragment)
	} else {
		b.Write(d.dialectOptions.DescFragment)
	}
	switch order.NullSortType() {
	case exp.NullsFirstSortType:
		b.Write(d.dialectOptions.NullsFirstFragment)
	case exp.NullsLastSortType:
		b.Write(d.dialectOptions.NullsLastFragment)
	}
}

// Generates SQL for an ExpressionList (e.g. And(I("a").Eq("a"), I("b").Eq("b")) -> (("a" = 'a') AND ("b" = 'b')))
func (d *sqlDialect) expressionListSQL(b sb.SQLBuilder, expressionList exp.ExpressionList) {
	if expressionList.IsEmpty() {
		return
	}
	var op []byte
	if expressionList.Type() == exp.AndType {
		op = d.dialectOptions.AndFragment
	} else {
		op = d.dialectOptions.OrFragment
	}
	exps := expressionList.Expressions()
	expLen := len(exps) - 1
	needsAppending := expLen > 0
	if needsAppending {
		b.WriteRunes(d.dialectOptions.LeftParenRune)
	} else {
		d.Literal(b, exps[0])
		return
	}
	for i, e := range exps {
		d.Literal(b, e)
		if i < expLen {
			b.Write(op)
		}
	}
	b.WriteRunes(d.dialectOptions.RightParenRune)
}

// Generates SQL for a ColumnListExpression
func (d *sqlDialect) columnListSQL(b sb.SQLBuilder, columnList exp.ColumnListExpression) {
	cols := columnList.Columns()
	colLen := len(cols)
	for i, col := range cols {
		d.Literal(b, col)
		if i < colLen-1 {
			b.WriteRunes(d.dialectOptions.CommaRune, d.dialectOptions.SpaceRune)
		}
	}
}

// Generates SQL for an UpdateEpxresion
func (d *sqlDialect) updateExpressionSQL(b sb.SQLBuilder, update exp.UpdateExpression) {
	d.Literal(b, update.Col())
	b.WriteRunes(d.dialectOptions.SetOperatorRune)
	d.Literal(b, update.Val())
}

// Generates SQL for a LiteralExpression
//    L("a + b") -> a + b
//    L("a = ?", 1) -> a = 1
func (d *sqlDialect) literalExpressionSQL(b sb.SQLBuilder, literal exp.LiteralExpression) {
	lit := literal.Literal()
	args := literal.Args()
	argsLen := len(args)
	if argsLen > 0 {
		currIndex := 0
		for _, char := range lit {
			if char == replacementRune && currIndex < argsLen {
				d.Literal(b, args[currIndex])
				currIndex++
			} else {
				b.WriteRunes(char)
			}
		}
	} else {
		b.WriteStrings(lit)
	}
}

// Generates SQL for a SQLFunctionExpression
//   COUNT(I("a")) -> COUNT("a")
func (d *sqlDialect) sqlFunctionExpressionSQL(b sb.SQLBuilder, sqlFunc exp.SQLFunctionExpression) {
	b.WriteStrings(sqlFunc.Name())
	d.Literal(b, sqlFunc.Args())

	if sqlWinFunc, ok := sqlFunc.(exp.SQLWindowFunctionExpression); ok {
		b.Write(d.dialectOptions.WindowOverFragment)
		if sqlWinFunc.HasWindowName() {
			d.Literal(b, I(sqlWinFunc.WindowName()))
		} else if sqlWinFunc.HasWindow() {
			d.WindowSQL(b, sqlWinFunc.Window(), false)
		} else {
			d.WindowSQL(b, emptyWindow, false)
		}
	}
}

// Generates SQL for a CastExpression
//   I("a").Cast("NUMERIC") -> CAST("a" AS NUMERIC)
func (d *sqlDialect) castExpressionSQL(b sb.SQLBuilder, cast exp.CastExpression) {
	b.Write(d.dialectOptions.CastFragment).WriteRunes(d.dialectOptions.LeftParenRune)
	d.Literal(b, cast.Casted())
	b.Write(d.dialectOptions.AsFragment)
	d.Literal(b, cast.Type())
	b.WriteRunes(d.dialectOptions.RightParenRune)
}

// Generates SQL for a CommonTableExpression
func (d *sqlDialect) commonTableExpressionSQL(b sb.SQLBuilder, cte exp.CommonTableExpression) {
	d.Literal(b, cte.Name())
	b.Write(d.dialectOptions.AsFragment)
	d.Literal(b, cte.SubQuery())
}

// Generates SQL for a CompoundExpression
func (d *sqlDialect) compoundExpressionSQL(b sb.SQLBuilder, compound exp.CompoundExpression) {
	switch compound.Type() {
	case exp.UnionCompoundType:
		b.Write(d.dialectOptions.UnionFragment)
	case exp.UnionAllCompoundType:
		b.Write(d.dialectOptions.UnionAllFragment)
	case exp.IntersectCompoundType:
		b.Write(d.dialectOptions.IntersectFragment)
	case exp.IntersectAllCompoundType:
		b.Write(d.dialectOptions.IntersectAllFragment)
	}
	if d.dialectOptions.WrapCompoundsInParens {
		b.WriteRunes(d.dialectOptions.LeftParenRune)
		compound.RHS().AppendSQL(b)
		b.WriteRunes(d.dialectOptions.RightParenRune)
	} else {
		compound.RHS().AppendSQL(b)
	}

}

func (d *sqlDialect) expressionMapSQL(b sb.SQLBuilder, ex exp.Ex) {
	expressionList, err := ex.ToExpressions()
	if err != nil {
		b.SetError(err)
		return
	}
	d.Literal(b, expressionList)
}

func (d *sqlDialect) expressionOrMapSQL(b sb.SQLBuilder, ex exp.ExOr) {
	expressionList, err := ex.ToExpressions()
	if err != nil {
		b.SetError(err)
		return
	}
	d.Literal(b, expressionList)
}
