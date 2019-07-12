package goqu

import (
	"database/sql/driver"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/doug-martin/goqu/v7/exp"
	"github.com/doug-martin/goqu/v7/internal/errors"
	"github.com/doug-martin/goqu/v7/internal/sb"
	"github.com/doug-martin/goqu/v7/internal/util"
)

type (
	// An adapter interface to be used by a Dataset to generate SQL for a specific dialect.
	// See DefaultAdapter for a concrete implementation and examples.
	SQLDialect interface {
		ToSelectSQL(b sb.SQLBuilder, clauses exp.Clauses)
		ToUpdateSQL(b sb.SQLBuilder, clauses exp.Clauses, update interface{})
		ToInsertSQL(b sb.SQLBuilder, clauses exp.Clauses, ie exp.InsertExpression)
		ToDeleteSQL(b sb.SQLBuilder, clauses exp.Clauses)
		ToTruncateSQL(b sb.SQLBuilder, clauses exp.Clauses, options exp.TruncateOptions)
	}
	// The default adapter. This class should be used when building a new adapter. When creating a new adapter you can
	// either override methods, or more typically update default values.
	// See (github.com/doug-martin/goqu/adapters/postgres)
	sqlDialect struct {
		dialectOptions *SQLDialectOptions
	}
)

var (
	replacementRune = '?'
	dialects        = make(map[string]SQLDialect)
	TrueLiteral     = exp.NewLiteralExpression("TRUE")
	FalseLiteral    = exp.NewLiteralExpression("FALSE")

	errCTENotSupported              = errors.New("adapter does not support CTE with clause")
	errRecursiveCTENotSupported     = errors.New("adapter does not support CTE with recursive clause")
	errNoUpdatedValuesProvided      = errors.New("no update values provided")
	errConflictUpdateValuesRequired = errors.New("values are required for on conflict update expression")
	errUpsertWithWhereNotSupported  = errors.New("adapter does not support upsert with where clause")
	errNoSourceForUpdate            = errors.New("no source found when generating update sql")
	errNoSourceForInsert            = errors.New("no source found when generating insert sql")
	errNoSourceForDelete            = errors.New("no source found when generating delete sql")
	errNoSourceForTruncate          = errors.New("no source found when generating truncate sql")
	errReturnNotSupported           = errors.New("adapter does not support RETURNING clause")
)

func notSupportedFragmentErr(sqlType string, f SQLFragmentType) error {
	return errors.New("unsupported %s SQL fragment %s", sqlType, f)
}

func notSupportedJoinTypeErr(j exp.JoinExpression) error {
	return errors.New("dialect does not support %v", j.JoinType())
}

func joinConditionRequiredErr(j exp.JoinExpression) error {
	return errors.New("join condition required for conditioned join %v", j.JoinType())
}

func misMatchedRowLengthErr(expectedL, actualL int) error {
	return errors.New("rows with different value length expected %d got %d", expectedL, actualL)
}

func unsupportedExpressionTypeErr(e exp.Expression) error {
	return errors.New("unsupported expression type %T", e)
}

func unsupportedIdentifierExpressionErr(t interface{}) error {
	return errors.New("unexpected col type must be string or LiteralExpression %+v", t)
}

func unsupportedBooleanExpressionOperator(op exp.BooleanOperation) error {
	return errors.New("boolean operator %+v not supported", op)
}

func unsupportedRangeExpressionOperator(op exp.RangeOperation) error {
	return errors.New("range operator %+v not supported", op)
}

func init() {
	RegisterDialect("default", DefaultDialectOptions())
}

func RegisterDialect(name string, do *SQLDialectOptions) {
	dialects[strings.ToLower(name)] = newDialect(do)
}

func DeregisterDialect(name string) {
	delete(dialects, strings.ToLower(name))
}

func GetDialect(name string) SQLDialect {
	name = strings.ToLower(name)
	if d, ok := dialects[name]; ok {
		return d
	}
	return newDialect(DefaultDialectOptions())
}

func newDialect(do *SQLDialectOptions) SQLDialect {
	return &sqlDialect{dialectOptions: do}
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

func (d *sqlDialect) ToSelectSQL(b sb.SQLBuilder, clauses exp.Clauses) {
	for _, f := range d.dialectOptions.SelectSQLOrder {
		if b.Error() != nil {
			return
		}
		switch f {
		case CommonTableSQLFragment:
			d.CommonTablesSQL(b, clauses.CommonTables())
		case SelectSQLFragment:
			if clauses.HasSelectDistinct() {
				d.SelectDistinctSQL(b, clauses.SelectDistinct())
			} else {
				d.SelectSQL(b, clauses.Select())
			}
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
			b.SetError(notSupportedFragmentErr("SELECT", f))
		}
	}
}

func (d *sqlDialect) ToUpdateSQL(b sb.SQLBuilder, clauses exp.Clauses, update interface{}) {
	updates, err := exp.NewUpdateExpressions(update)
	if err != nil {
		b.SetError(err)
		return
	}
	if !clauses.HasSources() {
		b.SetError(errNoSourceForUpdate)
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
			d.SourcesSQL(b, clauses.From())
		case UpdateSQLFragment:
			d.UpdateExpressionsSQL(b, updates...)
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
			b.SetError(notSupportedFragmentErr("UPDATE", f))
		}
	}
}

func (d *sqlDialect) ToInsertSQL(
	b sb.SQLBuilder,
	clauses exp.Clauses,
	ie exp.InsertExpression,
) {
	if !clauses.HasSources() {
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
			d.InsertBeginSQL(b, ie.OnConflict())
		case SourcesSQLFragment:
			d.SourcesSQL(b, clauses.From())
		case InsertSQLFragment:
			d.InsertSQL(b, ie)
		case ReturningSQLFragment:
			d.ReturningSQL(b, clauses.Returning())
		default:
			b.SetError(notSupportedFragmentErr("INSERT", f))
		}
	}

}

func (d *sqlDialect) ToDeleteSQL(b sb.SQLBuilder, clauses exp.Clauses) {
	if !clauses.HasSources() {
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
			d.FromSQL(b, clauses.From())
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
			b.SetError(notSupportedFragmentErr("DELETE", f))
		}
	}
}

func (d *sqlDialect) ToTruncateSQL(b sb.SQLBuilder, clauses exp.Clauses, opts exp.TruncateOptions) {
	if !clauses.HasSources() {
		b.SetError(errNoSourceForTruncate)
		return
	}
	for _, f := range d.dialectOptions.TruncateSQLOrder {
		if b.Error() != nil {
			return
		}
		switch f {
		case TruncateSQLFragment:
			d.TruncateSQL(b, clauses.From(), opts)
		default:
			b.SetError(notSupportedFragmentErr("TRUNCATE", f))
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
	if b.Error() != nil {
		return
	}
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
func (d *sqlDialect) InsertSQL(b sb.SQLBuilder, ie exp.InsertExpression) {
	switch {
	case b.Error() != nil:
		return
	case ie.IsInsertFrom():
		d.insertFromSQL(b, ie.From())
	case ie.IsEmpty():
		d.defaultValuesSQL(b)
	default:
		d.insertColumnsSQL(b, ie.Cols())
		d.insertValuesSQL(b, ie.Vals())
	}
	d.onConflictSQL(b, ie.OnConflict())
}

// Adds column setters in an update SET clause
func (d *sqlDialect) UpdateExpressionsSQL(b sb.SQLBuilder, updates ...exp.UpdateExpression) {
	if b.Error() != nil {
		return
	}
	b.Write(d.dialectOptions.SetFragment)
	d.updateValuesSQL(b, updates...)

}

// Adds the SELECT clause and columns to a sql statement
func (d *sqlDialect) SelectSQL(b sb.SQLBuilder, cols exp.ColumnListExpression) {
	if b.Error() != nil {
		return
	}
	b.Write(d.dialectOptions.SelectClause).
		WriteRunes(d.dialectOptions.SpaceRune)
	if len(cols.Columns()) == 0 {
		b.WriteRunes(d.dialectOptions.StarRune)
	} else {
		d.Literal(b, cols)
	}
}

// Adds the SELECT DISTINCT clause and columns to a sql statement
func (d *sqlDialect) SelectDistinctSQL(b sb.SQLBuilder, cols exp.ColumnListExpression) {
	if b.Error() != nil {
		return
	}
	b.Write(d.dialectOptions.SelectClause).Write(d.dialectOptions.DistinctFragment)
	d.Literal(b, cols)
}

func (d *sqlDialect) ReturningSQL(b sb.SQLBuilder, returns exp.ColumnListExpression) {
	if b.Error() != nil {
		return
	}
	if returns != nil && len(returns.Columns()) > 0 {
		if d.SupportsReturn() {
			b.Write(d.dialectOptions.ReturningFragment)
			d.Literal(b, returns)
		} else {
			b.SetError(errReturnNotSupported)
		}
	}

}

// Adds the FROM clause and tables to an sql statement
func (d *sqlDialect) FromSQL(b sb.SQLBuilder, from exp.ColumnListExpression) {
	if b.Error() != nil {
		return
	}
	if from != nil && len(from.Columns()) > 0 {
		b.Write(d.dialectOptions.FromFragment)
		d.SourcesSQL(b, from)
	}
}

// Adds the generates the SQL for a column list
func (d *sqlDialect) SourcesSQL(b sb.SQLBuilder, from exp.ColumnListExpression) {
	if b.Error() != nil {
		return
	}
	b.WriteRunes(d.dialectOptions.SpaceRune)
	d.Literal(b, from)
}

// Generates the JOIN clauses for an SQL statement
func (d *sqlDialect) JoinSQL(b sb.SQLBuilder, joins exp.JoinExpressions) {
	if b.Error() != nil {
		return
	}
	if len(joins) > 0 {
		for _, j := range joins {
			joinType, ok := d.dialectOptions.JoinTypeLookup[j.JoinType()]
			if !ok {
				b.SetError(notSupportedJoinTypeErr(j))
				return
			}
			b.Write(joinType)
			d.Literal(b, j.Table())
			if t, ok := j.(exp.ConditionedJoinExpression); ok {
				if t.IsConditionEmpty() {
					b.SetError(joinConditionRequiredErr(j))
					return
				}
				d.joinConditionSQL(b, t.Condition())
			}
		}
	}
}

// Generates the WHERE clause for an SQL statement
func (d *sqlDialect) WhereSQL(b sb.SQLBuilder, where exp.ExpressionList) {
	if b.Error() != nil {
		return
	}
	if where != nil && !where.IsEmpty() {
		b.Write(d.dialectOptions.WhereFragment)
		d.Literal(b, where)
	}
}

// Generates the GROUP BY clause for an SQL statement
func (d *sqlDialect) GroupBySQL(b sb.SQLBuilder, groupBy exp.ColumnListExpression) {
	if b.Error() != nil {
		return
	}
	if groupBy != nil && len(groupBy.Columns()) > 0 {
		b.Write(d.dialectOptions.GroupByFragment)
		d.Literal(b, groupBy)
	}
}

// Generates the HAVING clause for an SQL statement
func (d *sqlDialect) HavingSQL(b sb.SQLBuilder, having exp.ExpressionList) {
	if b.Error() != nil {
		return
	}
	if having != nil && len(having.Expressions()) > 0 {
		b.Write(d.dialectOptions.HavingFragment)
		d.Literal(b, having)
	}
}

// Generates the ORDER BY clause for an SQL statement
func (d *sqlDialect) OrderSQL(b sb.SQLBuilder, order exp.ColumnListExpression) {
	if b.Error() != nil {
		return
	}
	if order != nil && len(order.Columns()) > 0 {
		b.Write(d.dialectOptions.OrderByFragment)
		d.Literal(b, order)
	}
}

// Generates the LIMIT clause for an SQL statement
func (d *sqlDialect) LimitSQL(b sb.SQLBuilder, limit interface{}) {
	if b.Error() != nil {
		return
	}
	if limit != nil {
		b.Write(d.dialectOptions.LimitFragment)
		d.Literal(b, limit)
	}
}

// Generates the OFFSET clause for an SQL statement
func (d *sqlDialect) OffsetSQL(b sb.SQLBuilder, offset uint) {
	if b.Error() != nil {
		return
	}
	if offset > 0 {
		b.Write(d.dialectOptions.OffsetFragment)
		d.Literal(b, offset)
	}
}

// Generates the sql for the WITH clauses for common table expressions (CTE)
func (d *sqlDialect) CommonTablesSQL(b sb.SQLBuilder, ctes []exp.CommonTableExpression) {
	if b.Error() != nil {
		return
	}
	if l := len(ctes); l > 0 {
		if !d.dialectOptions.SupportsWithCTE {
			b.SetError(errCTENotSupported)
			return
		}
		b.Write(d.dialectOptions.WithFragment)
		anyRecursive := false
		for _, cte := range ctes {
			anyRecursive = anyRecursive || cte.IsRecursive()
		}
		if anyRecursive {
			if !d.dialectOptions.SupportsWithCTERecursive {
				b.SetError(errRecursiveCTENotSupported)
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
	if b.Error() != nil {
		return
	}
	for _, compound := range compounds {
		d.Literal(b, compound)
	}
}

// Generates the FOR (aka "locking") clause for an SQL statement
func (d *sqlDialect) ForSQL(b sb.SQLBuilder, lockingClause exp.Lock) {
	if b.Error() != nil {
		return
	}
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
			b.SetError(misMatchedRowLengthErr(rowLen, len(row)))
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
	if o.WhereClause() != nil {
		if !d.dialectOptions.SupportsConflictUpdateWhere {
			b.SetError(errUpsertWithWhereNotSupported)
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
		b.SetError(unsupportedExpressionTypeErr(e))
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
		b.SetError(unsupportedIdentifierExpressionErr(col))
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
	d.Literal(b, t.UTC().Format(d.dialectOptions.TimeFormat))
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
	if operator.RHS() == nil {
		switch operatorOp {
		case exp.EqOp:
			operatorOp = exp.IsOp
		case exp.NeqOp:
			operatorOp = exp.IsNotOp
		}
	}
	if val, ok := d.dialectOptions.BooleanOperatorLookup[operatorOp]; ok {
		b.Write(val)
	} else {
		b.SetError(unsupportedBooleanExpressionOperator(operatorOp))
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
		b.SetError(unsupportedRangeExpressionOperator(operatorOp))
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
