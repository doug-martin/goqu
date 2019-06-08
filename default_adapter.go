package goqu

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

var (
	replacement_rune                   = '?'
	comma_rune                         = ','
	space_rune                         = ' '
	left_paren_rune                    = '('
	right_paren_rune                   = ')'
	star_rune                          = '*'
	default_quote                      = '"'
	period_rune                        = '.'
	empty_string                       = ""
	default_null                       = []byte("NULL")
	default_true                       = []byte("TRUE")
	default_false                      = []byte("FALSE")
	default_update_clause              = []byte("UPDATE")
	default_insert_clause              = []byte("INSERT INTO")
	default_select_clause              = []byte("SELECT")
	default_delete_clause              = []byte("DELETE")
	default_truncate_clause            = []byte("TRUNCATE")
	default_with_fragment              = []byte("WITH ")
	default_recursive_fragment         = []byte("RECURSIVE ")
	default_cascade_fragment           = []byte(" CASCADE")
	default_retrict_fragment           = []byte(" RESTRICT")
	default_default_values_fragment    = []byte(" DEFAULT VALUES")
	default_values_fragment            = []byte(" VALUES ")
	default_identity_fragment          = []byte(" IDENTITY")
	default_set_fragment               = []byte(" SET ")
	default_distinct_fragment          = []byte(" DISTINCT ")
	default_returning_fragment         = []byte(" RETURNING ")
	default_from_fragment              = []byte(" FROM")
	default_where_fragment             = []byte(" WHERE ")
	default_group_by_fragment          = []byte(" GROUP BY ")
	default_having_fragment            = []byte(" HAVING ")
	default_order_by_fragment          = []byte(" ORDER BY ")
	default_limit_fragment             = []byte(" LIMIT ")
	default_offset_fragment            = []byte(" OFFSET ")
	default_for_update_fragment        = []byte(" FOR UPDATE ")
	default_for_no_key_update_fragment = []byte(" FOR NO KEY UPDATE ")
	default_for_share_fragment         = []byte(" FOR SHARE ")
	default_for_key_share_fragment     = []byte(" FOR KEY SHARE ")
	default_nowait_fragment            = []byte("NOWAIT")
	default_skip_locked_fragment       = []byte("SKIP LOCKED")
	default_as_fragment                = []byte(" AS ")
	default_asc_fragment               = []byte(" ASC")
	default_desc_fragment              = []byte(" DESC")
	default_nulls_first_fragment       = []byte(" NULLS FIRST")
	default_nulls_last_fragment        = []byte(" NULLS LAST")
	default_and_fragment               = []byte(" AND ")
	default_or_fragment                = []byte(" OR ")
	default_union_fragment             = []byte(" UNION ")
	default_union_all_fragment         = []byte(" UNION ALL ")
	default_intersect_fragment         = []byte(" INTERSECT ")
	default_intersect_all_fragment     = []byte(" INTERSECT ALL ")
	default_set_operator_rune          = '='
	default_string_quote_rune          = '\''
	default_place_holder_rune          = '?'
	default_operator_lookup            = map[BooleanOperation][]byte{
		EQ_OP:                []byte("="),
		NEQ_OP:               []byte("!="),
		GT_OP:                []byte(">"),
		GTE_OP:               []byte(">="),
		LT_OP:                []byte("<"),
		LTE_OP:               []byte("<="),
		IN_OP:                []byte("IN"),
		NOT_IN_OP:            []byte("NOT IN"),
		IS_OP:                []byte("IS"),
		IS_NOT_OP:            []byte("IS NOT"),
		LIKE_OP:              []byte("LIKE"),
		NOT_LIKE_OP:          []byte("NOT LIKE"),
		I_LIKE_OP:            []byte("ILIKE"),
		NOT_I_LIKE_OP:        []byte("NOT ILIKE"),
		REGEXP_LIKE_OP:       []byte("~"),
		REGEXP_NOT_LIKE_OP:   []byte("!~"),
		REGEXP_I_LIKE_OP:     []byte("~*"),
		REGEXP_NOT_I_LIKE_OP: []byte("!~*"),
	}
	default_rangeop_lookup = map[RangeOperation][]byte{
		BETWEEN_OP:  []byte("BETWEEN"),
		NBETWEEN_OP: []byte("NOT BETWEEN"),
	}
	default_join_lookup = map[JoinType][]byte{
		INNER_JOIN:         []byte(" INNER JOIN "),
		FULL_OUTER_JOIN:    []byte(" FULL OUTER JOIN "),
		RIGHT_OUTER_JOIN:   []byte(" RIGHT OUTER JOIN "),
		LEFT_OUTER_JOIN:    []byte(" LEFT OUTER JOIN "),
		FULL_JOIN:          []byte(" FULL JOIN "),
		RIGHT_JOIN:         []byte(" RIGHT JOIN "),
		LEFT_JOIN:          []byte(" LEFT JOIN "),
		NATURAL_JOIN:       []byte(" NATURAL JOIN "),
		NATURAL_LEFT_JOIN:  []byte(" NATURAL LEFT JOIN "),
		NATURAL_RIGHT_JOIN: []byte(" NATURAL RIGHT JOIN "),
		NATURAL_FULL_JOIN:  []byte(" NATURAL FULL JOIN "),
		CROSS_JOIN:         []byte(" CROSS JOIN "),
	}
	default_escape_runes = map[rune][]byte{
		'\'': []byte("''"),
	}
	default_conflict_fragment         = []byte(" ON CONFLICT")
	default_conflict_nothing_fragment = []byte(" DO NOTHING")
	default_conflict_update_fragment  = []byte(" DO UPDATE SET ")
)

type (
	//The default adapter. This class should be used when building a new adapter. When creating a new adapter you can either override methods, or more typically update default values. See (github.com/doug-martin/goqu/adapters/postgres)
	DefaultAdapter struct {
		Adapter
		dataset *Dataset
		//The UPDATE fragment to use when generating sql. (DEFAULT=[]byte("UPDATE"))
		UpdateClause []byte
		//The INSERT fragment to use when generating sql. (DEFAULT=[]byte("INSERT INTO"))
		InsertClause []byte
		//The INSERT IGNORE INTO fragment to use when generating sql. (DEFAULT=[]byte("INSERT INTO"))
		InsertIgnoreClause []byte
		//The SELECT fragment to use when generating sql. (DEFAULT=[]byte("SELECT"))
		SelectClause []byte
		//The DELETE fragment to use when generating sql. (DEFAULT=[]byte("DELETE"))
		DeleteClause []byte
		//The TRUNCATE fragment to use when generating sql. (DEFAULT=[]byte("TRUNCATE"))
		TruncateClause []byte
		//The WITH fragment to use when generating sql. (DEFAULT=[]byte("WITH "))
		WithFragment []byte
		//The RECURSIVE fragment to use when generating sql (after WITH). (DEFAULT=[]byte("RECURSIVE "))
		RecursiveFragment []byte
		//The CASCADE fragment to use when generating sql. (DEFAULT=[]byte(" CASCADE"))
		CascadeFragment []byte
		//The RESTRICT fragment to use when generating sql. (DEFAULT=[]byte(" RESTRICT"))
		RestrictFragment []byte
		//The SQL fragment to use when generating insert sql and using DEFAULT VALUES (e.g. postgres="DEFAULT VALUES", mysql="", sqlite3=""). (DEFAULT=[]byte(" DEFAULT VALUES"))
		DefaultValuesFragment []byte
		//The SQL fragment to use when generating insert sql and listing columns using a VALUES clause (DEFAULT=[]byte(" VALUES "))
		ValuesFragment []byte
		//The SQL fragment to use when generating truncate sql and using the IDENTITY clause (DEFAULT=[]byte(" IDENTITY"))
		IdentityFragment []byte
		//The SQL fragment to use when generating update sql and using the SET clause (DEFAULT=[]byte(" SET "))
		SetFragment []byte
		//The SQL DISTINCT keyword (DEFAULT=[]byte(" DISTINCT "))
		DistinctFragment []byte
		//The SQL RETURNING clause (DEFAULT=[]byte(" RETURNING "))
		ReturningFragment []byte
		//The SQL FROM clause fragment (DEFAULT=[]byte(" FROM"))
		FromFragment []byte
		//The SQL WHERE clause fragment (DEFAULT=[]byte(" WHERE"))
		WhereFragment []byte
		//The SQL GROUP BY clause fragment(DEFAULT=[]byte(" GROUP BY "))
		GroupByFragment []byte
		//The SQL HAVING clause fragment(DELiFAULT=[]byte(" HAVING "))
		HavingFragment []byte
		//The SQL ORDER BY clause fragment(DEFAULT=[]byte(" ORDER BY "))
		OrderByFragment []byte
		//The SQL LIMIT BY clause fragment(DEFAULT=[]byte(" LIMIT "))
		LimitFragment []byte
		//The SQL OFFSET BY clause fragment(DEFAULT=[]byte(" OFFSET "))
		OffsetFragment []byte
		// The SQL FOR UPDATE fragment(DEFAULT=[]byte(" FOR UPDATE "))
		ForUpdateFragment []byte
		// The SQL FOR NO KEY UPDATE fragment(DEFAULT=[]byte(" FOR NO KEY UPDATE "))
		ForNoKeyUpdateFragment []byte
		// The SQL FOR SHARE fragment(DEFAULT=[]byte(" FOR SHARE "))
		ForShareFragment []byte
		// The SQL FOR KEY SHARE fragment(DEFAULT=[]byte(" FOR KEY SHARE "))
		ForKeyShareFragment []byte
		// The SQL NOWAIT fragment(DEFAULT=[]byte("NOWAIT"))
		NowaitFragment []byte
		// The SQL SKIP LOCKED fragment(DEFAULT=[]byte("SKIP LOCKED"))
		SkipLockedFragment []byte
		//The SQL AS fragment when aliasing an Expression(DEFAULT=[]byte(" AS "))
		AsFragment []byte
		//The quote rune to use when quoting identifiers(DEFAULT='"')
		QuoteRune rune
		//The NULL literal to use when interpolating nulls values (DEFAULT=[]byte("NULL"))
		Null []byte
		//The TRUE literal to use when interpolating bool true values (DEFAULT=[]byte("TRUE"))
		True []byte
		//The FALSE literal to use when interpolating bool false values (DEFAULT=[]byte("FALSE"))
		False []byte
		//The ASC fragment when specifying column order (DEFAULT=[]byte(" ASC"))
		AscFragment []byte
		//The DESC fragment when specifying column order (DEFAULT=[]byte(" DESC"))
		DescFragment []byte
		//The NULLS FIRST fragment when specifying column order (DEFAULT=[]byte(" NULLS FIRST"))
		NullsFirstFragment []byte
		//The NULLS LAST fragment when specifying column order (DEFAULT=[]byte(" NULLS LAST"))
		NullsLastFragment []byte
		//The AND keyword used when joining ExpressionLists (DEFAULT=[]byte(" AND "))
		AndFragment []byte
		//The OR keyword used when joining ExpressionLists (DEFAULT=[]byte(" OR "))
		OrFragment []byte
		//The UNION keyword used when creating compound statements (DEFAULT=[]byte(" UNION "))
		UnionFragment []byte
		//The UNION ALL keyword used when creating compound statements (DEFAULT=[]byte(" UNION ALL "))
		UnionAllFragment []byte
		//The INTERSECT keyword used when creating compound statements (DEFAULT=[]byte(" INTERSECT "))
		IntersectFragment []byte
		//The INTERSECT ALL keyword used when creating compound statements (DEFAULT=[]byte(" INTERSECT ALL "))
		IntersectAllFragment []byte
		//The quote rune to use when quoting string literals (DEFAULT='\'')
		StringQuote rune
		//The operator to use when setting values in an update statement (DEFAULT='=')
		SetOperatorRune rune
		//The placeholder rune to use when generating a non interpolated statement (DEFAULT='?')
		PlaceHolderRune rune
		//Set to true to include positional argument numbers when creating a prepared statement
		IncludePlaceholderNum bool
		//The time format to use when serializing time.Time (DEFAULT=time.RFC3339Nano)
		TimeFormat string
		//A map used to look up BooleanOperations and their SQL equivalents
		BooleanOperatorLookup map[BooleanOperation][]byte
		//A map used to look up RangeOperations and their SQL equivalents
		RangeOperatorLookup map[RangeOperation][]byte
		//A map used to look up JoinTypes and their SQL equivalents
		JoinTypeLookup map[JoinType][]byte
		//Whether or not to use literal TRUE or FALSE for IS statements (e.g. IS TRUE or IS 0)
		UseLiteralIsBools bool
		//EscapedRunes is a map of a rune and the corresponding escape sequence in bytes. Used when escaping text types.
		EscapedRunes map[rune][]byte

		ConflictFragment             []byte
		ConflictDoNothingFragment    []byte
		ConflictDoUpdateFragment     []byte
		ConflictTargetSupported      bool
		ConflictUpdateWhereSupported bool
		InsertIgnoreSyntaxSupported  bool
		WithCTESupported             bool
		WithCTERecursiveSupported    bool
	}
)

func NewDefaultAdapter(ds *Dataset) Adapter {
	return &DefaultAdapter{
		dataset:                      ds,
		UpdateClause:                 default_update_clause,
		InsertClause:                 default_insert_clause,
		SelectClause:                 default_select_clause,
		DeleteClause:                 default_delete_clause,
		TruncateClause:               default_truncate_clause,
		WithFragment:                 default_with_fragment,
		RecursiveFragment:            default_recursive_fragment,
		CascadeFragment:              default_cascade_fragment,
		RestrictFragment:             default_retrict_fragment,
		DefaultValuesFragment:        default_default_values_fragment,
		ValuesFragment:               default_values_fragment,
		IdentityFragment:             default_identity_fragment,
		SetFragment:                  default_set_fragment,
		DistinctFragment:             default_distinct_fragment,
		ReturningFragment:            default_returning_fragment,
		FromFragment:                 default_from_fragment,
		WhereFragment:                default_where_fragment,
		GroupByFragment:              default_group_by_fragment,
		HavingFragment:               default_having_fragment,
		OrderByFragment:              default_order_by_fragment,
		LimitFragment:                default_limit_fragment,
		OffsetFragment:               default_offset_fragment,
		ForUpdateFragment:            default_for_update_fragment,
		ForNoKeyUpdateFragment:       default_for_no_key_update_fragment,
		ForShareFragment:             default_for_share_fragment,
		ForKeyShareFragment:          default_for_key_share_fragment,
		NowaitFragment:               default_nowait_fragment,
		SkipLockedFragment:           default_skip_locked_fragment,
		AsFragment:                   default_as_fragment,
		QuoteRune:                    default_quote,
		Null:                         default_null,
		True:                         default_true,
		False:                        default_false,
		StringQuote:                  default_string_quote_rune,
		AscFragment:                  default_asc_fragment,
		DescFragment:                 default_desc_fragment,
		NullsFirstFragment:           default_nulls_first_fragment,
		NullsLastFragment:            default_nulls_last_fragment,
		AndFragment:                  default_and_fragment,
		OrFragment:                   default_or_fragment,
		SetOperatorRune:              default_set_operator_rune,
		UnionFragment:                default_union_fragment,
		UnionAllFragment:             default_union_all_fragment,
		IntersectFragment:            default_intersect_fragment,
		IntersectAllFragment:         default_intersect_all_fragment,
		PlaceHolderRune:              default_place_holder_rune,
		BooleanOperatorLookup:        default_operator_lookup,
		RangeOperatorLookup:          default_rangeop_lookup,
		JoinTypeLookup:               default_join_lookup,
		TimeFormat:                   time.RFC3339Nano,
		UseLiteralIsBools:            true,
		EscapedRunes:                 default_escape_runes,
		ConflictFragment:             default_conflict_fragment,
		ConflictDoUpdateFragment:     default_conflict_update_fragment,
		ConflictDoNothingFragment:    default_conflict_nothing_fragment,
		ConflictUpdateWhereSupported: true,
		InsertIgnoreSyntaxSupported:  false,
		ConflictTargetSupported:      true,
		WithCTESupported:             true,
		WithCTERecursiveSupported:    true,
	}
}

//Override to prevent return statements from being generated when creating SQL
func (me *DefaultAdapter) SupportsReturn() bool {
	return true
}

//Override to allow LIMIT on DELETE statements
func (me *DefaultAdapter) SupportsLimitOnDelete() bool {
	return false
}

//Override to allow LIMIT on UPDATE statements
func (me *DefaultAdapter) SupportsLimitOnUpdate() bool {
	return false
}

//Override to allow ORDER BY on DELETE statements
func (me *DefaultAdapter) SupportsOrderByOnDelete() bool {
	return false
}

//Override to allow ORDER BY on UPDATE statements
func (me *DefaultAdapter) SupportsConflictUpdateWhere() bool {
	return me.ConflictUpdateWhereSupported
}

//Override to allow ORDER BY on UPDATE statements
func (me *DefaultAdapter) SupportsConflictTarget() bool {
	return me.ConflictTargetSupported
}

//Override to allow ORDER BY on UPDATE statements
func (me *DefaultAdapter) SupportsOrderByOnUpdate() bool {
	return false
}

func (me *DefaultAdapter) SupportsInsertIgnoreSyntax() bool {
	return me.InsertIgnoreSyntaxSupported
}

func (me *DefaultAdapter) SupportConflictUpdateWhere() bool {
	return me.ConflictUpdateWhereSupported
}

func (me *DefaultAdapter) SupportsWithCTE() bool {
	return me.WithCTESupported
}

func (me *DefaultAdapter) SupportsWithRecursiveCTE() bool {
	return me.WithCTERecursiveSupported
}

//This is a proxy to Dataset.Literal. Used internally to ensure the correct method is called on any subclasses and to prevent duplication of code
func (me *DefaultAdapter) Literal(buf *SqlBuilder, val interface{}) error {
	return me.dataset.Literal(buf, val)
}

//Generates a placeholder (e.g. ?, $1)
func (me *DefaultAdapter) PlaceHolderSql(buf *SqlBuilder, i interface{}) error {
	buf.WriteRune(me.PlaceHolderRune)
	if me.IncludePlaceholderNum {
		buf.WriteString(strconv.FormatInt(int64(buf.CurrentArgPosition), 10))
	}
	buf.WriteArg(i)
	return nil
}

//Adds the correct fragment to being an UPDATE statement
func (me *DefaultAdapter) UpdateBeginSql(buf *SqlBuilder) error {
	buf.Write(me.UpdateClause)
	return nil
}

//Adds the correct fragment to being an INSERT statement
func (me *DefaultAdapter) InsertBeginSql(buf *SqlBuilder, o ConflictExpression) error {
	if me.SupportsInsertIgnoreSyntax() && o != nil {
		buf.Write(me.InsertIgnoreClause)
	} else {
		buf.Write(me.InsertClause)
	}
	return nil
}

//Adds the correct fragment to being an DELETE statement
func (me *DefaultAdapter) DeleteBeginSql(buf *SqlBuilder) error {
	buf.Write(me.DeleteClause)
	return nil
}

//Generates a TRUNCATE statement
func (me *DefaultAdapter) TruncateSql(buf *SqlBuilder, from ColumnList, opts TruncateOptions) error {
	buf.Write(me.TruncateClause)
	if err := me.SourcesSql(buf, from); err != nil {
		return err
	}
	if opts.Identity != empty_string {
		buf.WriteRune(space_rune)
		buf.WriteString(strings.ToUpper(opts.Identity))
		buf.Write(me.IdentityFragment)
	}
	if opts.Cascade {
		buf.Write(me.CascadeFragment)
	} else if opts.Restrict {
		buf.Write(me.RestrictFragment)
	}
	return nil
}

//Adds the DefaultValuesFragment to an SQL statement
func (me *DefaultAdapter) DefaultValuesSql(buf *SqlBuilder) error {
	buf.Write(me.DefaultValuesFragment)
	return nil
}

//Adds the columns list to an insert statement
func (me *DefaultAdapter) InsertColumnsSql(buf *SqlBuilder, cols ColumnList) error {
	buf.WriteRune(space_rune)
	buf.WriteRune(left_paren_rune)
	if err := me.Literal(buf, cols); err != nil {
		return err
	}
	buf.WriteRune(right_paren_rune)
	return nil
}

//Adds the values clause to an SQL statement
func (me *DefaultAdapter) InsertValuesSql(buf *SqlBuilder, values [][]interface{}) error {
	buf.Write(me.ValuesFragment)
	rowLen := len(values[0])
	valueLen := len(values)
	for i, row := range values {
		if len(row) != rowLen {
			return fmt.Errorf("Rows with different value length expected %d got %d", rowLen, len(row))
		}
		if err := me.Literal(buf, row); err != nil {
			return err
		}
		if i < valueLen-1 {
			buf.WriteRune(comma_rune)
			buf.WriteRune(space_rune)
		}
	}
	return nil
}

//Adds the DefaultValuesFragment to an SQL statement
func (me *DefaultAdapter) OnConflictSql(buf *SqlBuilder, o ConflictExpression) error {
	if o == nil {
		return nil
	}
	buf.Write(me.ConflictFragment)
	if u := o.Updates(); u != nil {
		target := u.Target
		if me.SupportsConflictTarget() && target != "" {
			wrapParens := !strings.HasPrefix(strings.ToLower(target), "on constraint")

			buf.Write([]byte(" "))
			if wrapParens {
				buf.Write([]byte("("))
			}
			buf.Write([]byte(target))
			if wrapParens {
				buf.Write([]byte(")"))
			}
		}
		if err := me.onConflictDoUpdateSql(buf, *u); err != nil {
			return err
		}
	} else {
		buf.Write(me.ConflictDoNothingFragment)
	}
	return nil
}

func (me *DefaultAdapter) onConflictDoUpdateSql(buf *SqlBuilder, o ConflictUpdate) error {
	buf.Write(me.ConflictDoUpdateFragment)
	update := o.Update
	if update == nil {
		return NewGoquError("Values are required")
	}
	exp, err := me.dataset.getUpdateExpressions(update)
	if err != nil {
		return err
	}
	if err := me.updateValuesSql(buf, exp...); err != nil {
		return err
	}
	if o.WhereClause != nil {
		if !me.SupportsConflictUpdateWhere() {
			return NewGoquError("Adapter does not support upsert with where clause")
		}

		if err := me.WhereSql(buf, o.WhereClause); err != nil {
			return err
		}
	}
	return nil
}

//Adds column setters in an update SET clause
func (me *DefaultAdapter) UpdateExpressionsSql(buf *SqlBuilder, updates ...UpdateExpression) error {
	buf.Write(me.SetFragment)
	return me.updateValuesSql(buf, updates...)

}

//Adds column setters in an update SET clause
func (me *DefaultAdapter) updateValuesSql(buf *SqlBuilder, updates ...UpdateExpression) error {
	if len(updates) == 0 {
		return NewGoquError("No update values provided")
	}
	updateLen := len(updates)
	for i, update := range updates {
		if err := me.Literal(buf, update); err != nil {
			return err
		}
		if i < updateLen-1 {
			buf.WriteRune(comma_rune)
		}
	}
	return nil

}

//Adds the SELECT clause and columns to a sql statement
func (me *DefaultAdapter) SelectSql(buf *SqlBuilder, cols ColumnList) error {
	buf.Write(me.SelectClause)
	buf.WriteRune(space_rune)
	if len(cols.Columns()) == 0 {
		buf.WriteRune(star_rune)
	} else {
		return me.Literal(buf, cols)
	}
	return nil
}

//Adds the SELECT DISTINCT clause and columns to a sql statement
func (me *DefaultAdapter) SelectDistinctSql(buf *SqlBuilder, cols ColumnList) error {
	buf.Write(me.SelectClause)
	buf.Write(me.DistinctFragment)
	return me.Literal(buf, cols)
}

func (me *DefaultAdapter) ReturningSql(buf *SqlBuilder, returns ColumnList) error {
	if returns != nil && len(returns.Columns()) > 0 {
		buf.Write(me.ReturningFragment)
		return me.Literal(buf, returns)
	}
	return nil
}

//Adds the FROM clause and tables to an sql statement
func (me *DefaultAdapter) FromSql(buf *SqlBuilder, from ColumnList) error {
	if from != nil && len(from.Columns()) > 0 {
		buf.Write(me.FromFragment)
		return me.SourcesSql(buf, from)
	}
	return nil
}

//Adds the generates the SQL for a column list
func (me *DefaultAdapter) SourcesSql(buf *SqlBuilder, from ColumnList) error {
	buf.WriteRune(space_rune)
	return me.Literal(buf, from)
}

//Generates the JOIN clauses for an SQL statement
func (me *DefaultAdapter) JoinSql(buf *SqlBuilder, joins JoiningClauses) error {
	if len(joins) > 0 {
		for _, j := range joins {
			joinType := me.JoinTypeLookup[j.JoinType]
			buf.Write(joinType)
			if err := me.Literal(buf, j.Table); err != nil {
				return err
			}
			if j.IsConditioned {
				buf.WriteRune(space_rune)
				if j.Condition == nil {
					return NewGoquError("Join condition required for conditioned join %s", string(joinType))
				}
				condition := j.Condition
				if condition.JoinCondition() == USING_COND {
					buf.WriteString("USING ")
				} else {
					buf.WriteString("ON ")
				}
				switch condition.(type) {
				case JoinOnExpression:
					if err := me.Literal(buf, condition.(JoinOnExpression).On()); err != nil {
						return err
					}
				case JoinUsingExpression:
					buf.WriteRune(left_paren_rune)
					if err := me.Literal(buf, condition.(JoinUsingExpression).Using()); err != nil {
						return err
					}
					buf.WriteRune(right_paren_rune)
				}
			}
		}
	}
	return nil
}

//Generates the WHERE clause for an SQL statement
func (me *DefaultAdapter) WhereSql(buf *SqlBuilder, where ExpressionList) error {
	if where != nil && len(where.Expressions()) > 0 {
		buf.Write(me.WhereFragment)
		return me.Literal(buf, where)
	}
	return nil
}

//Generates the GROUP BY clause for an SQL statement
func (me *DefaultAdapter) GroupBySql(buf *SqlBuilder, groupBy ColumnList) error {
	if groupBy != nil && len(groupBy.Columns()) > 0 {
		buf.Write(me.GroupByFragment)
		return me.Literal(buf, groupBy)
	}
	return nil
}

//Generates the HAVING clause for an SQL statement
func (me *DefaultAdapter) HavingSql(buf *SqlBuilder, having ExpressionList) error {
	if having != nil && len(having.Expressions()) > 0 {
		buf.Write(me.HavingFragment)
		return me.Literal(buf, having)
	}
	return nil
}

//Generates the sql for the WITH clauses for common table expressions (CTE)
func (me *DefaultAdapter) CommonTablesSql(buf *SqlBuilder, ctes []CommonTableExpression) error {
	if l := len(ctes); l > 0 {
		if !me.SupportsWithCTE() {
			return NewGoquError("Adapter does not support CTE with clause")
		}
		buf.Write(me.WithFragment)
		anyRecursive := false
		for _, cte := range ctes {
			anyRecursive = anyRecursive || cte.IsRecursive()
		}
		if anyRecursive {
			if !me.SupportsWithRecursiveCTE() {
				return NewGoquError("Adapter does not support CTE with recursive clause")
			}
			buf.Write(me.RecursiveFragment)
		}
		for i, cte := range ctes {
			if err := me.Literal(buf, cte); err != nil {
				return err
			}
			if i < l-1 {
				buf.WriteRune(comma_rune)
				buf.WriteRune(space_rune)
			}
		}
		buf.WriteRune(space_rune)
	}
	return nil
}

//Generates the compound sql clause for an SQL statement (e.g. UNION, INTERSECT)
func (me *DefaultAdapter) CompoundsSql(buf *SqlBuilder, compounds []CompoundExpression) error {
	for _, compound := range compounds {
		if err := me.Literal(buf, compound); err != nil {
			return err
		}
	}
	return nil
}

//Generates the ORDER BY clause for an SQL statement
func (me *DefaultAdapter) OrderSql(buf *SqlBuilder, order ColumnList) error {
	if order != nil && len(order.Columns()) > 0 {
		buf.Write(me.OrderByFragment)
		return me.Literal(buf, order)
	}
	return nil
}

//Generates the LIMIT clause for an SQL statement
func (me *DefaultAdapter) LimitSql(buf *SqlBuilder, limit interface{}) error {
	if limit != nil {
		buf.Write(me.LimitFragment)
		return me.Literal(buf, limit)
	}
	return nil
}

//Generates the OFFSET clause for an SQL statement
func (me *DefaultAdapter) OffsetSql(buf *SqlBuilder, offset uint) error {
	if offset > 0 {
		buf.Write(me.OffsetFragment)
		return me.Literal(buf, offset)
	}
	return nil
}

//Generates the FOR (aka "locking") clause for an SQL statement
func (me *DefaultAdapter) ForSql(buf *SqlBuilder, lockingClause Lock) error {
	switch lockingClause.Strength {
	case FOR_NOLOCK:
		return nil
	case FOR_UPDATE:
		buf.Write(me.ForUpdateFragment)
	case FOR_NO_KEY_UPDATE:
		buf.Write(me.ForNoKeyUpdateFragment)
	case FOR_SHARE:
		buf.Write(me.ForShareFragment)
	case FOR_KEY_SHARE:
		buf.Write(me.ForKeyShareFragment)
	}
	// the WAIT case is the default in Postgres, and is what you get if you don't specify NOWAIT or
	// SKIP LOCKED.  There's no special syntax for it in PG, so we don't do anything for it here
	switch lockingClause.WaitOption {
	case NOWAIT:
		buf.Write(me.NowaitFragment)
	case SKIP_LOCKED:
		buf.Write(me.SkipLockedFragment)
	}
	return nil
}

//Generates creates the sql for a sub select on a Dataset
func (me *DefaultAdapter) DatasetSql(buf *SqlBuilder, dataset Dataset) error {
	buf.WriteRune(left_paren_rune)
	if buf.IsPrepared {
		if err := dataset.selectSqlWriteTo(buf); err != nil {
			return err
		}
	} else {
		sql, _, err := dataset.Prepared(false).ToSql()
		if err != nil {
			return err
		}
		buf.WriteString(sql)
	}
	buf.WriteRune(right_paren_rune)
	alias := dataset.GetClauses().Alias
	if alias != nil {
		buf.Write(me.AsFragment)
		return me.Literal(buf, alias)
	}
	return nil
}

//Quotes an identifier (e.g. "col", "table"."col"
func (me *DefaultAdapter) QuoteIdentifier(buf *SqlBuilder, ident IdentifierExpression) error {
	schema, table, col := ident.GetSchema(), ident.GetTable(), ident.GetCol()
	if schema != empty_string {
		buf.WriteRune(me.QuoteRune)
		buf.WriteString(schema)
		buf.WriteRune(me.QuoteRune)
	}
	if table != empty_string {
		if schema != empty_string {
			buf.WriteRune(period_rune)
		}
		buf.WriteRune(me.QuoteRune)
		buf.WriteString(table)
		buf.WriteRune(me.QuoteRune)
	}
	switch col.(type) {
	case nil:
	case string:
		if table != empty_string || schema != empty_string {
			buf.WriteRune(period_rune)
		}
		buf.WriteRune(me.QuoteRune)
		buf.WriteString(col.(string))
		buf.WriteRune(me.QuoteRune)
	case LiteralExpression:
		if table != empty_string || schema != empty_string {
			buf.WriteRune(period_rune)
		}
		return me.Literal(buf, col)
	default:
		return NewGoquError("Unexpected col type must be string or LiteralExpression %+v", col)
	}
	return nil
}

//Generates SQL NULL value
func (me *DefaultAdapter) LiteralNil(buf *SqlBuilder) error {
	buf.Write(me.Null)
	return nil
}

//Generates SQL bool literal, (e.g. TRUE, FALSE, mysql 1, 0, sqlite3 1, 0)
func (me *DefaultAdapter) LiteralBool(buf *SqlBuilder, b bool) error {
	if buf.IsPrepared {
		return me.PlaceHolderSql(buf, b)
	}
	if b {
		buf.Write(me.True)
	} else {
		buf.Write(me.False)
	}
	return nil
}

//Generates SQL for a time.Time value
func (me *DefaultAdapter) LiteralTime(buf *SqlBuilder, t time.Time) error {
	if buf.IsPrepared {
		return me.PlaceHolderSql(buf, t)
	}
	return me.Literal(buf, t.UTC().Format(me.TimeFormat))
}

//Generates SQL for a Float Value
func (me *DefaultAdapter) LiteralFloat(buf *SqlBuilder, f float64) error {
	if buf.IsPrepared {
		return me.PlaceHolderSql(buf, f)
	}
	buf.WriteString(strconv.FormatFloat(f, 'f', -1, 64))
	return nil
}

//Generates SQL for an int value
func (me *DefaultAdapter) LiteralInt(buf *SqlBuilder, i int64) error {
	if buf.IsPrepared {
		return me.PlaceHolderSql(buf, i)
	}
	buf.WriteString(strconv.FormatInt(i, 10))
	return nil
}

//Generates SQL for a string
func (me *DefaultAdapter) LiteralString(buf *SqlBuilder, s string) error {
	if buf.IsPrepared {
		return me.PlaceHolderSql(buf, s)
	}
	buf.WriteRune(me.StringQuote)
	for _, char := range s {
		if e, ok := me.EscapedRunes[char]; ok {
			buf.Write(e)
		} else {
			buf.WriteRune(char)
		}
	}

	buf.WriteRune(me.StringQuote)
	return nil
}

// Generates SQL for a slice of bytes
func (me *DefaultAdapter) LiteralBytes(buf *SqlBuilder, bs []byte) error {
	if buf.IsPrepared {
		return me.PlaceHolderSql(buf, bs)
	}
	buf.WriteRune(me.StringQuote)
	i := 0
	for len(bs) > 0 {
		char, l := utf8.DecodeRune(bs)
		if e, ok := me.EscapedRunes[char]; ok {
			buf.Write(e)
		} else {
			buf.WriteRune(char)
		}
		i++
		bs = bs[l:]
	}
	buf.WriteRune(me.StringQuote)
	return nil
}

//Generates SQL for a slice of values (e.g. []int64{1,2,3,4} -> (1,2,3,4)
func (me *DefaultAdapter) SliceValueSql(buf *SqlBuilder, slice reflect.Value) error {
	buf.WriteRune(left_paren_rune)
	for i, l := 0, slice.Len(); i < l; i++ {
		if err := me.Literal(buf, slice.Index(i).Interface()); err != nil {
			return err
		}
		if i < l-1 {
			buf.WriteRune(comma_rune)
			buf.WriteRune(space_rune)
		}
	}
	buf.WriteRune(right_paren_rune)
	return nil
}

//Generates SQL for an AliasedExpression (e.g. I("a").As("b") -> "a" AS "b")
func (me *DefaultAdapter) AliasedExpressionSql(buf *SqlBuilder, aliased AliasedExpression) error {
	if err := me.Literal(buf, aliased.Aliased()); err != nil {
		return err
	}
	buf.Write(me.AsFragment)
	return me.Literal(buf, aliased.GetAs())
}

//Generates SQL for a BooleanExpresion (e.g. I("a").Eq(2) -> "a" = 2)
func (me *DefaultAdapter) BooleanExpressionSql(buf *SqlBuilder, operator BooleanExpression) error {
	buf.WriteRune(left_paren_rune)
	if err := me.Literal(buf, operator.Lhs()); err != nil {
		return err
	}
	buf.WriteRune(space_rune)
	operatorOp := operator.Op()
	if operator.Rhs() == nil {
		switch operatorOp {
		case EQ_OP:
			operatorOp = IS_OP
		case NEQ_OP:
			operatorOp = IS_NOT_OP
		}
	}
	if val, ok := me.BooleanOperatorLookup[operatorOp]; ok {
		buf.Write(val)
	} else {
		return NewGoquError("Boolean operator %+v not supported", operatorOp)
	}
	rhs := operator.Rhs()
	if (operatorOp == IS_OP || operatorOp == IS_NOT_OP) && me.UseLiteralIsBools {
		if rhs == true {
			rhs = L("TRUE")
		} else if rhs == false {
			rhs = L("FALSE")
		}
	}
	buf.WriteRune(space_rune)
	if err := me.Literal(buf, rhs); err != nil {
		return err
	}
	buf.WriteRune(right_paren_rune)
	return nil
}

//Generates SQL for a RangeExpresion (e.g. I("a").Between(RangeVal{Start:2,End:5}) -> "a" BETWEEN 2 AND 5)
func (me *DefaultAdapter) RangeExpressionSql(buf *SqlBuilder, operator RangeExpression) error {
	buf.WriteRune(left_paren_rune)
	if err := me.Literal(buf, operator.Lhs()); err != nil {
		return err
	}
	buf.WriteRune(space_rune)
	operatorOp := operator.Op()
	if val, ok := me.RangeOperatorLookup[operatorOp]; ok {
		buf.Write(val)
	} else {
		return NewGoquError("Range operator %+v not supported", operatorOp)
	}
	rhs := operator.Rhs()
	buf.WriteRune(space_rune)
	if err := me.Literal(buf, rhs.Start); err != nil {
		return err
	}
	buf.Write(default_and_fragment)
	if err := me.Literal(buf, rhs.End); err != nil {
		return err
	}
	buf.WriteRune(right_paren_rune)
	return nil
}

//Generates SQL for an OrderedExpression (e.g. I("a").Asc() -> "a" ASC)
func (me *DefaultAdapter) OrderedExpressionSql(buf *SqlBuilder, order OrderedExpression) error {
	if err := me.Literal(buf, order.SortExpression()); err != nil {
		return err
	}
	if order.Direction() == SORT_DESC {
		buf.Write(me.DescFragment)
	} else {
		buf.Write(me.AscFragment)
	}
	switch order.NullSortType() {
	case NULLS_FIRST:
		buf.Write(me.NullsFirstFragment)
	case NULLS_LAST:
		buf.Write(me.NullsLastFragment)
	}
	return nil
}

//Generates SQL for an ExpressionList (e.g. And(I("a").Eq("a"), I("b").Eq("b")) -> (("a" = 'a') AND ("b" = 'b')))
func (me *DefaultAdapter) ExpressionListSql(buf *SqlBuilder, expressionList ExpressionList) error {
	var op []byte
	if expressionList.Type() == AND_TYPE {
		op = me.AndFragment
	} else {
		op = me.OrFragment
	}
	exps := expressionList.Expressions()
	expLen := len(exps) - 1
	needsAppending := expLen > 0
	if needsAppending {
		buf.WriteRune(left_paren_rune)
	} else {
		return me.Literal(buf, exps[0])
	}
	for i, exp := range exps {
		if err := me.Literal(buf, exp); err != nil {
			return err
		}
		if i < expLen {
			buf.Write(op)
		}
	}
	buf.WriteRune(right_paren_rune)
	return nil
}

//Generates SQL for a ColumnList
func (me *DefaultAdapter) ColumnListSql(buf *SqlBuilder, columnList ColumnList) error {
	cols := columnList.Columns()
	colLen := len(cols)
	for i, col := range cols {
		if err := me.Literal(buf, col); err != nil {
			return err
		}
		if i < colLen-1 {
			buf.WriteRune(comma_rune)
			buf.WriteRune(space_rune)
		}
	}
	return nil
}

//Generates SQL for an UpdateEpxresion
func (me *DefaultAdapter) UpdateExpressionSql(buf *SqlBuilder, update UpdateExpression) error {
	if err := me.Literal(buf, update.Col()); err != nil {
		return err
	}
	buf.WriteRune(me.SetOperatorRune)
	return me.Literal(buf, update.Val())
}

//Generates SQL for a LiteralExpression
//    L("a + b") -> a + b
//    L("a = ?", 1) -> a = 1
func (me *DefaultAdapter) LiteralExpressionSql(buf *SqlBuilder, literal LiteralExpression) error {
	lit := literal.Literal()
	args := literal.Args()
	argsLen := len(args)
	if argsLen > 0 {
		currIndex := 0
		for _, char := range lit {
			if char == replacement_rune && currIndex < argsLen {
				if err := me.Literal(buf, args[currIndex]); err != nil {
					return err
				}
				currIndex++
			} else {
				buf.WriteRune(char)
			}
		}
	} else {
		buf.WriteString(lit)
	}
	return nil
}

//Generates SQL for a SqlFunctionExpression
//   COUNT(I("a")) -> COUNT("a")
func (me *DefaultAdapter) SqlFunctionExpressionSql(buf *SqlBuilder, sqlFunc SqlFunctionExpression) error {
	buf.WriteString(sqlFunc.Name())
	return me.Literal(buf, sqlFunc.Args())
}

//Generates SQL for a CastExpression
//   I("a").Cast("NUMERIC") -> CAST("a" AS NUMERIC)
func (me *DefaultAdapter) CastExpressionSql(buf *SqlBuilder, cast CastExpression) error {
	buf.WriteString("CAST")
	buf.WriteRune(left_paren_rune)
	if err := me.Literal(buf, cast.Casted()); err != nil {
		return err
	}
	buf.Write(me.AsFragment)
	if err := me.Literal(buf, cast.Type()); err != nil {
		return err
	}
	buf.WriteRune(right_paren_rune)
	return nil
}

//Generates SQL for a CommonTableExpression
func (me *DefaultAdapter) CommonTableExpressionSql(buf *SqlBuilder, cte CommonTableExpression) error {
	if err := me.Literal(buf, cte.Name()); err != nil {
		return err
	}
	buf.Write(me.AsFragment)
	if err := me.Literal(buf, cte.SubQuery()); err != nil {
		return err
	}
	return nil
}

//Generates SQL for a CompoundExpression
func (me *DefaultAdapter) CompoundExpressionSql(buf *SqlBuilder, compound CompoundExpression) error {
	switch compound.Type() {
	case UNION:
		buf.Write(me.UnionFragment)
	case UNION_ALL:
		buf.Write(me.UnionAllFragment)
	case INTERSECT:
		buf.Write(me.IntersectFragment)
	case INTERSECT_ALL:
		buf.Write(me.IntersectAllFragment)
	}
	return me.Literal(buf, compound.Rhs())
}

func (me *DefaultAdapter) ExpressionMapSql(buf *SqlBuilder, ex Ex) error {
	expressionList, err := ex.ToExpressions()
	if err != nil {
		return err
	}
	return me.Literal(buf, expressionList)
}

func (me *DefaultAdapter) ExpressionOrMapSql(buf *SqlBuilder, ex ExOr) error {
	expressionList, err := ex.ToExpressions()
	if err != nil {
		return err
	}
	return me.Literal(buf, expressionList)
}

func init() {
	RegisterAdapter("default", NewDefaultAdapter)
}
