package gql

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

var (
	replacement_rune                = '?'
	comma_rune                      = ','
	space_rune                      = ' '
	left_paren_rune                 = '('
	right_paren_rune                = ')'
	star_rune                       = '*'
	default_quote                   = '"'
	period_rune                     = '.'
	empty_string                    = ""
	default_null                    = []byte("NULL")
	default_true                    = []byte("TRUE")
	default_false                   = []byte("FALSE")
	default_update_clause           = []byte("UPDATE")
	default_insert_clause           = []byte("INSERT INTO")
	default_select_clause           = []byte("SELECT")
	default_delete_clause           = []byte("DELETE")
	default_truncate_clause         = []byte("TRUNCATE")
	default_cascade_fragment        = []byte(" CASCADE")
	default_retrict_fragment        = []byte(" RESTRICT")
	default_default_values_fragment = []byte(" DEFAULT VALUES")
	default_values_fragment         = []byte(" VALUES ")
	default_identity_fragment       = []byte(" IDENTITY")
	default_set_fragment            = []byte(" SET ")
	default_distinct_fragment       = []byte(" DISTINCT ")
	default_returning_fragment      = []byte(" RETURNING ")
	default_from_fragment           = []byte(" FROM")
	default_where_fragment          = []byte(" WHERE ")
	default_group_by_fragement      = []byte(" GROUP BY ")
	default_having_fragment         = []byte(" HAVING ")
	default_order_by_fragment       = []byte(" ORDER BY ")
	default_limit_fragment          = []byte(" LIMIT ")
	default_offset_fragment         = []byte(" OFFSET ")
	default_as_fragement            = []byte(" AS ")
	default_asc_fragement           = []byte(" ASC")
	default_desc_fragement          = []byte(" DESC")
	default_nulls_first_fragement   = []byte(" NULLS FIRST")
	default_nulls_last_fragement    = []byte(" NULLS LAST")
	default_and_fragment            = []byte(" AND ")
	default_or_fragment             = []byte(" OR ")
	default_union_fragment          = []byte(" UNION ")
	default_union_all_fragment      = []byte(" UNION ALL ")
	default_intersect_fragment      = []byte(" INTERSECT ")
	default_intersect_all_fragment  = []byte(" INTERSECT ALL ")
	default_set_operator_rune       = '='
	default_string_quote_rune       = '\''
	default_place_holder_rune       = '?'
	default_operator_lookup         = map[BooleanOperation][]byte{
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
)

type (
	DefaultAdapter struct {
		dataset               *Dataset
		UpdateClause          []byte
		InsertClause          []byte
		SelectClause          []byte
		DeleteClause          []byte
		TruncateClause        []byte
		CascadeFragment       []byte
		RestrictFragment      []byte
		DefaultValuesFragment []byte
		ValuesFragment        []byte
		IdentityFragment      []byte
		SetFragment           []byte
		DistinctFragment      []byte
		ReturningFragment     []byte
		FromFragment          []byte
		WhereFragment         []byte
		GroupByFragment       []byte
		HavingFragment        []byte
		OrderByFragment       []byte
		LimitFragment         []byte
		OffsetFragment        []byte
		AsFragment            []byte
		QuoteRune             rune
		Null                  []byte
		True                  []byte
		False                 []byte
		AscFragment           []byte
		DescFragment          []byte
		NullsFirstFragment    []byte
		NullsLastFragment     []byte
		AndFragment           []byte
		OrFragment            []byte
		UnionFragment         []byte
		UnionAllFragment      []byte
		IntersectFragment     []byte
		IntersectAllFragment  []byte
		StringQuote           rune
		SetOperatorRune       rune
		PlaceHolderRune       rune
		IncludePlaceholderNum bool
		TimeFormat            string
		BooleanOperatorLookup map[BooleanOperation][]byte
		JoinTypeLookup        map[JoinType][]byte
	}
)

func NewDefaultAdapter(ds *Dataset) Adapter {
	return &DefaultAdapter{
		dataset:               ds,
		UpdateClause:          default_update_clause,
		InsertClause:          default_insert_clause,
		SelectClause:          default_select_clause,
		DeleteClause:          default_delete_clause,
		TruncateClause:        default_truncate_clause,
		CascadeFragment:       default_cascade_fragment,
		RestrictFragment:      default_retrict_fragment,
		DefaultValuesFragment: default_default_values_fragment,
		ValuesFragment:        default_values_fragment,
		IdentityFragment:      default_identity_fragment,
		SetFragment:           default_set_fragment,
		DistinctFragment:      default_distinct_fragment,
		ReturningFragment:     default_returning_fragment,
		FromFragment:          default_from_fragment,
		WhereFragment:         default_where_fragment,
		GroupByFragment:       default_group_by_fragement,
		HavingFragment:        default_having_fragment,
		OrderByFragment:       default_order_by_fragment,
		LimitFragment:         default_limit_fragment,
		OffsetFragment:        default_offset_fragment,
		AsFragment:            default_as_fragement,
		QuoteRune:             default_quote,
		Null:                  default_null,
		True:                  default_true,
		False:                 default_false,
		StringQuote:           default_string_quote_rune,
		AscFragment:           default_asc_fragement,
		DescFragment:          default_desc_fragement,
		NullsFirstFragment:    default_nulls_first_fragement,
		NullsLastFragment:     default_nulls_last_fragement,
		AndFragment:           default_and_fragment,
		OrFragment:            default_or_fragment,
		SetOperatorRune:       default_set_operator_rune,
		UnionFragment:         default_union_fragment,
		UnionAllFragment:      default_union_all_fragment,
		IntersectFragment:     default_intersect_fragment,
		IntersectAllFragment:  default_intersect_all_fragment,
		PlaceHolderRune:       default_place_holder_rune,
		BooleanOperatorLookup: default_operator_lookup,
		JoinTypeLookup:        default_join_lookup,
		TimeFormat:            time.RFC3339Nano,
	}
}

func (me *DefaultAdapter) SupportsReturn() bool {
	return true
}

func (me *DefaultAdapter) SupportsLimitOnDelete() bool {
	return false
}

func (me *DefaultAdapter) SupportsLimitOnUpdate() bool {
	return false
}

func (me *DefaultAdapter) SupportsOrderByOnDelete() bool {
	return false
}

func (me *DefaultAdapter) SupportsOrderByOnUpdate() bool {
	return false
}

func (me *DefaultAdapter) Literal(buf *SqlBuilder, val interface{}) error {
	return me.dataset.Literal(buf, val)
}

func (me *DefaultAdapter) PlaceHolderSql(buf *SqlBuilder, i interface{}) error {
	buf.WriteRune(me.PlaceHolderRune)
	if me.IncludePlaceholderNum {
		buf.WriteString(strconv.FormatInt(int64(buf.CurrentArgPosition), 10))
	}
	buf.WriteArg(i)
	return nil
}

func (me *DefaultAdapter) UpdateBeginSql(buf *SqlBuilder) error {
	buf.Write(me.UpdateClause)
	return nil
}

func (me *DefaultAdapter) InsertBeginSql(buf *SqlBuilder) error {
	buf.Write(me.InsertClause)
	return nil
}

func (me *DefaultAdapter) DeleteBeginSql(buf *SqlBuilder) error {
	buf.Write(me.DeleteClause)
	return nil
}

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

func (me *DefaultAdapter) DefaultValuesSql(buf *SqlBuilder) error {
	buf.Write(me.DefaultValuesFragment)
	return nil
}

func (me *DefaultAdapter) InsertColumnsSql(buf *SqlBuilder, cols ColumnList) error {
	buf.WriteRune(space_rune)
	buf.WriteRune(left_paren_rune)
	if err := me.Literal(buf, cols); err != nil {
		return err
	}
	buf.WriteRune(right_paren_rune)
	return nil
}

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

func (me *DefaultAdapter) UpdateExpressionsSql(buf *SqlBuilder, updates ...UpdateExpression) error {
	if len(updates) == 0 {
		return NewGqlError("No update values provided")
	}
	updateLen := len(updates)
	buf.Write(me.SetFragment)
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

func (me *DefaultAdapter) FromSql(buf *SqlBuilder, from ColumnList) error {
	if from != nil && len(from.Columns()) > 0 {
		buf.Write(me.FromFragment)
		return me.SourcesSql(buf, from)
	}
	return nil
}

func (me *DefaultAdapter) SourcesSql(buf *SqlBuilder, from ColumnList) error {
	buf.WriteRune(space_rune)
	return me.Literal(buf, from)
}

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
					return NewGqlError("Join condition required for conditioned join %s", string(joinType))
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

func (me *DefaultAdapter) WhereSql(buf *SqlBuilder, where ExpressionList) error {
	if where != nil && len(where.Expressions()) > 0 {
		buf.Write(me.WhereFragment)
		return me.Literal(buf, where)
	}
	return nil
}

func (me *DefaultAdapter) GroupBySql(buf *SqlBuilder, groupBy ColumnList) error {
	if groupBy != nil && len(groupBy.Columns()) > 0 {
		buf.Write(me.GroupByFragment)
		return me.Literal(buf, groupBy)
	}
	return nil
}

func (me *DefaultAdapter) HavingSql(buf *SqlBuilder, having ExpressionList) error {
	if having != nil && len(having.Expressions()) > 0 {
		buf.Write(me.HavingFragment)
		return me.Literal(buf, having)
	}
	return nil
}

func (me *DefaultAdapter) CompoundsSql(buf *SqlBuilder, compounds []CompoundExpression) error {
	for _, compound := range compounds {
		if err := me.Literal(buf, compound); err != nil {
			return err
		}
	}
	return nil
}

func (me *DefaultAdapter) OrderSql(buf *SqlBuilder, order ColumnList) error {
	if order != nil && len(order.Columns()) > 0 {
		buf.Write(me.OrderByFragment)
		return me.Literal(buf, order)
	}
	return nil
}

func (me *DefaultAdapter) LimitSql(buf *SqlBuilder, limit interface{}) error {
	if limit != nil {
		buf.Write(me.LimitFragment)
		return me.Literal(buf, limit)
	}
	return nil
}

func (me *DefaultAdapter) OffsetSql(buf *SqlBuilder, offset uint) error {
	if offset > 0 {
		buf.Write(me.OffsetFragment)
		return me.Literal(buf, offset)
	}
	return nil
}

func (me *DefaultAdapter) DatasetSql(buf *SqlBuilder, dataset Dataset) error {
	buf.WriteRune(left_paren_rune)
	if buf.IsPrepared {
		if err := dataset.selectSqlWriteTo(buf); err != nil {
			return err
		}
	} else {
		sql, err := dataset.Sql()
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
		if table != empty_string || schema != "empty_string" {
			buf.WriteRune(period_rune)
		}
		return me.Literal(buf, col)
	default:
		return NewGqlError("Unexpected col type must be string or LiteralExpression %+v", col)
	}
	return nil
}

func (me *DefaultAdapter) LiteralNil(buf *SqlBuilder) error {
	if buf.IsPrepared {
		return me.PlaceHolderSql(buf, nil)
	} else {
		buf.Write(me.Null)
	}
	return nil
}

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

func (me *DefaultAdapter) LiteralTime(buf *SqlBuilder, t time.Time) error {
	if buf.IsPrepared {
		return me.PlaceHolderSql(buf, t)
	}
	return me.Literal(buf, t.UTC().Format(me.TimeFormat))
}

func (me *DefaultAdapter) LiteralFloat(buf *SqlBuilder, f float64) error {
	if buf.IsPrepared {
		return me.PlaceHolderSql(buf, f)
	}
	buf.WriteString(strconv.FormatFloat(f, 'f', -1, 64))
	return nil
}

func (me *DefaultAdapter) LiteralInt(buf *SqlBuilder, i int64) error {
	if buf.IsPrepared {
		return me.PlaceHolderSql(buf, i)
	}
	buf.WriteString(strconv.FormatInt(i, 10))
	return nil
}

func (me *DefaultAdapter) LiteralString(buf *SqlBuilder, s string) error {
	if buf.IsPrepared {
		return me.PlaceHolderSql(buf, s)
	}
	buf.WriteRune(me.StringQuote)
	for _, char := range s {
		if char == me.StringQuote { // single quote: ' -> \'
			buf.WriteRune(me.StringQuote)
			buf.WriteRune(me.StringQuote)
		} else {
			buf.WriteRune(char)
		}
	}

	buf.WriteRune(me.StringQuote)
	return nil
}

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

func (me *DefaultAdapter) AliasedExpressionSql(buf *SqlBuilder, aliased AliasedExpression) error {
	if err := me.Literal(buf, aliased.Aliased()); err != nil {
		return err
	}
	buf.Write(me.AsFragment)
	return me.Literal(buf, aliased.GetAs())
}

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
		return NewGqlError("Boolean operator %+v not supported", operatorOp)
	}
	rhs := operator.Rhs()
	if operatorOp == IS_OP || operatorOp == IS_NOT_OP {
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

func (me *DefaultAdapter) UpdateExpressionSql(buf *SqlBuilder, update UpdateExpression) error {
	if err := me.Literal(buf, update.Col()); err != nil {
		return err
	}
	buf.WriteRune(me.SetOperatorRune)
	return me.Literal(buf, update.Val())
}

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

func (me *DefaultAdapter) SqlFunctionExpressionSql(buf *SqlBuilder, sqlFunc SqlFunctionExpression) error {
	buf.WriteString(sqlFunc.Name())
	return me.Literal(buf, sqlFunc.Args())
}

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

type (
	TxAdapterGen     func(tx *sql.Tx) TxDbAdapter
	DefaultDbAdapter struct {
		TxDbAdapter TxAdapterGen
		Db          Db
		logger      Logger
	}
)

func NewDefaultDbAdapter(db Db, txDbAdapter TxAdapterGen) *DefaultDbAdapter {
	ret := new(DefaultDbAdapter)
	ret.Db = db
	ret.TxDbAdapter = txDbAdapter
	return ret
}

func (me DefaultDbAdapter) QueryAdapter(dataset *Dataset) Adapter {
	return NewDefaultAdapter(dataset)
}

func (me *DefaultDbAdapter) SetLogger(logger Logger) {
	me.logger = logger
}

func (me *DefaultDbAdapter) Logger() Logger {
	return me.logger
}

func (me DefaultDbAdapter) Trace(op, sql string, args ...interface{}) {
	if me.logger != nil {
		if sql != "" {
			if len(args) != 0 {
				me.logger.Printf("[gql] %s [query:=`%s` args:=%+v] ", op, sql, args)
			} else {
				me.logger.Printf("[gql] %s [query:=`%s`] ", op, sql)
			}
		} else {
			me.logger.Printf("[gql] %s", op)
		}
	}
}

func (me DefaultDbAdapter) Exec(query string, args ...interface{}) (sql.Result, error) {
	me.Trace("EXEC", query, args...)
	return me.Db.Exec(query, args...)
}

func (me DefaultDbAdapter) Prepare(query string) (*sql.Stmt, error) {
	me.Trace("PREPARE", query)
	return me.Db.Prepare(query)
}

func (me DefaultDbAdapter) Query(query string, args ...interface{}) (*sql.Rows, error) {
	me.Trace("QUERY", query, args...)
	return me.Db.Query(query, args...)
}

func (me DefaultDbAdapter) QueryRow(query string, args ...interface{}) *sql.Row {
	me.Trace("QUERY ROW", query, args...)
	return me.Db.QueryRow(query, args...)
}

func (me DefaultDbAdapter) Begin() (TxDbAdapter, error) {
	me.Trace("BEGIN", "")
	tx, err := me.Db.Begin()
	if err != nil {
		return nil, err
	}
	ret := me.TxDbAdapter(tx)
	ret.SetLogger(me.logger)
	return ret, nil
}

type DefaultTxDbAdapter struct {
	Db     *sql.Tx
	logger Logger
}

func NewDefaultTxDbAdapter(db *sql.Tx) *DefaultTxDbAdapter {
	return &DefaultTxDbAdapter{Db: db}
}

func (me DefaultTxDbAdapter) Commit() error {
	me.Trace("COMMIT", "")
	return me.Db.Commit()
}

func (me DefaultTxDbAdapter) Rollback() error {
	me.Trace("ROLLBACK", "")
	return me.Db.Rollback()
}

func (me DefaultTxDbAdapter) QueryAdapter(dataset *Dataset) Adapter {
	return NewDefaultAdapter(dataset)
}

func (me *DefaultTxDbAdapter) SetLogger(logger Logger) {
	me.logger = logger
}

func (me *DefaultTxDbAdapter) Logger() Logger {
	return me.logger
}

func (me DefaultTxDbAdapter) Trace(op, sql string, args ...interface{}) {
	if me.logger != nil {
		if sql != "" {
			if len(args) != 0 {
				me.logger.Printf("[gql - transaction] %s [query:=`%s` args:=%+v] ", op, sql, args)
			} else {
				me.logger.Printf("[gql - transaction] %s [query:=`%s`] ", op, sql)
			}
		} else {
			me.logger.Printf("[gql - transaction] %s", op)
		}
	}
}

func (me DefaultTxDbAdapter) Exec(query string, args ...interface{}) (sql.Result, error) {
	me.Trace("EXEC", query, args...)
	return me.Db.Exec(query, args...)
}

func (me DefaultTxDbAdapter) Prepare(query string) (*sql.Stmt, error) {
	me.Trace("PREPARE", query)
	return me.Db.Prepare(query)
}

func (me DefaultTxDbAdapter) Query(query string, args ...interface{}) (*sql.Rows, error) {
	me.Trace("QUERY", query, args...)
	return me.Db.Query(query, args...)
}

func (me DefaultTxDbAdapter) QueryRow(query string, args ...interface{}) *sql.Row {
	me.Trace("QUERY ROW", query, args...)
	return me.Db.QueryRow(query, args...)
}

func init() {
	RegisterDbAdapter("default", func(db Db) DbAdapter {
		return NewDefaultDbAdapter(db, func(tx *sql.Tx) TxDbAdapter {
			return NewDefaultTxDbAdapter(tx)
		})
	})
	RegisterDatasetAdapter("default", func(ds *Dataset) Adapter {
		return NewDefaultAdapter(ds)
	})
}
