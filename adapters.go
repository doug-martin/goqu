package gql

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"
)

type (
	Adapter interface {
		UpdateBeginSql() (string, error)
		InsertBeginSql() (string, error)
		SelectBeginSql() (string, error)
		DeleteBeginSql() (string, error)
		TruncateSql(ColumnList, TruncateOptions) (string, error)
		DefaultValuesSql() (string, error)
		UpdateExpressionsSql(updates ...UpdateExpression) (string, error)
		SelectSql(ColumnList) (string, error)
		SelectDistinctSql(cols ColumnList) (string, error)
		ReturningSql(ColumnList) (string, error)
		FromSql(from ColumnList) (string, error)
		SourcesSql(from ColumnList) (string, error)
		JoinSql(joins joiningClauses) (string, error)
		WhereSql(where ExpressionList) (string, error)
		GroupBySql(groupBy ColumnList) (string, error)
		HavingSql(having ExpressionList) (string, error)
		CompoundsSql(compounds []CompoundExpression) (string, error)
		OrderSql(order ColumnList) (string, error)
		LimitSql(limit interface{}) (string, error)
		OffsetSql(offset uint) (string, error)
		BuilderSql(builder Dataset) (string, error)
		QuoteIdentifier(ident IdentifierExpression) (string, error)
		LiteralNil() (string, error)
		LiteralBool(b bool) (string, error)
		LiteralTime(t time.Time) (string, error)
		LiteralFloat(f float64) (string, error)
		LiteralInt(i int64) (string, error)
		LiteralString(s string) (string, error)
		SliceValueSql(slice reflect.Value) (string, error)
		AliasedExpressionSql(aliased AliasedExpression) (string, error)
		BooleanExpressionSql(operator BooleanExpression) (string, error)
		OrderedExpressionSql(order OrderedExpression) (string, error)
		ExpressionListSql(expressionList ExpressionList) (string, error)
		SqlFunctionExpressionSql(sqlFunc SqlFunctionExpression) (string, error)
		CastExpressionSql(casted CastExpression) (string, error)
		CompoundExpressionSql(compound CompoundExpression) (string, error)
		ColumnListSql(columnList ColumnList) (string, error)
		UpdateExpressionSql(update UpdateExpression) (string, error)
		Literal(interface{}) (string, error)
		LiteralExpressionSql(literal LiteralExpression) (string, error)
		InsertColumnsSql(ColumnList) (string, error)
		InsertValuesSql(values [][]interface{}) (string, error)
	}
	adapter struct {
		dataset *Dataset
	}
)

func newAdapter(t string, dataset *Dataset) Adapter {
	return adapter{dataset: dataset}
}

func (me adapter) Literal(val interface{}) (string, error) {
	return me.dataset.Literal(val)
}

func (me adapter) UpdateBeginSql() (string, error) {
	return "UPDATE", nil
}

func (me adapter) InsertBeginSql() (string, error) {
	return "INSERT INTO", nil
}

func (me adapter) SelectBeginSql() (string, error) {
	return "SELECT", nil
}

func (me adapter) DeleteBeginSql() (string, error) {
	return "DELETE", nil
}

func (me adapter) TruncateSql(from ColumnList, opts TruncateOptions) (string, error) {
	var (
		sql string
		err error
	)
	truncateSql := []string{"TRUNCATE"}
	if sql, err = me.SourcesSql(from); err != nil {
		return "", err
	}
	truncateSql = append(truncateSql, sql)
	if opts.Identity != "" {
		truncateSql = append(truncateSql, fmt.Sprintf(" %s IDENTITY", strings.ToUpper(opts.Identity)))
	}
	if opts.Cascade {
		truncateSql = append(truncateSql, " CASCADE")
	} else if opts.Restrict {
		truncateSql = append(truncateSql, " RESTRICT")
	}
	return strings.Join(truncateSql, ""), nil
}

func (me adapter) DefaultValuesSql() (string, error) {
	return " DEFAULT VALUES", nil
}

func (me adapter) InsertColumnsSql(cols ColumnList) (string, error) {
	colLit, err := me.Literal(cols)
	if err != nil {
		return "", newGqlError(err.Error())
	}
	return fmt.Sprintf(" (%s)", colLit), nil
}

func (me adapter) InsertValuesSql(values [][]interface{}) (string, error) {
	rowLits := make([]string, len(values))
	rowLen := len(values[0])
	for i, row := range values {
		if len(row) != rowLen {
			return "", newGqlError("Rows with different value length expected %d got %d", rowLen, len(row))
		}
		lit, err := me.Literal(row)
		if err != nil {
			return "", err
		}
		rowLits[i] = lit
	}
	return fmt.Sprintf(" VALUES %s", strings.Join(rowLits, ", ")), nil
}

func (me adapter) UpdateExpressionsSql(updates ...UpdateExpression) (string, error) {
	if len(updates) == 0 {
		return "", newGqlError("No update values provided")
	}
	sets := make([]string, len(updates))
	for i, update := range updates {
		lit, err := me.Literal(update)
		if err != nil {
			return "", err
		}
		sets[i] = lit
	}
	return fmt.Sprintf(" SET %s", strings.Join(sets, ",")), nil

}

func (me adapter) SelectSql(cols ColumnList) (string, error) {
	lit, err := me.Literal(cols)
	if err != nil {
		return "", newGqlError(err.Error())
	}
	return fmt.Sprintf("SELECT %s", lit), nil
}

func (me adapter) SelectDistinctSql(cols ColumnList) (string, error) {
	lit, err := me.Literal(cols)
	if err != nil {
		return "", newGqlError(err.Error())
	}
	return fmt.Sprintf("SELECT DISTINCT %s", lit), nil
}

func (me adapter) ReturningSql(returns ColumnList) (string, error) {
	if returns != nil {
		lit, err := me.Literal(returns)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf(" RETURNING %s", lit), nil
	}
	return "", nil
}

func (me adapter) FromSql(from ColumnList) (string, error) {
	if from != nil && len(from.Columns()) > 0 {
		sources, err := me.SourcesSql(from)
		if err != nil {
			return "", err
		}
		if sources != "" {
			return fmt.Sprintf(" FROM%s", sources), nil
		}
	}
	return "", nil
}

func (me adapter) SourcesSql(from ColumnList) (string, error) {
	lit, err := me.Literal(from)
	if err != nil {
		return "", err
	}
	return " " + lit, err
}

func (me adapter) JoinSql(joins joiningClauses) (string, error) {
	if len(joins) > 0 {
		joinSql := ""
		for _, j := range joins {
			tableLit, err := me.Literal(j.table)
			if err != nil {
				return "", newGqlError(err.Error())
			}
			var joinType string
			switch j.joinType {
			case inner_join:
				joinType = "INNER"
			case full_outer_join:
				joinType = "FULL OUTER"
			case right_outer_join:
				joinType = "RIGHT OUTER"
			case left_outer_join:
				joinType = "LEFT OUTER"
			case full_join:
				joinType = "FULL"
			case right_join:
				joinType = "RIGHT"
			case left_join:
				joinType = "LEFT"
			case natural_join:
				joinType = "NATURAL"
			case natural_left_join:
				joinType = "NATURAL LEFT"
			case natural_right_join:
				joinType = "NATURAL RIGHT"
			case natural_full_join:
				joinType = "NATURAL FULL"
			case cross_join:
				joinType = "CROSS"
			default:
				return "", newGqlError("Unsupported join type %s", j.joinType)
			}
			joinSql = fmt.Sprintf("%s %s JOIN %s", joinSql, joinType, tableLit)
			if j.isConditioned {
				if j.condition == nil {
					return "", newGqlError("Join condition required for conditioned join %s", joinType)
				}
				condition := j.condition
				joinCondition := "ON"
				if condition.JoinCondition() == using_cond {
					joinCondition = "USING"
				}
				switch condition.(type) {
				case JoinOnExpression:
					onLit, err := me.Literal(condition.(JoinOnExpression).On())
					if err != nil {
						return "", newGqlError(err.Error())
					}
					joinSql = fmt.Sprintf("%s %s %s", joinSql, joinCondition, onLit)
				case JoinUsingExpression:
					onLit, err := me.Literal(condition.(JoinUsingExpression).Using())
					if err != nil {
						return "", newGqlError(err.Error())
					}
					joinSql = fmt.Sprintf("%s %s (%s)", joinSql, joinCondition, onLit)
				}
			}
		}
		return joinSql, nil
	}
	return "", nil
}

func (me adapter) WhereSql(where ExpressionList) (string, error) {
	if where != nil {
		lit, err := me.Literal(where)
		if err != nil {
			return "", err
		}
		if lit != "" {
			return fmt.Sprintf(" WHERE %s", lit), nil
		}
	}
	return "", nil
}

func (me adapter) GroupBySql(groupBy ColumnList) (string, error) {
	if groupBy != nil {
		lit, err := me.Literal(groupBy)
		if err != nil {
			return "", err
		}
		if lit != "" {
			return fmt.Sprintf(" GROUP BY %s", lit), nil
		}
	}
	return "", nil
}

func (me adapter) HavingSql(having ExpressionList) (string, error) {
	if having != nil {
		lit, err := me.Literal(having)
		if err != nil {
			return "", err
		}
		if lit != "" {
			return fmt.Sprintf(" HAVING %s", lit), nil
		}
	}
	return "", nil
}

func (me adapter) CompoundsSql(compounds []CompoundExpression) (string, error) {
	var sqls []string
	for _, compound := range compounds {
		lit, err := me.Literal(compound)
		if err != nil {
			return "", err
		}
		sqls = append(sqls, lit)
	}
	return strings.Join(sqls, ""), nil
}

func (me adapter) OrderSql(order ColumnList) (string, error) {
	if order != nil {
		lit, err := me.Literal(order)
		if err != nil {
			return "", err
		}
		if lit != "" {
			return fmt.Sprintf(" ORDER BY %s", lit), nil
		}
	}
	return "", nil
}

func (me adapter) LimitSql(limit interface{}) (string, error) {
	if limit != nil {
		lit, err := me.Literal(limit)
		if err != nil {
			return "", err
		}
		if lit != "" {
			return fmt.Sprintf(" LIMIT %s", lit), nil
		}
	}
	return "", nil
}

func (me adapter) OffsetSql(offset uint) (string, error) {
	if offset > 0 {
		lit, err := me.Literal(offset)
		if err != nil {
			return "", err
		}
		if lit != "" {
			return fmt.Sprintf(" OFFSET %s", lit), nil
		}
	}
	return "", nil
}

func (me adapter) BuilderSql(builder Dataset) (string, error) {
	sql, err := builder.Sql()
	if err != nil {
		return "", err
	}
	ret := fmt.Sprintf("(%s)", sql)
	alias := builder.getClauses().Alias
	if alias != nil {
		ident, err := me.Literal(alias)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s AS %s", ret, ident), nil
	}
	return ret, nil
}

func (me adapter) QuoteIdentifier(ident IdentifierExpression) (string, error) {
	schema, table, col := ident.GetSchema(), ident.GetTable(), ident.GetCol()
	var ret []string
	if schema != "" {
		ret = append(ret, fmt.Sprintf(`"%s"`, schema))
	}
	if table != "" {
		ret = append(ret, fmt.Sprintf(`"%s"`, table))
	}
	switch col.(type) {
	case nil:
	case string:
		ret = append(ret, fmt.Sprintf(`"%s"`, col))
	case LiteralExpression:
		lit, err := me.Literal(col)
		if err != nil {
			return "", err
		}
		ret = append(ret, lit)
	default:
		return "", newGqlError("Unexpected col type must be string or gql.LiteralExpression %+v", col)
	}
	return strings.Join(ret, "."), nil
}

func (me adapter) LiteralNil() (string, error) {
	return "NULL", nil
}

func (me adapter) LiteralBool(b bool) (string, error) {
	if b {
		return "TRUE", nil
	}
	return "FALSE", nil
}

func (me adapter) LiteralTime(t time.Time) (string, error) {
	return fmt.Sprintf("'%s'", t.Format(time.RFC3339Nano)), nil
}

func (me adapter) LiteralFloat(f float64) (string, error) {
	return fmt.Sprintf("%f", f), nil
}

func (me adapter) LiteralInt(i int64) (string, error) {
	return fmt.Sprintf("%d", i), nil
}

func (me adapter) LiteralString(s string) (string, error) {
	return fmt.Sprintf("'%s'", strings.Replace(s, "'", "''", -1)), nil
}

func (me adapter) SliceValueSql(slice reflect.Value) (string, error) {
	var vals []string
	for i := 0; i < slice.Len(); i++ {
		lit, err := me.Literal(slice.Index(i).Interface())
		if err != nil {
			return "", err
		}
		vals = append(vals, lit)
	}
	return fmt.Sprintf("(%s)", strings.Join(vals, ", ")), nil
}

func (me adapter) AliasedExpressionSql(aliased AliasedExpression) (string, error) {
	val, err := me.Literal(aliased.Aliased())
	if err != nil {
		return "", err
	}
	as, err := me.Literal(aliased.GetAs())
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s AS %s", val, as), nil
}

func (me adapter) BooleanExpressionSql(operator BooleanExpression) (string, error) {
	var op string
	switch operator.Op() {
	case eq_op:
		op = "="
	case neq_op:
		op = "!="
	case gt_op:
		op = ">"
	case gte_op:
		op = ">="
	case lt_op:
		op = "<"
	case lte_op:
		op = "<="
	case in_op:
		op = "IN"
	case not_in_op:
		op = "NOT IN"
	case is_op:
		op = "IS"
	case is_not_op:
		op = "IS NOT"
	case like_op:
		op = "LIKE"
	case not_like_op:
		op = "NOT LIKE"
	case i_like_op:
		op = "ILIKE"
	case not_i_like_op:
		op = "NOT ILIKE"
	case regexp_like_op:
		op = "~"
	case regexp_not_like_op:
		op = "!~"
	case regexp_i_like_op:
		op = "~*"
	case regexp_not_i_like_op:
		op = "!~*"
	default:
		return "", newGqlError("unsupported boolean operator %s", operator.Op())
	}
	lhs, err := me.Literal(operator.Lhs())
	if err != nil {
		return "", err
	}
	rhs, err := me.Literal(operator.Rhs())
	if err != nil {
		return "", err
	}
	if rhs == "NULL" {
		switch operator.Op() {
		case eq_op:
			op = "IS"
		case neq_op:
			op = "IS NOT"
		}
	}
	return fmt.Sprintf("(%s %s %s)", lhs, op, rhs), nil
}

func (me adapter) OrderedExpressionSql(order OrderedExpression) (string, error) {
	exp, err := me.Literal(order.SortExpression())
	if err != nil {
		return "", err
	}
	direction, nullSortType := "ASC", ""
	if order.Direction() == sort_desc {
		direction = "DESC"
	}
	switch order.NullSortType() {
	case nulls_first:
		nullSortType = " NULLS FIRST"
	case nulls_last:
		nullSortType = " NULLS LAST"
	}
	return fmt.Sprintf("%s %s%s", exp, direction, nullSortType), nil
}

func (me adapter) ExpressionListSql(expressionList ExpressionList) (string, error) {
	exps := expressionList.Expressions()
	var literals []string
	for _, exp := range exps {
		lit, err := me.Literal(exp)
		if err != nil {
			return "", err
		}
		literals = append(literals, lit)
	}
	if len(literals) == 1 {
		return literals[0], nil
	}
	var op string
	switch expressionList.Type() {
	case and_type:
		op = " AND "
	case or_type:
		op = " OR "
	default:
		return "", newGqlError("unsupported expression list type %s", expressionList.Type())
	}
	return fmt.Sprintf("(%s)", strings.Join(literals, op)), nil
}

func (me adapter) ColumnListSql(columnList ColumnList) (string, error) {
	cols := columnList.Columns()
	var literals []string
	for _, col := range cols {
		lit, err := me.Literal(col)
		if err != nil {
			return "", err
		}
		literals = append(literals, lit)
	}
	if len(literals) == 1 {
		return literals[0], nil
	}
	return strings.Join(literals, ", "), nil
}

func (me adapter) UpdateExpressionSql(update UpdateExpression) (string, error) {
	ident, err := me.Literal(update.Col())
	if err != nil {
		return "", err
	}
	val, err := me.Literal(update.Val())
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s=%s", ident, val), nil
}

func (me adapter) LiteralExpressionSql(literal LiteralExpression) (string, error) {
	return literal.GetLiteral(), nil
}

func (me adapter) SqlFunctionExpressionSql(sqlFunc SqlFunctionExpression) (string, error) {
	args, err := me.Literal(sqlFunc.Args())
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s%s", sqlFunc.Name(), args), nil
}

func (me adapter) CastExpressionSql(cast CastExpression) (string, error) {
	casted, err := me.Literal(cast.Casted())
	if err != nil {
		return "", err
	}
	t, err := me.Literal(cast.Type())
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("CAST(%s AS %s)", casted, t), nil
}

func (me adapter) CompoundExpressionSql(compound CompoundExpression) (string, error) {
	rhs, err := me.Literal(compound.Rhs())
	if err != nil {
		return "", err
	}
	var cType string
	switch compound.Type() {
	case union:
		cType = "UNION"
	case union_all:
		cType = "UNION ALL"
	case intersect:
		cType = "INTERSECT"
	case intersect_all:
		cType = "INTERSECT ALL"
	}
	return fmt.Sprintf(" %s %s", cType, rhs), nil
}

type (
	Db interface {
		Exec(query string, args ...interface{}) (sql.Result, error)
		Prepare(query string) (*sql.Stmt, error)
		Query(query string, args ...interface{}) (*sql.Rows, error)
		QueryRow(query string, args ...interface{}) *sql.Row
		Begin() (*sql.Tx, error)
	}
	result        map[string]interface{}
	selectResults []result
	DbAdapter     struct {
		db     Db
		logger Logger
	}
)

func newDbAdapter(t string, db Db) *DbAdapter {
	return &DbAdapter{db: db}
}

func (me DbAdapter) QueryAdapter(dataset *Dataset) Adapter {
	return newAdapter("", dataset)
}

func (me *DbAdapter) Logger(logger Logger) {
	me.logger = logger
}

func (me DbAdapter) Trace(message string, args ...interface{}) {
	if me.logger != nil {
		me.logger.Printf("[gql] "+message, args...)
	}
}

func (me DbAdapter) Exec(query string, args ...interface{}) (sql.Result, error) {
	me.Trace("EXEC [query:=`%s` args:=%+v]", query, args)
	return me.db.Exec(query, args...)
}

func (me DbAdapter) Prepare(query string) (*sql.Stmt, error) {
	me.Trace("PREPARE [query:=`%s`]", query)
	return me.db.Prepare(query)
}

func (me DbAdapter) Query(query string, args ...interface{}) (*sql.Rows, error) {
	me.Trace("QUERY [query:=`%s` args:=%+v]", query, args)
	return me.db.Query(query, args...)
}

func (me DbAdapter) QueryRow(query string, args ...interface{}) *sql.Row {
	me.Trace("QUERY ROW [query:=`%s` args:=%+v]", query, args)
	return me.db.QueryRow(query, args...)
}

func (me DbAdapter) Select(columnMap ColumnMap, query string, args ...interface{}) (selectResults, error) {
	rows, err := me.Query(query, args...)
	if err != nil {
		return nil, newGqlQueryError(err.Error())
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, newGqlQueryError(err.Error())
	}
	var results selectResults
	for rows.Next() {
		scans := make([]interface{}, len(columns))
		for i, col := range columns {
			if data, ok := columnMap[col]; ok {
				scans[i] = reflect.New(data.GoType).Interface()
			} else {
				return nil, newGqlQueryError(`Unable to find corresponding field to column "%s" returned by query`, col)
			}
		}
		if err := rows.Scan(scans...); err != nil {
			return nil, newGqlQueryError(err.Error())
		}
		result := result{}
		for index, col := range columns {
			result[col] = scans[index]
		}
		results = append(results, result)
	}
	if rows.Err() != nil {
		return nil, newGqlQueryError(rows.Err().Error())
	}
	return results, nil
}

func (me DbAdapter) Insert(query string, args ...interface{}) ([]map[string]interface{}, error) {
	me.Trace(query, args...)
	return nil, nil
}

func (me DbAdapter) Update(query string, args ...interface{}) (int64, error) {
	result, err := me.Exec(query, args...)
	if err != nil {
		return 0, newGqlQueryError(err.Error())
	}
	return result.RowsAffected()
}

func (me DbAdapter) Delete(query string, args ...interface{}) (int64, error) {
	result, err := me.Exec(query, args...)
	if err != nil {
		return 0, newGqlQueryError(err.Error())
	}
	return result.RowsAffected()
}

func (me DbAdapter) Begin() (*TxDbAdapter, error) {
	me.Trace("BEGIN")
	tx, err := me.db.Begin()
	if err != nil {
		return nil, err
	}
	ret := newTxDbAdapter("", tx)
	ret.logger = me.logger
	return ret, nil
}

type TxDbAdapter struct {
	db     *sql.Tx
	logger Logger
}

func newTxDbAdapter(t string, db *sql.Tx) *TxDbAdapter {
	return &TxDbAdapter{db: db}
}

func (me TxDbAdapter) Commit() error {
	me.Trace("COMMIT")
	return me.db.Commit()
}

func (me TxDbAdapter) Rollback() error {
	me.Trace("ROLLBACK")
	return me.db.Rollback()
}

func (me TxDbAdapter) QueryAdapter(dataset *Dataset) Adapter {
	return newAdapter("", dataset)
}

func (me *TxDbAdapter) Logger(logger Logger) {
	me.logger = logger
}

func (me TxDbAdapter) Trace(message string, args ...interface{}) {
	if me.logger != nil {
		me.logger.Printf("[gql - transaction] "+message, args...)
	}
}

func (me TxDbAdapter) Exec(query string, args ...interface{}) (sql.Result, error) {
	me.Trace("EXEC [query:=`%s` args:=%+v]", query, args)
	return me.db.Exec(query, args...)
}

func (me TxDbAdapter) Prepare(query string) (*sql.Stmt, error) {
	me.Trace("PREPRE [query:=`%s`]", query)
	return me.db.Prepare(query)
}

func (me TxDbAdapter) Query(query string, args ...interface{}) (*sql.Rows, error) {
	me.Trace("QUERY [query:=`%s` args:=%+v]", query, args)
	return me.db.Query(query, args...)
}

func (me TxDbAdapter) QueryRow(query string, args ...interface{}) *sql.Row {
	me.Trace("QUERY ROW [query:=`%s` args:=%+v]", query, args)
	return me.db.QueryRow(query, args...)
}

func (me TxDbAdapter) Select(columnMap ColumnMap, query string, args ...interface{}) (selectResults, error) {
	rows, err := me.Query(query, args...)
	if err != nil {
		return nil, newGqlQueryError(err.Error())
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, newGqlQueryError(err.Error())
	}
	var results selectResults
	for rows.Next() {
		scans := make([]interface{}, len(columns))
		for i, col := range columns {
			if data, ok := columnMap[col]; ok {
				scans[i] = reflect.New(data.GoType).Interface()
			} else {
				return nil, newGqlQueryError(`Unable to find corresponding field to column "%s" returned by query`, col)
			}
		}
		if err := rows.Scan(scans...); err != nil {
			return nil, newGqlQueryError(err.Error())
		}
		result := result{}
		for index, col := range columns {
			result[col] = scans[index]
		}
		results = append(results, result)
	}
	if rows.Err() != nil {
		return nil, newGqlQueryError(rows.Err().Error())
	}
	return results, nil
}

func (me TxDbAdapter) Insert(query string, args ...interface{}) ([]map[string]interface{}, error) {
	me.Trace(query, args...)
	return nil, nil
}

func (me TxDbAdapter) Update(query string, args ...interface{}) (int64, error) {
	result, err := me.Exec(query, args...)
	if err != nil {
		return 0, newGqlQueryError(err.Error())
	}
	return result.RowsAffected()
}

func (me TxDbAdapter) Delete(query string, args ...interface{}) (int64, error) {
	result, err := me.Exec(query, args...)
	if err != nil {
		return 0, newGqlQueryError(err.Error())
	}
	return result.RowsAffected()
}
