package goqu

type (
	//Options to use when generating a TRUNCATE statement
	TruncateOptions struct {
		//Set to true to add CASCADE to the TRUNCATE statement
		Cascade bool
		//Set to true to add RESTRICT to the TRUNCATE statement
		Restrict bool
		//Set to true to specify IDENTITY options, (e.g. RESTART, CONTINUE) to the TRUNCATE statement
		Identity string
	}
)

//Generates a DELETE statement, if Prepared has been called with true then the statement will not be interpolated. See examples.
//
//isPrepared: Set to true to true to ensure values are NOT interpolated
//
//Errors:
//  * There is no FROM clause
//  * Error generating SQL
func (me *Dataset) ToDeleteSql() (string, []interface{}, error) {
	buf := NewSqlBuilder(me.isPrepared)
	if !me.hasSources() {
		return "", nil, NewGoquError("No source found when generating delete sql")
	}
	if err := me.adapter.CommonTablesSql(buf, me.clauses.CommonTables); err != nil {
		return "", nil, err
	}
	if err := me.adapter.DeleteBeginSql(buf); err != nil {
		return "", nil, err
	}
	if err := me.adapter.FromSql(buf, me.clauses.From); err != nil {
		return "", nil, err
	}
	if err := me.adapter.WhereSql(buf, me.clauses.Where); err != nil {
		return "", nil, err
	}
	if me.adapter.SupportsOrderByOnDelete() {
		if err := me.adapter.OrderSql(buf, me.clauses.Order); err != nil {
			return "", nil, err
		}
	}
	if me.adapter.SupportsLimitOnDelete() {
		if err := me.adapter.LimitSql(buf, me.clauses.Limit); err != nil {
			return "", nil, err
		}
	}
	if me.adapter.SupportsReturn() {
		if err := me.adapter.ReturningSql(buf, me.clauses.Returning); err != nil {
			return "", nil, NewGoquError(err.Error())
		}
	} else if me.clauses.Returning != nil {
		return "", nil, NewGoquError("Adapter does not support RETURNING clause")
	}
	sql, args := buf.ToSql()
	return sql, args, nil
}

//Generates the default TRUNCATE statement. See examples.
//
//Errors:
//  * There is no FROM clause
//  * Error generating SQL
func (me *Dataset) ToTruncateSql() (string, []interface{}, error) {
	return me.ToTruncateWithOptsSql(TruncateOptions{})
}

//Generates the default TRUNCATE statement with the specified options. See examples.
//
//opts: Options to use when generating the TRUNCATE statement
//
//Errors:
//  * There is no FROM clause
//  * Error generating SQL
func (me *Dataset) ToTruncateWithOptsSql(opts TruncateOptions) (string, []interface{}, error) {
	return me.toTruncateSql(opts)
}

//Generates a TRUNCATE statement.
//
//isPrepared: Set to true to true to ensure values are NOT interpolated. See examples.
//
//opts: Options to use when generating the TRUNCATE statement
//
//Errors:
//  * There is no FROM clause
//  * Error generating SQL
func (me *Dataset) toTruncateSql(opts TruncateOptions) (string, []interface{}, error) {
	if !me.hasSources() {
		return "", nil, NewGoquError("No source found when generating truncate sql")
	}
	buf := NewSqlBuilder(me.isPrepared)
	if err := me.adapter.TruncateSql(buf, me.clauses.From, opts); err != nil {
		return "", nil, err
	}
	sql, args := buf.ToSql()
	return sql, args, nil
}
