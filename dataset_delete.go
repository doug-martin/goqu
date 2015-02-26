package gql

type (
	TruncateOptions struct {
		Cascade  bool
		Restrict bool
		Identity string
	}
)

func (me *Dataset) DeleteSql() (string, error) {
	sql, _, err := me.ToDeleteSql(false)
	return sql, err
}

func (me *Dataset) ToDeleteSql(isPrepared bool) (string, []interface{}, error) {
	buf := NewSqlBuilder(isPrepared)
	if !me.hasSources() {
		return "", nil, NewGqlError("No source found when generating delete sql")
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
			return "", nil, NewGqlError(err.Error())
		}
	}else if me.clauses.Returning != nil{
        return "", nil, NewGqlError("Adapter does not support RETURNING clause")
    }
	sql, args := buf.ToSql()
	return sql, args, nil
}

func (me *Dataset) TruncateSql() (string, error) {
	return me.TruncateWithOptsSql(TruncateOptions{})
}

func (me *Dataset) TruncateWithOptsSql(opts TruncateOptions) (string, error) {
	sql, _, err := me.ToTruncateSql(false, opts)
	return sql, err
}

func (me *Dataset) ToTruncateSql(isPrepared bool, opts TruncateOptions) (string, []interface{}, error) {
	if !me.hasSources() {
		return "", nil, NewGqlError("No source found when generating truncate sql")
	}
	buf := NewSqlBuilder(false)
	if err := me.adapter.TruncateSql(buf, me.clauses.From, opts); err != nil {
		return "", nil, err
	}
	sql, args := buf.ToSql()
	return sql, args, nil
}
