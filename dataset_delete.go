package gql

import "strings"

type (
	TruncateOptions struct {
		Cascade  bool
		Restrict bool
		Identity string
	}
)

func (me Dataset) DeleteSql() (string, error) {
	var (
		err       error
		sql       string
		deleteSql []string
	)
	if !me.hasSources() {
		return "", newGqlError("No source found when generating delete sql")
	}
	if sql, err = me.adapter.DeleteBeginSql(); err != nil {
		return "", err
	}
	deleteSql = append(deleteSql, sql)
	if sql, err = me.adapter.FromSql(me.clauses.From); err != nil {
		return "", err
	}
	deleteSql = append(deleteSql, sql)

	if sql, err = me.adapter.WhereSql(me.clauses.Where); err != nil {
		return "", err
	}
	deleteSql = append(deleteSql, sql)

	if sql, err = me.adapter.ReturningSql(me.clauses.Returning); err != nil {
		return "", newGqlError(err.Error())
	}
	deleteSql = append(deleteSql, sql)
	return strings.Join(deleteSql, ""), nil
}

func (me Dataset) TruncateSql() (string, error) {
	return me.TruncateWithOptsSql(TruncateOptions{})
}

func (me Dataset) TruncateWithOptsSql(opts TruncateOptions) (string, error) {
	if !me.hasSources() {
		return "", newGqlError("No source found when generating truncate sql")
	}
	return me.adapter.TruncateSql(me.clauses.From, opts)
}
