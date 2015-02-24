package gql

import "bytes"

type SqlBuilder struct {
	bytes.Buffer
	IsPrepared         bool
	CurrentArgPosition int
	args               []interface{}
}

func NewSqlBuilder(isPrepared bool) *SqlBuilder {
	return &SqlBuilder{IsPrepared: isPrepared, args: make([]interface{}, 0), CurrentArgPosition: 1}
}

func (me *SqlBuilder) WriteArg(i interface{}) {
	me.CurrentArgPosition++
	me.args = append(me.args, i)
}

func (me *SqlBuilder) ToSql() (string, []interface{}) {
	return me.String(), me.args
}
