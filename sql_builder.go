package goqu

import "bytes"

//Builder that is composed of a bytes.Buffer. It is used internally and by adapters to build SQL statements
type SqlBuilder struct {
	bytes.Buffer
	//True if the sql should not be interpolated
	IsPrepared bool
	//Current Number of arguments, used by adapters that need positional placeholders
	CurrentArgPosition int
	args               []interface{}
}

func NewSqlBuilder(isPrepared bool) *SqlBuilder {
	return &SqlBuilder{IsPrepared: isPrepared, args: make([]interface{}, 0), CurrentArgPosition: 1}
}

//Adds an argument to the builder, used when IsPrepared is false
func (me *SqlBuilder) WriteArg(i interface{}) {
	me.CurrentArgPosition++
	me.args = append(me.args, i)
}

//Returns the sql string, and arguments.
func (me *SqlBuilder) ToSql() (string, []interface{}) {
	return me.String(), me.args
}
