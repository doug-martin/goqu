package gql

func (me Dataset) ScanStructs(i interface{}) error {
	sql, err := me.Sql()
	return newExec(me.database, err, sql).ScanStructs(i)
}

func (me Dataset) ScanStruct(i interface{}) (bool, error) {
	sql, err := me.Limit(1).Sql()
	return newExec(me.database, err, sql).ScanStruct(i)
}

func (me Dataset) ScanVals(i interface{}) error {
	sql, err := me.Sql()
	return newExec(me.database, err, sql).ScanVals(i)
}

func (me Dataset) ScanVal(i interface{}) (bool, error) {
	sql, err := me.Sql()
	return newExec(me.database, err, sql).ScanVal(i)
}

func (me Dataset) Count() (int64, error) {
	var count int64
	_, err := me.Select(COUNT(Star()).As("count")).ScanVal(&count)
	return count, err
}

func (me Dataset) Pluck(i interface{}, col string) error {
	return me.Select(col).ScanVals(i)
}

func (me Dataset) Update(i interface{}) *exec {
	sql, err := me.UpdateSql(i)
	return newExec(me.database, err, sql)
}

func (me Dataset) Insert(i ...interface{}) *exec {
	sql, err := me.InsertSql(i...)
	return newExec(me.database, err, sql)
}

func (me Dataset) Delete() *exec {
	sql, err := me.DeleteSql()
	return newExec(me.database, err, sql)
}
