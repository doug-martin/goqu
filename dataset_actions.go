package gql

//Generates the SELECT sql for this dataset and uses Exec#ScanStructs to scan the results into a slice of structs
//
//i: A pointer to a slice of structs
func (me *Dataset) ScanStructs(i interface{}) error {
	sql, err := me.Sql()
	return newExec(me.database, err, sql).ScanStructs(i)
}

//Generates the SELECT sql for this dataset and uses Exec#ScanStruct to scan the result into a slice of structs
//
//i: A pointer to a structs
func (me *Dataset) ScanStruct(i interface{}) (bool, error) {
	sql, err := me.Limit(1).Sql()
	return newExec(me.database, err, sql).ScanStruct(i)
}

//Generates the SELECT sql for this dataset and uses Exec#ScanVals to scan the results into a slice of primitive values
//
//i: A pointer to a slice of primitive values
func (me *Dataset) ScanVals(i interface{}) error {
	sql, err := me.Sql()
	return newExec(me.database, err, sql).ScanVals(i)
}

//Generates the SELECT sql for this dataset and uses Exec#ScanVal to scan the result into a primitive value
//
//i: A pointer to a primitive value
func (me *Dataset) ScanVal(i interface{}) (bool, error) {
	sql, err := me.Sql()
	return newExec(me.database, err, sql).ScanVal(i)
}

//Generates the SELECT COUNT(*) sql for this dataset and uses Exec#ScanVal to scan the result into an int64.
func (me *Dataset) Count() (int64, error) {
	var count int64
	_, err := me.Select(COUNT(Star()).As("count")).ScanVal(&count)
	return count, err
}

//Generates the SELECT sql only selecting the passed in column and uses Exec#ScanVals to scan the result into a slice of primitive values.
//
//i: A slice of primitive values
//col: The column to select when generative the SQL
func (me *Dataset) Pluck(i interface{}, col string) error {
	return me.Select(col).ScanVals(i)
}

//Generates the UPDATE sql, and returns an Exec struct with the sql set to the UPDATE statement
//    db.From("test").Update(Record{"name":"Bob", update: time.Now()}).Exec()
//
//See Dataset#UpdateSql for arguments
func (me *Dataset) Update(i interface{}) *Exec {
	sql, err := me.UpdateSql(i)
	return newExec(me.database, err, sql)
}

//Generates the UPDATE sql, and returns an Exec struct with the sql set to the INSERT statement
//    db.From("test").Insert(Record{"name":"Bob").Exec()
//
//See Dataset#InsertSql for arguments
func (me *Dataset) Insert(i ...interface{}) *Exec {
	sql, err := me.InsertSql(i...)
	return newExec(me.database, err, sql)
}

//Generates the DELETE sql, and returns an Exec struct with the sql set to the DELETE statement
//    db.From("test").Where(I("id").Gt(10)).Exec()
func (me *Dataset) Delete() *Exec {
	sql, err := me.DeleteSql()
	return newExec(me.database, err, sql)
}
