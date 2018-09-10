package goqu

import "context"

//Generates the SELECT sql for this dataset and uses Exec#ScanStructs to scan the results into a slice of structs.
//
//ScanStructs will only select the columns that can be scanned in to the struct unless you have explicitly selected certain columns. See examples.
//
//i: A pointer to a slice of structs
func (me *Dataset) ScanStructs(i interface{}) error {
	ds := me
	if me.isDefaultSelect() {
		ds = ds.Select(i)
	}
	sql, args, err := ds.ToSql()
	return newCrudExec(me.database, err, sql, args...).ScanStructs(i)
}

//Generates the SELECT sql for this dataset and uses Exec#ScanStructsContext to scan the results into a slice of structs.
//
//ScanStructsContext will only select the columns that can be scanned in to the struct unless you have explicitly selected certain columns. See examples.
//
//i: A pointer to a slice of structs
func (me *Dataset) ScanStructsContext(ctx context.Context, i interface{}) error {
	ds := me
	if me.isDefaultSelect() {
		ds = ds.Select(i)
	}
	sql, args, err := ds.ToSql()
	return newCrudExec(me.database, err, sql, args...).ScanStructsContext(ctx, i)
}

//Generates the SELECT sql for this dataset and uses Exec#ScanStruct to scan the result into a slice of structs
//
//ScanStruct will only select the columns that can be scanned in to the struct unless you have explicitly selected certain columns. See examples.
//
//i: A pointer to a structs
func (me *Dataset) ScanStruct(i interface{}) (bool, error) {
	ds := me.Limit(1)
	if me.isDefaultSelect() {
		ds = ds.Select(i)
	}
	sql, args, err := ds.ToSql()
	return newCrudExec(me.database, err, sql, args...).ScanStruct(i)
}

//Generates the SELECT sql for this dataset and uses Exec#ScanStructContext to scan the result into a slice of structs
//
//ScanStructContext will only select the columns that can be scanned in to the struct unless you have explicitly selected certain columns. See examples.
//
//i: A pointer to a structs
func (me *Dataset) ScanStructContext(ctx context.Context, i interface{}) (bool, error) {
	ds := me.Limit(1)
	if me.isDefaultSelect() {
		ds = ds.Select(i)
	}
	sql, args, err := ds.ToSql()
	return newCrudExec(me.database, err, sql, args...).ScanStructContext(ctx, i)
}

//Generates the SELECT sql for this dataset and uses Exec#ScanVals to scan the results into a slice of primitive values
//
//i: A pointer to a slice of primitive values
func (me *Dataset) ScanVals(i interface{}) error {
	sql, args, err := me.ToSql()
	return newCrudExec(me.database, err, sql, args...).ScanVals(i)
}

//Generates the SELECT sql for this dataset and uses Exec#ScanValsContext to scan the results into a slice of primitive values
//
//i: A pointer to a slice of primitive values
func (me *Dataset) ScanValsContext(ctx context.Context, i interface{}) error {
	sql, args, err := me.ToSql()
	return newCrudExec(me.database, err, sql, args...).ScanValsContext(ctx, i)
}

//Generates the SELECT sql for this dataset and uses Exec#ScanVal to scan the result into a primitive value
//
//i: A pointer to a primitive value
func (me *Dataset) ScanVal(i interface{}) (bool, error) {
	sql, args, err := me.Limit(1).ToSql()
	return newCrudExec(me.database, err, sql, args...).ScanVal(i)
}

//Generates the SELECT sql for this dataset and uses Exec#ScanValContext to scan the result into a primitive value
//
//i: A pointer to a primitive value
func (me *Dataset) ScanValContext(ctx context.Context, i interface{}) (bool, error) {
	sql, args, err := me.Limit(1).ToSql()
	return newCrudExec(me.database, err, sql, args...).ScanValContext(ctx, i)
}

//Generates the SELECT COUNT(*) sql for this dataset and uses Exec#ScanVal to scan the result into an int64.
func (me *Dataset) Count() (int64, error) {
	var count int64
	_, err := me.Select(COUNT(Star()).As("count")).ScanVal(&count)
	return count, err
}

//Generates the SELECT COUNT(*) sql for this dataset and uses Exec#ScanValContext to scan the result into an int64.
func (me *Dataset) CountContext(ctx context.Context) (int64, error) {
	var count int64
	_, err := me.Select(COUNT(Star()).As("count")).ScanValContext(ctx, &count)
	return count, err
}

//Generates the SELECT sql only selecting the passed in column and uses Exec#ScanVals to scan the result into a slice of primitive values.
//
//i: A slice of primitive values
//
//col: The column to select when generative the SQL
func (me *Dataset) Pluck(i interface{}, col string) error {
	return me.Select(col).ScanVals(i)
}

//Generates the SELECT sql only selecting the passed in column and uses Exec#ScanValsContext to scan the result into a slice of primitive values.
//
//i: A slice of primitive values
//
//col: The column to select when generative the SQL
func (me *Dataset) PluckContext(ctx context.Context, i interface{}, col string) error {
	return me.Select(col).ScanValsContext(ctx, i)
}

//Generates the UPDATE sql, and returns an Exec struct with the sql set to the UPDATE statement
//    db.From("test").Update(Record{"name":"Bob", update: time.Now()}).Exec()
//
//See Dataset#UpdateSql for arguments
func (me *Dataset) Update(i interface{}) *CrudExec {
	sql, args, err := me.ToUpdateSql(i)
	return newCrudExec(me.database, err, sql, args...)
}

//Generates the INSERT sql, and returns an Exec struct with the sql set to the INSERT statement
//    db.From("test").Insert(Record{"name":"Bob"}).Exec()
//
//See Dataset#InsertSql for arguments
func (me *Dataset) Insert(i ...interface{}) *CrudExec {
	sql, args, err := me.ToInsertSql(i...)
	return newCrudExec(me.database, err, sql, args...)
}

//Generates the INSERT IGNORE (mysql) or INSERT ... ON CONFLICT DO NOTHING (postgres) and returns an Exec struct.
//    db.From("test").InsertIgnore(DoNothing(), Record{"name":"Bob"}).Exec()
//
//See Dataset#InsertIgnore for arguments
func (me *Dataset) InsertIgnore(i ...interface{}) *CrudExec {
	sql, args, err := me.ToInsertConflictSql(DoNothing(), i...)
	return newCrudExec(me.database, err, sql, args...)
}

//Generates the INSERT sql with (ON CONFLICT/ON DUPLICATE KEY) clause, and returns an Exec struct with the sql set to the INSERT statement
//    db.From("test").InsertConflict(DoNothing(), Record{"name":"Bob"}).Exec()
//
//See Dataset#Upsert for arguments
func (me *Dataset) InsertConflict(c ConflictExpression, i ...interface{}) *CrudExec {
	sql, args, err := me.ToInsertConflictSql(c, i...)
	return newCrudExec(me.database, err, sql, args...)
}

//Generates the DELETE sql, and returns an Exec struct with the sql set to the DELETE statement
//    db.From("test").Where(I("id").Gt(10)).Exec()
func (me *Dataset) Delete() *CrudExec {
	sql, args, err := me.ToDeleteSql()
	return newCrudExec(me.database, err, sql, args...)
}
