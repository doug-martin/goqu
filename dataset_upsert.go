package goqu

//Generates the default UPSERT (INSERT ... ON CONFLICT) statement. If Prepared has been called with true then the statement will not be interpolated. See examples.
//When using structs you may specify a column to be skipped in the insert, (e.g. id) by specifying a goqu tag with `skipinsert`
//    type Item struct{
//       Id   uint32 `db:"id" goqu:"skipinsert"`
//       Name string `db:"name"`
//    }
//
//c: ConflictExpression action. Can be DoNothing/Ignore or DoUpdate/DoUpdateWhere.
//rows: variable number arguments of either map[string]interface, Record, struct, or a single slice argument of the accepted types.
//
//Errors:
//  * There is no FROM clause
//  * Different row types passed in, all rows must be of the same type
//  * Maps with different numbers of K/V pairs
//  * Rows of different lengths, (i.e. (Record{"name": "a"}, Record{"name": "a", "age": 10})
//  * Error generating SQL
func (me *Dataset) ToUpsertSql(c ConflictExpression, rows ...interface{}) (string, []interface{}, error) {
	if c == nil {
		c = DoNothing()
	}
	return me.toInsertSql(c, rows...)
}