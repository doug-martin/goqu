package goqu

import (
	"reflect"
	"sort"
)

//Generates the default INSERT statement. This calls ToInsertSql with isPrepared set to false.
//When using structs you may specify a column to be skipped in the insert, (e.g. id) by specifying a goqu tag with `skipinsert`
//    type Item struct{
//       Id   uint32 `db:"id" goqu:"skipinsert"`
//       Name string `db:"name"`
//    }
//
//rows: variable number arguments of either map[string]interface, Record, struct, or a single slice argument of the accepted types.
//
//Errors:
//  * There is no FROM clause
//  * Different row types passed in, all rows must be of the same type
//  * Maps with different keys pairs (e.g. (Record{"name": "a"}, Record{"age": 10})
//  * Rows of different lengths, (e.g. (Record{"name": "a"}, Record{"name": "a", "age": 10})
//  * Error generating SQL
func (me *Dataset) InsertSql(rows ...interface{}) (string, error) {
	sql, _, err := me.ToInsertSql(false, rows...)
	return sql, err
}

//Generates the default INSERT statement. This calls ToInsertSql with isPrepared set to false.
//When using structs you may specify a column to be skipped in the insert, (e.g. id) by specifying a goqu tag with `skipinsert`
//    type Item struct{
//       Id   uint32 `db:"id" goqu:"skipinsert"`
//       Name string `db:"name"`
//    }
//
//isPrepared: Set to true to true to ensure values are NOT interpolated
//
//rows: variable number arguments of either map[string]interface, Record, struct, or a single slice argument of the accepted types.
//
//Errors:
//  * There is no FROM clause
//  * Different row types passed in, all rows must be of the same type
//  * Maps with different numbers of K/V pairs
//  * Rows of different lengths, (i.e. (Record{"name": "a"}, Record{"name": "a", "age": 10})
//  * Error generating SQL
func (me *Dataset) ToInsertSql(isPrepared bool, rows ...interface{}) (string, []interface{}, error) {
	if !me.hasSources() {
		return "", nil, NewGoquError("No source found when generating insert sql")
	}
	switch len(rows) {
	case 0:
		return me.insertSql(nil, nil, isPrepared)
	case 1:
		val := reflect.ValueOf(rows[0])
		if val.Kind() == reflect.Slice {
			vals := make([]interface{}, val.Len())
			for i := 0; i < val.Len(); i++ {
				vals[i] = val.Index(i).Interface()
			}
			return me.ToInsertSql(isPrepared, vals...)
		}
		switch rows[0].(type) {
		case *Dataset:
			return me.insertFromSql(*rows[0].(*Dataset), isPrepared)
		}

	}
	columns, vals, err := me.getInsertColsAndVals(rows...)
	if err != nil {
		return "", nil, err
	}
	return me.insertSql(columns, vals, isPrepared)
}

func (me *Dataset) canInsertField(field reflect.StructField) bool {
	goquTag, dbTag := tagOptions(field.Tag.Get("goqu")), field.Tag.Get("db")
	return !goquTag.Contains("skipinsert") && dbTag != "" && dbTag != "-"
}

//parses the rows gathering and sorting unique columns and values for each record
func (me *Dataset) getInsertColsAndVals(rows ...interface{}) (columns ColumnList, vals [][]interface{}, err error) {
	var mapKeys valueSlice
	rowValue := reflect.Indirect(reflect.ValueOf(rows[0]))
	rowType := rowValue.Type()
	rowKind := rowValue.Kind()
	vals = make([][]interface{}, len(rows))
	for i, row := range rows {
		if rowType != reflect.Indirect(reflect.ValueOf(row)).Type() {
			return nil, nil, NewGoquError("Rows must be all the same type expected %+v got %+v", rowType, reflect.Indirect(reflect.ValueOf(row)).Type())
		}
		newRowValue := reflect.Indirect(reflect.ValueOf(row))
		switch rowKind {
		case reflect.Map:
			if columns == nil {
				mapKeys = valueSlice(newRowValue.MapKeys())
				sort.Sort(mapKeys)
				colKeys := make([]interface{}, len(mapKeys))
				for j, key := range mapKeys {
					colKeys[j] = key.Interface()
				}
				columns = cols(colKeys...)
			}
			newMapKeys := valueSlice(newRowValue.MapKeys())
			if len(newMapKeys) != len(mapKeys) {
				return nil, nil, NewGoquError("Rows with different value length expected %d got %d", len(mapKeys), len(newMapKeys))
			}
			if !mapKeys.Equal(newMapKeys) {
				return nil, nil, NewGoquError("Rows with different keys expected %s got %s", mapKeys.String(), newMapKeys.String())
			}
			rowVals := make([]interface{}, len(mapKeys))
			for j, key := range mapKeys {
				rowVals[j] = newRowValue.MapIndex(key).Interface()
			}
			vals[i] = rowVals
		case reflect.Struct:
			var (
				rowCols []interface{}
				rowVals []interface{}
			)
			for j := 0; j < newRowValue.NumField(); j++ {
				f := newRowValue.Field(j)
				t := newRowValue.Type().Field(j)
				if me.canInsertField(t) {
					if columns == nil {
						rowCols = append(rowCols, t.Tag.Get("db"))
					}
					rowVals = append(rowVals, f.Interface())
				}
			}
			if columns == nil {
				columns = cols(rowCols...)
			}
			vals[i] = rowVals
		default:
			return nil, nil, NewGoquError("Unsupported insert must be map, goqu.Record, or struct type %+v", row)
		}
	}
	return columns, vals, nil
}

//Creates an INSERT statement with the columns and values passed in
func (me *Dataset) insertSql(cols ColumnList, values [][]interface{}, prepared bool) (string, []interface{}, error) {
	buf := NewSqlBuilder(prepared)
	if err := me.adapter.InsertBeginSql(buf); err != nil {
		return "", nil, err
	}
	if err := me.adapter.SourcesSql(buf, me.clauses.From); err != nil {
		return "", nil, NewGoquError(err.Error())
	}
	if cols == nil {
		if err := me.adapter.DefaultValuesSql(buf); err != nil {
			return "", nil, NewGoquError(err.Error())
		}
	} else {
		if err := me.adapter.InsertColumnsSql(buf, cols); err != nil {
			return "", nil, NewGoquError(err.Error())
		}
		if err := me.adapter.InsertValuesSql(buf, values); err != nil {
			return "", nil, NewGoquError(err.Error())
		}
	}
	if me.adapter.SupportsReturn() {
		if err := me.adapter.ReturningSql(buf, me.clauses.Returning); err != nil {
			return "", nil, err
		}
	} else if me.clauses.Returning != nil {
		return "", nil, NewGoquError("Adapter does not support RETURNING clause")
	}

	sql, args := buf.ToSql()
	return sql, args, nil
}

//Creates an insert statement with values coming from another dataset
func (me *Dataset) insertFromSql(other Dataset, prepared bool) (string, []interface{}, error) {
	buf := NewSqlBuilder(prepared)
	if err := me.adapter.InsertBeginSql(buf); err != nil {
		return "", nil, err
	}
	if err := me.adapter.SourcesSql(buf, me.clauses.From); err != nil {
		return "", nil, NewGoquError(err.Error())
	}
	buf.WriteString(" ")
	if err := other.selectSqlWriteTo(buf); err != nil {
		return "", nil, err
	}
	if me.adapter.SupportsReturn() {
		if err := me.adapter.ReturningSql(buf, me.clauses.Returning); err != nil {
			return "", nil, err
		}
	} else if me.clauses.Returning != nil {
		return "", nil, NewGoquError("Adapter does not support RETURNING clause")
	}
	sql, args := buf.ToSql()
	return sql, args, nil
}
