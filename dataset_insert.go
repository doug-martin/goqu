package gql

import (
	"reflect"
	"sort"
)

func (me *Dataset) InsertSql(rows ...interface{}) (string, error) {
	if !me.hasSources() {
		return "", newGqlError("No source found when generating insert sql")
	}
	switch len(rows) {
	case 0:
		sql, _, err := me.insertSql(nil, nil, false)
		return sql, err
	case 1:
		switch rows[0].(type) {
		case *Dataset:
			sql, _, err := me.insertFromSql(*rows[0].(*Dataset), false)
			return sql, err
		}

	}
	columns, vals, err := me.getInsertColsAndVals(rows...)
	if err != nil {
		return "", err
	}
	sql, _, err := me.insertSql(columns, vals, false)
	return sql, err
}

func (me *Dataset) PreparedInsertSql(rows ...interface{}) (string, []interface{}, error) {
	if !me.hasSources() {
		return "", nil, newGqlError("No source found when generating insert sql")
	}
	switch len(rows) {
	case 0:
		return me.insertSql(nil, nil, true)
	case 1:
		switch rows[0].(type) {
		case *Dataset:
			return me.insertFromSql(*rows[0].(*Dataset), true)
		}

	}
	columns, vals, err := me.getInsertColsAndVals(rows...)
	if err != nil {
		return "", nil, err
	}
	return me.insertSql(columns, vals, true)
}

func (me *Dataset) canInsertField(field reflect.StructField) bool {
	gqlTag, dbTag := tagOptions(field.Tag.Get("gql")), field.Tag.Get("db")
	return !gqlTag.Contains("skipinsert") && dbTag != "" && dbTag != "-"
}

func (me *Dataset) getInsertColsAndVals(rows ...interface{}) (columns ColumnList, vals [][]interface{}, err error) {
	var mapKeys valueSlice
	rowValue := reflect.Indirect(reflect.ValueOf(rows[0]))
	rowType := rowValue.Type()
	rowKind := rowValue.Kind()
	vals = make([][]interface{}, len(rows))
	for i, row := range rows {
		if rowType != reflect.Indirect(reflect.ValueOf(row)).Type() {
			return nil, nil, newGqlError("Rows must be all the same type expected %+v got %+v", rowType, reflect.Indirect(reflect.ValueOf(row)).Type())
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
				return nil, nil, newGqlError("Rows with different value length expected %d got %d", len(mapKeys), len(newMapKeys))
			}
			if !mapKeys.Equal(newMapKeys) {
				return nil, nil, newGqlError("Rows with different keys expected %s got %s", mapKeys.String(), newMapKeys.String())
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
			return nil, nil, newGqlError("Unsupported insert must be map or struct type %+v", row)
		}
	}
	return columns, vals, nil
}

func (me *Dataset) insertSql(cols ColumnList, values [][]interface{}, prepared bool) (string, []interface{}, error) {
	buf := NewSqlBuilder(prepared)
	if err := me.adapter.InsertBeginSql(buf); err != nil {
		return "", nil, err
	}
	if err := me.adapter.SourcesSql(buf, me.clauses.From); err != nil {
		return "", nil, newGqlError(err.Error())
	}
	if cols == nil {
		if err := me.adapter.DefaultValuesSql(buf); err != nil {
			return "", nil, newGqlError(err.Error())
		}
	} else {
		if err := me.adapter.InsertColumnsSql(buf, cols); err != nil {
			return "", nil, newGqlError(err.Error())
		}
		if err := me.adapter.InsertValuesSql(buf, values); err != nil {
			return "", nil, newGqlError(err.Error())
		}
	}
	if err := me.adapter.ReturningSql(buf, me.clauses.Returning); err != nil {
		return "", nil, err
	}
	sql, args := buf.ToSql()
	return sql, args, nil
}

func (me *Dataset) insertFromSql(other Dataset, prepared bool) (string, []interface{}, error) {
	buf := NewSqlBuilder(prepared)
	if err := me.adapter.InsertBeginSql(buf); err != nil {
		return "", nil, err
	}
	if err := me.adapter.SourcesSql(buf, me.clauses.From); err != nil {
		return "", nil, newGqlError(err.Error())
	}
	buf.WriteString(" ")
	if err := other.selectSqlWriteTo(buf); err != nil {
		return "", nil, err
	}
	if err := me.adapter.ReturningSql(buf, me.clauses.Returning); err != nil {
		return "", nil, err
	}
	sql, args := buf.ToSql()
	return sql, args, nil
}
