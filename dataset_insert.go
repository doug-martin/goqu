package gql

import (
	"reflect"
	"sort"
	"strings"
)

func (me Dataset) InsertSql(rows ...interface{}) (string, error) {
	if !me.hasSources() {
		return "", newGqlError("No source found when generating insert sql")
	}
	switch len(rows) {
	case 0:
		return me.insertSql(nil, nil)
	case 1:
		switch rows[0].(type) {
		case Dataset:
			return me.insertFromSql(rows[0].(Dataset))
		}

	}
	columns, vals, err := me.getInsertColsAndVals(rows...)
	if err != nil {
		return "", err
	}
	return me.insertSql(columns, vals)
}

func (me Dataset) canInsertField(field reflect.StructField) bool {
	gqlTag, dbTag := tagOptions(field.Tag.Get("gql")), field.Tag.Get("db")
	return !gqlTag.Contains("skipinsert") && dbTag != "" && dbTag != "-"
}

func (me Dataset) getInsertColsAndVals(rows ...interface{}) (columns ColumnList, vals [][]interface{}, err error) {
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

func (me Dataset) insertSql(cols ColumnList, values [][]interface{}) (string, error) {
	var (
		err        error
		sql        string
		insertStmt []string
	)

	if sql, err = me.adapter.InsertBeginSql(); err != nil {
		return "", err
	}
	insertStmt = append(insertStmt, sql)
	if sql, err = me.adapter.SourcesSql(me.clauses.From); err != nil {
		return "", newGqlError(err.Error())
	}
	insertStmt = append(insertStmt, sql)
	if cols == nil {
		if sql, err = me.adapter.DefaultValuesSql(); err != nil {
			return "", newGqlError(err.Error())
		}
		insertStmt = append(insertStmt, sql)
	} else {
		if sql, err = me.adapter.InsertColumnsSql(cols); err != nil {
			return "", newGqlError(err.Error())
		}
		insertStmt = append(insertStmt, sql)
		if sql, err = me.adapter.InsertValuesSql(values); err != nil {
			return "", newGqlError(err.Error())
		}
		insertStmt = append(insertStmt, sql)
	}
	if sql, err = me.adapter.ReturningSql(me.clauses.Returning); err != nil {
		return "", err
	}
	insertStmt = append(insertStmt, sql)
	return strings.Join(insertStmt, ""), nil
}

func (me Dataset) insertFromSql(other Dataset) (string, error) {
	var (
		err        error
		sql        string
		insertStmt []string
	)

	if sql, err = me.adapter.InsertBeginSql(); err != nil {
		return "", err
	}
	insertStmt = append(insertStmt, sql)

	if sql, err = me.adapter.SourcesSql(me.clauses.From); err != nil {
		return "", newGqlError(err.Error())
	}
	insertStmt = append(insertStmt, sql)

	if sql, err = other.Sql(); err != nil {
		return "", err
	}
	insertStmt = append(insertStmt, " "+sql)
	if sql, err = me.adapter.ReturningSql(me.clauses.Returning); err != nil {
		return "", err
	}
	insertStmt = append(insertStmt, sql)
	return strings.Join(insertStmt, ""), nil
}
