package gql

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

type (
	columnData struct {
		ColumnName string
		Transient  bool
		FieldName  string
		GoType     reflect.Type
	}
	columnMap map[string]columnData
	exec      struct {
		database database
		sql      string
		args     []interface{}
		err      error
	}
)

var struct_map_cache = make(map[interface{}]columnMap)

func newExec(database database, err error, sql string, args ...interface{}) *exec {
	return &exec{database: database, err: err, sql: sql, args: args}
}

func (me exec) Exec() (sql.Result, error) {
	if me.err != nil {
		return nil, me.err
	}
	return me.database.Exec(me.sql, me.args...)
}

func (me exec) ScanStructs(i interface{}) error {
	if me.err != nil {
		return me.err
	}
	val := reflect.ValueOf(i)
	if val.Kind() != reflect.Ptr {
		return NewGqlError("Type must be a pointer to a slice when calling ScanStructs")
	}
	if reflect.Indirect(val).Kind() != reflect.Slice {
		return NewGqlError("Type must be a pointer to a slice when calling ScanStructs")
	}
	_, err := me.scan(i, me.sql, me.args...)
	return err
}

func (me exec) ScanStruct(i interface{}) (bool, error) {
	if me.err != nil {
		return false, me.err
	}
	val := reflect.ValueOf(i)
	if val.Kind() != reflect.Ptr {
		return false, NewGqlError("Type must be a pointer to a struct when calling ScanStruct")
	}
	if reflect.Indirect(val).Kind() != reflect.Struct {
		return false, NewGqlError("Type must be a pointer to a struct when calling ScanStruct")
	}
	return me.scan(i, me.sql, me.args...)
}

func (me exec) ScanVals(i interface{}) error {
	if me.err != nil {
		return me.err
	}
	val := reflect.ValueOf(i)
	if val.Kind() != reflect.Ptr {
		return NewGqlError("Type must be a pointer to a slice when calling ScanVals")
	}
	val = reflect.Indirect(val)
	if val.Kind() != reflect.Slice {
		return NewGqlError("Type must be a pointer to a slice when calling ScanVals")
	}
	t, _, isSliceOfPointers := getTypeInfo(i, val)
	rows, err := me.database.Query(me.sql, me.args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		row := reflect.New(t)
		if err := rows.Scan(row.Interface()); err != nil {
			return err
		}
		if isSliceOfPointers {
			val.Set(reflect.Append(val, row.Addr()))
		} else {
			val.Set(reflect.Append(val, reflect.Indirect(row)))
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return nil

}

func (me exec) ScanVal(i interface{}) (bool, error) {
	if me.err != nil {
		return false, me.err
	}
	val := reflect.ValueOf(i)
	if val.Kind() != reflect.Ptr {
		return false, NewGqlError("Type must be a pointer to a slice when calling ScanVals")
	}
	rows, err := me.database.Query(me.sql, me.args...)
	if err != nil {
		return false, err
	}
	count := 0
	defer rows.Close()
	for rows.Next() {
		count++
		if err := rows.Scan(i); err != nil {
			return false, err
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	return count != 0, nil
}

func (me exec) scan(i interface{}, query string, args ...interface{}) (bool, error) {
	var (
		found   bool
		results []Result
	)
	cm, err := getColumnMap(i)
	if err != nil {
		return found, err
	}
	rows, err := me.database.Query(query, args...)
	if err != nil {
		return false, NewGqlError(err.Error())
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return false, NewGqlError(err.Error())
	}
	for rows.Next() {
		scans := make([]interface{}, len(columns))
		for i, col := range columns {
			if data, ok := cm[col]; ok {
				scans[i] = reflect.New(data.GoType).Interface()
			} else {
				return false, NewGqlError(`Unable to find corresponding field to column "%s" returned by query`, col)
			}
		}
		if err := rows.Scan(scans...); err != nil {
			return false, NewGqlError(err.Error())
		}
		result := Result{}
		for index, col := range columns {
			result[col] = scans[index]
		}
		results = append(results, result)
	}
	if rows.Err() != nil {
		return false, NewGqlError(rows.Err().Error())
	}
	if len(results) > 0 {
		found = true
		return found, assignVals(i, results, cm)
	}
	return found, nil
}

func (me exec) scanRowsToResult(rows *sql.Rows, columnMap columnMap) ([]Result, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var results []Result
	for rows.Next() {
		scans := make([]interface{}, len(columns))
		for i, col := range columns {
			if data, ok := columnMap[col]; ok {
				scans[i] = reflect.New(data.GoType).Interface()
			} else {
				return nil, NewGqlError(`Unable to find corresponding field to column "%s" returned by query`, col)
			}
		}
		if err := rows.Scan(scans...); err != nil {
			return nil, err
		}
		result := Result{}
		for index, col := range columns {
			result[col] = scans[index]
		}
		results = append(results, result)
	}
	if rows.Err() != nil {
		return nil, err
	}
	return results, nil
}

func assignVals(i interface{}, results []Result, cm columnMap) error {
	val := reflect.Indirect(reflect.ValueOf(i))
	t, _, isSliceOfPointers := getTypeInfo(i, val)
	switch val.Kind() {
	case reflect.Struct:
		result := results[0]
		for name, data := range cm {
			src, ok := result[name]
			if ok {
				srcVal := reflect.ValueOf(src)
				f := val.FieldByName(data.FieldName)
				if f.Kind() == reflect.Ptr {
					f.Set(reflect.ValueOf(srcVal))
				} else {
					f.Set(reflect.Indirect(srcVal))
				}
			}
		}
	case reflect.Slice:
		for _, result := range results {
			row := reflect.Indirect(reflect.New(t))
			for name, data := range cm {
				src, ok := result[name]
				if ok {
					srcVal := reflect.ValueOf(src)
					f := row.FieldByName(data.FieldName)
					if f.Kind() == reflect.Ptr {
						f.Set(reflect.ValueOf(srcVal))
					} else {
						f.Set(reflect.Indirect(srcVal))
					}
				}
			}
			if isSliceOfPointers {
				val.Set(reflect.Append(val, row.Addr()))
			} else {
				val.Set(reflect.Append(val, row))
			}
		}
	}
	return nil
}

func getColumnMap(i interface{}) (columnMap, error) {
	val := reflect.Indirect(reflect.ValueOf(i))
	t, valKind, _ := getTypeInfo(i, val)
	if valKind != reflect.Struct {
		return nil, NewGqlError(fmt.Sprintf("Cannot SELECT into this type: %v", t))
	}
	if _, ok := struct_map_cache[t]; !ok {
		struct_map_cache[t] = createColumnMap(t)
	}
	return struct_map_cache[t], nil
}

func createColumnMap(t reflect.Type) columnMap {
	cm, n := columnMap{}, t.NumField()
	var subColMaps []columnMap
	for i := 0; i < n; i++ {
		f := t.Field(i)
		if f.Anonymous && f.Type.Kind() == reflect.Struct {
			subColMaps = append(subColMaps, createColumnMap(f.Type))
		} else {
			columnName := f.Tag.Get("db")
			if columnName == "" {
				columnName = strings.ToLower(f.Name)
			}
			cm[columnName] = columnData{
				ColumnName: columnName,
				Transient:  columnName == "-",
				FieldName:  f.Name,
				GoType:     f.Type,
			}
		}
	}
	for _, subCm := range subColMaps {
		for key, val := range subCm {
			if _, ok := cm[key]; !ok {
				cm[key] = val
			}
		}
	}
	return cm
}

func getTypeInfo(i interface{}, val reflect.Value) (reflect.Type, reflect.Kind, bool) {
	var t reflect.Type
	isSliceOfPointers := false
	valKind := val.Kind()
	if valKind == reflect.Slice {
		t = reflect.TypeOf(i).Elem().Elem()
		if t.Kind() == reflect.Ptr {
			isSliceOfPointers = true
			t = t.Elem()
		}
		valKind = t.Kind()
	} else {
		t = val.Type()
	}
	return t, valKind, isSliceOfPointers
}
