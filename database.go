package gql

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

type (
	ColumnData struct {
		ColumnName string
		Transient  bool
		FieldName  string
		GoType     reflect.Type
	}
	ColumnMap map[string]ColumnData
	database  interface {
		QueryAdapter(builder *Dataset) Adapter
		From(cols ...interface{}) Dataset
		Logger(logger Logger)
		Exec(query string, args ...interface{}) (sql.Result, error)
		Prepare(query string) (*sql.Stmt, error)
		Query(query string, args ...interface{}) (*sql.Rows, error)
		QueryRow(query string, args ...interface{}) *sql.Row
		Select(i interface{}, query string, args ...interface{}) (bool, error)
		SelectIntoMap(cm ColumnMap, query string, args ...interface{}) (selectResults, error)
		Update(sql string, args ...interface{}) (int64, error)
		Delete(sql string, args ...interface{}) (int64, error)
	}
	Database struct {
		dbAdapter *DbAdapter
	}
)

var struct_map_cache = make(map[interface{}]ColumnMap)

func New(db Db) Database {
	return Database{newDbAdapter("", db)}
}

func (me Database) Begin() (TxDatabase, error) {
	txAdapter, err := me.dbAdapter.Begin()
	if err != nil {
		return TxDatabase{}, err
	}
	return TxDatabase{dbAdapter: txAdapter}, nil
}

func (me Database) QueryAdapter(builder *Dataset) Adapter {
	return me.dbAdapter.QueryAdapter(builder)
}

func (me Database) From(cols ...interface{}) Dataset {
	return withDatabase(me).From(cols...)

}

func (me Database) Logger(logger Logger) {
	me.dbAdapter.Logger(logger)
}

func (me Database) Exec(query string, args ...interface{}) (sql.Result, error) {
	return me.dbAdapter.Exec(query, args...)
}

func (me Database) Prepare(query string) (*sql.Stmt, error) {
	return me.dbAdapter.Prepare(query)
}
func (me Database) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return me.dbAdapter.Query(query, args...)
}
func (me Database) QueryRow(query string, args ...interface{}) *sql.Row {
	return me.dbAdapter.QueryRow(query, args...)
}

func (me Database) Select(i interface{}, query string, args ...interface{}) (bool, error) {
	var (
		found   bool
		results selectResults
	)
	val := reflect.ValueOf(i)
	if val.Kind() != reflect.Ptr {
		return found, newGqlError("Type must be a pointer to a slice when calling Query")
	}
	cm, err := getColumnMap(i)
	if err != nil {
		return found, err
	}
	if results, err = me.SelectIntoMap(cm, query, args...); err != nil {
		return found, err
	}
	if len(results) > 0 {
		found = true
		return found, assignVals(i, results, cm)
	}

	return found, nil
}

func (me Database) SelectIntoMap(cm ColumnMap, query string, args ...interface{}) (selectResults, error) {
	return me.dbAdapter.Select(cm, query, args...)
}

func (me Database) Update(sql string, args ...interface{}) (int64, error) {
	return me.dbAdapter.Update(sql, args...)
}

func (me Database) Delete(sql string, args ...interface{}) (int64, error) {
	return me.dbAdapter.Delete(sql, args...)
}

type TxDatabase struct {
	dbAdapter *TxDbAdapter
}

func (me TxDatabase) QueryAdapter(builder *Dataset) Adapter {
	return me.dbAdapter.QueryAdapter(builder)
}

func (me TxDatabase) From(cols ...interface{}) Dataset {
	return withDatabase(me).From(cols...)

}

func (me TxDatabase) Logger(logger Logger) {
	me.dbAdapter.Logger(logger)
}

func (me TxDatabase) Exec(query string, args ...interface{}) (sql.Result, error) {
	return me.dbAdapter.Exec(query, args...)
}

func (me TxDatabase) Prepare(query string) (*sql.Stmt, error) {
	return me.dbAdapter.Prepare(query)
}
func (me TxDatabase) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return me.dbAdapter.Query(query, args...)
}
func (me TxDatabase) QueryRow(query string, args ...interface{}) *sql.Row {
	return me.dbAdapter.QueryRow(query, args...)
}

func (me TxDatabase) Select(i interface{}, query string, args ...interface{}) (bool, error) {
	var (
		found   bool
		results selectResults
	)
	val := reflect.ValueOf(i)
	if val.Kind() != reflect.Ptr {
		return found, newGqlError("Type must be a pointer to a slice when calling Query")
	}
	cm, err := getColumnMap(i)
	if err != nil {
		return found, err
	}
	if results, err = me.SelectIntoMap(cm, query, args...); err != nil {
		return found, err
	}
	if len(results) > 0 {
		found = true
		return found, assignVals(i, results, cm)
	}

	return found, nil
}

func (me TxDatabase) SelectIntoMap(cm ColumnMap, query string, args ...interface{}) (selectResults, error) {
	return me.dbAdapter.Select(cm, query, args...)
}

func (me TxDatabase) Update(sql string, args ...interface{}) (int64, error) {
	return me.dbAdapter.Update(sql, args...)
}

func (me TxDatabase) Delete(sql string, args ...interface{}) (int64, error) {
	return me.dbAdapter.Delete(sql, args...)
}

func (me TxDatabase) Commit() error {
	return me.dbAdapter.Commit()
}

func (me TxDatabase) Rollback() error {
	return me.dbAdapter.Rollback()
}

func (me TxDatabase) Wrap(fn func() error) error {
	if err := fn(); err != nil {
		if rollbackErr := me.Rollback(); rollbackErr != nil {
			return rollbackErr
		}
		return err
	}
	return me.Commit()
}

func assignVals(i interface{}, results selectResults, cm ColumnMap) error {
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

func getColumnMap(i interface{}) (ColumnMap, error) {
	val := reflect.Indirect(reflect.ValueOf(i))
	t, valKind, _ := getTypeInfo(i, val)
	if valKind != reflect.Struct {
		return nil, newGqlError(fmt.Sprintf("Cannot SELECT into this type: %v", t))
	}
	if _, ok := struct_map_cache[t]; !ok {
		struct_map_cache[t] = createColumnMap(t)
	}
	return struct_map_cache[t], nil
}

func createColumnMap(t reflect.Type) ColumnMap {
	cm, n := ColumnMap{}, t.NumField()
	var subColMaps []ColumnMap
	for i := 0; i < n; i++ {
		f := t.Field(i)
		if f.Anonymous && f.Type.Kind() == reflect.Struct {
			subColMaps = append(subColMaps, createColumnMap(f.Type))
		} else {
			columnName := f.Tag.Get("db")
			if columnName == "" {
				columnName = strings.ToLower(f.Name)
			}
			cm[columnName] = ColumnData{
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
