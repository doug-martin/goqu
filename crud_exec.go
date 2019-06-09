package goqu

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

type (
	columnData struct {
		ColumnName string
		Transient  bool
		FieldName  string
		GoType     reflect.Type
	}
	columnMap map[string]columnData
	CrudExec  struct {
		database database
		Sql      string
		Args     []interface{}
		err      error
	}
	selectResults []Record
)

var defaultColumnRenameFunction = strings.ToLower
var columnRenameFunction = defaultColumnRenameFunction
func SetColumnRenameFunction(new_function func(string) string) {
	columnRenameFunction = new_function
}

var struct_map_cache = make(map[interface{}]columnMap)
var struct_map_cache_lock = sync.Mutex{}

func newCrudExec(database database, err error, sql string, args ...interface{}) *CrudExec {
	return &CrudExec{database: database, err: err, Sql: sql, Args: args}
}

func (me CrudExec) Exec() (sql.Result, error) {
	return me.ExecContext(context.Background())
}

func (me CrudExec) ExecContext(ctx context.Context) (sql.Result, error) {
	if me.err != nil {
		return nil, me.err
	}
	return me.database.ExecContext(ctx, me.Sql, me.Args...)
}

//This will execute the SQL and append results to the slice
//    var myStructs []MyStruct
//    if err := From("test").ScanStructs(&myStructs); err != nil{
//        panic(err.Error()
//    }
//    //use your structs
//
//
//i: A pointer to a slice of structs.
func (me CrudExec) ScanStructs(i interface{}) error {
	return me.ScanStructsContext(context.Background(), i)
}

//This will execute the SQL and append results to the slice
//    var myStructs []MyStruct
//    if err := From("test").ScanStructsContext(ctx, &myStructs); err != nil{
//        panic(err.Error()
//    }
//    //use your structs
//
//
//i: A pointer to a slice of structs.
func (me CrudExec) ScanStructsContext(ctx context.Context, i interface{}) error {
	if me.err != nil {
		return me.err
	}
	val := reflect.ValueOf(i)
	if val.Kind() != reflect.Ptr {
		return NewGoquError("Type must be a pointer to a slice when calling ScanStructs")
	}
	if reflect.Indirect(val).Kind() != reflect.Slice {
		return NewGoquError("Type must be a pointer to a slice when calling ScanStructs")
	}
	_, err := me.scanContext(ctx, i, me.Sql, me.Args...)
	return err
}

//This will execute the SQL and fill out the struct with the fields returned. This method returns a boolean value that is false if no record was found
//    var myStruct MyStruct
//    found, err := From("test").Limit(1).ScanStruct(&myStruct)
//    if err != nil{
//        panic(err.Error()
//    }
//    if !found{
//          fmt.Println("NOT FOUND")
//    }
//
//i: A pointer to a struct
func (me CrudExec) ScanStruct(i interface{}) (bool, error) {
	return me.ScanStructContext(context.Background(), i)
}

//This will execute the SQL and fill out the struct with the fields returned. This method returns a boolean value that is false if no record was found
//    var myStruct MyStruct
//    found, err := From("test").Limit(1).ScanStructContext(ctx, &myStruct)
//    if err != nil{
//        panic(err.Error()
//    }
//    if !found{
//          fmt.Println("NOT FOUND")
//    }
//
//i: A pointer to a struct
func (me CrudExec) ScanStructContext(ctx context.Context, i interface{}) (bool, error) {
	if me.err != nil {
		return false, me.err
	}
	val := reflect.ValueOf(i)
	if val.Kind() != reflect.Ptr {
		return false, NewGoquError("Type must be a pointer to a struct when calling ScanStruct")
	}
	if reflect.Indirect(val).Kind() != reflect.Struct {
		return false, NewGoquError("Type must be a pointer to a struct when calling ScanStruct")
	}
	return me.scanContext(ctx, i, me.Sql, me.Args...)
}

//This will execute the SQL and append results to the slice.
//    var ids []uint32
//    if err := From("test").Select("id").ScanVals(&ids); err != nil{
//        panic(err.Error()
//    }
//
//i: Takes a pointer to a slice of primitive values.
func (me CrudExec) ScanVals(i interface{}) error {
	return me.ScanValsContext(context.Background(), i)
}

//This will execute the SQL and append results to the slice.
//    var ids []uint32
//    if err := From("test").Select("id").ScanValsContext(ctx, &ids); err != nil{
//        panic(err.Error()
//    }
//
//i: Takes a pointer to a slice of primitive values.
func (me CrudExec) ScanValsContext(ctx context.Context, i interface{}) error {
	if me.err != nil {
		return me.err
	}
	val := reflect.ValueOf(i)
	if val.Kind() != reflect.Ptr {
		return NewGoquError("Type must be a pointer to a slice when calling ScanVals")
	}
	val = reflect.Indirect(val)
	if val.Kind() != reflect.Slice {
		return NewGoquError("Type must be a pointer to a slice when calling ScanVals")
	}
	t, _, isSliceOfPointers := getTypeInfo(i, val)
	rows, err := me.database.QueryContext(ctx, me.Sql, me.Args...)
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
			val.Set(reflect.Append(val, row))
		} else {
			val.Set(reflect.Append(val, reflect.Indirect(row)))
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}

//This will execute the SQL and set the value of the primitive. This method will return false if no record is found.
//    var id uint32
//    found, err := From("test").Select("id").Limit(1).ScanVal(&id)
//    if err != nil{
//        panic(err.Error()
//    }
//    if !found{
//        fmt.Println("NOT FOUND")
//    }
//
//   i: Takes a pointer to a primitive value.
func (me CrudExec) ScanVal(i interface{}) (bool, error) {
	return me.ScanValContext(context.Background(), i)
}

//This will execute the SQL and set the value of the primitive. This method will return false if no record is found.
//    var id uint32
//    found, err := From("test").Select("id").Limit(1).ScanValContext(ctx, &id)
//    if err != nil{
//        panic(err.Error()
//    }
//    if !found{
//        fmt.Println("NOT FOUND")
//    }
//
//   i: Takes a pointer to a primitive value.
func (me CrudExec) ScanValContext(ctx context.Context, i interface{}) (bool, error) {
	if me.err != nil {
		return false, me.err
	}
	val := reflect.ValueOf(i)
	if val.Kind() != reflect.Ptr {
		return false, NewGoquError("Type must be a pointer when calling ScanVal")
	}
	val = reflect.Indirect(val)
	if val.Kind() == reflect.Slice {
		return false, NewGoquError("Cannot scan into a slice when calling ScanVal")
	}
	rows, err := me.database.QueryContext(ctx, me.Sql, me.Args...)
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

func (me CrudExec) scanContext(ctx context.Context, i interface{}, query string, args ...interface{}) (bool, error) {
	var (
		found   bool
		results []Record
	)
	cm, err := getColumnMap(i)
	if err != nil {
		return found, err
	}
	rows, err := me.database.QueryContext(ctx, query, args...)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return false, err
	}
	for rows.Next() {
		scans := make([]interface{}, len(columns))
		for i, col := range columns {
			if data, ok := cm[col]; ok {
				scans[i] = reflect.New(data.GoType).Interface()
			} else {
				return false, NewGoquError(`Unable to find corresponding field to column "%s" returned by query`, col)
			}
		}
		if err := rows.Scan(scans...); err != nil {
			return false, err
		}
		result := Record{}
		for index, col := range columns {
			result[col] = scans[index]
		}
		results = append(results, result)
	}
	if rows.Err() != nil {
		return false, rows.Err()
	}
	if len(results) > 0 {
		found = true
		return found, assignVals(i, results, cm)
	}
	return found, nil
}

func assignVals(i interface{}, results []Record, cm columnMap) error {
	val := reflect.Indirect(reflect.ValueOf(i))
	t, _, isSliceOfPointers := getTypeInfo(i, val)
	switch val.Kind() {
	case reflect.Struct:
		result := results[0]
		initEmbeddedPtr(val)
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
			initEmbeddedPtr(row)
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

func initEmbeddedPtr(value reflect.Value) {
	for i := 0; i < value.NumField(); i++ {
		v := value.Field(i)
		kind := v.Kind()
		t := value.Type().Field(i)
		if t.Anonymous && kind == reflect.Ptr {
			z := reflect.New(t.Type.Elem())
			v.Set(z)
		}
	}
}

func getColumnMap(i interface{}) (columnMap, error) {
	val := reflect.Indirect(reflect.ValueOf(i))
	t, valKind, _ := getTypeInfo(i, val)
	if valKind != reflect.Struct {
		return nil, NewGoquError(fmt.Sprintf("Cannot SELECT into this type: %v", t))
	}

	struct_map_cache_lock.Lock()
	defer struct_map_cache_lock.Unlock()
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
		if f.Anonymous && (f.Type.Kind() == reflect.Struct || f.Type.Kind() == reflect.Ptr) {
			if f.Type.Kind() == reflect.Ptr {
				subColMaps = append(subColMaps, createColumnMap(f.Type.Elem()))
			} else {
				subColMaps = append(subColMaps, createColumnMap(f.Type))
			}
		} else {
			columnName := f.Tag.Get("db")
			if columnName == "" {
				columnName = columnRenameFunction(f.Name)
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
		if reflect.ValueOf(i).Kind() == reflect.Ptr {
			t = reflect.TypeOf(i).Elem().Elem()
		} else {
			t = reflect.TypeOf(i).Elem()
		}
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
