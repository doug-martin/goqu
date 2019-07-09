package util

import (
	"reflect"
	"strings"
	"sync"

	"github.com/doug-martin/goqu/v7/internal/errors"
)

type (
	ColumnData struct {
		ColumnName string
		FieldName  string
		GoType     reflect.Type
	}
	ColumnMap map[string]ColumnData
)

func IsUint(k reflect.Kind) bool {
	return (k == reflect.Uint) ||
		(k == reflect.Uint8) ||
		(k == reflect.Uint16) ||
		(k == reflect.Uint32) ||
		(k == reflect.Uint64)
}

func IsInt(k reflect.Kind) bool {
	return (k == reflect.Int) ||
		(k == reflect.Int8) ||
		(k == reflect.Int16) ||
		(k == reflect.Int32) ||
		(k == reflect.Int64)
}

func IsFloat(k reflect.Kind) bool {
	return (k == reflect.Float32) ||
		(k == reflect.Float64)
}

func IsString(k reflect.Kind) bool {
	return k == reflect.String
}

func IsBool(k reflect.Kind) bool {
	return k == reflect.Bool
}

func IsSlice(k reflect.Kind) bool {
	return k == reflect.Slice
}

func IsStruct(k reflect.Kind) bool {
	return k == reflect.Struct
}

func IsInvalid(k reflect.Kind) bool {
	return k == reflect.Invalid
}

func IsPointer(k reflect.Kind) bool {
	return k == reflect.Ptr
}

var structMapCache = make(map[interface{}]ColumnMap)
var structMapCacheLock = sync.Mutex{}

var defaultColumnRenameFunction = strings.ToLower
var columnRenameFunction = defaultColumnRenameFunction

func SetColumnRenameFunction(newFunction func(string) string) {
	columnRenameFunction = newFunction
}

func GetTypeInfo(i interface{}, val reflect.Value) (reflect.Type, reflect.Kind, bool) {
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

type rowData = map[string]interface{}

func AssignStructVals(i interface{}, results []rowData, cm ColumnMap) {
	val := reflect.Indirect(reflect.ValueOf(i))
	t, _, isSliceOfPointers := GetTypeInfo(i, val)
	switch val.Kind() {
	case reflect.Struct:
		result := results[0]
		assignRowData(val, result, cm)
	case reflect.Slice:
		for _, result := range results {
			row := reflect.Indirect(reflect.New(t))
			assignRowData(row, result, cm)
			if isSliceOfPointers {
				val.Set(reflect.Append(val, row.Addr()))
			} else {
				val.Set(reflect.Append(val, row))
			}
		}
	}
}

func assignRowData(row reflect.Value, rd rowData, cm ColumnMap) {
	initEmbeddedPtr(row)
	for name, data := range cm {
		src, ok := rd[name]
		if ok {
			srcVal := reflect.ValueOf(src)
			f := row.FieldByName(data.FieldName)
			if f.Kind() == reflect.Ptr {
				f.Set(srcVal)
			} else {
				f.Set(reflect.Indirect(srcVal))
			}
		}
	}
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

func GetColumnMap(i interface{}) (ColumnMap, error) {
	val := reflect.Indirect(reflect.ValueOf(i))
	t, valKind, _ := GetTypeInfo(i, val)
	if valKind != reflect.Struct {
		return nil, errors.New("cannot scan into this type: %v", t) // #nosec
	}

	structMapCacheLock.Lock()
	defer structMapCacheLock.Unlock()
	if _, ok := structMapCache[t]; !ok {
		structMapCache[t] = createColumnMap(t)
	}
	return structMapCache[t], nil
}

func createColumnMap(t reflect.Type) ColumnMap {
	cm, n := ColumnMap{}, t.NumField()
	var subColMaps []ColumnMap
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
			if columnName != "-" {
				cm[columnName] = ColumnData{
					ColumnName: columnName,
					FieldName:  f.Name,
					GoType:     f.Type,
				}
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
