package util

import (
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/doug-martin/goqu/v9/internal/errors"
	"github.com/doug-martin/goqu/v9/internal/tag"
)

type (
	ColumnData struct {
		ColumnName     string
		FieldIndex     []int
		ShouldInsert   bool
		ShouldUpdate   bool
		DefaultIfEmpty bool
		GoType         reflect.Type
	}
	ColumnMap map[string]ColumnData
)

const (
	skipUpdateTagName     = "skipupdate"
	skipInsertTagName     = "skipinsert"
	defaultIfEmptyTagName = "defaultifempty"
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

func IsEmptyValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	case reflect.Invalid:
		return true
	}
	return false
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

func SafeGetFieldByIndex(v reflect.Value, fieldIndex []int) (result reflect.Value, isAvailable bool) {
	switch len(fieldIndex) {
	case 0:
		return v, true
	case 1:
		return v.FieldByIndex(fieldIndex), true
	default:
		if f := reflect.Indirect(v.Field(fieldIndex[0])); f.IsValid() {
			return SafeGetFieldByIndex(f, fieldIndex[1:])
		}
	}
	return reflect.ValueOf(nil), false
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
			f := row.FieldByIndex(data.FieldIndex)
			srcVal := reflect.ValueOf(src)
			f.Set(reflect.Indirect(srcVal))
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
		structMapCache[t] = createColumnMap(t, []int{})
	}
	return structMapCache[t], nil
}

func createColumnMap(t reflect.Type, fieldIndex []int) ColumnMap {
	cm, n := ColumnMap{}, t.NumField()
	var subColMaps []ColumnMap
	for i := 0; i < n; i++ {
		f := t.Field(i)
		if f.Anonymous && (f.Type.Kind() == reflect.Struct || f.Type.Kind() == reflect.Ptr) {
			goquTag := tag.New("db", f.Tag)
			if !goquTag.Contains("-") {
				if f.Type.Kind() == reflect.Ptr {
					subColMaps = append(subColMaps, createColumnMap(f.Type.Elem(), append(fieldIndex, f.Index...)))
				} else {
					subColMaps = append(subColMaps, createColumnMap(f.Type, append(fieldIndex, f.Index...)))
				}
			}
		} else if f.PkgPath == "" {
			// if PkgPath is empty then it is an exported field
			dbTag := tag.New("db", f.Tag)
			var columnName string
			if dbTag.IsEmpty() {
				columnName = columnRenameFunction(f.Name)
			} else {
				columnName = dbTag.Values()[0]
			}
			goquTag := tag.New("goqu", f.Tag)
			if !dbTag.Equals("-") {
				cm[columnName] = ColumnData{
					ColumnName:     columnName,
					ShouldInsert:   !goquTag.Contains(skipInsertTagName),
					ShouldUpdate:   !goquTag.Contains(skipUpdateTagName),
					DefaultIfEmpty: goquTag.Contains(defaultIfEmptyTagName),
					FieldIndex:     append(fieldIndex, f.Index...),
					GoType:         f.Type,
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

func (cm ColumnMap) Cols() []string {
	var structCols []string
	for key := range cm {
		structCols = append(structCols, key)
	}
	sort.Strings(structCols)
	return structCols
}
