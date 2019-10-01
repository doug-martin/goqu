package util

import (
	"database/sql"
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

var scannerType = reflect.TypeOf((*sql.Scanner)(nil)).Elem()

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

// GetSliceElementType returns the type for a slices elements.
func GetSliceElementType(val reflect.Value) reflect.Type {
	elemType := val.Type().Elem()
	if elemType.Kind() == reflect.Ptr {
		elemType = elemType.Elem()
	}

	return elemType
}

// AppendSliceElement will append val to slice. Handles slice of pointers and
// not pointers. Val needs to be a pointer.
func AppendSliceElement(slice, val reflect.Value) {
	if slice.Type().Elem().Kind() == reflect.Ptr {
		slice.Set(reflect.Append(slice, val))
	} else {
		slice.Set(reflect.Append(slice, reflect.Indirect(val)))
	}
}

func GetTypeInfo(i interface{}, val reflect.Value) (reflect.Type, reflect.Kind) {
	var t reflect.Type
	valKind := val.Kind()
	if valKind == reflect.Slice {
		if reflect.ValueOf(i).Kind() == reflect.Ptr {
			t = reflect.TypeOf(i).Elem().Elem()
		} else {
			t = reflect.TypeOf(i).Elem()
		}
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		valKind = t.Kind()
	} else {
		t = val.Type()
	}
	return t, valKind
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

func SafeSetFieldByIndex(v reflect.Value, fieldIndex []int, src interface{}) (result reflect.Value) {
	v = reflect.Indirect(v)
	switch len(fieldIndex) {
	case 0:
		return v
	case 1:
		f := v.FieldByIndex(fieldIndex)
		srcVal := reflect.ValueOf(src)
		f.Set(reflect.Indirect(srcVal))
	default:
		f := v.Field(fieldIndex[0])
		switch f.Kind() {
		case reflect.Ptr:
			s := f
			if f.IsNil() || !f.IsValid() {
				s = reflect.New(f.Type().Elem())
				f.Set(s)
			}
			SafeSetFieldByIndex(reflect.Indirect(s), fieldIndex[1:], src)
		case reflect.Struct:
			SafeSetFieldByIndex(f, fieldIndex[1:], src)
		}
	}
	return v
}

type rowData = map[string]interface{}

// AssignStructVals will assign the data from rd to i.
func AssignStructVals(i interface{}, rd rowData, cm ColumnMap) {
	val := reflect.Indirect(reflect.ValueOf(i))

	for name, data := range cm {
		src, ok := rd[name]
		if ok {
			SafeSetFieldByIndex(val, data.FieldIndex, src)
		}
	}
}

func GetColumnMap(i interface{}) (ColumnMap, error) {
	val := reflect.Indirect(reflect.ValueOf(i))
	t, valKind := GetTypeInfo(i, val)
	if valKind != reflect.Struct {
		return nil, errors.New("cannot scan into this type: %v", t) // #nosec
	}

	structMapCacheLock.Lock()
	defer structMapCacheLock.Unlock()
	if _, ok := structMapCache[t]; !ok {
		structMapCache[t] = createColumnMap(t, []int{}, []string{})
	}
	return structMapCache[t], nil
}

func createColumnMap(t reflect.Type, fieldIndex []int, prefixes []string) ColumnMap {
	cm, n := ColumnMap{}, t.NumField()
	var subColMaps []ColumnMap
	for i := 0; i < n; i++ {
		f := t.Field(i)
		if f.Anonymous && (f.Type.Kind() == reflect.Struct || f.Type.Kind() == reflect.Ptr) {
			goquTag := tag.New("db", f.Tag)
			if !goquTag.Contains("-") {
				subFieldIndexes := append(fieldIndex, f.Index...)
				subPrefixes := append(prefixes, goquTag.Values()...)
				if f.Type.Kind() == reflect.Ptr {
					subColMaps = append(subColMaps, createColumnMap(f.Type.Elem(), subFieldIndexes, subPrefixes))
				} else {
					subColMaps = append(subColMaps, createColumnMap(f.Type, subFieldIndexes, subPrefixes))
				}
			}
		} else if f.PkgPath == "" {
			dbTag := tag.New("db", f.Tag)
			// if PkgPath is empty then it is an exported field
			var columnName string
			if dbTag.IsEmpty() {
				columnName = columnRenameFunction(f.Name)
			} else {
				columnName = dbTag.Values()[0]
			}
			if !dbTag.Equals("-") {
				if !implementsScanner(f.Type) {
					subFieldIndexes := append(fieldIndex, f.Index...)
					subPrefixes := append(prefixes, columnName)
					var subCm ColumnMap
					if f.Type.Kind() == reflect.Ptr {
						subCm = createColumnMap(f.Type.Elem(), subFieldIndexes, subPrefixes)
					} else {
						subCm = createColumnMap(f.Type, subFieldIndexes, subPrefixes)
					}
					if len(subCm) != 0 {
						subColMaps = append(subColMaps, subCm)
						continue
					}
				}
				goquTag := tag.New("goqu", f.Tag)
				columnName = strings.Join(append(prefixes, columnName), ".")
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

func implementsScanner(t reflect.Type) bool {
	if IsPointer(t.Kind()) {
		t = t.Elem()
	}
	if reflect.PtrTo(t).Implements(scannerType) {
		return true
	}
	if !IsStruct(t.Kind()) {
		return true
	}

	return false
}
