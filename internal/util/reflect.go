package util

import (
	"database/sql"
	"reflect"
	"strings"
	"sync"

	"github.com/slessard/goqu/v9/internal/errors"
)

const (
	skipUpdateTagName     = "skipupdate"
	skipInsertTagName     = "skipinsert"
	defaultIfEmptyTagName = "defaultifempty"
	omitNilTagName        = "omitnil"
	omitEmptyTagName      = "omitempty"
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

func IsNil(v reflect.Value) bool {
	if !v.IsValid() {
		return true
	}
	switch v.Kind() {
	case reflect.Ptr, reflect.Interface, reflect.Slice, reflect.Map, reflect.Chan, reflect.Func:
		return v.IsNil()
	default:
		return false
	}
}

func IsEmptyValue(v reflect.Value) bool {
	return !v.IsValid() || v.IsZero()
}

var (
	structMapCache     = make(map[interface{}]ColumnMap)
	structMapCacheLock = sync.Mutex{}
)

var (
	DefaultColumnRenameFunction = strings.ToLower
	columnRenameFunction        = DefaultColumnRenameFunction
	ignoreUntaggedFields        = false
)

func SetIgnoreUntaggedFields(ignore bool) {
	// If the value here is changing, reset the struct map cache
	if ignore != ignoreUntaggedFields {
		ignoreUntaggedFields = ignore

		structMapCacheLock.Lock()
		defer structMapCacheLock.Unlock()

		structMapCache = make(map[interface{}]ColumnMap)
	}
}

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
		default: // use the original value
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
		structMapCache[t] = newColumnMap(t, []int{}, []string{})
	}
	return structMapCache[t], nil
}
