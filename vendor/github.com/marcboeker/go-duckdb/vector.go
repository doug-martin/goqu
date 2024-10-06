package duckdb

/*
#include <duckdb.h>
*/
import "C"

import (
	"reflect"
	"unsafe"
)

// vector storage of a DuckDB column.
type vector struct {
	// The underlying DuckDB vector.
	duckdbVector C.duckdb_vector
	// The underlying data ptr.
	ptr unsafe.Pointer
	// The vector's validity mask.
	mask *C.uint64_t
	// A callback function to get a value from this vector.
	getFn fnGetVectorValue
	// A callback function to write to this vector.
	setFn fnSetVectorValue
	// The child vectors of nested data types.
	childVectors []vector

	// The vector's type information.
	vectorTypeInfo
}

func (*vector) canNil(val reflect.Value) bool {
	switch val.Kind() {
	case reflect.Chan, reflect.Func, reflect.Map, reflect.Pointer,
		reflect.UnsafePointer, reflect.Interface, reflect.Slice:
		return true
	default:
		return false
	}
}

func (vec *vector) init(logicalType C.duckdb_logical_type, colIdx int) error {
	t := Type(C.duckdb_get_type_id(logicalType))
	name, inMap := unsupportedTypeToStringMap[t]
	if inMap {
		return addIndexToError(unsupportedTypeError(name), colIdx)
	}

	switch t {
	case TYPE_BOOLEAN:
		initBool(vec)
	case TYPE_TINYINT:
		initNumeric[int8](vec, t)
	case TYPE_SMALLINT:
		initNumeric[int16](vec, t)
	case TYPE_INTEGER:
		initNumeric[int32](vec, t)
	case TYPE_BIGINT:
		initNumeric[int64](vec, t)
	case TYPE_UTINYINT:
		initNumeric[uint8](vec, t)
	case TYPE_USMALLINT:
		initNumeric[uint16](vec, t)
	case TYPE_UINTEGER:
		initNumeric[uint32](vec, t)
	case TYPE_UBIGINT:
		initNumeric[uint64](vec, t)
	case TYPE_FLOAT:
		initNumeric[float32](vec, t)
	case TYPE_DOUBLE:
		initNumeric[float64](vec, t)
	case TYPE_TIMESTAMP, TYPE_TIMESTAMP_S, TYPE_TIMESTAMP_MS, TYPE_TIMESTAMP_NS, TYPE_TIMESTAMP_TZ:
		vec.initTS(t)
	case TYPE_DATE:
		vec.initDate()
	case TYPE_TIME:
		vec.initTime()
	case TYPE_INTERVAL:
		vec.initInterval()
	case TYPE_HUGEINT:
		vec.initHugeint()
	case TYPE_VARCHAR, TYPE_BLOB:
		vec.initBytes(t)
	case TYPE_DECIMAL:
		return vec.initDecimal(logicalType, colIdx)
	case TYPE_ENUM:
		return vec.initEnum(logicalType, colIdx)
	case TYPE_LIST:
		return vec.initList(logicalType, colIdx)
	case TYPE_STRUCT:
		return vec.initStruct(logicalType, colIdx)
	case TYPE_MAP:
		return vec.initMap(logicalType, colIdx)
	case TYPE_UUID:
		vec.initUUID()
	case TYPE_SQLNULL:
		vec.initSQLNull()
	default:
		return addIndexToError(unsupportedTypeError(unknownTypeErrMsg), colIdx)
	}
	return nil
}

func (vec *vector) resizeListVector(newLength C.idx_t) {
	C.duckdb_list_vector_reserve(vec.duckdbVector, newLength)
	C.duckdb_list_vector_set_size(vec.duckdbVector, newLength)
	vec.resetChildData()
}

func (vec *vector) resetChildData() {
	for i := range vec.childVectors {
		vec.childVectors[i].ptr = C.duckdb_vector_get_data(vec.childVectors[i].duckdbVector)
		vec.childVectors[i].resetChildData()
	}
}

func (vec *vector) initVectors(v C.duckdb_vector, writable bool) {
	vec.duckdbVector = v
	vec.ptr = C.duckdb_vector_get_data(v)
	if writable {
		C.duckdb_vector_ensure_validity_writable(v)
	}
	vec.mask = C.duckdb_vector_get_validity(v)
	vec.getChildVectors(v, writable)
}

func (vec *vector) getChildVectors(v C.duckdb_vector, writable bool) {
	switch vec.Type {
	case TYPE_LIST, TYPE_MAP:
		child := C.duckdb_list_vector_get_child(v)
		vec.childVectors[0].initVectors(child, writable)
	case TYPE_STRUCT:
		for i := 0; i < len(vec.childVectors); i++ {
			child := C.duckdb_struct_vector_get_child(v, C.idx_t(i))
			vec.childVectors[i].initVectors(child, writable)
		}
	}
}

func initBool(vec *vector) {
	vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return getPrimitive[bool](vec, rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) error {
		if val == nil {
			vec.setNull(rowIdx)
			return nil
		}
		return setBool(vec, rowIdx, val)
	}
	vec.Type = TYPE_BOOLEAN
}

func initNumeric[T numericType](vec *vector, t Type) {
	vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return getPrimitive[T](vec, rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) error {
		if val == nil {
			vec.setNull(rowIdx)
			return nil
		}
		return setNumeric[any, T](vec, rowIdx, val)
	}
	vec.Type = t
}

func (vec *vector) initTS(t Type) {
	vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return vec.getTS(t, rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) error {
		if val == nil {
			vec.setNull(rowIdx)
			return nil
		}
		return setTS(vec, t, rowIdx, val)
	}
	vec.Type = t
}

func (vec *vector) initDate() {
	vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return vec.getDate(rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) error {
		if val == nil {
			vec.setNull(rowIdx)
			return nil
		}
		return setDate(vec, rowIdx, val)
	}
	vec.Type = TYPE_DATE
}

func (vec *vector) initTime() {
	vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return vec.getTime(rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) error {
		if val == nil {
			vec.setNull(rowIdx)
			return nil
		}
		return setTime(vec, rowIdx, val)
	}
	vec.Type = TYPE_TIME
}

func (vec *vector) initInterval() {
	vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return vec.getInterval(rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) error {
		if val == nil {
			vec.setNull(rowIdx)
			return nil
		}
		return setInterval(vec, rowIdx, val)
	}
	vec.Type = TYPE_INTERVAL
}

func (vec *vector) initHugeint() {
	vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return vec.getHugeint(rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) error {
		if val == nil {
			vec.setNull(rowIdx)
			return nil
		}
		return setHugeint(vec, rowIdx, val)
	}
	vec.Type = TYPE_HUGEINT
}

func (vec *vector) initBytes(t Type) {
	vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return vec.getCString(rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) error {
		if val == nil {
			vec.setNull(rowIdx)
			return nil
		}
		return setBytes(vec, rowIdx, val)
	}
	vec.Type = t
}

func (vec *vector) initDecimal(logicalType C.duckdb_logical_type, colIdx int) error {
	vec.decimalWidth = uint8(C.duckdb_decimal_width(logicalType))
	vec.decimalScale = uint8(C.duckdb_decimal_scale(logicalType))

	t := Type(C.duckdb_decimal_internal_type(logicalType))
	switch t {
	case TYPE_SMALLINT, TYPE_INTEGER, TYPE_BIGINT, TYPE_HUGEINT:
		vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
			if vec.getNull(rowIdx) {
				return nil
			}
			return vec.getDecimal(rowIdx)
		}
		vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) error {
			if val == nil {
				vec.setNull(rowIdx)
				return nil
			}
			return setDecimal(vec, rowIdx, val)
		}
	default:
		return addIndexToError(unsupportedTypeError(typeToStringMap[t]), colIdx)
	}

	vec.Type = TYPE_DECIMAL
	vec.internalType = t
	return nil
}

func (vec *vector) initEnum(logicalType C.duckdb_logical_type, colIdx int) error {
	// Initialize the dictionary.
	dictSize := uint32(C.duckdb_enum_dictionary_size(logicalType))
	vec.dict = make(map[string]uint32)
	for i := uint32(0); i < dictSize; i++ {
		cStr := C.duckdb_enum_dictionary_value(logicalType, C.idx_t(i))
		str := C.GoString(cStr)
		vec.dict[str] = i
		C.duckdb_free(unsafe.Pointer(cStr))
	}

	t := Type(C.duckdb_enum_internal_type(logicalType))
	switch t {
	case TYPE_UTINYINT, TYPE_USMALLINT, TYPE_UINTEGER, TYPE_UBIGINT:
		vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
			if vec.getNull(rowIdx) {
				return nil
			}
			return vec.getEnum(rowIdx)
		}
		vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) error {
			if val == nil {
				vec.setNull(rowIdx)
				return nil
			}
			return setEnum(vec, rowIdx, val)
		}
	default:
		return addIndexToError(unsupportedTypeError(typeToStringMap[t]), colIdx)
	}

	vec.Type = TYPE_ENUM
	vec.internalType = t
	return nil
}

func (vec *vector) initList(logicalType C.duckdb_logical_type, colIdx int) error {
	// Get the child vector type.
	childType := C.duckdb_list_type_child_type(logicalType)
	defer C.duckdb_destroy_logical_type(&childType)

	// Recurse into the child.
	vec.childVectors = make([]vector, 1)
	err := vec.childVectors[0].init(childType, colIdx)
	if err != nil {
		return err
	}

	vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return vec.getList(rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) error {
		if val == nil {
			vec.setNull(rowIdx)
			return nil
		}
		return setList(vec, rowIdx, val)
	}
	vec.Type = TYPE_LIST
	return nil
}

func (vec *vector) initStruct(logicalType C.duckdb_logical_type, colIdx int) error {
	childCount := int(C.duckdb_struct_type_child_count(logicalType))
	var structEntries []StructEntry
	for i := 0; i < childCount; i++ {
		name := C.duckdb_struct_type_child_name(logicalType, C.idx_t(i))
		entry, err := NewStructEntry(nil, C.GoString(name))
		structEntries = append(structEntries, entry)
		C.duckdb_free(unsafe.Pointer(name))
		if err != nil {
			return err
		}
	}

	vec.childVectors = make([]vector, childCount)
	vec.structEntries = structEntries

	// Recurse into the children.
	for i := 0; i < childCount; i++ {
		childType := C.duckdb_struct_type_child_type(logicalType, C.idx_t(i))
		err := vec.childVectors[i].init(childType, colIdx)
		C.duckdb_destroy_logical_type(&childType)

		if err != nil {
			return err
		}
	}

	vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return vec.getStruct(rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) error {
		if val == nil {
			vec.setNull(rowIdx)
			return nil
		}
		return setStruct(vec, rowIdx, val)
	}
	vec.Type = TYPE_STRUCT
	return nil
}

func (vec *vector) initMap(logicalType C.duckdb_logical_type, colIdx int) error {
	// A MAP is a LIST of STRUCT values. Each STRUCT holds two children: a key and a value.

	// Get the child vector type.
	childType := C.duckdb_list_type_child_type(logicalType)
	defer C.duckdb_destroy_logical_type(&childType)

	// Recurse into the child.
	vec.childVectors = make([]vector, 1)
	err := vec.childVectors[0].init(childType, colIdx)
	if err != nil {
		return err
	}

	// DuckDB supports more MAP key types than Go, which only supports comparable types.
	// We ensure that the key type itself is comparable.
	keyType := C.duckdb_map_type_key_type(logicalType)
	defer C.duckdb_destroy_logical_type(&keyType)

	t := Type(C.duckdb_get_type_id(keyType))
	switch t {
	case TYPE_LIST, TYPE_STRUCT, TYPE_MAP, TYPE_ARRAY:
		return addIndexToError(errUnsupportedMapKeyType, colIdx)
	}

	vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		return vec.getMap(rowIdx)
	}
	vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) error {
		if val == nil {
			vec.setNull(rowIdx)
			return nil
		}
		return setMap(vec, rowIdx, val)
	}
	vec.Type = TYPE_MAP
	return nil
}

func (vec *vector) initUUID() {
	vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
		if vec.getNull(rowIdx) {
			return nil
		}
		hugeInt := getPrimitive[C.duckdb_hugeint](vec, rowIdx)
		return hugeIntToUUID(hugeInt)
	}
	vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) error {
		if val == nil {
			vec.setNull(rowIdx)
			return nil
		}
		return setUUID(vec, rowIdx, val)
	}
	vec.Type = TYPE_UUID
}

func (vec *vector) initSQLNull() {
	vec.getFn = func(vec *vector, rowIdx C.idx_t) any {
		return nil
	}
	vec.setFn = func(vec *vector, rowIdx C.idx_t, val any) error {
		return errSetSQLNULLValue
	}
	vec.Type = TYPE_SQLNULL
}
