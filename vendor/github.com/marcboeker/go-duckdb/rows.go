package duckdb

/*
#include <duckdb.h>
*/
import "C"

import (
	"database/sql/driver"
	"fmt"
	"io"
	"math/big"
	"reflect"
	"strings"
	"time"
	"unsafe"
)

// rows is a helper struct for scanning a duckdb result.
type rows struct {
	// stmt is a pointer to the stmt of which we are scanning the result.
	stmt *stmt
	// res is the result of stmt.
	res C.duckdb_result
	// chunk holds the currently active data chunk.
	chunk DataChunk
	// chunkCount is the number of chunks in the result.
	chunkCount C.idx_t
	// chunkIdx is the chunk index in the result.
	chunkIdx C.idx_t
	// rowCount is the number of scanned rows.
	rowCount int
}

func newRowsWithStmt(res C.duckdb_result, stmt *stmt) *rows {
	columnCount := C.duckdb_column_count(&res)
	r := rows{
		res:        res,
		stmt:       stmt,
		chunk:      DataChunk{},
		chunkCount: C.duckdb_result_chunk_count(res),
		chunkIdx:   0,
		rowCount:   0,
	}

	for i := C.idx_t(0); i < columnCount; i++ {
		columnName := C.GoString(C.duckdb_column_name(&res, i))
		r.chunk.columnNames = append(r.chunk.columnNames, columnName)
	}
	return &r
}

func (r *rows) Columns() []string {
	return r.chunk.columnNames
}

func (r *rows) Next(dst []driver.Value) error {
	for r.rowCount == r.chunk.size {
		r.chunk.close()
		if r.chunkIdx == r.chunkCount {
			return io.EOF
		}
		data := C.duckdb_result_get_chunk(r.res, r.chunkIdx)
		if err := r.chunk.initFromDuckDataChunk(data, false); err != nil {
			return getError(err, nil)
		}

		r.chunkIdx++
		r.rowCount = 0
	}

	columnCount := len(r.chunk.columns)
	for colIdx := 0; colIdx < columnCount; colIdx++ {
		var err error
		if dst[colIdx], err = r.chunk.GetValue(colIdx, r.rowCount); err != nil {
			return err
		}
	}

	r.rowCount++
	return nil
}

// ColumnTypeScanType implements driver.RowsColumnTypeScanType.
func (r *rows) ColumnTypeScanType(index int) reflect.Type {
	t := Type(C.duckdb_column_type(&r.res, C.idx_t(index)))
	switch t {
	case TYPE_INVALID:
		return nil
	case TYPE_BOOLEAN:
		return reflect.TypeOf(true)
	case TYPE_TINYINT:
		return reflect.TypeOf(int8(0))
	case TYPE_SMALLINT:
		return reflect.TypeOf(int16(0))
	case TYPE_INTEGER:
		return reflect.TypeOf(int32(0))
	case TYPE_BIGINT:
		return reflect.TypeOf(int64(0))
	case TYPE_UTINYINT:
		return reflect.TypeOf(uint8(0))
	case TYPE_USMALLINT:
		return reflect.TypeOf(uint16(0))
	case TYPE_UINTEGER:
		return reflect.TypeOf(uint32(0))
	case TYPE_UBIGINT:
		return reflect.TypeOf(uint64(0))
	case TYPE_FLOAT:
		return reflect.TypeOf(float32(0))
	case TYPE_DOUBLE:
		return reflect.TypeOf(float64(0))
	case TYPE_TIMESTAMP, TYPE_TIMESTAMP_S, TYPE_TIMESTAMP_MS, TYPE_TIMESTAMP_NS, TYPE_DATE, TYPE_TIME, TYPE_TIMESTAMP_TZ:
		return reflect.TypeOf(time.Time{})
	case TYPE_INTERVAL:
		return reflect.TypeOf(Interval{})
	case TYPE_HUGEINT:
		return reflect.TypeOf(big.NewInt(0))
	case TYPE_VARCHAR, TYPE_ENUM:
		return reflect.TypeOf("")
	case TYPE_BLOB:
		return reflect.TypeOf([]byte{})
	case TYPE_DECIMAL:
		return reflect.TypeOf(Decimal{})
	case TYPE_LIST:
		return reflect.TypeOf([]any{})
	case TYPE_STRUCT:
		return reflect.TypeOf(map[string]any{})
	case TYPE_MAP:
		return reflect.TypeOf(Map{})
	case TYPE_UUID:
		return reflect.TypeOf([]byte{})
	default:
		return nil
	}
}

// ColumnTypeDatabaseTypeName implements driver.RowsColumnTypeScanType.
func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	t := Type(C.duckdb_column_type(&r.res, C.idx_t(index)))
	switch t {
	case TYPE_DECIMAL, TYPE_ENUM, TYPE_LIST, TYPE_STRUCT, TYPE_MAP:
		// Only allocate the logical type if necessary.
		logicalType := C.duckdb_column_logical_type(&r.res, C.idx_t(index))
		defer C.duckdb_destroy_logical_type(&logicalType)
		return logicalTypeName(logicalType)
	default:
		return typeToStringMap[t]
	}
}

func (r *rows) Close() error {
	r.chunk.close()
	C.duckdb_destroy_result(&r.res)

	var err error
	if r.stmt != nil {
		r.stmt.rows = false
		if r.stmt.closeOnRowsClose {
			err = r.stmt.Close()
		}
		r.stmt = nil
	}
	return err
}

func logicalTypeName(logicalType C.duckdb_logical_type) string {
	t := Type(C.duckdb_get_type_id(logicalType))
	switch t {
	case TYPE_DECIMAL:
		width := C.duckdb_decimal_width(logicalType)
		scale := C.duckdb_decimal_scale(logicalType)
		return fmt.Sprintf("DECIMAL(%d,%d)", width, scale)
	case TYPE_ENUM:
		// The C API does not expose ENUM names.
		return "ENUM"
	case TYPE_LIST:
		childType := C.duckdb_list_type_child_type(logicalType)
		defer C.duckdb_destroy_logical_type(&childType)
		return logicalTypeName(childType) + "[]"
	case TYPE_STRUCT:
		return logicalTypeNameStruct(logicalType)
	case TYPE_MAP:
		return logicalTypeNameMap(logicalType)
	default:
		return typeToStringMap[t]
	}
}

func logicalTypeNameStruct(logicalType C.duckdb_logical_type) string {
	count := int(C.duckdb_struct_type_child_count(logicalType))
	name := "STRUCT("

	for i := 0; i < count; i++ {
		ptrToChildName := C.duckdb_struct_type_child_name(logicalType, C.idx_t(i))
		childName := C.GoString(ptrToChildName)
		childType := C.duckdb_struct_type_child_type(logicalType, C.idx_t(i))

		// Add comma if not at the end of the list.
		name += escapeStructFieldName(childName) + " " + logicalTypeName(childType)
		if i != count-1 {
			name += ", "
		}

		C.duckdb_free(unsafe.Pointer(ptrToChildName))
		C.duckdb_destroy_logical_type(&childType)
	}
	return name + ")"
}

func logicalTypeNameMap(logicalType C.duckdb_logical_type) string {
	keyType := C.duckdb_map_type_key_type(logicalType)
	defer C.duckdb_destroy_logical_type(&keyType)

	valueType := C.duckdb_map_type_value_type(logicalType)
	defer C.duckdb_destroy_logical_type(&valueType)

	return fmt.Sprintf("MAP(%s, %s)", logicalTypeName(keyType), logicalTypeName(valueType))
}

func escapeStructFieldName(s string) string {
	// DuckDB escapes STRUCT field names by doubling double quotes, then wrapping in double quotes.
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
