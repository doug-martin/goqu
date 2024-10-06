package duckdb

/*
#include <duckdb.h>
*/
import "C"

import (
	"database/sql/driver"
	"errors"
	"unsafe"
)

// Appender holds the DuckDB appender. It allows efficient bulk loading into a DuckDB database.
type Appender struct {
	con            *conn
	schema         string
	table          string
	duckdbAppender C.duckdb_appender
	closed         bool

	// The appender storage before flushing any data.
	chunks []DataChunk
	// The column types of the table to append to.
	types []C.duckdb_logical_type
	// A pointer to the allocated memory of the column types.
	ptr unsafe.Pointer
	// The number of appended rows.
	rowCount int
}

// NewAppenderFromConn returns a new Appender from a DuckDB driver connection.
func NewAppenderFromConn(driverConn driver.Conn, schema, table string) (*Appender, error) {
	con, ok := driverConn.(*conn)
	if !ok {
		return nil, getError(errInvalidCon, nil)
	}
	if con.closed {
		return nil, getError(errClosedCon, nil)
	}

	var cSchema *C.char
	if schema != "" {
		cSchema = C.CString(schema)
		defer C.duckdb_free(unsafe.Pointer(cSchema))
	}

	cTable := C.CString(table)
	defer C.duckdb_free(unsafe.Pointer(cTable))

	var duckdbAppender C.duckdb_appender
	state := C.duckdb_appender_create(con.duckdbCon, cSchema, cTable, &duckdbAppender)

	if state == C.DuckDBError {
		// We destroy the error message when destroying the appender.
		err := duckdbError(C.duckdb_appender_error(duckdbAppender))
		C.duckdb_appender_destroy(&duckdbAppender)
		return nil, getError(errAppenderCreation, err)
	}

	a := &Appender{
		con:            con,
		schema:         schema,
		table:          table,
		duckdbAppender: duckdbAppender,
		rowCount:       0,
	}

	// Get the column types.
	columnCount := int(C.duckdb_appender_column_count(duckdbAppender))
	a.ptr, a.types = mallocTypeSlice(columnCount)
	for i := 0; i < columnCount; i++ {
		a.types[i] = C.duckdb_appender_column_type(duckdbAppender, C.idx_t(i))

		// Ensure that we only create an appender for supported column types.
		t := Type(C.duckdb_get_type_id(a.types[i]))
		name, found := unsupportedTypeToStringMap[t]
		if found {
			err := addIndexToError(unsupportedTypeError(name), i+1)
			destroyTypeSlice(a.ptr, a.types)
			C.duckdb_appender_destroy(&duckdbAppender)
			return nil, getError(errAppenderCreation, err)
		}
	}

	return a, nil
}

// Flush the data chunks to the underlying table and clear the internal cache.
// Does not close the appender, even if it returns an error. Unless you have a good reason to call this,
// call Close when you are done with the appender.
func (a *Appender) Flush() error {
	if err := a.appendDataChunks(); err != nil {
		return getError(errAppenderFlush, invalidatedAppenderError(err))
	}

	state := C.duckdb_appender_flush(a.duckdbAppender)
	if state == C.DuckDBError {
		err := duckdbError(C.duckdb_appender_error(a.duckdbAppender))
		return getError(errAppenderFlush, invalidatedAppenderError(err))
	}
	return nil
}

// Close the appender. This will flush the appender to the underlying table.
// It is vital to call this when you are done with the appender to avoid leaking memory.
func (a *Appender) Close() error {
	if a.closed {
		return getError(errAppenderDoubleClose, nil)
	}
	a.closed = true

	// Append all remaining chunks.
	errAppend := a.appendDataChunks()

	// We flush before closing to get a meaningful error message.
	var errFlush error
	state := C.duckdb_appender_flush(a.duckdbAppender)
	if state == C.DuckDBError {
		errFlush = duckdbError(C.duckdb_appender_error(a.duckdbAppender))
	}

	// Destroy all appender data and the appender.
	destroyTypeSlice(a.ptr, a.types)
	var errClose error
	state = C.duckdb_appender_destroy(&a.duckdbAppender)
	if state == C.DuckDBError {
		errClose = errAppenderClose
	}

	err := errors.Join(errAppend, errFlush, errClose)
	if err != nil {
		return getError(invalidatedAppenderError(err), nil)
	}
	return nil
}

// AppendRow loads a row of values into the appender. The values are provided as separate arguments.
func (a *Appender) AppendRow(args ...driver.Value) error {
	if a.closed {
		return getError(errAppenderAppendAfterClose, nil)
	}

	err := a.appendRowSlice(args)
	if err != nil {
		return getError(errAppenderAppendRow, err)
	}
	return nil
}

func (a *Appender) addDataChunk() error {
	var chunk DataChunk
	if err := chunk.initFromTypes(a.ptr, a.types, true); err != nil {
		return err
	}
	a.chunks = append(a.chunks, chunk)
	return nil
}

func (a *Appender) appendRowSlice(args []driver.Value) error {
	// Early-out, if the number of args does not match the column count.
	if len(args) != len(a.types) {
		return columnCountError(len(args), len(a.types))
	}

	// Create a new data chunk if the current chunk is full.
	if a.rowCount == GetDataChunkCapacity() || len(a.chunks) == 0 {
		if err := a.addDataChunk(); err != nil {
			return err
		}
		a.rowCount = 0
	}

	// Set all values.
	for i, val := range args {
		chunk := &a.chunks[len(a.chunks)-1]
		err := chunk.SetValue(i, a.rowCount, val)
		if err != nil {
			return err
		}
	}

	a.rowCount++
	return nil
}

func (a *Appender) appendDataChunks() error {
	var state C.duckdb_state
	var err error

	for i, chunk := range a.chunks {
		// All data chunks except the last are at maximum capacity.
		size := GetDataChunkCapacity()
		if i == len(a.chunks)-1 {
			size = a.rowCount
		}
		if err = chunk.SetSize(size); err != nil {
			break
		}

		state = C.duckdb_append_data_chunk(a.duckdbAppender, chunk.data)
		if state == C.DuckDBError {
			err = duckdbError(C.duckdb_appender_error(a.duckdbAppender))
			break
		}
	}

	for _, chunk := range a.chunks {
		chunk.close()
	}

	a.chunks = a.chunks[:0]
	a.rowCount = 0
	return err
}

func mallocTypeSlice(count int) (unsafe.Pointer, []C.duckdb_logical_type) {
	var dummy C.duckdb_logical_type
	size := C.size_t(unsafe.Sizeof(dummy))

	ptr := unsafe.Pointer(C.malloc(C.size_t(count) * size))
	slice := (*[1 << 30]C.duckdb_logical_type)(ptr)[:count:count]

	return ptr, slice
}

func destroyTypeSlice(ptr unsafe.Pointer, slice []C.duckdb_logical_type) {
	for _, t := range slice {
		C.duckdb_destroy_logical_type(&t)
	}
	C.duckdb_free(ptr)
}
