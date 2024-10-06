package duckdb

/*
#include <duckdb.h>
*/
import "C"

import (
	"runtime"
	"runtime/cgo"
	"unsafe"
)

type pinnedValue[T any] struct {
	pinner *runtime.Pinner
	value  T
}

type unpinner interface {
	unpin()
}

func (v pinnedValue[T]) unpin() {
	v.pinner.Unpin()
}

func setFuncError(function_info C.duckdb_function_info, msg string) {
	err := C.CString(msg)
	C.duckdb_scalar_function_set_error(function_info, err)
	C.duckdb_free(unsafe.Pointer(err))
}

func udf_delete_callback(info unsafe.Pointer) {
	h := (*cgo.Handle)(info)
	h.Value().(unpinner).unpin()
	h.Delete()
}
