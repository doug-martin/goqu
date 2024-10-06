package duckdb

/*
#include <duckdb.h>

void scalar_udf_callback(duckdb_function_info, duckdb_data_chunk, duckdb_vector);
void scalar_udf_delete_callback(void *);

typedef void (*scalar_udf_callback_t)(duckdb_function_info, duckdb_data_chunk, duckdb_vector);
*/
import "C"

import (
	"database/sql"
	"database/sql/driver"
	"runtime"
	"runtime/cgo"
	"unsafe"
)

// ScalarFuncConfig contains the fields to configure a user-defined scalar function.
type ScalarFuncConfig struct {
	// InputTypeInfos contains Type information for each input parameter of the scalar function.
	InputTypeInfos []TypeInfo
	// ResultTypeInfo holds the Type information of the scalar function's result type.
	ResultTypeInfo TypeInfo

	// VariadicTypeInfo configures the number of input parameters.
	// If this field is nil, then the input parameters match InputTypeInfos.
	// Otherwise, the scalar function's input parameters are set to variadic, allowing any number of input parameters.
	// The Type of the first len(InputTypeInfos) parameters is configured by InputTypeInfos, and all
	// remaining parameters must match the variadic Type. To configure different variadic parameter types,
	// you must set the VariadicTypeInfo's Type to TYPE_ANY.
	VariadicTypeInfo TypeInfo
	// Volatile sets the stability of the scalar function to volatile, if true.
	// Volatile scalar functions might create a different result per row.
	// E.g., random() is a volatile scalar function.
	Volatile bool
	// SpecialNullHandling disables the default NULL handling of scalar functions, if true.
	// The default NULL handling is: NULL in, NULL out. I.e., if any input parameter is NULL, then the result is NULL.
	SpecialNullHandling bool
}

// ScalarFuncExecutor contains the callback function to execute a user-defined scalar function.
// Currently, its only field is a row-based executor.
type ScalarFuncExecutor struct {
	// RowExecutor accepts a row-based execution function.
	// []driver.Value contains the row values, and it returns the row execution result, or error.
	RowExecutor func(values []driver.Value) (any, error)
}

// ScalarFunc is the user-defined scalar function interface.
// Any scalar function must implement a Config function, and an Executor function.
type ScalarFunc interface {
	// Config returns ScalarFuncConfig to configure the scalar function.
	Config() ScalarFuncConfig
	// Executor returns ScalarFuncExecutor to execute the scalar function.
	Executor() ScalarFuncExecutor
}

// RegisterScalarUDF registers a user-defined scalar function.
// *sql.Conn is the SQL connection on which to register the scalar function.
// name is the function name, and f is the scalar function's interface ScalarFunc.
// RegisterScalarUDF takes ownership of f, so you must pass it as a pointer.
func RegisterScalarUDF(c *sql.Conn, name string, f ScalarFunc) error {
	function, err := createScalarFunc(name, f)
	if err != nil {
		return getError(errAPI, err)
	}

	// Register the function on the underlying driver connection exposed by c.Raw.
	err = c.Raw(func(driverConn any) error {
		con := driverConn.(*conn)
		state := C.duckdb_register_scalar_function(con.duckdbCon, function)
		C.duckdb_destroy_scalar_function(&function)
		if state == C.DuckDBError {
			return getError(errAPI, errScalarUDFCreate)
		}
		return nil
	})
	return err
}

// RegisterScalarUDFSet registers a set of user-defined scalar functions with the same name.
// This enables overloading of scalar functions.
// E.g., the function my_length() can have implementations like my_length(LIST(ANY)) and my_length(VARCHAR).
// *sql.Conn is the SQL connection on which to register the scalar function set.
// name is the function name of each function in the set.
// functions contains all ScalarFunc functions of the scalar function set.
func RegisterScalarUDFSet(c *sql.Conn, name string, functions ...ScalarFunc) error {
	cName := C.CString(name)
	set := C.duckdb_create_scalar_function_set(cName)
	C.duckdb_free(unsafe.Pointer(cName))

	// Create each function and add it to the set.
	for i, f := range functions {
		function, err := createScalarFunc(name, f)
		if err != nil {
			C.duckdb_destroy_scalar_function(&function)
			C.duckdb_destroy_scalar_function_set(&set)
			return getError(errAPI, err)
		}

		state := C.duckdb_add_scalar_function_to_set(set, function)
		C.duckdb_destroy_scalar_function(&function)
		if state == C.DuckDBError {
			C.duckdb_destroy_scalar_function_set(&set)
			return getError(errAPI, addIndexToError(errScalarUDFAddToSet, i))
		}
	}

	// Register the function set on the underlying driver connection exposed by c.Raw.
	err := c.Raw(func(driverConn any) error {
		con := driverConn.(*conn)
		state := C.duckdb_register_scalar_function_set(con.duckdbCon, set)
		C.duckdb_destroy_scalar_function_set(&set)
		if state == C.DuckDBError {
			return getError(errAPI, errScalarUDFCreateSet)
		}
		return nil
	})
	return err
}

//export scalar_udf_callback
func scalar_udf_callback(function_info C.duckdb_function_info, input C.duckdb_data_chunk, output C.duckdb_vector) {
	extraInfo := C.duckdb_scalar_function_get_extra_info(function_info)

	// extraInfo is a void* pointer to our pinned ScalarFunc f.
	h := *(*cgo.Handle)(unsafe.Pointer(extraInfo))
	function := h.Value().(pinnedValue[ScalarFunc]).value

	// Initialize the input chunk.
	var inputChunk DataChunk
	if err := inputChunk.initFromDuckDataChunk(input, false); err != nil {
		setFuncError(function_info, getError(errAPI, err).Error())
		return
	}

	// Initialize the output chunk.
	var outputChunk DataChunk
	if err := outputChunk.initFromDuckVector(output, true); err != nil {
		setFuncError(function_info, getError(errAPI, err).Error())
		return
	}

	executor := function.Executor()
	nullInNullOut := !function.Config().SpecialNullHandling
	values := make([]driver.Value, len(inputChunk.columns))
	columnCount := len(values)
	rowCount := inputChunk.GetSize()

	// Execute the user-defined scalar function for each row.
	var err error
	for rowIdx := 0; rowIdx < rowCount; rowIdx++ {
		nullRow := false

		// Get each column value.
		for colIdx := 0; colIdx < columnCount; colIdx++ {
			if values[colIdx], err = inputChunk.GetValue(colIdx, rowIdx); err != nil {
				setFuncError(function_info, getError(errAPI, err).Error())
				return
			}

			// NULL handling.
			if nullInNullOut && values[colIdx] == nil {
				if err = outputChunk.SetValue(0, rowIdx, nil); err != nil {
					setFuncError(function_info, getError(errAPI, err).Error())
					return
				}
				nullRow = true
				break
			}
		}
		if nullRow {
			continue
		}

		// Execute the function.
		var val any
		if val, err = executor.RowExecutor(values); err != nil {
			setFuncError(function_info, getError(errAPI, err).Error())
			return
		}

		// Write the result to the output chunk.
		if err = outputChunk.SetValue(0, rowIdx, val); err != nil {
			setFuncError(function_info, getError(errAPI, err).Error())
			return
		}
	}
}

//export scalar_udf_delete_callback
func scalar_udf_delete_callback(info unsafe.Pointer) {
	udf_delete_callback(info)
}

func registerInputParams(config ScalarFuncConfig, f C.duckdb_scalar_function) error {
	// Set variadic input parameters.
	if config.VariadicTypeInfo != nil {
		t := config.VariadicTypeInfo.logicalType()
		C.duckdb_scalar_function_set_varargs(f, t)
		C.duckdb_destroy_logical_type(&t)
	}

	// Early-out, if the function does not take any (non-variadic) parameters.
	if config.InputTypeInfos == nil {
		return nil
	}
	if len(config.InputTypeInfos) == 0 {
		return nil
	}

	// Set non-variadic input parameters.
	for i, info := range config.InputTypeInfos {
		if info == nil {
			return addIndexToError(errScalarUDFInputTypeIsNil, i)
		}
		t := info.logicalType()
		C.duckdb_scalar_function_add_parameter(f, t)
		C.duckdb_destroy_logical_type(&t)
	}
	return nil
}

func registerResultParams(config ScalarFuncConfig, f C.duckdb_scalar_function) error {
	if config.ResultTypeInfo == nil {
		return errScalarUDFResultTypeIsNil
	}
	if config.ResultTypeInfo.InternalType() == TYPE_ANY {
		return errScalarUDFResultTypeIsANY
	}
	t := config.ResultTypeInfo.logicalType()
	C.duckdb_scalar_function_set_return_type(f, t)
	C.duckdb_destroy_logical_type(&t)
	return nil
}

func createScalarFunc(name string, f ScalarFunc) (C.duckdb_scalar_function, error) {
	if name == "" {
		return nil, errScalarUDFNoName
	}
	if f == nil {
		return nil, errScalarUDFIsNil
	}
	if f.Executor().RowExecutor == nil {
		return nil, errScalarUDFNoExecutor
	}

	function := C.duckdb_create_scalar_function()

	// Set the name.
	cName := C.CString(name)
	C.duckdb_scalar_function_set_name(function, cName)
	C.duckdb_free(unsafe.Pointer(cName))

	// Configure the scalar function.
	config := f.Config()
	if err := registerInputParams(config, function); err != nil {
		return nil, err
	}
	if err := registerResultParams(config, function); err != nil {
		return nil, err
	}
	if config.SpecialNullHandling {
		C.duckdb_scalar_function_set_special_handling(function)
	}
	if config.Volatile {
		C.duckdb_scalar_function_set_volatile(function)
	}

	// Set the function callback.
	C.duckdb_scalar_function_set_function(function, C.scalar_udf_callback_t(C.scalar_udf_callback))

	// Pin the ScalarFunc f.
	value := pinnedValue[ScalarFunc]{
		pinner: &runtime.Pinner{},
		value:  f,
	}
	h := cgo.NewHandle(value)
	value.pinner.Pin(&h)

	// Set the execution data, which is the ScalarFunc f.
	C.duckdb_scalar_function_set_extra_info(
		function,
		unsafe.Pointer(&h),
		C.duckdb_delete_callback_t(C.scalar_udf_delete_callback))

	return function, nil
}
