package duckdb

/*
#include <duckdb.h>
*/
import "C"

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"math/big"
	"time"
	"unsafe"
)

type stmt struct {
	c                *conn
	stmt             *C.duckdb_prepared_statement
	closeOnRowsClose bool
	closed           bool
	rows             bool
}

func (s *stmt) Close() error {
	if s.rows {
		panic("database/sql/driver: misuse of duckdb driver: Close with active Rows")
	}
	if s.closed {
		panic("database/sql/driver: misuse of duckdb driver: double Close of Stmt")
	}

	s.closed = true
	C.duckdb_destroy_prepare(s.stmt)
	return nil
}

func (s *stmt) NumInput() int {
	if s.closed {
		panic("database/sql/driver: misuse of duckdb driver: NumInput after Close")
	}
	paramCount := C.duckdb_nparams(*s.stmt)
	return int(paramCount)
}

func (s *stmt) bind(args []driver.NamedValue) error {
	if s.NumInput() > len(args) {
		return fmt.Errorf("incorrect argument count for command: have %d want %d", len(args), s.NumInput())
	}

	// FIXME (feature): we can't pass nested types as parameters (bind_value) yet

	// relaxed length check allow for unused parameters.
	for i := 0; i < s.NumInput(); i++ {
		name := C.duckdb_parameter_name(*s.stmt, C.idx_t(i+1))
		paramName := C.GoString(name)
		C.duckdb_free(unsafe.Pointer(name))

		// fallback on index position
		arg := args[i]

		// override with ordinal if set
		for _, v := range args {
			if v.Ordinal == i+1 {
				arg = v
			}
		}

		// override with name if set
		for _, v := range args {
			if v.Name == paramName {
				arg = v
			}
		}

		switch v := arg.Value.(type) {
		case bool:
			if rv := C.duckdb_bind_boolean(*s.stmt, C.idx_t(i+1), C.bool(v)); rv == C.DuckDBError {
				return errCouldNotBind
			}
		case int8:
			if rv := C.duckdb_bind_int8(*s.stmt, C.idx_t(i+1), C.int8_t(v)); rv == C.DuckDBError {
				return errCouldNotBind
			}
		case int16:
			if rv := C.duckdb_bind_int16(*s.stmt, C.idx_t(i+1), C.int16_t(v)); rv == C.DuckDBError {
				return errCouldNotBind
			}
		case int32:
			if rv := C.duckdb_bind_int32(*s.stmt, C.idx_t(i+1), C.int32_t(v)); rv == C.DuckDBError {
				return errCouldNotBind
			}
		case int64:
			if rv := C.duckdb_bind_int64(*s.stmt, C.idx_t(i+1), C.int64_t(v)); rv == C.DuckDBError {
				return errCouldNotBind
			}
		case int:
			if rv := C.duckdb_bind_int64(*s.stmt, C.idx_t(i+1), C.int64_t(v)); rv == C.DuckDBError {
				return errCouldNotBind
			}
		case *big.Int:
			val, err := hugeIntFromNative(v)
			if err != nil {
				return err
			}
			if rv := C.duckdb_bind_hugeint(*s.stmt, C.idx_t(i+1), val); rv == C.DuckDBError {
				return errCouldNotBind
			}
		case uint8:
			if rv := C.duckdb_bind_uint8(*s.stmt, C.idx_t(i+1), C.uchar(v)); rv == C.DuckDBError {
				return errCouldNotBind
			}
		case uint16:
			if rv := C.duckdb_bind_uint16(*s.stmt, C.idx_t(i+1), C.uint16_t(v)); rv == C.DuckDBError {
				return errCouldNotBind
			}
		case uint32:
			if rv := C.duckdb_bind_uint32(*s.stmt, C.idx_t(i+1), C.uint32_t(v)); rv == C.DuckDBError {
				return errCouldNotBind
			}
		case uint64:
			if rv := C.duckdb_bind_uint64(*s.stmt, C.idx_t(i+1), C.uint64_t(v)); rv == C.DuckDBError {
				return errCouldNotBind
			}
		case float32:
			if rv := C.duckdb_bind_float(*s.stmt, C.idx_t(i+1), C.float(v)); rv == C.DuckDBError {
				return errCouldNotBind
			}
		case float64:
			if rv := C.duckdb_bind_double(*s.stmt, C.idx_t(i+1), C.double(v)); rv == C.DuckDBError {
				return errCouldNotBind
			}
		case string:
			val := C.CString(v)
			if rv := C.duckdb_bind_varchar(*s.stmt, C.idx_t(i+1), val); rv == C.DuckDBError {
				C.duckdb_free(unsafe.Pointer(val))
				return errCouldNotBind
			}
			C.duckdb_free(unsafe.Pointer(val))
		case []byte:
			val := C.CBytes(v)
			l := len(v)
			if rv := C.duckdb_bind_blob(*s.stmt, C.idx_t(i+1), val, C.uint64_t(l)); rv == C.DuckDBError {
				C.duckdb_free(unsafe.Pointer(val))
				return errCouldNotBind
			}
			C.duckdb_free(unsafe.Pointer(val))
		case time.Time:
			val := C.duckdb_timestamp{
				micros: C.int64_t(v.UTC().UnixMicro()),
			}
			if rv := C.duckdb_bind_timestamp(*s.stmt, C.idx_t(i+1), val); rv == C.DuckDBError {
				return errCouldNotBind
			}
		case Interval:
			val := C.duckdb_interval{
				months: C.int32_t(v.Months),
				days:   C.int32_t(v.Days),
				micros: C.int64_t(v.Micros),
			}
			if rv := C.duckdb_bind_interval(*s.stmt, C.idx_t(i+1), val); rv == C.DuckDBError {
				return errCouldNotBind
			}
		case nil:
			if rv := C.duckdb_bind_null(*s.stmt, C.idx_t(i+1)); rv == C.DuckDBError {
				return errCouldNotBind
			}
		default:
			return driver.ErrSkip
		}
	}

	return nil
}

// Deprecated: Use ExecContext instead.
func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), argsToNamedArgs(args))
}

func (s *stmt) ExecContext(ctx context.Context, nargs []driver.NamedValue) (driver.Result, error) {
	res, err := s.execute(ctx, nargs)
	if err != nil {
		return nil, err
	}
	defer C.duckdb_destroy_result(res)

	ra := int64(C.duckdb_value_int64(res, 0, 0))
	return &result{ra}, nil
}

// Deprecated: Use QueryContext instead.
func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), argsToNamedArgs(args))
}

func (s *stmt) QueryContext(ctx context.Context, nargs []driver.NamedValue) (driver.Rows, error) {
	res, err := s.execute(ctx, nargs)
	if err != nil {
		return nil, err
	}
	s.rows = true
	return newRowsWithStmt(*res, s), nil
}

// This method executes the query in steps and checks if context is cancelled before executing each step.
// It uses Pending Result Interface C APIs to achieve this. Reference - https://duckdb.org/docs/api/c/api#pending-result-interface
func (s *stmt) execute(ctx context.Context, args []driver.NamedValue) (*C.duckdb_result, error) {
	if s.closed {
		panic("database/sql/driver: misuse of duckdb driver: ExecContext or QueryContext after Close")
	}
	if s.rows {
		panic("database/sql/driver: misuse of duckdb driver: ExecContext or QueryContext with active Rows")
	}

	if err := s.bind(args); err != nil {
		return nil, err
	}

	var pendingRes C.duckdb_pending_result
	if state := C.duckdb_pending_prepared(*s.stmt, &pendingRes); state == C.DuckDBError {
		dbErr := getDuckDBError(C.GoString(C.duckdb_pending_error(pendingRes)))
		C.duckdb_destroy_pending(&pendingRes)
		return nil, dbErr
	}
	defer C.duckdb_destroy_pending(&pendingRes)

	mainDoneCh := make(chan struct{})
	bgDoneCh := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			C.duckdb_interrupt(s.c.duckdbCon)
			close(bgDoneCh)
			return
		case <-mainDoneCh:
			close(bgDoneCh)
			return
		}
	}()

	var res C.duckdb_result
	state := C.duckdb_execute_pending(pendingRes, &res)
	close(mainDoneCh)
	// also wait for background goroutine to finish
	// sometimes the bg goroutine is not scheduled immediately and by that time if another query is running on this connection
	// it can cancel that query so need to wait for it to finish as well
	<-bgDoneCh
	if state == C.DuckDBError {
		if ctx.Err() != nil {
			C.duckdb_destroy_result(&res)
			return nil, ctx.Err()
		}

		err := getDuckDBError(C.GoString(C.duckdb_result_error(&res)))
		C.duckdb_destroy_result(&res)
		return nil, err
	}

	return &res, nil
}

func argsToNamedArgs(values []driver.Value) []driver.NamedValue {
	args := make([]driver.NamedValue, len(values))
	for n, param := range values {
		args[n].Value = param
		args[n].Ordinal = n + 1
	}
	return args
}

var errCouldNotBind = errors.New("could not bind parameter")
