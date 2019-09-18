package exec

import (
	"context"
	gsql "database/sql"
	"reflect"

	"github.com/doug-martin/goqu/v9/internal/errors"
	"github.com/doug-martin/goqu/v9/internal/util"
)

type (
	QueryExecutor struct {
		de    DbExecutor
		err   error
		query string
		args  []interface{}
	}
)

var (
	errUnsupportedScanStructType  = errors.New("type must be a pointer to a struct when scanning into a struct")
	errUnsupportedScanStructsType = errors.New("type must be a pointer to a slice when scanning into structs")
	errUnsupportedScanValsType    = errors.New("type must be a pointer to a slice when scanning into vals")
	errScanValPointer             = errors.New("type must be a pointer when scanning into val")
	errScanValNonSlice            = errors.New("type cannot be a pointer to a slice when scanning into val")
)

func newQueryExecutor(de DbExecutor, err error, query string, args ...interface{}) QueryExecutor {
	return QueryExecutor{de: de, err: err, query: query, args: args}
}

func (q QueryExecutor) ToSQL() (sql string, args []interface{}, err error) {
	return q.query, q.args, q.err
}

func (q QueryExecutor) Exec() (gsql.Result, error) {
	return q.ExecContext(context.Background())
}

func (q QueryExecutor) ExecContext(ctx context.Context) (gsql.Result, error) {
	if q.err != nil {
		return nil, q.err
	}
	return q.de.ExecContext(ctx, q.query, q.args...)
}

func (q QueryExecutor) Query() (*gsql.Rows, error) {
	return q.QueryContext(context.Background())
}

func (q QueryExecutor) QueryContext(ctx context.Context) (*gsql.Rows, error) {
	if q.err != nil {
		return nil, q.err
	}
	return q.de.QueryContext(ctx, q.query, q.args...)
}

// This will execute the SQL and append results to the slice
//    var myStructs []MyStruct
//    if err := db.From("test").ScanStructs(&myStructs); err != nil{
//        panic(err.Error()
//    }
//    //use your structs
//
//
// i: A pointer to a slice of structs.
func (q QueryExecutor) ScanStructs(i interface{}) error {
	return q.ScanStructsContext(context.Background(), i)
}

// This will execute the SQL and append results to the slice
//    var myStructs []MyStruct
//    if err := db.From("test").ScanStructsContext(ctx, &myStructs); err != nil{
//        panic(err.Error()
//    }
//    //use your structs
//
//
// i: A pointer to a slice of structs.
func (q QueryExecutor) ScanStructsContext(ctx context.Context, i interface{}) error {
	val := reflect.ValueOf(i)
	if !util.IsPointer(val.Kind()) {
		return errUnsupportedScanStructsType
	}
	val = reflect.Indirect(val)
	if !util.IsSlice(val.Kind()) {
		return errUnsupportedScanStructsType
	}
	scanner, err := q.rowsScanner(ctx)
	if err != nil {
		return err
	}
	_, err = scanner.ScanStructs(i)
	return err
}

// This will execute the SQL and fill out the struct with the fields returned.
// This method returns a boolean value that is false if no record was found
//    var myStruct MyStruct
//    found, err := db.From("test").Limit(1).ScanStruct(&myStruct)
//    if err != nil{
//        panic(err.Error()
//    }
//    if !found{
//          fmt.Println("NOT FOUND")
//    }
//
// i: A pointer to a struct
func (q QueryExecutor) ScanStruct(i interface{}) (bool, error) {
	return q.ScanStructContext(context.Background(), i)
}

// This will execute the SQL and fill out the struct with the fields returned.
// This method returns a boolean value that is false if no record was found
//    var myStruct MyStruct
//    found, err := db.From("test").Limit(1).ScanStructContext(ctx, &myStruct)
//    if err != nil{
//        panic(err.Error()
//    }
//    if !found{
//          fmt.Println("NOT FOUND")
//    }
//
// i: A pointer to a struct
func (q QueryExecutor) ScanStructContext(ctx context.Context, i interface{}) (bool, error) {
	val := reflect.ValueOf(i)
	if !util.IsPointer(val.Kind()) {
		return false, errUnsupportedScanStructType
	}
	val = reflect.Indirect(val)
	if !util.IsStruct(val.Kind()) {
		return false, errUnsupportedScanStructType
	}
	scanner, err := q.rowsScanner(ctx)
	if err != nil {
		return false, err
	}
	return scanner.ScanStructs(i)
}

// This will execute the SQL and append results to the slice.
//    var ids []uint32
//    if err := db.From("test").Select("id").ScanVals(&ids); err != nil{
//        panic(err.Error()
//    }
//
// i: Takes a pointer to a slice of primitive values.
func (q QueryExecutor) ScanVals(i interface{}) error {
	return q.ScanValsContext(context.Background(), i)
}

// This will execute the SQL and append results to the slice.
//    var ids []uint32
//    if err := db.From("test").Select("id").ScanValsContext(ctx, &ids); err != nil{
//        panic(err.Error()
//    }
//
// i: Takes a pointer to a slice of primitive values.
func (q QueryExecutor) ScanValsContext(ctx context.Context, i interface{}) error {
	val := reflect.ValueOf(i)
	if !util.IsPointer(val.Kind()) {
		return errUnsupportedScanValsType
	}
	val = reflect.Indirect(val)
	if !util.IsSlice(val.Kind()) {
		return errUnsupportedScanValsType
	}
	scanner, err := q.rowsScanner(ctx)
	if err != nil {
		return err
	}
	_, err = scanner.ScanVals(i)
	return err
}

// This will execute the SQL and set the value of the primitive. This method will return false if no record is found.
//    var id uint32
//    found, err := db.From("test").Select("id").Limit(1).ScanVal(&id)
//    if err != nil{
//        panic(err.Error()
//    }
//    if !found{
//        fmt.Println("NOT FOUND")
//    }
//
//   i: Takes a pointer to a primitive value.
func (q QueryExecutor) ScanVal(i interface{}) (bool, error) {
	return q.ScanValContext(context.Background(), i)
}

// This will execute the SQL and set the value of the primitive. This method will return false if no record is found.
//    var id uint32
//    found, err := db.From("test").Select("id").Limit(1).ScanValContext(ctx, &id)
//    if err != nil{
//        panic(err.Error()
//    }
//    if !found{
//        fmt.Println("NOT FOUND")
//    }
//
//   i: Takes a pointer to a primitive value.
func (q QueryExecutor) ScanValContext(ctx context.Context, i interface{}) (bool, error) {
	val := reflect.ValueOf(i)
	if !util.IsPointer(val.Kind()) {
		return false, errScanValPointer
	}
	val = reflect.Indirect(val)
	if util.IsSlice(val.Kind()) {
		switch i.(type) {
		case *gsql.RawBytes: // do nothing
		case *[]byte: // do nothing
		case gsql.Scanner: // do nothing
		default:
			return false, errScanValNonSlice
		}
	}
	rows, err := q.QueryContext(ctx)
	if err != nil {
		return false, err
	}
	return NewScanner(rows).ScanVal(i)
}

func (q QueryExecutor) rowsScanner(ctx context.Context) (Scanner, error) {
	rows, err := q.QueryContext(ctx)
	if err != nil {
		return nil, err
	}
	return NewScanner(rows), nil
}
