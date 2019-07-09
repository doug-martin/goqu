package exec

import (
	"database/sql"
	"reflect"

	"github.com/doug-martin/goqu/v7/exp"
	"github.com/doug-martin/goqu/v7/internal/errors"
	"github.com/doug-martin/goqu/v7/internal/util"
)

type (
	Scanner interface {
		ScanStructs(i interface{}) (bool, error)
		ScanVals(i interface{}) (bool, error)
	}
	scanner struct {
		rows *sql.Rows
	}
)

func unableToFindFieldError(col string) error {
	return errors.New(`unable to find corresponding field to column "%s" returned by query`, col)
}

func NewScanner(rows *sql.Rows) Scanner {
	return &scanner{rows: rows}
}

// This will execute the SQL and append results to the slice
//    var myStructs []MyStruct
//    if err := From("test").ScanStructs(&myStructs); err != nil{
//        panic(err.Error()
//    }
//    //use your structs
//
//
// i: A pointer to a slice of structs.
func (q *scanner) ScanStructs(i interface{}) (found bool, err error) {
	defer q.rows.Close()
	cm, err := util.GetColumnMap(i)
	if err != nil {
		return found, err
	}
	var results []map[string]interface{}
	columns, err := q.rows.Columns()
	if err != nil {
		return false, err
	}
	for q.rows.Next() {
		record, err := q.scanIntoRecord(columns, cm)
		if err != nil {
			return found, err
		}

		results = append(results, record)
	}
	if q.rows.Err() != nil {
		return false, q.rows.Err()
	}
	if len(results) > 0 {
		found = true
		util.AssignStructVals(i, results, cm)
	}
	return found, nil
}

// This will execute the SQL and append results to the slice.
//    var ids []uint32
//    if err := From("test").Select("id").ScanVals(&ids); err != nil{
//        panic(err.Error()
//    }
//
// i: Takes a pointer to a slice of primitive values.
func (q *scanner) ScanVals(i interface{}) (found bool, err error) {
	defer q.rows.Close()
	val := reflect.Indirect(reflect.ValueOf(i))
	t, _, isSliceOfPointers := util.GetTypeInfo(i, val)
	switch val.Kind() {
	case reflect.Slice:
		for q.rows.Next() {
			found = true
			row := reflect.New(t)
			if err = q.rows.Scan(row.Interface()); err != nil {
				return found, err
			}
			if isSliceOfPointers {
				val.Set(reflect.Append(val, row))
			} else {
				val.Set(reflect.Append(val, reflect.Indirect(row)))
			}
		}
	default:
		for q.rows.Next() {
			found = true
			if err = q.rows.Scan(i); err != nil {
				return false, err
			}
		}

	}
	return found, q.rows.Err()
}

func (q *scanner) scanIntoRecord(columns []string, cm util.ColumnMap) (record exp.Record, err error) {
	scans := make([]interface{}, len(columns))
	for i, col := range columns {
		data, ok := cm[col]
		switch {
		case !ok:
			return record, unableToFindFieldError(col)
		case util.IsPointer(data.GoType.Kind()):
			scans[i] = reflect.New(data.GoType.Elem()).Interface()
		default:
			scans[i] = reflect.New(data.GoType).Interface()
		}
	}
	if err := q.rows.Scan(scans...); err != nil {
		return record, err
	}
	record = exp.Record{}
	for index, col := range columns {
		record[col] = scans[index]
	}
	return record, nil
}
