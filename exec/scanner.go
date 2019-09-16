package exec

import (
	"database/sql"
	"reflect"

	"github.com/doug-martin/goqu/v8/exp"
	"github.com/doug-martin/goqu/v8/internal/errors"
	"github.com/doug-martin/goqu/v8/internal/util"
)

type (
	// Scanner knows how to scan sql.Rows into structs.
	Scanner interface {
		Next() bool
		ScanStruct(i interface{}) error
		ScanVal(i interface{}) error
		Close() error
		Err() error
	}

	scanner struct {
		rows      *sql.Rows
		columnMap util.ColumnMap
		columns   []string
	}
)

func unableToFindFieldError(col string) error {
	return errors.New(`unable to find corresponding field to column "%s" returned by query`, col)
}

// NewScanner returns a scanner that can be used for scanning rows into structs.
func NewScanner(rows *sql.Rows) Scanner {
	return &scanner{rows: rows}
}

// Next prepares the next row for Scanning. See sql.Rows#Next for more
// information.
func (it *scanner) Next() bool {
	return it.rows.Next()
}

// Err returns the error, if any that was encountered during iteration. See
// sql.Rows#Err for more information.
func (it *scanner) Err() error {
	return it.rows.Err()
}

// ScanStruct will scan the current row into i.
func (it *scanner) ScanStruct(i interface{}) error {
	// Setup columnMap and columns, but only once.
	if it.columnMap == nil || it.columns == nil {
		cm, err := util.GetColumnMap(i)
		if err != nil {
			return err
		}

		cols, err := it.rows.Columns()
		if err != nil {
			return err
		}

		it.columnMap = cm
		it.columns = cols
	}

	scans := make([]interface{}, len(it.columns))
	for idx, col := range it.columns {
		data, ok := it.columnMap[col]
		switch {
		case !ok:
			return unableToFindFieldError(col)
		default:
			scans[idx] = reflect.New(data.GoType).Interface()
		}
	}

	err := it.rows.Scan(scans...)
	if err != nil {
		return err
	}

	record := exp.Record{}
	for index, col := range it.columns {
		record[col] = scans[index]
	}

	util.AssignStructVals(i, record, it.columnMap)

	return it.Err()
}

// ScanVal will scan the current row and column into i.
func (it *scanner) ScanVal(i interface{}) error {
	err := it.rows.Scan(i)
	if err != nil {
		return err
	}

	return it.Err()
}

// Close closes the Rows, preventing further enumeration. See sql.Rows#Close
// for more info.
func (it *scanner) Close() error {
	return it.rows.Close()
}
