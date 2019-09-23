package exec

import (
	"database/sql"
	"reflect"

	"github.com/doug-martin/goqu/v9/exp"
	"github.com/doug-martin/goqu/v9/internal/errors"
	"github.com/doug-martin/goqu/v9/internal/util"
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
func (s *scanner) Next() bool {
	return s.rows.Next()
}

// Err returns the error, if any that was encountered during iteration. See
// sql.Rows#Err for more information.
func (s *scanner) Err() error {
	return s.rows.Err()
}

// ScanStruct will scan the current row into i.
func (s *scanner) ScanStruct(i interface{}) error {
	// Setup columnMap and columns, but only once.
	if s.columnMap == nil || s.columns == nil {
		cm, err := util.GetColumnMap(i)
		if err != nil {
			return err
		}

		cols, err := s.rows.Columns()
		if err != nil {
			return err
		}

		s.columnMap = cm
		s.columns = cols
	}

	scans := make([]interface{}, len(s.columns))
	for idx, col := range s.columns {
		data, ok := s.columnMap[col]
		switch {
		case !ok:
			return unableToFindFieldError(col)
		default:
			scans[idx] = reflect.New(data.GoType).Interface()
		}
	}

	err := s.rows.Scan(scans...)
	if err != nil {
		return err
	}

	record := exp.Record{}
	for index, col := range s.columns {
		record[col] = scans[index]
	}

	util.AssignStructVals(i, record, s.columnMap)

	return s.Err()
}

// ScanVal will scan the current row and column into i.
func (s *scanner) ScanVal(i interface{}) error {
	err := s.rows.Scan(i)
	if err != nil {
		return err
	}

	return s.Err()
}

// Close closes the Rows, preventing further enumeration. See sql.Rows#Close
// for more info.
func (s *scanner) Close() error {
	return s.rows.Close()
}
