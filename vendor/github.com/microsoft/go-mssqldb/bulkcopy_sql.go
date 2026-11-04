package mssql

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
)

type copyin struct {
	cn       *Conn
	bulkcopy *Bulk
	closed   bool
}

type serializableBulkConfig struct {
	TableName   string
	ColumnsName []string
	Options     BulkOptions
}

func (d *Driver) OpenConnection(dsn string) (*Conn, error) {
	return d.open(context.Background(), dsn)
}

func (c *Conn) prepareCopyIn(ctx context.Context, query string) (_ driver.Stmt, err error) {
	config_json := query[11:]

	bulkconfig := serializableBulkConfig{}
	err = json.Unmarshal([]byte(config_json), &bulkconfig)
	if err != nil {
		return
	}

	bulkcopy := c.CreateBulkContext(ctx, bulkconfig.TableName, bulkconfig.ColumnsName)
	bulkcopy.Options = bulkconfig.Options

	ci := &copyin{
		cn:       c,
		bulkcopy: bulkcopy,
	}

	return ci, nil
}

// CopyIn creates a bulk import statement that can be passed to Prepare.
//
// table is the destination object name. It is treated as an object name and is
// quoted before it is sent to the server, so a name that is not a regular
// identifier does not need to be delimited by the caller. It may be qualified,
// as in "schema.table" or "database.schema.table", and individual parts may be
// delimited, as in "[my schema].[my table]".
//
// Because the destination is always an object name, a value that relied on
// being spliced into the statement as raw text no longer resolves. Pass
// "table" and options.Tablock rather than "table WITH (TABLOCK)". A name that
// cannot identify an object, because it is empty or has more than four parts,
// is reported before any statement is sent.
//
// columns are the destination column names, in the order their values are
// passed to Exec.
//
// Each options.Order entry names one or more columns to declare the data is
// already sorted by, as in "id" or "id ASC, name DESC". Column names are quoted
// the same way table is. A column name is separated from an optional trailing
// ASC or DESC sort direction by whitespace, and from the next column by a
// comma, so a column whose name ends in "asc" or "desc" or contains a comma has
// to be delimited by the caller to be told apart from a direction or a
// separator, as in "[sort desc]" or "[order, id]".
func CopyIn(table string, options BulkOptions, columns ...string) string {
	bulkconfig := &serializableBulkConfig{TableName: table, Options: options, ColumnsName: columns}

	config_json, err := json.Marshal(bulkconfig)
	if err != nil {
		panic(err)
	}

	stmt := "INSERTBULK " + string(config_json)

	return stmt
}

func (ci *copyin) NumInput() int {
	return -1
}

func (ci *copyin) Query(v []driver.Value) (r driver.Rows, err error) {
	panic("should never be called")
}

func (ci *copyin) Exec(v []driver.Value) (r driver.Result, err error) {
	if ci.closed {
		return nil, errors.New("copyin query is closed")
	}

	if len(v) == 0 {
		rowCount, err := ci.bulkcopy.Done()
		ci.closed = true
		return driver.RowsAffected(rowCount), err
	}

	t := make([]interface{}, len(v))
	for i, val := range v {
		t[i] = val
	}

	err = ci.bulkcopy.AddRow(t)
	if err != nil {
		return
	}

	return driver.RowsAffected(0), nil
}

func (ci *copyin) Close() (err error) {
	return nil
}
