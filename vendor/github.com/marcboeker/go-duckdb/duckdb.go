// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

// Package duckdb implements a database/sql driver for the DuckDB database.
package duckdb

/*
#include <duckdb.h>
*/
import "C"

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/url"
	"strings"
	"unsafe"
)

func init() {
	sql.Register("duckdb", Driver{})
}

type Driver struct{}

func (d Driver) Open(dsn string) (driver.Conn, error) {
	c, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	return c.Connect(context.Background())
}

func (Driver) OpenConnector(dsn string) (driver.Connector, error) {
	return NewConnector(dsn, func(execerContext driver.ExecerContext) error {
		return nil
	})
}

// NewConnector opens a new Connector for a DuckDB database.
// The user must close the Connector, if it is not passed to the sql.OpenDB function.
// Otherwise, sql.DB closes the Connector when calling sql.DB.Close().
func NewConnector(dsn string, connInitFn func(execer driver.ExecerContext) error) (*Connector, error) {
	var db C.duckdb_database

	parsedDSN, err := url.Parse(dsn)
	if err != nil {
		return nil, getError(errParseDSN, err)
	}

	config, err := prepareConfig(parsedDSN)
	if err != nil {
		return nil, err
	}
	defer C.duckdb_destroy_config(&config)

	connStr := C.CString(getConnString(dsn))
	defer C.duckdb_free(unsafe.Pointer(connStr))

	var outError *C.char
	defer C.duckdb_free(unsafe.Pointer(outError))

	if state := C.duckdb_open_ext(connStr, &db, config, &outError); state == C.DuckDBError {
		return nil, getError(errOpen, duckdbError(outError))
	}

	return &Connector{
		db:         db,
		connInitFn: connInitFn,
	}, nil
}

type Connector struct {
	db         C.duckdb_database
	connInitFn func(execer driver.ExecerContext) error
}

func (*Connector) Driver() driver.Driver {
	return Driver{}
}

func (c *Connector) Connect(context.Context) (driver.Conn, error) {
	var duckdbCon C.duckdb_connection
	if state := C.duckdb_connect(c.db, &duckdbCon); state == C.DuckDBError {
		return nil, getError(errConnect, nil)
	}

	con := &conn{duckdbCon: duckdbCon}

	if c.connInitFn != nil {
		if err := c.connInitFn(con); err != nil {
			return nil, err
		}
	}

	return con, nil
}

func (c *Connector) Close() error {
	C.duckdb_close(&c.db)
	c.db = nil
	return nil
}

func getConnString(dsn string) string {
	idx := strings.Index(dsn, "?")
	if idx < 0 {
		idx = len(dsn)
	}
	return dsn[0:idx]
}

func prepareConfig(parsedDSN *url.URL) (C.duckdb_config, error) {
	var config C.duckdb_config
	if state := C.duckdb_create_config(&config); state == C.DuckDBError {
		C.duckdb_destroy_config(&config)
		return nil, getError(errCreateConfig, nil)
	}

	if err := setConfigOption(config, "duckdb_api", "go"); err != nil {
		return nil, err
	}

	// Early-out, if the DSN does not contain configuration options.
	if len(parsedDSN.RawQuery) == 0 {
		return config, nil
	}

	for k, v := range parsedDSN.Query() {
		if len(v) == 0 {
			continue
		}
		if err := setConfigOption(config, k, v[0]); err != nil {
			return nil, err
		}
	}

	return config, nil
}

func setConfigOption(config C.duckdb_config, name string, option string) error {
	cName := C.CString(name)
	defer C.duckdb_free(unsafe.Pointer(cName))

	cOption := C.CString(option)
	defer C.duckdb_free(unsafe.Pointer(cOption))

	state := C.duckdb_set_config(config, cName, cOption)
	if state == C.DuckDBError {
		C.duckdb_destroy_config(&config)
		return getError(errSetConfig, fmt.Errorf("%s=%s", name, option))
	}

	return nil
}
