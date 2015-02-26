package gql

import "database/sql"

type (
	database interface {
		QueryAdapter(builder *Dataset) Adapter
		From(cols ...interface{}) *Dataset
		Logger(logger Logger)
		Exec(query string, args ...interface{}) (sql.Result, error)
		Prepare(query string) (*sql.Stmt, error)
		Query(query string, args ...interface{}) (*sql.Rows, error)
		QueryRow(query string, args ...interface{}) *sql.Row
		ScanStructs(i interface{}, query string, args ...interface{}) error
		ScanStruct(i interface{}, query string, args ...interface{}) (bool, error)
		ScanVals(i interface{}, query string, args ...interface{}) error
		ScanVal(i interface{}, query string, args ...interface{}) (bool, error)
	}
	Database struct {
		dbAdapter DbAdapter
	}
)

func New(dialect string, db Db) Database {
	return Database{NewDbAdapter(dialect, db)}
}

func (me Database) Begin() (TxDatabase, error) {
	txAdapter, err := me.dbAdapter.Begin()
	if err != nil {
		return TxDatabase{}, err
	}
	return TxDatabase{dbAdapter: txAdapter}, nil
}

func (me Database) QueryAdapter(builder *Dataset) Adapter {
	return me.dbAdapter.QueryAdapter(builder)
}

func (me Database) From(cols ...interface{}) *Dataset {
	return withDatabase(me).From(cols...)
}

func (me Database) Logger(logger Logger) {
	me.dbAdapter.SetLogger(logger)
}

func (me Database) Exec(query string, args ...interface{}) (sql.Result, error) {
	return me.dbAdapter.Exec(query, args...)
}

func (me Database) Prepare(query string) (*sql.Stmt, error) {
	return me.dbAdapter.Prepare(query)
}
func (me Database) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return me.dbAdapter.Query(query, args...)
}
func (me Database) QueryRow(query string, args ...interface{}) *sql.Row {
	return me.dbAdapter.QueryRow(query, args...)
}

func (me Database) ScanStructs(i interface{}, query string, args ...interface{}) error {
	exec := newExec(me, nil, query, args...)
	return exec.ScanStructs(i)
}

func (me Database) ScanStruct(i interface{}, query string, args ...interface{}) (bool, error) {
	exec := newExec(me, nil, query, args...)
	return exec.ScanStruct(i)
}

func (me Database) ScanVals(i interface{}, query string, args ...interface{}) error {
	exec := newExec(me, nil, query, args...)
	return exec.ScanVals(i)
}

func (me Database) ScanVal(i interface{}, query string, args ...interface{}) (bool, error) {
	exec := newExec(me, nil, query, args...)
	return exec.ScanVal(i)
}

type TxDatabase struct {
	dbAdapter TxDbAdapter
}

func (me TxDatabase) QueryAdapter(builder *Dataset) Adapter {
	return me.dbAdapter.QueryAdapter(builder)
}

func (me TxDatabase) From(cols ...interface{}) *Dataset {
	return withDatabase(me).From(cols...)

}

func (me TxDatabase) Logger(logger Logger) {
	me.dbAdapter.SetLogger(logger)
}

func (me TxDatabase) Exec(query string, args ...interface{}) (sql.Result, error) {
	return me.dbAdapter.Exec(query, args...)
}

func (me TxDatabase) Prepare(query string) (*sql.Stmt, error) {
	return me.dbAdapter.Prepare(query)
}
func (me TxDatabase) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return me.dbAdapter.Query(query, args...)
}
func (me TxDatabase) QueryRow(query string, args ...interface{}) *sql.Row {
	return me.dbAdapter.QueryRow(query, args...)
}

func (me TxDatabase) ScanStructs(i interface{}, query string, args ...interface{}) error {
	exec := newExec(me, nil, query, args...)
	return exec.ScanStructs(i)
}

func (me TxDatabase) ScanStruct(i interface{}, query string, args ...interface{}) (bool, error) {
	exec := newExec(me, nil, query, args...)
	return exec.ScanStruct(i)
}

func (me TxDatabase) ScanVals(i interface{}, query string, args ...interface{}) error {
	exec := newExec(me, nil, query, args...)
	return exec.ScanVals(i)
}

func (me TxDatabase) ScanVal(i interface{}, query string, args ...interface{}) (bool, error) {
	exec := newExec(me, nil, query, args...)
	return exec.ScanVal(i)
}

func (me TxDatabase) Commit() error {
	return me.dbAdapter.Commit()
}

func (me TxDatabase) Rollback() error {
	return me.dbAdapter.Rollback()
}

func (me TxDatabase) Wrap(fn func() error) error {
	if err := fn(); err != nil {
		if rollbackErr := me.Rollback(); rollbackErr != nil {
			return rollbackErr
		}
		return err
	}
	return me.Commit()
}
