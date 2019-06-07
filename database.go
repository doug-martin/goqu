package goqu

import (
	"context"
	"database/sql"
)

type (
	database interface {
		queryAdapter(builder *Dataset) Adapter
		From(cols ...interface{}) *Dataset
		Logger(logger Logger)
		Exec(query string, args ...interface{}) (sql.Result, error)
		ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
		Prepare(query string) (*sql.Stmt, error)
		PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
		Query(query string, args ...interface{}) (*sql.Rows, error)
		QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
		QueryRow(query string, args ...interface{}) *sql.Row
		QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
		ScanStructs(i interface{}, query string, args ...interface{}) error
		ScanStructsContext(ctx context.Context, i interface{}, query string, args ...interface{}) error
		ScanStruct(i interface{}, query string, args ...interface{}) (bool, error)
		ScanStructContext(ctx context.Context, i interface{}, query string, args ...interface{}) (bool, error)
		ScanVals(i interface{}, query string, args ...interface{}) error
		ScanValsContext(ctx context.Context, i interface{}, query string, args ...interface{}) error
		ScanVal(i interface{}, query string, args ...interface{}) (bool, error)
		ScanValContext(ctx context.Context, i interface{}, query string, args ...interface{}) (bool, error)
	}
	//This struct is the wrapper for a Db. The struct delegates most calls to either an Exec instance or to the Db passed into the constructor.
	Database struct {
		logger  Logger
		Dialect string
		Db      *sql.DB
	}
)

//This is the common entry point into goqu.
//
//dialect: This is the adapter dialect, you should see your database adapter for the string to use. Built in adpaters can be found at https://github.com/doug-martin/goqu/tree/master/adapters
//
//db: A sql.Db to use for querying the database
//      import (
//          "database/sql"
//          "fmt"
//          "github.com/doug-martin/goqu/v6"
//          _ "github.com/doug-martin/goqu/v6/adapters/postgres"
//          _ "github.com/lib/pq"
//      )
//
//      func main() {
//          sqlDb, err := sql.Open("postgres", "user=postgres dbname=goqupostgres sslmode=disable ")
//          if err != nil {
//              panic(err.Error())
//          }
//          db := goqu.New("postgres", sqlDb)
//      }
//The most commonly used Database method is From, which creates a new Dataset that uses the correct adapter and supports queries.
//          var ids []uint32
//          if err := db.From("items").Where(goqu.I("id").Gt(10)).Pluck("id", &ids); err != nil {
//              panic(err.Error())
//          }
//          fmt.Printf("%+v", ids)
func New(dialect string, db *sql.DB) *Database {
	return &Database{Dialect: dialect, Db: db}
}

//Starts a new Transaction.
func (me *Database) Begin() (*TxDatabase, error) {
	tx, err := me.Db.Begin()
	if err != nil {
		return nil, err
	}
	return &TxDatabase{Dialect: me.Dialect, Tx: tx, logger: me.logger}, nil
}

//used internally to create a new Adapter for a dataset
func (me *Database) queryAdapter(dataset *Dataset) Adapter {
	return NewAdapter(me.Dialect, dataset)
}

//Creates a new Dataset that uses the correct adapter and supports queries.
//          var ids []uint32
//          if err := db.From("items").Where(goqu.I("id").Gt(10)).Pluck("id", &ids); err != nil {
//              panic(err.Error())
//          }
//          fmt.Printf("%+v", ids)
//
//from...: Sources for you dataset, could be table names (strings), a goqu.Literal or another goqu.Dataset
func (me *Database) From(from ...interface{}) *Dataset {
	return withDatabase(me).From(from...)
}

//Sets the logger for to use when logging queries
func (me *Database) Logger(logger Logger) {
	me.logger = logger
}

//Logs a given operation with the specified sql and arguments
func (me *Database) Trace(op, sql string, args ...interface{}) {
	if me.logger != nil {
		if sql != "" {
			if len(args) != 0 {
				me.logger.Printf("[goqu] %s [query:=`%s` args:=%+v]", op, sql, args)
			} else {
				me.logger.Printf("[goqu] %s [query:=`%s`]", op, sql)
			}
		} else {
			me.logger.Printf("[goqu] %s", op)
		}
	}
}

//Uses the db to Execute the query with arguments and return the sql.Result
//
//query: The SQL to execute
//
//args...: for any placeholder parameters in the query
func (me *Database) Exec(query string, args ...interface{}) (sql.Result, error) {
	me.Trace("EXEC", query, args...)
	return me.Db.Exec(query, args...)
}

//Uses the db to Execute the query with arguments and return the sql.Result
//
//query: The SQL to execute
//
//args...: for any placeholder parameters in the query
func (me *Database) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	me.Trace("EXEC", query, args...)
	return me.Db.ExecContext(ctx, query, args...)
}

//Can be used to prepare a query.
//
//You can use this in tandem with a dataset by doing the following.
//    sql, args, err := db.From("items").Where(goqu.I("id").Gt(10)).ToSql(true)
//    if err != nil{
//        panic(err.Error()) //you could gracefully handle the error also
//    }
//    stmt, err := db.Prepare(sql)
//    if err != nil{
//        panic(err.Error()) //you could gracefully handle the error also
//    }
//    defer stmt.Close()
//    rows, err := stmt.Query(args)
//    if err != nil{
//        panic(err.Error()) //you could gracefully handle the error also
//    }
//    defer rows.Close()
//    for rows.Next(){
//              //scan your rows
//    }
//    if rows.Err() != nil{
//        panic(err.Error()) //you could gracefully handle the error also
//    }
//
//query: The SQL statement to prepare.
func (me *Database) Prepare(query string) (*sql.Stmt, error) {
	me.Trace("PREPARE", query)
	return me.Db.Prepare(query)
}

//Can be used to prepare a query.
//
//You can use this in tandem with a dataset by doing the following.
//    sql, args, err := db.From("items").Where(goqu.I("id").Gt(10)).ToSql(true)
//    if err != nil{
//        panic(err.Error()) //you could gracefully handle the error also
//    }
//    stmt, err := db.Prepare(sql)
//    if err != nil{
//        panic(err.Error()) //you could gracefully handle the error also
//    }
//    defer stmt.Close()
//    rows, err := stmt.QueryContext(ctx, args)
//    if err != nil{
//        panic(err.Error()) //you could gracefully handle the error also
//    }
//    defer rows.Close()
//    for rows.Next(){
//              //scan your rows
//    }
//    if rows.Err() != nil{
//        panic(err.Error()) //you could gracefully handle the error also
//    }
//
//query: The SQL statement to prepare.
func (me *Database) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	me.Trace("PREPARE", query)
	return me.Db.PrepareContext(ctx, query)
}

//Used to query for multiple rows.
//
//You can use this in tandem with a dataset by doing the following.
//    sql, err := db.From("items").Where(goqu.I("id").Gt(10)).Sql()
//    if err != nil{
//        panic(err.Error()) //you could gracefully handle the error also
//    }
//    rows, err := stmt.Query(args)
//    if err != nil{
//        panic(err.Error()) //you could gracefully handle the error also
//    }
//    defer rows.Close()
//    for rows.Next(){
//              //scan your rows
//    }
//    if rows.Err() != nil{
//        panic(err.Error()) //you could gracefully handle the error also
//    }
//
//query: The SQL to execute
//
//args...: for any placeholder parameters in the query
func (me *Database) Query(query string, args ...interface{}) (*sql.Rows, error) {
	me.Trace("QUERY", query, args...)
	return me.Db.Query(query, args...)
}

//Used to query for multiple rows.
//
//You can use this in tandem with a dataset by doing the following.
//    sql, err := db.From("items").Where(goqu.I("id").Gt(10)).Sql()
//    if err != nil{
//        panic(err.Error()) //you could gracefully handle the error also
//    }
//    rows, err := stmt.QueryContext(ctx, args)
//    if err != nil{
//        panic(err.Error()) //you could gracefully handle the error also
//    }
//    defer rows.Close()
//    for rows.Next(){
//              //scan your rows
//    }
//    if rows.Err() != nil{
//        panic(err.Error()) //you could gracefully handle the error also
//    }
//
//query: The SQL to execute
//
//args...: for any placeholder parameters in the query
func (me *Database) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	me.Trace("QUERY", query, args...)
	return me.Db.QueryContext(ctx, query, args...)
}

//Used to query for a single row.
//
//You can use this in tandem with a dataset by doing the following.
//    sql, err := db.From("items").Where(goqu.I("id").Gt(10)).Limit(1).Sql()
//    if err != nil{
//        panic(err.Error()) //you could gracefully handle the error also
//    }
//    rows, err := stmt.QueryRow(args)
//    if err != nil{
//        panic(err.Error()) //you could gracefully handle the error also
//    }
//    //scan your row
//
//query: The SQL to execute
//
//args...: for any placeholder parameters in the query
func (me *Database) QueryRow(query string, args ...interface{}) *sql.Row {
	me.Trace("QUERY ROW", query, args...)
	return me.Db.QueryRow(query, args...)
}

//Used to query for a single row.
//
//You can use this in tandem with a dataset by doing the following.
//    sql, err := db.From("items").Where(goqu.I("id").Gt(10)).Limit(1).Sql()
//    if err != nil{
//        panic(err.Error()) //you could gracefully handle the error also
//    }
//    rows, err := stmt.QueryRowContext(ctx, args)
//    if err != nil{
//        panic(err.Error()) //you could gracefully handle the error also
//    }
//    //scan your row
//
//query: The SQL to execute
//
//args...: for any placeholder parameters in the query
func (me *Database) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	me.Trace("QUERY ROW", query, args...)
	return me.Db.QueryRowContext(ctx, query, args...)
}

//Queries the database using the supplied query, and args and uses CrudExec.ScanStructs to scan the results into a slice of structs
//
//i: A pointer to a slice of structs
//
//query: The SQL to execute
//
//args...: for any placeholder parameters in the query
func (me *Database) ScanStructs(i interface{}, query string, args ...interface{}) error {
	exec := newCrudExec(me, nil, query, args...)
	return exec.ScanStructs(i)
}

//Queries the database using the supplied context, query, and args and uses CrudExec.ScanStructsContext to scan the results into a slice of structs
//
//i: A pointer to a slice of structs
//
//query: The SQL to execute
//
//args...: for any placeholder parameters in the query
func (me *Database) ScanStructsContext(ctx context.Context, i interface{}, query string, args ...interface{}) error {
	exec := newCrudExec(me, nil, query, args...)
	return exec.ScanStructsContext(ctx, i)
}

//Queries the database using the supplied query, and args and uses CrudExec.ScanStruct to scan the results into a struct
//
//i: A pointer to a struct
//
//query: The SQL to execute
//
//args...: for any placeholder parameters in the query
func (me *Database) ScanStruct(i interface{}, query string, args ...interface{}) (bool, error) {
	exec := newCrudExec(me, nil, query, args...)
	return exec.ScanStruct(i)
}

//Queries the database using the supplied context, query, and args and uses CrudExec.ScanStructContext to scan the results into a struct
//
//i: A pointer to a struct
//
//query: The SQL to execute
//
//args...: for any placeholder parameters in the query
func (me *Database) ScanStructContext(ctx context.Context, i interface{}, query string, args ...interface{}) (bool, error) {
	exec := newCrudExec(me, nil, query, args...)
	return exec.ScanStructContext(ctx, i)
}

//Queries the database using the supplied query, and args and uses CrudExec.ScanVals to scan the results into a slice of primitive values
//
//i: A pointer to a slice of primitive values
//
//query: The SQL to execute
//
//args...: for any placeholder parameters in the query
func (me *Database) ScanVals(i interface{}, query string, args ...interface{}) error {
	exec := newCrudExec(me, nil, query, args...)
	return exec.ScanVals(i)
}

//Queries the database using the supplied context, query, and args and uses CrudExec.ScanValsContext to scan the results into a slice of primitive values
//
//i: A pointer to a slice of primitive values
//
//query: The SQL to execute
//
//args...: for any placeholder parameters in the query
func (me *Database) ScanValsContext(ctx context.Context, i interface{}, query string, args ...interface{}) error {
	exec := newCrudExec(me, nil, query, args...)
	return exec.ScanValsContext(ctx, i)
}

//Queries the database using the supplied query, and args and uses CrudExec.ScanVal to scan the results into a primitive value
//
//i: A pointer to a primitive value
//
//query: The SQL to execute
//
//args...: for any placeholder parameters in the query
func (me *Database) ScanVal(i interface{}, query string, args ...interface{}) (bool, error) {
	exec := newCrudExec(me, nil, query, args...)
	return exec.ScanVal(i)
}

//Queries the database using the supplied context, query, and args and uses CrudExec.ScanValContext to scan the results into a primitive value
//
//i: A pointer to a primitive value
//
//query: The SQL to execute
//
//args...: for any placeholder parameters in the query
func (me *Database) ScanValContext(ctx context.Context, i interface{}, query string, args ...interface{}) (bool, error) {
	exec := newCrudExec(me, nil, query, args...)
	return exec.ScanValContext(ctx, i)
}

//A wrapper around a sql.Tx and works the same way as Database
type TxDatabase struct {
	logger  Logger
	Dialect string
	Tx      *sql.Tx
}

//used internally to create a new query adapter for a Dataset
func (me *TxDatabase) queryAdapter(dataset *Dataset) Adapter {
	return NewAdapter(me.Dialect, dataset)
}

//Creates a new Dataset for querying a Database.
func (me *TxDatabase) From(cols ...interface{}) *Dataset {
	return withDatabase(me).From(cols...)

}

//Sets the logger
func (me *TxDatabase) Logger(logger Logger) {
	me.logger = logger
}

func (me *TxDatabase) Trace(op, sql string, args ...interface{}) {
	if me.logger != nil {
		if sql != "" {
			if len(args) != 0 {
				me.logger.Printf("[goqu - transaction] %s [query:=`%s` args:=%+v] ", op, sql, args)
			} else {
				me.logger.Printf("[goqu - transaction] %s [query:=`%s`] ", op, sql)
			}
		} else {
			me.logger.Printf("[goqu - transaction] %s", op)
		}
	}
}

//See Database#Exec
func (me *TxDatabase) Exec(query string, args ...interface{}) (sql.Result, error) {
	me.Trace("EXEC", query, args...)
	return me.Tx.Exec(query, args...)
}

//See Database#ExecContext
func (me *TxDatabase) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	me.Trace("EXEC", query, args...)
	return me.Tx.ExecContext(ctx, query, args...)
}

//See Database#Prepare
func (me *TxDatabase) Prepare(query string) (*sql.Stmt, error) {
	me.Trace("PREPARE", query)
	return me.Tx.Prepare(query)
}

//See Database#PrepareContext
func (me *TxDatabase) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	me.Trace("PREPARE", query)
	return me.Tx.PrepareContext(ctx, query)
}

//See Database#Query
func (me *TxDatabase) Query(query string, args ...interface{}) (*sql.Rows, error) {
	me.Trace("QUERY", query, args...)
	return me.Tx.Query(query, args...)
}

//See Database#QueryContext
func (me *TxDatabase) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	me.Trace("QUERY", query, args...)
	return me.Tx.QueryContext(ctx, query, args...)
}

//See Database#QueryRow
func (me *TxDatabase) QueryRow(query string, args ...interface{}) *sql.Row {
	me.Trace("QUERY ROW", query, args...)
	return me.Tx.QueryRow(query, args...)
}

//See Database#QueryRowContext
func (me *TxDatabase) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	me.Trace("QUERY ROW", query, args...)
	return me.Tx.QueryRowContext(ctx, query, args...)
}

//See Database#ScanStructs
func (me *TxDatabase) ScanStructs(i interface{}, query string, args ...interface{}) error {
	exec := newCrudExec(me, nil, query, args...)
	return exec.ScanStructs(i)
}

//See Database#ScanStructsContext
func (me *TxDatabase) ScanStructsContext(ctx context.Context, i interface{}, query string, args ...interface{}) error {
	exec := newCrudExec(me, nil, query, args...)
	return exec.ScanStructsContext(ctx, i)
}

//See Database#ScanStruct
func (me *TxDatabase) ScanStruct(i interface{}, query string, args ...interface{}) (bool, error) {
	exec := newCrudExec(me, nil, query, args...)
	return exec.ScanStruct(i)
}

//See Database#ScanStructContext
func (me *TxDatabase) ScanStructContext(ctx context.Context, i interface{}, query string, args ...interface{}) (bool, error) {
	exec := newCrudExec(me, nil, query, args...)
	return exec.ScanStructContext(ctx, i)
}

//See Database#ScanVals
func (me *TxDatabase) ScanVals(i interface{}, query string, args ...interface{}) error {
	exec := newCrudExec(me, nil, query, args...)
	return exec.ScanVals(i)
}

//See Database#ScanValsContext
func (me *TxDatabase) ScanValsContext(ctx context.Context, i interface{}, query string, args ...interface{}) error {
	exec := newCrudExec(me, nil, query, args...)
	return exec.ScanValsContext(ctx, i)
}

//See Database#ScanVal
func (me *TxDatabase) ScanVal(i interface{}, query string, args ...interface{}) (bool, error) {
	exec := newCrudExec(me, nil, query, args...)
	return exec.ScanVal(i)
}

//See Database#ScanValContext
func (me *TxDatabase) ScanValContext(ctx context.Context, i interface{}, query string, args ...interface{}) (bool, error) {
	exec := newCrudExec(me, nil, query, args...)
	return exec.ScanValContext(ctx, i)
}

//COMMIT the transaction
func (me *TxDatabase) Commit() error {
	me.Trace("COMMIT", "")
	return me.Tx.Commit()
}

//ROLLBACK the transaction
func (me *TxDatabase) Rollback() error {
	me.Trace("ROLLBACK", "")
	return me.Tx.Rollback()
}

//A helper method that will automatically COMMIT or ROLLBACK once the  supplied function is done executing
//
//      tx, err := db.Begin()
//      if err != nil{
//           panic(err.Error()) //you could gracefully handle the error also
//      }
//      if err := tx.Wrap(func() error{
//          if _, err := tx.From("test").Insert(Record{"a":1, "b": "b"}).Exec(){
//              //this error will be the return error from the Wrap call
//              return err
//          }
//          return nil
//      }); err != nil{
//           panic(err.Error()) //you could gracefully handle the error also
//      }
func (me *TxDatabase) Wrap(fn func() error) error {
	if err := fn(); err != nil {
		if rollbackErr := me.Rollback(); rollbackErr != nil {
			return rollbackErr
		}
		return err
	}
	return me.Commit()
}
