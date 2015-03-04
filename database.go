package gql

import "database/sql"

type (
	database interface {
		queryAdapter(builder *Dataset) Adapter
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
	//This struct is the wrapper for a Db. The struct delegates most calls to either an Exec instance or to the Db passed into the constructor.
	Database struct {
		logger  Logger
		Dialect string
		Db      *sql.DB
	}
)

//This is the common entry point into gql.
//
//dialect: This is the adapter dialect, you should see your database adapter for the string to use. Built in adpaters can be found at https://github.com/doug-martin/gql/tree/master/adapters
//db: A sql.Db to use for querying the database
//      import (
//          "database/sql"
//          "fmt"
//          "github.com/doug-martin/gql"
//          _ "github.com/doug-martin/gql/adapters/postgres"
//          _ "github.com/lib/pq"
//      )
//
//      func main() {
//          sqlDb, err := sql.Open("postgres", "user=postgres dbname=gqlpostgres sslmode=disable ")
//          if err != nil {
//              panic(err.Error())
//          }
//          db := gql.New("postgres", sqlDb)
//      }
//The most commonly used Database method is From, which creates a new Dataset that uses the correct adapter and supports queries.
//          var ids []uint32
//          if err := db.From("items").Where(gql.I("id").Gt(10)).Pluck("id", &ids); err != nil {
//              panic(err.Error())
//          }
//          fmt.Printf("%+v", ids)
func New(dialect string, db *sql.DB) Database {
	return Database{Dialect: dialect, Db: db}
}

//Starts a new Transaction.
func (me Database) Begin() (TxDatabase, error) {
	tx, err := me.Db.Begin()
	if err != nil {
		return TxDatabase{}, err
	}
	return TxDatabase{Dialect: me.Dialect, Tx: tx}, nil
}

//used internally to create a new Adapter for a dataset
func (me Database) queryAdapter(dataset *Dataset) Adapter {
	return NewAdapter(me.Dialect, dataset)
}

//Creates a new Dataset that uses the correct adapter and supports queries.
//          var ids []uint32
//          if err := db.From("items").Where(gql.I("id").Gt(10)).Pluck("id", &ids); err != nil {
//              panic(err.Error())
//          }
//          fmt.Printf("%+v", ids)
//
//from...: Sources for you dataset, could be table names (strings), a gql.Literal or another gql.Dataset
func (me Database) From(from ...interface{}) *Dataset {
	return withDatabase(me).From(from...)
}

//Sets the logger for to use when logging queries
func (me Database) Logger(logger Logger) {
	me.logger = logger
}

//Logs a given operation with the specified sql and arguments
func (me Database) Trace(op, sql string, args ...interface{}) {
	if me.logger != nil {
		if sql != "" {
			if len(args) != 0 {
				me.logger.Printf("[gql] %s [query:=`%s` args:=%+v] ", op, sql, args)
			} else {
				me.logger.Printf("[gql] %s [query:=`%s`] ", op, sql)
			}
		} else {
			me.logger.Printf("[gql] %s", op)
		}
	}
}

//Uses the db to Execute the query with arguments and return the sql.Result
//
//query: The SQL to execute
//args...: for any placeholder parameters in the query
func (me Database) Exec(query string, args ...interface{}) (sql.Result, error) {
	me.Trace("EXEC", query, args...)
	return me.Db.Exec(query, args...)
}

//Can be used to prepare a query.
//
//You can use this in tandem with a dataset by doing the following.
//    sql, args, err := db.From("items").Where(gql.I("id").Gt(10)).ToSql(true)
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
func (me Database) Prepare(query string) (*sql.Stmt, error) {
	me.Trace("PREPARE", query)
	return me.Db.Prepare(query)
}

//Can be used to prepare a query.
//
//You can use this in tandem with a dataset by doing the following.
//    sql, err := db.From("items").Where(gql.I("id").Gt(10)).Sql()
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
//args...: for any placeholder parameters in the query
func (me Database) Query(query string, args ...interface{}) (*sql.Rows, error) {
	me.Trace("QUERY", query, args...)
	return me.Db.Query(query, args...)
}

//Can be used to prepare a query.
//
//You can use this in tandem with a dataset by doing the following.
//    sql, err := db.From("items").Where(gql.I("id").Gt(10)).Limit(1).Sql()
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
//args...: for any placeholder parameters in the query
func (me Database) QueryRow(query string, args ...interface{}) *sql.Row {
	me.Trace("QUERY ROW", query, args...)
	return me.Db.QueryRow(query, args...)
}

//Queries the database using the supplied query, and args and uses Exec#ScanStructs to scan the results into a slice of structs
//
//i: A pointer to a slice of structs
//query: The SQL to execute
//args...: for any placeholder parameters in the query
func (me Database) ScanStructs(i interface{}, query string, args ...interface{}) error {
	exec := newExec(me, nil, query, args...)
	return exec.ScanStructs(i)
}

//Queries the database using the supplied query, and args and uses Exec#ScanStruct to scan the results into a struct
//
//i: A pointer to a struct
//query: The SQL to execute
//args...: for any placeholder parameters in the query
func (me Database) ScanStruct(i interface{}, query string, args ...interface{}) (bool, error) {
	exec := newExec(me, nil, query, args...)
	return exec.ScanStruct(i)
}

//Queries the database using the supplied query, and args and uses Exec#ScanVals to scan the results into a slice of primitive values
//
//i: A pointer to a slice of primitive values
//query: The SQL to execute
//args...: for any placeholder parameters in the query
func (me Database) ScanVals(i interface{}, query string, args ...interface{}) error {
	exec := newExec(me, nil, query, args...)
	return exec.ScanVals(i)
}

//Queries the database using the supplied query, and args and uses Exec#ScanVal to scan the results into a primitive value
//
//i: A pointer to a primitive value
//query: The SQL to execute
//args...: for any placeholder parameters in the query
func (me Database) ScanVal(i interface{}, query string, args ...interface{}) (bool, error) {
	exec := newExec(me, nil, query, args...)
	return exec.ScanVal(i)
}

//A wrapper around a sql.Tx and works the same way as Database
type TxDatabase struct {
	logger  Logger
	Dialect string
	Tx      *sql.Tx
}

//used internally to create a new query adapter for a Dataset
func (me TxDatabase) queryAdapter(dataset *Dataset) Adapter {
	return NewAdapter(me.Dialect, dataset)
}

//Creates a new Dataset for querying a Database.
func (me TxDatabase) From(cols ...interface{}) *Dataset {
	return withDatabase(me).From(cols...)

}

//Sets the logger
func (me TxDatabase) Logger(logger Logger) {
	me.logger = logger
}

func (me TxDatabase) Trace(op, sql string, args ...interface{}) {
	if me.logger != nil {
		if sql != "" {
			if len(args) != 0 {
				me.logger.Printf("[gql - transaction] %s [query:=`%s` args:=%+v] ", op, sql, args)
			} else {
				me.logger.Printf("[gql - transaction] %s [query:=`%s`] ", op, sql)
			}
		} else {
			me.logger.Printf("[gql - transaction] %s", op)
		}
	}
}

//See Database#Exec
func (me TxDatabase) Exec(query string, args ...interface{}) (sql.Result, error) {
	me.Trace("EXEC", query, args...)
	return me.Tx.Exec(query, args...)
}

//See Database#Prepare
func (me TxDatabase) Prepare(query string) (*sql.Stmt, error) {
	me.Trace("PREPARE", query)
	return me.Tx.Prepare(query)
}

//See Database#Query
func (me TxDatabase) Query(query string, args ...interface{}) (*sql.Rows, error) {
	me.Trace("QUERY", query, args...)
	return me.Tx.Query(query, args...)
}

//See Database#QueryRow
func (me TxDatabase) QueryRow(query string, args ...interface{}) *sql.Row {
	me.Trace("QUERY ROW", query, args...)
	return me.Tx.QueryRow(query, args...)
}

//See Database#ScanStructs
func (me TxDatabase) ScanStructs(i interface{}, query string, args ...interface{}) error {
	exec := newExec(me, nil, query, args...)
	return exec.ScanStructs(i)
}

//See Database#ScanStruct
func (me TxDatabase) ScanStruct(i interface{}, query string, args ...interface{}) (bool, error) {
	exec := newExec(me, nil, query, args...)
	return exec.ScanStruct(i)
}

//See Database#ScanVals
func (me TxDatabase) ScanVals(i interface{}, query string, args ...interface{}) error {
	exec := newExec(me, nil, query, args...)
	return exec.ScanVals(i)
}

//See Database#ScanVal
func (me TxDatabase) ScanVal(i interface{}, query string, args ...interface{}) (bool, error) {
	exec := newExec(me, nil, query, args...)
	return exec.ScanVal(i)
}

//COMMIT the transaction
func (me TxDatabase) Commit() error {
	return me.Tx.Commit()
}

//ROLLBACK the transaction
func (me TxDatabase) Rollback() error {
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
func (me TxDatabase) Wrap(fn func() error) error {
	if err := fn(); err != nil {
		if rollbackErr := me.Rollback(); rollbackErr != nil {
			return rollbackErr
		}
		return err
	}
	return me.Commit()
}
