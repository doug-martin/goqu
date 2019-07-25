<a name="database"></a>
### Database

The Database also allows you to execute queries but expects raw SQL to execute. The supported methods are

* [`Exec`](http://godoc.org/github.com/doug-martin/goqu#Database.Exec)
* [`Prepare`](http://godoc.org/github.com/doug-martin/goqu#Database.Prepare)
* [`Query`](http://godoc.org/github.com/doug-martin/goqu#Database.Query)
* [`QueryRow`](http://godoc.org/github.com/doug-martin/goqu#Database.QueryRow)
* [`ScanStructs`](http://godoc.org/github.com/doug-martin/goqu#Database.ScanStructs)
* [`ScanStruct`](http://godoc.org/github.com/doug-martin/goqu#Database.ScanStruct)
* [`ScanVals`](http://godoc.org/github.com/doug-martin/goqu#Database.ScanVals)
* [`ScanVal`](http://godoc.org/github.com/doug-martin/goqu#Database.ScanVal)
* [`Begin`](http://godoc.org/github.com/doug-martin/goqu#Database.Begin)

<a name="transactions"></a>
### Transactions

`goqu` has builtin support for transactions to make the use of the Datasets and querying seamless

```go
tx, err := db.Begin()
if err != nil{
   return err
}
//use tx.From to get a dataset that will execute within this transaction
update := tx.From("user").
    Where(goqu.Ex("password": nil}).
    Update(goqu.Record{"status": "inactive"})
if _, err = update.Exec(); err != nil{
    if rErr := tx.Rollback(); rErr != nil{
        return rErr
    }
    return err
}
if err = tx.Commit(); err != nil{
    return err
}
return
```

The [`TxDatabase`](http://godoc.org/github.com/doug-martin/goqu/#TxDatabase)  also has all methods that the [`Database`](http://godoc.org/github.com/doug-martin/goqu/#Database) has along with

* [`Commit`](http://godoc.org/github.com/doug-martin/goqu#TxDatabase.Commit)
* [`Rollback`](http://godoc.org/github.com/doug-martin/goqu#TxDatabase.Rollback)
* [`Wrap`](http://godoc.org/github.com/doug-martin/goqu#TxDatabase.Wrap)

#### Wrap

The [`TxDatabase.Wrap`](http://godoc.org/github.com/doug-martin/goqu/#TxDatabase.Wrap) is a convience method for automatically handling `COMMIT` and `ROLLBACK`

```go
tx, err := db.Begin()
if err != nil{
   return err
}
err = tx.Wrap(func() error{
  update := tx.From("user").
      Where(goqu.Ex("password": nil}).
      Update(goqu.Record{"status": "inactive"})
  return update.Exec()
})
//err will be the original error from the update statement, unless there was an error executing ROLLBACK
if err != nil{
    return err
}
```

<a name="logging"></a>
## Logging

To enable trace logging of SQL statements use the [`Database.Logger`](http://godoc.org/github.com/doug-martin/goqu/#Database.Logger) method to set your logger.

**NOTE** The logger must implement the [`Logger`](http://godoc.org/github.com/doug-martin/goqu/#Logger) interface

**NOTE** If you start a transaction using a database your set a logger on the transaction will inherit that logger automatically

