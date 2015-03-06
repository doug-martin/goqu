/*
goqu is an expressive SQL builder.

      __ _  ___   __ _ _   _
     / _` |/ _ \ / _` | | | |
    | (_| | (_) | (_| | |_| |
     \__, |\___/ \__, |\__,_|
     |___/          |_|


goqu was built with the following goals:

    1. Make the generation of SQL easy and enjoyable
    2. Provide a DSL that accounts for the common SQL expressions, NOT every nuance for each database.
    3. Allow users to use SQL when desired
    4. Provide a simple query API for scanning rows
    5. Allow the user to use the native sql.Db methods when desired

Features

goqu comes with many features but here are a few of the more notable ones

    1. Query Builder
    2. Parameter interpolation (e.g SELECT * FROM "items" WHERE "id" = ? -> SELECT * FROM "items" WHERE "id" = 1)
    3. Built from the ground up with adapters in mind
    4. Insert, Multi Insert, Update, and Delete support
    5. Scanning of rows to struct[s] or primitive value[s]

While goqu may support the scanning of rows into structs it is not intended to be used as an ORM if you are looking for common ORM features like associations,
or hooks I would recommend looking at some of the great ORM packages such as:
    * https://github.com/jinzhu/gorm
    * https://github.com/eaigner/hood

Basics

In order to start using goqu with your database you need to load an adapter. We have included some adapters by default.

    1. Postgres - github.com/doug-martin/goqu/adapters/postgres
    2. MySQL - github.com/doug-martin/goqu/adapters/mysql

Adapters in goqu work the same way as a driver with the database in that they register themselves with goqu once loaded.

    import (
          "database/sql"
          "github.com/doug-martin/goqu"
          _ "github.com/doug-martin/goqu/adapters/postgres"
          _ "github.com/lib/pq"
      )
Notice that we imported the adapter and driver for side effect only.

Once you have your adapter and driver loaded you can create a goqu.Database instance

      pgDb, err := sql.Open("postgres", "user=postgres dbname=goqupostgres sslmode=disable ")
      if err != nil {
          panic(err.Error())
      }
      db := goqu.New("postgres", pgDb)
Once you have your goqu.Database you can build your SQL and it will be formatted appropiately for the provided dialect.

     sql, _ := db.From("user").Where(goqu.I("password").IsNotNull()).Sql()
     fmt.Println(sql) //SELECT * FROM "user" WHERE "password" IS NOT NULL

     sql, args, _ := db.From("user").Where(goqu.I("id").Eq(10)).ToSql(true)
     fmt.Println(sql) //SELECT * FROM "user" WHERE "id" = $1

Expressions

goqu provides a DSL for generating the SQL however the Dataset only provides the the different clause methods (e.g. Where, From, Select), most of these clause methods accept Expressions(with a few exceptions) which are the building blocks for your SQL statement, you can think of them as fragments of SQL.

The entry points for expressions are:

* I() - An Identifier represents a schema, table, or column or any combination
        goqu.I("my_schema.table.col")
        goqu.I("table.col")
        goqu.I("col")
If you look at the IdentiferExpression it implements many of your common sql operations that you would perform.
    goqu.I("col").Eq(10)
    goqu.I("col").In([]int64{1,2,3,4})
    goqu.I("col").Like(regexp.MustCompile("(a|b)")
    goqu.I("col").IsNull()
Please see the exmaples for I() to see more in depth examples

* L() - An SQL literal. You may find yourself in a situation where an IdentifierExpression cannot expression an SQL fragment that your database supports. In that case you can use a LiteralExpression
        goqu.L(`"col"::TEXT = ""other_col"::text`)
You can also use placeholders in your literal. When using the LiteralExpressions placeholders are normalized to the ? character and will be transformed to the correct placeholder for your adapter (e.g. ? - $1 in postgres)
	    goqu.L("col IN (?, ?, ?)", "a", "b", "c")
Putting it together
   db.From("test").Where(
      goqu.I("col").Eq(10),
      goqu.L(`"col"::TEXT = ""other_col"::text`),
   )
Both the Identifier and Literal expressions will be ANDed together by default.
You may however want to have your expressions ORed together you can use the Or() function to create an ExpressionList
   db.From("test").Where(
      Or(
         goqu.I("col").Eq(10),
         goqu.L(`"col"::TEXT = ""other_col"::text`),
      ),
   )
You can also use Or and the And function in tandem which will give you control not only over how the Expressions are joined together, but also how they are grouped
   db.From("test").Where(
      Or(
         I("a").Gt(10),
         And(
            I("b").Eq(100),
            I("c").Neq("test"),
         ),
      ),
   ) //SELECT * FROM "test" WHERE (("a" > 10) OR (("b" = 100) AND ("c" != 'test')))

Complex Example

    db.From("test").
        Select(goqu.COUNT("*")).
        InnerJoin(goqu.I("test2"), goqu.On(goqu.I("test.fkey").Eq(goqu.I("test2.id")))).
        LeftJoin(goqu.I("test3"), goqu.On(goqu.I("test2.fkey").Eq(goqu.I("test3.id")))).
        Where(
            goqu.I("test.name").Like(regexp.MustCompile("(a|b)\\w+")),
            goqu.I("test2.amount").IsNotNull(),
            goqu.Or(
			    goqu.I("test3.id").IsNull(),
			    goqu.I("test3.status").In("passed", "active", "registered"),
		    ),
        ).
        Order(goqu.I("test.created").Desc().NullsLast()).
        GroupBy(goqu.I("test.user_id")).
        Having(goqu.AVG("test3.age").Gt(10))

    //SELECT COUNT(*)
    //FROM "test"
    //  INNER JOIN "test2" ON ("test"."fkey" = "test2"."id")
    //  LEFT JOIN "test3" ON ("test2"."fkey" = "test3"."id")
    //WHERE (
    //  ("test"."name" ~ '(a|b)\w+') AND
    //  ("test2"."amount" IS NOT NULL) AND
    //  (
    //      ("test3"."id" IS NULL) OR
    //      ("test3"."status" IN ('passed', 'active', 'registered'))
    //  )
    //)
    //GROUP BY "test"."user_id"
    //HAVING (AVG("test3"."age") > 10)
    //ORDER BY "test"."created" DESC NULLS LAST

Querying

goqu also has basic query support through the use of either the Database or the Dataset.

Dataset

* ScanStructs - scans rows into a slice of structs
    var users []User
    if err := db.From("user").ScanStructs(&users){
        fmt.Println(err.Error())
        return
    }
    fmt.Printf("\n%+v", users)

* ScanStruct - scans a row into a slice a struct, returns false if a row wasnt found
    var user User
    found, err := db.From("user").ScanStruct(&user)
    if err != nil{
        fmt.Println(err.Error())
        return
    }
    if !found{
        fmt.Println("No user found")
    }else{
        fmt.Printf("\nFound user: %+v", user)
    }

* ScanVals - scans a rows of 1 column into a slice of primitive values
    var ids []int64
    if err := db.From("user").Select("id").ScanVals(&ids){
        fmt.Println(err.Error())
        return
    }
    fmt.Printf("\n%+v", ids)

* ScanVal - scans a row of 1 column into a primitive value, returns false if a row wasnt found. **Note** when using the dataset a `LIMIT` of 1 is automatically applied.
    var id int64
    found, err := db.From("user").Select("id").ScanVal(&id)
    if err != nil{
        fmt.Println(err.Error())
        return
    }
    if !found{
        fmt.Println("No id found")
    }else{
        fmt.Printf("\nFound id: %d", id)
    }

* Count - Returns the count for the current query
    count, err := db.From("user").Count()
    if err != nil{
        fmt.Println(err.Error())
        return
    }
    fmt.Printf("\nCount:= %d", count)

* Pluck - Selects a single column and stores the results into a slice of primitive values
    var ids []int64
    if err := db.From("user").Pluck(&ids, "id"); err != nil{
        fmt.Println(err.Error())
        return
    }
    fmt.Printf("\nIds := %+v", ids)

* Insert - Creates an `INSERT` statement and returns a CrudExec to execute the statement
    insert := db.From("user").Insert(goqu.Record{"first_name": "Bob", "last_name":"Yukon", "created": time.Now()})
    if _, err := insert.Exec(); err != nil{
        fmt.Println(err.Error())
        return
    }
Insert will also handle multi inserts if supported by the database
    users := []goqu.Record{
        {"first_name": "Bob", "last_name":"Yukon", "created": time.Now()},
        {"first_name": "Sally", "last_name":"Yukon", "created": time.Now()},
        {"first_name": "Jimmy", "last_name":"Yukon", "created": time.Now()},
    }
    if _, err := db.From("user").Insert(users).Exec(); err != nil{
        fmt.Println(err.Error())
        return
    }
If your database supports the `RETURN` clause you can also use the different Scan methods to get results
    var ids []int64
    users := []goqu.Record{
        {"first_name": "Bob", "last_name":"Yukon", "created": time.Now()},
        {"first_name": "Sally", "last_name":"Yukon", "created": time.Now()},
        {"first_name": "Jimmy", "last_name":"Yukon", "created": time.Now()},
    }
    if err := db.From("user").Returning(goqu.I("id")).Insert(users).ScanVals(&ids); err != nil{
        fmt.Println(err.Error())
        return
    }

* Update - Creates an `UPDATE` statement and returns a CrudExec to execute the statement
    update := db.From("user").
        Where(goqu.I("status").Eq("inactive")).
        Update(goqu.Record{"password": nil, "updated": time.Now()})
    if _, err := update.Exec(); err != nil{
        fmt.Println(err.Error())
        return
    }
If your database supports the `RETURN` clause you can also use the different Scan methods to get results
    var ids []int64
    update := db.From("user").
        Where(goqu.I("status").Eq("inactive")).
        Returning(goqu.I("id")).
        Update(goqu.Record{"password": nil, "updated": time.Now()})
    if err := update.ScanVals(&ids); err != nil{
        fmt.Println(err.Error())
        return
    }

* Delete - Creates an `DELETE` statement and returns a CrudExec to execute the statement
    delete := db.From("invoice").
        Where(goqu.I("status").Eq("paid")).
        Delete()
    if _, err := delete.Exec(); err != nil{
        fmt.Println(err.Error())
        return
    }
If your database supports the `RETURN` clause you can also use the different Scan methods to get results
    var ids []int64
    delete := db.From("invoice").
        Where(goqu.I("status").Eq("paid")).
        Returning(goqu.I("id")).
        Delete()
    if err := delete.ScanVals(&ids); err != nil{
        fmt.Println(err.Error())
        return
    }

Database

The Database also allows you to execute queries but expects raw SQL to execute. The supported methods are
    * Exec - http://godoc.org/github.com/doug-martin/goqu#Database.Exec
    * Prepare - http://godoc.org/github.com/doug-martin/goqu#Database.Prepare
    * Query - http://godoc.org/github.com/doug-martin/goqu#Database.Query
    * QueryRow - http://godoc.org/github.com/doug-martin/goqu#Database.QueryRow
    * ScanStructs - http://godoc.org/github.com/doug-martin/goqu#Database.ScanStructs
    * ScanStruct - http://godoc.org/github.com/doug-martin/goqu#Database.ScanStruct
    * ScanVals - http://godoc.org/github.com/doug-martin/goqu#Database.ScanVals
    * ScanVal - http://godoc.org/github.com/doug-martin/goqu#Database.ScanVal
    * Begin - http://godoc.org/github.com/doug-martin/goqu#Database.Begin


Transactions

goqu has builtin support for transactions to make the use of the Datasets and querying seamless
    tx, err := db.Begin()
    if err != nil{
       return err
    }
    //use tx.From to get a dataset that will execute within this transaction
    update := tx.From("user").Where(goqu.I("password").IsNull()).Update(goqu.Record{"status": "inactive"})
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

The TxDatabase also has all methods that the Database has along with
    * Commit - http://godoc.org/github.com/doug-martin/goqu#TxDatabase.Commit
    * Rollback - http://godoc.org/github.com/doug-martin/goqu#TxDatabase.Rollback
    * Wrap - http://godoc.org/github.com/doug-martin/goqu#TxDatabase.Wrap

Wrap

The TxDatabase.Wrap is a convience method for automatically handling `COMMIT` and `ROLLBACK`
    tx, err := db.Begin()
    if err != nil{
       return err
    }
    err = tx.Wrap(func() error{
      update := tx.From("user").Where(goqu.I("password").IsNull()).Update(goqu.Record{"status": "inactive"})
      if _, err = update.Exec(); err != nil{
          return err
      }
      return nil
    })
    //err will be the original error from the update statement, unless there was an error executing ROLLBACK
    if err != nil{
        return err
    }

Logging

To enable trace logging of SQL statments use the Database.Logger method to set your logger.
    NOTE The logger must implement the [`Logger`](http://godoc.org/github.com/doug-martin/goqu/#Logger) interface
    NOTE If you start a transaction using a database your set a logger on the transaction will inherit that logger automatically

Adapters

Adapters in goqu are the foundation of building the correct SQL for each DB dialect.

When creating your adapters you must register your adapter with goqu.RegisterAdapter. This method requires 2 arguments.
   1. Dialect - The dialect for your adapter.
   2. DatasetAdapterFactory - This is a factory function that will return a new goqu.Adapter  used to create the the dialect specific SQL.

Between most dialects there is a large portion of shared syntax, for this reason we have a DefaultAdapter that can be used as a base for any new Dialect specific adapter.
In fact for most use cases you will not have to override any methods but instead just override the default values as documented for DefaultAdapter.

For example the code for the postgres adapter is fairly short.

    package postgres

    import (
	    "github.com/doug-martin/goqu"
    )

    //postgres requires a $ placeholder for prepared statements
    const placeholder_rune = '$'

    func newDatasetAdapter(ds *goqu.Dataset) goqu.Adapter {
	    ret := goqu.NewDefaultAdapter(ds).(*goqu.DefaultAdapter)

	    //override the settings required
	    ret.PlaceHolderRune = placeholder_rune
	    //postgres requires a paceholder number (e.g. $1)
	    ret.IncludePlaceholderNum = true

	    return ret
    }

    func init() {
        //register our adapter with goqu
	    goqu.RegisterAdapter("postgres", newDatasetAdapter)
    }

If you are looking to write your own adapter take a look at the postgres or mysql adapter located at https://github.com/doug-martin/goqu/tree/master/adapters.

Contributions

I am always welcoming contributions of any type. Please open an issue or create a PR if you find an issue with any of the following.
    * An issue with Documentation
    * You found the documentation lacking in some way
If you have an issue with the package please include the following
    * The dialect you are using
    * A description of the problem
    * A short example of how to reproduce (if applicable)
Without those basics it can be difficult to reproduce your issue locally. You may be asked for more information but that is a good starting point.

New Features

New features and/or enhancements are great and I encourage you to either submit a PR or create an issue. In both cases include the following as the need/requirement may not be readily apparent.
    1. The use case
    2. A short example
If you are issuing a PR also also include the following
    1. Tests - otherwise the PR will not be merged
    2. Documentation - otherwise the PR will not be merged
    3. Examples - [If applicable] see example_test.go for examples
If you find an issue you want to work on please comment on it letting other people know you are looking at it and I will assign the issue to you.

If want to work on an issue but dont know where to start just leave a comment and I'll be more than happy to point you in the right direction.

*/
package goqu
