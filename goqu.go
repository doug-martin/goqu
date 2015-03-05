/*
goqu is an expressive SQL builder

goqu was built with the following goals:

    1. Make the generation of SQL easy and enjoyable
    2. Provide a DSL that accounts for the common SQL expressions, NOT every nuance for each database.
    3. Allow users to use SQL when desired
    4. Provide a simple query API for scanning rows
    5. Allow the user to use the native sql.Db methods when desired

Features

goqu was comes with many features but here are a few of the more notable ones

    1. Query Builder
    2. Parameter interpolation (e.g SELECT * FROM "items" WHERE "id" = ? -> SELECT * FROM "items" WHERE "id" = 1)
    3. Built from the ground up with adapters in mind
    4. Insert, Multi Insert, Update, and Delete support
    5. Scanning of rows to struct[s] or primitive value[s]

While goqu may support the scanning of rows into structs it is not intended to be used as an ORM if you are looking for common ORM features like associations,
or hooks I would recommend looking at some of the great ORM libraries such as https://github.com/jinzhu/gorm

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

     sql, _ := db.From("user").Where(gq.I("password").IsNotNull()).Sql()
     fmt.Println(sql) //SELECT * FROM "user" WHERE "password" IS NOT NULL

     sql, args, _ := db.From("user").Where(gq.I("id").Eq(10)).ToSql(true)
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
*/
package goqu
