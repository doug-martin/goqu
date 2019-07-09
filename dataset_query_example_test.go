package goqu_test

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/doug-martin/goqu/v7"
	_ "github.com/doug-martin/goqu/v7/dialect/postgres"
	"github.com/lib/pq"
)

const schema = `
        DROP TABLE IF EXISTS "goqu_user";
        CREATE  TABLE "goqu_user" (
            "id" SERIAL PRIMARY KEY NOT NULL,
            "first_name" VARCHAR(45) NOT NULL,
			"last_name" VARCHAR(45) NOT NULL,
			"created" TIMESTAMP NOT NULL DEFAULT now()
		);
        INSERT INTO "goqu_user" ("first_name", "last_name") VALUES
            ('Bob', 'Yukon'),
            ('Sally', 'Yukon'),
			('Vinita', 'Yukon'),
			('John', 'Doe')
    `

const defaultDbURI = "postgres://postgres:@localhost:5435/goqupostgres?sslmode=disable"

var goquDb *goqu.Database

func getDb() *goqu.Database {
	if goquDb == nil {
		dbURI := os.Getenv("PG_URI")
		if dbURI == "" {
			dbURI = defaultDbURI
		}
		uri, err := pq.ParseURL(dbURI)
		if err != nil {
			panic(err)
		}
		pdb, err := sql.Open("postgres", uri)
		if err != nil {
			panic(err)
		}
		goquDb = goqu.New("postgres", pdb)
	}
	// reset the db
	if _, err := goquDb.Exec(schema); err != nil {
		panic(err)
	}
	return goquDb
}

func ExampleDataset_ScanStructs() {
	type User struct {
		FirstName string `db:"first_name"`
		LastName  string `db:"last_name"`
	}
	db := getDb()
	var users []User
	if err := db.From("goqu_user").ScanStructs(&users); err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Printf("\n%+v", users)

	users = users[0:0]
	if err := db.From("goqu_user").Select("first_name").ScanStructs(&users); err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Printf("\n%+v", users)

	// Output:
	// [{FirstName:Bob LastName:Yukon} {FirstName:Sally LastName:Yukon} {FirstName:Vinita LastName:Yukon} {FirstName:John LastName:Doe}]
	// [{FirstName:Bob LastName:} {FirstName:Sally LastName:} {FirstName:Vinita LastName:} {FirstName:John LastName:}]
}

func ExampleDataset_ScanStructs_prepared() {
	type User struct {
		FirstName string `db:"first_name"`
		LastName  string `db:"last_name"`
	}
	db := getDb()

	ds := db.From("goqu_user").
		Prepared(true).
		Where(goqu.Ex{
			"last_name": "Yukon",
		})

	var users []User
	if err := ds.ScanStructs(&users); err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Printf("\n%+v", users)

	// Output:
	// [{FirstName:Bob LastName:Yukon} {FirstName:Sally LastName:Yukon} {FirstName:Vinita LastName:Yukon}]
}

func ExampleDataset_ScanStruct() {
	type User struct {
		FirstName string `db:"first_name"`
		LastName  string `db:"last_name"`
	}
	db := getDb()
	findUserByName := func(name string) {
		var user User
		ds := db.From("goqu_user").Where(goqu.C("first_name").Eq(name))
		found, err := ds.ScanStruct(&user)
		switch {
		case err != nil:
			fmt.Println(err.Error())
		case !found:
			fmt.Printf("No user found for first_name %s\n", name)
		default:
			fmt.Printf("Found user: %+v\n", user)
		}
	}

	findUserByName("Bob")
	findUserByName("Zeb")

	// Output:
	// Found user: {FirstName:Bob LastName:Yukon}
	// No user found for first_name Zeb
}

func ExampleDataset_ScanVals() {
	var ids []int64
	if err := getDb().From("goqu_user").Select("id").ScanVals(&ids); err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Printf("UserIds = %+v", ids)

	// Output:
	// UserIds = [1 2 3 4]
}

func ExampleDataset_ScanVal() {

	db := getDb()
	findUserIDByName := func(name string) {
		var id int64
		ds := db.From("goqu_user").
			Select("id").
			Where(goqu.C("first_name").Eq(name))

		found, err := ds.ScanVal(&id)
		switch {
		case err != nil:
			fmt.Println(err.Error())
		case !found:
			fmt.Printf("No id found for user %s", name)
		default:
			fmt.Printf("\nFound userId: %+v\n", id)
		}
	}

	findUserIDByName("Bob")
	findUserIDByName("Zeb")
	// Output:
	// Found userId: 1
	// No id found for user Zeb
}

func ExampleDataset_Count() {

	if count, err := getDb().From("goqu_user").Count(); err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Printf("\nCount:= %d", count)
	}

	// Output:
	// Count:= 4
}

func ExampleDataset_Pluck() {
	var lastNames []string
	if err := getDb().From("goqu_user").Pluck(&lastNames, "last_name"); err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Printf("LastNames := %+v", lastNames)

	// Output:
	// LastNames := [Yukon Yukon Yukon Doe]
}

func ExampleDataset_Insert_recordExec() {
	db := getDb()
	insert := db.From("goqu_user").Insert(
		goqu.Record{"first_name": "Jed", "last_name": "Riley", "created": time.Now()},
	)
	if _, err := insert.Exec(); err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("Inserted 1 user")
	}

	users := []goqu.Record{
		{"first_name": "Greg", "last_name": "Farley", "created": time.Now()},
		{"first_name": "Jimmy", "last_name": "Stewart", "created": time.Now()},
		{"first_name": "Jeff", "last_name": "Jeffers", "created": time.Now()},
	}
	if _, err := db.From("goqu_user").Insert(users).Exec(); err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Printf("Inserted %d users", len(users))
	}

	// Output:
	// Inserted 1 user
	// Inserted 3 users
}

func ExampleDataset_Insert_recordReturning() {
	db := getDb()

	type User struct {
		ID        sql.NullInt64 `db:"id"`
		FirstName string        `db:"first_name"`
		LastName  string        `db:"last_name"`
		Created   time.Time     `db:"created"`
	}

	insert := db.From("goqu_user").Returning(goqu.C("id")).Insert(
		goqu.Record{"first_name": "Jed", "last_name": "Riley", "created": time.Now()},
	)
	var id int64
	if _, err := insert.ScanVal(&id); err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Printf("Inserted 1 user id:=%d\n", id)
	}

	insert = db.From("goqu_user").Returning(goqu.Star()).Insert([]goqu.Record{
		{"first_name": "Greg", "last_name": "Farley", "created": time.Now()},
		{"first_name": "Jimmy", "last_name": "Stewart", "created": time.Now()},
		{"first_name": "Jeff", "last_name": "Jeffers", "created": time.Now()},
	})
	var insertedUsers []User
	if err := insert.ScanStructs(&insertedUsers); err != nil {
		fmt.Println(err.Error())
	} else {
		for _, u := range insertedUsers {
			fmt.Printf("Inserted user: [ID=%d], [FirstName=%+s] [LastName=%s]\n", u.ID.Int64, u.FirstName, u.LastName)
		}

	}

	// Output:
	// Inserted 1 user id:=5
	// Inserted user: [ID=6], [FirstName=Greg] [LastName=Farley]
	// Inserted user: [ID=7], [FirstName=Jimmy] [LastName=Stewart]
	// Inserted user: [ID=8], [FirstName=Jeff] [LastName=Jeffers]
}

func ExampleDataset_Insert_scanStructs() {
	db := getDb()

	type User struct {
		ID        sql.NullInt64 `db:"id" goqu:"skipinsert"`
		FirstName string        `db:"first_name"`
		LastName  string        `db:"last_name"`
		Created   time.Time     `db:"created"`
	}

	insert := db.From("goqu_user").Returning("id").Insert(
		User{FirstName: "Jed", LastName: "Riley"},
	)
	var id int64
	if _, err := insert.ScanVal(&id); err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Printf("Inserted 1 user id:=%d\n", id)
	}

	insert = db.From("goqu_user").Returning(goqu.Star()).Insert([]User{
		{FirstName: "Greg", LastName: "Farley", Created: time.Now()},
		{FirstName: "Jimmy", LastName: "Stewart", Created: time.Now()},
		{FirstName: "Jeff", LastName: "Jeffers", Created: time.Now()},
	})
	var insertedUsers []User
	if err := insert.ScanStructs(&insertedUsers); err != nil {
		fmt.Println(err.Error())
	} else {
		for _, u := range insertedUsers {
			fmt.Printf("Inserted user: [ID=%d], [FirstName=%+s] [LastName=%s]\n", u.ID.Int64, u.FirstName, u.LastName)
		}

	}

	// Output:
	// Inserted 1 user id:=5
	// Inserted user: [ID=6], [FirstName=Greg] [LastName=Farley]
	// Inserted user: [ID=7], [FirstName=Jimmy] [LastName=Stewart]
	// Inserted user: [ID=8], [FirstName=Jeff] [LastName=Jeffers]
}

func ExampleDataset_Update() {
	db := getDb()
	update := db.From("goqu_user").
		Where(goqu.C("first_name").Eq("Bob")).
		Update(goqu.Record{"first_name": "Bobby"})

	if r, err := update.Exec(); err != nil {
		fmt.Println(err.Error())
	} else {
		c, _ := r.RowsAffected()
		fmt.Printf("Updated %d users", c)
	}

	// Output:
	// Updated 1 users
}

func ExampleDataset_Update_returning() {
	db := getDb()
	var ids []int64
	update := db.From("goqu_user").
		Where(goqu.Ex{"last_name": "Yukon"}).
		Returning("id").
		Update(goqu.Record{"last_name": "ucon"})
	if err := update.ScanVals(&ids); err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Printf("Updated users with ids %+v", ids)
	}

	// Output:
	// Updated users with ids [1 2 3]
}
func ExampleDataset_Delete() {
	db := getDb()

	de := db.From("goqu_user").
		Where(goqu.Ex{"first_name": "Bob"}).
		Delete()
	if r, err := de.Exec(); err != nil {
		fmt.Println(err.Error())
	} else {
		c, _ := r.RowsAffected()
		fmt.Printf("Deleted %d users", c)
	}

	// Output:
	// Deleted 1 users
}

func ExampleDataset_Delete_returning() {
	db := getDb()

	de := db.From("goqu_user").
		Where(goqu.C("last_name").Eq("Yukon")).
		Returning(goqu.C("id")).
		Delete()

	var ids []int64
	if err := de.ScanVals(&ids); err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Printf("Deleted users [ids:=%+v]", ids)
	}

	// Output:
	// Deleted users [ids:=[1 2 3]]
}
