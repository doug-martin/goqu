package goqu_test

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/doug-martin/goqu/v9"
)

func ExampleDatabase_Begin() {
	db := getDB()

	tx, err := db.Begin()
	if err != nil {
		fmt.Println("Error starting transaction", err.Error())
	}

	// use tx.From to get a dataset that will execute within this transaction
	update := tx.Update("goqu_user").
		Set(goqu.Record{"last_name": "Ucon"}).
		Where(goqu.Ex{"last_name": "Yukon"}).
		Returning("id").
		Executor()

	var ids []int64
	if err := update.ScanVals(&ids); err != nil {
		if rErr := tx.Rollback(); rErr != nil {
			fmt.Println("An error occurred while issuing ROLLBACK\n\t", rErr.Error())
		} else {
			fmt.Println("An error occurred while updating users ROLLBACK transaction\n\t", err.Error())
		}
		return
	}
	if err := tx.Commit(); err != nil {
		fmt.Println("An error occurred while issuing COMMIT\n\t", err.Error())
	} else {
		fmt.Printf("Updated users in transaction [ids:=%+v]", ids)
	}
	// Output:
	// Updated users in transaction [ids:=[1 2 3]]
}

func ExampleDatabase_BeginTx() {
	db := getDB()

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		fmt.Println("Error starting transaction", err.Error())
	}

	// use tx.From to get a dataset that will execute within this transaction
	update := tx.Update("goqu_user").
		Set(goqu.Record{"last_name": "Ucon"}).
		Where(goqu.Ex{"last_name": "Yukon"}).
		Returning("id").
		Executor()

	var ids []int64
	if err := update.ScanVals(&ids); err != nil {
		if rErr := tx.Rollback(); rErr != nil {
			fmt.Println("An error occurred while issuing ROLLBACK\n\t", rErr.Error())
		} else {
			fmt.Println("An error occurred while updating users ROLLBACK transaction\n\t", err.Error())
		}
		return
	}
	if err := tx.Commit(); err != nil {
		fmt.Println("An error occurred while issuing COMMIT\n\t", err.Error())
	} else {
		fmt.Printf("Updated users in transaction [ids:=%+v]", ids)
	}
	// Output:
	// Updated users in transaction [ids:=[1 2 3]]
}

func ExampleDatabase_WithTx() {
	db := getDB()
	var ids []int64
	if err := db.WithTx(func(tx *goqu.TxDatabase) error {
		// use tx.From to get a dataset that will execute within this transaction
		update := tx.Update("goqu_user").
			Where(goqu.Ex{"last_name": "Yukon"}).
			Returning("id").
			Set(goqu.Record{"last_name": "Ucon"}).
			Executor()

		return update.ScanVals(&ids)
	}); err != nil {
		fmt.Println("An error occurred in transaction\n\t", err.Error())
	} else {
		fmt.Printf("Updated users in transaction [ids:=%+v]", ids)
	}
	// Output:
	// Updated users in transaction [ids:=[1 2 3]]
}

func ExampleDatabase_Dialect() {
	db := getDB()

	fmt.Println(db.Dialect())

	// Output:
	// postgres
}

func ExampleDatabase_Exec() {
	db := getDB()

	_, err := db.Exec(`DROP TABLE "user_role"; DROP TABLE "goqu_user"`)
	if err != nil {
		fmt.Println("Error occurred while dropping tables", err.Error())
	}
	fmt.Println("Dropped tables user_role and goqu_user")
	// Output:
	// Dropped tables user_role and goqu_user
}

func ExampleDatabase_ExecContext() {
	db := getDB()
	d := time.Now().Add(50 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), d)
	defer cancel()
	_, err := db.ExecContext(ctx, `DROP TABLE "user_role"; DROP TABLE "goqu_user"`)
	if err != nil {
		fmt.Println("Error occurred while dropping tables", err.Error())
	}
	fmt.Println("Dropped tables user_role and goqu_user")
	// Output:
	// Dropped tables user_role and goqu_user
}

func ExampleDatabase_From() {
	db := getDB()
	var names []string

	if err := db.From("goqu_user").Select("first_name").ScanVals(&names); err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("Fetched Users names:", names)
	}
	// Output:
	// Fetched Users names: [Bob Sally Vinita John]
}
