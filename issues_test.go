package goqu_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/doug-martin/goqu/v9"
	"github.com/stretchr/testify/suite"
)

type githubIssuesSuite struct {
	suite.Suite
}

func (gis *githubIssuesSuite) AfterTest(suiteName, testName string) {
	goqu.SetColumnRenameFunction(strings.ToLower)
}

// Test for https://github.com/doug-martin/goqu/issues/49
func (gis *githubIssuesSuite) TestIssue49() {
	dialect := goqu.Dialect("default")

	filters := goqu.Or()
	sql, args, err := dialect.From("table").Where(filters).ToSQL()
	gis.NoError(err)
	gis.Empty(args)
	gis.Equal(`SELECT * FROM "table"`, sql)

	sql, args, err = dialect.From("table").Where(goqu.Ex{}).ToSQL()
	gis.NoError(err)
	gis.Empty(args)
	gis.Equal(`SELECT * FROM "table"`, sql)

	sql, args, err = dialect.From("table").Where(goqu.ExOr{}).ToSQL()
	gis.NoError(err)
	gis.Empty(args)
	gis.Equal(`SELECT * FROM "table"`, sql)
}

// Test for https://github.com/doug-martin/goqu/issues/115
func (gis *githubIssuesSuite) TestIssue115() {

	type TestStruct struct {
		Field string
	}
	goqu.SetColumnRenameFunction(func(col string) string {
		return ""
	})

	_, _, err := goqu.Insert("test").Rows(TestStruct{Field: "hello"}).ToSQL()
	gis.EqualError(err, `goqu: a empty identifier was encountered, please specify a "schema", "table" or "column"`)
}

// Test for https://github.com/doug-martin/goqu/issues/118
func (gis *githubIssuesSuite) TestIssue118_withEmbeddedStructWithoutExportedFields() {
	// struct is in a custom package
	type SimpleRole struct {
		sync.RWMutex
		permissions []string // nolint:structcheck,unused
	}

	// .....

	type Role struct {
		*SimpleRole

		ID        string    `json:"id" db:"id" goqu:"skipinsert"`
		Key       string    `json:"key" db:"key"`
		Name      string    `json:"name" db:"name"`
		CreatedAt time.Time `json:"-" db:"created_at" goqu:"skipinsert"`
	}

	rUser := &Role{
		Key:  `user`,
		Name: `User role`,
	}

	sql, arg, err := goqu.Insert(`rbac_roles`).
		Returning(goqu.C(`id`)).
		Rows(rUser).
		ToSQL()
	gis.NoError(err)
	gis.Empty(arg)
	gis.Equal(`INSERT INTO "rbac_roles" ("key", "name") VALUES ('user', 'User role') RETURNING "id"`, sql)

	sql, arg, err = goqu.Update(`rbac_roles`).
		Returning(goqu.C(`id`)).
		Set(rUser).
		ToSQL()
	gis.NoError(err)
	gis.Empty(arg)
	gis.Equal(
		`UPDATE "rbac_roles" SET "created_at"='0001-01-01T00:00:00Z',"id"='',"key"='user',"name"='User role' RETURNING "id"`,
		sql,
	)

	rUser = &Role{
		SimpleRole: &SimpleRole{},
		Key:        `user`,
		Name:       `User role`,
	}

	sql, arg, err = goqu.Insert(`rbac_roles`).
		Returning(goqu.C(`id`)).
		Rows(rUser).
		ToSQL()
	gis.NoError(err)
	gis.Empty(arg)
	gis.Equal(`INSERT INTO "rbac_roles" ("key", "name") VALUES ('user', 'User role') RETURNING "id"`, sql)

	sql, arg, err = goqu.Update(`rbac_roles`).
		Returning(goqu.C(`id`)).
		Set(rUser).
		ToSQL()
	gis.NoError(err)
	gis.Empty(arg)
	gis.Equal(
		`UPDATE "rbac_roles" SET `+
			`"created_at"='0001-01-01T00:00:00Z',"id"='',"key"='user',"name"='User role' RETURNING "id"`,
		sql,
	)

}

// Test for https://github.com/doug-martin/goqu/issues/118
func (gis *githubIssuesSuite) TestIssue118_withNilEmbeddedStructWithExportedFields() {
	// struct is in a custom package
	type SimpleRole struct {
		sync.RWMutex
		permissions []string // nolint:structcheck,unused
		IDStr       string
	}

	// .....

	type Role struct {
		*SimpleRole

		ID        string    `json:"id" db:"id" goqu:"skipinsert"`
		Key       string    `json:"key" db:"key"`
		Name      string    `json:"name" db:"name"`
		CreatedAt time.Time `json:"-" db:"created_at" goqu:"skipinsert"`
	}

	rUser := &Role{
		Key:  `user`,
		Name: `User role`,
	}
	sql, arg, err := goqu.Insert(`rbac_roles`).
		Returning(goqu.C(`id`)).
		Rows(rUser).
		ToSQL()
	gis.NoError(err)
	gis.Empty(arg)
	// it should not insert fields on nil embedded pointers
	gis.Equal(`INSERT INTO "rbac_roles" ("key", "name") VALUES ('user', 'User role') RETURNING "id"`, sql)

	sql, arg, err = goqu.Update(`rbac_roles`).
		Returning(goqu.C(`id`)).
		Set(rUser).
		ToSQL()
	gis.NoError(err)
	gis.Empty(arg)
	// it should not insert fields on nil embedded pointers
	gis.Equal(
		`UPDATE "rbac_roles" SET "created_at"='0001-01-01T00:00:00Z',"id"='',"key"='user',"name"='User role' RETURNING "id"`,
		sql,
	)

	rUser = &Role{
		SimpleRole: &SimpleRole{},
		Key:        `user`,
		Name:       `User role`,
	}
	sql, arg, err = goqu.Insert(`rbac_roles`).
		Returning(goqu.C(`id`)).
		Rows(rUser).
		ToSQL()
	gis.NoError(err)
	gis.Empty(arg)
	// it should not insert fields on nil embedded pointers
	gis.Equal(
		`INSERT INTO "rbac_roles" ("idstr", "key", "name") VALUES ('', 'user', 'User role') RETURNING "id"`,
		sql,
	)

	sql, arg, err = goqu.Update(`rbac_roles`).
		Returning(goqu.C(`id`)).
		Set(rUser).
		ToSQL()
	gis.NoError(err)
	gis.Empty(arg)
	// it should not insert fields on nil embedded pointers
	gis.Equal(
		`UPDATE "rbac_roles" SET `+
			`"created_at"='0001-01-01T00:00:00Z',"id"='',"idstr"='',"key"='user',"name"='User role' RETURNING "id"`,
		sql,
	)

}

// Test for https://github.com/doug-martin/goqu/issues/118
func (gis *githubIssuesSuite) TestIssue140() {

	sql, arg, err := goqu.Insert(`test`).Returning().ToSQL()
	gis.NoError(err)
	gis.Empty(arg)
	gis.Equal(`INSERT INTO "test" DEFAULT VALUES`, sql)

	sql, arg, err = goqu.Update(`test`).Set(goqu.Record{"a": "b"}).Returning().ToSQL()
	gis.NoError(err)
	gis.Empty(arg)
	gis.Equal(
		`UPDATE "test" SET "a"='b'`,
		sql,
	)

	sql, arg, err = goqu.Delete(`test`).Returning().ToSQL()
	gis.NoError(err)
	gis.Empty(arg)
	gis.Equal(
		`DELETE FROM "test"`,
		sql,
	)

	sql, arg, err = goqu.Insert(`test`).Returning(nil).ToSQL()
	gis.NoError(err)
	gis.Empty(arg)
	gis.Equal(`INSERT INTO "test" DEFAULT VALUES`, sql)

	sql, arg, err = goqu.Update(`test`).Set(goqu.Record{"a": "b"}).Returning(nil).ToSQL()
	gis.NoError(err)
	gis.Empty(arg)
	gis.Equal(
		`UPDATE "test" SET "a"='b'`,
		sql,
	)

	sql, arg, err = goqu.Delete(`test`).Returning(nil).ToSQL()
	gis.NoError(err)
	gis.Empty(arg)
	gis.Equal(
		`DELETE FROM "test"`,
		sql,
	)

}

// Test for https://github.com/doug-martin/goqu/issues/164
func (gis *githubIssuesSuite) TestIssue164() {
	insertDs := goqu.Insert("foo").Rows(goqu.Record{"user_id": 10}).Returning("id")

	ds := goqu.From("bar").
		With("ins", insertDs).
		Select("bar_name").
		Where(goqu.Ex{"bar.user_id": goqu.I("ins.user_id")})

	sql, args, err := ds.ToSQL()
	gis.NoError(err)
	gis.Empty(args)
	gis.Equal(
		`WITH ins AS (INSERT INTO "foo" ("user_id") VALUES (10) RETURNING "id") `+
			`SELECT "bar_name" FROM "bar" WHERE ("bar"."user_id" = "ins"."user_id")`,
		sql,
	)

	sql, args, err = ds.Prepared(true).ToSQL()
	gis.NoError(err)
	gis.Equal([]interface{}{int64(10)}, args)
	gis.Equal(
		`WITH ins AS (INSERT INTO "foo" ("user_id") VALUES (?) RETURNING "id")`+
			` SELECT "bar_name" FROM "bar" WHERE ("bar"."user_id" = "ins"."user_id")`,
		sql,
	)

	updateDs := goqu.Update("foo").Set(goqu.Record{"bar": "baz"}).Returning("id")

	ds = goqu.From("bar").
		With("upd", updateDs).
		Select("bar_name").
		Where(goqu.Ex{"bar.user_id": goqu.I("upd.user_id")})

	sql, args, err = ds.ToSQL()
	gis.NoError(err)
	gis.Empty(args)
	gis.Equal(
		`WITH upd AS (UPDATE "foo" SET "bar"='baz' RETURNING "id") SELECT "bar_name" FROM "bar" WHERE ("bar"."user_id" = "upd"."user_id")`,
		sql,
	)

	sql, args, err = ds.Prepared(true).ToSQL()
	gis.NoError(err)
	gis.Equal([]interface{}{"baz"}, args)
	gis.Equal(
		`WITH upd AS (UPDATE "foo" SET "bar"=? RETURNING "id") SELECT "bar_name" FROM "bar" WHERE ("bar"."user_id" = "upd"."user_id")`,
		sql,
	)

	deleteDs := goqu.Delete("foo").Where(goqu.Ex{"bar": "baz"}).Returning("id")

	ds = goqu.From("bar").
		With("del", deleteDs).
		Select("bar_name").
		Where(goqu.Ex{"bar.user_id": goqu.I("del.user_id")})

	sql, args, err = ds.ToSQL()
	gis.NoError(err)
	gis.Empty(args)
	gis.Equal(
		`WITH del AS (DELETE FROM "foo" WHERE ("bar" = 'baz') RETURNING "id")`+
			` SELECT "bar_name" FROM "bar" WHERE ("bar"."user_id" = "del"."user_id")`,
		sql,
	)

	sql, args, err = ds.Prepared(true).ToSQL()
	gis.NoError(err)
	gis.Equal([]interface{}{"baz"}, args)
	gis.Equal(
		`WITH del AS (DELETE FROM "foo" WHERE ("bar" = ?) RETURNING "id")`+
			` SELECT "bar_name" FROM "bar" WHERE ("bar"."user_id" = "del"."user_id")`,
		sql,
	)
}

// Test for https://github.com/doug-martin/goqu/issues/185
func (gis *githubIssuesSuite) TestIssue185() {
	mDb, sqlMock, err := sqlmock.New()
	gis.NoError(err)
	sqlMock.ExpectQuery(
		`SELECT \* FROM \(SELECT "id" FROM "table" ORDER BY "id" ASC\) AS "t1" UNION 
\(SELECT \* FROM \(SELECT "id" FROM "table" ORDER BY "id" ASC\) AS "t1"\)`,
	).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("1\n2\n3\n4\n"))
	db := goqu.New("mock", mDb)

	ds := db.Select("id").From("table").Order(goqu.C("id").Asc()).
		Union(
			db.Select("id").From("table").Order(goqu.C("id").Asc()),
		)

	ctx := context.Background()
	var i []int
	gis.NoError(ds.ScanValsContext(ctx, &i))
	gis.Equal([]int{1, 2, 3, 4}, i)

}

func TestGithubIssuesSuite(t *testing.T) {
	suite.Run(t, new(githubIssuesSuite))
}
