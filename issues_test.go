package goqu_test

import (
	"sync"
	"testing"
	"time"

	"github.com/doug-martin/goqu/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type githubIssuesSuite struct {
	suite.Suite
}

// Test for https://github.com/doug-martin/goqu/issues/49
func (gis *githubIssuesSuite) TestIssue49() {
	t := gis.T()
	dialect := goqu.Dialect("default")

	filters := goqu.Or()
	sql, args, err := dialect.From("table").Where(filters).ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `SELECT * FROM "table"`, sql)

	sql, args, err = dialect.From("table").Where(goqu.Ex{}).ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `SELECT * FROM "table"`, sql)

	sql, args, err = dialect.From("table").Where(goqu.ExOr{}).ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `SELECT * FROM "table"`, sql)
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

func TestGithubIssuesSuite(t *testing.T) {
	suite.Run(t, new(githubIssuesSuite))
}
