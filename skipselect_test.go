package goqu_test

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	"github.com/doug-martin/goqu/v9"
)

type skipSelectAudit struct {
	CreatedBy string `db:"created_by"`
	UpdatedBy string `db:"updated_by"`
}

type skipSelectUser struct {
	ID    int64  `db:"id"`
	Name  string `db:"name"`
	Group string `db:"group_name" goqu:"skipselect"`
	// every column of a nested struct inherits the tag
	Audit skipSelectAudit `db:"audit" goqu:"skipselect"`
	// skipselect does not change insert and update
	Note string `db:"note" goqu:"skipselect,skipinsert"`
}

// skipselect leaves a field out of the columns selected for a struct (#433)
func TestSkipSelect(t *testing.T) {
	t.Run("select from a struct", func(t *testing.T) {
		sql, _, err := goqu.From("users").Select(&skipSelectUser{}).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `SELECT "id", "name" FROM "users"`, sql)
	})

	t.Run("an explicit column is added next to the struct", func(t *testing.T) {
		sql, _, err := goqu.From("users").
			Join(goqu.T("groups"), goqu.On(goqu.I("groups.id").Eq(goqu.I("users.group_id")))).
			Select(&skipSelectUser{}, goqu.I("groups.name").As("group_name")).ToSQL()
		require.NoError(t, err)
		require.Equal(t,
			`SELECT "id", "name", "groups"."name" AS "group_name" FROM "users" `+
				`INNER JOIN "groups" ON ("groups"."id" = "users"."group_id")`, sql)
	})

	t.Run("insert and update still use the field", func(t *testing.T) {
		u := skipSelectUser{ID: 1, Name: "n", Group: "g", Audit: skipSelectAudit{CreatedBy: "c"}, Note: "x"}
		sql, _, err := goqu.Insert("users").Rows(u).ToSQL()
		require.NoError(t, err)
		require.Contains(t, sql, `"group_name"`)
		require.Contains(t, sql, `"audit"."created_by"`)
		require.NotContains(t, sql, `"note"`) // skipinsert

		sql, _, err = goqu.Update("users").Set(u).ToSQL()
		require.NoError(t, err)
		require.Contains(t, sql, `"group_name"='g'`)
		require.Contains(t, sql, `"note"='x'`)
	})

	t.Run("a selected column is still scanned", func(t *testing.T) {
		mDB, mock, err := sqlmock.New()
		require.NoError(t, err)
		mock.ExpectQuery(`SELECT "id", "name", "groups"."name" AS "group_name" FROM "users"`).
			WillReturnRows(sqlmock.NewRows([]string{"id", "name", "group_name"}).AddRow(1, "ann", "admins"))

		db := goqu.New("default", mDB)
		var users []skipSelectUser
		err = db.From("users").Select(&skipSelectUser{}, goqu.I("groups.name").As("group_name")).ScanStructs(&users)
		require.NoError(t, err)
		require.Equal(t, []skipSelectUser{{ID: 1, Name: "ann", Group: "admins"}}, users)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("ScanStructs selects the struct columns without the skipped ones", func(t *testing.T) {
		mDB, mock, err := sqlmock.New()
		require.NoError(t, err)
		mock.ExpectQuery(`SELECT "id", "name" FROM "users"`).
			WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "ann"))

		var users []skipSelectUser
		require.NoError(t, goqu.New("default", mDB).From("users").ScanStructs(&users))
		require.Equal(t, []skipSelectUser{{ID: 1, Name: "ann"}}, users)
		require.NoError(t, mock.ExpectationsWereMet())
	})

	// the tag is for SELECT only, the other places that expand a struct to columns keep every column
	t.Run("insert cols, group by, distinct on and returning are unchanged", func(t *testing.T) {
		d := goqu.Dialect("postgres")

		sql, _, err := d.Insert("users").Cols(&skipSelectTwo{}).Vals(goqu.Vals{1, 2}).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `INSERT INTO "users" ("a", "b") VALUES (1, 2)`, sql)

		sql, _, err = d.From("users").Select("id").GroupBy(&skipSelectTwo{}).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `SELECT "id" FROM "users" GROUP BY "a", "b"`, sql)

		sql, _, err = d.From("users").Distinct(&skipSelectAudit{}).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `SELECT DISTINCT ON ("created_by", "updated_by") * FROM "users"`, sql)

		sql, _, err = d.Update("users").Set(goqu.Record{"name": "n"}).Returning(&skipSelectTwo{}).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `UPDATE "users" SET "name"='n' RETURNING "a", "b"`, sql)
	})

	t.Run("SelectAppend and SelectDistinct skip the tagged columns", func(t *testing.T) {
		sql, _, err := goqu.From("users").Select("x").SelectAppend(&skipSelectUser{}).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `SELECT "x", "id", "name" FROM "users"`, sql)

		sql, _, err = goqu.From("users").SelectDistinct(&skipSelectUser{}).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `SELECT DISTINCT "id", "name" FROM "users"`, sql)
	})

	// an empty column list is written as *, which would select the skipped columns
	t.Run("a struct where every column is skipped is an error", func(t *testing.T) {
		base := goqu.From("users")
		for name, ds := range map[string]*goqu.SelectDataset{
			"Select":         base.Select(&skipSelectAll{}),
			"SelectAppend":   base.SelectAppend(&skipSelectAll{}),
			"SelectDistinct": base.SelectDistinct(&skipSelectAll{}),
		} {
			t.Run(name, func(t *testing.T) {
				_, _, err := ds.ToSQL()
				require.EqualError(t, err, "goqu: all of the columns of the struct are tagged skipselect, "+
					"there is nothing to select")
			})
		}
		// the dataset it was built from is not changed
		require.NoError(t, base.Error())

		// another column next to it is fine
		sql, _, err := base.Select(&skipSelectAll{}, "id").ToSQL()
		require.NoError(t, err)
		require.Equal(t, `SELECT "id" FROM "users"`, sql)
	})

	t.Run("a pointer to a struct field and an embedded struct inherit the tag", func(t *testing.T) {
		sql, _, err := goqu.From("t").Select(&skipSelectEmbedded{}).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `SELECT "c" FROM "t"`, sql)

		sql, _, err = goqu.From("t").Select(&skipSelectPtr{}).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `SELECT "p" FROM "t"`, sql)
	})
}

type skipSelectAll struct {
	A string `db:"a" goqu:"skipselect"`
	B string `db:"b" goqu:"skipselect"`
}

type skipSelectInner struct {
	X string `db:"x"`
}

type skipSelectEmbedded struct {
	skipSelectInner `goqu:"skipselect"`
	C               string `db:"c"`
}

type skipSelectPtr struct {
	P     string           `db:"p"`
	Inner *skipSelectInner `db:"inner" goqu:"skipselect"`
}

type skipSelectTwo struct {
	A string `db:"a"`
	B string `db:"b" goqu:"skipselect"`
}
