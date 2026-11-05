package goqu_test

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/mysql"
	_ "github.com/doug-martin/goqu/v9/dialect/postgres"
	_ "github.com/doug-martin/goqu/v9/dialect/sqlserver"
	"github.com/doug-martin/goqu/v9/sqlgen"
)

// Limit(0) clears the limit, LimitZero forces LIMIT 0 (#194)
func TestLimitZero(t *testing.T) {
	t.Run("select", func(t *testing.T) {
		sql, _, err := goqu.Dialect("postgres").From("t").LimitZero().ToSQL()
		require.NoError(t, err)
		require.Equal(t, `SELECT * FROM "t" LIMIT 0`, sql)

		sql, args, err := goqu.Dialect("postgres").From("t").LimitZero().Prepared(true).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `SELECT * FROM "t" LIMIT $1`, sql)
		require.Equal(t, []any{int64(0)}, args)

		sql, _, err = goqu.Dialect("mysql").From("t").Limit(5).LimitZero().ToSQL()
		require.NoError(t, err)
		require.Equal(t, "SELECT * FROM `t` LIMIT 0", sql)
	})

	t.Run("sqlserver", func(t *testing.T) {
		sql, _, err := goqu.Dialect("sqlserver").From("t").LimitZero().ToSQL()
		require.NoError(t, err)
		require.Equal(t, `SELECT  TOP (0) * FROM "t"`, sql)
	})

	// FETCH needs at least one row on sqlserver, a zero limit with an offset is an error instead of invalid SQL
	t.Run("sqlserver with an offset", func(t *testing.T) {
		ds := goqu.Dialect("sqlserver").From("t").Order(goqu.C("id").Asc()).Offset(2).LimitZero()
		_, _, err := ds.ToSQL()
		require.EqualError(t, err, "goqu: dialect does not support a limit of 0 with an OFFSET [dialect=sqlserver]")
		_, _, err = ds.Prepared(true).ToSQL()
		require.Error(t, err)

		sql, _, err := goqu.Dialect("sqlserver").From("t").Order(goqu.C("id").Asc()).Offset(2).Limit(5).ToSQL()
		require.NoError(t, err)
		require.Equal(t, `SELECT * FROM "t" ORDER BY "id" ASC OFFSET 2 ROWS FETCH FIRST 5 ROWS ONLY`, sql)
	})

	// databases where FETCH accepts 0 rows (AlwaysOffsetFetch dialects) are unchanged
	t.Run("always offset fetch", func(t *testing.T) {
		o := sqlgen.DefaultDialectOptions()
		o.FetchFragment = []byte(" FETCH NEXT ")
		o.AlwaysOffsetFetch = true
		order := make([]sqlgen.SQLFragmentType, 0, len(o.SelectSQLOrder))
		for _, f := range o.SelectSQLOrder {
			switch f {
			case sqlgen.OrderSQLFragment:
				order = append(order, sqlgen.OrderWithOffsetFetchSQLFragment)
			case sqlgen.LimitSQLFragment, sqlgen.OffsetSQLFragment:
			default:
				order = append(order, f)
			}
		}
		o.SelectSQLOrder = order
		goqu.RegisterDialect("test-limit-zero-fetch", o)
		defer goqu.DeregisterDialect("test-limit-zero-fetch")

		sql, _, err := goqu.Dialect("test-limit-zero-fetch").From("t").Offset(2).LimitZero().ToSQL()
		require.NoError(t, err)
		require.Equal(t, `SELECT * FROM "t" OFFSET 2 ROWS FETCH NEXT 0 ROWS ONLY`, sql)
	})

	// the single row scans limit to one row, but keep an explicit LimitZero
	t.Run("single row scans keep the zero limit", func(t *testing.T) {
		newDB := func() (*goqu.Database, sqlmock.Sqlmock) {
			mDB, mock, err := sqlmock.New()
			require.NoError(t, err)
			return goqu.New("postgres", mDB), mock
		}

		db, mock := newDB()
		mock.ExpectQuery(`^SELECT "id" FROM "t" LIMIT 0$`).WillReturnRows(sqlmock.NewRows([]string{"id"}))
		var id int
		found, err := db.From("t").Select("id").LimitZero().ScanVal(&id)
		require.NoError(t, err)
		require.False(t, found)
		require.NoError(t, mock.ExpectationsWereMet())

		db, mock = newDB()
		mock.ExpectQuery(`^SELECT "id" FROM "t" LIMIT 0$`).WillReturnRows(sqlmock.NewRows([]string{"id"}))
		var row struct {
			ID int `db:"id"`
		}
		found, err = db.From("t").LimitZero().ScanStruct(&row)
		require.NoError(t, err)
		require.False(t, found)
		require.NoError(t, mock.ExpectationsWereMet())

		// without it they still add LIMIT 1
		db, mock = newDB()
		mock.ExpectQuery(`^SELECT "id" FROM "t" LIMIT 1$`).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(7))
		found, err = db.From("t").Select("id").ScanVal(&id)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, 7, id)
	})
}
