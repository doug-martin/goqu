package sqlite3_test

import (
	"testing"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/sqlite3"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/stretchr/testify/assert"
)

// The sqlite3 dialect's conflict fragments carry their own spacing (" ON CONFLICT ", " DO NOTHING "), which is why
// the SQL below has a double space and a trailing space. It is valid SQL and kept for output compatibility.
func TestConflictTargets(t *testing.T) {
	cols := exp.NewColumnListExpression("a", "b")
	upd := goqu.Record{"b": goqu.L("EXCLUDED.b")}
	ds := goqu.Dialect("sqlite3").Insert("test").Rows(goqu.Record{"a": 1, "b": 2})

	sql, _, err := ds.OnConflict(goqu.DoNothing().SetCols(cols).TargetWhere(goqu.C("c").IsNull())).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "INSERT INTO `test` (`a`, `b`) VALUES (1, 2) ON CONFLICT  (`a`, `b`) WHERE (`c` IS NULL) DO NOTHING ", sql)

	sql, _, err = ds.OnConflict(goqu.DoUpdate("", upd).SetCols(cols)).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, "INSERT INTO `test` (`a`, `b`) VALUES (1, 2) ON CONFLICT  (`a`, `b`) DO UPDATE SET `b`=EXCLUDED.b", sql)

	_, _, err = ds.OnConflict(goqu.DoNothing().OnConstraint("k")).ToSQL()
	assert.EqualError(t, err, "goqu: dialect does not support ON CONFLICT ON CONSTRAINT [dialect=sqlite3]")

	_, _, err = ds.OnConflict(goqu.DoUpdate("", upd).OnConstraint("k")).ToSQL()
	assert.EqualError(t, err, "goqu: dialect does not support ON CONFLICT ON CONSTRAINT [dialect=sqlite3]")
}
