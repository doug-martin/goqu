package sqlserver_test

import (
	"testing"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/sqlserver"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/stretchr/testify/assert"
)

func TestConflictTargets(t *testing.T) {
	cols := exp.NewColumnListExpression("a", "b")
	upd := goqu.Record{"b": goqu.L("EXCLUDED.b")}
	ds := goqu.Dialect("sqlserver").Insert("test").Rows(goqu.Record{"a": 1, "b": 2})

	_, _, err := ds.OnConflict(goqu.DoNothing().SetCols(cols)).ToSQL()
	assert.EqualError(t, err, "goqu: dialect does not support Conflict Target clause [dialect=sqlserver]")

	_, _, err = ds.OnConflict(goqu.DoUpdate("", upd).SetCols(cols)).ToSQL()
	assert.EqualError(t, err, "goqu: dialect does not support Conflict Target clause [dialect=sqlserver]")

	_, _, err = ds.OnConflict(goqu.DoNothing().OnConstraint("k")).ToSQL()
	assert.EqualError(t, err, "goqu: dialect does not support ON CONFLICT ON CONSTRAINT [dialect=sqlserver]")

	_, _, err = ds.OnConflict(goqu.DoUpdate("", upd).OnConstraint("k")).ToSQL()
	assert.EqualError(t, err, "goqu: dialect does not support ON CONFLICT ON CONSTRAINT [dialect=sqlserver]")
}
