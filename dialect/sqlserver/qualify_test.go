package sqlserver_test

import (
	"testing"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/sqlserver"
	"github.com/stretchr/testify/require"
)

// SQL Server defines its own SelectSQLOrder, so make sure QUALIFY is not silently dropped.
func TestQualifyNotSupported(t *testing.T) {
	t.Parallel()

	_, _, err := goqu.Dialect("sqlserver").From("test").Qualify(goqu.C("a").Eq(1)).ToSQL()
	require.EqualError(t, err, "goqu: dialect does not support QUALIFY clause [dialect=sqlserver]")
}
