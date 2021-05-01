package sqlgen_test

import (
	"testing"

	"github.com/doug-martin/goqu/v9/sqlgen"
	"github.com/stretchr/testify/suite"
)

type sqlFragmentTypeSuite struct {
	suite.Suite
}

func (sfts *sqlFragmentTypeSuite) TestOptions_SQLFragmentType() {
	for _, tt := range []struct {
		typ         sqlgen.SQLFragmentType
		expectedStr string
	}{
		{typ: sqlgen.CommonTableSQLFragment, expectedStr: "CommonTableSQLFragment"},
		{typ: sqlgen.SelectSQLFragment, expectedStr: "SelectSQLFragment"},
		{typ: sqlgen.FromSQLFragment, expectedStr: "FromSQLFragment"},
		{typ: sqlgen.JoinSQLFragment, expectedStr: "JoinSQLFragment"},
		{typ: sqlgen.WhereSQLFragment, expectedStr: "WhereSQLFragment"},
		{typ: sqlgen.GroupBySQLFragment, expectedStr: "GroupBySQLFragment"},
		{typ: sqlgen.HavingSQLFragment, expectedStr: "HavingSQLFragment"},
		{typ: sqlgen.CompoundsSQLFragment, expectedStr: "CompoundsSQLFragment"},
		{typ: sqlgen.OrderSQLFragment, expectedStr: "OrderSQLFragment"},
		{typ: sqlgen.LimitSQLFragment, expectedStr: "LimitSQLFragment"},
		{typ: sqlgen.OffsetSQLFragment, expectedStr: "OffsetSQLFragment"},
		{typ: sqlgen.ForSQLFragment, expectedStr: "ForSQLFragment"},
		{typ: sqlgen.UpdateBeginSQLFragment, expectedStr: "UpdateBeginSQLFragment"},
		{typ: sqlgen.SourcesSQLFragment, expectedStr: "SourcesSQLFragment"},
		{typ: sqlgen.IntoSQLFragment, expectedStr: "IntoSQLFragment"},
		{typ: sqlgen.UpdateSQLFragment, expectedStr: "UpdateSQLFragment"},
		{typ: sqlgen.UpdateFromSQLFragment, expectedStr: "UpdateFromSQLFragment"},
		{typ: sqlgen.ReturningSQLFragment, expectedStr: "ReturningSQLFragment"},
		{typ: sqlgen.InsertBeingSQLFragment, expectedStr: "InsertBeingSQLFragment"},
		{typ: sqlgen.DeleteBeginSQLFragment, expectedStr: "DeleteBeginSQLFragment"},
		{typ: sqlgen.TruncateSQLFragment, expectedStr: "TruncateSQLFragment"},
		{typ: sqlgen.WindowSQLFragment, expectedStr: "WindowSQLFragment"},
		{typ: sqlgen.SQLFragmentType(10000), expectedStr: "10000"},
	} {
		sfts.Equal(tt.expectedStr, tt.typ.String())
	}
}

func TestSQLFragmentType(t *testing.T) {
	suite.Run(t, new(sqlFragmentTypeSuite))
}
