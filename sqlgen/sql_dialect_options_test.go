package sqlgen

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type sqlFragmentTypeSuite struct {
	suite.Suite
}

func (sfts *sqlFragmentTypeSuite) TestOptions_SQLFragmentType() {
	for _, tt := range []struct {
		typ         SQLFragmentType
		expectedStr string
	}{
		{typ: CommonTableSQLFragment, expectedStr: "CommonTableSQLFragment"},
		{typ: SelectSQLFragment, expectedStr: "SelectSQLFragment"},
		{typ: FromSQLFragment, expectedStr: "FromSQLFragment"},
		{typ: JoinSQLFragment, expectedStr: "JoinSQLFragment"},
		{typ: WhereSQLFragment, expectedStr: "WhereSQLFragment"},
		{typ: GroupBySQLFragment, expectedStr: "GroupBySQLFragment"},
		{typ: HavingSQLFragment, expectedStr: "HavingSQLFragment"},
		{typ: CompoundsSQLFragment, expectedStr: "CompoundsSQLFragment"},
		{typ: OrderSQLFragment, expectedStr: "OrderSQLFragment"},
		{typ: LimitSQLFragment, expectedStr: "LimitSQLFragment"},
		{typ: OffsetSQLFragment, expectedStr: "OffsetSQLFragment"},
		{typ: ForSQLFragment, expectedStr: "ForSQLFragment"},
		{typ: UpdateBeginSQLFragment, expectedStr: "UpdateBeginSQLFragment"},
		{typ: SourcesSQLFragment, expectedStr: "SourcesSQLFragment"},
		{typ: IntoSQLFragment, expectedStr: "IntoSQLFragment"},
		{typ: UpdateSQLFragment, expectedStr: "UpdateSQLFragment"},
		{typ: UpdateFromSQLFragment, expectedStr: "UpdateFromSQLFragment"},
		{typ: ReturningSQLFragment, expectedStr: "ReturningSQLFragment"},
		{typ: InsertBeingSQLFragment, expectedStr: "InsertBeingSQLFragment"},
		{typ: DeleteBeginSQLFragment, expectedStr: "DeleteBeginSQLFragment"},
		{typ: TruncateSQLFragment, expectedStr: "TruncateSQLFragment"},
		{typ: WindowSQLFragment, expectedStr: "WindowSQLFragment"},
		{typ: SQLFragmentType(10000), expectedStr: "10000"},
	} {
		sfts.Equal(tt.expectedStr, tt.typ.String())
	}
}

func TestSQLFragmentType(t *testing.T) {
	suite.Run(t, new(sqlFragmentTypeSuite))
}
