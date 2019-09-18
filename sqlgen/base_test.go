package sqlgen

import (
	"github.com/doug-martin/goqu/v9/internal/sb"
	"github.com/stretchr/testify/suite"
)

type baseSQLGeneratorSuite struct {
	suite.Suite
}

func (bsgs *baseSQLGeneratorSuite) assertNotPreparedSQL(b sb.SQLBuilder, expectedSQL string) {
	actualSQL, actualArgs, err := b.ToSQL()
	bsgs.NoError(err)
	bsgs.Equal(expectedSQL, actualSQL)
	bsgs.Empty(actualArgs)
}

func (bsgs *baseSQLGeneratorSuite) assertPreparedSQL(
	b sb.SQLBuilder,
	expectedSQL string,
	expectedArgs []interface{},
) {
	actualSQL, actualArgs, err := b.ToSQL()
	bsgs.NoError(err)
	bsgs.Equal(expectedSQL, actualSQL)
	if len(actualArgs) == 0 {
		bsgs.Empty(expectedArgs)
	} else {
		bsgs.Equal(expectedArgs, actualArgs)
	}

}

func (bsgs *baseSQLGeneratorSuite) assertErrorSQL(b sb.SQLBuilder, errMsg string) {
	actualSQL, actualArgs, err := b.ToSQL()
	bsgs.EqualError(err, errMsg)
	bsgs.Empty(actualSQL)
	bsgs.Empty(actualArgs)
}
