package postgres

import (
	"github.com/doug-martin/gql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"testing"
)

type datasetAdapterTest struct {
	suite.Suite
}

func (me *datasetAdapterTest) TestPlaceholderSql() {
	t := me.T()
	buf := gql.NewSqlBuilder(true)
	dsAdapter := newDatasetAdapter(gql.From("test"))
	dsAdapter.PlaceHolderSql(buf, 1)
	dsAdapter.PlaceHolderSql(buf, 2)
	dsAdapter.PlaceHolderSql(buf, 3)
	dsAdapter.PlaceHolderSql(buf, 4)
	sql, args := buf.ToSql()
	assert.Equal(t, args, []interface{}{1, 2, 3, 4})
	assert.Equal(t, sql, "$1$2$3$4")
}

func TestDatasetAdapterSuite(t *testing.T) {
	suite.Run(t, new(datasetAdapterTest))
}
