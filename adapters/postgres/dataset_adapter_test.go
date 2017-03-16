package postgres

import (
	"testing"

	"github.com/c2fo/testify/suite"
	"github.com/c2fo/testify/assert"
	"gopkg.in/doug-martin/goqu.v4"
)

type datasetAdapterTest struct {
	suite.Suite
}

func (me *datasetAdapterTest) TestPlaceholderSql() {
	t := me.T()
	buf := goqu.NewSqlBuilder(true)
	dsAdapter := newDatasetAdapter(goqu.From("test"))
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
