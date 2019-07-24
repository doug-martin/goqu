package goqu_test

import (
	"testing"

	"github.com/doug-martin/goqu/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type githubIssuesSuite struct {
	suite.Suite
}

// Test for https://github.com/doug-martin/goqu/issues/49
func (gis *githubIssuesSuite) TestIssue49() {
	t := gis.T()
	dialect := goqu.Dialect("default")

	filters := goqu.Or()
	sql, args, err := dialect.From("table").Where(filters).ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `SELECT * FROM "table"`, sql)

	sql, args, err = dialect.From("table").Where(goqu.Ex{}).ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `SELECT * FROM "table"`, sql)

	sql, args, err = dialect.From("table").Where(goqu.ExOr{}).ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `SELECT * FROM "table"`, sql)
}

func TestGithubIssuesSuite(t *testing.T) {
	suite.Run(t, new(githubIssuesSuite))
}
