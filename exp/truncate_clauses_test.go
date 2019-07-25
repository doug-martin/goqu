package exp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type truncateClausesSuite struct {
	suite.Suite
}

func TestTruncateClausesSuite(t *testing.T) {
	suite.Run(t, new(truncateClausesSuite))
}

func (ucs *truncateClausesSuite) TestHasTable() {
	t := ucs.T()
	c := NewTruncateClauses()
	cle := NewColumnListExpression("test1", "test2")
	c2 := c.SetTable(cle)

	assert.False(t, c.HasTable())

	assert.True(t, c2.HasTable())
}

func (ucs *truncateClausesSuite) TestTable() {
	t := ucs.T()
	c := NewTruncateClauses()
	cle := NewColumnListExpression("test1", "test2")
	c2 := c.SetTable(cle)

	assert.Nil(t, c.Table())

	assert.Equal(t, cle, c2.Table())
}

func (ucs *truncateClausesSuite) TestSetTable() {
	t := ucs.T()
	cle := NewColumnListExpression("test1", "test2")
	c := NewTruncateClauses().SetTable(cle)
	cle2 := NewColumnListExpression("test3", "test4")
	c2 := c.SetTable(cle2)

	assert.Equal(t, cle, c.Table())

	assert.Equal(t, cle2, c2.Table())
}

func (ucs *truncateClausesSuite) TestOptions() {
	t := ucs.T()
	c := NewTruncateClauses()
	opts := TruncateOptions{Restrict: true, Identity: "RESTART", Cascade: true}
	c2 := c.SetOptions(opts)

	assert.Equal(t, TruncateOptions{}, c.Options())

	assert.Equal(t, opts, c2.Options())
}

func (ucs *truncateClausesSuite) TestSetOptions() {
	t := ucs.T()
	opts := TruncateOptions{Restrict: true, Identity: "RESTART", Cascade: true}
	c := NewTruncateClauses().SetOptions(opts)
	opts2 := TruncateOptions{Restrict: false, Identity: "RESTART", Cascade: false}
	c2 := c.SetOptions(opts2)

	assert.Equal(t, opts, c.Options())

	assert.Equal(t, opts2, c2.Options())
}
