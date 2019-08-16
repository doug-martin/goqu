package exp

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type truncateClausesSuite struct {
	suite.Suite
}

func TestTruncateClausesSuite(t *testing.T) {
	suite.Run(t, new(truncateClausesSuite))
}

func (tcs *truncateClausesSuite) TestHasTable() {
	c := NewTruncateClauses()
	cle := NewColumnListExpression("test1", "test2")
	c2 := c.SetTable(cle)

	tcs.False(c.HasTable())

	tcs.True(c2.HasTable())
}

func (tcs *truncateClausesSuite) TestTable() {
	c := NewTruncateClauses()
	cle := NewColumnListExpression("test1", "test2")
	c2 := c.SetTable(cle)

	tcs.Nil(c.Table())

	tcs.Equal(cle, c2.Table())
}

func (tcs *truncateClausesSuite) TestSetTable() {
	cle := NewColumnListExpression("test1", "test2")
	c := NewTruncateClauses().SetTable(cle)
	cle2 := NewColumnListExpression("test3", "test4")
	c2 := c.SetTable(cle2)

	tcs.Equal(cle, c.Table())

	tcs.Equal(cle2, c2.Table())
}

func (tcs *truncateClausesSuite) TestOptions() {
	c := NewTruncateClauses()
	opts := TruncateOptions{Restrict: true, Identity: "RESTART", Cascade: true}
	c2 := c.SetOptions(opts)

	tcs.Equal(TruncateOptions{}, c.Options())

	tcs.Equal(opts, c2.Options())
}

func (tcs *truncateClausesSuite) TestSetOptions() {
	opts := TruncateOptions{Restrict: true, Identity: "RESTART", Cascade: true}
	c := NewTruncateClauses().SetOptions(opts)
	opts2 := TruncateOptions{Restrict: false, Identity: "RESTART", Cascade: false}
	c2 := c.SetOptions(opts2)

	tcs.Equal(opts, c.Options())

	tcs.Equal(opts2, c2.Options())
}
