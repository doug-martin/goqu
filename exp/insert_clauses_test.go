package exp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type insertClausesSuite struct {
	suite.Suite
}

func TestInsertClausesSuite(t *testing.T) {
	suite.Run(t, new(insertClausesSuite))
}

func (ics *insertClausesSuite) TestInto() {
	t := ics.T()
	c := NewInsertClauses()
	ti := NewIdentifierExpression("", "test", "")
	c2 := c.SetInto(ti)

	assert.Nil(t, c.Into())

	assert.Equal(t, ti, c2.Into())
}

func (ics *insertClausesSuite) TestHasInto() {
	t := ics.T()
	c := NewInsertClauses()
	ti := NewIdentifierExpression("", "test", "")
	c2 := c.SetInto(ti)

	assert.False(t, c.HasInto())

	assert.True(t, c2.HasInto())
}

func (ics *insertClausesSuite) TestFrom() {
	t := ics.T()
	c := NewInsertClauses()
	ae := newTestAppendableExpression("select * from test", nil)
	c2 := c.SetFrom(ae)

	assert.Nil(t, c.From())

	assert.Equal(t, ae, c2.From())
}

func (ics *insertClausesSuite) TestHasFrom() {
	t := ics.T()
	c := NewInsertClauses()
	ae := newTestAppendableExpression("select * from test", nil)
	c2 := c.SetFrom(ae)

	assert.False(t, c.HasFrom())

	assert.True(t, c2.HasFrom())
}

func (ics *insertClausesSuite) TestSetFrom() {
	t := ics.T()
	c := NewInsertClauses()
	ae := newTestAppendableExpression("select * from test", nil)
	c2 := c.SetFrom(ae)

	assert.Nil(t, c.From())

	assert.Equal(t, ae, c2.From())
}

func (ics *insertClausesSuite) TestCols() {
	t := ics.T()
	c := NewInsertClauses()
	cle := NewColumnListExpression("a", "b")
	c2 := c.SetCols(cle)

	assert.Nil(t, c.Cols())

	assert.Equal(t, cle, c2.Cols())
}

func (ics *insertClausesSuite) TestHasCols() {
	t := ics.T()
	c := NewInsertClauses()
	cle := NewColumnListExpression("a", "b")
	c2 := c.SetCols(cle)

	assert.False(t, c.HasCols())

	assert.True(t, c2.HasCols())
}
func (ics *insertClausesSuite) TestColsAppend() {
	t := ics.T()
	cle := NewColumnListExpression("a")
	cle2 := NewColumnListExpression("b")
	c := NewInsertClauses().SetCols(cle)
	c2 := c.ColsAppend(cle2)

	assert.Equal(t, cle, c.Cols())

	assert.Equal(t, NewColumnListExpression("a", "b"), c2.Cols())
}

func (ics *insertClausesSuite) TestVals() {
	t := ics.T()
	c := NewInsertClauses()
	vals := [][]interface{}{{"a", "b"}}
	c2 := c.SetVals(vals)

	assert.Nil(t, c.Vals())

	assert.Equal(t, vals, c2.Vals())
}

func (ics *insertClausesSuite) TestHasVals() {
	t := ics.T()
	c := NewInsertClauses()
	vals := [][]interface{}{{"a", "b"}}
	c2 := c.SetVals(vals)

	assert.False(t, c.HasVals())

	assert.True(t, c2.HasVals())
}
func (ics *insertClausesSuite) TestValsAppend() {
	t := ics.T()
	vals := [][]interface{}{{"a", "b"}}
	vals2 := [][]interface{}{{"c", "d"}}
	c := NewInsertClauses().SetVals(vals)
	c2 := c.ValsAppend(vals2)

	assert.Equal(t, vals, c.Vals())

	assert.Equal(t, [][]interface{}{
		{"a", "b"},
		{"c", "d"},
	}, c2.Vals())
}

func (ics *insertClausesSuite) TestRows() {
	t := ics.T()
	c := NewInsertClauses()
	rs := []interface{}{Record{"a": "a1", "b": "b1"}}
	c2 := c.SetRows(rs)

	assert.Nil(t, c.Rows())

	assert.Equal(t, rs, c2.Rows())
}

func (ics *insertClausesSuite) TestHasRows() {
	t := ics.T()
	c := NewInsertClauses()
	rs := []interface{}{Record{"a": "a1", "b": "b1"}}
	c2 := c.SetRows(rs)

	assert.False(t, c.HasRows())

	assert.True(t, c2.HasRows())
}
func (ics *insertClausesSuite) TestSetRows() {
	t := ics.T()
	rs := []interface{}{Record{"a": "a1", "b": "b1"}}
	c := NewInsertClauses().SetRows(rs)
	rs2 := []interface{}{Record{"a": "a2", "b": "b2"}}
	c2 := c.SetRows(rs2)

	assert.Equal(t, rs, c.Rows())

	assert.Equal(t, rs2, c2.Rows())
}

func (ics *insertClausesSuite) TestCommonTables() {
	t := ics.T()

	cte := NewCommonTableExpression(true, "test", newTestAppendableExpression(`SELECT * FROM "foo"`, []interface{}{}))

	c := NewInsertClauses()
	c2 := c.CommonTablesAppend(cte)

	assert.Nil(t, c.CommonTables())

	assert.Equal(t, []CommonTableExpression{cte}, c2.CommonTables())
}

func (ics *insertClausesSuite) TestAddCommonTablesAppend() {
	t := ics.T()

	cte := NewCommonTableExpression(true, "test", testSQLExpression("test_cte"))
	cte2 := NewCommonTableExpression(true, "test", testSQLExpression("test_cte2"))

	c := NewInsertClauses().CommonTablesAppend(cte)
	c2 := c.CommonTablesAppend(cte2)

	assert.Equal(t, []CommonTableExpression{cte}, c.CommonTables())

	assert.Equal(t, []CommonTableExpression{cte, cte2}, c2.CommonTables())
}

func (ics *insertClausesSuite) TestOnConflict() {
	t := ics.T()

	ce := NewDoNothingConflictExpression()

	c := NewInsertClauses()
	c2 := c.SetOnConflict(ce)

	assert.Nil(t, c.OnConflict())

	assert.Equal(t, ce, c2.OnConflict())
}

func (ics *insertClausesSuite) TestSetOnConflict() {
	t := ics.T()

	ce := NewDoNothingConflictExpression()

	c := NewInsertClauses().SetOnConflict(ce)
	ce2 := NewDoUpdateConflictExpression("test", Record{"a": "a1"})
	c2 := c.SetOnConflict(ce2)

	assert.Equal(t, ce, c.OnConflict())

	assert.Equal(t, ce2, c2.OnConflict())
}

func (ics *insertClausesSuite) TestReturning() {
	t := ics.T()

	cl := NewColumnListExpression(NewIdentifierExpression("", "", "col"))

	c := NewInsertClauses()
	c2 := c.SetReturning(cl)

	assert.Nil(t, c.Returning())

	assert.Equal(t, cl, c2.Returning())
}

func (ics *insertClausesSuite) TestHasReturning() {
	t := ics.T()

	cl := NewColumnListExpression(NewIdentifierExpression("", "", "col"))

	c := NewInsertClauses()
	c2 := c.SetReturning(cl)

	assert.False(t, c.HasReturning())

	assert.True(t, c2.HasReturning())
}

func (ics *insertClausesSuite) TestSetReturning() {
	t := ics.T()

	cl := NewColumnListExpression(NewIdentifierExpression("", "", "col"))
	cl2 := NewColumnListExpression(NewIdentifierExpression("", "", "col2"))

	c := NewInsertClauses().SetReturning(cl)
	c2 := c.SetReturning(cl2)

	assert.Equal(t, cl, c.Returning())

	assert.Equal(t, cl2, c2.Returning())
}
