package exp_test

import (
	"testing"

	"github.com/doug-martin/goqu/v9/exp"
	"github.com/stretchr/testify/suite"
)

type insertClausesSuite struct {
	suite.Suite
}

func TestInsertClausesSuite(t *testing.T) {
	suite.Run(t, new(insertClausesSuite))
}

func (ics *insertClausesSuite) TestInto() {
	c := exp.NewInsertClauses()
	ti := exp.NewIdentifierExpression("", "test", "")
	c2 := c.SetInto(ti)

	ics.Nil(c.Into())

	ics.Equal(ti, c2.Into())
}

func (ics *insertClausesSuite) TestHasInto() {
	c := exp.NewInsertClauses()
	ti := exp.NewIdentifierExpression("", "test", "")
	c2 := c.SetInto(ti)

	ics.False(c.HasInto())

	ics.True(c2.HasInto())
}

func (ics *insertClausesSuite) TestFrom() {
	c := exp.NewInsertClauses()
	ae := newTestAppendableExpression("select * from test", nil)
	c2 := c.SetFrom(ae)

	ics.Nil(c.From())

	ics.Equal(ae, c2.From())
}

func (ics *insertClausesSuite) TestHasFrom() {
	c := exp.NewInsertClauses()
	ae := newTestAppendableExpression("select * from test", nil)
	c2 := c.SetFrom(ae)

	ics.False(c.HasFrom())

	ics.True(c2.HasFrom())
}

func (ics *insertClausesSuite) TestSetFrom() {
	c := exp.NewInsertClauses()
	ae := newTestAppendableExpression("select * from test", nil)
	c2 := c.SetFrom(ae)

	ics.Nil(c.From())

	ics.Equal(ae, c2.From())
}

func (ics *insertClausesSuite) TestCols() {
	c := exp.NewInsertClauses()
	cle := exp.NewColumnListExpression("a", "b")
	c2 := c.SetCols(cle)

	ics.Nil(c.Cols())

	ics.Equal(cle, c2.Cols())
}

func (ics *insertClausesSuite) TestHasCols() {
	c := exp.NewInsertClauses()
	cle := exp.NewColumnListExpression("a", "b")
	c2 := c.SetCols(cle)

	ics.False(c.HasCols())

	ics.True(c2.HasCols())
}

func (ics *insertClausesSuite) TestColsAppend() {
	cle := exp.NewColumnListExpression("a")
	cle2 := exp.NewColumnListExpression("b")
	c := exp.NewInsertClauses().SetCols(cle)
	c2 := c.ColsAppend(cle2)

	ics.Equal(cle, c.Cols())

	ics.Equal(exp.NewColumnListExpression("a", "b"), c2.Cols())
}

func (ics *insertClausesSuite) TestVals() {
	c := exp.NewInsertClauses()
	vals := [][]interface{}{{"a", "b"}}
	c2 := c.SetVals(vals)

	ics.Nil(c.Vals())

	ics.Equal(vals, c2.Vals())
}

func (ics *insertClausesSuite) TestHasVals() {
	c := exp.NewInsertClauses()
	vals := [][]interface{}{{"a", "b"}}
	c2 := c.SetVals(vals)

	ics.False(c.HasVals())

	ics.True(c2.HasVals())
}

func (ics *insertClausesSuite) TestValsAppend() {
	vals := [][]interface{}{{"a", "b"}}
	vals2 := [][]interface{}{{"c", "d"}}
	c := exp.NewInsertClauses().SetVals(vals)
	c2 := c.ValsAppend(vals2)

	ics.Equal(vals, c.Vals())

	ics.Equal([][]interface{}{
		{"a", "b"},
		{"c", "d"},
	}, c2.Vals())
}

func (ics *insertClausesSuite) TestRows() {
	c := exp.NewInsertClauses()
	rs := []interface{}{exp.Record{"a": "a1", "b": "b1"}}
	c2 := c.SetRows(rs)

	ics.Nil(c.Rows())

	ics.Equal(rs, c2.Rows())
}

func (ics *insertClausesSuite) TestHasRows() {
	c := exp.NewInsertClauses()
	rs := []interface{}{exp.Record{"a": "a1", "b": "b1"}}
	c2 := c.SetRows(rs)

	ics.False(c.HasRows())

	ics.True(c2.HasRows())
}

func (ics *insertClausesSuite) TestSetRows() {
	rs := []interface{}{exp.Record{"a": "a1", "b": "b1"}}
	c := exp.NewInsertClauses().SetRows(rs)
	rs2 := []interface{}{exp.Record{"a": "a2", "b": "b2"}}
	c2 := c.SetRows(rs2)

	ics.Equal(rs, c.Rows())

	ics.Equal(rs2, c2.Rows())
}

func (ics *insertClausesSuite) TestCommonTables() {
	cte := exp.NewCommonTableExpression(true, "test", newTestAppendableExpression(`SELECT * FROM "foo"`, []interface{}{}))

	c := exp.NewInsertClauses()
	c2 := c.CommonTablesAppend(cte)

	ics.Nil(c.CommonTables())

	ics.Equal([]exp.CommonTableExpression{cte}, c2.CommonTables())
}

func (ics *insertClausesSuite) TestAddCommonTablesAppend() {
	cte := exp.NewCommonTableExpression(true, "test", testSQLExpression("test_cte"))
	cte2 := exp.NewCommonTableExpression(true, "test", testSQLExpression("test_cte2"))

	c := exp.NewInsertClauses().CommonTablesAppend(cte)
	c2 := c.CommonTablesAppend(cte2)

	ics.Equal([]exp.CommonTableExpression{cte}, c.CommonTables())

	ics.Equal([]exp.CommonTableExpression{cte, cte2}, c2.CommonTables())
}

func (ics *insertClausesSuite) TestOnConflict() {
	ce := exp.NewDoNothingConflictExpression()

	c := exp.NewInsertClauses()
	c2 := c.SetOnConflict(ce)

	ics.Nil(c.OnConflict())

	ics.Equal(ce, c2.OnConflict())
}

func (ics *insertClausesSuite) TestSetOnConflict() {
	ce := exp.NewDoNothingConflictExpression()

	c := exp.NewInsertClauses().SetOnConflict(ce)
	ce2 := exp.NewDoUpdateConflictExpression("test", exp.Record{"a": "a1"})
	c2 := c.SetOnConflict(ce2)

	ics.Equal(ce, c.OnConflict())

	ics.Equal(ce2, c2.OnConflict())
}

func (ics *insertClausesSuite) TestReturning() {
	cl := exp.NewColumnListExpression(exp.NewIdentifierExpression("", "", "col"))

	c := exp.NewInsertClauses()
	c2 := c.SetReturning(cl)

	ics.Nil(c.Returning())

	ics.Equal(cl, c2.Returning())
}

func (ics *insertClausesSuite) TestHasReturning() {
	cl := exp.NewColumnListExpression(exp.NewIdentifierExpression("", "", "col"))

	c := exp.NewInsertClauses()
	c2 := c.SetReturning(cl)

	ics.False(c.HasReturning())

	ics.True(c2.HasReturning())
}

func (ics *insertClausesSuite) TestSetReturning() {
	cl := exp.NewColumnListExpression(exp.NewIdentifierExpression("", "", "col"))
	cl2 := exp.NewColumnListExpression(exp.NewIdentifierExpression("", "", "col2"))

	c := exp.NewInsertClauses().SetReturning(cl)
	c2 := c.SetReturning(cl2)

	ics.Equal(cl, c.Returning())

	ics.Equal(cl2, c2.Returning())
}
