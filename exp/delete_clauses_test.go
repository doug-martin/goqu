package exp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type deleteClausesSuite struct {
	suite.Suite
}

func TestDeleteClausesSuite(t *testing.T) {
	suite.Run(t, new(deleteClausesSuite))
}

func (dcs *deleteClausesSuite) TestHasFrom() {
	t := dcs.T()
	c := NewDeleteClauses()
	c2 := c.SetFrom(NewIdentifierExpression("", "test", ""))

	assert.False(t, c.HasFrom())

	assert.True(t, c2.HasFrom())
}

func (dcs *deleteClausesSuite) TestFrom() {
	t := dcs.T()
	c := NewDeleteClauses()
	ti := NewIdentifierExpression("", "a", "")
	c2 := c.SetFrom(ti)

	assert.Nil(t, c.From())

	assert.Equal(t, ti, c2.From())
}

func (dcs *deleteClausesSuite) TestSetFrom() {
	t := dcs.T()
	c := NewDeleteClauses()
	ti := NewIdentifierExpression("", "a", "")
	c2 := c.SetFrom(ti)

	assert.Nil(t, c.From())

	assert.Equal(t, ti, c2.From())
}

func (dcs *deleteClausesSuite) TestWhere() {
	t := dcs.T()
	w := Ex{"a": 1}

	c := NewDeleteClauses()
	c2 := c.WhereAppend(w)

	assert.Nil(t, c.Where())

	assert.Equal(t, NewExpressionList(AndType, w), c2.Where())
}

func (dcs *deleteClausesSuite) TestClearWhere() {
	t := dcs.T()
	w := Ex{"a": 1}

	c := NewDeleteClauses().WhereAppend(w)
	c2 := c.ClearWhere()

	assert.Equal(t, NewExpressionList(AndType, w), c.Where())

	assert.Nil(t, c2.Where())
}

func (dcs *deleteClausesSuite) TestWhereAppend() {
	t := dcs.T()
	w := Ex{"a": 1}
	w2 := Ex{"b": 2}

	c := NewDeleteClauses()
	c2 := c.WhereAppend(w)

	c3 := c.WhereAppend(w).WhereAppend(w2)

	c4 := c.WhereAppend(w, w2)

	assert.Nil(t, c.Where())

	assert.Equal(t, NewExpressionList(AndType, w), c2.Where())
	assert.Equal(t, NewExpressionList(AndType, w).Append(w2), c3.Where())
	assert.Equal(t, NewExpressionList(AndType, w, w2), c4.Where())
}

func (dcs *deleteClausesSuite) TestOrder() {
	t := dcs.T()
	oe := NewIdentifierExpression("", "", "a").Desc()

	c := NewDeleteClauses()
	c2 := c.SetOrder(oe)

	assert.Nil(t, c.Order())

	assert.Equal(t, NewColumnListExpression(oe), c2.Order())
}

func (dcs *deleteClausesSuite) TestHasOrder() {
	t := dcs.T()
	oe := NewIdentifierExpression("", "", "a").Desc()

	c := NewDeleteClauses()
	c2 := c.SetOrder(oe)

	assert.False(t, c.HasOrder())

	assert.True(t, c2.HasOrder())
}

func (dcs *deleteClausesSuite) TestClearOrder() {
	t := dcs.T()
	oe := NewIdentifierExpression("", "", "a").Desc()

	c := NewDeleteClauses().SetOrder(oe)
	c2 := c.ClearOrder()

	assert.Equal(t, NewColumnListExpression(oe), c.Order())

	assert.Nil(t, c2.Order())
}

func (dcs *deleteClausesSuite) TestSetOrder() {
	t := dcs.T()
	oe := NewIdentifierExpression("", "", "a").Desc()
	oe2 := NewIdentifierExpression("", "", "b").Desc()

	c := NewDeleteClauses().SetOrder(oe)
	c2 := c.SetOrder(oe2)

	assert.Equal(t, NewColumnListExpression(oe), c.Order())

	assert.Equal(t, NewColumnListExpression(oe2), c2.Order())
}

func (dcs *deleteClausesSuite) TestOrderAppend() {
	t := dcs.T()
	oe := NewIdentifierExpression("", "", "a").Desc()
	oe2 := NewIdentifierExpression("", "", "b").Desc()

	c := NewDeleteClauses().SetOrder(oe)
	c2 := c.OrderAppend(oe2)

	assert.Equal(t, NewColumnListExpression(oe), c.Order())

	assert.Equal(t, NewColumnListExpression(oe, oe2), c2.Order())
}

func (dcs *deleteClausesSuite) TestOrderPrepend() {
	t := dcs.T()
	oe := NewIdentifierExpression("", "", "a").Desc()
	oe2 := NewIdentifierExpression("", "", "b").Desc()

	c := NewDeleteClauses().SetOrder(oe)
	c2 := c.OrderPrepend(oe2)

	assert.Equal(t, NewColumnListExpression(oe), c.Order())

	assert.Equal(t, NewColumnListExpression(oe2, oe), c2.Order())
}

func (dcs *deleteClausesSuite) TestLimit() {
	t := dcs.T()
	l := 1

	c := NewDeleteClauses()
	c2 := c.SetLimit(l)

	assert.Nil(t, c.Limit())

	assert.Equal(t, l, c2.Limit())
}

func (dcs *deleteClausesSuite) TestHasLimit() {
	t := dcs.T()
	l := 1

	c := NewDeleteClauses()
	c2 := c.SetLimit(l)

	assert.False(t, c.HasLimit())

	assert.True(t, c2.HasLimit())
}

func (dcs *deleteClausesSuite) TestCLearLimit() {
	t := dcs.T()
	l := 1

	c := NewDeleteClauses().SetLimit(l)
	c2 := c.ClearLimit()

	assert.True(t, c.HasLimit())

	assert.False(t, c2.HasLimit())
}

func (dcs *deleteClausesSuite) TestSetLimit() {
	t := dcs.T()
	l := 1
	l2 := 2

	c := NewDeleteClauses().SetLimit(l)
	c2 := c.SetLimit(2)

	assert.Equal(t, l, c.Limit())

	assert.Equal(t, l2, c2.Limit())
}

func (dcs *deleteClausesSuite) TestCommonTables() {
	t := dcs.T()

	cte := NewCommonTableExpression(true, "test", newTestAppendableExpression(`SELECT * FROM "foo"`, []interface{}{}))

	c := NewDeleteClauses()
	c2 := c.CommonTablesAppend(cte)

	assert.Nil(t, c.CommonTables())

	assert.Equal(t, []CommonTableExpression{cte}, c2.CommonTables())
}

func (dcs *deleteClausesSuite) TestAddCommonTablesAppend() {
	t := dcs.T()

	cte := NewCommonTableExpression(true, "test", testSQLExpression("test_cte"))
	cte2 := NewCommonTableExpression(true, "test", testSQLExpression("test_cte2"))

	c := NewDeleteClauses().CommonTablesAppend(cte)
	c2 := c.CommonTablesAppend(cte2)

	assert.Equal(t, []CommonTableExpression{cte}, c.CommonTables())

	assert.Equal(t, []CommonTableExpression{cte, cte2}, c2.CommonTables())
}

func (dcs *deleteClausesSuite) TestReturning() {
	t := dcs.T()

	cl := NewColumnListExpression(NewIdentifierExpression("", "", "col"))

	c := NewDeleteClauses()
	c2 := c.SetReturning(cl)

	assert.Nil(t, c.Returning())

	assert.Equal(t, cl, c2.Returning())
}

func (dcs *deleteClausesSuite) TestHasReturning() {
	t := dcs.T()

	cl := NewColumnListExpression(NewIdentifierExpression("", "", "col"))

	c := NewDeleteClauses()
	c2 := c.SetReturning(cl)

	assert.False(t, c.HasReturning())

	assert.True(t, c2.HasReturning())
}

func (dcs *deleteClausesSuite) TestSetReturning() {
	t := dcs.T()

	cl := NewColumnListExpression(NewIdentifierExpression("", "", "col"))
	cl2 := NewColumnListExpression(NewIdentifierExpression("", "", "col2"))

	c := NewDeleteClauses().SetReturning(cl)
	c2 := c.SetReturning(cl2)

	assert.Equal(t, cl, c.Returning())

	assert.Equal(t, cl2, c2.Returning())
}
