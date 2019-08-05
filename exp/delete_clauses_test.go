package exp

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type deleteClausesSuite struct {
	suite.Suite
}

func TestDeleteClausesSuite(t *testing.T) {
	suite.Run(t, new(deleteClausesSuite))
}

func (dcs *deleteClausesSuite) TestHasFrom() {
	c := NewDeleteClauses()
	c2 := c.SetFrom(NewIdentifierExpression("", "test", ""))

	dcs.False(c.HasFrom())

	dcs.True(c2.HasFrom())
}

func (dcs *deleteClausesSuite) TestFrom() {
	c := NewDeleteClauses()
	ti := NewIdentifierExpression("", "a", "")
	c2 := c.SetFrom(ti)

	dcs.Nil(c.From())

	dcs.Equal(ti, c2.From())
}

func (dcs *deleteClausesSuite) TestSetFrom() {
	c := NewDeleteClauses()
	ti := NewIdentifierExpression("", "a", "")
	c2 := c.SetFrom(ti)

	dcs.Nil(c.From())

	dcs.Equal(ti, c2.From())
}

func (dcs *deleteClausesSuite) TestWhere() {
	w := Ex{"a": 1}

	c := NewDeleteClauses()
	c2 := c.WhereAppend(w)

	dcs.Nil(c.Where())

	dcs.Equal(NewExpressionList(AndType, w), c2.Where())
}

func (dcs *deleteClausesSuite) TestClearWhere() {
	w := Ex{"a": 1}

	c := NewDeleteClauses().WhereAppend(w)
	c2 := c.ClearWhere()

	dcs.Equal(NewExpressionList(AndType, w), c.Where())

	dcs.Nil(c2.Where())
}

func (dcs *deleteClausesSuite) TestWhereAppend() {
	w := Ex{"a": 1}
	w2 := Ex{"b": 2}

	c := NewDeleteClauses()
	c2 := c.WhereAppend(w)

	c3 := c.WhereAppend(w).WhereAppend(w2)

	c4 := c.WhereAppend(w, w2)

	dcs.Nil(c.Where())

	dcs.Equal(NewExpressionList(AndType, w), c2.Where())
	dcs.Equal(NewExpressionList(AndType, w).Append(w2), c3.Where())
	dcs.Equal(NewExpressionList(AndType, w, w2), c4.Where())
}

func (dcs *deleteClausesSuite) TestOrder() {
	oe := NewIdentifierExpression("", "", "a").Desc()

	c := NewDeleteClauses()
	c2 := c.SetOrder(oe)

	dcs.Nil(c.Order())

	dcs.Equal(NewColumnListExpression(oe), c2.Order())
}

func (dcs *deleteClausesSuite) TestHasOrder() {
	oe := NewIdentifierExpression("", "", "a").Desc()

	c := NewDeleteClauses()
	c2 := c.SetOrder(oe)

	dcs.False(c.HasOrder())

	dcs.True(c2.HasOrder())
}

func (dcs *deleteClausesSuite) TestClearOrder() {
	oe := NewIdentifierExpression("", "", "a").Desc()

	c := NewDeleteClauses().SetOrder(oe)
	c2 := c.ClearOrder()

	dcs.Equal(NewColumnListExpression(oe), c.Order())

	dcs.Nil(c2.Order())
}

func (dcs *deleteClausesSuite) TestSetOrder() {
	oe := NewIdentifierExpression("", "", "a").Desc()
	oe2 := NewIdentifierExpression("", "", "b").Desc()

	c := NewDeleteClauses().SetOrder(oe)
	c2 := c.SetOrder(oe2)

	dcs.Equal(NewColumnListExpression(oe), c.Order())

	dcs.Equal(NewColumnListExpression(oe2), c2.Order())
}

func (dcs *deleteClausesSuite) TestOrderAppend() {
	oe := NewIdentifierExpression("", "", "a").Desc()
	oe2 := NewIdentifierExpression("", "", "b").Desc()

	c := NewDeleteClauses().SetOrder(oe)
	c2 := c.OrderAppend(oe2)

	dcs.Equal(NewColumnListExpression(oe), c.Order())

	dcs.Equal(NewColumnListExpression(oe, oe2), c2.Order())
}

func (dcs *deleteClausesSuite) TestOrderPrepend() {
	oe := NewIdentifierExpression("", "", "a").Desc()
	oe2 := NewIdentifierExpression("", "", "b").Desc()

	c := NewDeleteClauses().SetOrder(oe)
	c2 := c.OrderPrepend(oe2)

	dcs.Equal(NewColumnListExpression(oe), c.Order())

	dcs.Equal(NewColumnListExpression(oe2, oe), c2.Order())
}

func (dcs *deleteClausesSuite) TestLimit() {
	l := 1

	c := NewDeleteClauses()
	c2 := c.SetLimit(l)

	dcs.Nil(c.Limit())

	dcs.Equal(l, c2.Limit())
}

func (dcs *deleteClausesSuite) TestHasLimit() {
	l := 1

	c := NewDeleteClauses()
	c2 := c.SetLimit(l)

	dcs.False(c.HasLimit())

	dcs.True(c2.HasLimit())
}

func (dcs *deleteClausesSuite) TestCLearLimit() {
	l := 1

	c := NewDeleteClauses().SetLimit(l)
	c2 := c.ClearLimit()

	dcs.True(c.HasLimit())

	dcs.False(c2.HasLimit())
}

func (dcs *deleteClausesSuite) TestSetLimit() {
	l := 1
	l2 := 2

	c := NewDeleteClauses().SetLimit(l)
	c2 := c.SetLimit(2)

	dcs.Equal(l, c.Limit())

	dcs.Equal(l2, c2.Limit())
}

func (dcs *deleteClausesSuite) TestCommonTables() {
	cte := NewCommonTableExpression(true, "test", newTestAppendableExpression(`SELECT * FROM "foo"`, []interface{}{}))

	c := NewDeleteClauses()
	c2 := c.CommonTablesAppend(cte)

	dcs.Nil(c.CommonTables())

	dcs.Equal([]CommonTableExpression{cte}, c2.CommonTables())
}

func (dcs *deleteClausesSuite) TestAddCommonTablesAppend() {
	cte := NewCommonTableExpression(true, "test", testSQLExpression("test_cte"))
	cte2 := NewCommonTableExpression(true, "test", testSQLExpression("test_cte2"))

	c := NewDeleteClauses().CommonTablesAppend(cte)
	c2 := c.CommonTablesAppend(cte2)

	dcs.Equal([]CommonTableExpression{cte}, c.CommonTables())

	dcs.Equal([]CommonTableExpression{cte, cte2}, c2.CommonTables())
}

func (dcs *deleteClausesSuite) TestReturning() {
	cl := NewColumnListExpression(NewIdentifierExpression("", "", "col"))

	c := NewDeleteClauses()
	c2 := c.SetReturning(cl)

	dcs.Nil(c.Returning())

	dcs.Equal(cl, c2.Returning())
}

func (dcs *deleteClausesSuite) TestHasReturning() {
	cl := NewColumnListExpression(NewIdentifierExpression("", "", "col"))

	c := NewDeleteClauses()
	c2 := c.SetReturning(cl)

	dcs.False(c.HasReturning())

	dcs.True(c2.HasReturning())
}

func (dcs *deleteClausesSuite) TestSetReturning() {
	cl := NewColumnListExpression(NewIdentifierExpression("", "", "col"))
	cl2 := NewColumnListExpression(NewIdentifierExpression("", "", "col2"))

	c := NewDeleteClauses().SetReturning(cl)
	c2 := c.SetReturning(cl2)

	dcs.Equal(cl, c.Returning())

	dcs.Equal(cl2, c2.Returning())
}
