package exp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type updateClausesSuite struct {
	suite.Suite
}

func TestUpdateClausesSuite(t *testing.T) {
	suite.Run(t, new(updateClausesSuite))
}

func (ucs *updateClausesSuite) TestHasTable() {
	t := ucs.T()
	c := NewUpdateClauses()
	c2 := c.SetTable(NewIdentifierExpression("", "test", ""))

	assert.False(t, c.HasTable())

	assert.True(t, c2.HasTable())
}

func (ucs *updateClausesSuite) TestTable() {
	t := ucs.T()
	c := NewUpdateClauses()
	ti := NewIdentifierExpression("", "a", "")
	c2 := c.SetTable(ti)

	assert.Nil(t, c.Table())

	assert.Equal(t, ti, c2.Table())
}

func (ucs *updateClausesSuite) TestSetTable() {
	t := ucs.T()
	c := NewUpdateClauses()
	ti := NewIdentifierExpression("", "a", "")
	c2 := c.SetTable(ti)

	assert.Nil(t, c.Table())

	assert.Equal(t, ti, c2.Table())
}

func (ucs *updateClausesSuite) TestSetValues() {
	t := ucs.T()
	c := NewUpdateClauses()
	r := Record{"a": "a1", "b": "b1"}
	c2 := c.SetSetValues(r)

	assert.Nil(t, c.SetValues())

	assert.Equal(t, r, c2.SetValues())
}

func (ucs *updateClausesSuite) TestSetSetValues() {
	t := ucs.T()
	r := Record{"a": "a1", "b": "b1"}
	c := NewUpdateClauses().SetSetValues(r)
	r2 := Record{"a": "a2", "b": "b2"}
	c2 := c.SetSetValues(r2)

	assert.Equal(t, r, c.SetValues())

	assert.Equal(t, r2, c2.SetValues())
}

func (ucs *updateClausesSuite) TestFrom() {
	t := ucs.T()
	c := NewUpdateClauses()
	ce := NewColumnListExpression("a", "b")
	c2 := c.SetFrom(ce)

	assert.Nil(t, c.From())

	assert.Equal(t, ce, c2.From())
}

func (ucs *updateClausesSuite) TestSetFrom() {
	t := ucs.T()
	ce1 := NewColumnListExpression("a", "b")
	c := NewUpdateClauses().SetFrom(ce1)
	ce2 := NewColumnListExpression("a", "b")
	c2 := c.SetFrom(ce2)

	assert.Equal(t, ce1, c.From())

	assert.Equal(t, ce2, c2.From())
}

func (ucs *updateClausesSuite) TestWhere() {
	t := ucs.T()
	w := Ex{"a": 1}

	c := NewUpdateClauses()
	c2 := c.WhereAppend(w)

	assert.Nil(t, c.Where())

	assert.Equal(t, NewExpressionList(AndType, w), c2.Where())
}

func (ucs *updateClausesSuite) TestClearWhere() {
	t := ucs.T()
	w := Ex{"a": 1}

	c := NewUpdateClauses().WhereAppend(w)
	c2 := c.ClearWhere()

	assert.Equal(t, NewExpressionList(AndType, w), c.Where())

	assert.Nil(t, c2.Where())
}

func (ucs *updateClausesSuite) TestWhereAppend() {
	t := ucs.T()
	w := Ex{"a": 1}
	w2 := Ex{"b": 2}

	c := NewUpdateClauses()
	c2 := c.WhereAppend(w)

	c3 := c.WhereAppend(w).WhereAppend(w2)

	c4 := c.WhereAppend(w, w2)

	assert.Nil(t, c.Where())

	assert.Equal(t, NewExpressionList(AndType, w), c2.Where())
	assert.Equal(t, NewExpressionList(AndType, w).Append(w2), c3.Where())
	assert.Equal(t, NewExpressionList(AndType, w, w2), c4.Where())
}

func (ucs *updateClausesSuite) TestOrder() {
	t := ucs.T()
	oe := NewIdentifierExpression("", "", "a").Desc()

	c := NewUpdateClauses()
	c2 := c.SetOrder(oe)

	assert.Nil(t, c.Order())

	assert.Equal(t, NewColumnListExpression(oe), c2.Order())
}

func (ucs *updateClausesSuite) TestHasOrder() {
	t := ucs.T()
	oe := NewIdentifierExpression("", "", "a").Desc()

	c := NewUpdateClauses()
	c2 := c.SetOrder(oe)

	assert.False(t, c.HasOrder())

	assert.True(t, c2.HasOrder())
}

func (ucs *updateClausesSuite) TestClearOrder() {
	t := ucs.T()
	oe := NewIdentifierExpression("", "", "a").Desc()

	c := NewUpdateClauses().SetOrder(oe)
	c2 := c.ClearOrder()

	assert.Equal(t, NewColumnListExpression(oe), c.Order())

	assert.Nil(t, c2.Order())
}

func (ucs *updateClausesSuite) TestSetOrder() {
	t := ucs.T()
	oe := NewIdentifierExpression("", "", "a").Desc()
	oe2 := NewIdentifierExpression("", "", "b").Desc()

	c := NewUpdateClauses().SetOrder(oe)
	c2 := c.SetOrder(oe2)

	assert.Equal(t, NewColumnListExpression(oe), c.Order())

	assert.Equal(t, NewColumnListExpression(oe2), c2.Order())
}

func (ucs *updateClausesSuite) TestOrderAppend() {
	t := ucs.T()
	oe := NewIdentifierExpression("", "", "a").Desc()
	oe2 := NewIdentifierExpression("", "", "b").Desc()

	c := NewUpdateClauses().SetOrder(oe)
	c2 := c.OrderAppend(oe2)

	assert.Equal(t, NewColumnListExpression(oe), c.Order())

	assert.Equal(t, NewColumnListExpression(oe, oe2), c2.Order())
}

func (ucs *updateClausesSuite) TestOrderPrepend() {
	t := ucs.T()
	oe := NewIdentifierExpression("", "", "a").Desc()
	oe2 := NewIdentifierExpression("", "", "b").Desc()

	c := NewUpdateClauses().SetOrder(oe)
	c2 := c.OrderPrepend(oe2)

	assert.Equal(t, NewColumnListExpression(oe), c.Order())

	assert.Equal(t, NewColumnListExpression(oe2, oe), c2.Order())
}

func (ucs *updateClausesSuite) TestLimit() {
	t := ucs.T()
	l := 1

	c := NewUpdateClauses()
	c2 := c.SetLimit(l)

	assert.Nil(t, c.Limit())

	assert.Equal(t, l, c2.Limit())
}

func (ucs *updateClausesSuite) TestHasLimit() {
	t := ucs.T()
	l := 1

	c := NewUpdateClauses()
	c2 := c.SetLimit(l)

	assert.False(t, c.HasLimit())

	assert.True(t, c2.HasLimit())
}

func (ucs *updateClausesSuite) TestCLearLimit() {
	t := ucs.T()
	l := 1

	c := NewUpdateClauses().SetLimit(l)
	c2 := c.ClearLimit()

	assert.True(t, c.HasLimit())

	assert.False(t, c2.HasLimit())
}

func (ucs *updateClausesSuite) TestSetLimit() {
	t := ucs.T()
	l := 1
	l2 := 2

	c := NewUpdateClauses().SetLimit(l)
	c2 := c.SetLimit(2)

	assert.Equal(t, l, c.Limit())

	assert.Equal(t, l2, c2.Limit())
}

func (ucs *updateClausesSuite) TestCommonTables() {
	t := ucs.T()

	cte := NewCommonTableExpression(true, "test", newTestAppendableExpression(`SELECT * FROM "foo"`, []interface{}{}))

	c := NewUpdateClauses()
	c2 := c.CommonTablesAppend(cte)

	assert.Nil(t, c.CommonTables())

	assert.Equal(t, []CommonTableExpression{cte}, c2.CommonTables())
}

func (ucs *updateClausesSuite) TestAddCommonTablesAppend() {
	t := ucs.T()

	cte := NewCommonTableExpression(true, "test", testSQLExpression("test_cte"))
	cte2 := NewCommonTableExpression(true, "test", testSQLExpression("test_cte2"))

	c := NewUpdateClauses().CommonTablesAppend(cte)
	c2 := c.CommonTablesAppend(cte2)

	assert.Equal(t, []CommonTableExpression{cte}, c.CommonTables())

	assert.Equal(t, []CommonTableExpression{cte, cte2}, c2.CommonTables())
}

func (ucs *updateClausesSuite) TestReturning() {
	t := ucs.T()

	cl := NewColumnListExpression(NewIdentifierExpression("", "", "col"))

	c := NewUpdateClauses()
	c2 := c.SetReturning(cl)

	assert.Nil(t, c.Returning())

	assert.Equal(t, cl, c2.Returning())
}

func (ucs *updateClausesSuite) TestHasReturning() {
	t := ucs.T()

	cl := NewColumnListExpression(NewIdentifierExpression("", "", "col"))

	c := NewUpdateClauses()
	c2 := c.SetReturning(cl)

	assert.False(t, c.HasReturning())

	assert.True(t, c2.HasReturning())
}

func (ucs *updateClausesSuite) TestSetReturning() {
	t := ucs.T()

	cl := NewColumnListExpression(NewIdentifierExpression("", "", "col"))
	cl2 := NewColumnListExpression(NewIdentifierExpression("", "", "col2"))

	c := NewUpdateClauses().SetReturning(cl)
	c2 := c.SetReturning(cl2)

	assert.Equal(t, cl, c.Returning())

	assert.Equal(t, cl2, c2.Returning())
}
