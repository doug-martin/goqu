package exp

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type updateClausesSuite struct {
	suite.Suite
}

func TestUpdateClausesSuite(t *testing.T) {
	suite.Run(t, new(updateClausesSuite))
}

func (ucs *updateClausesSuite) TestHasTable() {
	c := NewUpdateClauses()
	c2 := c.SetTable(NewIdentifierExpression("", "test", ""))

	ucs.False(c.HasTable())

	ucs.True(c2.HasTable())
}

func (ucs *updateClausesSuite) TestTable() {
	c := NewUpdateClauses()
	ti := NewIdentifierExpression("", "a", "")
	c2 := c.SetTable(ti)

	ucs.Nil(c.Table())

	ucs.Equal(ti, c2.Table())
}

func (ucs *updateClausesSuite) TestSetTable() {
	c := NewUpdateClauses()
	ti := NewIdentifierExpression("", "a", "")
	c2 := c.SetTable(ti)

	ucs.Nil(c.Table())

	ucs.Equal(ti, c2.Table())
}

func (ucs *updateClausesSuite) TestSetValues() {
	c := NewUpdateClauses()
	r := Record{"a": "a1", "b": "b1"}
	c2 := c.SetSetValues(r)

	ucs.Nil(c.SetValues())

	ucs.Equal(r, c2.SetValues())
}

func (ucs *updateClausesSuite) TestSetSetValues() {
	r := Record{"a": "a1", "b": "b1"}
	c := NewUpdateClauses().SetSetValues(r)
	r2 := Record{"a": "a2", "b": "b2"}
	c2 := c.SetSetValues(r2)

	ucs.Equal(r, c.SetValues())

	ucs.Equal(r2, c2.SetValues())
}

func (ucs *updateClausesSuite) TestFrom() {
	c := NewUpdateClauses()
	ce := NewColumnListExpression("a", "b")
	c2 := c.SetFrom(ce)

	ucs.Nil(c.From())

	ucs.Equal(ce, c2.From())
}

func (ucs *updateClausesSuite) TestSetFrom() {
	ce1 := NewColumnListExpression("a", "b")
	c := NewUpdateClauses().SetFrom(ce1)
	ce2 := NewColumnListExpression("a", "b")
	c2 := c.SetFrom(ce2)

	ucs.Equal(ce1, c.From())

	ucs.Equal(ce2, c2.From())
}

func (ucs *updateClausesSuite) TestWhere() {
	w := Ex{"a": 1}

	c := NewUpdateClauses()
	c2 := c.WhereAppend(w)

	ucs.Nil(c.Where())

	ucs.Equal(NewExpressionList(AndType, w), c2.Where())
}

func (ucs *updateClausesSuite) TestClearWhere() {
	w := Ex{"a": 1}

	c := NewUpdateClauses().WhereAppend(w)
	c2 := c.ClearWhere()

	ucs.Equal(NewExpressionList(AndType, w), c.Where())

	ucs.Nil(c2.Where())
}

func (ucs *updateClausesSuite) TestWhereAppend() {
	w := Ex{"a": 1}
	w2 := Ex{"b": 2}

	c := NewUpdateClauses()
	c2 := c.WhereAppend(w)

	c3 := c.WhereAppend(w).WhereAppend(w2)

	c4 := c.WhereAppend(w, w2)

	ucs.Nil(c.Where())

	ucs.Equal(NewExpressionList(AndType, w), c2.Where())
	ucs.Equal(NewExpressionList(AndType, w).Append(w2), c3.Where())
	ucs.Equal(NewExpressionList(AndType, w, w2), c4.Where())
}

func (ucs *updateClausesSuite) TestOrder() {
	oe := NewIdentifierExpression("", "", "a").Desc()

	c := NewUpdateClauses()
	c2 := c.SetOrder(oe)

	ucs.Nil(c.Order())

	ucs.Equal(NewColumnListExpression(oe), c2.Order())
}

func (ucs *updateClausesSuite) TestHasOrder() {
	oe := NewIdentifierExpression("", "", "a").Desc()

	c := NewUpdateClauses()
	c2 := c.SetOrder(oe)

	ucs.False(c.HasOrder())

	ucs.True(c2.HasOrder())
}

func (ucs *updateClausesSuite) TestClearOrder() {
	oe := NewIdentifierExpression("", "", "a").Desc()

	c := NewUpdateClauses().SetOrder(oe)
	c2 := c.ClearOrder()

	ucs.Equal(NewColumnListExpression(oe), c.Order())

	ucs.Nil(c2.Order())
}

func (ucs *updateClausesSuite) TestSetOrder() {
	oe := NewIdentifierExpression("", "", "a").Desc()
	oe2 := NewIdentifierExpression("", "", "b").Desc()

	c := NewUpdateClauses().SetOrder(oe)
	c2 := c.SetOrder(oe2)

	ucs.Equal(NewColumnListExpression(oe), c.Order())

	ucs.Equal(NewColumnListExpression(oe2), c2.Order())
}

func (ucs *updateClausesSuite) TestOrderAppend() {
	oe := NewIdentifierExpression("", "", "a").Desc()
	oe2 := NewIdentifierExpression("", "", "b").Desc()

	c := NewUpdateClauses().SetOrder(oe)
	c2 := c.OrderAppend(oe2)

	ucs.Equal(NewColumnListExpression(oe), c.Order())

	ucs.Equal(NewColumnListExpression(oe, oe2), c2.Order())
}

func (ucs *updateClausesSuite) TestOrderPrepend() {
	oe := NewIdentifierExpression("", "", "a").Desc()
	oe2 := NewIdentifierExpression("", "", "b").Desc()

	c := NewUpdateClauses().SetOrder(oe)
	c2 := c.OrderPrepend(oe2)

	ucs.Equal(NewColumnListExpression(oe), c.Order())

	ucs.Equal(NewColumnListExpression(oe2, oe), c2.Order())
}

func (ucs *updateClausesSuite) TestLimit() {
	l := 1

	c := NewUpdateClauses()
	c2 := c.SetLimit(l)

	ucs.Nil(c.Limit())

	ucs.Equal(l, c2.Limit())
}

func (ucs *updateClausesSuite) TestHasLimit() {
	l := 1

	c := NewUpdateClauses()
	c2 := c.SetLimit(l)

	ucs.False(c.HasLimit())

	ucs.True(c2.HasLimit())
}

func (ucs *updateClausesSuite) TestCLearLimit() {
	l := 1

	c := NewUpdateClauses().SetLimit(l)
	c2 := c.ClearLimit()

	ucs.True(c.HasLimit())

	ucs.False(c2.HasLimit())
}

func (ucs *updateClausesSuite) TestSetLimit() {
	l := 1
	l2 := 2

	c := NewUpdateClauses().SetLimit(l)
	c2 := c.SetLimit(2)

	ucs.Equal(l, c.Limit())

	ucs.Equal(l2, c2.Limit())
}

func (ucs *updateClausesSuite) TestCommonTables() {

	cte := NewCommonTableExpression(true, "test", newTestAppendableExpression(`SELECT * FROM "foo"`, []interface{}{}))

	c := NewUpdateClauses()
	c2 := c.CommonTablesAppend(cte)

	ucs.Nil(c.CommonTables())

	ucs.Equal([]CommonTableExpression{cte}, c2.CommonTables())
}

func (ucs *updateClausesSuite) TestAddCommonTablesAppend() {

	cte := NewCommonTableExpression(true, "test", testSQLExpression("test_cte"))
	cte2 := NewCommonTableExpression(true, "test", testSQLExpression("test_cte2"))

	c := NewUpdateClauses().CommonTablesAppend(cte)
	c2 := c.CommonTablesAppend(cte2)

	ucs.Equal([]CommonTableExpression{cte}, c.CommonTables())

	ucs.Equal([]CommonTableExpression{cte, cte2}, c2.CommonTables())
}

func (ucs *updateClausesSuite) TestReturning() {

	cl := NewColumnListExpression(NewIdentifierExpression("", "", "col"))

	c := NewUpdateClauses()
	c2 := c.SetReturning(cl)

	ucs.Nil(c.Returning())

	ucs.Equal(cl, c2.Returning())
}

func (ucs *updateClausesSuite) TestHasReturning() {

	cl := NewColumnListExpression(NewIdentifierExpression("", "", "col"))

	c := NewUpdateClauses()
	c2 := c.SetReturning(cl)

	ucs.False(c.HasReturning())

	ucs.True(c2.HasReturning())
}

func (ucs *updateClausesSuite) TestSetReturning() {

	cl := NewColumnListExpression(NewIdentifierExpression("", "", "col"))
	cl2 := NewColumnListExpression(NewIdentifierExpression("", "", "col2"))

	c := NewUpdateClauses().SetReturning(cl)
	c2 := c.SetReturning(cl2)

	ucs.Equal(cl, c.Returning())

	ucs.Equal(cl2, c2.Returning())
}
