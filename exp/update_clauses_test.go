package exp_test

import (
	"testing"

	"github.com/doug-martin/goqu/v9/exp"
	"github.com/stretchr/testify/suite"
)

type updateClausesSuite struct {
	suite.Suite
}

func TestUpdateClausesSuite(t *testing.T) {
	suite.Run(t, new(updateClausesSuite))
}

func (ucs *updateClausesSuite) TestHasTable() {
	c := exp.NewUpdateClauses()
	c2 := c.SetTable(exp.NewIdentifierExpression("", "test", ""))

	ucs.False(c.HasTable())

	ucs.True(c2.HasTable())
}

func (ucs *updateClausesSuite) TestTable() {
	c := exp.NewUpdateClauses()
	ti := exp.NewIdentifierExpression("", "a", "")
	c2 := c.SetTable(ti)

	ucs.Nil(c.Table())

	ucs.Equal(ti, c2.Table())
}

func (ucs *updateClausesSuite) TestSetTable() {
	c := exp.NewUpdateClauses()
	ti := exp.NewIdentifierExpression("", "a", "")
	c2 := c.SetTable(ti)

	ucs.Nil(c.Table())

	ucs.Equal(ti, c2.Table())
}

func (ucs *updateClausesSuite) TestSetValues() {
	c := exp.NewUpdateClauses()
	r := exp.Record{"a": "a1", "b": "b1"}
	c2 := c.SetSetValues(r)

	ucs.Nil(c.SetValues())

	ucs.Equal(r, c2.SetValues())
}

func (ucs *updateClausesSuite) TestSetSetValues() {
	r := exp.Record{"a": "a1", "b": "b1"}
	c := exp.NewUpdateClauses().SetSetValues(r)
	r2 := exp.Record{"a": "a2", "b": "b2"}
	c2 := c.SetSetValues(r2)

	ucs.Equal(r, c.SetValues())

	ucs.Equal(r2, c2.SetValues())
}

func (ucs *updateClausesSuite) TestFrom() {
	c := exp.NewUpdateClauses()
	ce := exp.NewColumnListExpression("a", "b")
	c2 := c.SetFrom(ce)

	ucs.Nil(c.From())

	ucs.Equal(ce, c2.From())
}

func (ucs *updateClausesSuite) TestSetFrom() {
	ce1 := exp.NewColumnListExpression("a", "b")
	c := exp.NewUpdateClauses().SetFrom(ce1)
	ce2 := exp.NewColumnListExpression("a", "b")
	c2 := c.SetFrom(ce2)

	ucs.Equal(ce1, c.From())

	ucs.Equal(ce2, c2.From())
}

func (ucs *updateClausesSuite) TestWhere() {
	w := exp.Ex{"a": 1}

	c := exp.NewUpdateClauses()
	c2 := c.WhereAppend(w)

	ucs.Nil(c.Where())

	ucs.Equal(exp.NewExpressionList(exp.AndType, w), c2.Where())
}

func (ucs *updateClausesSuite) TestClearWhere() {
	w := exp.Ex{"a": 1}

	c := exp.NewUpdateClauses().WhereAppend(w)
	c2 := c.ClearWhere()

	ucs.Equal(exp.NewExpressionList(exp.AndType, w), c.Where())

	ucs.Nil(c2.Where())
}

func (ucs *updateClausesSuite) TestWhereAppend() {
	w := exp.Ex{"a": 1}
	w2 := exp.Ex{"b": 2}

	c := exp.NewUpdateClauses()
	c2 := c.WhereAppend(w)

	c3 := c.WhereAppend(w).WhereAppend(w2)

	c4 := c.WhereAppend(w, w2)

	ucs.Nil(c.Where())

	ucs.Equal(exp.NewExpressionList(exp.AndType, w), c2.Where())
	ucs.Equal(exp.NewExpressionList(exp.AndType, w).Append(w2), c3.Where())
	ucs.Equal(exp.NewExpressionList(exp.AndType, w, w2), c4.Where())
}

func (ucs *updateClausesSuite) TestOrder() {
	oe := exp.NewIdentifierExpression("", "", "a").Desc()

	c := exp.NewUpdateClauses()
	c2 := c.SetOrder(oe)

	ucs.Nil(c.Order())

	ucs.Equal(exp.NewColumnListExpression(oe), c2.Order())
}

func (ucs *updateClausesSuite) TestHasOrder() {
	oe := exp.NewIdentifierExpression("", "", "a").Desc()

	c := exp.NewUpdateClauses()
	c2 := c.SetOrder(oe)

	ucs.False(c.HasOrder())

	ucs.True(c2.HasOrder())
}

func (ucs *updateClausesSuite) TestClearOrder() {
	oe := exp.NewIdentifierExpression("", "", "a").Desc()

	c := exp.NewUpdateClauses().SetOrder(oe)
	c2 := c.ClearOrder()

	ucs.Equal(exp.NewColumnListExpression(oe), c.Order())

	ucs.Nil(c2.Order())
}

func (ucs *updateClausesSuite) TestSetOrder() {
	oe := exp.NewIdentifierExpression("", "", "a").Desc()
	oe2 := exp.NewIdentifierExpression("", "", "b").Desc()

	c := exp.NewUpdateClauses().SetOrder(oe)
	c2 := c.SetOrder(oe2)

	ucs.Equal(exp.NewColumnListExpression(oe), c.Order())

	ucs.Equal(exp.NewColumnListExpression(oe2), c2.Order())
}

func (ucs *updateClausesSuite) TestOrderAppend() {
	oe := exp.NewIdentifierExpression("", "", "a").Desc()
	oe2 := exp.NewIdentifierExpression("", "", "b").Desc()

	c := exp.NewUpdateClauses().SetOrder(oe)
	c2 := c.OrderAppend(oe2)

	ucs.Equal(exp.NewColumnListExpression(oe), c.Order())

	ucs.Equal(exp.NewColumnListExpression(oe, oe2), c2.Order())
}

func (ucs *updateClausesSuite) TestOrderPrepend() {
	oe := exp.NewIdentifierExpression("", "", "a").Desc()
	oe2 := exp.NewIdentifierExpression("", "", "b").Desc()

	c := exp.NewUpdateClauses().SetOrder(oe)
	c2 := c.OrderPrepend(oe2)

	ucs.Equal(exp.NewColumnListExpression(oe), c.Order())

	ucs.Equal(exp.NewColumnListExpression(oe2, oe), c2.Order())
}

func (ucs *updateClausesSuite) TestLimit() {
	l := 1

	c := exp.NewUpdateClauses()
	c2 := c.SetLimit(l)

	ucs.Nil(c.Limit())

	ucs.Equal(l, c2.Limit())
}

func (ucs *updateClausesSuite) TestHasLimit() {
	l := 1

	c := exp.NewUpdateClauses()
	c2 := c.SetLimit(l)

	ucs.False(c.HasLimit())

	ucs.True(c2.HasLimit())
}

func (ucs *updateClausesSuite) TestCLearLimit() {
	l := 1

	c := exp.NewUpdateClauses().SetLimit(l)
	c2 := c.ClearLimit()

	ucs.True(c.HasLimit())

	ucs.False(c2.HasLimit())
}

func (ucs *updateClausesSuite) TestSetLimit() {
	l := 1
	l2 := 2

	c := exp.NewUpdateClauses().SetLimit(l)
	c2 := c.SetLimit(2)

	ucs.Equal(l, c.Limit())

	ucs.Equal(l2, c2.Limit())
}

func (ucs *updateClausesSuite) TestCommonTables() {
	cte := exp.NewCommonTableExpression(true, "test", newTestAppendableExpression(`SELECT * FROM "foo"`, []interface{}{}))

	c := exp.NewUpdateClauses()
	c2 := c.CommonTablesAppend(cte)

	ucs.Nil(c.CommonTables())

	ucs.Equal([]exp.CommonTableExpression{cte}, c2.CommonTables())
}

func (ucs *updateClausesSuite) TestAddCommonTablesAppend() {
	cte := exp.NewCommonTableExpression(true, "test", testSQLExpression("test_cte"))
	cte2 := exp.NewCommonTableExpression(true, "test", testSQLExpression("test_cte2"))

	c := exp.NewUpdateClauses().CommonTablesAppend(cte)
	c2 := c.CommonTablesAppend(cte2)

	ucs.Equal([]exp.CommonTableExpression{cte}, c.CommonTables())

	ucs.Equal([]exp.CommonTableExpression{cte, cte2}, c2.CommonTables())
}

func (ucs *updateClausesSuite) TestReturning() {
	cl := exp.NewColumnListExpression(exp.NewIdentifierExpression("", "", "col"))

	c := exp.NewUpdateClauses()
	c2 := c.SetReturning(cl)

	ucs.Nil(c.Returning())

	ucs.Equal(cl, c2.Returning())
}

func (ucs *updateClausesSuite) TestHasReturning() {
	cl := exp.NewColumnListExpression(exp.NewIdentifierExpression("", "", "col"))

	c := exp.NewUpdateClauses()
	c2 := c.SetReturning(cl)

	ucs.False(c.HasReturning())

	ucs.True(c2.HasReturning())
}

func (ucs *updateClausesSuite) TestSetReturning() {
	cl := exp.NewColumnListExpression(exp.NewIdentifierExpression("", "", "col"))
	cl2 := exp.NewColumnListExpression(exp.NewIdentifierExpression("", "", "col2"))

	c := exp.NewUpdateClauses().SetReturning(cl)
	c2 := c.SetReturning(cl2)

	ucs.Equal(cl, c.Returning())

	ucs.Equal(cl2, c2.Returning())
}
