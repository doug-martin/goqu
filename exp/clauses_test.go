package exp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type testSQLExpression string

func (tse testSQLExpression) Expression() Expression {
	return tse
}

func (tse testSQLExpression) Clone() Expression {
	return tse
}

func (tse testSQLExpression) ToSQL() (sql string, args []interface{}, err error) {
	return "", nil, nil
}

type clausesTest struct {
	suite.Suite
}

func TestClausesSuite(t *testing.T) {
	suite.Run(t, new(clausesTest))
}

func (ct *clausesTest) TestHasSources() {
	t := ct.T()
	c := NewClauses()
	c2 := c.SetFrom(NewColumnListExpression("test"))

	assert.False(t, c.HasSources())

	assert.True(t, c2.HasSources())
}

func (ct *clausesTest) TestIsDefaultSelect() {
	t := ct.T()
	c := NewClauses()
	c2 := c.SelectAppend(NewColumnListExpression("a"))

	assert.True(t, c.IsDefaultSelect())

	assert.False(t, c2.IsDefaultSelect())
}

func (ct *clausesTest) TestSelect() {
	t := ct.T()
	c := NewClauses()
	c2 := c.SetSelect(NewColumnListExpression("a"))

	assert.Equal(t, NewColumnListExpression(Star()), c.Select())

	assert.Equal(t, NewColumnListExpression("a"), c2.Select())
}

func (ct *clausesTest) TestSelectAppend() {
	t := ct.T()
	c := NewClauses()
	c2 := c.SelectAppend(NewColumnListExpression("a"))

	assert.Equal(t, NewColumnListExpression(Star()), c.Select())
	assert.Nil(t, c.SelectDistinct())

	assert.Equal(t, NewColumnListExpression(Star(), "a"), c2.Select())
	assert.Nil(t, c2.SelectDistinct())
}

func (ct *clausesTest) TestSetSelect() {
	t := ct.T()
	c := NewClauses()
	c2 := c.SetSelect(NewColumnListExpression("a"))

	assert.Equal(t, NewColumnListExpression(Star()), c.Select())
	assert.Nil(t, c.SelectDistinct())

	assert.Equal(t, NewColumnListExpression("a"), c2.Select())
	assert.Nil(t, c2.SelectDistinct())

}

func (ct *clausesTest) TestSelectDistinct() {
	t := ct.T()
	c := NewClauses()
	c2 := c.SetSelectDistinct(NewColumnListExpression("a"))

	assert.Nil(t, c.SelectDistinct())
	assert.Equal(t, NewColumnListExpression(Star()), c.Select())

	assert.Equal(t, NewColumnListExpression("a"), c2.SelectDistinct())
	assert.Nil(t, c2.Select())
}

func (ct *clausesTest) TestSetSelectDistinct() {
	t := ct.T()
	c := NewClauses()
	c2 := c.SetSelectDistinct(NewColumnListExpression("a"))

	assert.Nil(t, c.SelectDistinct())
	assert.Equal(t, NewColumnListExpression(Star()), c.Select())

	assert.Equal(t, NewColumnListExpression("a"), c2.SelectDistinct())
	assert.Nil(t, c2.Select())
}

func (ct *clausesTest) TestFrom() {
	t := ct.T()
	c := NewClauses()
	c2 := c.SetFrom(NewColumnListExpression("a"))

	assert.Nil(t, c.From())

	assert.Equal(t, NewColumnListExpression("a"), c2.From())
}

func (ct *clausesTest) TestSetFrom() {
	t := ct.T()
	c := NewClauses()
	c2 := c.SetFrom(NewColumnListExpression("a"))

	assert.Nil(t, c.From())

	assert.Equal(t, NewColumnListExpression("a"), c2.From())
}
func (ct *clausesTest) TestHasAlias() {
	t := ct.T()
	c := NewClauses()
	c2 := c.SetAlias(NewIdentifierExpression("", "", "a"))

	assert.False(t, c.HasAlias())

	assert.True(t, c2.HasAlias())
}

func (ct *clausesTest) TestAlias() {
	t := ct.T()
	c := NewClauses()
	a := NewIdentifierExpression("", "a", "")
	c2 := c.SetAlias(a)

	assert.Nil(t, c.Alias())

	assert.Equal(t, a, c2.Alias())
}

func (ct *clausesTest) TestSetAlias() {
	t := ct.T()
	c := NewClauses()
	a := NewIdentifierExpression("", "a", "")
	c2 := c.SetAlias(a)

	assert.Nil(t, c.Alias())

	assert.Equal(t, a, c2.Alias())
}

func (ct *clausesTest) TestJoins() {
	t := ct.T()

	jc := NewConditionedJoinExpression(
		LeftJoinType,
		NewIdentifierExpression("", "test", ""),
		nil,
	)
	c := NewClauses()
	c2 := c.JoinsAppend(jc)

	assert.Nil(t, c.Joins())

	assert.Equal(t, JoinExpressions{jc}, c2.Joins())
}

func (ct *clausesTest) TestJoinsAppend() {
	t := ct.T()
	jc := NewConditionedJoinExpression(
		LeftJoinType,
		NewIdentifierExpression("", "test", ""),
		nil,
	)
	jc2 := NewUnConditionedJoinExpression(
		LeftJoinType,
		NewIdentifierExpression("", "test", ""),
	)
	c := NewClauses()
	c2 := c.JoinsAppend(jc)
	c3 := c2.JoinsAppend(jc2)

	assert.Nil(t, c.Joins())

	assert.Equal(t, JoinExpressions{jc}, c2.Joins())
	assert.Equal(t, JoinExpressions{jc, jc2}, c3.Joins())
}

func (ct *clausesTest) TestWhere() {
	t := ct.T()
	w := Ex{"a": 1}

	c := NewClauses()
	c2 := c.WhereAppend(w)

	assert.Nil(t, c.Where())

	assert.Equal(t, NewExpressionList(AndType, w), c2.Where())
}

func (ct *clausesTest) TestClearWhere() {
	t := ct.T()
	w := Ex{"a": 1}

	c := NewClauses().WhereAppend(w)
	c2 := c.ClearWhere()

	assert.Equal(t, NewExpressionList(AndType, w), c.Where())

	assert.Nil(t, c2.Where())
}

func (ct *clausesTest) TestWhereAppend() {
	t := ct.T()
	w := Ex{"a": 1}
	w2 := Ex{"b": 2}

	c := NewClauses()
	c2 := c.WhereAppend(w)

	c3 := c.WhereAppend(w).WhereAppend(w2)

	c4 := c.WhereAppend(w, w2)

	assert.Nil(t, c.Where())

	assert.Equal(t, NewExpressionList(AndType, w), c2.Where())
	assert.Equal(t, NewExpressionList(AndType, w).Append(w2), c3.Where())
	assert.Equal(t, NewExpressionList(AndType, w, w2), c4.Where())
}

func (ct *clausesTest) TestHaving() {
	t := ct.T()
	w := Ex{"a": 1}

	c := NewClauses()
	c2 := c.HavingAppend(w)

	assert.Nil(t, c.Having())

	assert.Equal(t, NewExpressionList(AndType, w), c2.Having())
}

func (ct *clausesTest) TestClearHaving() {
	t := ct.T()
	w := Ex{"a": 1}

	c := NewClauses().HavingAppend(w)
	c2 := c.ClearHaving()

	assert.Equal(t, NewExpressionList(AndType, w), c.Having())

	assert.Nil(t, c2.Having())
}

func (ct *clausesTest) TestHavingAppend() {
	t := ct.T()
	w := Ex{"a": 1}
	w2 := Ex{"b": 2}

	c := NewClauses()
	c2 := c.HavingAppend(w)

	c3 := c.HavingAppend(w).HavingAppend(w2)

	c4 := c.HavingAppend(w, w2)

	assert.Nil(t, c.Having())

	assert.Equal(t, NewExpressionList(AndType, w), c2.Having())
	assert.Equal(t, NewExpressionList(AndType, w).Append(w2), c3.Having())
	assert.Equal(t, NewExpressionList(AndType, w, w2), c4.Having())
}

func (ct *clausesTest) TestOrder() {
	t := ct.T()
	oe := NewIdentifierExpression("", "", "a").Desc()

	c := NewClauses()
	c2 := c.SetOrder(oe)

	assert.Nil(t, c.Order())

	assert.Equal(t, NewColumnListExpression(oe), c2.Order())
}

func (ct *clausesTest) TestHasOrder() {
	t := ct.T()
	oe := NewIdentifierExpression("", "", "a").Desc()

	c := NewClauses()
	c2 := c.SetOrder(oe)

	assert.False(t, c.HasOrder())

	assert.True(t, c2.HasOrder())
}

func (ct *clausesTest) TestClearOrder() {
	t := ct.T()
	oe := NewIdentifierExpression("", "", "a").Desc()

	c := NewClauses().SetOrder(oe)
	c2 := c.ClearOrder()

	assert.Equal(t, NewColumnListExpression(oe), c.Order())

	assert.Nil(t, c2.Order())
}

func (ct *clausesTest) TestSetOrder() {
	t := ct.T()
	oe := NewIdentifierExpression("", "", "a").Desc()
	oe2 := NewIdentifierExpression("", "", "b").Desc()

	c := NewClauses().SetOrder(oe)
	c2 := c.SetOrder(oe2)

	assert.Equal(t, NewColumnListExpression(oe), c.Order())

	assert.Equal(t, NewColumnListExpression(oe2), c2.Order())
}

func (ct *clausesTest) TestOrderAppend() {
	t := ct.T()
	oe := NewIdentifierExpression("", "", "a").Desc()
	oe2 := NewIdentifierExpression("", "", "b").Desc()

	c := NewClauses().SetOrder(oe)
	c2 := c.OrderAppend(oe2)

	assert.Equal(t, NewColumnListExpression(oe), c.Order())

	assert.Equal(t, NewColumnListExpression(oe, oe2), c2.Order())
}

func (ct *clausesTest) TestOrderPrepend() {
	t := ct.T()
	oe := NewIdentifierExpression("", "", "a").Desc()
	oe2 := NewIdentifierExpression("", "", "b").Desc()

	c := NewClauses().SetOrder(oe)
	c2 := c.OrderPrepend(oe2)

	assert.Equal(t, NewColumnListExpression(oe), c.Order())

	assert.Equal(t, NewColumnListExpression(oe2, oe), c2.Order())
}

func (ct *clausesTest) TestGroupBy() {
	t := ct.T()
	g := NewColumnListExpression(NewIdentifierExpression("", "", "a"))

	c := NewClauses()
	c2 := c.SetGroupBy(g)

	assert.Nil(t, c.GroupBy())

	assert.Equal(t, g, c2.GroupBy())
}

func (ct *clausesTest) TestSetGroupBy() {
	t := ct.T()
	g := NewColumnListExpression(NewIdentifierExpression("", "", "a"))
	g2 := NewColumnListExpression(NewIdentifierExpression("", "", "b"))

	c := NewClauses().SetGroupBy(g)
	c2 := c.SetGroupBy(g2)

	assert.Equal(t, g, c.GroupBy())

	assert.Equal(t, g2, c2.GroupBy())
}

func (ct *clausesTest) TestLimit() {
	t := ct.T()
	l := 1

	c := NewClauses()
	c2 := c.SetLimit(l)

	assert.Nil(t, c.Limit())

	assert.Equal(t, l, c2.Limit())
}

func (ct *clausesTest) TestHasLimit() {
	t := ct.T()
	l := 1

	c := NewClauses()
	c2 := c.SetLimit(l)

	assert.False(t, c.HasLimit())

	assert.True(t, c2.HasLimit())
}

func (ct *clausesTest) TestCLearLimit() {
	t := ct.T()
	l := 1

	c := NewClauses().SetLimit(l)
	c2 := c.ClearLimit()

	assert.True(t, c.HasLimit())

	assert.False(t, c2.HasLimit())
}

func (ct *clausesTest) TestSetLimit() {
	t := ct.T()
	l := 1
	l2 := 2

	c := NewClauses().SetLimit(l)
	c2 := c.SetLimit(2)

	assert.Equal(t, l, c.Limit())

	assert.Equal(t, l2, c2.Limit())
}

func (ct *clausesTest) TestOffset() {
	t := ct.T()
	o := uint(1)

	c := NewClauses()
	c2 := c.SetOffset(o)

	assert.Equal(t, uint(0), c.Offset())

	assert.Equal(t, o, c2.Offset())
}

func (ct *clausesTest) TestClearOffset() {
	t := ct.T()
	o := uint(1)

	c := NewClauses().SetOffset(o)
	c2 := c.ClearOffset()

	assert.Equal(t, o, c.Offset())

	assert.Equal(t, uint(0), c2.Offset())
}

func (ct *clausesTest) TestSetOffset() {
	t := ct.T()
	o := uint(1)
	o2 := uint(2)

	c := NewClauses().SetOffset(o)
	c2 := c.SetOffset(2)

	assert.Equal(t, o, c.Offset())

	assert.Equal(t, o2, c2.Offset())
}

func (ct *clausesTest) TestCompounds() {
	t := ct.T()

	ce := NewCompoundExpression(UnionCompoundType, newTestAppendableExpression("SELECT * FROM foo", []interface{}{}))

	c := NewClauses()
	c2 := c.CompoundsAppend(ce)

	assert.Nil(t, c.Compounds())

	assert.Equal(t, []CompoundExpression{ce}, c2.Compounds())
}
func (ct *clausesTest) TestCompoundsAppend() {
	t := ct.T()

	ce := NewCompoundExpression(UnionCompoundType, newTestAppendableExpression("SELECT * FROM foo1", []interface{}{}))
	ce2 := NewCompoundExpression(UnionCompoundType, newTestAppendableExpression("SELECT * FROM foo2", []interface{}{}))

	c := NewClauses().CompoundsAppend(ce)
	c2 := c.CompoundsAppend(ce2)

	assert.Equal(t, []CompoundExpression{ce}, c.Compounds())

	assert.Equal(t, []CompoundExpression{ce, ce2}, c2.Compounds())
}

func (ct *clausesTest) TestLock() {
	t := ct.T()

	l := NewLock(ForUpdate, Wait)

	c := NewClauses()
	c2 := c.SetLock(l)

	assert.Nil(t, c.Lock())

	assert.Equal(t, l, c2.Lock())
}

func (ct *clausesTest) TestSetLock() {
	t := ct.T()

	l := NewLock(ForUpdate, Wait)
	l2 := NewLock(ForUpdate, NoWait)

	c := NewClauses().SetLock(l)
	c2 := c.SetLock(l2)

	assert.Equal(t, l, c.Lock())

	assert.Equal(t, l2, c2.Lock())
}

func (ct *clausesTest) TestCommonTables() {
	t := ct.T()

	cte := NewCommonTableExpression(true, "test", newTestAppendableExpression(`SELECT * FROM "foo"`, []interface{}{}))

	c := NewClauses()
	c2 := c.CommonTablesAppend(cte)

	assert.Nil(t, c.CommonTables())

	assert.Equal(t, []CommonTableExpression{cte}, c2.CommonTables())
}

func (ct *clausesTest) TestAddCommonTablesAppend() {
	t := ct.T()

	cte := NewCommonTableExpression(true, "test", testSQLExpression("test_cte"))
	cte2 := NewCommonTableExpression(true, "test", testSQLExpression("test_cte2"))

	c := NewClauses().CommonTablesAppend(cte)
	c2 := c.CommonTablesAppend(cte2)

	assert.Equal(t, []CommonTableExpression{cte}, c.CommonTables())

	assert.Equal(t, []CommonTableExpression{cte, cte2}, c2.CommonTables())
}

func (ct *clausesTest) TestReturning() {
	t := ct.T()

	cl := NewColumnListExpression(NewIdentifierExpression("", "", "col"))

	c := NewClauses()
	c2 := c.SetReturning(cl)

	assert.Nil(t, c.Returning())

	assert.Equal(t, cl, c2.Returning())
}

func (ct *clausesTest) TestSetReturning() {
	t := ct.T()

	cl := NewColumnListExpression(NewIdentifierExpression("", "", "col"))
	cl2 := NewColumnListExpression(NewIdentifierExpression("", "", "col2"))

	c := NewClauses().SetReturning(cl)
	c2 := c.SetReturning(cl2)

	assert.Equal(t, cl, c.Returning())

	assert.Equal(t, cl2, c2.Returning())
}
