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

type selectClausesTest struct {
	suite.Suite
}

func TestSelectClausesSuite(t *testing.T) {
	suite.Run(t, new(selectClausesTest))
}

func (sct *selectClausesTest) TestHasSources() {
	t := sct.T()
	c := NewSelectClauses()
	c2 := c.SetFrom(NewColumnListExpression("test"))

	assert.False(t, c.HasSources())

	assert.True(t, c2.HasSources())
}

func (sct *selectClausesTest) TestIsDefaultSelect() {
	t := sct.T()
	c := NewSelectClauses()
	c2 := c.SelectAppend(NewColumnListExpression("a"))

	assert.True(t, c.IsDefaultSelect())

	assert.False(t, c2.IsDefaultSelect())
}

func (sct *selectClausesTest) TestSelect() {
	t := sct.T()
	c := NewSelectClauses()
	c2 := c.SetSelect(NewColumnListExpression("a"))

	assert.Equal(t, NewColumnListExpression(Star()), c.Select())

	assert.Equal(t, NewColumnListExpression("a"), c2.Select())
}

func (sct *selectClausesTest) TestSelectAppend() {
	t := sct.T()
	c := NewSelectClauses()
	c2 := c.SelectAppend(NewColumnListExpression("a"))

	assert.Equal(t, NewColumnListExpression(Star()), c.Select())
	assert.Equal(t, NewColumnListExpression(Star(), "a"), c2.Select())
}

func (sct *selectClausesTest) TestSetSelect() {
	t := sct.T()
	c := NewSelectClauses()
	c2 := c.SetSelect(NewColumnListExpression("a"))

	assert.Equal(t, NewColumnListExpression(Star()), c.Select())
	assert.Equal(t, NewColumnListExpression("a"), c2.Select())

}

func (sct *selectClausesTest) TestDistinct() {
	t := sct.T()
	c := NewSelectClauses()
	c2 := c.SetDistinct(NewColumnListExpression("a"))

	assert.Nil(t, c.Distinct())
	assert.Equal(t, NewColumnListExpression(Star()), c.Select())

	assert.Equal(t, NewColumnListExpression("a"), c2.Distinct())
	assert.Equal(t, NewColumnListExpression(Star()), c.Select())
}

func (sct *selectClausesTest) TestSetSelectDistinct() {
	t := sct.T()
	c := NewSelectClauses()
	c2 := c.SetDistinct(NewColumnListExpression("a"))

	assert.Nil(t, c.Distinct())
	assert.Equal(t, NewColumnListExpression(Star()), c.Select())

	assert.Equal(t, NewColumnListExpression("a"), c2.Distinct())
	assert.Equal(t, NewColumnListExpression(Star()), c.Select())
}

func (sct *selectClausesTest) TestFrom() {
	t := sct.T()
	c := NewSelectClauses()
	c2 := c.SetFrom(NewColumnListExpression("a"))

	assert.Nil(t, c.From())

	assert.Equal(t, NewColumnListExpression("a"), c2.From())
}

func (sct *selectClausesTest) TestSetFrom() {
	t := sct.T()
	c := NewSelectClauses()
	c2 := c.SetFrom(NewColumnListExpression("a"))

	assert.Nil(t, c.From())

	assert.Equal(t, NewColumnListExpression("a"), c2.From())
}
func (sct *selectClausesTest) TestHasAlias() {
	t := sct.T()
	c := NewSelectClauses()
	c2 := c.SetAlias(NewIdentifierExpression("", "", "a"))

	assert.False(t, c.HasAlias())

	assert.True(t, c2.HasAlias())
}

func (sct *selectClausesTest) TestAlias() {
	t := sct.T()
	c := NewSelectClauses()
	a := NewIdentifierExpression("", "a", "")
	c2 := c.SetAlias(a)

	assert.Nil(t, c.Alias())

	assert.Equal(t, a, c2.Alias())
}

func (sct *selectClausesTest) TestSetAlias() {
	t := sct.T()
	c := NewSelectClauses()
	a := NewIdentifierExpression("", "a", "")
	c2 := c.SetAlias(a)

	assert.Nil(t, c.Alias())

	assert.Equal(t, a, c2.Alias())
}

func (sct *selectClausesTest) TestJoins() {
	t := sct.T()

	jc := NewConditionedJoinExpression(
		LeftJoinType,
		NewIdentifierExpression("", "test", ""),
		nil,
	)
	c := NewSelectClauses()
	c2 := c.JoinsAppend(jc)

	assert.Nil(t, c.Joins())

	assert.Equal(t, JoinExpressions{jc}, c2.Joins())
}

func (sct *selectClausesTest) TestJoinsAppend() {
	t := sct.T()
	jc := NewConditionedJoinExpression(
		LeftJoinType,
		NewIdentifierExpression("", "test", ""),
		nil,
	)
	jc2 := NewUnConditionedJoinExpression(
		LeftJoinType,
		NewIdentifierExpression("", "test", ""),
	)
	c := NewSelectClauses()
	c2 := c.JoinsAppend(jc)
	c3 := c2.JoinsAppend(jc2)

	assert.Nil(t, c.Joins())

	assert.Equal(t, JoinExpressions{jc}, c2.Joins())
	assert.Equal(t, JoinExpressions{jc, jc2}, c3.Joins())
}

func (sct *selectClausesTest) TestWhere() {
	t := sct.T()
	w := Ex{"a": 1}

	c := NewSelectClauses()
	c2 := c.WhereAppend(w)

	assert.Nil(t, c.Where())

	assert.Equal(t, NewExpressionList(AndType, w), c2.Where())
}

func (sct *selectClausesTest) TestClearWhere() {
	t := sct.T()
	w := Ex{"a": 1}

	c := NewSelectClauses().WhereAppend(w)
	c2 := c.ClearWhere()

	assert.Equal(t, NewExpressionList(AndType, w), c.Where())

	assert.Nil(t, c2.Where())
}

func (sct *selectClausesTest) TestWhereAppend() {
	t := sct.T()
	w := Ex{"a": 1}
	w2 := Ex{"b": 2}

	c := NewSelectClauses()
	c2 := c.WhereAppend(w)

	c3 := c.WhereAppend(w).WhereAppend(w2)

	c4 := c.WhereAppend(w, w2)

	assert.Nil(t, c.Where())

	assert.Equal(t, NewExpressionList(AndType, w), c2.Where())
	assert.Equal(t, NewExpressionList(AndType, w).Append(w2), c3.Where())
	assert.Equal(t, NewExpressionList(AndType, w, w2), c4.Where())
}

func (sct *selectClausesTest) TestHaving() {
	t := sct.T()
	w := Ex{"a": 1}

	c := NewSelectClauses()
	c2 := c.HavingAppend(w)

	assert.Nil(t, c.Having())

	assert.Equal(t, NewExpressionList(AndType, w), c2.Having())
}

func (sct *selectClausesTest) TestClearHaving() {
	t := sct.T()
	w := Ex{"a": 1}

	c := NewSelectClauses().HavingAppend(w)
	c2 := c.ClearHaving()

	assert.Equal(t, NewExpressionList(AndType, w), c.Having())

	assert.Nil(t, c2.Having())
}

func (sct *selectClausesTest) TestHavingAppend() {
	t := sct.T()
	w := Ex{"a": 1}
	w2 := Ex{"b": 2}

	c := NewSelectClauses()
	c2 := c.HavingAppend(w)

	c3 := c.HavingAppend(w).HavingAppend(w2)

	c4 := c.HavingAppend(w, w2)

	assert.Nil(t, c.Having())

	assert.Equal(t, NewExpressionList(AndType, w), c2.Having())
	assert.Equal(t, NewExpressionList(AndType, w).Append(w2), c3.Having())
	assert.Equal(t, NewExpressionList(AndType, w, w2), c4.Having())
}

func (sct *selectClausesTest) TestOrder() {
	t := sct.T()
	oe := NewIdentifierExpression("", "", "a").Desc()

	c := NewSelectClauses()
	c2 := c.SetOrder(oe)

	assert.Nil(t, c.Order())

	assert.Equal(t, NewColumnListExpression(oe), c2.Order())
}

func (sct *selectClausesTest) TestHasOrder() {
	t := sct.T()
	oe := NewIdentifierExpression("", "", "a").Desc()

	c := NewSelectClauses()
	c2 := c.SetOrder(oe)

	assert.False(t, c.HasOrder())

	assert.True(t, c2.HasOrder())
}

func (sct *selectClausesTest) TestClearOrder() {
	t := sct.T()
	oe := NewIdentifierExpression("", "", "a").Desc()

	c := NewSelectClauses().SetOrder(oe)
	c2 := c.ClearOrder()

	assert.Equal(t, NewColumnListExpression(oe), c.Order())

	assert.Nil(t, c2.Order())
}

func (sct *selectClausesTest) TestSetOrder() {
	t := sct.T()
	oe := NewIdentifierExpression("", "", "a").Desc()
	oe2 := NewIdentifierExpression("", "", "b").Desc()

	c := NewSelectClauses().SetOrder(oe)
	c2 := c.SetOrder(oe2)

	assert.Equal(t, NewColumnListExpression(oe), c.Order())

	assert.Equal(t, NewColumnListExpression(oe2), c2.Order())
}

func (sct *selectClausesTest) TestOrderAppend() {
	t := sct.T()
	oe := NewIdentifierExpression("", "", "a").Desc()
	oe2 := NewIdentifierExpression("", "", "b").Desc()

	c := NewSelectClauses().SetOrder(oe)
	c2 := c.OrderAppend(oe2)

	assert.Equal(t, NewColumnListExpression(oe), c.Order())

	assert.Equal(t, NewColumnListExpression(oe, oe2), c2.Order())
}

func (sct *selectClausesTest) TestOrderPrepend() {
	t := sct.T()
	oe := NewIdentifierExpression("", "", "a").Desc()
	oe2 := NewIdentifierExpression("", "", "b").Desc()

	c := NewSelectClauses().SetOrder(oe)
	c2 := c.OrderPrepend(oe2)

	assert.Equal(t, NewColumnListExpression(oe), c.Order())

	assert.Equal(t, NewColumnListExpression(oe2, oe), c2.Order())
}

func (sct *selectClausesTest) TestGroupBy() {
	t := sct.T()
	g := NewColumnListExpression(NewIdentifierExpression("", "", "a"))

	c := NewSelectClauses()
	c2 := c.SetGroupBy(g)

	assert.Nil(t, c.GroupBy())

	assert.Equal(t, g, c2.GroupBy())
}

func (sct *selectClausesTest) TestSetGroupBy() {
	t := sct.T()
	g := NewColumnListExpression(NewIdentifierExpression("", "", "a"))
	g2 := NewColumnListExpression(NewIdentifierExpression("", "", "b"))

	c := NewSelectClauses().SetGroupBy(g)
	c2 := c.SetGroupBy(g2)

	assert.Equal(t, g, c.GroupBy())

	assert.Equal(t, g2, c2.GroupBy())
}

func (sct *selectClausesTest) TestLimit() {
	t := sct.T()
	l := 1

	c := NewSelectClauses()
	c2 := c.SetLimit(l)

	assert.Nil(t, c.Limit())

	assert.Equal(t, l, c2.Limit())
}

func (sct *selectClausesTest) TestHasLimit() {
	t := sct.T()
	l := 1

	c := NewSelectClauses()
	c2 := c.SetLimit(l)

	assert.False(t, c.HasLimit())

	assert.True(t, c2.HasLimit())
}

func (sct *selectClausesTest) TestCLearLimit() {
	t := sct.T()
	l := 1

	c := NewSelectClauses().SetLimit(l)
	c2 := c.ClearLimit()

	assert.True(t, c.HasLimit())

	assert.False(t, c2.HasLimit())
}

func (sct *selectClausesTest) TestSetLimit() {
	t := sct.T()
	l := 1
	l2 := 2

	c := NewSelectClauses().SetLimit(l)
	c2 := c.SetLimit(2)

	assert.Equal(t, l, c.Limit())

	assert.Equal(t, l2, c2.Limit())
}

func (sct *selectClausesTest) TestOffset() {
	t := sct.T()
	o := uint(1)

	c := NewSelectClauses()
	c2 := c.SetOffset(o)

	assert.Equal(t, uint(0), c.Offset())

	assert.Equal(t, o, c2.Offset())
}

func (sct *selectClausesTest) TestClearOffset() {
	t := sct.T()
	o := uint(1)

	c := NewSelectClauses().SetOffset(o)
	c2 := c.ClearOffset()

	assert.Equal(t, o, c.Offset())

	assert.Equal(t, uint(0), c2.Offset())
}

func (sct *selectClausesTest) TestSetOffset() {
	t := sct.T()
	o := uint(1)
	o2 := uint(2)

	c := NewSelectClauses().SetOffset(o)
	c2 := c.SetOffset(2)

	assert.Equal(t, o, c.Offset())

	assert.Equal(t, o2, c2.Offset())
}

func (sct *selectClausesTest) TestCompounds() {
	t := sct.T()

	ce := NewCompoundExpression(UnionCompoundType, newTestAppendableExpression("SELECT * FROM foo", []interface{}{}))

	c := NewSelectClauses()
	c2 := c.CompoundsAppend(ce)

	assert.Nil(t, c.Compounds())

	assert.Equal(t, []CompoundExpression{ce}, c2.Compounds())
}
func (sct *selectClausesTest) TestCompoundsAppend() {
	t := sct.T()

	ce := NewCompoundExpression(UnionCompoundType, newTestAppendableExpression("SELECT * FROM foo1", []interface{}{}))
	ce2 := NewCompoundExpression(UnionCompoundType, newTestAppendableExpression("SELECT * FROM foo2", []interface{}{}))

	c := NewSelectClauses().CompoundsAppend(ce)
	c2 := c.CompoundsAppend(ce2)

	assert.Equal(t, []CompoundExpression{ce}, c.Compounds())

	assert.Equal(t, []CompoundExpression{ce, ce2}, c2.Compounds())
}

func (sct *selectClausesTest) TestLock() {
	t := sct.T()

	l := NewLock(ForUpdate, Wait)

	c := NewSelectClauses()
	c2 := c.SetLock(l)

	assert.Nil(t, c.Lock())

	assert.Equal(t, l, c2.Lock())
}

func (sct *selectClausesTest) TestSetLock() {
	t := sct.T()

	l := NewLock(ForUpdate, Wait)
	l2 := NewLock(ForUpdate, NoWait)

	c := NewSelectClauses().SetLock(l)
	c2 := c.SetLock(l2)

	assert.Equal(t, l, c.Lock())

	assert.Equal(t, l2, c2.Lock())
}

func (sct *selectClausesTest) TestCommonTables() {
	t := sct.T()

	cte := NewCommonTableExpression(true, "test", newTestAppendableExpression(`SELECT * FROM "foo"`, []interface{}{}))

	c := NewSelectClauses()
	c2 := c.CommonTablesAppend(cte)

	assert.Nil(t, c.CommonTables())

	assert.Equal(t, []CommonTableExpression{cte}, c2.CommonTables())
}

func (sct *selectClausesTest) TestAddCommonTablesAppend() {
	t := sct.T()

	cte := NewCommonTableExpression(true, "test", testSQLExpression("test_cte"))
	cte2 := NewCommonTableExpression(true, "test", testSQLExpression("test_cte2"))

	c := NewSelectClauses().CommonTablesAppend(cte)
	c2 := c.CommonTablesAppend(cte2)

	assert.Equal(t, []CommonTableExpression{cte}, c.CommonTables())

	assert.Equal(t, []CommonTableExpression{cte, cte2}, c2.CommonTables())
}

// func (ct *selectClausesTest) TestReturning() {
// 	t := ct.T()
//
// 	cl := NewColumnListExpression(NewIdentifierExpression("", "", "col"))
//
// 	c := NewSelectClauses()
// 	c2 := c.SetReturning(cl)
//
// 	assert.Nil(t, c.Returning())
//
// 	assert.Equal(t, cl, c2.Returning())
// }
//
// func (ct *selectClausesTest) TestSetReturning() {
// 	t := ct.T()
//
// 	cl := NewColumnListExpression(NewIdentifierExpression("", "", "col"))
// 	cl2 := NewColumnListExpression(NewIdentifierExpression("", "", "col2"))
//
// 	c := NewSelectClauses().SetReturning(cl)
// 	c2 := c.SetReturning(cl2)
//
// 	assert.Equal(t, cl, c.Returning())
//
// 	assert.Equal(t, cl2, c2.Returning())
// }
