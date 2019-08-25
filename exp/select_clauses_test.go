package exp

import (
	"testing"

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

type selectClausesSuite struct {
	suite.Suite
}

func TestSelectClausesSuite(t *testing.T) {
	suite.Run(t, new(selectClausesSuite))
}

func (scs *selectClausesSuite) TestHasSources() {
	c := NewSelectClauses()
	c2 := c.SetFrom(NewColumnListExpression("test"))

	scs.False(c.HasSources())

	scs.True(c2.HasSources())
}

func (scs *selectClausesSuite) TestIsDefaultSelect() {
	c := NewSelectClauses()
	c2 := c.SelectAppend(NewColumnListExpression("a"))

	scs.True(c.IsDefaultSelect())

	scs.False(c2.IsDefaultSelect())
}

func (scs *selectClausesSuite) TestSelect() {
	c := NewSelectClauses()
	c2 := c.SetSelect(NewColumnListExpression("a"))

	scs.Equal(NewColumnListExpression(Star()), c.Select())

	scs.Equal(NewColumnListExpression("a"), c2.Select())
}

func (scs *selectClausesSuite) TestSelectAppend() {
	c := NewSelectClauses()
	c2 := c.SelectAppend(NewColumnListExpression("a"))

	scs.Equal(NewColumnListExpression(Star()), c.Select())
	scs.Equal(NewColumnListExpression(Star(), "a"), c2.Select())
}

func (scs *selectClausesSuite) TestSetSelect() {
	c := NewSelectClauses()
	c2 := c.SetSelect(NewColumnListExpression("a"))

	scs.Equal(NewColumnListExpression(Star()), c.Select())
	scs.Equal(NewColumnListExpression("a"), c2.Select())

}

func (scs *selectClausesSuite) TestDistinct() {
	c := NewSelectClauses()
	c2 := c.SetDistinct(NewColumnListExpression("a"))

	scs.Nil(c.Distinct())
	scs.Equal(NewColumnListExpression(Star()), c.Select())

	scs.Equal(NewColumnListExpression("a"), c2.Distinct())
	scs.Equal(NewColumnListExpression(Star()), c.Select())
}

func (scs *selectClausesSuite) TestSetSelectDistinct() {
	c := NewSelectClauses()
	c2 := c.SetDistinct(NewColumnListExpression("a"))

	scs.Nil(c.Distinct())
	scs.Equal(NewColumnListExpression(Star()), c.Select())

	scs.Equal(NewColumnListExpression("a"), c2.Distinct())
	scs.Equal(NewColumnListExpression(Star()), c.Select())
}

func (scs *selectClausesSuite) TestFrom() {
	c := NewSelectClauses()
	c2 := c.SetFrom(NewColumnListExpression("a"))

	scs.Nil(c.From())

	scs.Equal(NewColumnListExpression("a"), c2.From())
}

func (scs *selectClausesSuite) TestSetFrom() {
	c := NewSelectClauses()
	c2 := c.SetFrom(NewColumnListExpression("a"))

	scs.Nil(c.From())

	scs.Equal(NewColumnListExpression("a"), c2.From())
}
func (scs *selectClausesSuite) TestHasAlias() {
	c := NewSelectClauses()
	c2 := c.SetAlias(NewIdentifierExpression("", "", "a"))

	scs.False(c.HasAlias())

	scs.True(c2.HasAlias())
}

func (scs *selectClausesSuite) TestAlias() {
	c := NewSelectClauses()
	a := NewIdentifierExpression("", "a", "")
	c2 := c.SetAlias(a)

	scs.Nil(c.Alias())

	scs.Equal(a, c2.Alias())
}

func (scs *selectClausesSuite) TestSetAlias() {
	c := NewSelectClauses()
	a := NewIdentifierExpression("", "a", "")
	c2 := c.SetAlias(a)

	scs.Nil(c.Alias())

	scs.Equal(a, c2.Alias())
}

func (scs *selectClausesSuite) TestJoins() {

	jc := NewConditionedJoinExpression(
		LeftJoinType,
		NewIdentifierExpression("", "test", ""),
		nil,
	)
	c := NewSelectClauses()
	c2 := c.JoinsAppend(jc)

	scs.Nil(c.Joins())

	scs.Equal(JoinExpressions{jc}, c2.Joins())
}

func (scs *selectClausesSuite) TestJoinsAppend() {
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

	scs.Nil(c.Joins())

	scs.Equal(JoinExpressions{jc}, c2.Joins())
	scs.Equal(JoinExpressions{jc, jc2}, c3.Joins())
}

func (scs *selectClausesSuite) TestWhere() {
	w := Ex{"a": 1}

	c := NewSelectClauses()
	c2 := c.WhereAppend(w)

	scs.Nil(c.Where())

	scs.Equal(NewExpressionList(AndType, w), c2.Where())
}

func (scs *selectClausesSuite) TestClearWhere() {
	w := Ex{"a": 1}

	c := NewSelectClauses().WhereAppend(w)
	c2 := c.ClearWhere()

	scs.Equal(NewExpressionList(AndType, w), c.Where())

	scs.Nil(c2.Where())
}

func (scs *selectClausesSuite) TestWhereAppend() {
	w := Ex{"a": 1}
	w2 := Ex{"b": 2}

	c := NewSelectClauses()
	c2 := c.WhereAppend(w)

	c3 := c.WhereAppend(w).WhereAppend(w2)

	c4 := c.WhereAppend(w, w2)

	scs.Nil(c.Where())

	scs.Equal(NewExpressionList(AndType, w), c2.Where())
	scs.Equal(NewExpressionList(AndType, w).Append(w2), c3.Where())
	scs.Equal(NewExpressionList(AndType, w, w2), c4.Where())
}

func (scs *selectClausesSuite) TestHaving() {
	w := Ex{"a": 1}

	c := NewSelectClauses()
	c2 := c.HavingAppend(w)

	scs.Nil(c.Having())

	scs.Equal(NewExpressionList(AndType, w), c2.Having())
}

func (scs *selectClausesSuite) TestClearHaving() {
	w := Ex{"a": 1}

	c := NewSelectClauses().HavingAppend(w)
	c2 := c.ClearHaving()

	scs.Equal(NewExpressionList(AndType, w), c.Having())

	scs.Nil(c2.Having())
}

func (scs *selectClausesSuite) TestHavingAppend() {
	w := Ex{"a": 1}
	w2 := Ex{"b": 2}

	c := NewSelectClauses()
	c2 := c.HavingAppend(w)

	c3 := c.HavingAppend(w).HavingAppend(w2)

	c4 := c.HavingAppend(w, w2)

	scs.Nil(c.Having())

	scs.Equal(NewExpressionList(AndType, w), c2.Having())
	scs.Equal(NewExpressionList(AndType, w).Append(w2), c3.Having())
	scs.Equal(NewExpressionList(AndType, w, w2), c4.Having())
}

func (scs *selectClausesSuite) TestWindows() {
	w := NewWindowExpression(NewIdentifierExpression("", "", "w"), nil, nil, nil)

	c := NewSelectClauses()
	c2 := c.WindowsAppend(w)

	scs.Nil(c.Windows())

	scs.Equal([]WindowExpression{w}, c2.Windows())
}

func (scs *selectClausesSuite) TestSetWindows() {
	w := NewWindowExpression(NewIdentifierExpression("", "", "w"), nil, nil, nil)

	c := NewSelectClauses()
	c2 := c.SetWindows([]WindowExpression{w})

	scs.Nil(c.Windows())

	scs.Equal([]WindowExpression{w}, c2.Windows())
}

func (scs *selectClausesSuite) TestWindowsAppend() {
	w1 := NewWindowExpression(NewIdentifierExpression("", "", "w1"), nil, nil, nil)
	w2 := NewWindowExpression(NewIdentifierExpression("", "", "w2"), nil, nil, nil)

	c := NewSelectClauses()
	c2 := c.WindowsAppend(w1).WindowsAppend(w2)

	scs.Nil(c.Windows())

	scs.Equal([]WindowExpression{w1, w2}, c2.Windows())
}

func (scs *selectClausesSuite) TestClearWindows() {
	w := NewWindowExpression(NewIdentifierExpression("", "", "w"), nil, nil, nil)

	c := NewSelectClauses().SetWindows([]WindowExpression{w})
	scs.Nil(c.ClearWindows().Windows())
	scs.Equal([]WindowExpression{w}, c.Windows())
}

func (scs *selectClausesSuite) TestOrder() {
	oe := NewIdentifierExpression("", "", "a").Desc()

	c := NewSelectClauses()
	c2 := c.SetOrder(oe)

	scs.Nil(c.Order())

	scs.Equal(NewColumnListExpression(oe), c2.Order())
}

func (scs *selectClausesSuite) TestHasOrder() {
	oe := NewIdentifierExpression("", "", "a").Desc()

	c := NewSelectClauses()
	c2 := c.SetOrder(oe)

	scs.False(c.HasOrder())

	scs.True(c2.HasOrder())
}

func (scs *selectClausesSuite) TestClearOrder() {
	oe := NewIdentifierExpression("", "", "a").Desc()

	c := NewSelectClauses().SetOrder(oe)
	c2 := c.ClearOrder()

	scs.Equal(NewColumnListExpression(oe), c.Order())

	scs.Nil(c2.Order())
}

func (scs *selectClausesSuite) TestSetOrder() {
	oe := NewIdentifierExpression("", "", "a").Desc()
	oe2 := NewIdentifierExpression("", "", "b").Desc()

	c := NewSelectClauses().SetOrder(oe)
	c2 := c.SetOrder(oe2)

	scs.Equal(NewColumnListExpression(oe), c.Order())

	scs.Equal(NewColumnListExpression(oe2), c2.Order())
}

func (scs *selectClausesSuite) TestOrderAppend() {
	oe := NewIdentifierExpression("", "", "a").Desc()
	oe2 := NewIdentifierExpression("", "", "b").Desc()

	c := NewSelectClauses().SetOrder(oe)
	c2 := c.OrderAppend(oe2)

	scs.Equal(NewColumnListExpression(oe), c.Order())

	scs.Equal(NewColumnListExpression(oe, oe2), c2.Order())
}

func (scs *selectClausesSuite) TestOrderPrepend() {
	oe := NewIdentifierExpression("", "", "a").Desc()
	oe2 := NewIdentifierExpression("", "", "b").Desc()

	c := NewSelectClauses().SetOrder(oe)
	c2 := c.OrderPrepend(oe2)

	scs.Equal(NewColumnListExpression(oe), c.Order())

	scs.Equal(NewColumnListExpression(oe2, oe), c2.Order())
}

func (scs *selectClausesSuite) TestGroupBy() {
	g := NewColumnListExpression(NewIdentifierExpression("", "", "a"))

	c := NewSelectClauses()
	c2 := c.SetGroupBy(g)

	scs.Nil(c.GroupBy())

	scs.Equal(g, c2.GroupBy())
}

func (scs *selectClausesSuite) TestSetGroupBy() {
	g := NewColumnListExpression(NewIdentifierExpression("", "", "a"))
	g2 := NewColumnListExpression(NewIdentifierExpression("", "", "b"))

	c := NewSelectClauses().SetGroupBy(g)
	c2 := c.SetGroupBy(g2)

	scs.Equal(g, c.GroupBy())

	scs.Equal(g2, c2.GroupBy())
}

func (scs *selectClausesSuite) TestLimit() {
	l := 1

	c := NewSelectClauses()
	c2 := c.SetLimit(l)

	scs.Nil(c.Limit())

	scs.Equal(l, c2.Limit())
}

func (scs *selectClausesSuite) TestHasLimit() {
	l := 1

	c := NewSelectClauses()
	c2 := c.SetLimit(l)

	scs.False(c.HasLimit())

	scs.True(c2.HasLimit())
}

func (scs *selectClausesSuite) TestCLearLimit() {
	l := 1

	c := NewSelectClauses().SetLimit(l)
	c2 := c.ClearLimit()

	scs.True(c.HasLimit())

	scs.False(c2.HasLimit())
}

func (scs *selectClausesSuite) TestSetLimit() {
	l := 1
	l2 := 2

	c := NewSelectClauses().SetLimit(l)
	c2 := c.SetLimit(2)

	scs.Equal(l, c.Limit())

	scs.Equal(l2, c2.Limit())
}

func (scs *selectClausesSuite) TestOffset() {
	o := uint(1)

	c := NewSelectClauses()
	c2 := c.SetOffset(o)

	scs.Equal(uint(0), c.Offset())

	scs.Equal(o, c2.Offset())
}

func (scs *selectClausesSuite) TestClearOffset() {
	o := uint(1)

	c := NewSelectClauses().SetOffset(o)
	c2 := c.ClearOffset()

	scs.Equal(o, c.Offset())

	scs.Equal(uint(0), c2.Offset())
}

func (scs *selectClausesSuite) TestSetOffset() {
	o := uint(1)
	o2 := uint(2)

	c := NewSelectClauses().SetOffset(o)
	c2 := c.SetOffset(2)

	scs.Equal(o, c.Offset())

	scs.Equal(o2, c2.Offset())
}

func (scs *selectClausesSuite) TestCompounds() {

	ce := NewCompoundExpression(UnionCompoundType, newTestAppendableExpression("SELECT * FROM foo", []interface{}{}))

	c := NewSelectClauses()
	c2 := c.CompoundsAppend(ce)

	scs.Nil(c.Compounds())

	scs.Equal([]CompoundExpression{ce}, c2.Compounds())
}
func (scs *selectClausesSuite) TestCompoundsAppend() {

	ce := NewCompoundExpression(UnionCompoundType, newTestAppendableExpression("SELECT * FROM foo1", []interface{}{}))
	ce2 := NewCompoundExpression(UnionCompoundType, newTestAppendableExpression("SELECT * FROM foo2", []interface{}{}))

	c := NewSelectClauses().CompoundsAppend(ce)
	c2 := c.CompoundsAppend(ce2)

	scs.Equal([]CompoundExpression{ce}, c.Compounds())

	scs.Equal([]CompoundExpression{ce, ce2}, c2.Compounds())
}

func (scs *selectClausesSuite) TestLock() {

	l := NewLock(ForUpdate, Wait)

	c := NewSelectClauses()
	c2 := c.SetLock(l)

	scs.Nil(c.Lock())

	scs.Equal(l, c2.Lock())
}

func (scs *selectClausesSuite) TestSetLock() {

	l := NewLock(ForUpdate, Wait)
	l2 := NewLock(ForUpdate, NoWait)

	c := NewSelectClauses().SetLock(l)
	c2 := c.SetLock(l2)

	scs.Equal(l, c.Lock())

	scs.Equal(l2, c2.Lock())
}

func (scs *selectClausesSuite) TestCommonTables() {

	cte := NewCommonTableExpression(true, "test", newTestAppendableExpression(`SELECT * FROM "foo"`, []interface{}{}))

	c := NewSelectClauses()
	c2 := c.CommonTablesAppend(cte)

	scs.Nil(c.CommonTables())

	scs.Equal([]CommonTableExpression{cte}, c2.CommonTables())
}

func (scs *selectClausesSuite) TestAddCommonTablesAppend() {

	cte := NewCommonTableExpression(true, "test", testSQLExpression("test_cte"))
	cte2 := NewCommonTableExpression(true, "test", testSQLExpression("test_cte2"))

	c := NewSelectClauses().CommonTablesAppend(cte)
	c2 := c.CommonTablesAppend(cte2)

	scs.Equal([]CommonTableExpression{cte}, c.CommonTables())

	scs.Equal([]CommonTableExpression{cte, cte2}, c2.CommonTables())
}
