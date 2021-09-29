package exp_test

import (
	"testing"

	"github.com/doug-martin/goqu/v9/exp"
	"github.com/stretchr/testify/suite"
)

type testSQLExpression string

func (tse testSQLExpression) Expression() exp.Expression {
	return tse
}

func (tse testSQLExpression) Clone() exp.Expression {
	return tse
}

func (tse testSQLExpression) ToSQL() (sql string, args []interface{}, err error) {
	return "", nil, nil
}

func (tse testSQLExpression) IsPrepared() bool {
	return false
}

type selectClausesSuite struct {
	suite.Suite
}

func TestSelectClausesSuite(t *testing.T) {
	suite.Run(t, new(selectClausesSuite))
}

func (scs *selectClausesSuite) TestHasSources() {
	c := exp.NewSelectClauses()
	c2 := c.SetFrom(exp.NewColumnListExpression("test"))

	scs.False(c.HasSources())

	scs.True(c2.HasSources())
}

func (scs *selectClausesSuite) TestIsDefaultSelect() {
	c := exp.NewSelectClauses()
	c2 := c.SelectAppend(exp.NewColumnListExpression("a"))

	scs.True(c.IsDefaultSelect())

	scs.False(c2.IsDefaultSelect())
}

func (scs *selectClausesSuite) TestSelect() {
	c := exp.NewSelectClauses()
	c2 := c.SetSelect(exp.NewColumnListExpression("a"))

	scs.Equal(exp.NewColumnListExpression(exp.Star()), c.Select())

	scs.Equal(exp.NewColumnListExpression("a"), c2.Select())
}

func (scs *selectClausesSuite) TestSelectAppend() {
	c := exp.NewSelectClauses()
	c2 := c.SelectAppend(exp.NewColumnListExpression("a"))

	scs.Equal(exp.NewColumnListExpression(exp.Star()), c.Select())
	scs.Equal(exp.NewColumnListExpression(exp.Star(), "a"), c2.Select())
}

func (scs *selectClausesSuite) TestSetSelect() {
	c := exp.NewSelectClauses()
	c2 := c.SetSelect(exp.NewColumnListExpression("a"))

	scs.Equal(exp.NewColumnListExpression(exp.Star()), c.Select())
	scs.Equal(exp.NewColumnListExpression("a"), c2.Select())
}

func (scs *selectClausesSuite) TestDistinct() {
	c := exp.NewSelectClauses()
	c2 := c.SetDistinct(exp.NewColumnListExpression("a"))

	scs.Nil(c.Distinct())
	scs.Equal(exp.NewColumnListExpression(exp.Star()), c.Select())

	scs.Equal(exp.NewColumnListExpression("a"), c2.Distinct())
	scs.Equal(exp.NewColumnListExpression(exp.Star()), c.Select())
}

func (scs *selectClausesSuite) TestSetSelectDistinct() {
	c := exp.NewSelectClauses()
	c2 := c.SetDistinct(exp.NewColumnListExpression("a"))

	scs.Nil(c.Distinct())
	scs.Equal(exp.NewColumnListExpression(exp.Star()), c.Select())

	scs.Equal(exp.NewColumnListExpression("a"), c2.Distinct())
	scs.Equal(exp.NewColumnListExpression(exp.Star()), c.Select())
}

func (scs *selectClausesSuite) TestFrom() {
	c := exp.NewSelectClauses()
	c2 := c.SetFrom(exp.NewColumnListExpression("a"))

	scs.Nil(c.From())

	scs.Equal(exp.NewColumnListExpression("a"), c2.From())
}

func (scs *selectClausesSuite) TestSetFrom() {
	c := exp.NewSelectClauses()
	c2 := c.SetFrom(exp.NewColumnListExpression("a"))

	scs.Nil(c.From())

	scs.Equal(exp.NewColumnListExpression("a"), c2.From())
}

func (scs *selectClausesSuite) TestHasAlias() {
	c := exp.NewSelectClauses()
	c2 := c.SetAlias(exp.NewIdentifierExpression("", "", "a"))

	scs.False(c.HasAlias())

	scs.True(c2.HasAlias())
}

func (scs *selectClausesSuite) TestAlias() {
	c := exp.NewSelectClauses()
	a := exp.NewIdentifierExpression("", "a", "")
	c2 := c.SetAlias(a)

	scs.Nil(c.Alias())

	scs.Equal(a, c2.Alias())
}

func (scs *selectClausesSuite) TestSetAlias() {
	c := exp.NewSelectClauses()
	a := exp.NewIdentifierExpression("", "a", "")
	c2 := c.SetAlias(a)

	scs.Nil(c.Alias())

	scs.Equal(a, c2.Alias())
}

func (scs *selectClausesSuite) TestJoins() {
	jc := exp.NewConditionedJoinExpression(
		exp.LeftJoinType,
		exp.NewIdentifierExpression("", "test", ""),
		nil,
	)
	c := exp.NewSelectClauses()
	c2 := c.JoinsAppend(jc)

	scs.Nil(c.Joins())

	scs.Equal(exp.JoinExpressions{jc}, c2.Joins())
}

func (scs *selectClausesSuite) TestJoinsAppend() {
	jc := exp.NewConditionedJoinExpression(
		exp.LeftJoinType,
		exp.NewIdentifierExpression("", "test1", ""),
		nil,
	)
	jc2 := exp.NewUnConditionedJoinExpression(
		exp.LeftJoinType,
		exp.NewIdentifierExpression("", "test2", ""),
	)
	jc3 := exp.NewUnConditionedJoinExpression(
		exp.InnerJoinType,
		exp.NewIdentifierExpression("", "test3", ""),
	)
	c := exp.NewSelectClauses()
	c2 := c.JoinsAppend(jc)
	c3 := c2.JoinsAppend(jc2)

	c4 := c3.JoinsAppend(jc2) // len(c4.joins) == 3, cap(c4.joins) == 4
	// next two appends shouldn't affect one another
	c5 := c4.JoinsAppend(jc2)
	c6 := c4.JoinsAppend(jc3)

	scs.Nil(c.Joins())

	scs.Equal(exp.JoinExpressions{jc}, c2.Joins())
	scs.Equal(exp.JoinExpressions{jc, jc2}, c3.Joins())
	scs.Equal(exp.JoinExpressions{jc, jc2, jc2}, c4.Joins())
	scs.Equal(exp.JoinExpressions{jc, jc2, jc2, jc2}, c5.Joins())
	scs.Equal(exp.JoinExpressions{jc, jc2, jc2, jc3}, c6.Joins())
}

func (scs *selectClausesSuite) TestWhere() {
	w := exp.Ex{"a": 1}

	c := exp.NewSelectClauses()
	c2 := c.WhereAppend(w)

	scs.Nil(c.Where())

	scs.Equal(exp.NewExpressionList(exp.AndType, w), c2.Where())
}

func (scs *selectClausesSuite) TestClearWhere() {
	w := exp.Ex{"a": 1}

	c := exp.NewSelectClauses().WhereAppend(w)
	c2 := c.ClearWhere()

	scs.Equal(exp.NewExpressionList(exp.AndType, w), c.Where())

	scs.Nil(c2.Where())
}

func (scs *selectClausesSuite) TestWhereAppend() {
	w := exp.Ex{"a": 1}
	w2 := exp.Ex{"b": 2}

	c := exp.NewSelectClauses()
	c2 := c.WhereAppend(w)

	c3 := c.WhereAppend(w).WhereAppend(w2)

	c4 := c.WhereAppend(w, w2)

	scs.Nil(c.Where())

	scs.Equal(exp.NewExpressionList(exp.AndType, w), c2.Where())
	scs.Equal(exp.NewExpressionList(exp.AndType, w).Append(w2), c3.Where())
	scs.Equal(exp.NewExpressionList(exp.AndType, w, w2), c4.Where())
}

func (scs *selectClausesSuite) TestHaving() {
	w := exp.Ex{"a": 1}

	c := exp.NewSelectClauses()
	c2 := c.HavingAppend(w)

	scs.Nil(c.Having())

	scs.Equal(exp.NewExpressionList(exp.AndType, w), c2.Having())
}

func (scs *selectClausesSuite) TestClearHaving() {
	w := exp.Ex{"a": 1}

	c := exp.NewSelectClauses().HavingAppend(w)
	c2 := c.ClearHaving()

	scs.Equal(exp.NewExpressionList(exp.AndType, w), c.Having())

	scs.Nil(c2.Having())
}

func (scs *selectClausesSuite) TestHavingAppend() {
	w := exp.Ex{"a": 1}
	w2 := exp.Ex{"b": 2}

	c := exp.NewSelectClauses()
	c2 := c.HavingAppend(w)

	c3 := c.HavingAppend(w).HavingAppend(w2)

	c4 := c.HavingAppend(w, w2)

	scs.Nil(c.Having())

	scs.Equal(exp.NewExpressionList(exp.AndType, w), c2.Having())
	scs.Equal(exp.NewExpressionList(exp.AndType, w).Append(w2), c3.Having())
	scs.Equal(exp.NewExpressionList(exp.AndType, w, w2), c4.Having())
}

func (scs *selectClausesSuite) TestWindows() {
	w := exp.NewWindowExpression(exp.NewIdentifierExpression("", "", "w"), nil, nil, nil)

	c := exp.NewSelectClauses()
	c2 := c.WindowsAppend(w)

	scs.Nil(c.Windows())

	scs.Equal([]exp.WindowExpression{w}, c2.Windows())
}

func (scs *selectClausesSuite) TestSetWindows() {
	w := exp.NewWindowExpression(exp.NewIdentifierExpression("", "", "w"), nil, nil, nil)

	c := exp.NewSelectClauses()
	c2 := c.SetWindows([]exp.WindowExpression{w})

	scs.Nil(c.Windows())

	scs.Equal([]exp.WindowExpression{w}, c2.Windows())
}

func (scs *selectClausesSuite) TestWindowsAppend() {
	w1 := exp.NewWindowExpression(exp.NewIdentifierExpression("", "", "w1"), nil, nil, nil)
	w2 := exp.NewWindowExpression(exp.NewIdentifierExpression("", "", "w2"), nil, nil, nil)

	c := exp.NewSelectClauses()
	c2 := c.WindowsAppend(w1).WindowsAppend(w2)

	scs.Nil(c.Windows())

	scs.Equal([]exp.WindowExpression{w1, w2}, c2.Windows())
}

func (scs *selectClausesSuite) TestClearWindows() {
	w := exp.NewWindowExpression(exp.NewIdentifierExpression("", "", "w"), nil, nil, nil)

	c := exp.NewSelectClauses().SetWindows([]exp.WindowExpression{w})
	scs.Nil(c.ClearWindows().Windows())
	scs.Equal([]exp.WindowExpression{w}, c.Windows())
}

func (scs *selectClausesSuite) TestOrder() {
	oe := exp.NewIdentifierExpression("", "", "a").Desc()

	c := exp.NewSelectClauses()
	c2 := c.SetOrder(oe)

	scs.Nil(c.Order())

	scs.Equal(exp.NewColumnListExpression(oe), c2.Order())
}

func (scs *selectClausesSuite) TestHasOrder() {
	oe := exp.NewIdentifierExpression("", "", "a").Desc()

	c := exp.NewSelectClauses()
	c2 := c.SetOrder(oe)

	scs.False(c.HasOrder())

	scs.True(c2.HasOrder())
}

func (scs *selectClausesSuite) TestClearOrder() {
	oe := exp.NewIdentifierExpression("", "", "a").Desc()

	c := exp.NewSelectClauses().SetOrder(oe)
	c2 := c.ClearOrder()

	scs.Equal(exp.NewColumnListExpression(oe), c.Order())

	scs.Nil(c2.Order())
}

func (scs *selectClausesSuite) TestSetOrder() {
	oe := exp.NewIdentifierExpression("", "", "a").Desc()
	oe2 := exp.NewIdentifierExpression("", "", "b").Desc()

	c := exp.NewSelectClauses().SetOrder(oe)
	c2 := c.SetOrder(oe2)

	scs.Equal(exp.NewColumnListExpression(oe), c.Order())

	scs.Equal(exp.NewColumnListExpression(oe2), c2.Order())
}

func (scs *selectClausesSuite) TestOrderAppend() {
	oe := exp.NewIdentifierExpression("", "", "a").Desc()
	oe2 := exp.NewIdentifierExpression("", "", "b").Desc()

	c := exp.NewSelectClauses().SetOrder(oe)
	c2 := c.OrderAppend(oe2)

	scs.Equal(exp.NewColumnListExpression(oe), c.Order())

	scs.Equal(exp.NewColumnListExpression(oe, oe2), c2.Order())
}

func (scs *selectClausesSuite) TestOrderPrepend() {
	oe := exp.NewIdentifierExpression("", "", "a").Desc()
	oe2 := exp.NewIdentifierExpression("", "", "b").Desc()

	c := exp.NewSelectClauses().SetOrder(oe)
	c2 := c.OrderPrepend(oe2)

	scs.Equal(exp.NewColumnListExpression(oe), c.Order())

	scs.Equal(exp.NewColumnListExpression(oe2, oe), c2.Order())
}

func (scs *selectClausesSuite) TestGroupBy() {
	g := exp.NewColumnListExpression(exp.NewIdentifierExpression("", "", "a"))

	c := exp.NewSelectClauses()
	c2 := c.SetGroupBy(g)

	scs.Nil(c.GroupBy())

	scs.Equal(g, c2.GroupBy())
}

func (scs *selectClausesSuite) TestGroupByAppend() {
	g := exp.NewColumnListExpression(exp.NewIdentifierExpression("", "", "a"))
	g2 := exp.NewColumnListExpression(exp.NewIdentifierExpression("", "", "b"))

	c := exp.NewSelectClauses().SetGroupBy(g)
	c2 := c.GroupByAppend(g2)

	scs.Equal(g, c.GroupBy())

	scs.Equal(exp.NewColumnListExpression(g, g2), c2.GroupBy())
}

func (scs *selectClausesSuite) TestGroupByAppend_NoPreviousGroupBy() {
	g := exp.NewColumnListExpression(exp.NewIdentifierExpression("", "", "a"))
	g2 := exp.NewColumnListExpression(exp.NewIdentifierExpression("", "", "b"))

	c := exp.NewSelectClauses().GroupByAppend(g)
	c2 := c.GroupByAppend(g2)

	scs.Equal(g, c.GroupBy())

	scs.Equal(exp.NewColumnListExpression(g, g2), c2.GroupBy())
}

func (scs *selectClausesSuite) TestSetGroupBy() {
	g := exp.NewColumnListExpression(exp.NewIdentifierExpression("", "", "a"))
	g2 := exp.NewColumnListExpression(exp.NewIdentifierExpression("", "", "b"))

	c := exp.NewSelectClauses().SetGroupBy(g)
	c2 := c.SetGroupBy(g2)

	scs.Equal(g, c.GroupBy())

	scs.Equal(g2, c2.GroupBy())
}

func (scs *selectClausesSuite) TestLimit() {
	l := 1

	c := exp.NewSelectClauses()
	c2 := c.SetLimit(l)

	scs.Nil(c.Limit())

	scs.Equal(l, c2.Limit())
}

func (scs *selectClausesSuite) TestHasLimit() {
	l := 1

	c := exp.NewSelectClauses()
	c2 := c.SetLimit(l)

	scs.False(c.HasLimit())

	scs.True(c2.HasLimit())
}

func (scs *selectClausesSuite) TestCLearLimit() {
	l := 1

	c := exp.NewSelectClauses().SetLimit(l)
	c2 := c.ClearLimit()

	scs.True(c.HasLimit())

	scs.False(c2.HasLimit())
}

func (scs *selectClausesSuite) TestSetLimit() {
	l := 1
	l2 := 2

	c := exp.NewSelectClauses().SetLimit(l)
	c2 := c.SetLimit(2)

	scs.Equal(l, c.Limit())

	scs.Equal(l2, c2.Limit())
}

func (scs *selectClausesSuite) TestOffset() {
	o := uint(1)

	c := exp.NewSelectClauses()
	c2 := c.SetOffset(o)

	scs.Equal(uint(0), c.Offset())

	scs.Equal(o, c2.Offset())
}

func (scs *selectClausesSuite) TestClearOffset() {
	o := uint(1)

	c := exp.NewSelectClauses().SetOffset(o)
	c2 := c.ClearOffset()

	scs.Equal(o, c.Offset())

	scs.Equal(uint(0), c2.Offset())
}

func (scs *selectClausesSuite) TestSetOffset() {
	o := uint(1)
	o2 := uint(2)

	c := exp.NewSelectClauses().SetOffset(o)
	c2 := c.SetOffset(2)

	scs.Equal(o, c.Offset())

	scs.Equal(o2, c2.Offset())
}

func (scs *selectClausesSuite) TestCompounds() {
	ce := exp.NewCompoundExpression(exp.UnionCompoundType, newTestAppendableExpression("SELECT * FROM foo", []interface{}{}))

	c := exp.NewSelectClauses()
	c2 := c.CompoundsAppend(ce)

	scs.Nil(c.Compounds())

	scs.Equal([]exp.CompoundExpression{ce}, c2.Compounds())
}

func (scs *selectClausesSuite) TestCompoundsAppend() {
	ce := exp.NewCompoundExpression(exp.UnionCompoundType, newTestAppendableExpression("SELECT * FROM foo1", []interface{}{}))
	ce2 := exp.NewCompoundExpression(exp.UnionCompoundType, newTestAppendableExpression("SELECT * FROM foo2", []interface{}{}))

	c := exp.NewSelectClauses().CompoundsAppend(ce)
	c2 := c.CompoundsAppend(ce2)

	scs.Equal([]exp.CompoundExpression{ce}, c.Compounds())

	scs.Equal([]exp.CompoundExpression{ce, ce2}, c2.Compounds())
}

func (scs *selectClausesSuite) TestLock() {
	l := exp.NewLock(exp.ForUpdate, exp.Wait)

	c := exp.NewSelectClauses()
	c2 := c.SetLock(l)

	scs.Nil(c.Lock())

	scs.Equal(l, c2.Lock())
}

func (scs *selectClausesSuite) TestSetLock() {
	l := exp.NewLock(exp.ForUpdate, exp.Wait)
	l2 := exp.NewLock(exp.ForUpdate, exp.NoWait)

	c := exp.NewSelectClauses().SetLock(l)
	c2 := c.SetLock(l2)

	scs.Equal(l, c.Lock())

	scs.Equal(l2, c2.Lock())
}

func (scs *selectClausesSuite) TestCommonTables() {
	cte := exp.NewCommonTableExpression(true, "test", newTestAppendableExpression(`SELECT * FROM "foo"`, []interface{}{}))

	c := exp.NewSelectClauses()
	c2 := c.CommonTablesAppend(cte)

	scs.Nil(c.CommonTables())

	scs.Equal([]exp.CommonTableExpression{cte}, c2.CommonTables())
}

func (scs *selectClausesSuite) TestAddCommonTablesAppend() {
	cte := exp.NewCommonTableExpression(true, "test", testSQLExpression("test_cte"))
	cte2 := exp.NewCommonTableExpression(true, "test", testSQLExpression("test_cte2"))

	c := exp.NewSelectClauses().CommonTablesAppend(cte)
	c2 := c.CommonTablesAppend(cte2)

	scs.Equal([]exp.CommonTableExpression{cte}, c.CommonTables())

	scs.Equal([]exp.CommonTableExpression{cte, cte2}, c2.CommonTables())
}
