package exp_test

import (
	"testing"

	"github.com/doug-martin/goqu/v9/exp"
	"github.com/stretchr/testify/assert"
)

func TestDoNothingConflictCols(t *testing.T) {
	dn := exp.NewDoNothingConflictExpression()
	assert.Equal(t, exp.DoNothingConflictAction, dn.Action())
	assert.Nil(t, dn.Cols())

	cols := exp.NewColumnListExpression("a", "b")
	assert.Same(t, dn, dn.SetCols(cols))
	assert.Equal(t, cols, dn.Cols())
}

func TestDoNothingConflictClone(t *testing.T) {
	// no cols
	clone, ok := exp.NewDoNothingConflictExpression().Clone().(exp.ConflictNothingExpression)
	assert.True(t, ok, "clone must preserve ConflictNothingExpression")
	assert.Nil(t, clone.Cols())

	// cols are preserved and independent of the original
	dn := exp.NewDoNothingConflictExpression().SetCols(exp.NewColumnListExpression("a"))
	clone, ok = dn.Clone().(exp.ConflictNothingExpression)
	assert.True(t, ok)
	assert.NotSame(t, dn, clone)
	assert.Equal(t, dn.Cols(), clone.Cols())

	_, ok = dn.Expression().(exp.ConflictNothingExpression)
	assert.True(t, ok)
}

func TestConflictTargetBuilders(t *testing.T) {
	cols := exp.NewColumnListExpression("a", "b")
	where := exp.Ex{"c": nil}

	dn := exp.NewDoNothingConflictExpression()
	assert.Empty(t, dn.Constraint())
	assert.Nil(t, dn.TargetWhereClause())
	assert.Same(t, dn, dn.SetCols(cols).OnConstraint("k").TargetWhere(where))
	assert.Equal(t, "k", dn.Constraint())
	assert.NotNil(t, dn.TargetWhereClause())
	dnClone := dn.Clone().(exp.ConflictNothingExpression)
	assert.Equal(t, dn.Cols(), dnClone.Cols())
	assert.Equal(t, "k", dnClone.Constraint())
	assert.Equal(t, dn.TargetWhereClause(), dnClone.TargetWhereClause())

	du := exp.NewDoUpdateConflictExpression("t", exp.Record{"a": 1})
	assert.Nil(t, du.Cols())
	assert.Empty(t, du.Constraint())
	assert.Nil(t, du.TargetWhereClause())
	assert.Same(t, du, du.SetCols(cols).OnConstraint("k").TargetWhere(where).TargetWhere(exp.Ex{"d": 1}))
	assert.Equal(t, "k", du.Constraint())
	assert.Equal(t, "t", du.TargetColumn())
	assert.Len(t, du.TargetWhereClause().Expressions(), 2)
	duClone := du.Clone().(exp.ConflictUpdateExpression)
	assert.Equal(t, du.Cols(), duClone.Cols())
	assert.Equal(t, "k", duClone.Constraint())
	assert.Equal(t, du.TargetWhereClause(), duClone.TargetWhereClause())
	assert.Nil(t, duClone.WhereClause())
}

func TestDoUpdateConflictCloneWithWhere(t *testing.T) {
	du := exp.NewDoUpdateConflictExpression("t", exp.Record{"a": 1}).Where(exp.Ex{"b": 1})
	clone := du.Clone().(exp.ConflictUpdateExpression)
	assert.Equal(t, du.WhereClause(), clone.WhereClause())
}
