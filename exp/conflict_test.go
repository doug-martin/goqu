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

	cols := exp.NewColumnListExpression(nil, "db", "a", "b")
	assert.Same(t, dn, dn.SetCols(cols))
	assert.Equal(t, cols, dn.Cols())
}

func TestDoNothingConflictClone(t *testing.T) {
	// no cols
	clone, ok := exp.NewDoNothingConflictExpression().Clone().(exp.ConflictNothingExpression)
	assert.True(t, ok, "clone must preserve ConflictNothingExpression")
	assert.Nil(t, clone.Cols())

	// cols are preserved and independent of the original
	dn := exp.NewDoNothingConflictExpression().SetCols(exp.NewColumnListExpression(nil, "db", "a"))
	clone, ok = dn.Clone().(exp.ConflictNothingExpression)
	assert.True(t, ok)
	assert.NotSame(t, dn, clone)
	assert.Equal(t, dn.Cols(), clone.Cols())

	_, ok = dn.Expression().(exp.ConflictNothingExpression)
	assert.True(t, ok)
}
