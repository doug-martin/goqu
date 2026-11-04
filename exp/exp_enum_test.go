package exp_test

import (
	"testing"

	"github.com/doug-martin/goqu/v9/exp"
	"github.com/stretchr/testify/assert"
)

// The exported enums share one iota sequence, so inserting a constant in the middle silently changes the value of
// every constant after it (e.g. NeqOp becoming EqOp for anything that persisted the raw value).
func TestExportedEnumValuesAreStable(t *testing.T) {
	assert.Equal(t, 19, int(exp.CrossJoinType))
	assert.Equal(t, 20, int(exp.UsingJoinCondType))
	assert.Equal(t, 21, int(exp.OnJoinCondType))
	assert.Equal(t, 22, int(exp.NoNullsSortType))
	assert.Equal(t, 25, int(exp.AscDir))
	assert.Equal(t, 27, int(exp.BetweenOp))
	assert.Equal(t, 29, int(exp.EqOp))
	assert.Equal(t, 30, int(exp.NeqOp))
	assert.Equal(t, 48, int(exp.BitwiseInversionOp))
	assert.Equal(t, 54, int(exp.ArithmeticAddOp))
}

func TestStraightJoinType(t *testing.T) {
	assert.Equal(t, exp.CrossJoinType+1, exp.StraightJoinType)
	assert.Equal(t, "StraightJoinType", exp.StraightJoinType.String())
	assert.True(t, exp.ConditionedJoinTypes[exp.StraightJoinType])
}

func TestCustomJoinType(t *testing.T) {
	assert.Equal(t, exp.CrossJoinType+2, exp.CustomJoinType)
	assert.NotEqual(t, exp.StraightJoinType, exp.CustomJoinType)
	assert.Equal(t, "CustomJoinType", exp.CustomJoinType.String())
	assert.False(t, exp.ConditionedJoinTypes[exp.CustomJoinType])
}
