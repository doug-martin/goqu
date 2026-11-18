package exp_test

import (
	"testing"

	"github.com/doug-martin/goqu/v9/exp"
	"github.com/stretchr/testify/assert"
)

func TestValuesExpression(t *testing.T) {
	rows := [][]any{{1, "a"}, {2, "b"}}
	v := exp.NewValuesExpression(rows...)

	assert.Equal(t, rows, v.Rows())
	assert.Nil(t, v.GetAs())
	assert.Nil(t, v.Cols())
	assert.Equal(t, v, v.Expression())

	// As returns a copy and leaves the receiver untouched
	named := v.As("t", "id", "name")
	assert.Nil(t, v.GetAs())
	assert.Equal(t, exp.ParseIdentifier("t"), named.GetAs())
	assert.Equal(t, exp.NewColumnListExpression("id", "name"), named.Cols())

	// an identifier alias and no columns
	aliased := named.As(exp.NewIdentifierExpression("", "x", ""))
	assert.Equal(t, exp.NewIdentifierExpression("", "x", ""), aliased.GetAs())
	assert.Nil(t, aliased.Cols())
	assert.Panics(t, func() { v.As(1) })

	// clones do not share their rows
	clone := named.Clone().(exp.ValuesExpression)
	assert.Equal(t, named, clone)
	rows[0][0] = 99
	assert.Equal(t, 1, v.Rows()[0][0])
	assert.Equal(t, 1, clone.Rows()[0][0])
}
