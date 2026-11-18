package exp_test

import (
	"testing"

	"github.com/doug-martin/goqu/v9/exp"
	"github.com/stretchr/testify/assert"
)

func TestOpaqueExpression(t *testing.T) {
	val := []string{"a", "b"}
	o := exp.NewOpaqueExpression(val)

	assert.Equal(t, val, o.Value())
	assert.Equal(t, o, o.Expression())

	clone, ok := o.Clone().(exp.OpaqueExpression)
	assert.True(t, ok)
	assert.Equal(t, val, clone.Value())
	assert.Equal(t, o, clone)

	assert.Nil(t, exp.NewOpaqueExpression(nil).Value())
}
