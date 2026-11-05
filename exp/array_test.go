package exp_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/doug-martin/goqu/v9/exp"
)

func TestArrayExpression(t *testing.T) {
	a := exp.NewArrayExpression([]int{1, 2})
	require.Equal(t, []int{1, 2}, a.Elements())
	require.Equal(t, a, a.Expression())
	require.Equal(t, a, a.Clone())
}
