package exp_test

import (
	"testing"

	"github.com/doug-martin/goqu/v9/exp"
	"github.com/stretchr/testify/assert"
)

func TestHintedExpression(t *testing.T) {
	source := exp.NewIdentifierExpression("", "test", "")
	hints := []string{"NOLOCK", "INDEX(ix)"}
	h := exp.NewHintedExpression(source, hints...)

	assert.Equal(t, source, h.Source())
	assert.Equal(t, []string{"NOLOCK", "INDEX(ix)"}, h.Hints())
	assert.Equal(t, h, h.Expression())

	// the hints are copied on the way in and on the way out
	hints[0] = "changed"
	h.Hints()[1] = "changed"
	assert.Equal(t, []string{"NOLOCK", "INDEX(ix)"}, h.Hints())

	clone, ok := h.Clone().(exp.HintedExpression)
	if assert.True(t, ok) {
		assert.Equal(t, h, clone)
		assert.Equal(t, source, clone.Source())
		assert.Equal(t, []string{"NOLOCK", "INDEX(ix)"}, clone.Hints())
	}

	assert.Empty(t, exp.NewHintedExpression(source).Hints())
}

func TestHintedExpression_dropsBlankHints(t *testing.T) {
	source := exp.NewIdentifierExpression("", "test", "")
	assert.Equal(t, []string{"NOLOCK"}, exp.NewHintedExpression(source, "", "NOLOCK", "  ").Hints())
	assert.Empty(t, exp.NewHintedExpression(source, "", " ").Hints())
}

func TestHintedExpression_flattensNestedHints(t *testing.T) {
	source := exp.NewIdentifierExpression("", "test", "")
	inner := exp.NewHintedExpression(source, "NOLOCK")
	outer := exp.NewHintedExpression(inner, "", "INDEX(ix)")

	assert.Equal(t, source, outer.Source())
	assert.Equal(t, []string{"NOLOCK", "INDEX(ix)"}, outer.Hints())
	assert.Equal(t, []string{"NOLOCK"}, inner.Hints())
	assert.Empty(t, exp.NewHintedExpression(exp.NewHintedExpression(source, ""), " ").Hints())
}
