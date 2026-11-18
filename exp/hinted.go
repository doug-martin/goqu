package exp

import (
	"slices"
	"strings"
)

type hintedExpression struct {
	source Expression
	hints  []string
}

// Creates a table source with table hints written after it (and after its alias), for use anywhere a table is
// accepted (e.g. From or a join). Blank hints are dropped, and a nested HintedExpression is flattened so its hints
// precede the new ones in a single list. The hints are written as is, so never build them from untrusted input:
//
//	NewHintedExpression(NewIdentifierExpression("", "test", ""), "NOLOCK") -> "test" WITH (NOLOCK)   (SQL Server)
//	NewHintedExpression(NewIdentifierExpression("", "test", ""), "USE INDEX (idx)") -> "test" USE INDEX (idx)   (MySQL)
func NewHintedExpression(source Expression, hints ...string) HintedExpression {
	filtered := nonBlank(hints)
	if nested, ok := source.(HintedExpression); ok {
		return hintedExpression{source: nested.Source(), hints: append(nested.Hints(), filtered...)}
	}
	return hintedExpression{source: source, hints: filtered}
}

// nonBlank copies the hints without the empty ones, so a hint list built from conditions cannot write "WITH ()".
func nonBlank(hints []string) []string {
	var out []string
	for _, h := range hints {
		if strings.TrimSpace(h) != "" {
			out = append(out, h)
		}
	}
	return out
}

func (he hintedExpression) Clone() Expression {
	return hintedExpression{source: he.source.Clone(), hints: slices.Clone(he.hints)}
}

func (he hintedExpression) Expression() Expression { return he }

func (he hintedExpression) Source() Expression { return he.source }

func (he hintedExpression) Hints() []string { return slices.Clone(he.hints) }
