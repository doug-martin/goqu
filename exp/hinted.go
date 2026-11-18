package exp

import "slices"

type hintedExpression struct {
	source Expression
	hints  []string
}

// Creates a table source with table hints written after it (and after its alias), for use anywhere a table is
// accepted (e.g. From or a join). The hints are written as is, so never build them from untrusted input:
//
//	NewHintedExpression(NewIdentifierExpression("", "test", ""), "NOLOCK") -> "test" WITH (NOLOCK)   (SQL Server)
//	NewHintedExpression(NewIdentifierExpression("", "test", ""), "USE INDEX (idx)") -> "test" USE INDEX (idx)   (MySQL)
func NewHintedExpression(source Expression, hints ...string) HintedExpression {
	return hintedExpression{source: source, hints: slices.Clone(hints)}
}

func (he hintedExpression) Clone() Expression {
	return hintedExpression{source: he.source.Clone(), hints: slices.Clone(he.hints)}
}

func (he hintedExpression) Expression() Expression { return he }

func (he hintedExpression) Source() Expression { return he.source }

func (he hintedExpression) Hints() []string { return slices.Clone(he.hints) }
