package exp

import "fmt"

type valuesExpression struct {
	rows  [][]any
	alias IdentifierExpression
	cols  ColumnListExpression
}

// Creates a VALUES table expression from rows of values, for use anywhere a table is accepted (e.g. From):
//
//	NewValuesExpression([]any{1, "a"}, []any{2, "b"}).As("t", "id", "name") ->
//		(VALUES (1, 'a'), (2, 'b')) AS "t"("id", "name")
func NewValuesExpression(rows ...[]any) ValuesExpression {
	return valuesExpression{rows: cloneRows(rows)}
}

func cloneRows(rows [][]any) [][]any {
	clone := make([][]any, len(rows))
	for i, row := range rows {
		clone[i] = append([]any(nil), row...)
	}
	return clone
}

func (ve valuesExpression) Clone() Expression {
	clone := valuesExpression{rows: cloneRows(ve.rows)}
	if ve.alias != nil {
		clone.alias = ve.alias.Clone().(IdentifierExpression)
	}
	if ve.cols != nil {
		clone.cols = ve.cols.Clone().(ColumnListExpression)
	}
	return clone
}

func (ve valuesExpression) Expression() Expression {
	return ve
}

func (ve valuesExpression) Rows() [][]any {
	return ve.rows
}

func (ve valuesExpression) GetAs() IdentifierExpression {
	return ve.alias
}

func (ve valuesExpression) Cols() ColumnListExpression {
	return ve.cols
}

// Returns a new ValuesExpression with the table alias and, optionally, the names of its columns.
func (ve valuesExpression) As(alias any, cols ...any) ValuesExpression {
	clone := ve.Clone().(valuesExpression)
	switch v := alias.(type) {
	case string:
		clone.alias = ParseIdentifier(v)
	case IdentifierExpression:
		clone.alias = v
	default:
		panic(fmt.Sprintf("Cannot create alias from %+v", v))
	}
	clone.cols = nil
	if len(cols) > 0 {
		clone.cols = NewColumnListExpression(cols...)
	}
	return clone
}
