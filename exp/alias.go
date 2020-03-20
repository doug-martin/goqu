package exp

import "fmt"

type (
	aliasExpression struct {
		aliased Expression
		alias   IdentifierExpression
	}
)

// used internally by other expressions to create a new aliased expression
func aliased(exp Expression, alias interface{}) AliasedExpression {
	switch v := alias.(type) {
	case string:
		return aliasExpression{aliased: exp, alias: ParseIdentifier(v)}
	case IdentifierExpression:
		return aliasExpression{aliased: exp, alias: v}
	default:
		panic(fmt.Sprintf("Cannot create alias from %+v", v))
	}
}

func (ae aliasExpression) Clone() Expression {
	return aliasExpression{aliased: ae.aliased, alias: ae.alias.Clone().(IdentifierExpression)}
}

func (ae aliasExpression) Expression() Expression {
	return ae
}

func (ae aliasExpression) Aliased() Expression {
	return ae.aliased
}

func (ae aliasExpression) GetAs() IdentifierExpression {
	return ae.alias
}

// Returns a new IdentifierExpression with the specified schema
func (ae aliasExpression) Schema(schema string) IdentifierExpression {
	return ae.alias.Schema(schema)
}

// Returns a new IdentifierExpression with the specified table
func (ae aliasExpression) Table(table string) IdentifierExpression {
	return ae.alias.Table(table)
}

// Returns a new IdentifierExpression with the specified column
func (ae aliasExpression) Col(col interface{}) IdentifierExpression {
	return ae.alias.Col(col)
}

// Returns a new IdentifierExpression with the column set to *
//   I("my_table").As("t").All() //"t".*
func (ae aliasExpression) All() IdentifierExpression {
	return ae.alias.All()
}
