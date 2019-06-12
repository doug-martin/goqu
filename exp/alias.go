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
