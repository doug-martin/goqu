package exp

import (
	"sort"
	"strings"

	"github.com/doug-martin/goqu/v7/internal/errors"
)

type (
	expressionList struct {
		operator    ExpressionListType
		expressions []Expression
	}
)

// A list of expressions that should be ORed together
//    Or(I("a").Eq(10), I("b").Eq(11)) //(("a" = 10) OR ("b" = 11))
func NewExpressionList(operator ExpressionListType, expressions ...Expression) ExpressionList {
	return expressionList{operator: operator, expressions: expressions}
}

func (el expressionList) Clone() Expression {
	newExps := make([]Expression, len(el.expressions))
	for i, exp := range el.expressions {
		newExps[i] = exp.Clone()
	}
	return expressionList{operator: el.operator, expressions: newExps}
}

func (el expressionList) Expression() Expression {
	return el
}

func (el expressionList) Type() ExpressionListType {
	return el.operator
}

func (el expressionList) Expressions() []Expression {
	return el.expressions
}

func (el expressionList) Append(expressions ...Expression) ExpressionList {
	ret := new(expressionList)
	ret.operator = el.operator
	exps := make([]Expression, len(el.expressions))
	copy(exps, el.expressions)
	exps = append(exps, expressions...)
	ret.expressions = exps
	return ret
}

func getExMapKeys(ex map[string]interface{}) []string {
	var keys []string
	for key := range ex {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func mapToExpressionList(ex map[string]interface{}, eType ExpressionListType) (ExpressionList, error) {
	keys := getExMapKeys(ex)
	ret := make([]Expression, len(keys))
	for i, key := range keys {
		lhs := ParseIdentifier(key)
		rhs := ex[key]
		var exp Expression
		if op, ok := rhs.(Op); ok {
			ors, err := createOredExpressionFromMap(lhs, op)
			if err != nil {
				return nil, err
			}
			exp = NewExpressionList(OrType, ors...)
		} else {
			exp = lhs.Eq(rhs)
		}
		ret[i] = exp
	}
	if eType == OrType {
		return NewExpressionList(OrType, ret...), nil
	}
	return NewExpressionList(AndType, ret...), nil
}

func createOredExpressionFromMap(lhs IdentifierExpression, op Op) ([]Expression, error) {
	opKeys := getExMapKeys(op)
	ors := make([]Expression, len(opKeys))
	for j, opKey := range opKeys {
		if exp, err := createExpressionFromOp(lhs, opKey, op); err != nil {
			return nil, err
		} else if exp != nil {
			ors[j] = exp
		}
	}
	return ors, nil
}

func createExpressionFromOp(lhs IdentifierExpression, opKey string, op Op) (exp Expression, err error) {
	switch strings.ToLower(opKey) {
	case "eq":
		exp = lhs.Eq(op[opKey])
	case "neq":
		exp = lhs.Neq(op[opKey])
	case "is":
		exp = lhs.Is(op[opKey])
	case "isnot":
		exp = lhs.IsNot(op[opKey])
	case "gt":
		exp = lhs.Gt(op[opKey])
	case "gte":
		exp = lhs.Gte(op[opKey])
	case "lt":
		exp = lhs.Lt(op[opKey])
	case "lte":
		exp = lhs.Lte(op[opKey])
	case "in":
		exp = lhs.In(op[opKey])
	case "notin":
		exp = lhs.NotIn(op[opKey])
	case "like":
		exp = lhs.Like(op[opKey])
	case "notlike":
		exp = lhs.NotLike(op[opKey])
	case "ilike":
		exp = lhs.ILike(op[opKey])
	case "notilike":
		exp = lhs.NotILike(op[opKey])
	case "between":
		rangeVal, ok := op[opKey].(RangeVal)
		if ok {
			exp = lhs.Between(rangeVal)
		}
	case "notbetween":
		rangeVal, ok := op[opKey].(RangeVal)
		if ok {
			exp = lhs.NotBetween(rangeVal)
		}
	default:
		err = errors.New("unsupported expression type %s", op)
	}
	return exp, err
}
