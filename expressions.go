package gql

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
)

type (
	//Parent of all expression types
	Expression interface {
		Clone() Expression
		Expression() Expression
	}
	//An Expression that generates its own sql (e.g Dataset)
	SqlExpression interface {
		Expression
		Sql() (string, error)
	}
)

type (
	ExpressionListType int
	ExpressionList     interface {
		Expression
		Type() ExpressionListType
		Expressions() []Expression
		Append(...Expression) ExpressionList
	}
	expressionList struct {
		operator    ExpressionListType
		expressions []Expression
	}
)

const (
	and_type ExpressionListType = iota
	or_type
)

// A list of expressions that should be ORed together
func Or(expressions ...Expression) expressionList {
	return expressionList{operator: or_type, expressions: expressions}
}

// A list of expressions that should be ANDed together
func And(expressions ...Expression) expressionList {
	return expressionList{operator: and_type, expressions: expressions}
}

func (me expressionList) Clone() Expression {
	newExps := make([]Expression, len(me.expressions))
	for i, exp := range me.expressions {
		newExps[i] = exp.Clone()
	}
	return expressionList{operator: me.operator, expressions: newExps}
}

func (me expressionList) Expression() Expression {
	return me
}

func (me expressionList) Type() ExpressionListType {
	return me.operator
}

func (me expressionList) Expressions() []Expression {
	return me.expressions
}

func (me expressionList) Append(expressions ...Expression) ExpressionList {
	ret := new(expressionList)
	ret.operator = me.operator
	exps := me.expressions
	for _, exp := range expressions {
		exps = append(exps, exp)
	}
	ret.expressions = exps
	return ret
}

type (
	ColumnList interface {
		Expression
		Columns() []Expression
		Append(...Expression) ColumnList
	}
	columnList struct {
		columns []Expression
	}
)

func emptyCols() ColumnList {
	return columnList{}
}

func cols(vals ...interface{}) ColumnList {
	var cols []Expression
	for _, val := range vals {
		switch val.(type) {
		case string:
			cols = append(cols, I(val.(string)))
		case Expression:
			cols = append(cols, val.(Expression))
		default:
			panic(fmt.Sprintf("Cannot created expression from  %+v", val))
		}
	}
	return columnList{columns: cols}
}

func orderList(vals ...OrderedExpression) ColumnList {
	exps := make([]Expression, len(vals))
	for i, col := range vals {
		exps[i] = col.Expression()
	}
	return columnList{columns: exps}
}

func (me columnList) Clone() Expression {
	newExps := make([]Expression, len(me.columns))
	for i, exp := range me.columns {
		newExps[i] = exp.Clone()
	}
	return columnList{columns: newExps}
}

func (me columnList) Expression() Expression {
	return me
}

func (me columnList) Columns() []Expression {
	return me.columns
}

func (me columnList) Append(cols ...Expression) ColumnList {
	ret := new(columnList)
	exps := append(ret.columns, me.columns...)
	for _, exp := range cols {
		exps = append(exps, exp)
	}
	ret.columns = exps
	return ret
}

type (
	JoinType      int
	JoinCondition int
	//Parent type for join expressions
	JoinExpression interface {
		Expression
		JoinCondition() JoinCondition
	}
	joinClause struct {
		joinCondition JoinCondition
	}
)

const (
	inner_join JoinType = iota
	full_outer_join
	right_outer_join
	left_outer_join
	full_join
	right_join
	left_join
	natural_join
	natural_left_join
	natural_right_join
	natural_full_join
	cross_join

	using_cond JoinCondition = iota
	on_cond
)

func (me joinClause) Clone() Expression {
	return joinClause{me.joinCondition}
}

func (me joinClause) Expression() Expression {
	return me
}

func (me joinClause) JoinCondition() JoinCondition {
	return me.joinCondition
}

type (
	//A join expression that uses an ON clause
	JoinOnExpression interface {
		JoinExpression
		On() ExpressionList
	}
	joinOnClause struct {
		joinClause
		on ExpressionList
	}
)

//Creates a new ON clause to be used within a join
func On(expressions ...Expression) JoinExpression {
	return joinOnClause{joinClause{on_cond}, And(expressions...)}
}

func (me joinOnClause) Clone() Expression {
	return joinOnClause{me.joinClause.Clone().(joinClause), me.on.Clone().(ExpressionList)}
}

func (me joinOnClause) Expression() Expression {
	return me
}

func (me joinOnClause) On() ExpressionList {
	return me.on
}

type (
	JoinUsingExpression interface {
		JoinExpression
		Using() ColumnList
	}
	//A join expression that uses an USING clause
	joinUsingClause struct {
		joinClause
		using ColumnList
	}
)

//Creates a new USING clause to be used within a join
func Using(expressions ...interface{}) JoinExpression {
	return joinUsingClause{joinClause{using_cond}, cols(expressions...)}
}

func (me joinUsingClause) Clone() Expression {
	return joinUsingClause{me.joinClause.Clone().(joinClause), me.using.Clone().(ColumnList)}
}

func (me joinUsingClause) Expression() Expression {
	return me
}

func (me joinUsingClause) Using() ColumnList {
	return me.using
}

type (
	aliasMethods interface {
		As(string) AliasedExpression
	}
	equalityMethods interface {
		Eq(interface{}) BooleanExpression
		Neq(interface{}) BooleanExpression
	}
	comparisonMethods interface {
		equalityMethods
		Gt(interface{}) BooleanExpression
		Gte(interface{}) BooleanExpression
		Lt(interface{}) BooleanExpression
		Lte(interface{}) BooleanExpression
	}
	inMethods interface {
		In(...interface{}) BooleanExpression
		NotIn(...interface{}) BooleanExpression
	}
	orderedMethods interface {
		Asc() OrderedExpression
		Desc() OrderedExpression
	}
	stringMethods interface {
		Like(interface{}) BooleanExpression
		NotLike(interface{}) BooleanExpression
		ILike(interface{}) BooleanExpression
		NotILike(interface{}) BooleanExpression
	}
	booleanMethods interface {
		Is(interface{}) BooleanExpression
		IsNot(interface{}) BooleanExpression
		IsNull() BooleanExpression
		IsNotNull() BooleanExpression
		IsTrue() BooleanExpression
		IsNotTrue() BooleanExpression
		IsFalse() BooleanExpression
		IsNotFalse() BooleanExpression
	}
	castMethods interface {
		Cast(val string) CastExpression
	}
	updateMethods interface {
		Set(interface{}) UpdateExpression
	}
	distinctMethods interface {
		Distinct() SqlFunctionExpression
	}
)

type (
	//An Identifier that can contain schema, table and column identifiers
	IdentifierExpression interface {
		Expression
		aliasMethods
		comparisonMethods
		inMethods
		stringMethods
		booleanMethods
		orderedMethods
		updateMethods
		distinctMethods
		castMethods
		Schema(string) IdentifierExpression
		GetSchema() string
		Table(string) IdentifierExpression
		GetTable() string
		Col(interface{}) IdentifierExpression
		GetCol() interface{}
		All() IdentifierExpression
	}
	identifier struct {
		schema string
		table  string
		col    interface{}
	}
)

//Creates a new Identifier to be used in queries see examples
func I(ident string) IdentifierExpression {
	parts := strings.Split(ident, ".")
	switch len(parts) {
	case 2:
		return identifier{}.Table(parts[0]).Col(parts[1])
	case 3:
		return identifier{}.Schema(parts[0]).Table(parts[1]).Col(parts[2])
	}
	return identifier{}.Col(ident)
}

func (me identifier) clone() identifier {
	return identifier{schema: me.schema, table: me.table, col: me.col}
}

func (me identifier) Clone() Expression {
	return me.clone()
}

func (me identifier) Table(table string) IdentifierExpression {
	ret := me.clone()
	if s, ok := me.col.(string); ok && s != "" && me.table == "" && me.schema == "" {
		ret.schema = s
		ret.col = nil
	}
	ret.table = table
	return ret
}

func (me identifier) GetTable() string {
	return me.table
}

func (me identifier) Schema(schema string) IdentifierExpression {
	ret := me.clone()
	ret.schema = schema
	return ret
}

func (me identifier) GetSchema() string {
	return me.schema
}

func (me identifier) Col(col interface{}) IdentifierExpression {
	ret := me.clone()
	if s, ok := me.col.(string); ok && s != "" && me.table == "" {
		ret.table = s
	}
	if col == "*" {
		ret.col = Star()
	} else {
		ret.col = col
	}

	return ret
}

func (me identifier) Expression() Expression { return me }

//Qualifies the epression with a * literal (e.g. "table".*)
func (me identifier) All() IdentifierExpression { return me.Col("*") }

//Gets the column identifier
func (me identifier) GetCol() interface{} { return me.col }

//Used within updates to set a column value
func (me identifier) Set(val interface{}) UpdateExpression { return set(me, val) }

//Alias an identifer (e.g "my_col" AS "other_col")
func (me identifier) As(as string) AliasedExpression { return aliased(me, as) }

//Returns a BooleanExpression for equality (e.g "my_col" = 1)
func (me identifier) Eq(val interface{}) BooleanExpression { return eq(me, val) }

//Returns a BooleanExpression for in equality (e.g "my_col" != 1)
func (me identifier) Neq(val interface{}) BooleanExpression { return neq(me, val) }

//Returns a BooleanExpression for checking that a identifier is greater than another value (e.g "my_col" > 1)
func (me identifier) Gt(val interface{}) BooleanExpression { return gt(me, val) }

//Returns a BooleanExpression for checking that a identifier is greater than or equal to another value (e.g "my_col" >= 1)
func (me identifier) Gte(val interface{}) BooleanExpression { return gte(me, val) }

//Returns a BooleanExpression for checking that a identifier is less than another value (e.g "my_col" < 1)
func (me identifier) Lt(val interface{}) BooleanExpression { return lt(me, val) }

//Returns a BooleanExpression for checking that a identifier is less than or equal to another value (e.g "my_col" <= 1)
func (me identifier) Lte(val interface{}) BooleanExpression { return lte(me, val) }

//Returns a BooleanExpression for checking that a identifier is in a list of values or  (e.g "my_col" > 1)
func (me identifier) In(vals ...interface{}) BooleanExpression    { return in(me, vals...) }
func (me identifier) NotIn(vals ...interface{}) BooleanExpression { return notIn(me, vals...) }
func (me identifier) Like(val interface{}) BooleanExpression      { return like(me, val) }
func (me identifier) NotLike(val interface{}) BooleanExpression   { return notLike(me, val) }
func (me identifier) ILike(val interface{}) BooleanExpression     { return iLike(me, val) }
func (me identifier) NotILike(val interface{}) BooleanExpression  { return notILike(me, val) }
func (me identifier) Is(val interface{}) BooleanExpression        { return is(me, val) }
func (me identifier) IsNot(val interface{}) BooleanExpression     { return isNot(me, val) }
func (me identifier) IsNull() BooleanExpression                   { return is(me, nil) }
func (me identifier) IsNotNull() BooleanExpression                { return isNot(me, nil) }
func (me identifier) IsTrue() BooleanExpression                   { return is(me, true) }
func (me identifier) IsNotTrue() BooleanExpression                { return isNot(me, true) }
func (me identifier) IsFalse() BooleanExpression                  { return is(me, false) }
func (me identifier) IsNotFalse() BooleanExpression               { return isNot(me, false) }
func (me identifier) Asc() OrderedExpression                      { return asc(me) }
func (me identifier) Desc() OrderedExpression                     { return desc(me) }
func (me identifier) Distinct() SqlFunctionExpression             { return DISTINCT(me) }
func (me identifier) Cast(t string) CastExpression                { return Cast(me, t) }

type (
	LiteralExpression interface {
		Expression
		aliasMethods
		comparisonMethods
		orderedMethods
		Literal(string) LiteralExpression
		GetLiteral() string
	}
	literal struct {
		literal string
	}
)

func Literal(val string) LiteralExpression {
	return literal{literal: val}
}

func Default() LiteralExpression {
	return literal{literal: "DEFAULT"}
}

func Star() LiteralExpression {
	return literal{literal: "*"}
}

func (me literal) Clone() Expression {
	return Literal(me.literal)
}

func (me literal) Literal(literal string) LiteralExpression {
	return Literal(literal)
}

func (me literal) GetLiteral() string {
	return me.literal
}

func (me literal) Expression() Expression                { return me }
func (me literal) As(as string) AliasedExpression        { return aliased(me, as) }
func (me literal) Eq(val interface{}) BooleanExpression  { return eq(me, val) }
func (me literal) Neq(val interface{}) BooleanExpression { return neq(me, val) }
func (me literal) Gt(val interface{}) BooleanExpression  { return gt(me, val) }
func (me literal) Gte(val interface{}) BooleanExpression { return gte(me, val) }
func (me literal) Lt(val interface{}) BooleanExpression  { return lt(me, val) }
func (me literal) Lte(val interface{}) BooleanExpression { return lte(me, val) }
func (me literal) Asc() OrderedExpression                { return asc(me) }
func (me literal) Desc() OrderedExpression               { return desc(me) }

type (
	UpdateExpression interface {
		Col() IdentifierExpression
		Val() interface{}
	}
	update struct {
		col IdentifierExpression
		val interface{}
	}
)

func set(col IdentifierExpression, val interface{}) UpdateExpression {
	return update{col: col, val: val}
}

func (me update) Expression() Expression {
	return me
}

func (me update) Clone() Expression {
	return update{col: me.col.Clone().(IdentifierExpression), val: me.val}
}

func (me update) Col() IdentifierExpression {
	return me.col
}

func (me update) Val() interface{} {
	return me.val
}

type (
	BooleanOperation  int
	BooleanExpression interface {
		Expression
		Op() BooleanOperation
		Lhs() Expression
		Rhs() interface{}
	}
	boolean struct {
		lhs Expression
		rhs interface{}
		op  BooleanOperation
	}
)

const (
	eq_op BooleanOperation = iota
	neq_op
	is_op
	is_not_op
	gt_op
	gte_op
	lt_op
	lte_op
	in_op
	not_in_op
	like_op
	not_like_op
	i_like_op
	not_i_like_op
	regexp_like_op
	regexp_not_like_op
	regexp_i_like_op
	regexp_not_i_like_op
)

var operator_inversions = map[BooleanOperation]BooleanOperation{
	is_op:                is_not_op,
	eq_op:                neq_op,
	gt_op:                lte_op,
	gte_op:               lt_op,
	lt_op:                gte_op,
	lte_op:               gt_op,
	in_op:                not_in_op,
	like_op:              not_like_op,
	i_like_op:            not_i_like_op,
	regexp_like_op:       regexp_not_like_op,
	regexp_i_like_op:     regexp_not_i_like_op,
	is_not_op:            is_op,
	neq_op:               eq_op,
	not_in_op:            in_op,
	not_like_op:          like_op,
	not_i_like_op:        i_like_op,
	regexp_not_like_op:   regexp_like_op,
	regexp_not_i_like_op: regexp_i_like_op,
}

func (me boolean) Clone() Expression {
	return boolean{op: me.op, lhs: me.lhs.Clone(), rhs: me.rhs}
}

func (me boolean) Expression() Expression {
	return me
}

func (me boolean) Rhs() interface{} {
	return me.rhs
}

func (me boolean) Lhs() Expression {
	return me.lhs
}

func (me boolean) Op() BooleanOperation {
	return me.op
}

func eq(lhs Expression, rhs interface{}) BooleanExpression {
	return checkBoolExpType(eq_op, lhs, rhs, false)
}

func neq(lhs Expression, rhs interface{}) BooleanExpression {
	return checkBoolExpType(eq_op, lhs, rhs, true)
}

func gt(lhs Expression, rhs interface{}) BooleanExpression {
	return boolean{op: gt_op, lhs: lhs, rhs: rhs}
}

func gte(lhs Expression, rhs interface{}) BooleanExpression {
	return boolean{op: gte_op, lhs: lhs, rhs: rhs}
}

func lt(lhs Expression, rhs interface{}) BooleanExpression {
	return boolean{op: lt_op, lhs: lhs, rhs: rhs}
}

func lte(lhs Expression, rhs interface{}) BooleanExpression {
	return boolean{op: lte_op, lhs: lhs, rhs: rhs}
}

func in(lhs Expression, vals ...interface{}) BooleanExpression {
	if len(vals) == 1 && reflect.Indirect(reflect.ValueOf(vals[0])).Kind() == reflect.Slice {
		return boolean{op: in_op, lhs: lhs, rhs: vals[0]}
	}
	return boolean{op: in_op, lhs: lhs, rhs: vals}
}

func notIn(lhs Expression, vals ...interface{}) BooleanExpression {
	if len(vals) == 1 && reflect.Indirect(reflect.ValueOf(vals[0])).Kind() == reflect.Slice {
		return boolean{op: not_in_op, lhs: lhs, rhs: vals[0]}
	}
	return boolean{op: not_in_op, lhs: lhs, rhs: vals}
}

func is(lhs Expression, val interface{}) BooleanExpression {
	return checkBoolExpType(is_op, lhs, val, false)
}
func isNot(lhs Expression, val interface{}) BooleanExpression {
	return checkBoolExpType(is_op, lhs, val, true)
}
func like(lhs Expression, val interface{}) BooleanExpression {
	return checkLikeExp(like_op, lhs, val, false)
}
func iLike(lhs Expression, val interface{}) BooleanExpression {
	return checkLikeExp(i_like_op, lhs, val, false)
}
func notLike(lhs Expression, val interface{}) BooleanExpression {
	return checkLikeExp(like_op, lhs, val, true)
}
func notILike(lhs Expression, val interface{}) BooleanExpression {
	return checkLikeExp(i_like_op, lhs, val, true)
}

func checkLikeExp(op BooleanOperation, lhs Expression, val interface{}, invert bool) BooleanExpression {
	rhs := val
	switch val.(type) {
	case *regexp.Regexp:
		if op == like_op {
			op = regexp_like_op
		} else if op == i_like_op {
			op = regexp_i_like_op
		}
		rhs = val.(*regexp.Regexp).String()
	}
	if invert {
		op = operator_inversions[op]
	}
	return boolean{op: op, lhs: lhs, rhs: rhs}
}

func checkBoolExpType(op BooleanOperation, lhs Expression, rhs interface{}, invert bool) BooleanExpression {
	if rhs == nil {
		op = is_op
	} else {
		switch reflect.Indirect(reflect.ValueOf(rhs)).Kind() {
		case reflect.Bool:
			op = is_op
		case reflect.Slice:
			op = in_op
		case reflect.Struct:
			switch rhs.(type) {
			case *regexp.Regexp:
				return checkLikeExp(like_op, lhs, rhs, invert)
			}
		}
	}
	if invert {
		op = operator_inversions[op]
	}
	return boolean{op: op, lhs: lhs, rhs: rhs}
}

type (
	AliasedExpression interface {
		Expression
		Aliased() Expression
		GetAs() IdentifierExpression
	}
	aliasExpression struct {
		aliased Expression
		alias   IdentifierExpression
	}
)

func aliased(exp Expression, alias string) AliasedExpression {
	return aliasExpression{aliased: exp, alias: I(alias)}
}

func (me aliasExpression) Clone() Expression {
	return aliasExpression{aliased: me.aliased, alias: me.alias.Clone().(IdentifierExpression)}
}

func (me aliasExpression) Expression() Expression {
	return me
}

func (me aliasExpression) Aliased() Expression {
	return me.aliased
}

func (me aliasExpression) GetAs() IdentifierExpression {
	return me.alias
}

type (
	null_sort_type    int
	sort_direction    int
	OrderedExpression interface {
		Expression
		SortExpression() Expression
		Direction() sort_direction
		NullSortType() null_sort_type
		NullsFirst() OrderedExpression
		NullsLast() OrderedExpression
	}
	orderedExpression struct {
		sortExpression Expression
		direction      sort_direction
		nullSortType   null_sort_type
	}
)

const (
	no_nulls null_sort_type = iota
	nulls_first
	nulls_last

	sort_asc sort_direction = iota
	sort_desc
)

func asc(exp Expression) OrderedExpression {
	return orderedExpression{sortExpression: exp, direction: sort_asc, nullSortType: no_nulls}
}

func desc(exp Expression) OrderedExpression {
	return orderedExpression{sortExpression: exp, direction: sort_desc, nullSortType: no_nulls}
}

func (me orderedExpression) Clone() Expression {
	return orderedExpression{sortExpression: me.sortExpression, direction: me.direction, nullSortType: me.nullSortType}
}

func (me orderedExpression) Expression() Expression {
	return me
}

func (me orderedExpression) SortExpression() Expression {
	return me.sortExpression
}

func (me orderedExpression) Direction() sort_direction {
	return me.direction
}

func (me orderedExpression) NullSortType() null_sort_type {
	return me.nullSortType
}

func (me orderedExpression) NullsFirst() OrderedExpression {
	return orderedExpression{sortExpression: me.sortExpression, direction: me.direction, nullSortType: nulls_first}
}

func (me orderedExpression) NullsLast() OrderedExpression {
	return orderedExpression{sortExpression: me.sortExpression, direction: me.direction, nullSortType: nulls_last}
}

type (
	SqlFunctionExpression interface {
		Expression
		aliasMethods
		comparisonMethods
		orderedMethods
		Name() string
		Args() []interface{}
	}
	sqlFunctionExpression struct {
		name string
		args []interface{}
	}
)

func Func(name string, args ...interface{}) SqlFunctionExpression {
	return sqlFunctionExpression{name: name, args: args}
}

func colFunc(name string, col interface{}) SqlFunctionExpression {
	if s, ok := col.(string); ok {
		col = I(s)
	}
	return Func(name, col)
}

func DISTINCT(col interface{}) SqlFunctionExpression { return colFunc("DISTINCT", col) }
func COUNT(col interface{}) SqlFunctionExpression    { return colFunc("COUNT", col) }
func MIN(col interface{}) SqlFunctionExpression      { return colFunc("MIN", col) }
func MAX(col interface{}) SqlFunctionExpression      { return colFunc("MAX", col) }
func AVG(col interface{}) SqlFunctionExpression      { return colFunc("AVG", col) }
func FIRST(col interface{}) SqlFunctionExpression    { return colFunc("FIRST", col) }
func LAST(col interface{}) SqlFunctionExpression     { return colFunc("LAST", col) }
func SUM(col interface{}) SqlFunctionExpression      { return colFunc("SUM", col) }

func COALESCE(vals ...interface{}) SqlFunctionExpression {
	return Func("COALESCE", vals...)
}

func (me sqlFunctionExpression) Clone() Expression {
	return sqlFunctionExpression{name: me.name, args: me.args}
}

func (me sqlFunctionExpression) Expression() Expression                { return me }
func (me sqlFunctionExpression) Args() []interface{}                   { return me.args }
func (me sqlFunctionExpression) Name() string                          { return me.name }
func (me sqlFunctionExpression) As(as string) AliasedExpression        { return aliased(me, as) }
func (me sqlFunctionExpression) Eq(val interface{}) BooleanExpression  { return eq(me, val) }
func (me sqlFunctionExpression) Neq(val interface{}) BooleanExpression { return neq(me, val) }
func (me sqlFunctionExpression) Gt(val interface{}) BooleanExpression  { return gt(me, val) }
func (me sqlFunctionExpression) Gte(val interface{}) BooleanExpression { return gte(me, val) }
func (me sqlFunctionExpression) Lt(val interface{}) BooleanExpression  { return lt(me, val) }
func (me sqlFunctionExpression) Lte(val interface{}) BooleanExpression { return lte(me, val) }
func (me sqlFunctionExpression) Asc() OrderedExpression                { return asc(me) }
func (me sqlFunctionExpression) Desc() OrderedExpression               { return desc(me) }

type (
	CastExpression interface {
		Expression
		aliasMethods
		comparisonMethods
		inMethods
		stringMethods
		booleanMethods
		orderedMethods
		distinctMethods
		Casted() Expression
		Type() LiteralExpression
	}
	cast struct {
		casted Expression
		t      LiteralExpression
	}
)

func Cast(e Expression, t string) CastExpression {
	return cast{casted: e, t: Literal(t)}
}

func (me cast) Casted() Expression {
	return me.casted
}

func (me cast) Type() LiteralExpression {
	return me.t
}

func (me cast) Clone() Expression {
	return cast{casted: me.casted.Clone(), t: me.t}
}

func (me cast) Expression() Expression                   { return me }
func (me cast) As(as string) AliasedExpression           { return aliased(me, as) }
func (me cast) Eq(val interface{}) BooleanExpression     { return eq(me, val) }
func (me cast) Neq(val interface{}) BooleanExpression    { return neq(me, val) }
func (me cast) Gt(val interface{}) BooleanExpression     { return gt(me, val) }
func (me cast) Gte(val interface{}) BooleanExpression    { return gte(me, val) }
func (me cast) Lt(val interface{}) BooleanExpression     { return lt(me, val) }
func (me cast) Lte(val interface{}) BooleanExpression    { return lte(me, val) }
func (me cast) Asc() OrderedExpression                   { return asc(me) }
func (me cast) Desc() OrderedExpression                  { return desc(me) }
func (me cast) Like(i interface{}) BooleanExpression     { return like(me, i) }
func (me cast) NotLike(i interface{}) BooleanExpression  { return notLike(me, i) }
func (me cast) ILike(i interface{}) BooleanExpression    { return iLike(me, i) }
func (me cast) NotILike(i interface{}) BooleanExpression { return notILike(me, i) }
func (me cast) In(i ...interface{}) BooleanExpression    { return in(me, i...) }
func (me cast) NotIn(i ...interface{}) BooleanExpression { return notIn(me, i...) }
func (me cast) Is(i interface{}) BooleanExpression       { return is(me, i) }
func (me cast) IsNot(i interface{}) BooleanExpression    { return isNot(me, i) }
func (me cast) IsNull() BooleanExpression                { return is(me, nil) }
func (me cast) IsNotNull() BooleanExpression             { return isNot(me, nil) }
func (me cast) IsTrue() BooleanExpression                { return is(me, true) }
func (me cast) IsNotTrue() BooleanExpression             { return isNot(me, true) }
func (me cast) IsFalse() BooleanExpression               { return is(me, false) }
func (me cast) IsNotFalse() BooleanExpression            { return isNot(me, nil) }
func (me cast) Distinct() SqlFunctionExpression          { return DISTINCT(me) }

type (
	compoundType       int
	CompoundExpression interface {
		Expression
		Type() compoundType
		Rhs() SqlExpression
	}
	compound struct {
		t   compoundType
		rhs SqlExpression
	}
)

const (
	union compoundType = iota
	union_all
	intersect
	intersect_all
)

func Union(rhs SqlExpression) CompoundExpression {
	return compound{t: union, rhs: rhs}
}

func UnionAll(rhs SqlExpression) CompoundExpression {
	return compound{t: union_all, rhs: rhs}
}

func Intersect(rhs SqlExpression) CompoundExpression {
	return compound{t: intersect, rhs: rhs}
}

func IntersectAll(rhs SqlExpression) CompoundExpression {
	return compound{t: intersect_all, rhs: rhs}
}

func (me compound) Expression() Expression { return me }

func (me compound) Clone() Expression {
	return compound{t: me.t, rhs: me.rhs.Clone().(SqlExpression)}
}

func (me compound) Type() compoundType { return me.t }
func (me compound) Rhs() SqlExpression { return me.rhs }
