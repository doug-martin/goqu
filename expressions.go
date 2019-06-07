package goqu

import (
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
)

type (
	//Alternative to writing map[string]interface{}. Can be used for Inserts, Updates or Deletes
	Record map[string]interface{}
	//Parent of all expression types
	Expression interface {
		Clone() Expression
		Expression() Expression
	}
	//An Expression that generates its own sql (e.g Dataset)
	SqlExpression interface {
		Expression
		ToSql() (string, []interface{}, error)
	}
)

type (
	ExpressionListType int
	//A list of expressions that should be joined together
	//    And(I("a").Eq(10), I("b").Eq(11)) //(("a" = 10) AND ("b" = 11))
	//    Or(I("a").Eq(10), I("b").Eq(11)) //(("a" = 10) OR ("b" = 11))
	ExpressionList interface {
		Expression
		//Returns type (e.g. OR, AND)
		Type() ExpressionListType
		//Slice of expressions that should be joined togehter
		Expressions() []Expression
		//Returns a new expression list with the given expressions appended to the current Expressions list
		Append(...Expression) ExpressionList
	}
	expressionList struct {
		operator    ExpressionListType
		expressions []Expression
	}
)

const (
	AND_TYPE ExpressionListType = iota
	OR_TYPE
)

func getExMapKeys(ex map[string]interface{}) []string {
	var keys []string
	for key, _ := range ex {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func mapToExpressionList(ex map[string]interface{}, eType ExpressionListType) (ExpressionList, error) {
	keys := getExMapKeys(ex)
	ret := make([]Expression, len(keys))
	for i, key := range keys {
		lhs := I(key)
		rhs := ex[key]
		var exp Expression
		if op, ok := rhs.(Op); ok {
			opKeys := getExMapKeys(op)
			ors := make([]Expression, len(opKeys))
			for j, opKey := range opKeys {
				var ored Expression
				switch strings.ToLower(opKey) {
				case "eq":
					ored = lhs.Eq(op[opKey])
				case "neq":
					ored = lhs.Neq(op[opKey])
				case "is":
					ored = lhs.Is(op[opKey])
				case "isnot":
					ored = lhs.IsNot(op[opKey])
				case "gt":
					ored = lhs.Gt(op[opKey])
				case "gte":
					ored = lhs.Gte(op[opKey])
				case "lt":
					ored = lhs.Lt(op[opKey])
				case "lte":
					ored = lhs.Lte(op[opKey])
				case "in":
					ored = lhs.In(op[opKey])
				case "notin":
					ored = lhs.NotIn(op[opKey])
				case "like":
					ored = lhs.Like(op[opKey])
				case "notlike":
					ored = lhs.NotLike(op[opKey])
				case "ilike":
					ored = lhs.ILike(op[opKey])
				case "notilike":
					ored = lhs.NotILike(op[opKey])
				case "between":
					rangeVal, ok := op[opKey].(RangeVal)
					if ok {
						ored = lhs.Between(rangeVal)
					}
				case "notbetween":
					rangeVal, ok := op[opKey].(RangeVal)
					if ok {
						ored = lhs.NotBetween(rangeVal)
					}
				default:
					return nil, NewGoquError("Unsupported expression type %s", op)
				}
				ors[j] = ored
			}
			exp = Or(ors...)
		} else {
			exp = lhs.Eq(rhs)
		}
		ret[i] = exp
	}
	if eType == OR_TYPE {
		return Or(ret...), nil
	}
	return And(ret...), nil
}

// A list of expressions that should be ORed together
//    Or(I("a").Eq(10), I("b").Eq(11)) //(("a" = 10) OR ("b" = 11))
func Or(expressions ...Expression) expressionList {
	return expressionList{operator: OR_TYPE, expressions: expressions}
}

// A list of expressions that should be ANDed together
//    And(I("a").Eq(10), I("b").Eq(11)) //(("a" = 10) AND ("b" = 11))
func And(expressions ...Expression) expressionList {
	return expressionList{operator: AND_TYPE, expressions: expressions}
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
	exps := make([]Expression, len(me.expressions))
	copy(exps, me.expressions)
	for _, exp := range expressions {
		exps = append(exps, exp)
	}
	ret.expressions = exps
	return ret
}

type (
	//A map of expressions to be ANDed together where the keys are string that will be used as Identifiers and values will be used in a boolean operation.
	//The Ex map can be used in tandem with Op map to create more complex expression such as LIKE, GT, LT... See examples.
	Ex map[string]interface{}
	//A map of expressions to be ORed together where the keys are string that will be used as Identifiers and values will be used in a boolean operation.
	//The Ex map can be used in tandem with Op map to create more complex expression such as LIKE, GT, LT... See examples.
	ExOr map[string]interface{}
	//Used in tandem with the Ex map to create complex comparisons such as LIKE, GT, LT... See examples
	Op map[string]interface{}
)

func (me Ex) Expression() Expression {
	return me
}

func (me Ex) Clone() Expression {
	ret := Ex{}
	for key, val := range me {
		ret[key] = val
	}
	return ret
}

func (me Ex) ToExpressions() (ExpressionList, error) {
	return mapToExpressionList(me, AND_TYPE)
}

func (me ExOr) Expression() Expression {
	return me
}

func (me ExOr) Clone() Expression {
	ret := Ex{}
	for key, val := range me {
		ret[key] = val
	}
	return ret
}

func (me ExOr) ToExpressions() (ExpressionList, error) {
	return mapToExpressionList(me, OR_TYPE)
}

type (
	//A list of columns. Typically used internally by Select, Order, From
	ColumnList interface {
		Expression
		//Returns the list of columns
		Columns() []Expression
		//Returns a new ColumnList with the columns appended.
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
			_, valKind, _ := getTypeInfo(val, reflect.Indirect(reflect.ValueOf(val)))

			if valKind == reflect.Struct {
				cm, err := getColumnMap(val)
				if err != nil {
					panic(err.Error())
				}
				var structCols []string
				for key, col := range cm {
					if !col.Transient {
						structCols = append(structCols, key)
					}
				}
				sort.Strings(structCols)
				for _, col := range structCols {
					cols = append(cols, I(col))
				}
			} else {
				panic(fmt.Sprintf("Cannot created expression from  %+v", val))
			}

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
	LockStrength int
	WaitOption   int
	Lock         struct {
		Strength   LockStrength
		WaitOption WaitOption
	}
)

const (
	FOR_NOLOCK LockStrength = iota
	FOR_UPDATE
	FOR_NO_KEY_UPDATE
	FOR_SHARE
	FOR_KEY_SHARE

	WAIT WaitOption = iota
	NOWAIT
	SKIP_LOCKED
)

type (
	JoinType      int
	JoinCondition int
	//Parent type for join expressions
	joinExpression interface {
		Expression
		JoinCondition() JoinCondition
	}
	//Container for all joins within a dataset
	JoiningClause struct {
		//The JoinType
		JoinType JoinType
		//If this is a conditioned join (e.g. NATURAL, or INNER)
		IsConditioned bool
		//The table expressions (e.g. LEFT JOIN "my_table", ON (....))
		Table Expression
		//The condition to join (e.g. USING("a", "b"), ON("my_table"."fkey" = "other_table"."id")
		Condition joinExpression
	}
	JoiningClauses []JoiningClause
	joinClause     struct {
		joinCondition JoinCondition
	}
)

const (
	INNER_JOIN JoinType = iota
	FULL_OUTER_JOIN
	RIGHT_OUTER_JOIN
	LEFT_OUTER_JOIN
	FULL_JOIN
	RIGHT_JOIN
	LEFT_JOIN
	NATURAL_JOIN
	NATURAL_LEFT_JOIN
	NATURAL_RIGHT_JOIN
	NATURAL_FULL_JOIN
	CROSS_JOIN

	USING_COND JoinCondition = iota
	ON_COND
)

func (me JoiningClause) Clone() JoiningClause {
	return JoiningClause{JoinType: me.JoinType, IsConditioned: me.IsConditioned, Table: me.Table.Clone(), Condition: me.Condition.Clone().(joinExpression)}
}

func (me JoiningClauses) Clone() JoiningClauses {
	ret := make(JoiningClauses, len(me))
	for i, jc := range me {
		ret[i] = jc.Clone()
	}
	return ret
}

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
		joinExpression
		On() ExpressionList
	}
	joinOnClause struct {
		joinClause
		on ExpressionList
	}
)

//Creates a new ON clause to be used within a join
//    ds.Join(I("my_table"), On(I("my_table.fkey").Eq(I("other_table.id")))
func On(expressions ...Expression) joinExpression {
	return joinOnClause{joinClause{ON_COND}, And(expressions...)}
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
		joinExpression
		Using() ColumnList
	}
	//A join expression that uses an USING clause
	joinUsingClause struct {
		joinClause
		using ColumnList
	}
)

//Creates a new USING clause to be used within a join
func Using(expressions ...interface{}) joinExpression {
	return joinUsingClause{joinClause{USING_COND}, cols(expressions...)}
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
	//Interface that an expression should implement if it can be aliased.
	AliasMethods interface {
		//Returns an AliasedExpression
		//    I("col").As("other_col") //"col" AS "other_col"
		//    I("col").As(I("other_col")) //"col" AS "other_col"
		As(interface{}) AliasedExpression
	}
	//Interface that an expression should implement if it can be compared with other values.
	ComparisonMethods interface {
		//Creates a Boolean expression comparing equality
		//    I("col").Eq(1) //("col" = 1)
		Eq(interface{}) BooleanExpression
		//Creates a Boolean expression comparing in-equality
		//    I("col").Neq(1) //("col" != 1)
		Neq(interface{}) BooleanExpression
		//Creates a Boolean expression for greater than comparisons
		//    I("col").Gt(1) //("col" > 1)
		Gt(interface{}) BooleanExpression
		//Creates a Boolean expression for greater than or equal to than comparisons
		//    I("col").Gte(1) //("col" >= 1)
		Gte(interface{}) BooleanExpression
		//Creates a Boolean expression for less than comparisons
		//    I("col").Lt(1) //("col" < 1)
		Lt(interface{}) BooleanExpression
		//Creates a Boolean expression for less than or equal to comparisons
		//    I("col").Lte(1) //("col" <= 1)
		Lte(interface{}) BooleanExpression
	}
	RangeMethods interface {
		//Creates a Range expression for between comparisons
		//    I("col").Between(RangeVal{Start:1, End:10}) //("col" BETWEEN 1 AND 10)
		Between(RangeVal) RangeExpression
		//Creates a Range expression for between comparisons
		//    I("col").NotBetween(RangeVal{Start:1, End:10}) //("col" NOT BETWEEN 1 AND 10)
		NotBetween(RangeVal) RangeExpression
	}
	//Interface that an expression should implement if it can be used in an IN expression
	InMethods interface {
		//Creates a Boolean expression for IN clauses
		//    I("col").In([]string{"a", "b", "c"}) //("col" IN ('a', 'b', 'c'))
		In(...interface{}) BooleanExpression
		//Creates a Boolean expression for NOT IN clauses
		//    I("col").NotIn([]string{"a", "b", "c"}) //("col" NOT IN ('a', 'b', 'c'))
		NotIn(...interface{}) BooleanExpression
	}
	//Interface that an expression should implement if it can be ORDERED.
	OrderedMethods interface {
		//Creates an Ordered Expression for sql ASC order
		//   ds.Order(I("a").Asc()) //ORDER BY "a" ASC
		Asc() OrderedExpression
		//Creates an Ordered Expression for sql DESC order
		//   ds.Order(I("a").Desc()) //ORDER BY "a" DESC
		Desc() OrderedExpression
	}
	//Interface that an expression should implement if it can be used in string operations (e.g. LIKE, NOT LIKE...).
	StringMethods interface {
		//Creates an Boolean expression for LIKE clauses
		//   ds.Where(I("a").Like("a%")) //("a" LIKE 'a%')
		Like(interface{}) BooleanExpression
		//Creates an Boolean expression for NOT LIKE clauses
		//   ds.Where(I("a").NotLike("a%")) //("a" NOT LIKE 'a%')
		NotLike(interface{}) BooleanExpression
		//Creates an Boolean expression for case insensitive LIKE clauses
		//   ds.Where(I("a").ILike("a%")) //("a" ILIKE 'a%')
		ILike(interface{}) BooleanExpression
		//Creates an Boolean expression for case insensitive NOT LIKE clauses
		//   ds.Where(I("a").NotILike("a%")) //("a" NOT ILIKE 'a%')
		NotILike(interface{}) BooleanExpression
	}
	//Interface that an expression should implement if it can be used in simple boolean operations (e.g IS, IS NOT).
	BooleanMethods interface {
		//Creates an Boolean expression IS clauses
		//   ds.Where(I("a").Is(nil)) //("a" IS NULL)
		//   ds.Where(I("a").Is(true)) //("a" IS TRUE)
		//   ds.Where(I("a").Is(false)) //("a" IS FALSE)
		Is(interface{}) BooleanExpression
		//Creates an Boolean expression IS NOT clauses
		//   ds.Where(I("a").IsNot(nil)) //("a" IS NOT NULL)
		//   ds.Where(I("a").IsNot(true)) //("a" IS NOT TRUE)
		//   ds.Where(I("a").IsNot(false)) //("a" IS NOT FALSE)
		IsNot(interface{}) BooleanExpression
		//Shortcut for Is(nil)
		IsNull() BooleanExpression
		//Shortcut for IsNot(nil)
		IsNotNull() BooleanExpression
		//Shortcut for Is(true)
		IsTrue() BooleanExpression
		//Shortcut for IsNot(true)
		IsNotTrue() BooleanExpression
		//Shortcut for Is(false)
		IsFalse() BooleanExpression
		//Shortcut for IsNot(false)
		IsNotFalse() BooleanExpression
	}
	//Interface that an expression should implement if it can be casted to another SQL type .
	CastMethods interface {
		//Casts an expression to the specified type
		//   I("a").Cast("numeric")//CAST("a" AS numeric)
		Cast(val string) CastExpression
	}
	updateMethods interface {
		//Used internally by update sql
		Set(interface{}) UpdateExpression
	}
	//Interface that an expression should implement if it can be used in a DISTINCT epxression.
	DistinctMethods interface {
		//Creates a DISTINCT clause
		//   I("a").Distinct() //DISTINCT("a")
		Distinct() SqlFunctionExpression
	}
)

type (
	//An Identifier that can contain schema, table and column identifiers
	IdentifierExpression interface {
		Expression
		AliasMethods
		ComparisonMethods
		RangeMethods
		InMethods
		StringMethods
		BooleanMethods
		OrderedMethods
		updateMethods
		DistinctMethods
		CastMethods
		//Returns a new IdentifierExpression with the specified schema
		Schema(string) IdentifierExpression
		//Returns the current schema
		GetSchema() string
		//Returns a new IdentifierExpression with the specified table
		Table(string) IdentifierExpression
		//Returns the current table
		GetTable() string
		//Returns a new IdentifierExpression with the specified column
		Col(interface{}) IdentifierExpression
		//Returns the current column
		GetCol() interface{}
		//Returns a new IdentifierExpression with the column set to *
		//   I("my_table").All() //"my_table".*
		All() IdentifierExpression
	}
	identifier struct {
		schema string
		table  string
		col    interface{}
	}
)

//Creates a new Identifier, the generated sql will use adapter specific quoting or '"' by default, this ensures case sensitivity and in certain databases allows for special characters, (e.g. "curr-table", "my table").
//An Identifier can represent a one or a combination of schema, table, and/or column.
//    I("column") -> "column" //A Column
//    I("table.column") -> "table"."column" //A Column and table
//    I("schema.table.column") //Schema table and column
//    I("table.*") //Also handles the * operator
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

//Sets the table on the current identifier
//  I("col").Table("table") -> "table"."col" //postgres
//  I("col").Table("table") -> `table`.`col` //mysql
//  I("col").Table("table") -> `table`.`col` //sqlite3
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

//Sets the table on the current identifier
//  I("table").Schema("schema") -> "schema"."table" //postgres
//  I("col").Schema("table") -> `schema`.`table` //mysql
//  I("col").Schema("table") -> `schema`.`table` //sqlite3
func (me identifier) Schema(schema string) IdentifierExpression {
	ret := me.clone()
	ret.schema = schema
	return ret
}

func (me identifier) GetSchema() string {
	return me.schema
}

//Sets the table on the current identifier
//  I("table").Col("col") -> "table"."col" //postgres
//  I("table").Schema("col") -> `table`.`col` //mysql
//  I("table").Schema("col") -> `table`.`col` //sqlite3
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
func (me identifier) As(val interface{}) AliasedExpression { return aliased(me, val) }

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

//Returns a RangeExpression for checking that a identifier is between two values (e.g "my_col" BETWEEN 1 AND 10)
func (me identifier) Between(val RangeVal) RangeExpression { return between(me, val) }

//Returns a RangeExpression for checking that a identifier is between two values (e.g "my_col" BETWEEN 1 AND 10)
func (me identifier) NotBetween(val RangeVal) RangeExpression { return notBetween(me, val) }

type (
	//Expression for representing "literal" sql.
	//  L("col = 1") -> col = 1)
	//  L("? = ?", I("col"), 1) -> "col" = 1
	LiteralExpression interface {
		Expression
		AliasMethods
		ComparisonMethods
		RangeMethods
		OrderedMethods
		//Returns the literal sql
		Literal() string
		//Arguments to be replaced within the sql
		Args() []interface{}
	}
	literal struct {
		literal string
		args    []interface{}
	}
)

//Alias for L
func Literal(val string, args ...interface{}) LiteralExpression {
	return L(val, args...)
}

//Creates a new SQL literal with the provided arguments.
//   L("a = 1") -> a = 1
//You can also you placeholders. All placeholders within a Literal are represented by '?'
//   L("a = ?", "b") -> a = 'b'
//Literals can also contain placeholders for other expressions
//   L("(? AND ?) OR (?)", I("a").Eq(1), I("b").Eq("b"), I("c").In([]string{"a", "b", "c"}))

func L(val string, args ...interface{}) LiteralExpression {
	return literal{literal: val, args: args}
}

//Returns a literal for DEFAULT sql keyword
func Default() LiteralExpression {
	return literal{literal: "DEFAULT"}
}

//Returns a literal for the '*' operator
func Star() LiteralExpression {
	return literal{literal: "*"}
}

func (me literal) Clone() Expression {
	return Literal(me.literal)
}

func (me literal) Literal() string {
	return me.literal
}

func (me literal) Args() []interface{} {
	return me.args
}

func (me literal) Expression() Expression                  { return me }
func (me literal) As(val interface{}) AliasedExpression    { return aliased(me, val) }
func (me literal) Eq(val interface{}) BooleanExpression    { return eq(me, val) }
func (me literal) Neq(val interface{}) BooleanExpression   { return neq(me, val) }
func (me literal) Gt(val interface{}) BooleanExpression    { return gt(me, val) }
func (me literal) Gte(val interface{}) BooleanExpression   { return gte(me, val) }
func (me literal) Lt(val interface{}) BooleanExpression    { return lt(me, val) }
func (me literal) Lte(val interface{}) BooleanExpression   { return lte(me, val) }
func (me literal) Asc() OrderedExpression                  { return asc(me) }
func (me literal) Desc() OrderedExpression                 { return desc(me) }
func (me literal) Between(val RangeVal) RangeExpression    { return between(me, val) }
func (me literal) NotBetween(val RangeVal) RangeExpression { return notBetween(me, val) }

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
		//Returns the operator for the expression
		Op() BooleanOperation
		//The left hand side of the expression (e.g. I("a")
		Lhs() Expression
		//The right hand side of the expression could be a primitive value, dataset, or expression
		Rhs() interface{}
	}
	boolean struct {
		lhs Expression
		rhs interface{}
		op  BooleanOperation
	}
)

const (
	//=
	EQ_OP BooleanOperation = iota
	//!= or <>
	NEQ_OP
	//IS
	IS_OP
	//IS NOT
	IS_NOT_OP
	//>
	GT_OP
	//>=
	GTE_OP
	//<
	LT_OP
	//<=
	LTE_OP
	//IN
	IN_OP
	//NOT IN
	NOT_IN_OP
	//LIKE, LIKE BINARY...
	LIKE_OP
	//NOT LIKE, NOT LIKE BINARY...
	NOT_LIKE_OP
	//ILIKE, LIKE
	I_LIKE_OP
	//NOT ILIKE, NOT LIKE
	NOT_I_LIKE_OP
	//~, REGEXP BINARY
	REGEXP_LIKE_OP
	//!~, NOT REGEXP BINARY
	REGEXP_NOT_LIKE_OP
	//~*, REGEXP
	REGEXP_I_LIKE_OP
	//!~*, NOT REGEXP
	REGEXP_NOT_I_LIKE_OP
)

//used internally for inverting operators
var operator_inversions = map[BooleanOperation]BooleanOperation{
	IS_OP:                IS_NOT_OP,
	EQ_OP:                NEQ_OP,
	GT_OP:                LTE_OP,
	GTE_OP:               LT_OP,
	LT_OP:                GTE_OP,
	LTE_OP:               GT_OP,
	IN_OP:                NOT_IN_OP,
	LIKE_OP:              NOT_LIKE_OP,
	I_LIKE_OP:            NOT_I_LIKE_OP,
	REGEXP_LIKE_OP:       REGEXP_NOT_LIKE_OP,
	REGEXP_I_LIKE_OP:     REGEXP_NOT_I_LIKE_OP,
	IS_NOT_OP:            IS_OP,
	NEQ_OP:               EQ_OP,
	NOT_IN_OP:            IN_OP,
	NOT_LIKE_OP:          LIKE_OP,
	NOT_I_LIKE_OP:        I_LIKE_OP,
	REGEXP_NOT_LIKE_OP:   REGEXP_LIKE_OP,
	REGEXP_NOT_I_LIKE_OP: REGEXP_I_LIKE_OP,
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

//used internally to create an equality BooleanExpression
func eq(lhs Expression, rhs interface{}) BooleanExpression {
	return checkBoolExpType(EQ_OP, lhs, rhs, false)
}

//used internally to create an in-equality BooleanExpression
func neq(lhs Expression, rhs interface{}) BooleanExpression {
	return checkBoolExpType(EQ_OP, lhs, rhs, true)
}

//used internally to create an gt comparison BooleanExpression
func gt(lhs Expression, rhs interface{}) BooleanExpression {
	return boolean{op: GT_OP, lhs: lhs, rhs: rhs}
}

//used internally to create an gte comparison BooleanExpression
func gte(lhs Expression, rhs interface{}) BooleanExpression {
	return boolean{op: GTE_OP, lhs: lhs, rhs: rhs}
}

//used internally to create an lt comparison BooleanExpression
func lt(lhs Expression, rhs interface{}) BooleanExpression {
	return boolean{op: LT_OP, lhs: lhs, rhs: rhs}
}

//used internally to create an lte comparison BooleanExpression
func lte(lhs Expression, rhs interface{}) BooleanExpression {
	return boolean{op: LTE_OP, lhs: lhs, rhs: rhs}
}

//used internally to create an IN BooleanExpression
func in(lhs Expression, vals ...interface{}) BooleanExpression {
	if len(vals) == 1 && reflect.Indirect(reflect.ValueOf(vals[0])).Kind() == reflect.Slice {
		return boolean{op: IN_OP, lhs: lhs, rhs: vals[0]}
	}
	return boolean{op: IN_OP, lhs: lhs, rhs: vals}
}

//used internally to create a NOT IN BooleanExpression
func notIn(lhs Expression, vals ...interface{}) BooleanExpression {
	if len(vals) == 1 && reflect.Indirect(reflect.ValueOf(vals[0])).Kind() == reflect.Slice {
		return boolean{op: NOT_IN_OP, lhs: lhs, rhs: vals[0]}
	}
	return boolean{op: NOT_IN_OP, lhs: lhs, rhs: vals}
}

//used internally to create an IS BooleanExpression
func is(lhs Expression, val interface{}) BooleanExpression {
	return checkBoolExpType(IS_OP, lhs, val, false)
}

//used internally to create an IS NOT BooleanExpression
func isNot(lhs Expression, val interface{}) BooleanExpression {
	return checkBoolExpType(IS_OP, lhs, val, true)
}

//used internally to create a LIKE BooleanExpression
func like(lhs Expression, val interface{}) BooleanExpression {
	return checkLikeExp(LIKE_OP, lhs, val, false)
}

//used internally to create an ILIKE BooleanExpression
func iLike(lhs Expression, val interface{}) BooleanExpression {
	return checkLikeExp(I_LIKE_OP, lhs, val, false)
}

//used internally to create a NOT LIKE BooleanExpression
func notLike(lhs Expression, val interface{}) BooleanExpression {
	return checkLikeExp(LIKE_OP, lhs, val, true)
}

//used internally to create a NOT ILIKE BooleanExpression
func notILike(lhs Expression, val interface{}) BooleanExpression {
	return checkLikeExp(I_LIKE_OP, lhs, val, true)
}

//checks an like rhs to create the proper like expression for strings or regexps
func checkLikeExp(op BooleanOperation, lhs Expression, val interface{}, invert bool) BooleanExpression {
	rhs := val
	switch val.(type) {
	case *regexp.Regexp:
		if op == LIKE_OP {
			op = REGEXP_LIKE_OP
		} else if op == I_LIKE_OP {
			op = REGEXP_I_LIKE_OP
		}
		rhs = val.(*regexp.Regexp).String()
	}
	if invert {
		op = operator_inversions[op]
	}
	return boolean{op: op, lhs: lhs, rhs: rhs}
}

//checks a boolean operation normalizing the operation based on the RHS (e.g. "a" = true vs "a" IS TRUE
func checkBoolExpType(op BooleanOperation, lhs Expression, rhs interface{}, invert bool) BooleanExpression {
	if rhs == nil {
		op = IS_OP
	} else {
		switch reflect.Indirect(reflect.ValueOf(rhs)).Kind() {
		case reflect.Bool:
			op = IS_OP
		case reflect.Slice:
			//if its a slice of bytes dont treat as an IN
			if _, ok := rhs.([]byte); !ok {
				op = IN_OP
			}
		case reflect.Struct:
			switch rhs.(type) {
			case SqlExpression:
				op = IN_OP
			case *regexp.Regexp:
				return checkLikeExp(LIKE_OP, lhs, rhs, invert)
			}
		}
	}
	if invert {
		op = operator_inversions[op]
	}
	return boolean{op: op, lhs: lhs, rhs: rhs}
}

type (
	RangeOperation  int
	RangeExpression interface {
		Expression
		//Returns the operator for the expression
		Op() RangeOperation
		//The left hand side of the expression (e.g. I("a")
		Lhs() Expression
		//The right hand side of the expression could be a primitive value, dataset, or expression
		Rhs() RangeVal
	}
	ranged struct {
		lhs Expression
		rhs RangeVal
		op  RangeOperation
	}
	RangeVal struct {
		Start interface{}
		End   interface{}
	}
)

const (
	//BETWEEN
	BETWEEN_OP RangeOperation = iota
	//NOT BETWEEN
	NBETWEEN_OP
)

func (me ranged) Clone() Expression {
	return ranged{op: me.op, lhs: me.lhs.Clone(), rhs: me.rhs}
}

func (me ranged) Expression() Expression {
	return me
}

func (me ranged) Rhs() RangeVal {
	return me.rhs
}

func (me ranged) Lhs() Expression {
	return me.lhs
}

func (me ranged) Op() RangeOperation {
	return me.op
}

//used internally to create an BETWEEN comparison RangeExpression
func between(lhs Expression, rhs RangeVal) RangeExpression {
	return ranged{op: BETWEEN_OP, lhs: lhs, rhs: rhs}
}

//used internally to create an NOT BETWEEN comparison RangeExpression
func notBetween(lhs Expression, rhs RangeVal) RangeExpression {
	return ranged{op: NBETWEEN_OP, lhs: lhs, rhs: rhs}
}

type (
	//Expression for Aliased expressions
	//   I("a").As("b") -> "a" AS "b"
	//   SUM("a").As(I("a_sum")) -> SUM("a") AS "a_sum"
	AliasedExpression interface {
		Expression
		//Returns the Epxression being aliased
		Aliased() Expression
		//Returns the alias value as an identiier expression
		GetAs() IdentifierExpression
	}
	aliasExpression struct {
		aliased Expression
		alias   IdentifierExpression
	}
)

//used internally by other expressions to create a new aliased expression
func aliased(exp Expression, alias interface{}) AliasedExpression {
	switch v := alias.(type) {
	case string:
		return aliasExpression{aliased: exp, alias: I(v)}
	case IdentifierExpression:
		return aliasExpression{aliased: exp, alias: v}
	default:
		panic(fmt.Sprintf("Cannot create alias from %+v", v))
	}
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
	null_sort_type int
	sort_direction int
	//An expression for specifying sort order and options
	OrderedExpression interface {
		Expression
		//The expression being sorted
		SortExpression() Expression
		//Sort direction (e.g. ASC, DESC)
		Direction() sort_direction
		//If the adapter supports it null sort type (e.g. NULLS FIRST, NULLS LAST)
		NullSortType() null_sort_type
		//Returns a new OrderedExpression with NullSortType set to NULLS_FIRST
		NullsFirst() OrderedExpression
		//Returns a new OrderedExpression with NullSortType set to NULLS_LAST
		NullsLast() OrderedExpression
	}
	orderedExpression struct {
		sortExpression Expression
		direction      sort_direction
		nullSortType   null_sort_type
	}
)

const (
	//Default null sort type with no null sort order
	NO_NULLS null_sort_type = iota
	//NULLS FIRST
	NULLS_FIRST
	//NULLS LAST
	NULLS_LAST

	//ASC
	SORT_ASC sort_direction = iota
	//DESC
	SORT_DESC
)

//used internally to create a new SORT_ASC OrderedExpression
func asc(exp Expression) OrderedExpression {
	return orderedExpression{sortExpression: exp, direction: SORT_ASC, nullSortType: NO_NULLS}
}

//used internally to create a new SORT_DESC OrderedExpression
func desc(exp Expression) OrderedExpression {
	return orderedExpression{sortExpression: exp, direction: SORT_DESC, nullSortType: NO_NULLS}
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
	return orderedExpression{sortExpression: me.sortExpression, direction: me.direction, nullSortType: NULLS_FIRST}
}

func (me orderedExpression) NullsLast() OrderedExpression {
	return orderedExpression{sortExpression: me.sortExpression, direction: me.direction, nullSortType: NULLS_LAST}
}

type (
	//Expression for representing a SqlFunction(e.g. COUNT, SUM, MIN, MAX...)
	SqlFunctionExpression interface {
		Expression
		AliasMethods
		RangeMethods
		ComparisonMethods
		//The function name
		Name() string
		//Arguments to be passed to the function
		Args() []interface{}
	}
	sqlFunctionExpression struct {
		name string
		args []interface{}
	}
)

//Creates a new SqlFunctionExpression with the given name and arguments
func Func(name string, args ...interface{}) SqlFunctionExpression {
	return sqlFunctionExpression{name: name, args: args}
}

//used internally to normalize the column name if passed in as a string it should be turned into an identifier
func colFunc(name string, col interface{}) SqlFunctionExpression {
	if s, ok := col.(string); ok {
		col = I(s)
	}
	return Func(name, col)
}

//Creates a new DISTINCT sql function
//   DISTINCT("a") -> DISTINCT("a")
//   DISTINCT(I("a")) -> DISTINCT("a")
func DISTINCT(col interface{}) SqlFunctionExpression { return colFunc("DISTINCT", col) }

//Creates a new COUNT sql function
//   COUNT("a") -> COUNT("a")
//   COUNT("*") -> COUNT("*")
//   COUNT(I("a")) -> COUNT("a")
func COUNT(col interface{}) SqlFunctionExpression { return colFunc("COUNT", col) }

//Creates a new MIN sql function
//   MIN("a") -> MIN("a")
//   MIN(I("a")) -> MIN("a")
func MIN(col interface{}) SqlFunctionExpression { return colFunc("MIN", col) }

//Creates a new MAX sql function
//   MAX("a") -> MAX("a")
//   MAX(I("a")) -> MAX("a")
func MAX(col interface{}) SqlFunctionExpression { return colFunc("MAX", col) }

//Creates a new AVG sql function
//   AVG("a") -> AVG("a")
//   AVG(I("a")) -> AVG("a")
func AVG(col interface{}) SqlFunctionExpression { return colFunc("AVG", col) }

//Creates a new FIRST sql function
//   FIRST("a") -> FIRST("a")
//   FIRST(I("a")) -> FIRST("a")
func FIRST(col interface{}) SqlFunctionExpression { return colFunc("FIRST", col) }

//Creates a new LAST sql function
//   LAST("a") -> LAST("a")
//   LAST(I("a")) -> LAST("a")
func LAST(col interface{}) SqlFunctionExpression { return colFunc("LAST", col) }

//Creates a new SUM sql function
//   SUM("a") -> SUM("a")
//   SUM(I("a")) -> SUM("a")
func SUM(col interface{}) SqlFunctionExpression { return colFunc("SUM", col) }

//Creates a new COALESCE sql function
//   COALESCE(I("a"), "a") -> COALESCE("a", 'a')
//   COALESCE(I("a"), I("b"), nil) -> COALESCE("a", "b", NULL)
func COALESCE(vals ...interface{}) SqlFunctionExpression {
	return Func("COALESCE", vals...)
}

func (me sqlFunctionExpression) Clone() Expression {
	return sqlFunctionExpression{name: me.name, args: me.args}
}

func (me sqlFunctionExpression) Expression() Expression                  { return me }
func (me sqlFunctionExpression) Args() []interface{}                     { return me.args }
func (me sqlFunctionExpression) Name() string                            { return me.name }
func (me sqlFunctionExpression) As(val interface{}) AliasedExpression    { return aliased(me, val) }
func (me sqlFunctionExpression) Eq(val interface{}) BooleanExpression    { return eq(me, val) }
func (me sqlFunctionExpression) Neq(val interface{}) BooleanExpression   { return neq(me, val) }
func (me sqlFunctionExpression) Gt(val interface{}) BooleanExpression    { return gt(me, val) }
func (me sqlFunctionExpression) Gte(val interface{}) BooleanExpression   { return gte(me, val) }
func (me sqlFunctionExpression) Lt(val interface{}) BooleanExpression    { return lt(me, val) }
func (me sqlFunctionExpression) Lte(val interface{}) BooleanExpression   { return lte(me, val) }
func (me sqlFunctionExpression) Between(val RangeVal) RangeExpression    { return between(me, val) }
func (me sqlFunctionExpression) NotBetween(val RangeVal) RangeExpression { return notBetween(me, val) }

type (
	//An Expression that represents another Expression casted to a SQL type
	CastExpression interface {
		Expression
		AliasMethods
		ComparisonMethods
		InMethods
		StringMethods
		BooleanMethods
		OrderedMethods
		DistinctMethods
		RangeMethods
		//The exression being casted
		Casted() Expression
		//The the SQL type to cast the expression to
		Type() LiteralExpression
	}
	cast struct {
		casted Expression
		t      LiteralExpression
	}
)

//Creates a new Casted expression
//  Cast(I("a"), "NUMERIC") -> CAST("a" AS NUMERIC)
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
func (me cast) As(val interface{}) AliasedExpression     { return aliased(me, val) }
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
func (me cast) Between(val RangeVal) RangeExpression     { return between(me, val) }
func (me cast) NotBetween(val RangeVal) RangeExpression  { return notBetween(me, val) }

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
	UNION compoundType = iota
	UNION_ALL
	INTERSECT
	INTERSECT_ALL
)

//Creates a new UNION compound expression between SqlExpression, typically Datasets'. This function is used internally by Dataset when compounded with another Dataset
func Union(rhs SqlExpression) CompoundExpression {
	return compound{t: UNION, rhs: rhs}
}

//Creates a new UNION ALL compound expression between SqlExpression, typically Datasets'. This function is used internally by Dataset when compounded with another Dataset
func UnionAll(rhs SqlExpression) CompoundExpression {
	return compound{t: UNION_ALL, rhs: rhs}
}

//Creates a new INTERSECT compound expression between SqlExpression, typically Datasets'. This function is used internally by Dataset when compounded with another Dataset
func Intersect(rhs SqlExpression) CompoundExpression {
	return compound{t: INTERSECT, rhs: rhs}
}

//Creates a new INTERSECT ALL compound expression between SqlExpression, typically Datasets'. This function is used internally by Dataset when compounded with another Dataset
func IntersectAll(rhs SqlExpression) CompoundExpression {
	return compound{t: INTERSECT_ALL, rhs: rhs}
}

func (me compound) Expression() Expression { return me }

func (me compound) Clone() Expression {
	return compound{t: me.t, rhs: me.rhs.Clone().(SqlExpression)}
}

func (me compound) Type() compoundType { return me.t }
func (me compound) Rhs() SqlExpression { return me.rhs }

type (
	CommonTableExpression interface {
		Expression
		IsRecursive() bool
		//Returns the alias name for the extracted expression
		Name() LiteralExpression
		//Returns the Expression being extracted
		SubQuery() SqlExpression
	}
	commonExpr struct {
		recursive bool
		name      LiteralExpression
		subQuery  SqlExpression
	}
)

//Creates a new WITH common table expression for a SqlExpression, typically Datasets'. This function is used internally by Dataset when a CTE is added to another Dataset
func With(recursive bool, name string, subQuery SqlExpression) CommonTableExpression {
	return commonExpr{recursive: recursive, name: Literal(name), subQuery: subQuery}
}

func (me commonExpr) Expression() Expression { return me }

func (me commonExpr) Clone() Expression {
	return commonExpr{recursive: me.recursive, name: me.name, subQuery: me.subQuery.Clone().(SqlExpression)}
}

func (me commonExpr) IsRecursive() bool       { return me.recursive }
func (me commonExpr) Name() LiteralExpression { return me.name }
func (me commonExpr) SubQuery() SqlExpression { return me.subQuery }

type (
	//An Expression that the ON CONFLICT/ON DUPLICATE KEY portion of an INSERT statement
	ConflictExpression interface {
		Updates() *ConflictUpdate
	}
	Conflict struct{}
	//ConflictUpdate is the struct that represents the UPDATE fragment of an INSERT ... ON CONFLICT/ON DUPLICATE KEY DO UPDATE statement
	ConflictUpdate struct {
		Target      string
		Update      interface{}
		WhereClause ExpressionList
	}
)

//Updates returns the struct that represents the UPDATE fragment of an INSERT ... ON CONFLICT/ON DUPLICATE KEY DO UPDATE statement
//If nil, no update is preformed.
func (c Conflict) Updates() *ConflictUpdate {
	return nil
}

//Returns the target conflict column. Only necessary for Postgres.
//Will return an error for mysql/sqlite. Will also return an error if missing from a postgres ConflictUpdate.
func (c ConflictUpdate) TargetColumn() string {
	return c.Target
}

//Returns the Updates which represent the ON CONFLICT DO UPDATE portion of an insert statement. If nil, there are no updates.
func (c ConflictUpdate) Updates() *ConflictUpdate {
	return &c
}

//Append to the existing Where clause for an ON CONFLICT DO UPDATE ... WHERE ...
//  InsertConflict(DoNothing(),...) -> INSERT INTO ... ON CONFLICT DO NOTHING
func (c *ConflictUpdate) Where(expressions ...Expression) *ConflictUpdate {
	if c.WhereClause == nil {
		c.WhereClause = And(expressions...)
	} else {
		c.WhereClause = c.WhereClause.Append(expressions...)
	}
	return c
}

//Creates a Conflict struct to be passed to InsertConflict to ignore constraint errors
//  InsertConflict(DoNothing(),...) -> INSERT INTO ... ON CONFLICT DO NOTHING
func DoNothing() *Conflict {
	return &Conflict{}
}

//Creates a ConflictUpdate struct to be passed to InsertConflict
//Represents a ON CONFLICT DO UPDATE portion of an INSERT statement (ON DUPLICATE KEY UPDATE for mysql)
//  InsertConflict(DoUpdate("target_column", update),...) -> INSERT INTO ... ON CONFLICT DO UPDATE SET a=b
//  InsertConflict(DoUpdate("target_column", update).Where(Ex{"a": 1},...) -> INSERT INTO ... ON CONFLICT DO UPDATE SET a=b WHERE a=1
func DoUpdate(target string, update interface{}) *ConflictUpdate {
	return &ConflictUpdate{Target: target, Update: update}
}
