package sqlgen

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/doug-martin/goqu/v9/exp"
	"github.com/doug-martin/goqu/v9/internal/errors"
	"github.com/doug-martin/goqu/v9/internal/sb"
	"github.com/doug-martin/goqu/v9/internal/util"
)

type (
	// An adapter interface to be used by a Dataset to generate SQL for a specific dialect.
	// See DefaultAdapter for a concrete implementation and examples.
	ExpressionSQLGenerator interface {
		Dialect() string
		Generate(b sb.SQLBuilder, val interface{})
	}
	// The default adapter. This class should be used when building a new adapter. When creating a new adapter you can
	// either override methods, or more typically update default values.
	// See (github.com/doug-martin/goqu/dialect/postgres)
	expressionSQLGenerator struct {
		dialect        string
		dialectOptions *SQLDialectOptions
	}
)

// replacementByte is replacementRune as a byte so placeholders can be found bytewise in valid UTF-8 text.
var (
	defaultBinaryLiteralPrefix = []byte("X'")
	defaultBinaryLiteralSuffix = []byte("'")
)

const (
	replacementByte byte = '?'
	// quoteOverhead is the room reserved for the surrounding quotes of a string/bytes literal.
	quoteOverhead = 2
	// identifierOverhead is the room reserved for quotes and separators of a qualified identifier.
	identifierOverhead = 32
)

var (
	replacementRune = '?'
	TrueLiteral     = exp.NewLiteralExpression("TRUE")
	FalseLiteral    = exp.NewLiteralExpression("FALSE")

	ErrEmptyIdentifier = errors.New(
		`a empty identifier was encountered, please specify a "schema", "table" or "column"`,
	)
	ErrUnexpectedNamedWindow = errors.New(`unexpected named window function`)
	ErrEmptyCaseWhens        = errors.New(`when conditions not found for case statement`)
)

func errUnsupportedExpressionType(e exp.Expression) error {
	return errors.New("unsupported expression type %T", e)
}

func errUnsupportedIdentifierExpression(t interface{}) error {
	return errors.New("unexpected col type must be string or LiteralExpression received %T", t)
}

func errUnsupportedBooleanExpressionOperator(op exp.BooleanOperation) error {
	return errors.New("boolean operator '%+v' not supported", op)
}

func errUnsupportedBitwiseExpressionOperator(op exp.BitwiseOperation) error {
	return errors.New("bitwise operator '%+v' not supported", op)
}

func errUnsupportedRangeExpressionOperator(op exp.RangeOperation) error {
	return errors.New("range operator %+v not supported", op)
}

func errLateralNotSupported(dialect string) error {
	return errors.New("dialect does not support lateral expressions [dialect=%s]", dialect)
}

func NewExpressionSQLGenerator(dialect string, do *SQLDialectOptions) ExpressionSQLGenerator {
	return &expressionSQLGenerator{dialect: dialect, dialectOptions: do}
}

func (esg *expressionSQLGenerator) Dialect() string {
	return esg.dialect
}

var valuerReflectType = reflect.TypeOf((*driver.Valuer)(nil)).Elem()

func (esg *expressionSQLGenerator) Generate(b sb.SQLBuilder, val interface{}) {
	if b.Error() != nil {
		return
	}
	if val == nil {
		esg.literalNil(b)
		return
	}
	switch v := val.(type) {
	case exp.Expression:
		esg.expressionSQL(b, v)
	case driver.Valuer:
		esg.valuerSQL(b, v)
	case int:
		esg.literalInt(b, int64(v))
	case int32:
		esg.literalInt(b, int64(v))
	case int64:
		esg.literalInt(b, v)
	case uint64:
		// uint and uint32 (and named unsigned types) reach here through reflectSQL.
		esg.literalUint(b, v)
	case float32:
		esg.literalFloat(b, float64(v))
	case float64:
		esg.literalFloat(b, v)
	case string:
		esg.literalString(b, v)
	case bool:
		esg.literalBool(b, v)
	case time.Time:
		esg.literalTime(b, v)
	case *time.Time:
		if v == nil {
			esg.literalNil(b)
			return
		}
		esg.literalTime(b, *v)
	default:
		if !esg.typedSliceSQL(b, val) {
			esg.reflectSQL(b, val)
		}
	}
}

// valuerSQL renders a driver.Valuer by generating SQL for the value it returns.
func (esg *expressionSQLGenerator) valuerSQL(b sb.SQLBuilder, v driver.Valuer) {
	// See https://github.com/golang/go/commit/0ce1d79a6a771f7449ec493b993ed2a720917870
	if rv := reflect.ValueOf(v); rv.Kind() == reflect.Ptr &&
		rv.IsNil() &&
		rv.Type().Elem().Implements(valuerReflectType) {
		esg.literalNil(b)
		return
	}
	dVal, err := v.Value()
	if err != nil {
		b.SetError(err)
		return
	}
	esg.Generate(b, dVal)
}

// typedSliceSQL handles the most common slice types without reflection. It returns false if val is not one of them.
// Only exact types are matched (not named types such as pq.StringArray), exactly like the reflection path did for
// []byte and []exp.CommonTableExpression.
func (esg *expressionSQLGenerator) typedSliceSQL(b sb.SQLBuilder, val any) bool {
	switch v := val.(type) {
	case []any:
		sliceSQL(esg, b, v, sliceElemHint)
	case []int64:
		sliceSQL(esg, b, v, sliceElemHint)
	case []int:
		sliceSQL(esg, b, v, sliceElemHint)
	case []string:
		sliceSQL(esg, b, v, sliceElemHint)
	case []float64:
		sliceSQL(esg, b, v, sliceElemHint)
	case []bool:
		sliceSQL(esg, b, v, sliceElemHint)
	case []time.Time:
		sliceSQL(esg, b, v, sliceElemHint)
	case []byte:
		esg.literalBytes(b, v)
	case json.RawMessage:
		if v == nil {
			// an unset RawMessage is not valid JSON, so write NULL instead of an empty string
			esg.literalNil(b)
		} else {
			esg.literalBytes(b, v)
		}
	case []exp.CommonTableExpression:
		esg.commonTablesSliceSQL(b, v)
	default:
		return false
	}
	return true
}

// sliceElemHint is a rough number of bytes per element used to pre-size the buffer for unprepared statements
// (prepared statements only write a short placeholder per element).
const (
	sliceElemHint         = 12
	sliceElemHintPrepared = 4
)

// sliceSQL writes (e1, e2, ...) for any slice, pre-sizing the buffer and the args for large slices.
func sliceSQL[T any](esg *expressionSQLGenerator, b sb.SQLBuilder, s []T, hint int) {
	b.WriteRunes(esg.dialectOptions.LeftParenRune)
	if b.IsPrepared() {
		b.GrowArgs(len(s))
		b.GrowBuffer(len(s) * sliceElemHintPrepared)
	} else {
		b.GrowBuffer(len(s) * hint)
	}
	for i, v := range s {
		esg.Generate(b, v)
		if i < len(s)-1 {
			b.WriteRunes(esg.dialectOptions.CommaRune, esg.dialectOptions.SpaceRune)
		}
	}
	b.WriteRunes(esg.dialectOptions.RightParenRune)
}

// isBindableKind returns false for kinds that can never be sent to a database as an argument.
func isBindableKind(k reflect.Kind) bool {
	switch k {
	case reflect.Func, reflect.Chan, reflect.UnsafePointer, reflect.Complex64, reflect.Complex128:
		return false
	default:
		return true
	}
}

func (esg *expressionSQLGenerator) reflectSQL(b sb.SQLBuilder, val interface{}) {
	v := reflect.Indirect(reflect.ValueOf(val))
	valKind := v.Kind()
	switch {
	case util.IsInvalid(valKind):
		esg.literalNil(b)
	case util.IsSlice(valKind):
		// pointers to byte slices reach here (the pointer is dereferenced above)
		if v.CanInterface() && esg.typedSliceSQL(b, v.Interface()) {
			return
		}
		esg.sliceValueSQL(b, v)
	case util.IsInt(valKind):
		esg.Generate(b, v.Int())
	case util.IsUint(valKind):
		esg.Generate(b, v.Uint())
	case util.IsFloat(valKind):
		esg.Generate(b, v.Float())
	case util.IsString(valKind):
		esg.Generate(b, v.String())
	case util.IsBool(valKind):
		esg.Generate(b, v.Bool())
	default:
		// In prepared mode, pass values of other kinds (structs, maps, arrays such as UUIDs)
		// through as arguments and let the driver decide whether it can bind them.
		// Kinds that no driver can bind are still rejected here.
		if b.IsPrepared() && isBindableKind(valKind) {
			esg.placeHolderSQL(b, val)

			return
		}

		b.SetError(errors.NewEncodeError(val))
	}
}

//nolint:gocyclo // not complex just long
func (esg *expressionSQLGenerator) expressionSQL(b sb.SQLBuilder, expression exp.Expression) {
	switch e := expression.(type) {
	case exp.ColumnListExpression:
		esg.columnListSQL(b, e)
	case exp.ExpressionList:
		esg.expressionListSQL(b, e)
	case exp.LiteralExpression:
		esg.literalExpressionSQL(b, e)
	case exp.IdentifierExpression:
		esg.identifierExpressionSQL(b, e)
	case exp.LateralExpression:
		esg.lateralExpressionSQL(b, e)
	case exp.AliasedExpression:
		esg.aliasedExpressionSQL(b, e)
	case exp.BooleanExpression:
		esg.booleanExpressionSQL(b, e)
	case exp.BitwiseExpression:
		esg.bitwiseExpressionSQL(b, e)
	case exp.RangeExpression:
		esg.rangeExpressionSQL(b, e)
	case exp.OrderedExpression:
		esg.orderedExpressionSQL(b, e)
	case exp.UpdateExpression:
		esg.updateExpressionSQL(b, e)
	case exp.SQLFunctionExpression:
		esg.sqlFunctionExpressionSQL(b, e)
	case exp.SQLWindowFunctionExpression:
		esg.sqlWindowFunctionExpression(b, e)
	case exp.WindowExpression:
		esg.windowExpressionSQL(b, e)
	case exp.CastExpression:
		esg.castExpressionSQL(b, e)
	case exp.CollateExpression:
		esg.collateExpressionSQL(b, e)
	case exp.AppendableExpression:
		esg.appendableExpressionSQL(b, e)
	case exp.CommonTableExpression:
		esg.commonTableExpressionSQL(b, e)
	case exp.CompoundExpression:
		esg.compoundExpressionSQL(b, e)
	case exp.CaseExpression:
		esg.caseExpressionSQL(b, e)
	case exp.Ex:
		esg.expressionMapSQL(b, e)
	case exp.ExOr:
		esg.expressionOrMapSQL(b, e)
	default:
		b.SetError(errUnsupportedExpressionType(e))
	}
}

// Generates a placeholder (e.g. ?, $1)
func (esg *expressionSQLGenerator) placeHolderSQL(b sb.SQLBuilder, i interface{}) {
	b.Write(esg.dialectOptions.PlaceHolderFragment)
	if esg.dialectOptions.IncludePlaceholderNum {
		b.WriteInt(int64(b.CurrentArgPosition()))
	}
	b.WriteArg(i)
}

// Generates creates the sql for a sub select on a Dataset
func (esg *expressionSQLGenerator) appendableExpressionSQL(b sb.SQLBuilder, a exp.AppendableExpression) {
	b.WriteRunes(esg.dialectOptions.LeftParenRune)
	a.AppendSQL(b)
	b.WriteRunes(esg.dialectOptions.RightParenRune)
	if a.GetAs() != nil {
		b.Write(esg.dialectOptions.AsFragment)
		esg.Generate(b, a.GetAs())
	}
}

// writeEscapedIdentifier writes an identifier part, doubling any embedded quote characters so that
// e.g. a"b is written as a""b between double quotes.
func (esg *expressionSQLGenerator) writeEscapedIdentifier(b sb.SQLBuilder, s string) {
	q := esg.dialectOptions.QuoteRune
	i := strings.IndexRune(s, q)
	if i < 0 {
		b.WriteStrings(s)
		return
	}
	quote := string(q)
	b.WriteStrings(strings.ReplaceAll(s, quote, quote+quote))
}

// Quotes an identifier (e.g. "col", "table"."col"
func (esg *expressionSQLGenerator) identifierExpressionSQL(b sb.SQLBuilder, ident exp.IdentifierExpression) {
	if ident.IsEmpty() {
		b.SetError(ErrEmptyIdentifier)
		return
	}
	schema, table, col := ident.GetSchema(), ident.GetTable(), ident.GetCol()
	b.GrowBuffer(len(schema) + len(table) + identifierOverhead)
	if schema != esg.dialectOptions.EmptyString {
		b.WriteRunes(esg.dialectOptions.QuoteRune)
		esg.writeEscapedIdentifier(b, schema)
		b.WriteRunes(esg.dialectOptions.QuoteRune)
	}
	if table != esg.dialectOptions.EmptyString {
		if schema != esg.dialectOptions.EmptyString {
			b.WriteRunes(esg.dialectOptions.PeriodRune)
		}
		b.WriteRunes(esg.dialectOptions.QuoteRune)
		esg.writeEscapedIdentifier(b, table)
		b.WriteRunes(esg.dialectOptions.QuoteRune)
	}
	switch t := col.(type) {
	case nil:
	case string:
		if col != esg.dialectOptions.EmptyString {
			if table != esg.dialectOptions.EmptyString || schema != esg.dialectOptions.EmptyString {
				b.WriteRunes(esg.dialectOptions.PeriodRune)
			}
			b.WriteRunes(esg.dialectOptions.QuoteRune)
			esg.writeEscapedIdentifier(b, t)
			b.WriteRunes(esg.dialectOptions.QuoteRune)
		}
	case exp.LiteralExpression:
		if table != esg.dialectOptions.EmptyString || schema != esg.dialectOptions.EmptyString {
			b.WriteRunes(esg.dialectOptions.PeriodRune)
		}
		esg.Generate(b, t)
	default:
		b.SetError(errUnsupportedIdentifierExpression(col))
	}
}

func (esg *expressionSQLGenerator) lateralExpressionSQL(b sb.SQLBuilder, le exp.LateralExpression) {
	if !esg.dialectOptions.SupportsLateral {
		b.SetError(errLateralNotSupported(esg.dialect))
		return
	}
	b.Write(esg.dialectOptions.LateralFragment)
	esg.Generate(b, le.Table())
}

// Generates SQL NULL value
func (esg *expressionSQLGenerator) literalNil(b sb.SQLBuilder) {
	if b.IsPrepared() {
		esg.placeHolderSQL(b, nil)
		return
	}
	b.Write(esg.dialectOptions.Null)
}

// Generates SQL bool literal, (e.g. TRUE, FALSE, mysql 1, 0, sqlite3 1, 0)
func (esg *expressionSQLGenerator) literalBool(b sb.SQLBuilder, bl bool) {
	if b.IsPrepared() {
		esg.placeHolderSQL(b, bl)
		return
	}
	if bl {
		b.Write(esg.dialectOptions.True)
	} else {
		b.Write(esg.dialectOptions.False)
	}
}

// Generates SQL for a time.Time value
func (esg *expressionSQLGenerator) literalTime(b sb.SQLBuilder, t time.Time) {
	if b.IsPrepared() {
		esg.placeHolderSQL(b, t)
		return
	}
	esg.Generate(b, t.In(timeLocation).Format(esg.dialectOptions.TimeFormat))
}

// Generates SQL for a Float Value
func (esg *expressionSQLGenerator) literalFloat(b sb.SQLBuilder, f float64) {
	if b.IsPrepared() {
		esg.placeHolderSQL(b, f)
		return
	}
	b.WriteFloat(f)
}

// Generates SQL for an int value
func (esg *expressionSQLGenerator) literalInt(b sb.SQLBuilder, i int64) {
	if b.IsPrepared() {
		esg.placeHolderSQL(b, i)
		return
	}
	b.WriteInt(i)
}

// Generates SQL for an uint value
// Values that fit in an int64 are sent as int64, as they always have been, so existing
// prepared-statement arguments keep their type. Only larger values stay uint64, since
// converting them to int64 would overflow into a negative number.
func (esg *expressionSQLGenerator) literalUint(b sb.SQLBuilder, i uint64) {
	if b.IsPrepared() {
		if i <= math.MaxInt64 {
			esg.placeHolderSQL(b, int64(i))
		} else {
			esg.placeHolderSQL(b, i)
		}
		return
	}
	b.WriteStrings(strconv.FormatUint(i, 10))
}

// Generates SQL for a string
func (esg *expressionSQLGenerator) literalString(b sb.SQLBuilder, s string) {
	if b.IsPrepared() {
		esg.placeHolderSQL(b, s)
		return
	}
	b.GrowBuffer(len(s) + quoteOverhead)
	if hasNonASCII(s) {
		b.Write(esg.dialectOptions.UnicodeStringPrefix)
	}
	b.WriteRunes(esg.dialectOptions.StringQuote)
	for _, char := range s {
		if e, ok := esg.dialectOptions.EscapedRunes[char]; ok {
			b.Write(e)
		} else {
			b.WriteRunes(char)
		}
	}
	b.WriteRunes(esg.dialectOptions.StringQuote)
}

// hasNonASCII returns true if the string contains a byte outside of the ASCII range.
func hasNonASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= utf8.RuneSelf {
			return true
		}
	}
	return false
}

// Generates SQL for a slice of bytes
func (esg *expressionSQLGenerator) literalBytes(b sb.SQLBuilder, bs []byte) {
	if b.IsPrepared() {
		esg.placeHolderSQL(b, bs)
		return
	}
	if esg.isBinary(bs) {
		esg.literalBinary(b, bs)
		return
	}
	b.GrowBuffer(len(bs) + quoteOverhead)
	if hasNonASCII(string(bs)) {
		b.Write(esg.dialectOptions.UnicodeStringPrefix)
	}
	b.WriteRunes(esg.dialectOptions.StringQuote)
	i := 0
	for len(bs) > 0 {
		char, l := utf8.DecodeRune(bs)
		if e, ok := esg.dialectOptions.EscapedRunes[char]; ok {
			b.Write(e)
		} else {
			b.WriteRunes(char)
		}
		i++
		bs = bs[l:]
	}
	b.WriteRunes(esg.dialectOptions.StringQuote)
}

// isBinary returns true if the bytes cannot be written as a text literal without losing data, i.e. they are not
// valid UTF-8 or contain one of the bytes in BinaryLiteralBytes (e.g. NUL, which postgres does not allow in text).
func (esg *expressionSQLGenerator) isBinary(bs []byte) bool {
	if !utf8.Valid(bs) {
		return true
	}
	for _, c := range esg.dialectOptions.BinaryLiteralBytes {
		if bytes.IndexByte(bs, c) >= 0 {
			return true
		}
	}
	return false
}

// Generates SQL for binary data as a hex literal, e.g. X'00ff' (see BinaryLiteralPrefix and BinaryLiteralSuffix)
func (esg *expressionSQLGenerator) literalBinary(b sb.SQLBuilder, bs []byte) {
	const (
		hexDigits = "0123456789abcdef"
		chunk     = 64
	)
	prefix, suffix := esg.dialectOptions.BinaryLiteralPrefix, esg.dialectOptions.BinaryLiteralSuffix
	if len(prefix) == 0 && len(suffix) == 0 {
		// A dialect that was not built from DefaultDialectOptions would otherwise write bare, unquoted hex digits.
		prefix, suffix = defaultBinaryLiteralPrefix, defaultBinaryLiteralSuffix
	}
	b.GrowBuffer(len(bs)*2 + len(prefix) + len(suffix))
	b.Write(prefix)
	var buf [chunk * 2]byte
	for len(bs) > 0 {
		n := min(len(bs), chunk)
		for i, c := range bs[:n] {
			buf[i*2], buf[i*2+1] = hexDigits[c>>4], hexDigits[c&0x0f]
		}
		b.Write(buf[:n*2])
		bs = bs[n:]
	}
	b.Write(suffix)
}

// Generates SQL for a slice of values (e.g. []int64{1,2,3,4} -> (1,2,3,4)
func (esg *expressionSQLGenerator) sliceValueSQL(b sb.SQLBuilder, slice reflect.Value) {
	b.WriteRunes(esg.dialectOptions.LeftParenRune)
	for i, l := 0, slice.Len(); i < l; i++ {
		esg.Generate(b, slice.Index(i).Interface())
		if i < l-1 {
			b.WriteRunes(esg.dialectOptions.CommaRune, esg.dialectOptions.SpaceRune)
		}
	}
	b.WriteRunes(esg.dialectOptions.RightParenRune)
}

// Generates SQL for an AliasedExpression (e.g. I("a").As("b") -> "a" AS "b")
func (esg *expressionSQLGenerator) aliasedExpressionSQL(b sb.SQLBuilder, aliased exp.AliasedExpression) {
	esg.Generate(b, aliased.Aliased())
	b.Write(esg.dialectOptions.AsFragment)
	esg.Generate(b, aliased.GetAs())
}

// Generates SQL for a BooleanExpresion (e.g. I("a").Eq(2) -> "a" = 2)
func (esg *expressionSQLGenerator) booleanExpressionSQL(b sb.SQLBuilder, operator exp.BooleanExpression) {
	b.WriteRunes(esg.dialectOptions.LeftParenRune)
	esg.Generate(b, operator.LHS())
	b.WriteRunes(esg.dialectOptions.SpaceRune)
	operatorOp := operator.Op()
	if val, ok := esg.dialectOptions.BooleanOperatorLookup[operatorOp]; ok {
		b.Write(val)
	} else {
		b.SetError(errUnsupportedBooleanExpressionOperator(operatorOp))
		return
	}
	rhs := operator.RHS()

	if (operatorOp == exp.IsOp || operatorOp == exp.IsNotOp) && rhs != nil && !esg.dialectOptions.BooleanDataTypeSupported {
		b.SetError(errors.New("boolean data type is not supported by dialect %q", esg.dialect))
		return
	}

	if (operatorOp == exp.IsOp || operatorOp == exp.IsNotOp) && esg.dialectOptions.UseLiteralIsBools {
		// these values must be interpolated because preparing them generates invalid SQL
		switch rhs {
		case true:
			rhs = TrueLiteral
		case false:
			rhs = FalseLiteral
		case nil:
			rhs = exp.NewLiteralExpression(string(esg.dialectOptions.Null))
		}
	}
	b.WriteRunes(esg.dialectOptions.SpaceRune)

	if (operatorOp == exp.IsOp || operatorOp == exp.IsNotOp) && rhs == nil && !esg.dialectOptions.BooleanDataTypeSupported {
		// e.g. for SQL server dialect which does not support "IS @p1" for "IS NULL"
		b.Write(esg.dialectOptions.Null)
	} else {
		esg.Generate(b, rhs)
	}

	b.WriteRunes(esg.dialectOptions.RightParenRune)
}

// Generates SQL for a BitwiseExpresion (e.g. I("a").BitwiseOr(2) - > "a" | 2)
func (esg *expressionSQLGenerator) bitwiseExpressionSQL(b sb.SQLBuilder, operator exp.BitwiseExpression) {
	b.WriteRunes(esg.dialectOptions.LeftParenRune)

	if operator.LHS() != nil {
		esg.Generate(b, operator.LHS())
		b.WriteRunes(esg.dialectOptions.SpaceRune)
	}

	operatorOp := operator.Op()
	if val, ok := esg.dialectOptions.BitwiseOperatorLookup[operatorOp]; ok {
		b.Write(val)
	} else {
		b.SetError(errUnsupportedBitwiseExpressionOperator(operatorOp))
		return
	}

	b.WriteRunes(esg.dialectOptions.SpaceRune)
	esg.Generate(b, operator.RHS())
	b.WriteRunes(esg.dialectOptions.RightParenRune)
}

// Generates SQL for a RangeExpresion (e.g. I("a").Between(RangeVal{Start:2,End:5}) -> "a" BETWEEN 2 AND 5)
func (esg *expressionSQLGenerator) rangeExpressionSQL(b sb.SQLBuilder, operator exp.RangeExpression) {
	b.WriteRunes(esg.dialectOptions.LeftParenRune)
	esg.Generate(b, operator.LHS())
	b.WriteRunes(esg.dialectOptions.SpaceRune)
	operatorOp := operator.Op()
	if val, ok := esg.dialectOptions.RangeOperatorLookup[operatorOp]; ok {
		b.Write(val)
	} else {
		b.SetError(errUnsupportedRangeExpressionOperator(operatorOp))
		return
	}
	rhs := operator.RHS()
	b.WriteRunes(esg.dialectOptions.SpaceRune)
	esg.Generate(b, rhs.Start())
	b.Write(esg.dialectOptions.AndFragment)
	esg.Generate(b, rhs.End())
	b.WriteRunes(esg.dialectOptions.RightParenRune)
}

// Generates SQL for an OrderedExpression (e.g. I("a").Asc() -> "a" ASC)
func (esg *expressionSQLGenerator) orderedExpressionSQL(b sb.SQLBuilder, order exp.OrderedExpression) {
	esg.Generate(b, order.SortExpression())
	if order.IsAsc() {
		b.Write(esg.dialectOptions.AscFragment)
	} else {
		b.Write(esg.dialectOptions.DescFragment)
	}
	switch order.NullSortType() {
	case exp.NoNullsSortType:
		return
	case exp.NullsFirstSortType:
		b.Write(esg.dialectOptions.NullsFirstFragment)
	case exp.NullsLastSortType:
		b.Write(esg.dialectOptions.NullsLastFragment)
	}
}

// Generates SQL for an ExpressionList (e.g. And(I("a").Eq("a"), I("b").Eq("b")) -> (("a" = 'a') AND ("b" = 'b')))
func (esg *expressionSQLGenerator) expressionListSQL(b sb.SQLBuilder, expressionList exp.ExpressionList) {
	if expressionList.IsEmpty() {
		return
	}
	var op []byte
	if expressionList.Type() == exp.AndType {
		op = esg.dialectOptions.AndFragment
	} else {
		op = esg.dialectOptions.OrFragment
	}
	exps := expressionList.Expressions()
	expLen := len(exps) - 1
	if expLen > 0 {
		b.WriteRunes(esg.dialectOptions.LeftParenRune)
	} else {
		esg.Generate(b, exps[0])
		return
	}
	for i, e := range exps {
		esg.Generate(b, e)
		if i < expLen {
			b.Write(op)
		}
	}
	b.WriteRunes(esg.dialectOptions.RightParenRune)
}

// Generates SQL for a ColumnListExpression
func (esg *expressionSQLGenerator) columnListSQL(b sb.SQLBuilder, columnList exp.ColumnListExpression) {
	cols := columnList.Columns()
	colLen := len(cols)
	for i, col := range cols {
		esg.Generate(b, col)
		if i < colLen-1 {
			b.WriteRunes(esg.dialectOptions.CommaRune, esg.dialectOptions.SpaceRune)
		}
	}
}

// Generates SQL for an UpdateEpxresion
func (esg *expressionSQLGenerator) updateExpressionSQL(b sb.SQLBuilder, update exp.UpdateExpression) {
	esg.Generate(b, update.Col())
	b.WriteRunes(esg.dialectOptions.SetOperatorRune)
	esg.Generate(b, update.Val())
}

// Generates SQL for a LiteralExpression
//
//	L("a + b") -> a + b
//	L("a = ?", 1) -> a = 1
func (esg *expressionSQLGenerator) literalExpressionSQL(b sb.SQLBuilder, literal exp.LiteralExpression) {
	l := literal.Literal()
	args := literal.Args()
	if argsLen := len(args); argsLen > 0 {
		currIndex := 0
		if !utf8.ValidString(l) {
			// Keep the historical behavior for invalid UTF-8: ranging over the string turns invalid bytes into U+FFFD.
			for _, char := range l {
				if char == replacementRune && currIndex < argsLen {
					esg.Generate(b, args[currIndex])
					currIndex++
				} else {
					b.WriteRunes(char)
				}
			}
			return
		}
		// '?' is ASCII, so it can be found bytewise and the text between placeholders written in one go.
		for l != "" {
			i := strings.IndexByte(l, replacementByte)
			if i == -1 {
				b.WriteStrings(l)
				return
			}
			b.WriteStrings(l[:i])
			if currIndex < argsLen {
				esg.Generate(b, args[currIndex])
				currIndex++
			} else {
				b.WriteRunes(replacementRune)
			}
			l = l[i+1:]
		}
		return
	}
	b.WriteStrings(l)
}

// Generates SQL for a SQLFunctionExpression
//
//	COUNT(I("a")) -> COUNT("a")
func (esg *expressionSQLGenerator) sqlFunctionExpressionSQL(b sb.SQLBuilder, sqlFunc exp.SQLFunctionExpression) {
	b.WriteStrings(sqlFunc.Name())
	esg.Generate(b, sqlFunc.Args())
}

func (esg *expressionSQLGenerator) sqlWindowFunctionExpression(b sb.SQLBuilder, sqlWinFunc exp.SQLWindowFunctionExpression) {
	if !esg.dialectOptions.SupportsWindowFunction {
		b.SetError(ErrWindowNotSupported(esg.dialect))
		return
	}
	esg.Generate(b, sqlWinFunc.Func())
	b.Write(esg.dialectOptions.WindowOverFragment)
	switch {
	case sqlWinFunc.HasWindowName():
		esg.Generate(b, sqlWinFunc.WindowName())
	case sqlWinFunc.HasWindow():
		if sqlWinFunc.Window().HasName() {
			b.SetError(ErrUnexpectedNamedWindow)
			return
		}
		esg.Generate(b, sqlWinFunc.Window())
	default:
		esg.Generate(b, exp.NewWindowExpression(nil, nil, nil, nil))
	}
}

func (esg *expressionSQLGenerator) windowExpressionSQL(b sb.SQLBuilder, we exp.WindowExpression) {
	if !esg.dialectOptions.SupportsWindowFunction {
		b.SetError(ErrWindowNotSupported(esg.dialect))
		return
	}
	if we.HasName() {
		esg.Generate(b, we.Name())
		b.Write(esg.dialectOptions.AsFragment)
	}
	b.WriteRunes(esg.dialectOptions.LeftParenRune)

	hasPartition := we.HasPartitionBy()
	hasOrder := we.HasOrder()

	if we.HasParent() {
		esg.Generate(b, we.Parent())
		if hasPartition || hasOrder {
			b.WriteRunes(esg.dialectOptions.SpaceRune)
		}
	}

	if hasPartition {
		b.Write(esg.dialectOptions.WindowPartitionByFragment)
		esg.Generate(b, we.PartitionCols())
		if hasOrder {
			b.WriteRunes(esg.dialectOptions.SpaceRune)
		}
	}
	if hasOrder {
		b.Write(esg.dialectOptions.WindowOrderByFragment)
		esg.Generate(b, we.OrderCols())
	}

	b.WriteRunes(esg.dialectOptions.RightParenRune)
}

// Generates SQL for a CastExpression
//
//	I("a").Cast("NUMERIC") -> CAST("a" AS NUMERIC)
func (esg *expressionSQLGenerator) castExpressionSQL(b sb.SQLBuilder, cast exp.CastExpression) {
	b.Write(esg.dialectOptions.CastFragment).WriteRunes(esg.dialectOptions.LeftParenRune)
	esg.Generate(b, cast.Casted())
	b.Write(esg.dialectOptions.AsFragment)
	esg.Generate(b, cast.Type())
	b.WriteRunes(esg.dialectOptions.RightParenRune)
}

// Generates SQL for a CollateExpression
//
//	I("a").Collate("en_US") -> "a" COLLATE en_US
//	I("a").Collate("en_US") -> "a" COLLATE "en_US" (dialects with QuoteCollation)
//	I("a").Collate(I("pg_catalog.default")) -> "a" COLLATE "pg_catalog"."default" (dialects with QuoteCollation)
func (esg *expressionSQLGenerator) collateExpressionSQL(b sb.SQLBuilder, collate exp.CollateExpression) {
	opts := esg.dialectOptions
	parts, ok := esg.collationParts(collate.Collation())
	if !ok {
		b.SetError(ErrInvalidCollation(describeCollation(collate.Collation())))
		return
	}

	// a bare literal could be an arbitrary expression (e.g. a || b), only the last term would be collated
	_, isLiteral := collate.Collated().(exp.LiteralExpression)
	if isLiteral {
		b.WriteRunes(opts.LeftParenRune)
	}
	esg.Generate(b, collate.Collated())
	if isLiteral {
		b.WriteRunes(opts.RightParenRune)
	}
	b.Write(opts.CollateFragment)
	for i, part := range parts {
		if i > 0 {
			b.WriteRunes(opts.PeriodRune)
		}
		if opts.QuoteCollation {
			quote := string(opts.QuoteRune)
			b.WriteStrings(quote, strings.ReplaceAll(part, quote, quote+quote), quote)
		} else {
			b.WriteStrings(part)
		}
	}
}

// Returns the (unquoted) parts of the collation name and whether it is valid for the dialect.
//
// A string is a single quoted name for dialects that quote collations (it may contain anything, e.g.
// "en_US.utf8"), otherwise it is split on '.' into schema qualified parts. An identifier is always split into its parts.
func (esg *expressionSQLGenerator) collationParts(collation any) (parts []string, ok bool) {
	quote := esg.dialectOptions.QuoteCollation
	switch c := collation.(type) {
	case string:
		if quote {
			return []string{c}, c != "" && !strings.ContainsRune(c, 0)
		}
		parts = strings.Split(c, ".")
	case exp.IdentifierExpression:
		if c.IsEmpty() {
			return nil, false
		}
		for _, part := range []any{c.GetSchema(), c.GetTable(), c.GetCol()} {
			switch p := part.(type) {
			case nil:
			case string:
				if p != esg.dialectOptions.EmptyString {
					parts = append(parts, p)
				}
			default:
				return nil, false
			}
		}
	default:
		return nil, false
	}
	if len(parts) == 0 || len(parts) > 3 {
		return nil, false
	}
	for _, part := range parts {
		if quote {
			if part == "" || strings.ContainsRune(part, 0) {
				return nil, false
			}
		} else if !validCollationPart(part) {
			return nil, false
		}
	}
	return parts, true
}

// describes a collation for error messages, identifiers are shown as their dotted name rather than as a struct
func describeCollation(collation any) string {
	if ident, ok := collation.(exp.IdentifierExpression); ok {
		var parts []string
		for _, part := range []any{ident.GetSchema(), ident.GetTable(), ident.GetCol()} {
			if p, isString := part.(string); isString && p != "" {
				parts = append(parts, p)
			}
		}
		return strings.Join(parts, ".")
	}
	return fmt.Sprintf("%v", collation)
}

// unquoted collation name parts are restricted to simple identifiers
func validCollationPart(part string) bool {
	for i, r := range part {
		switch {
		case r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z'):
		case i > 0 && r >= '0' && r <= '9':
		default:
			return false
		}
	}
	return part != ""
}

// Generates the sql for the WITH clauses for common table expressions (CTE)
func (esg *expressionSQLGenerator) commonTablesSliceSQL(b sb.SQLBuilder, ctes []exp.CommonTableExpression) {
	l := len(ctes)
	if l == 0 {
		return
	}
	if !esg.dialectOptions.SupportsWithCTE {
		b.SetError(ErrCTENotSupported(esg.dialect))
		return
	}
	b.Write(esg.dialectOptions.WithFragment)
	anyRecursive := false
	for _, cte := range ctes {
		anyRecursive = anyRecursive || cte.IsRecursive()
	}
	if anyRecursive {
		if !esg.dialectOptions.SupportsWithCTERecursive {
			b.SetError(ErrRecursiveCTENotSupported(esg.dialect))
			return
		}
		b.Write(esg.dialectOptions.RecursiveFragment)
	}
	for i, cte := range ctes {
		esg.Generate(b, cte)
		if i < l-1 {
			b.WriteRunes(esg.dialectOptions.CommaRune, esg.dialectOptions.SpaceRune)
		}
	}
	b.WriteRunes(esg.dialectOptions.SpaceRune)
}

// Generates SQL for a CommonTableExpression
func (esg *expressionSQLGenerator) commonTableExpressionSQL(b sb.SQLBuilder, cte exp.CommonTableExpression) {
	esg.Generate(b, cte.Name())
	b.Write(esg.dialectOptions.AsFragment)
	esg.Generate(b, cte.SubQuery())
}

// Generates SQL for a CompoundExpression
func (esg *expressionSQLGenerator) compoundExpressionSQL(b sb.SQLBuilder, compound exp.CompoundExpression) {
	switch compound.Type() {
	case exp.UnionCompoundType:
		b.Write(esg.dialectOptions.UnionFragment)
	case exp.UnionAllCompoundType:
		b.Write(esg.dialectOptions.UnionAllFragment)
	case exp.IntersectCompoundType:
		b.Write(esg.dialectOptions.IntersectFragment)
	case exp.IntersectAllCompoundType:
		b.Write(esg.dialectOptions.IntersectAllFragment)
	}
	if esg.dialectOptions.WrapCompoundsInParens {
		b.WriteRunes(esg.dialectOptions.LeftParenRune)
		compound.RHS().AppendSQL(b)
		b.WriteRunes(esg.dialectOptions.RightParenRune)
	} else {
		compound.RHS().AppendSQL(b)
	}
}

// Generates SQL for a CaseExpression
func (esg *expressionSQLGenerator) caseExpressionSQL(b sb.SQLBuilder, caseExpression exp.CaseExpression) {
	caseVal := caseExpression.GetValue()
	whens := caseExpression.GetWhens()
	elseResult := caseExpression.GetElse()

	if len(whens) == 0 {
		b.SetError(ErrEmptyCaseWhens)
		return
	}
	b.Write(esg.dialectOptions.CaseFragment)
	if caseVal != nil {
		esg.Generate(b, caseVal)
	}
	for _, when := range whens {
		b.Write(esg.dialectOptions.WhenFragment)
		esg.Generate(b, when.Condition())
		b.Write(esg.dialectOptions.ThenFragment)
		esg.Generate(b, when.Result())
	}
	if elseResult != nil {
		b.Write(esg.dialectOptions.ElseFragment)
		esg.Generate(b, elseResult.Result())
	}
	b.Write(esg.dialectOptions.EndFragment)
}

func (esg *expressionSQLGenerator) expressionMapSQL(b sb.SQLBuilder, ex exp.Ex) {
	expressionList, err := ex.ToExpressions()
	if err != nil {
		b.SetError(err)
		return
	}
	esg.Generate(b, expressionList)
}

func (esg *expressionSQLGenerator) expressionOrMapSQL(b sb.SQLBuilder, ex exp.ExOr) {
	expressionList, err := ex.ToExpressions()
	if err != nil {
		b.SetError(err)
		return
	}
	esg.Generate(b, expressionList)
}
