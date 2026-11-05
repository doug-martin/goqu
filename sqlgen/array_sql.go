package sqlgen

import (
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"math"
	"reflect"
	"strconv"
	"time"

	"github.com/doug-martin/goqu/v9/exp"
	"github.com/doug-martin/goqu/v9/internal/errors"
	"github.com/doug-martin/goqu/v9/internal/sb"
)

// isArrayValue returns true for slices and arrays that should render as an array value: []byte (and other byte
// slices), json.RawMessage and values that render themselves (expressions, driver.Valuers) never do.
func isArrayValue(val interface{}) bool {
	switch val.(type) {
	case string, bool, int, int32, int64, uint64, float32, float64, time.Time, *time.Time:
		// the common scalars, skip the reflection below
		return false
	case nil, exp.Expression, driver.Valuer, []byte, json.RawMessage, []exp.CommonTableExpression:
		return false
	}
	rv := reflect.ValueOf(val)
	if rv.Kind() == reflect.Ptr && !rv.IsNil() {
		// pointers to slices render like the slice, a nil pointer is NULL
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return false
	}
	return rv.Type().Elem().Kind() != reflect.Uint8
}

// asArrayValue wraps val in an array expression when SliceAsArray is set and val is a slice.
func (do *SQLDialectOptions) asArrayValue(val interface{}) interface{} {
	if do.SliceAsArray && isArrayValue(val) {
		return exp.NewArrayExpression(val)
	}
	return val
}

// asArrayRow returns the row with its slices wrapped in array expressions. The row is only copied when needed.
func (do *SQLDialectOptions) asArrayRow(row []interface{}) []interface{} {
	if !do.SliceAsArray {
		return row
	}
	var out []interface{}
	for i, v := range row {
		if !isArrayValue(v) {
			continue
		}
		if out == nil {
			out = make([]interface{}, len(row))
			copy(out, row)
		}
		out[i] = exp.NewArrayExpression(v)
	}
	if out == nil {
		return row
	}
	return out
}

// ErrEmptyNestedArray is the error set when a nested array is empty (e.g. [][]int{{}, {}}). Postgres cannot store an
// empty sub array.
var ErrEmptyNestedArray = errors.New("empty nested arrays are not supported")

// arraySQL writes a slice or array as a single array value. It is written as the text literal of the array (e.g.
// '{"a","b"}'), the same value pq.Array produces. A literal (or its prepared parameter) takes the type of the column it
// is assigned to or compared with, ARRAY['1', '2'] is always text[] and fails for an integer[] or uuid[] column. When
// an element cannot be written in the text form (e.g. an expression) the array is written as ARRAY[e1, e2, ...].
// A nil slice is NULL, an empty slice is '{}'.
func (esg *expressionSQLGenerator) arraySQL(b sb.SQLBuilder, elems interface{}) {
	rv := reflect.Indirect(reflect.ValueOf(elems))
	if !rv.IsValid() || (rv.Kind() == reflect.Slice && rv.IsNil()) {
		esg.literalNil(b)
		return
	}
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		b.SetError(errors.New("goqu.Array requires a slice or array, got %T", elems))
		return
	}
	text, ok, err := appendArrayLiteral(nil, rv, 0)
	switch {
	case err != nil:
		b.SetError(err)
	case ok:
		esg.Generate(b, string(text))
	default:
		esg.arrayConstructorSQL(b, rv, 0)
	}
}

// arrayConstructorSQL writes ARRAY[e1, e2, ...] for arrays with elements that have no text form.
func (esg *expressionSQLGenerator) arrayConstructorSQL(b sb.SQLBuilder, rv reflect.Value, depth int) {
	n := rv.Len()
	if n == 0 {
		// only nested arrays get here, an empty top level array is written as '{}' by arraySQL
		b.SetError(ErrEmptyNestedArray)
		return
	}
	b.Write([]byte("ARRAY"))
	b.WriteRunes('[')
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteRunes(esg.dialectOptions.CommaRune, esg.dialectOptions.SpaceRune)
		}
		elem := rv.Index(i).Interface()
		if isArrayValue(elem) {
			esg.arrayConstructorSQL(b, reflect.Indirect(reflect.ValueOf(elem)), depth+1)
		} else {
			esg.Generate(b, elem)
		}
	}
	b.WriteRunes(']')
}

// appendArrayLiteral appends the text literal of a slice or array (e.g. {"a","b"}). ok is false when an element has no
// text form.
func appendArrayLiteral(buf []byte, rv reflect.Value, depth int) (out []byte, ok bool, err error) {
	n := rv.Len()
	if n == 0 && depth > 0 {
		return buf, false, ErrEmptyNestedArray
	}
	buf = append(buf, '{')
	for i := 0; i < n; i++ {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf, ok, err = appendArrayElement(buf, rv.Index(i).Interface(), depth+1)
		if !ok || err != nil {
			return buf, false, err
		}
	}
	return append(buf, '}'), true, nil
}

func appendArrayElement(buf []byte, v interface{}, depth int) (out []byte, ok bool, err error) {
	rv := reflect.ValueOf(v)
	if !rv.IsValid() || ((rv.Kind() == reflect.Ptr || rv.Kind() == reflect.Interface) && rv.IsNil()) {
		return append(buf, "NULL"...), true, nil
	}
	switch t := v.(type) {
	case exp.Expression:
		return buf, false, nil
	case driver.Valuer:
		dv, vErr := t.Value()
		if vErr != nil {
			return buf, false, vErr
		}
		return appendArrayElement(buf, dv, depth)
	case time.Time:
		return appendQuotedElement(buf, t.Format("2006-01-02 15:04:05.999999999Z07:00")), true, nil
	}
	switch rv.Kind() {
	case reflect.Ptr:
		return appendArrayElement(buf, rv.Elem().Interface(), depth)
	case reflect.Slice, reflect.Array:
		if rv.Kind() == reflect.Slice && rv.Type().Elem().Kind() == reflect.Uint8 {
			return appendQuotedElement(buf, `\x`+hex.EncodeToString(rv.Bytes())), true, nil
		}
		return appendArrayLiteral(buf, rv, depth)
	default:
		buf, ok = appendScalarElement(buf, rv)
		return buf, ok, nil
	}
}

const decimalBase = 10

// appendScalarElement appends strings, booleans and numbers. It returns false for any other kind.
func appendScalarElement(buf []byte, rv reflect.Value) ([]byte, bool) {
	switch rv.Kind() {
	case reflect.String:
		return appendQuotedElement(buf, rv.String()), true
	case reflect.Bool:
		return strconv.AppendBool(buf, rv.Bool()), true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.AppendInt(buf, rv.Int(), decimalBase), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return strconv.AppendUint(buf, rv.Uint(), decimalBase), true
	case reflect.Float32, reflect.Float64:
		f := rv.Float()
		switch {
		case math.IsInf(f, 1):
			return append(buf, "Infinity"...), true
		case math.IsInf(f, -1):
			return append(buf, "-Infinity"...), true
		}
		return strconv.AppendFloat(buf, f, 'g', -1, rv.Type().Bits()), true
	default:
		return buf, false
	}
}

// appendQuotedElement appends s as a double quoted array element, escaping backslashes and double quotes.
func appendQuotedElement(buf []byte, s string) []byte {
	buf = append(buf, '"')
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' || s[i] == '"' {
			buf = append(buf, '\\')
		}
		buf = append(buf, s[i])
	}
	return append(buf, '"')
}
