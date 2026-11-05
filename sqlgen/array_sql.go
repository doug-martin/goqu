package sqlgen

import (
	"database/sql/driver"
	"encoding/json"
	"reflect"
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

// arraySQL writes a slice as ARRAY[e1, e2, ...]. A nil slice is NULL and an empty slice is '{}' because the type of an
// empty ARRAY[] cannot be inferred. Nested slices are nested arrays.
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
	n := rv.Len()
	if n == 0 {
		esg.Generate(b, "{}")
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
			esg.arraySQL(b, elem)
		} else {
			esg.Generate(b, elem)
		}
	}
	b.WriteRunes(']')
}
