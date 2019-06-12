package exp

import (
	"reflect"
	"sort"

	"github.com/doug-martin/goqu/v7/internal/errors"
	"github.com/doug-martin/goqu/v7/internal/tag"
	"github.com/doug-martin/goqu/v7/internal/util"
)

type (
	insert struct {
		from       AppendableExpression
		cols       ColumnListExpression
		vals       [][]interface{}
		onConflict ConflictExpression
	}
)

func NewInsertExpression(rows ...interface{}) (insertExpression InsertExpression, err error) {
	switch len(rows) {
	case 0:
		return new(insert), nil
	case 1:
		val := reflect.ValueOf(rows[0])
		if val.Kind() == reflect.Slice {
			vals := make([]interface{}, val.Len())
			for i := 0; i < val.Len(); i++ {
				vals[i] = val.Index(i).Interface()
			}
			return NewInsertExpression(vals...)
		}
		if ae, ok := rows[0].(AppendableExpression); ok {
			return &insert{from: ae}, nil
		}

	}
	return newInsert(rows...)
}

func (i *insert) Expression() Expression {
	return i
}

func (i *insert) Clone() Expression {
	return i.clone()
}

func (i *insert) clone() *insert {
	return &insert{from: i.from, cols: i.cols, vals: i.vals, onConflict: i.onConflict}
}

func (i *insert) IsEmpty() bool {
	return i.from == nil && (i.cols == nil || i.cols.IsEmpty())
}

func (i *insert) IsInsertFrom() bool {
	return i.from != nil
}
func (i *insert) From() AppendableExpression {
	return i.from
}
func (i *insert) Cols() ColumnListExpression {
	return i.cols
}

func (i *insert) SetCols(cols ColumnListExpression) InsertExpression {
	ci := i.clone()
	ci.cols = cols
	return ci
}

func (i *insert) Vals() [][]interface{} {
	return i.vals
}

func (i *insert) SetVals(vals [][]interface{}) InsertExpression {
	ci := i.clone()
	ci.vals = vals
	return ci
}

func (i *insert) OnConflict() ConflictExpression {
	return i.onConflict
}

func (i *insert) SetOnConflict(ce ConflictExpression) InsertExpression {
	ci := i.clone()
	ci.onConflict = ce
	return ci
}

func (i *insert) DoNothing() InsertExpression {
	return i.SetOnConflict(NewDoNothingConflictExpression())

}
func (i *insert) DoUpdate(target string, update interface{}) InsertExpression {
	return i.SetOnConflict(NewDoUpdateConflictExpression(target, update))
}

// parses the rows gathering and sorting unique columns and values for each record
func newInsert(rows ...interface{}) (insertExp InsertExpression, err error) {
	var mapKeys util.ValueSlice
	rowValue := reflect.Indirect(reflect.ValueOf(rows[0]))
	rowType := rowValue.Type()
	rowKind := rowValue.Kind()
	vals := make([][]interface{}, len(rows))
	var columns ColumnListExpression
	for i, row := range rows {
		if rowType != reflect.Indirect(reflect.ValueOf(row)).Type() {
			return nil, errors.New(
				"rows must be all the same type expected %+v got %+v",
				rowType,
				reflect.Indirect(reflect.ValueOf(row)).Type(),
			)
		}
		newRowValue := reflect.Indirect(reflect.ValueOf(row))
		switch rowKind {
		case reflect.Map:
			if columns == nil {
				mapKeys = util.ValueSlice(newRowValue.MapKeys())
				sort.Sort(mapKeys)
				colKeys := make([]interface{}, len(mapKeys))
				for j, key := range mapKeys {
					colKeys[j] = key.Interface()
				}
				columns = NewColumnListExpression(colKeys...)
			}
			newMapKeys := util.ValueSlice(newRowValue.MapKeys())
			if len(newMapKeys) != len(mapKeys) {
				return nil, errors.New("rows with different value length expected %d got %d", len(mapKeys), len(newMapKeys))
			}
			if !mapKeys.Equal(newMapKeys) {
				return nil, errors.New("rows with different keys expected %s got %s", mapKeys.String(), newMapKeys.String())
			}
			rowVals := make([]interface{}, len(mapKeys))
			for j, key := range mapKeys {
				rowVals[j] = newRowValue.MapIndex(key).Interface()
			}
			vals[i] = rowVals
		case reflect.Struct:
			rowCols, rowVals := getFieldsValues(newRowValue)
			if columns == nil {
				columns = NewColumnListExpression(rowCols...)
			}
			vals[i] = rowVals
		default:
			return nil, errors.New(
				"unsupported insert must be map, goqu.Record, or struct type got: %T",
				row,
			)
		}
	}
	return &insert{cols: columns, vals: vals}, nil
}

func getFieldsValues(value reflect.Value) (rowCols, rowVals []interface{}) {
	if value.IsValid() {
		for i := 0; i < value.NumField(); i++ {
			v := value.Field(i)
			t := value.Type().Field(i)
			if !t.Anonymous {
				if canInsertField(&t) {
					rowCols = append(rowCols, t.Tag.Get("db"))
					rowVals = append(rowVals, v.Interface())
				}
			} else {
				cols, vals := getFieldsValues(reflect.Indirect(reflect.ValueOf(v.Interface())))
				rowCols = append(rowCols, cols...)
				rowVals = append(rowVals, vals...)
			}
		}
	}
	return rowCols, rowVals
}

func canInsertField(field *reflect.StructField) bool {
	goquTag := tag.New("goqu", field.Tag)
	dbTag := tag.New("db", field.Tag)
	return !goquTag.Contains("skipinsert") && !(dbTag.IsEmpty() || dbTag.Equals("-"))
}
