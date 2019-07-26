package exp

import (
	"reflect"
	"sort"

	"github.com/doug-martin/goqu/v8/internal/errors"
	"github.com/doug-martin/goqu/v8/internal/util"
)

type (
	update struct {
		col IdentifierExpression
		val interface{}
	}
)

func set(col IdentifierExpression, val interface{}) UpdateExpression {
	return update{col: col, val: val}
}

func NewUpdateExpressions(update interface{}) (updates []UpdateExpression, err error) {
	if u, ok := update.(UpdateExpression); ok {
		updates = append(updates, u)
		return updates, nil
	}
	updateValue := reflect.Indirect(reflect.ValueOf(update))
	switch updateValue.Kind() {
	case reflect.Map:
		keys := util.ValueSlice(updateValue.MapKeys())
		sort.Sort(keys)
		for _, key := range keys {
			updates = append(updates, ParseIdentifier(key.String()).Set(updateValue.MapIndex(key).Interface()))
		}
	case reflect.Struct:
		return getUpdateExpressionsStruct(updateValue)
	default:
		return nil, errors.New("unsupported update interface type %+v", updateValue.Type())
	}
	return updates, nil
}

func getUpdateExpressionsStruct(value reflect.Value) (updates []UpdateExpression, err error) {
	cm, err := util.GetColumnMap(value.Interface())
	if err != nil {
		return updates, err
	}
	cols := cm.Cols()
	for _, col := range cols {
		f := cm[col]
		if f.ShouldUpdate {
			v := value.FieldByIndex(f.FieldIndex)
			setV := v.Interface()
			if f.DefaultIfEmpty && util.IsEmptyValue(v) {
				setV = Default()
			}
			updates = append(updates, ParseIdentifier(col).Set(setV))
		}
	}
	return updates, nil
}

func (u update) Expression() Expression {
	return u
}

func (u update) Clone() Expression {
	return update{col: u.col.Clone().(IdentifierExpression), val: u.val}
}

func (u update) Col() IdentifierExpression {
	return u.col
}

func (u update) Val() interface{} {
	return u.val
}
