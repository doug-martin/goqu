package exp

import (
	"reflect"
	"sort"

	"github.com/doug-martin/goqu/v7/internal/errors"
	"github.com/doug-martin/goqu/v7/internal/tag"
	"github.com/doug-martin/goqu/v7/internal/util"
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
		updates = getUpdateExpressionsStruct(updateValue)
	default:
		return nil, errors.New("unsupported update interface type %+v", updateValue.Type())
	}
	return updates, nil
}

func getUpdateExpressionsStruct(value reflect.Value) (updates []UpdateExpression) {
	for i := 0; i < value.NumField(); i++ {
		v := value.Field(i)
		t := value.Type().Field(i)
		if !t.Anonymous {
			if canUpdateField(&t) {
				updates = append(updates, ParseIdentifier(t.Tag.Get("db")).Set(v.Interface()))
			}
		} else {
			updates = append(updates, getUpdateExpressionsStruct(reflect.Indirect(reflect.ValueOf(v.Interface())))...)
		}
	}

	return updates
}

func canUpdateField(field *reflect.StructField) bool {
	goquTag := tag.New("goqu", field.Tag)
	dbTag := tag.New("db", field.Tag)
	return !goquTag.Contains("skipupdate") && !(dbTag.IsEmpty() || dbTag.Equals("-"))
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
