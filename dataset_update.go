package goqu

import (
	"reflect"
	"sort"
)

func (me *Dataset) canUpdateField(field reflect.StructField) bool {
	goquTag, dbTag := tagOptions(field.Tag.Get("goqu")), field.Tag.Get("db")
	return !goquTag.Contains("skipupdate") && dbTag != "" && dbTag != "-"
}

//Generates an UPDATE statement. If `Prepared` has been called with true then the statement will not be interpolated.
//When using structs you may specify a column to be skipped in the update, (e.g. created) by specifying a goqu tag with `skipupdate`
//    type Item struct{
//       Id      uint32    `db:"id"
//       Created time.Time `db:"created" goqu:"skipupdate"`
//       Name    string    `db:"name"`
//    }
//
//update: can either be a a map[string]interface{}, Record or a struct
//
//Errors:
//  * The update is not a of type struct, Record, or map[string]interface{}
//  * The update statement has no FROM clause
//  * There is an error generating the SQL
func (me *Dataset) ToUpdateSql(update interface{}) (string, []interface{}, error) {
	if !me.hasSources() {
		return "", nil, NewGoquError("No source found when generating update sql")
	}
	updates, err := me.getUpdateExpressions(update)
	if err != nil {
		return "", nil, err
	}
	buf := NewSqlBuilder(me.isPrepared)
	if err := me.adapter.CommonTablesSql(buf, me.clauses.CommonTables); err != nil {
		return "", nil, err
	}
	if err := me.adapter.UpdateBeginSql(buf); err != nil {
		return "", nil, err
	}
	if err := me.adapter.SourcesSql(buf, me.clauses.From); err != nil {
		return "", nil, err
	}
	if err := me.adapter.UpdateExpressionsSql(buf, updates...); err != nil {
		return "", nil, err
	}
	if err := me.adapter.WhereSql(buf, me.clauses.Where); err != nil {
		return "", nil, err
	}
	if me.adapter.SupportsOrderByOnUpdate() {
		if err := me.adapter.OrderSql(buf, me.clauses.Order); err != nil {
			return "", nil, err
		}
	}
	if me.adapter.SupportsLimitOnUpdate() {
		if err := me.adapter.LimitSql(buf, me.clauses.Limit); err != nil {
			return "", nil, err
		}
	}
	if me.adapter.SupportsReturn() {
		if err := me.adapter.ReturningSql(buf, me.clauses.Returning); err != nil {
			return "", nil, err
		}
	} else if me.clauses.Returning != nil {
		return "", nil, NewGoquError("Adapter does not support RETURNING clause")
	}
	sql, args := buf.ToSql()
	return sql, args, nil
}

func (me *Dataset) getUpdateExpressions(update interface{}) ([]UpdateExpression, error) {
	updateValue := reflect.Indirect(reflect.ValueOf(update))
	var updates []UpdateExpression
	switch updateValue.Kind() {
	case reflect.Map:
		keys := valueSlice(updateValue.MapKeys())
		sort.Sort(keys)
		for _, key := range keys {
			updates = append(updates, I(key.String()).Set(updateValue.MapIndex(key).Interface()))
		}
	case reflect.Struct:
		updates = me.getUpdateExpressionsStruct(updateValue)
	default:
		return nil, NewGoquError("Unsupported update interface type %+v", updateValue.Type())
	}
	return updates, nil
}

func (me *Dataset) getUpdateExpressionsStruct(value reflect.Value) (updates []UpdateExpression) {
	for i := 0; i < value.NumField(); i++ {
		v := value.Field(i)
		t := value.Type().Field(i)
		if !t.Anonymous {
			if me.canUpdateField(t) {
				updates = append(updates, I(t.Tag.Get("db")).Set(v.Interface()))
			}
		} else {
			updates = append(updates, me.getUpdateExpressionsStruct(reflect.Indirect(reflect.ValueOf(v.Interface())))...)
		}
	}

	return updates
}
