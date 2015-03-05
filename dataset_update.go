package goqu

import (
	"reflect"
	"sort"
)

//Generates the default UPDATE statement. This calls Dataset.ToUpdateSql with isPrepared set to false.
//When using structs you may specify a column to be skipped in the update, (e.g. created) by specifying a goqu tag with `skipupdate`
//    type Item struct{
//       Id      uint32    `db:"id"
//       Created time.Time `db:"created" goqu:"skipupdate"`
//       Name    string    `db:"name"`
//    }
//
//update: can either be a a map[string]interface or a struct
//
//Errors:
//  * The update is not a struct, Record, or map[string]interface
//  * The update statement has no FROM clause
//  * There is an error generating the SQL
func (me *Dataset) UpdateSql(update interface{}) (string, error) {
	sql, _, err := me.ToUpdateSql(false, update)
	return sql, err
}

func (me *Dataset) canUpdateField(field reflect.StructField) bool {
	goquTag, dbTag := tagOptions(field.Tag.Get("goqu")), field.Tag.Get("db")
	return !goquTag.Contains("skipupdate") && dbTag != "" && dbTag != "-"
}

//Generates an UPDATE statement.
//When using structs you may specify a column to be skipped in the update, (e.g. created) by specifying a goqu tag with `skipupdate`
//    type Item struct{
//       Id      uint32    `db:"id"
//       Created time.Time `db:"created" goqu:"skipupdate"`
//       Name    string    `db:"name"`
//    }
//
//isPrepared: set to true to generate an sql statement with placeholders for primitive values
//update: can either be a a map[string]interface{}, Record or a struct
//
//Errors:
//  * The update is not a of type struct, Record, or map[string]interface{}
//  * The update statement has no FROM clause
//  * There is an error generating the SQL
func (me *Dataset) ToUpdateSql(isPrepared bool, update interface{}) (string, []interface{}, error) {
	if !me.hasSources() {
		return "", nil, NewGoquError("No source found when generating update sql")
	}
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
		for j := 0; j < updateValue.NumField(); j++ {
			f := updateValue.Field(j)
			t := updateValue.Type().Field(j)
			if me.canUpdateField(t) {
				updates = append(updates, I(t.Tag.Get("db")).Set(f.Interface()))
			}
		}
	default:
		return "", nil, NewGoquError("Unsupported update interface type %+v", updateValue.Type())
	}
	buf := NewSqlBuilder(isPrepared)
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
