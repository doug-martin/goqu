package gql

import (
	"reflect"
	"sort"
)

func (me *Dataset) UpdateSql(update interface{}) (string, error) {
	sql, _, err := me.ToUpdateSql(false, update)
	return sql, err
}

func (me *Dataset) canUpdateField(field reflect.StructField) bool {
	gqlTag, dbTag := tagOptions(field.Tag.Get("gql")), field.Tag.Get("db")
	return !gqlTag.Contains("skipupdate") && dbTag != "" && dbTag != "-"
}

func (me *Dataset) ToUpdateSql(isPrepared bool, update interface{}) (string, []interface{}, error) {
	if !me.hasSources() {
		return "", nil, NewGqlError("No source found when generating update sql")
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
		return "", nil, NewGqlError("Unsupported update interface type %+v", updateValue.Type())
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
		return "", nil, NewGqlError("Adapter does not support RETURNING clause")
	}
	sql, args := buf.ToSql()
	return sql, args, nil
}
