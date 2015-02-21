package gql

import (
	"reflect"
	"sort"
	"strings"
)

func (me Dataset) UpdateSql(update interface{}) (string, error) {
	if !me.hasSources() {
		return "", newGqlError("No source found when generating update sql")
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
		return "", newGqlError("Unsupported update interface type %+v", updateValue.Type())
	}

	return me.updateSql(updates...)
}

func (me Dataset) canUpdateField(field reflect.StructField) bool {
	gqlTag, dbTag := tagOptions(field.Tag.Get("gql")), field.Tag.Get("db")
	return !gqlTag.Contains("skipupdate") && dbTag != "" && dbTag != "-"
}

func (me Dataset) updateSql(updates ...UpdateExpression) (string, error) {
	var (
		err        error
		sql        string
		updateStmt []string
	)
	if sql, err = me.adapter.UpdateBeginSql(); err != nil {
		return "", err
	}
	updateStmt = append(updateStmt, sql)
	if sql, err = me.adapter.SourcesSql(me.clauses.From); err != nil {
		return "", err
	}
	updateStmt = append(updateStmt, sql)
	if sql, err = me.adapter.UpdateExpressionsSql(updates...); err != nil {
		return "", err
	}
	updateStmt = append(updateStmt, sql)
	sql, err = me.adapter.WhereSql(me.clauses.Where)
	if err != nil {
		return "", err
	}
	updateStmt = append(updateStmt, sql)
	sql, err = me.adapter.ReturningSql(me.clauses.Returning)
	if err != nil {
		return "", err
	}
	updateStmt = append(updateStmt, sql)
	return strings.Join(updateStmt, ""), nil
}
