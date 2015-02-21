package gql

import "reflect"

func (me Dataset) Query(i interface{}) (bool, error) {
	var (
		sql string
		err error
	)
	switch reflect.Indirect(reflect.ValueOf(i)).Kind() {
	case reflect.Struct:
		sql, err = me.Limit(1).Sql()
	case reflect.Slice:
		sql, err = me.Sql()
	default:
		return false, newGqlError("Type must be a pointer to a slice or struct when calling Query")
	}
	if err != nil {
		return false, newGqlQueryError(err.Error())
	}
	return me.database.Select(i, sql)
}

func (me Dataset) Count() (int64, error) {
	var count countResult
	if _, err := me.Select(COUNT(Star()).As("count")).Query(&count); err != nil {
		return 0, err
	}
	return count.Count, nil
}

func (me Dataset) Pluck(i interface{}, col string) error {
	var (
		results selectResults
		sql     string
		err     error
	)
	val := reflect.ValueOf(i)
	if val.Kind() != reflect.Ptr {
		return newGqlError("Type must be a pointer to a slice when calling Pluck")
	}
	//create a temp column map
	val = reflect.Indirect(val)
	if val.Kind() != reflect.Slice {
		return newGqlError("Type must be a pointer to a slice when calling Pluck")
	}
	t, _, isSliceOfPointers := getTypeInfo(i, val)
	cm := ColumnMap{col: ColumnData{ColumnName: col, FieldName: col, GoType: t}}
	if sql, err = me.Select(col).Sql(); err != nil {
		return err
	}
	if results, err = me.database.SelectIntoMap(cm, sql); err != nil {
		return err
	}
	if len(results) > 0 {
		for _, result := range results {
			row := reflect.ValueOf(result[col])
			if isSliceOfPointers {
				val.Set(reflect.Append(val, row.Addr()))
			} else {
				val.Set(reflect.Append(val, reflect.Indirect(row)))
			}
		}
	}
	return nil
}

func (me Dataset) Update(i interface{}) (int64, error) {
	sql, err := me.UpdateSql(i)
	if err != nil {
		return 0, err
	}
	return me.database.Update(sql)
}

func (me Dataset) Delete() (int64, error) {
	sql, err := me.DeleteSql()
	if err != nil {
		return 0, err
	}
	return me.database.Delete(sql)
}
