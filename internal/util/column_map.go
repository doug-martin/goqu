package util

import (
	"reflect"
	"sort"
	"strings"

	"github.com/doug-martin/goqu/v9/internal/tag"
)

type (
	ColumnData struct {
		ColumnName     string
		FieldIndex     []int
		ShouldInsert   bool
		ShouldUpdate   bool
		DefaultIfEmpty bool
		OmitNil        bool
		OmitEmpty      bool
		GoType         reflect.Type
	}
	ColumnMap map[string]ColumnData
)

func newColumnMap(t reflect.Type, fieldIndex []int, prefixes []string) ColumnMap {
	cm, n := ColumnMap{}, t.NumField()
	var subColMaps []ColumnMap
	for i := 0; i < n; i++ {
		f := t.Field(i)
		if f.Anonymous && (f.Type.Kind() == reflect.Struct || f.Type.Kind() == reflect.Ptr) {
			goquTag := tag.New("db", f.Tag)
			if !goquTag.Contains("-") {
				subCm := getStructColumnMap(&f, fieldIndex, goquTag.Values(), prefixes)
				subColMaps = append(subColMaps, subCm.inheritSkipTags(tag.New("goqu", f.Tag)))
			}
		} else if f.PkgPath == "" {
			dbTag := tag.New("db", f.Tag)
			// if PkgPath is empty then it is an exported field
			columnName := getColumnName(&f, dbTag)
			if !shouldIgnoreField(dbTag) {
				if !implementsScanner(f.Type) {
					subCm := getStructColumnMap(&f, fieldIndex, []string{columnName}, prefixes)
					if len(subCm) != 0 {
						subColMaps = append(subColMaps, subCm.inheritSkipTags(tag.New("goqu", f.Tag)))
						continue
					}
				}
				goquTag := tag.New("goqu", f.Tag)
				columnName = strings.Join(append(prefixes, columnName), ".")
				cm[columnName] = newColumnData(&f, columnName, fieldIndex, goquTag)
			}
		}
	}
	return cm.Merge(subColMaps)
}

func (cm ColumnMap) Cols() []string {
	structCols := make([]string, 0, len(cm))
	for key := range cm {
		structCols = append(structCols, key)
	}
	sort.Strings(structCols)
	return structCols
}

// inheritSkipTags applies the skipinsert and skipupdate options of a struct field to every column that was
// flattened out of that field, so that e.g. `goqu:"skipinsert"` on a nested struct skips all of its columns.
func (cm ColumnMap) inheritSkipTags(goquTag tag.Options) ColumnMap {
	skipInsert, skipUpdate := goquTag.Contains(skipInsertTagName), goquTag.Contains(skipUpdateTagName)
	if !skipInsert && !skipUpdate {
		return cm
	}
	for key, val := range cm {
		if skipInsert {
			val.ShouldInsert = false
		}
		if skipUpdate {
			val.ShouldUpdate = false
		}
		cm[key] = val
	}
	return cm
}

func (cm ColumnMap) Merge(colMaps []ColumnMap) ColumnMap {
	for _, subCm := range colMaps {
		for key, val := range subCm {
			if _, ok := cm[key]; !ok {
				cm[key] = val
			}
		}
	}
	return cm
}

func implementsScanner(t reflect.Type) bool {
	if IsPointer(t.Kind()) {
		t = t.Elem()
	}
	if reflect.PointerTo(t).Implements(scannerType) {
		return true
	}
	if !IsStruct(t.Kind()) {
		return true
	}

	return false
}

func newColumnData(f *reflect.StructField, columnName string, fieldIndex []int, goquTag tag.Options) ColumnData {
	return ColumnData{
		ColumnName:     columnName,
		ShouldInsert:   !goquTag.Contains(skipInsertTagName),
		ShouldUpdate:   !goquTag.Contains(skipUpdateTagName),
		DefaultIfEmpty: goquTag.Contains(defaultIfEmptyTagName),
		OmitNil:        goquTag.Contains(omitNilTagName),
		OmitEmpty:      goquTag.Contains(omitEmptyTagName),
		FieldIndex:     concatFieldIndexes(fieldIndex, f.Index),
		GoType:         f.Type,
	}
}

func getStructColumnMap(f *reflect.StructField, fieldIndex []int, fieldNames, prefixes []string) ColumnMap {
	subFieldIndexes := concatFieldIndexes(fieldIndex, f.Index)
	subPrefixes := prefixes
	subPrefixes = append(subPrefixes, fieldNames...)
	if f.Type.Kind() == reflect.Ptr {
		return newColumnMap(f.Type.Elem(), subFieldIndexes, subPrefixes)
	}
	return newColumnMap(f.Type, subFieldIndexes, subPrefixes)
}

func getColumnName(f *reflect.StructField, dbTag tag.Options) string {
	if dbTag.IsEmpty() {
		return columnRenameFunction(f.Name)
	}
	return dbTag.Values()[0]
}

func shouldIgnoreField(dbTag tag.Options) bool {
	if dbTag.Equals("-") {
		return true
	} else if dbTag.IsEmpty() && ignoreUntaggedFields {
		return true
	}

	return false
}

// safely concat two fieldIndex slices into one.
func concatFieldIndexes(fieldIndexPath, fieldIndex []int) []int {
	fieldIndexes := make([]int, 0, len(fieldIndexPath)+len(fieldIndex))
	fieldIndexes = append(fieldIndexes, fieldIndexPath...)
	return append(fieldIndexes, fieldIndex...)
}
