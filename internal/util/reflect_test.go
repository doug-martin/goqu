package util_test

import (
	"database/sql"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/doug-martin/goqu/v9/internal/util"
	"github.com/stretchr/testify/suite"
)

var (
	uints = []interface{}{
		uint(10),
		uint8(10),
		uint16(10),
		uint32(10),
		uint64(10),
	}
	ints = []interface{}{
		int(10),
		int8(10),
		int16(10),
		int32(10),
		int64(10),
	}
	floats = []interface{}{
		float32(3.14),
		float64(3.14),
	}
	strs = []interface{}{
		"abc",
		"",
	}
	bools = []interface{}{
		true,
		false,
	}
	structs = []interface{}{
		sql.NullString{},
	}
	invalids = []interface{}{
		nil,
	}
	pointers = []interface{}{
		&sql.NullString{},
	}
)

type (
	TestInterface interface {
		A() string
	}
	TestInterfaceImpl struct {
		str string
	}
	TestStruct struct {
		arr  [0]string
		slc  []string
		mp   map[string]interface{}
		str  string
		bl   bool
		i    int
		i8   int8
		i16  int16
		i32  int32
		i64  int64
		ui   uint
		ui8  uint8
		ui16 uint16
		ui32 uint32
		ui64 uint64
		f32  float32
		f64  float64
		intr TestInterface
		ptr  *sql.NullString
	}
)

func (t TestInterfaceImpl) A() string {
	return t.str
}

type reflectTest struct {
	suite.Suite
}

func (rt *reflectTest) TestIsUint() {
	for _, v := range uints {
		rt.True(util.IsUint(reflect.ValueOf(v).Kind()))
	}

	for _, v := range ints {
		rt.False(util.IsUint(reflect.ValueOf(v).Kind()))
	}
	for _, v := range floats {
		rt.False(util.IsUint(reflect.ValueOf(v).Kind()))
	}
	for _, v := range strs {
		rt.False(util.IsUint(reflect.ValueOf(v).Kind()))
	}
	for _, v := range bools {
		rt.False(util.IsUint(reflect.ValueOf(v).Kind()))
	}
	for _, v := range structs {
		rt.False(util.IsUint(reflect.ValueOf(v).Kind()))
	}
	for _, v := range invalids {
		rt.False(util.IsUint(reflect.ValueOf(v).Kind()))
	}
	for _, v := range pointers {
		rt.False(util.IsUint(reflect.ValueOf(v).Kind()))
	}
}

func (rt *reflectTest) TestIsInt() {
	for _, v := range ints {
		rt.True(util.IsInt(reflect.ValueOf(v).Kind()))
	}

	for _, v := range uints {
		rt.False(util.IsInt(reflect.ValueOf(v).Kind()))
	}
	for _, v := range floats {
		rt.False(util.IsInt(reflect.ValueOf(v).Kind()))
	}
	for _, v := range strs {
		rt.False(util.IsInt(reflect.ValueOf(v).Kind()))
	}
	for _, v := range bools {
		rt.False(util.IsInt(reflect.ValueOf(v).Kind()))
	}
	for _, v := range structs {
		rt.False(util.IsInt(reflect.ValueOf(v).Kind()))
	}
	for _, v := range invalids {
		rt.False(util.IsInt(reflect.ValueOf(v).Kind()))
	}
	for _, v := range pointers {
		rt.False(util.IsInt(reflect.ValueOf(v).Kind()))
	}
}

func (rt *reflectTest) TestIsFloat() {
	for _, v := range floats {
		rt.True(util.IsFloat(reflect.ValueOf(v).Kind()))
	}

	for _, v := range uints {
		rt.False(util.IsFloat(reflect.ValueOf(v).Kind()))
	}
	for _, v := range ints {
		rt.False(util.IsFloat(reflect.ValueOf(v).Kind()))
	}
	for _, v := range strs {
		rt.False(util.IsFloat(reflect.ValueOf(v).Kind()))
	}
	for _, v := range bools {
		rt.False(util.IsFloat(reflect.ValueOf(v).Kind()))
	}
	for _, v := range structs {
		rt.False(util.IsFloat(reflect.ValueOf(v).Kind()))
	}
	for _, v := range invalids {
		rt.False(util.IsFloat(reflect.ValueOf(v).Kind()))
	}
	for _, v := range pointers {
		rt.False(util.IsFloat(reflect.ValueOf(v).Kind()))
	}
}

func (rt *reflectTest) TestIsString() {
	for _, v := range strs {
		rt.True(util.IsString(reflect.ValueOf(v).Kind()))
	}

	for _, v := range uints {
		rt.False(util.IsString(reflect.ValueOf(v).Kind()))
	}
	for _, v := range ints {
		rt.False(util.IsString(reflect.ValueOf(v).Kind()))
	}
	for _, v := range floats {
		rt.False(util.IsString(reflect.ValueOf(v).Kind()))
	}
	for _, v := range bools {
		rt.False(util.IsString(reflect.ValueOf(v).Kind()))
	}
	for _, v := range structs {
		rt.False(util.IsString(reflect.ValueOf(v).Kind()))
	}
	for _, v := range invalids {
		rt.False(util.IsString(reflect.ValueOf(v).Kind()))
	}
	for _, v := range pointers {
		rt.False(util.IsString(reflect.ValueOf(v).Kind()))
	}
}

func (rt *reflectTest) TestIsBool() {
	for _, v := range bools {
		rt.True(util.IsBool(reflect.ValueOf(v).Kind()))
	}

	for _, v := range uints {
		rt.False(util.IsBool(reflect.ValueOf(v).Kind()))
	}
	for _, v := range ints {
		rt.False(util.IsBool(reflect.ValueOf(v).Kind()))
	}
	for _, v := range floats {
		rt.False(util.IsBool(reflect.ValueOf(v).Kind()))
	}
	for _, v := range strs {
		rt.False(util.IsBool(reflect.ValueOf(v).Kind()))
	}
	for _, v := range structs {
		rt.False(util.IsBool(reflect.ValueOf(v).Kind()))
	}
	for _, v := range invalids {
		rt.False(util.IsBool(reflect.ValueOf(v).Kind()))
	}
	for _, v := range pointers {
		rt.False(util.IsBool(reflect.ValueOf(v).Kind()))
	}
}

func (rt *reflectTest) TestIsStruct() {
	for _, v := range structs {
		rt.True(util.IsStruct(reflect.ValueOf(v).Kind()))
	}

	for _, v := range uints {
		rt.False(util.IsStruct(reflect.ValueOf(v).Kind()))
	}
	for _, v := range ints {
		rt.False(util.IsStruct(reflect.ValueOf(v).Kind()))
	}
	for _, v := range floats {
		rt.False(util.IsStruct(reflect.ValueOf(v).Kind()))
	}
	for _, v := range bools {
		rt.False(util.IsStruct(reflect.ValueOf(v).Kind()))
	}
	for _, v := range strs {
		rt.False(util.IsStruct(reflect.ValueOf(v).Kind()))
	}
	for _, v := range invalids {
		rt.False(util.IsStruct(reflect.ValueOf(v).Kind()))
	}
	for _, v := range pointers {
		rt.False(util.IsStruct(reflect.ValueOf(v).Kind()))
	}
}

func (rt *reflectTest) TestIsSlice() {
	rt.True(util.IsSlice(reflect.ValueOf(uints).Kind()))
	rt.True(util.IsSlice(reflect.ValueOf(ints).Kind()))
	rt.True(util.IsSlice(reflect.ValueOf(floats).Kind()))
	rt.True(util.IsSlice(reflect.ValueOf(structs).Kind()))

	rt.False(util.IsSlice(reflect.ValueOf(structs[0]).Kind()))
}

func (rt *reflectTest) TestIsInvalid() {
	for _, v := range invalids {
		rt.True(util.IsInvalid(reflect.ValueOf(v).Kind()))
	}

	for _, v := range uints {
		rt.False(util.IsInvalid(reflect.ValueOf(v).Kind()))
	}
	for _, v := range ints {
		rt.False(util.IsInvalid(reflect.ValueOf(v).Kind()))
	}
	for _, v := range floats {
		rt.False(util.IsInvalid(reflect.ValueOf(v).Kind()))
	}
	for _, v := range bools {
		rt.False(util.IsInvalid(reflect.ValueOf(v).Kind()))
	}
	for _, v := range strs {
		rt.False(util.IsInvalid(reflect.ValueOf(v).Kind()))
	}
	for _, v := range structs {
		rt.False(util.IsInvalid(reflect.ValueOf(v).Kind()))
	}
	for _, v := range pointers {
		rt.False(util.IsInvalid(reflect.ValueOf(v).Kind()))
	}
}

func (rt *reflectTest) TestIsPointer() {
	for _, v := range pointers {
		rt.True(util.IsPointer(reflect.ValueOf(v).Kind()))
	}

	for _, v := range uints {
		rt.False(util.IsPointer(reflect.ValueOf(v).Kind()))
	}
	for _, v := range ints {
		rt.False(util.IsPointer(reflect.ValueOf(v).Kind()))
	}
	for _, v := range floats {
		rt.False(util.IsPointer(reflect.ValueOf(v).Kind()))
	}
	for _, v := range bools {
		rt.False(util.IsPointer(reflect.ValueOf(v).Kind()))
	}
	for _, v := range strs {
		rt.False(util.IsPointer(reflect.ValueOf(v).Kind()))
	}
	for _, v := range structs {
		rt.False(util.IsPointer(reflect.ValueOf(v).Kind()))
	}
	for _, v := range invalids {
		rt.False(util.IsPointer(reflect.ValueOf(v).Kind()))
	}
}

func (rt *reflectTest) TestIsEmptyValue_emptyValues() {
	ts := TestStruct{}
	rt.True(util.IsEmptyValue(reflect.ValueOf(ts.arr)))
	rt.True(util.IsEmptyValue(reflect.ValueOf(ts.slc)))
	rt.True(util.IsEmptyValue(reflect.ValueOf(ts.mp)))
	rt.True(util.IsEmptyValue(reflect.ValueOf(ts.str)))
	rt.True(util.IsEmptyValue(reflect.ValueOf(ts.bl)))
	rt.True(util.IsEmptyValue(reflect.ValueOf(ts.i)))
	rt.True(util.IsEmptyValue(reflect.ValueOf(ts.i8)))
	rt.True(util.IsEmptyValue(reflect.ValueOf(ts.i16)))
	rt.True(util.IsEmptyValue(reflect.ValueOf(ts.i32)))
	rt.True(util.IsEmptyValue(reflect.ValueOf(ts.i64)))
	rt.True(util.IsEmptyValue(reflect.ValueOf(ts.ui)))
	rt.True(util.IsEmptyValue(reflect.ValueOf(ts.ui8)))
	rt.True(util.IsEmptyValue(reflect.ValueOf(ts.ui16)))
	rt.True(util.IsEmptyValue(reflect.ValueOf(ts.ui32)))
	rt.True(util.IsEmptyValue(reflect.ValueOf(ts.ui64)))
	rt.True(util.IsEmptyValue(reflect.ValueOf(ts.f32)))
	rt.True(util.IsEmptyValue(reflect.ValueOf(ts.f64)))
	rt.True(util.IsEmptyValue(reflect.ValueOf(ts.intr)))
	rt.True(util.IsEmptyValue(reflect.ValueOf(ts.ptr)))
	rt.True(util.IsEmptyValue(reflect.ValueOf(ts)))
	rt.True(util.IsNil(reflect.ValueOf(nil)))
}

func (rt *reflectTest) TestIsEmptyValue_validValues() {
	ts := TestStruct{intr: TestInterfaceImpl{"hello"}}
	rt.False(util.IsEmptyValue(reflect.ValueOf([1]string{"a"})))
	rt.False(util.IsEmptyValue(reflect.ValueOf([]string{"a"})))
	rt.False(util.IsEmptyValue(reflect.ValueOf(map[string]interface{}{"a": true})))
	rt.False(util.IsEmptyValue(reflect.ValueOf("str")))
	rt.False(util.IsEmptyValue(reflect.ValueOf(true)))
	rt.False(util.IsEmptyValue(reflect.ValueOf(int(1))))
	rt.False(util.IsEmptyValue(reflect.ValueOf(int8(1))))
	rt.False(util.IsEmptyValue(reflect.ValueOf(int16(1))))
	rt.False(util.IsEmptyValue(reflect.ValueOf(int32(1))))
	rt.False(util.IsEmptyValue(reflect.ValueOf(int64(1))))
	rt.False(util.IsEmptyValue(reflect.ValueOf(uint(1))))
	rt.False(util.IsEmptyValue(reflect.ValueOf(uint8(1))))
	rt.False(util.IsEmptyValue(reflect.ValueOf(uint16(1))))
	rt.False(util.IsEmptyValue(reflect.ValueOf(uint32(1))))
	rt.False(util.IsEmptyValue(reflect.ValueOf(uint64(1))))
	rt.False(util.IsEmptyValue(reflect.ValueOf(float32(0.1))))
	rt.False(util.IsEmptyValue(reflect.ValueOf(float64(0.2))))
	rt.False(util.IsEmptyValue(reflect.ValueOf(ts.intr)))
	rt.False(util.IsEmptyValue(reflect.ValueOf(&TestStruct{str: "a"})))
	rt.False(util.IsEmptyValue(reflect.ValueOf(ts)))
}

func (rt *reflectTest) TestIsNil() {
	ts := TestStruct{}
	rt.False(util.IsNil(reflect.ValueOf(ts.arr)))
	rt.True(util.IsNil(reflect.ValueOf(ts.slc)))
	rt.False(util.IsEmptyValue(reflect.ValueOf([]string{"a"})))
	rt.True(util.IsNil(reflect.ValueOf(ts.mp)))
	rt.False(util.IsEmptyValue(reflect.ValueOf(map[string]interface{}{"a": true})))
	rt.False(util.IsNil(reflect.ValueOf(ts.str)))
	rt.False(util.IsNil(reflect.ValueOf(ts.bl)))
	rt.False(util.IsNil(reflect.ValueOf(ts.i)))
	rt.False(util.IsNil(reflect.ValueOf(ts.i8)))
	rt.False(util.IsNil(reflect.ValueOf(ts.i16)))
	rt.False(util.IsNil(reflect.ValueOf(ts.i32)))
	rt.False(util.IsNil(reflect.ValueOf(ts.i64)))
	rt.False(util.IsNil(reflect.ValueOf(ts.ui)))
	rt.False(util.IsNil(reflect.ValueOf(ts.ui8)))
	rt.False(util.IsNil(reflect.ValueOf(ts.ui16)))
	rt.False(util.IsNil(reflect.ValueOf(ts.ui32)))
	rt.False(util.IsNil(reflect.ValueOf(ts.ui64)))
	rt.False(util.IsNil(reflect.ValueOf(ts.f32)))
	rt.False(util.IsNil(reflect.ValueOf(ts.f64)))
	rt.True(util.IsNil(reflect.ValueOf(ts.intr)))
	rt.True(util.IsNil(reflect.ValueOf(ts.ptr)))
	rt.False(util.IsNil(reflect.ValueOf(ts)))
	rt.True(util.IsNil(reflect.ValueOf(nil)))
}

func (rt *reflectTest) TestColumnRename() {
	// different key names are used each time to circumvent the caching that happens
	// it seems like a solid assumption that when people use this feature,
	// they would simply set a renaming function once at startup,
	// and not change between requests like this
	lowerAnon := struct {
		FirstLower string
		LastLower  string
	}{}
	lowerColumnMap, lowerErr := util.GetColumnMap(&lowerAnon)
	rt.NoError(lowerErr)

	lowerKeys := make([]string, 0, len(lowerColumnMap))
	for key := range lowerColumnMap {
		lowerKeys = append(lowerKeys, key)
	}
	rt.Contains(lowerKeys, "firstlower")
	rt.Contains(lowerKeys, "lastlower")

	// changing rename function
	util.SetColumnRenameFunction(strings.ToUpper)

	upperAnon := struct {
		FirstUpper string
		LastUpper  string
	}{}
	upperColumnMap, upperErr := util.GetColumnMap(&upperAnon)
	rt.NoError(upperErr)

	upperKeys := make([]string, 0, len(upperColumnMap))
	for key := range upperColumnMap {
		upperKeys = append(upperKeys, key)
	}
	rt.Contains(upperKeys, "FIRSTUPPER")
	rt.Contains(upperKeys, "LASTUPPER")

	util.SetColumnRenameFunction(util.DefaultColumnRenameFunction)
}

func (rt *reflectTest) TestParallelGetColumnMap() {
	type item struct {
		id   uint
		name string
	}

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		i := item{id: 1, name: "bob"}
		m, err := util.GetColumnMap(i)
		rt.NoError(err)
		rt.NotNil(m)
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		i := item{id: 2, name: "sally"}
		m, err := util.GetColumnMap(i)
		rt.NoError(err)
		rt.NotNil(m)
		wg.Done()
	}()

	wg.Wait()
}

func (rt *reflectTest) TestAssignStructVals_withStruct() {
	type TestStruct struct {
		Str    string
		Int    int64
		Bool   bool
		Valuer sql.NullString
	}
	var ts TestStruct
	cm, err := util.GetColumnMap(&ts)
	rt.NoError(err)
	data := map[string]interface{}{
		"str":    "string",
		"int":    int64(10),
		"bool":   true,
		"valuer": sql.NullString{String: "null_str", Valid: true},
	}

	util.AssignStructVals(&ts, data, cm)
	rt.Equal(ts, TestStruct{
		Str:    "string",
		Int:    10,
		Bool:   true,
		Valuer: sql.NullString{String: "null_str", Valid: true},
	})
}

func (rt *reflectTest) TestAssignStructVals_withStructWithPointerVals() {
	type TestStruct struct {
		Str    string
		Int    int64
		Bool   bool
		Valuer *sql.NullString
	}
	var ts TestStruct
	cm, err := util.GetColumnMap(&ts)
	rt.NoError(err)
	ns := &sql.NullString{String: "null_str1", Valid: true}
	data := map[string]interface{}{
		"str":    "string",
		"int":    int64(10),
		"bool":   true,
		"valuer": &ns,
	}
	util.AssignStructVals(&ts, data, cm)
	rt.Equal(ts, TestStruct{
		Str:    "string",
		Int:    10,
		Bool:   true,
		Valuer: ns,
	})
}

func (rt *reflectTest) TestAssignStructVals_withStructWithEmbeddedStruct() {
	type EmbeddedStruct struct {
		Str string
	}
	type TestStruct struct {
		EmbeddedStruct
		Int    int64
		Bool   bool
		Valuer *sql.NullString
	}
	var ts TestStruct
	cm, err := util.GetColumnMap(&ts)
	rt.NoError(err)
	ns := &sql.NullString{String: "null_str1", Valid: true}
	data := map[string]interface{}{
		"str":    "string",
		"int":    int64(10),
		"bool":   true,
		"valuer": &ns,
	}
	util.AssignStructVals(&ts, data, cm)
	rt.Equal(ts, TestStruct{
		EmbeddedStruct: EmbeddedStruct{Str: "string"},
		Int:            10,
		Bool:           true,
		Valuer:         ns,
	})
}

func (rt *reflectTest) TestAssignStructVals_withStructWithEmbeddedStructPointer() {
	type EmbeddedStruct struct {
		Str string
	}
	type TestStruct struct {
		*EmbeddedStruct
		Int    int64
		Bool   bool
		Valuer *sql.NullString
	}
	var ts TestStruct
	cm, err := util.GetColumnMap(&ts)
	rt.NoError(err)
	ns := &sql.NullString{String: "null_str1", Valid: true}
	data := map[string]interface{}{
		"str":    "string",
		"int":    int64(10),
		"bool":   true,
		"valuer": &ns,
	}
	util.AssignStructVals(&ts, data, cm)
	rt.Equal(ts, TestStruct{
		EmbeddedStruct: &EmbeddedStruct{Str: "string"},
		Int:            10,
		Bool:           true,
		Valuer:         ns,
	})
}

func (rt *reflectTest) TestAssignStructVals_withStructWithTaggedEmbeddedStruct() {
	type EmbeddedStruct struct {
		Str string
	}
	type TestStruct struct {
		EmbeddedStruct `db:"embedded"`
		Int            int64
		Bool           bool
		Valuer         *sql.NullString
	}
	var ts TestStruct
	cm, err := util.GetColumnMap(&ts)
	rt.NoError(err)
	ns := &sql.NullString{String: "null_str1", Valid: true}
	data := map[string]interface{}{
		"embedded.str": "string",
		"int":          int64(10),
		"bool":         true,
		"valuer":       &ns,
	}
	util.AssignStructVals(&ts, data, cm)
	rt.Equal(ts, TestStruct{
		EmbeddedStruct: EmbeddedStruct{Str: "string"},
		Int:            10,
		Bool:           true,
		Valuer:         ns,
	})
}

func (rt *reflectTest) TestAssignStructVals_withStructWithTaggedEmbeddedPointer() {
	type EmbeddedStruct struct {
		Str string
	}
	type TestStruct struct {
		*EmbeddedStruct `db:"embedded"`
		Int             int64
		Bool            bool
		Valuer          *sql.NullString
	}
	var ts TestStruct
	cm, err := util.GetColumnMap(&ts)
	rt.NoError(err)
	ns := &sql.NullString{String: "null_str1", Valid: true}
	data := map[string]interface{}{
		"embedded.str": "string",
		"int":          int64(10),
		"bool":         true,
		"valuer":       &ns,
	}
	util.AssignStructVals(&ts, data, cm)
	rt.Equal(ts, TestStruct{
		EmbeddedStruct: &EmbeddedStruct{Str: "string"},
		Int:            10,
		Bool:           true,
		Valuer:         ns,
	})
}

func (rt *reflectTest) TestAssignStructVals_withStructWithTaggedStructField() {
	type EmbeddedStruct struct {
		Str string
	}
	type TestStruct struct {
		Embedded EmbeddedStruct `db:"embedded"`
		Int      int64
		Bool     bool
		Valuer   *sql.NullString
	}
	var ts TestStruct
	cm, err := util.GetColumnMap(&ts)
	rt.NoError(err)
	ns := &sql.NullString{String: "null_str1", Valid: true}
	data := map[string]interface{}{
		"embedded.str": "string",
		"int":          int64(10),
		"bool":         true,
		"valuer":       &ns,
	}
	util.AssignStructVals(&ts, data, cm)
	rt.Equal(ts, TestStruct{
		Embedded: EmbeddedStruct{Str: "string"},
		Int:      10,
		Bool:     true,
		Valuer:   ns,
	})
}

func (rt *reflectTest) TestAssignStructVals_withStructWithTaggedPointerField() {
	type EmbeddedStruct struct {
		Str string
	}
	type TestStruct struct {
		Embedded *EmbeddedStruct `db:"embedded"`
		Int      int64
		Bool     bool
		Valuer   *sql.NullString
	}
	var ts TestStruct
	cm, err := util.GetColumnMap(&ts)
	rt.NoError(err)
	ns := &sql.NullString{String: "null_str1", Valid: true}
	data := map[string]interface{}{
		"embedded.str": "string",
		"int":          int64(10),
		"bool":         true,
		"valuer":       &ns,
	}
	util.AssignStructVals(&ts, data, cm)
	rt.Equal(ts, TestStruct{
		Embedded: &EmbeddedStruct{Str: "string"},
		Int:      10,
		Bool:     true,
		Valuer:   ns,
	})
}

func (rt *reflectTest) TestGetColumnMap_withStruct() {
	type TestStruct struct {
		Str    string
		Int    int64
		Bool   bool
		Valuer *sql.NullString
	}
	var ts TestStruct
	cm, err := util.GetColumnMap(&ts)
	rt.NoError(err)
	rt.Equal(util.ColumnMap{
		"str":    {ColumnName: "str", FieldIndex: []int{0}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf("")},
		"int":    {ColumnName: "int", FieldIndex: []int{1}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(int64(1))},
		"bool":   {ColumnName: "bool", FieldIndex: []int{2}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(true)},
		"valuer": {ColumnName: "valuer", FieldIndex: []int{3}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(&sql.NullString{})},
	}, cm)
}

func (rt *reflectTest) TestGetColumnMap_withStructGoquTags() {
	type TestStruct struct {
		Str       string `goqu:"skipinsert,skipupdate"`
		Int       int64  `goqu:"skipinsert"`
		Bool      bool   `goqu:"skipupdate"`
		Empty     bool   `goqu:"defaultifempty"`
		OmitNil   bool   `goqu:"omitnil"`
		OmitEmpty bool   `goqu:"omitempty"`
		Valuer    *sql.NullString
	}
	var ts TestStruct
	cm, err := util.GetColumnMap(&ts)
	rt.NoError(err)
	rt.Equal(util.ColumnMap{
		"str":  {ColumnName: "str", FieldIndex: []int{0}, ShouldInsert: false, ShouldUpdate: false, GoType: reflect.TypeOf("")},
		"int":  {ColumnName: "int", FieldIndex: []int{1}, ShouldInsert: false, ShouldUpdate: true, GoType: reflect.TypeOf(int64(1))},
		"bool": {ColumnName: "bool", FieldIndex: []int{2}, ShouldInsert: true, ShouldUpdate: false, GoType: reflect.TypeOf(true)},
		"empty": {
			ColumnName:     "empty",
			FieldIndex:     []int{3},
			ShouldInsert:   true,
			ShouldUpdate:   true,
			DefaultIfEmpty: true,
			GoType:         reflect.TypeOf(true),
		},
		"omitnil": {
			ColumnName:   "omitnil",
			FieldIndex:   []int{4},
			ShouldInsert: true,
			ShouldUpdate: true,
			OmitNil:      true,
			GoType:       reflect.TypeOf(true),
		},
		"omitempty": {
			ColumnName:   "omitempty",
			FieldIndex:   []int{5},
			ShouldInsert: true,
			ShouldUpdate: true,
			OmitEmpty:    true,
			GoType:       reflect.TypeOf(true),
		},
		"valuer": {ColumnName: "valuer", FieldIndex: []int{6}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(&sql.NullString{})},
	}, cm)
}

func (rt *reflectTest) TestGetColumnMap_withStructWithIgnoreUntagged() {
	defer util.SetIgnoreUntaggedFields(false)
	util.SetIgnoreUntaggedFields(true)

	type EmbeddedStruct struct {
		Float float64 `db:"f"`
		Rune  rune    // Ignored
	}

	type TestStruct struct {
		EmbeddedStruct
		Str  string `db:"s"`
		Int  int64  `db:"i"`
		Bool bool   // Ignored
	}
	var ts TestStruct
	cm, err := util.GetColumnMap(&ts)
	rt.NoError(err)
	rt.Equal(util.ColumnMap{
		"f": {ColumnName: "f", FieldIndex: []int{0, 0}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(float64(1))},
		"s": {ColumnName: "s", FieldIndex: []int{1}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf("")},
		"i": {ColumnName: "i", FieldIndex: []int{2}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(int64(1))},
	}, cm)
}

func (rt *reflectTest) TestGetColumnMap_withStructWithTag() {
	type TestStruct struct {
		Str     string          `db:"s"`
		Int     int64           `db:"i"`
		Bool    bool            `db:"b"`
		Valuer  *sql.NullString `db:"v"`
		Ignored string          `db:"-"`
	}
	var ts TestStruct
	cm, err := util.GetColumnMap(&ts)
	rt.NoError(err)
	rt.Equal(util.ColumnMap{
		"s": {ColumnName: "s", FieldIndex: []int{0}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf("")},
		"i": {ColumnName: "i", FieldIndex: []int{1}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(int64(1))},
		"b": {ColumnName: "b", FieldIndex: []int{2}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(true)},
		"v": {ColumnName: "v", FieldIndex: []int{3}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(&sql.NullString{})},
	}, cm)
}

func (rt *reflectTest) TestGetColumnMap_withStructWithTagAndGoquTag() {
	type TestStruct struct {
		Str    string          `db:"s" goqu:"skipinsert,skipupdate"`
		Int    int64           `db:"i" goqu:"skipinsert"`
		Bool   bool            `db:"b" goqu:"skipupdate"`
		Valuer *sql.NullString `db:"v"`
	}
	var ts TestStruct
	cm, err := util.GetColumnMap(&ts)
	rt.NoError(err)
	rt.Equal(util.ColumnMap{
		"s": {ColumnName: "s", FieldIndex: []int{0}, ShouldInsert: false, ShouldUpdate: false, GoType: reflect.TypeOf("")},
		"i": {ColumnName: "i", FieldIndex: []int{1}, ShouldInsert: false, ShouldUpdate: true, GoType: reflect.TypeOf(int64(1))},
		"b": {ColumnName: "b", FieldIndex: []int{2}, ShouldInsert: true, ShouldUpdate: false, GoType: reflect.TypeOf(true)},
		"v": {ColumnName: "v", FieldIndex: []int{3}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(&sql.NullString{})},
	}, cm)
}

func (rt *reflectTest) TestGetColumnMap_withStructWithTransientFields() {
	type TestStruct struct {
		Str    string
		Int    int64
		Bool   bool
		Valuer *sql.NullString `db:"-"`
	}
	var ts TestStruct
	cm, err := util.GetColumnMap(&ts)
	rt.NoError(err)
	rt.Equal(util.ColumnMap{
		"str":  {ColumnName: "str", FieldIndex: []int{0}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf("")},
		"int":  {ColumnName: "int", FieldIndex: []int{1}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(int64(1))},
		"bool": {ColumnName: "bool", FieldIndex: []int{2}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(true)},
	}, cm)
}

func (rt *reflectTest) TestGetColumnMap_withSliceOfStructs() {
	type TestStruct struct {
		Str    string
		Int    int64
		Bool   bool
		Valuer *sql.NullString
	}
	var ts []TestStruct
	cm, err := util.GetColumnMap(&ts)
	rt.NoError(err)
	rt.Equal(util.ColumnMap{
		"str":    {ColumnName: "str", FieldIndex: []int{0}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf("")},
		"int":    {ColumnName: "int", FieldIndex: []int{1}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(int64(1))},
		"bool":   {ColumnName: "bool", FieldIndex: []int{2}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(true)},
		"valuer": {ColumnName: "valuer", FieldIndex: []int{3}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(&sql.NullString{})},
	}, cm)
}

func (rt *reflectTest) TestGetColumnMap_withNonStruct() {
	var v int64
	_, err := util.GetColumnMap(&v)
	rt.EqualError(err, "goqu: cannot scan into this type: int64")
}

func (rt *reflectTest) TestGetColumnMap_withStructWithEmbeddedStruct() {
	type EmbeddedStruct struct {
		Str string
	}
	type TestStruct struct {
		EmbeddedStruct
		Int    int64
		Bool   bool
		Valuer *sql.NullString
	}
	var ts TestStruct
	cm, err := util.GetColumnMap(&ts)
	rt.NoError(err)
	rt.Equal(util.ColumnMap{
		"str":    {ColumnName: "str", FieldIndex: []int{0, 0}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf("")},
		"int":    {ColumnName: "int", FieldIndex: []int{1}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(int64(1))},
		"bool":   {ColumnName: "bool", FieldIndex: []int{2}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(true)},
		"valuer": {ColumnName: "valuer", FieldIndex: []int{3}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(&sql.NullString{})},
	}, cm)
}

func (rt *reflectTest) TestGetColumnMap_withStructWithEmbeddedStructPointer() {
	type EmbeddedStruct struct {
		Str string
	}
	type TestStruct struct {
		*EmbeddedStruct
		Int    int64
		Bool   bool
		Valuer *sql.NullString
	}
	var ts TestStruct
	cm, err := util.GetColumnMap(&ts)
	rt.NoError(err)
	rt.Equal(util.ColumnMap{
		"str":    {ColumnName: "str", FieldIndex: []int{0, 0}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf("")},
		"int":    {ColumnName: "int", FieldIndex: []int{1}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(int64(1))},
		"bool":   {ColumnName: "bool", FieldIndex: []int{2}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(true)},
		"valuer": {ColumnName: "valuer", FieldIndex: []int{3}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(&sql.NullString{})},
	}, cm)
}

func (rt *reflectTest) TestGetColumnMap_withIgnoredEmbeddedStruct() {
	type EmbeddedStruct struct {
		Str string
	}
	type TestStruct struct {
		EmbeddedStruct `db:"-"`
		Int            int64
		Bool           bool
		Valuer         *sql.NullString
	}
	var ts TestStruct
	cm, err := util.GetColumnMap(&ts)
	rt.NoError(err)
	rt.Equal(util.ColumnMap{
		"int":    {ColumnName: "int", FieldIndex: []int{1}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(int64(1))},
		"bool":   {ColumnName: "bool", FieldIndex: []int{2}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(true)},
		"valuer": {ColumnName: "valuer", FieldIndex: []int{3}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(&sql.NullString{})},
	}, cm)
}

func (rt *reflectTest) TestGetColumnMap_withIgnoredEmbeddedPointerStruct() {
	type EmbeddedStruct struct {
		Str string
	}
	type TestStruct struct {
		*EmbeddedStruct `db:"-"`
		Int             int64
		Bool            bool
		Valuer          *sql.NullString
	}
	var ts TestStruct
	cm, err := util.GetColumnMap(&ts)
	rt.NoError(err)
	rt.Equal(util.ColumnMap{
		"int":    {ColumnName: "int", FieldIndex: []int{1}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(int64(1))},
		"bool":   {ColumnName: "bool", FieldIndex: []int{2}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(true)},
		"valuer": {ColumnName: "valuer", FieldIndex: []int{3}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(&sql.NullString{})},
	}, cm)
}

func (rt *reflectTest) TestGetColumnMap_withPrivateFields() {
	type TestStruct struct {
		str    string //nolint:structcheck,unused // not used directly but needed for test
		Int    int64
		Bool   bool
		Valuer *sql.NullString
	}
	var ts TestStruct
	cm, err := util.GetColumnMap(&ts)
	rt.NoError(err)
	rt.Equal(util.ColumnMap{
		"int":    {ColumnName: "int", FieldIndex: []int{1}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(int64(1))},
		"bool":   {ColumnName: "bool", FieldIndex: []int{2}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(true)},
		"valuer": {ColumnName: "valuer", FieldIndex: []int{3}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(&sql.NullString{})},
	}, cm)
}

func (rt *reflectTest) TestGetColumnMap_withPrivateEmbeddedFields() {
	type TestEmbedded struct {
		str string //nolint:structcheck,unused // not used directly but need for test
		Int int64
	}

	type TestStruct struct {
		TestEmbedded
		Bool   bool
		Valuer *sql.NullString
	}
	var ts TestStruct
	cm, err := util.GetColumnMap(&ts)
	rt.NoError(err)
	rt.Equal(util.ColumnMap{
		"int":    {ColumnName: "int", FieldIndex: []int{0, 1}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(int64(1))},
		"bool":   {ColumnName: "bool", FieldIndex: []int{1}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(true)},
		"valuer": {ColumnName: "valuer", FieldIndex: []int{2}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(&sql.NullString{})},
	}, cm)
}

func (rt *reflectTest) TestGetColumnMap_withEmbeddedTaggedStruct() {
	type TestEmbedded struct {
		Bool   bool
		Valuer *sql.NullString
	}

	type TestStruct struct {
		TestEmbedded `db:"test_embedded"`
		Bool         bool
		Valuer       *sql.NullString
	}
	var ts TestStruct
	cm, err := util.GetColumnMap(&ts)
	rt.NoError(err)
	rt.Equal(util.ColumnMap{
		"test_embedded.bool": {
			ColumnName:   "test_embedded.bool",
			FieldIndex:   []int{0, 0},
			ShouldInsert: true,
			ShouldUpdate: true,
			GoType:       reflect.TypeOf(true),
		},
		"test_embedded.valuer": {
			ColumnName:   "test_embedded.valuer",
			FieldIndex:   []int{0, 1},
			ShouldInsert: true,
			ShouldUpdate: true,
			GoType:       reflect.TypeOf(&sql.NullString{}),
		},
		"bool": {
			ColumnName:   "bool",
			FieldIndex:   []int{1},
			ShouldInsert: true,
			ShouldUpdate: true,
			GoType:       reflect.TypeOf(true),
		},
		"valuer": {
			ColumnName:   "valuer",
			FieldIndex:   []int{2},
			ShouldInsert: true,
			ShouldUpdate: true,
			GoType:       reflect.TypeOf(&sql.NullString{}),
		},
	}, cm)
}

func (rt *reflectTest) TestGetColumnMap_withEmbeddedTaggedStructPointer() {
	type TestEmbedded struct {
		Bool   bool
		Valuer *sql.NullString
	}

	type TestStruct struct {
		*TestEmbedded `db:"test_embedded"`
		Bool          bool
		Valuer        *sql.NullString
	}
	var ts TestStruct
	cm, err := util.GetColumnMap(&ts)
	rt.NoError(err)
	rt.Equal(util.ColumnMap{
		"test_embedded.bool": {
			ColumnName:   "test_embedded.bool",
			FieldIndex:   []int{0, 0},
			ShouldInsert: true,
			ShouldUpdate: true,
			GoType:       reflect.TypeOf(true),
		},
		"test_embedded.valuer": {
			ColumnName:   "test_embedded.valuer",
			FieldIndex:   []int{0, 1},
			ShouldInsert: true, ShouldUpdate: true,
			GoType: reflect.TypeOf(&sql.NullString{}),
		},
		"bool": {
			ColumnName:   "bool",
			FieldIndex:   []int{1},
			ShouldInsert: true,
			ShouldUpdate: true,
			GoType:       reflect.TypeOf(true),
		},
		"valuer": {
			ColumnName:   "valuer",
			FieldIndex:   []int{2},
			ShouldInsert: true,
			ShouldUpdate: true,
			GoType:       reflect.TypeOf(&sql.NullString{}),
		},
	}, cm)
}

func (rt *reflectTest) TestGetColumnMap_withTaggedStructField() {
	type TestEmbedded struct {
		Bool   bool
		Valuer *sql.NullString
	}

	type TestStruct struct {
		Embedded TestEmbedded `db:"test_embedded"`
		Bool     bool
		Valuer   *sql.NullString
	}
	var ts TestStruct
	cm, err := util.GetColumnMap(&ts)
	rt.NoError(err)
	rt.Equal(util.ColumnMap{
		"test_embedded.bool": {
			ColumnName:   "test_embedded.bool",
			FieldIndex:   []int{0, 0},
			ShouldInsert: true,
			ShouldUpdate: true,
			GoType:       reflect.TypeOf(true),
		},
		"test_embedded.valuer": {
			ColumnName:   "test_embedded.valuer",
			FieldIndex:   []int{0, 1},
			ShouldInsert: true,
			ShouldUpdate: true,
			GoType:       reflect.TypeOf(&sql.NullString{}),
		},
		"bool": {
			ColumnName:   "bool",
			FieldIndex:   []int{1},
			ShouldInsert: true,
			ShouldUpdate: true,
			GoType:       reflect.TypeOf(true),
		},
		"valuer": {
			ColumnName:   "valuer",
			FieldIndex:   []int{2},
			ShouldInsert: true,
			ShouldUpdate: true,
			GoType:       reflect.TypeOf(&sql.NullString{}),
		},
	}, cm)
}

func (rt *reflectTest) TestGetColumnMap_withTaggedStructPointerField() {
	type TestEmbedded struct {
		Bool   bool
		Valuer *sql.NullString
	}

	type TestStruct struct {
		Embedded *TestEmbedded `db:"test_embedded"`
		Bool     bool
		Valuer   *sql.NullString
	}
	var ts TestStruct
	cm, err := util.GetColumnMap(&ts)
	rt.NoError(err)
	rt.Equal(util.ColumnMap{
		"test_embedded.bool": {
			ColumnName:   "test_embedded.bool",
			FieldIndex:   []int{0, 0},
			ShouldInsert: true,
			ShouldUpdate: true,
			GoType:       reflect.TypeOf(true),
		},
		"test_embedded.valuer": {
			ColumnName:   "test_embedded.valuer",
			FieldIndex:   []int{0, 1},
			ShouldInsert: true,
			ShouldUpdate: true,
			GoType:       reflect.TypeOf(&sql.NullString{}),
		},
		"bool": {
			ColumnName:   "bool",
			FieldIndex:   []int{1},
			ShouldInsert: true,
			ShouldUpdate: true,
			GoType:       reflect.TypeOf(true),
		},
		"valuer": {
			ColumnName:   "valuer",
			FieldIndex:   []int{2},
			ShouldInsert: true,
			ShouldUpdate: true,
			GoType:       reflect.TypeOf(&sql.NullString{}),
		},
	}, cm)
}

func (rt *reflectTest) TestGetTypeInfo() {
	var a int64
	var b []int64
	var c []*time.Time

	t, k := util.GetTypeInfo(&a, reflect.ValueOf(a))
	rt.Equal(reflect.TypeOf(a), t)
	rt.Equal(reflect.Int64, k)

	t, k = util.GetTypeInfo(&b, reflect.ValueOf(a))
	rt.Equal(reflect.TypeOf(a), t)
	rt.Equal(reflect.Int64, k)

	t, k = util.GetTypeInfo(c, reflect.ValueOf(c))
	rt.Equal(reflect.TypeOf(time.Time{}), t)
	rt.Equal(reflect.Struct, k)
}

func (rt *reflectTest) TestSafeGetFieldByIndex() {
	type TestEmbedded struct {
		FieldA int
	}
	type TestEmbeddedPointerStruct struct {
		*TestEmbedded
		FieldB string
	}
	type TestEmbeddedStruct struct {
		TestEmbedded
		FieldB string
	}
	v := reflect.ValueOf(TestEmbeddedPointerStruct{})
	f, isAvailable := util.SafeGetFieldByIndex(v, []int{0, 0})
	rt.False(isAvailable)
	rt.False(f.IsValid())
	f, isAvailable = util.SafeGetFieldByIndex(v, []int{1})
	rt.True(isAvailable)
	rt.True(f.IsValid())
	rt.Equal(reflect.String, f.Type().Kind())
	f, isAvailable = util.SafeGetFieldByIndex(v, []int{})
	rt.True(isAvailable)
	rt.Equal(v, f)

	v = reflect.ValueOf(TestEmbeddedPointerStruct{TestEmbedded: &TestEmbedded{}})
	f, isAvailable = util.SafeGetFieldByIndex(v, []int{0, 0})
	rt.True(isAvailable)
	rt.True(f.IsValid())
	rt.Equal(reflect.Int, f.Type().Kind())
	f, isAvailable = util.SafeGetFieldByIndex(v, []int{1})
	rt.True(isAvailable)
	rt.True(f.IsValid())
	rt.Equal(reflect.String, f.Type().Kind())
	f, isAvailable = util.SafeGetFieldByIndex(v, []int{})
	rt.True(isAvailable)
	rt.Equal(v, f)

	v = reflect.ValueOf(TestEmbeddedStruct{})
	f, isAvailable = util.SafeGetFieldByIndex(v, []int{0, 0})
	rt.True(isAvailable)
	rt.True(f.IsValid())
	rt.Equal(reflect.Int, f.Type().Kind())
	f, isAvailable = util.SafeGetFieldByIndex(v, []int{1})
	rt.True(isAvailable)
	rt.True(f.IsValid())
	rt.Equal(reflect.String, f.Type().Kind())
	f, isAvailable = util.SafeGetFieldByIndex(v, []int{})
	rt.True(isAvailable)
	rt.Equal(v, f)

	v = reflect.ValueOf(TestEmbeddedStruct{TestEmbedded: TestEmbedded{}})
	f, isAvailable = util.SafeGetFieldByIndex(v, []int{0, 0})
	rt.True(isAvailable)
	rt.True(f.IsValid())
	f, isAvailable = util.SafeGetFieldByIndex(v, []int{1})
	rt.True(isAvailable)
	rt.True(f.IsValid())
	rt.Equal(reflect.String, f.Type().Kind())
	f, isAvailable = util.SafeGetFieldByIndex(v, []int{})
	rt.True(isAvailable)
	rt.Equal(v, f)
}

func (rt *reflectTest) TestSafeSetFieldByIndex() {
	type TestEmbedded struct {
		FieldA int
	}
	type TestEmbeddedPointerStruct struct {
		*TestEmbedded
		FieldB string
	}
	type TestEmbeddedStruct struct {
		TestEmbedded
		FieldB string
	}
	var teps TestEmbeddedPointerStruct
	v := reflect.ValueOf(&teps)
	f := util.SafeSetFieldByIndex(v, []int{}, nil)
	rt.Equal(TestEmbeddedPointerStruct{}, f.Interface())

	f = util.SafeSetFieldByIndex(v, []int{0, 0}, 1)
	rt.Equal(TestEmbeddedPointerStruct{
		TestEmbedded: &TestEmbedded{FieldA: 1},
	}, f.Interface())

	f = util.SafeSetFieldByIndex(v, []int{1}, "hello")
	rt.Equal(TestEmbeddedPointerStruct{
		TestEmbedded: &TestEmbedded{FieldA: 1},
		FieldB:       "hello",
	}, f.Interface())
	rt.Equal(TestEmbeddedPointerStruct{
		TestEmbedded: &TestEmbedded{FieldA: 1},
		FieldB:       "hello",
	}, teps)

	var tes TestEmbeddedStruct
	v = reflect.ValueOf(&tes)
	f = util.SafeSetFieldByIndex(v, []int{}, nil)
	rt.Equal(TestEmbeddedStruct{}, f.Interface())

	f = util.SafeSetFieldByIndex(v, []int{0, 0}, 1)
	rt.Equal(TestEmbeddedStruct{
		TestEmbedded: TestEmbedded{FieldA: 1},
	}, f.Interface())

	f = util.SafeSetFieldByIndex(v, []int{1}, "hello")
	rt.Equal(TestEmbeddedStruct{
		TestEmbedded: TestEmbedded{FieldA: 1},
		FieldB:       "hello",
	}, f.Interface())
	rt.Equal(TestEmbeddedStruct{
		TestEmbedded: TestEmbedded{FieldA: 1},
		FieldB:       "hello",
	}, tes)
}

func (rt *reflectTest) TestGetSliceElementType() {
	type MyStruct struct{}

	tests := []struct {
		slice interface{}
		want  reflect.Type
	}{
		{
			slice: []int{},
			want:  reflect.TypeOf(1),
		},
		{
			slice: []*int{},
			want:  reflect.TypeOf(1),
		},
		{
			slice: []MyStruct{},
			want:  reflect.TypeOf(MyStruct{}),
		},
		{
			slice: []*MyStruct{},
			want:  reflect.TypeOf(MyStruct{}),
		},
	}

	for _, tt := range tests {
		sliceVal := reflect.ValueOf(tt.slice)
		elementType := util.GetSliceElementType(sliceVal)

		rt.Equal(tt.want, elementType)
	}
}

func (rt *reflectTest) TestAppendSliceElement() {
	type MyStruct struct{}

	sliceVal := reflect.Indirect(reflect.ValueOf(&[]MyStruct{}))
	util.AppendSliceElement(sliceVal, reflect.ValueOf(&MyStruct{}))

	rt.Equal([]MyStruct{{}}, sliceVal.Interface())

	sliceVal = reflect.Indirect(reflect.ValueOf(&[]*MyStruct{}))
	util.AppendSliceElement(sliceVal, reflect.ValueOf(&MyStruct{}))

	rt.Equal([]*MyStruct{{}}, sliceVal.Interface())
}

func TestReflectSuite(t *testing.T) {
	suite.Run(t, new(reflectTest))
}
