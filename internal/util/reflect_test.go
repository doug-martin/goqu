package util

import (
	"database/sql"
	"reflect"
	"strings"
	"sync"
	"testing"

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
		rt.True(IsUint(reflect.ValueOf(v).Kind()))
	}

	for _, v := range ints {
		rt.False(IsUint(reflect.ValueOf(v).Kind()))
	}
	for _, v := range floats {
		rt.False(IsUint(reflect.ValueOf(v).Kind()))
	}
	for _, v := range strs {
		rt.False(IsUint(reflect.ValueOf(v).Kind()))
	}
	for _, v := range bools {
		rt.False(IsUint(reflect.ValueOf(v).Kind()))
	}
	for _, v := range structs {
		rt.False(IsUint(reflect.ValueOf(v).Kind()))
	}
	for _, v := range invalids {
		rt.False(IsUint(reflect.ValueOf(v).Kind()))
	}
	for _, v := range pointers {
		rt.False(IsUint(reflect.ValueOf(v).Kind()))
	}
}

func (rt *reflectTest) TestIsInt() {
	for _, v := range ints {
		rt.True(IsInt(reflect.ValueOf(v).Kind()))
	}

	for _, v := range uints {
		rt.False(IsInt(reflect.ValueOf(v).Kind()))
	}
	for _, v := range floats {
		rt.False(IsInt(reflect.ValueOf(v).Kind()))
	}
	for _, v := range strs {
		rt.False(IsInt(reflect.ValueOf(v).Kind()))
	}
	for _, v := range bools {
		rt.False(IsInt(reflect.ValueOf(v).Kind()))
	}
	for _, v := range structs {
		rt.False(IsInt(reflect.ValueOf(v).Kind()))
	}
	for _, v := range invalids {
		rt.False(IsInt(reflect.ValueOf(v).Kind()))
	}
	for _, v := range pointers {
		rt.False(IsInt(reflect.ValueOf(v).Kind()))
	}
}

func (rt *reflectTest) TestIsFloat() {
	for _, v := range floats {
		rt.True(IsFloat(reflect.ValueOf(v).Kind()))
	}

	for _, v := range uints {
		rt.False(IsFloat(reflect.ValueOf(v).Kind()))
	}
	for _, v := range ints {
		rt.False(IsFloat(reflect.ValueOf(v).Kind()))
	}
	for _, v := range strs {
		rt.False(IsFloat(reflect.ValueOf(v).Kind()))
	}
	for _, v := range bools {
		rt.False(IsFloat(reflect.ValueOf(v).Kind()))
	}
	for _, v := range structs {
		rt.False(IsFloat(reflect.ValueOf(v).Kind()))
	}
	for _, v := range invalids {
		rt.False(IsFloat(reflect.ValueOf(v).Kind()))
	}
	for _, v := range pointers {
		rt.False(IsFloat(reflect.ValueOf(v).Kind()))
	}
}

func (rt *reflectTest) TestIsString() {
	for _, v := range strs {
		rt.True(IsString(reflect.ValueOf(v).Kind()))
	}

	for _, v := range uints {
		rt.False(IsString(reflect.ValueOf(v).Kind()))
	}
	for _, v := range ints {
		rt.False(IsString(reflect.ValueOf(v).Kind()))
	}
	for _, v := range floats {
		rt.False(IsString(reflect.ValueOf(v).Kind()))
	}
	for _, v := range bools {
		rt.False(IsString(reflect.ValueOf(v).Kind()))
	}
	for _, v := range structs {
		rt.False(IsString(reflect.ValueOf(v).Kind()))
	}
	for _, v := range invalids {
		rt.False(IsString(reflect.ValueOf(v).Kind()))
	}
	for _, v := range pointers {
		rt.False(IsString(reflect.ValueOf(v).Kind()))
	}
}

func (rt *reflectTest) TestIsBool() {
	for _, v := range bools {
		rt.True(IsBool(reflect.ValueOf(v).Kind()))
	}

	for _, v := range uints {
		rt.False(IsBool(reflect.ValueOf(v).Kind()))
	}
	for _, v := range ints {
		rt.False(IsBool(reflect.ValueOf(v).Kind()))
	}
	for _, v := range floats {
		rt.False(IsBool(reflect.ValueOf(v).Kind()))
	}
	for _, v := range strs {
		rt.False(IsBool(reflect.ValueOf(v).Kind()))
	}
	for _, v := range structs {
		rt.False(IsBool(reflect.ValueOf(v).Kind()))
	}
	for _, v := range invalids {
		rt.False(IsBool(reflect.ValueOf(v).Kind()))
	}
	for _, v := range pointers {
		rt.False(IsBool(reflect.ValueOf(v).Kind()))
	}
}

func (rt *reflectTest) TestIsStruct() {
	for _, v := range structs {
		rt.True(IsStruct(reflect.ValueOf(v).Kind()))
	}

	for _, v := range uints {
		rt.False(IsStruct(reflect.ValueOf(v).Kind()))
	}
	for _, v := range ints {
		rt.False(IsStruct(reflect.ValueOf(v).Kind()))
	}
	for _, v := range floats {
		rt.False(IsStruct(reflect.ValueOf(v).Kind()))
	}
	for _, v := range bools {
		rt.False(IsStruct(reflect.ValueOf(v).Kind()))
	}
	for _, v := range strs {
		rt.False(IsStruct(reflect.ValueOf(v).Kind()))
	}
	for _, v := range invalids {
		rt.False(IsStruct(reflect.ValueOf(v).Kind()))
	}
	for _, v := range pointers {
		rt.False(IsStruct(reflect.ValueOf(v).Kind()))
	}
}

func (rt *reflectTest) TestIsSlice() {
	rt.True(IsSlice(reflect.ValueOf(uints).Kind()))
	rt.True(IsSlice(reflect.ValueOf(ints).Kind()))
	rt.True(IsSlice(reflect.ValueOf(floats).Kind()))
	rt.True(IsSlice(reflect.ValueOf(structs).Kind()))

	rt.False(IsSlice(reflect.ValueOf(structs[0]).Kind()))
}

func (rt *reflectTest) TestIsInvalid() {
	for _, v := range invalids {
		rt.True(IsInvalid(reflect.ValueOf(v).Kind()))
	}

	for _, v := range uints {
		rt.False(IsInvalid(reflect.ValueOf(v).Kind()))
	}
	for _, v := range ints {
		rt.False(IsInvalid(reflect.ValueOf(v).Kind()))
	}
	for _, v := range floats {
		rt.False(IsInvalid(reflect.ValueOf(v).Kind()))
	}
	for _, v := range bools {
		rt.False(IsInvalid(reflect.ValueOf(v).Kind()))
	}
	for _, v := range strs {
		rt.False(IsInvalid(reflect.ValueOf(v).Kind()))
	}
	for _, v := range structs {
		rt.False(IsInvalid(reflect.ValueOf(v).Kind()))
	}
	for _, v := range pointers {
		rt.False(IsInvalid(reflect.ValueOf(v).Kind()))
	}
}

func (rt *reflectTest) TestIsPointer() {
	for _, v := range pointers {
		rt.True(IsPointer(reflect.ValueOf(v).Kind()))
	}

	for _, v := range uints {
		rt.False(IsPointer(reflect.ValueOf(v).Kind()))
	}
	for _, v := range ints {
		rt.False(IsPointer(reflect.ValueOf(v).Kind()))
	}
	for _, v := range floats {
		rt.False(IsPointer(reflect.ValueOf(v).Kind()))
	}
	for _, v := range bools {
		rt.False(IsPointer(reflect.ValueOf(v).Kind()))
	}
	for _, v := range strs {
		rt.False(IsPointer(reflect.ValueOf(v).Kind()))
	}
	for _, v := range structs {
		rt.False(IsPointer(reflect.ValueOf(v).Kind()))
	}
	for _, v := range invalids {
		rt.False(IsPointer(reflect.ValueOf(v).Kind()))
	}
}

func (rt *reflectTest) TestIsEmptyValue_emptyValues() {
	ts := TestStruct{}
	rt.True(IsEmptyValue(reflect.ValueOf(ts.arr)))
	rt.True(IsEmptyValue(reflect.ValueOf(ts.slc)))
	rt.True(IsEmptyValue(reflect.ValueOf(ts.mp)))
	rt.True(IsEmptyValue(reflect.ValueOf(ts.str)))
	rt.True(IsEmptyValue(reflect.ValueOf(ts.bl)))
	rt.True(IsEmptyValue(reflect.ValueOf(ts.i)))
	rt.True(IsEmptyValue(reflect.ValueOf(ts.i8)))
	rt.True(IsEmptyValue(reflect.ValueOf(ts.i16)))
	rt.True(IsEmptyValue(reflect.ValueOf(ts.i32)))
	rt.True(IsEmptyValue(reflect.ValueOf(ts.i64)))
	rt.True(IsEmptyValue(reflect.ValueOf(ts.ui)))
	rt.True(IsEmptyValue(reflect.ValueOf(ts.ui8)))
	rt.True(IsEmptyValue(reflect.ValueOf(ts.ui16)))
	rt.True(IsEmptyValue(reflect.ValueOf(ts.ui32)))
	rt.True(IsEmptyValue(reflect.ValueOf(ts.ui64)))
	rt.True(IsEmptyValue(reflect.ValueOf(ts.f32)))
	rt.True(IsEmptyValue(reflect.ValueOf(ts.f64)))
	rt.True(IsEmptyValue(reflect.ValueOf(ts.intr)))
	rt.True(IsEmptyValue(reflect.ValueOf(ts.ptr)))
}

func (rt *reflectTest) TestIsEmptyValue_validValues() {
	ts := TestStruct{intr: TestInterfaceImpl{"hello"}}
	rt.False(IsEmptyValue(reflect.ValueOf([1]string{"a"})))
	rt.False(IsEmptyValue(reflect.ValueOf([]string{"a"})))
	rt.False(IsEmptyValue(reflect.ValueOf(map[string]interface{}{"a": true})))
	rt.False(IsEmptyValue(reflect.ValueOf("str")))
	rt.False(IsEmptyValue(reflect.ValueOf(true)))
	rt.False(IsEmptyValue(reflect.ValueOf(int(1))))
	rt.False(IsEmptyValue(reflect.ValueOf(int8(1))))
	rt.False(IsEmptyValue(reflect.ValueOf(int16(1))))
	rt.False(IsEmptyValue(reflect.ValueOf(int32(1))))
	rt.False(IsEmptyValue(reflect.ValueOf(int64(1))))
	rt.False(IsEmptyValue(reflect.ValueOf(uint(1))))
	rt.False(IsEmptyValue(reflect.ValueOf(uint8(1))))
	rt.False(IsEmptyValue(reflect.ValueOf(uint16(1))))
	rt.False(IsEmptyValue(reflect.ValueOf(uint32(1))))
	rt.False(IsEmptyValue(reflect.ValueOf(uint64(1))))
	rt.False(IsEmptyValue(reflect.ValueOf(float32(0.1))))
	rt.False(IsEmptyValue(reflect.ValueOf(float64(0.2))))
	rt.False(IsEmptyValue(reflect.ValueOf(ts.intr)))
	rt.False(IsEmptyValue(reflect.ValueOf(&TestStruct{str: "a"})))
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
	lowerColumnMap, lowerErr := GetColumnMap(&lowerAnon)
	rt.NoError(lowerErr)

	var lowerKeys []string
	for key := range lowerColumnMap {
		lowerKeys = append(lowerKeys, key)
	}
	rt.Contains(lowerKeys, "firstlower")
	rt.Contains(lowerKeys, "lastlower")

	// changing rename function
	SetColumnRenameFunction(strings.ToUpper)

	upperAnon := struct {
		FirstUpper string
		LastUpper  string
	}{}
	upperColumnMap, upperErr := GetColumnMap(&upperAnon)
	rt.NoError(upperErr)

	var upperKeys []string
	for key := range upperColumnMap {
		upperKeys = append(upperKeys, key)
	}
	rt.Contains(upperKeys, "FIRSTUPPER")
	rt.Contains(upperKeys, "LASTUPPER")

	SetColumnRenameFunction(defaultColumnRenameFunction)
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
		m, err := GetColumnMap(i)
		rt.NoError(err)
		rt.NotNil(m)
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		i := item{id: 2, name: "sally"}
		m, err := GetColumnMap(i)
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
	cm, err := GetColumnMap(&ts)
	rt.NoError(err)
	data := []map[string]interface{}{
		{
			"str":    "string",
			"int":    int64(10),
			"bool":   true,
			"valuer": sql.NullString{String: "null_str", Valid: true},
		},
	}
	AssignStructVals(&ts, data, cm)
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
	cm, err := GetColumnMap(&ts)
	rt.NoError(err)
	ns := &sql.NullString{String: "null_str1", Valid: true}
	data := []map[string]interface{}{
		{
			"str":    "string",
			"int":    int64(10),
			"bool":   true,
			"valuer": &ns,
		},
	}
	AssignStructVals(&ts, data, cm)
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
	cm, err := GetColumnMap(&ts)
	rt.NoError(err)
	ns := &sql.NullString{String: "null_str1", Valid: true}
	data := []map[string]interface{}{
		{
			"str":    "string",
			"int":    int64(10),
			"bool":   true,
			"valuer": &ns,
		},
	}
	AssignStructVals(&ts, data, cm)
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
	cm, err := GetColumnMap(&ts)
	rt.NoError(err)
	ns := &sql.NullString{String: "null_str1", Valid: true}
	data := []map[string]interface{}{
		{
			"str":    "string",
			"int":    int64(10),
			"bool":   true,
			"valuer": &ns,
		},
	}
	AssignStructVals(&ts, data, cm)
	rt.Equal(ts, TestStruct{
		EmbeddedStruct: &EmbeddedStruct{Str: "string"},
		Int:            10,
		Bool:           true,
		Valuer:         ns,
	})
}

func (rt *reflectTest) TestAssignStructVals_withSlice() {

	type TestStruct struct {
		Str    string
		Int    int64
		Bool   bool
		Valuer sql.NullString
	}
	var ts []TestStruct
	cm, err := GetColumnMap(&ts)
	rt.NoError(err)
	data := []map[string]interface{}{
		{
			"str":    "string1",
			"int":    int64(10),
			"bool":   true,
			"valuer": sql.NullString{String: "null_str1", Valid: true},
		},
		{
			"str":    "string2",
			"int":    int64(20),
			"bool":   false,
			"valuer": sql.NullString{String: "null_str2", Valid: true},
		},
	}
	AssignStructVals(&ts, data, cm)
	rt.Equal(ts, []TestStruct{
		{
			Str:    "string1",
			Int:    10,
			Bool:   true,
			Valuer: sql.NullString{String: "null_str1", Valid: true},
		},
		{
			Str:    "string2",
			Int:    20,
			Bool:   false,
			Valuer: sql.NullString{String: "null_str2", Valid: true},
		},
	})
}

func (rt *reflectTest) TestAssignStructVals_withSliceOfPointers() {

	type TestStruct struct {
		Str    string
		Int    int64
		Bool   bool
		Valuer sql.NullString
	}
	var ts []*TestStruct
	cm, err := GetColumnMap(&ts)
	rt.NoError(err)
	data := []map[string]interface{}{
		{
			"str":    "string1",
			"int":    int64(10),
			"bool":   true,
			"valuer": sql.NullString{String: "null_str1", Valid: true},
		},
		{
			"str":    "string2",
			"int":    int64(20),
			"bool":   false,
			"valuer": sql.NullString{String: "null_str2", Valid: true},
		},
	}
	AssignStructVals(&ts, data, cm)
	rt.Equal(ts, []*TestStruct{
		{
			Str:    "string1",
			Int:    10,
			Bool:   true,
			Valuer: sql.NullString{String: "null_str1", Valid: true},
		},
		{
			Str:    "string2",
			Int:    20,
			Bool:   false,
			Valuer: sql.NullString{String: "null_str2", Valid: true},
		},
	})
}

func (rt *reflectTest) TestAssignStructVals_withSliceOfStructsWithPointerVals() {

	type TestStruct struct {
		Str    string
		Int    int64
		Bool   bool
		Valuer *sql.NullString
	}
	var ts []TestStruct
	cm, err := GetColumnMap(&ts)
	rt.NoError(err)
	ns1 := &sql.NullString{String: "null_str1", Valid: true}
	ns2 := &sql.NullString{String: "null_str2", Valid: true}
	data := []map[string]interface{}{
		{
			"str":    "string1",
			"int":    int64(10),
			"bool":   true,
			"valuer": &ns1,
		},
		{
			"str":    "string2",
			"int":    int64(20),
			"bool":   false,
			"valuer": &ns2,
		},
	}
	AssignStructVals(&ts, data, cm)
	rt.Equal(ts, []TestStruct{
		{
			Str:    "string1",
			Int:    10,
			Bool:   true,
			Valuer: ns1,
		},
		{
			Str:    "string2",
			Int:    20,
			Bool:   false,
			Valuer: ns2,
		},
	})
}

func (rt *reflectTest) TestAssignStructVals_withSliceofStructsWithEmbeddedStruct() {

	type EmbeddedStruct struct {
		Str string
	}
	type TestStruct struct {
		EmbeddedStruct
		Int    int64
		Bool   bool
		Valuer *sql.NullString
	}
	var ts []TestStruct
	cm, err := GetColumnMap(&ts)
	rt.NoError(err)
	ns1 := &sql.NullString{String: "null_str1", Valid: true}
	ns2 := &sql.NullString{String: "null_str2", Valid: true}
	data := []map[string]interface{}{
		{
			"str":    "string1",
			"int":    int64(10),
			"bool":   true,
			"valuer": &ns1,
		},
		{
			"str":    "string2",
			"int":    int64(20),
			"bool":   false,
			"valuer": &ns2,
		},
	}
	AssignStructVals(&ts, data, cm)
	rt.Equal(ts, []TestStruct{
		{
			EmbeddedStruct: EmbeddedStruct{Str: "string1"},
			Int:            10,
			Bool:           true,
			Valuer:         ns1,
		},
		{
			EmbeddedStruct: EmbeddedStruct{Str: "string2"},
			Int:            20,
			Bool:           false,
			Valuer:         ns2,
		},
	})
}

func (rt *reflectTest) TestAssignStructVals_withSliceofStructsWithEmbeddedStructPointer() {

	type EmbeddedStruct struct {
		Str string
	}
	type TestStruct struct {
		*EmbeddedStruct
		Int    int64
		Bool   bool
		Valuer *sql.NullString
	}
	var ts []TestStruct
	cm, err := GetColumnMap(&ts)
	rt.NoError(err)
	ns1 := &sql.NullString{String: "null_str1", Valid: true}
	ns2 := &sql.NullString{String: "null_str2", Valid: true}
	data := []map[string]interface{}{
		{
			"str":    "string1",
			"int":    int64(10),
			"bool":   true,
			"valuer": &ns1,
		},
		{
			"str":    "string2",
			"int":    int64(20),
			"bool":   false,
			"valuer": &ns2,
		},
	}
	AssignStructVals(&ts, data, cm)
	rt.Equal(ts, []TestStruct{
		{
			EmbeddedStruct: &EmbeddedStruct{Str: "string1"},
			Int:            10,
			Bool:           true,
			Valuer:         ns1,
		},
		{
			EmbeddedStruct: &EmbeddedStruct{Str: "string2"},
			Int:            20,
			Bool:           false,
			Valuer:         ns2,
		},
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
	cm, err := GetColumnMap(&ts)
	rt.NoError(err)
	rt.Equal(ColumnMap{
		"str":    {ColumnName: "str", FieldIndex: []int{0}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf("")},
		"int":    {ColumnName: "int", FieldIndex: []int{1}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(int64(1))},
		"bool":   {ColumnName: "bool", FieldIndex: []int{2}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(true)},
		"valuer": {ColumnName: "valuer", FieldIndex: []int{3}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(&sql.NullString{})},
	}, cm)
}

func (rt *reflectTest) TestGetColumnMap_withStructGoquTags() {

	type TestStruct struct {
		Str    string `goqu:"skipinsert,skipupdate"`
		Int    int64  `goqu:"skipinsert"`
		Bool   bool   `goqu:"skipupdate"`
		Empty  bool   `goqu:"defaultifempty"`
		Valuer *sql.NullString
	}
	var ts TestStruct
	cm, err := GetColumnMap(&ts)
	rt.NoError(err)
	rt.Equal(ColumnMap{
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
		"valuer": {ColumnName: "valuer", FieldIndex: []int{4}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(&sql.NullString{})},
	}, cm)
}

func (rt *reflectTest) TestGetColumnMap_withStructWithTag() {

	type TestStruct struct {
		Str    string          `db:"s"`
		Int    int64           `db:"i"`
		Bool   bool            `db:"b"`
		Valuer *sql.NullString `db:"v"`
	}
	var ts TestStruct
	cm, err := GetColumnMap(&ts)
	rt.NoError(err)
	rt.Equal(ColumnMap{
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
	cm, err := GetColumnMap(&ts)
	rt.NoError(err)
	rt.Equal(ColumnMap{
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
	cm, err := GetColumnMap(&ts)
	rt.NoError(err)
	rt.Equal(ColumnMap{
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
	cm, err := GetColumnMap(&ts)
	rt.NoError(err)
	rt.Equal(ColumnMap{
		"str":    {ColumnName: "str", FieldIndex: []int{0}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf("")},
		"int":    {ColumnName: "int", FieldIndex: []int{1}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(int64(1))},
		"bool":   {ColumnName: "bool", FieldIndex: []int{2}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(true)},
		"valuer": {ColumnName: "valuer", FieldIndex: []int{3}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(&sql.NullString{})},
	}, cm)
}

func (rt *reflectTest) TestGetColumnMap_withNonStruct() {

	var v int64
	_, err := GetColumnMap(&v)
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
	cm, err := GetColumnMap(&ts)
	rt.NoError(err)
	rt.Equal(ColumnMap{
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
	cm, err := GetColumnMap(&ts)
	rt.NoError(err)
	rt.Equal(ColumnMap{
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
	cm, err := GetColumnMap(&ts)
	rt.NoError(err)
	rt.Equal(ColumnMap{
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
	cm, err := GetColumnMap(&ts)
	rt.NoError(err)
	rt.Equal(ColumnMap{
		"int":    {ColumnName: "int", FieldIndex: []int{1}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(int64(1))},
		"bool":   {ColumnName: "bool", FieldIndex: []int{2}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(true)},
		"valuer": {ColumnName: "valuer", FieldIndex: []int{3}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(&sql.NullString{})},
	}, cm)
}

func (rt *reflectTest) TestGetColumnMap_withPrivateFields() {

	type TestStruct struct {
		str    string // nolint:structcheck,unused
		Int    int64
		Bool   bool
		Valuer *sql.NullString
	}
	var ts TestStruct
	cm, err := GetColumnMap(&ts)
	rt.NoError(err)
	rt.Equal(ColumnMap{
		"int":    {ColumnName: "int", FieldIndex: []int{1}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(int64(1))},
		"bool":   {ColumnName: "bool", FieldIndex: []int{2}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(true)},
		"valuer": {ColumnName: "valuer", FieldIndex: []int{3}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(&sql.NullString{})},
	}, cm)
}

func (rt *reflectTest) TestGetColumnMap_withPrivateEmbeddedFields() {

	type TestEmbedded struct {
		str string // nolint:structcheck,unused
		Int int64
	}

	type TestStruct struct {
		TestEmbedded
		Bool   bool
		Valuer *sql.NullString
	}
	var ts TestStruct
	cm, err := GetColumnMap(&ts)
	rt.NoError(err)
	rt.Equal(ColumnMap{
		"int":    {ColumnName: "int", FieldIndex: []int{0, 1}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(int64(1))},
		"bool":   {ColumnName: "bool", FieldIndex: []int{1}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(true)},
		"valuer": {ColumnName: "valuer", FieldIndex: []int{2}, ShouldInsert: true, ShouldUpdate: true, GoType: reflect.TypeOf(&sql.NullString{})},
	}, cm)
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
	f, isAvailable := SafeGetFieldByIndex(v, []int{0, 0})
	rt.False(isAvailable)
	rt.False(f.IsValid())
	f, isAvailable = SafeGetFieldByIndex(v, []int{1})
	rt.True(isAvailable)
	rt.True(f.IsValid())
	rt.Equal(reflect.String, f.Type().Kind())
	f, isAvailable = SafeGetFieldByIndex(v, []int{})
	rt.True(isAvailable)
	rt.Equal(v, f)

	v = reflect.ValueOf(TestEmbeddedPointerStruct{TestEmbedded: &TestEmbedded{}})
	f, isAvailable = SafeGetFieldByIndex(v, []int{0, 0})
	rt.True(isAvailable)
	rt.True(f.IsValid())
	rt.Equal(reflect.Int, f.Type().Kind())
	f, isAvailable = SafeGetFieldByIndex(v, []int{1})
	rt.True(isAvailable)
	rt.True(f.IsValid())
	rt.Equal(reflect.String, f.Type().Kind())
	f, isAvailable = SafeGetFieldByIndex(v, []int{})
	rt.True(isAvailable)
	rt.Equal(v, f)

	v = reflect.ValueOf(TestEmbeddedStruct{})
	f, isAvailable = SafeGetFieldByIndex(v, []int{0, 0})
	rt.True(isAvailable)
	rt.True(f.IsValid())
	rt.Equal(reflect.Int, f.Type().Kind())
	f, isAvailable = SafeGetFieldByIndex(v, []int{1})
	rt.True(isAvailable)
	rt.True(f.IsValid())
	rt.Equal(reflect.String, f.Type().Kind())
	f, isAvailable = SafeGetFieldByIndex(v, []int{})
	rt.True(isAvailable)
	rt.Equal(v, f)

	v = reflect.ValueOf(TestEmbeddedStruct{TestEmbedded: TestEmbedded{}})
	f, isAvailable = SafeGetFieldByIndex(v, []int{0, 0})
	rt.True(isAvailable)
	rt.True(f.IsValid())
	f, isAvailable = SafeGetFieldByIndex(v, []int{1})
	rt.True(isAvailable)
	rt.True(f.IsValid())
	rt.Equal(reflect.String, f.Type().Kind())
	f, isAvailable = SafeGetFieldByIndex(v, []int{})
	rt.True(isAvailable)
	rt.Equal(v, f)
}

func TestReflectSuite(t *testing.T) {
	suite.Run(t, new(reflectTest))
}
