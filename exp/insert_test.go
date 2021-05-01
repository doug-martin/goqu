package exp_test

import (
	"testing"

	"github.com/doug-martin/goqu/v9/exp"
	"github.com/stretchr/testify/suite"
)

type testAppendableExpression struct {
	exp.AppendableExpression
	sql     string
	args    []interface{}
	clauses exp.SelectClauses
}

func newTestAppendableExpression(sql string, args []interface{}) exp.AppendableExpression {
	return &testAppendableExpression{sql: sql, args: args}
}

func (tae *testAppendableExpression) Expression() exp.Expression {
	return tae
}

func (tae *testAppendableExpression) GetClauses() exp.SelectClauses {
	return tae.clauses
}

func (tae *testAppendableExpression) Clone() exp.Expression {
	return tae
}

type insertExpressionTestSuite struct {
	suite.Suite
}

func (iets *insertExpressionTestSuite) TestNewInsertExpression_withDifferentRecordTypes() {
	type testRecord struct {
		C string `db:"c"`
	}
	type testRecord2 struct {
		C string `db:"c"`
	}
	_, err := exp.NewInsertExpression(
		testRecord{C: "v1"},
		exp.Record{"c": "v2"},
	)
	iets.EqualError(err, "goqu: rows must be all the same type expected exp_test.testRecord got exp.Record")
	_, err = exp.NewInsertExpression(
		testRecord{C: "v1"},
		testRecord2{C: "v2"},
	)
	iets.EqualError(err, "goqu: rows must be all the same type expected exp_test.testRecord got exp_test.testRecord2")
}

func (iets *insertExpressionTestSuite) TestNewInsertExpression_withInvalidValue() {
	_, err := exp.NewInsertExpression(true)
	iets.EqualError(err, "goqu: unsupported insert must be map, goqu.Record, or struct type got: bool")
}

func (iets *insertExpressionTestSuite) TestNewInsertExpression_withDifferentTypes() {
	_, err := exp.NewInsertExpression(exp.Record{"a": "a1"}, true)
	iets.EqualError(err, "goqu: rows must be all the same type expected exp.Record got bool")
}

func (iets *insertExpressionTestSuite) TestNewInsertExpression_withNoValues() {
	ie, err := exp.NewInsertExpression()
	iets.NoError(err)
	iets.Nil(ie.Cols())
	iets.Nil(ie.Vals())
	iets.True(ie.IsEmpty())
	iets.False(ie.IsInsertFrom())
}

func (iets *insertExpressionTestSuite) TestNewInsertExpression_Vals() {
	ie, err := exp.NewInsertExpression()
	iets.NoError(err)
	vals := [][]interface{}{
		{"a", "b"},
	}
	ie = ie.SetCols(exp.NewColumnListExpression("a", "b")).SetVals(vals)
	iets.False(ie.IsEmpty())
	iets.False(ie.IsInsertFrom())
	iets.Equal(vals, ie.Vals())
}

func (iets *insertExpressionTestSuite) TestNewInsertExpression_Cols() {
	ie, err := exp.NewInsertExpression()
	iets.NoError(err)
	vals := [][]interface{}{
		{"a", "b"},
	}
	ce := exp.NewColumnListExpression("a", "b")
	ie = ie.SetCols(ce).SetVals(vals)
	iets.False(ie.IsEmpty())
	iets.False(ie.IsInsertFrom())
	iets.Equal(vals, ie.Vals())
	iets.Equal(ce, ie.Cols())
}

func (iets *insertExpressionTestSuite) TestNewInsertExpression_From() {
	ae := newTestAppendableExpression("select * from test", []interface{}{})
	ie, err := exp.NewInsertExpression(ae)
	iets.NoError(err)
	iets.False(ie.IsEmpty())
	iets.True(ie.IsInsertFrom())
	iets.Equal(ae, ie.From())
}

func (iets *insertExpressionTestSuite) TestNewInsertExpression_appendableExpression() {
	ae := newTestAppendableExpression("test ae", nil)

	ie, err := exp.NewInsertExpression(ae)
	iets.NoError(err)
	iets.False(ie.IsEmpty())
	iets.True(ie.IsInsertFrom())
}

func (iets *insertExpressionTestSuite) TestNewInsertExpression_withRecords() {
	ie, err := exp.NewInsertExpression(exp.Record{"c": "a"}, exp.Record{"c": "b"})
	iets.NoError(err)
	iets.Equal(exp.NewColumnListExpression("c"), ie.Cols())
	iets.Equal([][]interface{}{{"a"}, {"b"}}, ie.Vals())
	iets.False(ie.IsEmpty())
	iets.False(ie.IsInsertFrom())
}

func (iets *insertExpressionTestSuite) TestNewInsertExpression_withRecordsSlice() {
	ie, err := exp.NewInsertExpression([]exp.Record{{"c": "a"}, {"c": "b"}})
	iets.NoError(err)
	iets.Equal(exp.NewColumnListExpression("c"), ie.Cols())
	iets.Equal([][]interface{}{{"a"}, {"b"}}, ie.Vals())
	iets.False(ie.IsEmpty())
	iets.False(ie.IsInsertFrom())
}

func (iets *insertExpressionTestSuite) TestNewInsertExpression_withRecordOfDifferentLength() {
	_, err := exp.NewInsertExpression(exp.Record{"c": "a"}, exp.Record{"c": "b", "c2": "d"})
	iets.EqualError(err, "goqu: rows with different value length expected 1 got 2")
}

func (iets *insertExpressionTestSuite) TestNewInsertExpression_withRecordWithDifferentkeys() {
	_, err := exp.NewInsertExpression(exp.Record{"c1": "a"}, exp.Record{"c2": "b"})
	iets.EqualError(err, `goqu: rows with different keys expected ["c1"] got ["c2"]`)
}

func (iets *insertExpressionTestSuite) TestNewInsertExpression_withMap() {
	ie, err := exp.NewInsertExpression(
		map[string]interface{}{"c": "a"},
		map[string]interface{}{"c": "b"},
	)
	iets.NoError(err)
	iets.Equal(exp.NewColumnListExpression("c"), ie.Cols())
	iets.Equal([][]interface{}{{"a"}, {"b"}}, ie.Vals())
	iets.False(ie.IsEmpty())
	iets.False(ie.IsInsertFrom())
}

func (iets *insertExpressionTestSuite) TestNewInsertExpression_withStructs() {
	type testRecord struct {
		C string `db:"c"`
	}
	ie, err := exp.NewInsertExpression(
		testRecord{C: "a"},
		testRecord{C: "b"},
	)
	iets.NoError(err)
	iets.Equal(exp.NewColumnListExpression("c"), ie.Cols())
	iets.Equal([][]interface{}{{"a"}, {"b"}}, ie.Vals())
	iets.False(ie.IsEmpty())
	iets.False(ie.IsInsertFrom())
}

func (iets *insertExpressionTestSuite) TestNewInsertExpression_withStructSlice() {
	type testRecord struct {
		C string `db:"c"`
	}
	ie, err := exp.NewInsertExpression([]testRecord{
		{C: "a"},
		{C: "b"},
	})
	iets.NoError(err)
	iets.Equal(exp.NewColumnListExpression("c"), ie.Cols())
	iets.Equal([][]interface{}{{"a"}, {"b"}}, ie.Vals())
	iets.False(ie.IsEmpty())
	iets.False(ie.IsInsertFrom())
}

func (iets *insertExpressionTestSuite) TestNewInsertExpression_withStructsWithoutTags() {
	type testRecord struct {
		FieldA int64
		FieldB bool
		FieldC string
	}
	ie, err := exp.NewInsertExpression(
		testRecord{FieldA: 1, FieldB: true, FieldC: "a"},
		testRecord{FieldA: 2, FieldB: false, FieldC: "b"},
	)
	iets.NoError(err)
	iets.Equal(exp.NewColumnListExpression("fielda", "fieldb", "fieldc"), ie.Cols())
	iets.Equal([][]interface{}{{int64(1), true, "a"}, {int64(2), false, "b"}}, ie.Vals())
	iets.False(ie.IsEmpty())
	iets.False(ie.IsInsertFrom())
}

func (iets *insertExpressionTestSuite) TestNewInsertExpression_withStructsIgnoredDbTag() {
	type testRecord struct {
		FieldA int64 `db:"-"`
		FieldB bool
		FieldC string
	}
	ie, err := exp.NewInsertExpression(
		testRecord{FieldA: 1, FieldB: true, FieldC: "a"},
		testRecord{FieldA: 2, FieldB: false, FieldC: "b"},
	)
	iets.NoError(err)
	iets.Equal(exp.NewColumnListExpression("fieldb", "fieldc"), ie.Cols())
	iets.Equal([][]interface{}{{true, "a"}, {false, "b"}}, ie.Vals())
	iets.False(ie.IsEmpty())
	iets.False(ie.IsInsertFrom())
}

func (iets *insertExpressionTestSuite) TestNewInsertExpression_withStructsWithGoquSkipInsert() {
	type testRecord struct {
		FieldA int64
		FieldB bool   `goqu:"skipupdate"`
		FieldC string `goqu:"skipinsert"`
	}
	ie, err := exp.NewInsertExpression(
		testRecord{FieldA: 1, FieldB: true, FieldC: "a"},
		testRecord{FieldA: 2, FieldB: false, FieldC: "b"},
	)
	iets.NoError(err)
	iets.Equal(exp.NewColumnListExpression("fielda", "fieldb"), ie.Cols())
	iets.Equal([][]interface{}{{int64(1), true}, {int64(2), false}}, ie.Vals())
	iets.False(ie.IsEmpty())
	iets.False(ie.IsInsertFrom())
}

func (iets *insertExpressionTestSuite) TestNewInsertExpression_withStructPointers() {
	type testRecord struct {
		C string `db:"c"`
	}
	ie, err := exp.NewInsertExpression(
		&testRecord{C: "a"},
		&testRecord{C: "b"},
	)
	iets.NoError(err)
	iets.Equal(exp.NewColumnListExpression("c"), ie.Cols())
	iets.Equal([][]interface{}{{"a"}, {"b"}}, ie.Vals())
	iets.False(ie.IsEmpty())
	iets.False(ie.IsInsertFrom())
}

func (iets *insertExpressionTestSuite) TestNewInsertExpression_withStructsWithEmbeddedStructs() {
	type Phone struct {
		Primary string `db:"primary_phone"`
		Home    string `db:"home_phone"`
	}
	type item struct {
		Phone
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	ie, err := exp.NewInsertExpression(
		item{Address: "111 Test Addr", Name: "Test1", Phone: Phone{Home: "123123", Primary: "456456"}},
		item{Address: "211 Test Addr", Name: "Test2", Phone: Phone{Home: "123123", Primary: "456456"}},
		item{Address: "311 Test Addr", Name: "Test3", Phone: Phone{Home: "123123", Primary: "456456"}},
		item{Address: "411 Test Addr", Name: "Test4", Phone: Phone{Home: "123123", Primary: "456456"}},
	)
	iets.NoError(err)
	iets.Equal(exp.NewColumnListExpression("address", "home_phone", "name", "primary_phone"), ie.Cols())
	iets.Equal([][]interface{}{
		{"111 Test Addr", "123123", "Test1", "456456"},
		{"211 Test Addr", "123123", "Test2", "456456"},
		{"311 Test Addr", "123123", "Test3", "456456"},
		{"411 Test Addr", "123123", "Test4", "456456"},
	}, ie.Vals())
	iets.False(ie.IsEmpty())
	iets.False(ie.IsInsertFrom())
}

func (iets *insertExpressionTestSuite) TestNewInsertExpression_withStructsWithEmbeddedStructPointers() {
	type Phone struct {
		Primary string `db:"primary_phone"`
		Home    string `db:"home_phone"`
	}
	type item struct {
		*Phone
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	ie, err := exp.NewInsertExpression(
		item{Address: "111 Test Addr", Name: "Test1", Phone: &Phone{Home: "123123", Primary: "456456"}},
		item{Address: "211 Test Addr", Name: "Test2", Phone: &Phone{Home: "123123", Primary: "456456"}},
		item{Address: "311 Test Addr", Name: "Test3", Phone: &Phone{Home: "123123", Primary: "456456"}},
		item{Address: "411 Test Addr", Name: "Test4", Phone: &Phone{Home: "123123", Primary: "456456"}},
	)
	iets.NoError(err)
	iets.Equal(exp.NewColumnListExpression("address", "home_phone", "name", "primary_phone"), ie.Cols())
	iets.Equal([][]interface{}{
		{"111 Test Addr", "123123", "Test1", "456456"},
		{"211 Test Addr", "123123", "Test2", "456456"},
		{"311 Test Addr", "123123", "Test3", "456456"},
		{"411 Test Addr", "123123", "Test4", "456456"},
	}, ie.Vals())
	iets.False(ie.IsEmpty())
	iets.False(ie.IsInsertFrom())
}

func (iets *insertExpressionTestSuite) TestNewInsertExpression_withNilEmbeddedStructPointers() {
	type Phone struct {
		Primary string `db:"primary_phone"`
		Home    string `db:"home_phone"`
	}
	type item struct {
		*Phone
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	ie, err := exp.NewInsertExpression(
		item{Address: "111 Test Addr", Name: "Test1"},
		item{Address: "211 Test Addr", Name: "Test2"},
		item{Address: "311 Test Addr", Name: "Test3"},
		item{Address: "411 Test Addr", Name: "Test4"},
	)
	iets.NoError(err)
	iets.Equal(exp.NewColumnListExpression("address", "name"), ie.Cols())
	iets.Equal([][]interface{}{
		{"111 Test Addr", "Test1"},
		{"211 Test Addr", "Test2"},
		{"311 Test Addr", "Test3"},
		{"411 Test Addr", "Test4"},
	}, ie.Vals())
	iets.False(ie.IsEmpty())
	iets.False(ie.IsInsertFrom())
}

func (iets *insertExpressionTestSuite) TestNewInsertExpression_withDifferentStructTypes() {
	type Phone struct {
		Primary string `db:"primary_phone"`
		Home    string `db:"home_phone"`
	}
	type item struct {
		*Phone
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	_, err := exp.NewInsertExpression(
		item{Address: "111 Test Addr", Name: "Test1"},
		Phone{Home: "123123", Primary: "456456"},
		item{Address: "311 Test Addr", Name: "Test3"},
		Phone{Home: "123123", Primary: "456456"},
	)
	iets.EqualError(err, "goqu: rows must be all the same type expected exp_test.item got exp_test.Phone")
}

func (iets *insertExpressionTestSuite) TestNewInsertExpression_withDifferentColumnLengths() {
	type Phone struct {
		Primary string `db:"primary_phone"`
		Home    string `db:"home_phone"`
	}
	type Phone2 struct {
		Primary string `db:"primary_phone2"`
		Home    string `db:"home_phone2"`
	}
	type item struct {
		*Phone
		*Phone2
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	_, err := exp.NewInsertExpression(
		item{Address: "111 Test Addr", Name: "Test1", Phone2: &Phone2{Home: "123123", Primary: "456456"}},
		item{Address: "311 Test Addr", Name: "Test3", Phone: &Phone{Home: "123123", Primary: "456456"}},
	)
	iets.EqualError(err, `goqu: rows with different keys expected `+
		`["address","home_phone2","name","primary_phone2"] got ["address","home_phone","name","primary_phone"]`)
}

func TestInsertExpressionSuite(t *testing.T) {
	suite.Run(t, new(insertExpressionTestSuite))
}
