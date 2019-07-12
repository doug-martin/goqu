package exp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type testAppendableExpression struct {
	AppendableExpression
	sql     string
	args    []interface{}
	clauses Clauses
}

func newTestAppendableExpression(sql string, args []interface{}) AppendableExpression {
	return &testAppendableExpression{sql: sql, args: args}
}

func (tae *testAppendableExpression) Expression() Expression {
	return tae
}

func (tae *testAppendableExpression) GetClauses() Clauses {
	return tae.clauses
}

func (tae *testAppendableExpression) Clone() Expression {
	return tae
}

type insertExpressionTestSuite struct {
	suite.Suite
}

func (iets *insertExpressionTestSuite) TestNewInsertExpression_withDifferentRecordTypes() {
	t := iets.T()
	type testRecord struct {
		C string `db:"c"`
	}
	type testRecord2 struct {
		C string `db:"c"`
	}
	_, err := NewInsertExpression(
		testRecord{C: "v1"},
		Record{"c": "v2"},
	)
	assert.EqualError(t, err, "goqu: rows must be all the same type expected exp.testRecord got exp.Record")
	_, err = NewInsertExpression(
		testRecord{C: "v1"},
		testRecord2{C: "v2"},
	)
	assert.EqualError(t, err, "goqu: rows must be all the same type expected exp.testRecord got exp.testRecord2")
}

func (iets *insertExpressionTestSuite) TestNewInsertExpression_withInvalidValue() {
	t := iets.T()
	_, err := NewInsertExpression(true)
	assert.EqualError(t, err, "goqu: unsupported insert must be map, goqu.Record, or struct type got: bool")
}

func (iets *insertExpressionTestSuite) TestNewInsertExpression_withNoValues() {
	t := iets.T()
	ie, err := NewInsertExpression()
	assert.NoError(t, err)
	eie := new(insert)
	assert.Equal(t, eie, ie)
	assert.True(t, ie.IsEmpty())
	assert.False(t, ie.IsInsertFrom())
}

func (iets *insertExpressionTestSuite) TestNewInsertExpression_appendableExpression() {
	t := iets.T()

	ae := newTestAppendableExpression("test ae", nil)

	ie, err := NewInsertExpression(ae)
	assert.NoError(t, err)
	eie := &insert{from: ae}
	assert.Equal(t, eie, ie)
	assert.False(t, ie.IsEmpty())
	assert.True(t, ie.IsInsertFrom())
}

func (iets *insertExpressionTestSuite) TestNewInsertExpression_withRecords() {
	t := iets.T()
	ie, err := NewInsertExpression(Record{"c": "a"}, Record{"c": "b"})
	assert.NoError(t, err)
	eie := new(insert).
		SetCols(NewColumnListExpression("c")).
		SetVals([][]interface{}{{"a"}, {"b"}})
	assert.Equal(t, eie, ie)
	assert.False(t, ie.IsEmpty())
	assert.False(t, ie.IsInsertFrom())
}

func (iets *insertExpressionTestSuite) TestNewInsertExpression_withRecordOfDifferentLength() {
	t := iets.T()
	_, err := NewInsertExpression(Record{"c": "a"}, Record{"c": "b", "c2": "d"})
	assert.EqualError(t, err, "goqu: rows with different value length expected 1 got 2")
}

func (iets *insertExpressionTestSuite) TestNewInsertExpression_withRecordWithDifferentkeys() {
	t := iets.T()
	_, err := NewInsertExpression(Record{"c1": "a"}, Record{"c2": "b"})
	assert.EqualError(t, err, `goqu: rows with different keys expected ["c1"] got ["c2"]`)
}

func (iets *insertExpressionTestSuite) TestNewInsertExpression_withMap() {
	t := iets.T()
	ie, err := NewInsertExpression(
		map[string]interface{}{"c": "a"},
		map[string]interface{}{"c": "b"},
	)
	assert.NoError(t, err)
	eie := new(insert).
		SetCols(NewColumnListExpression("c")).
		SetVals([][]interface{}{{"a"}, {"b"}})
	assert.Equal(t, eie, ie)
	assert.False(t, ie.IsEmpty())
	assert.False(t, ie.IsInsertFrom())
}

func (iets *insertExpressionTestSuite) TestNewInsertExpression_withStructs() {
	type testRecord struct {
		C string `db:"c"`
	}
	t := iets.T()
	ie, err := NewInsertExpression(
		testRecord{C: "a"},
		testRecord{C: "b"},
	)
	assert.NoError(t, err)
	eie := new(insert).
		SetCols(NewColumnListExpression("c")).
		SetVals([][]interface{}{{"a"}, {"b"}})
	assert.Equal(t, eie, ie)
	assert.False(t, ie.IsEmpty())
	assert.False(t, ie.IsInsertFrom())
}

func (iets *insertExpressionTestSuite) TestNewInsertExpression_withStructPointers() {
	type testRecord struct {
		C string `db:"c"`
	}
	t := iets.T()
	ie, err := NewInsertExpression(
		&testRecord{C: "a"},
		&testRecord{C: "b"},
	)
	assert.NoError(t, err)
	eie := new(insert).
		SetCols(NewColumnListExpression("c")).
		SetVals([][]interface{}{{"a"}, {"b"}})
	assert.Equal(t, eie, ie)
	assert.False(t, ie.IsEmpty())
	assert.False(t, ie.IsInsertFrom())
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
	t := iets.T()
	ie, err := NewInsertExpression(
		item{Address: "111 Test Addr", Name: "Test1", Phone: Phone{Home: "123123", Primary: "456456"}},
		item{Address: "211 Test Addr", Name: "Test2", Phone: Phone{Home: "123123", Primary: "456456"}},
		item{Address: "311 Test Addr", Name: "Test3", Phone: Phone{Home: "123123", Primary: "456456"}},
		item{Address: "411 Test Addr", Name: "Test4", Phone: Phone{Home: "123123", Primary: "456456"}},
	)
	assert.NoError(t, err)
	eie := new(insert).
		SetCols(NewColumnListExpression("primary_phone", "home_phone", "address", "name")).
		SetVals([][]interface{}{
			{"456456", "123123", "111 Test Addr", "Test1"},
			{"456456", "123123", "211 Test Addr", "Test2"},
			{"456456", "123123", "311 Test Addr", "Test3"},
			{"456456", "123123", "411 Test Addr", "Test4"},
		})
	assert.Equal(t, eie, ie)
	assert.False(t, ie.IsEmpty())
	assert.False(t, ie.IsInsertFrom())
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
	t := iets.T()
	ie, err := NewInsertExpression(
		item{Address: "111 Test Addr", Name: "Test1", Phone: &Phone{Home: "123123", Primary: "456456"}},
		item{Address: "211 Test Addr", Name: "Test2", Phone: &Phone{Home: "123123", Primary: "456456"}},
		item{Address: "311 Test Addr", Name: "Test3", Phone: &Phone{Home: "123123", Primary: "456456"}},
		item{Address: "411 Test Addr", Name: "Test4", Phone: &Phone{Home: "123123", Primary: "456456"}},
	)
	assert.NoError(t, err)
	eie := new(insert).
		SetCols(NewColumnListExpression("primary_phone", "home_phone", "address", "name")).
		SetVals([][]interface{}{
			{"456456", "123123", "111 Test Addr", "Test1"},
			{"456456", "123123", "211 Test Addr", "Test2"},
			{"456456", "123123", "311 Test Addr", "Test3"},
			{"456456", "123123", "411 Test Addr", "Test4"},
		})
	assert.Equal(t, eie, ie)
	assert.False(t, ie.IsEmpty())
	assert.False(t, ie.IsInsertFrom())
}

func TestInsertExpressionSuite(t *testing.T) {
	suite.Run(t, new(insertExpressionTestSuite))
}
