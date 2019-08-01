package exp

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type updateExpressionTestSuite struct {
	suite.Suite
}

func (uets *updateExpressionTestSuite) TestNewUpdateExpressions_withInvalidValue() {
	_, err := NewUpdateExpressions(true)
	uets.EqualError(err, "goqu: unsupported update interface type bool")
}

func (uets *updateExpressionTestSuite) TestNewUpdateExpressions_withRecords() {
	ie, err := NewUpdateExpressions(Record{"c": "a", "b": "d"})
	uets.NoError(err)
	eie := []UpdateExpression{
		update{col: ParseIdentifier("b"), val: "d"},
		update{col: ParseIdentifier("c"), val: "a"},
	}
	uets.Equal(eie, ie)
}

func (uets *updateExpressionTestSuite) TestNewUpdateExpressions_withMap() {
	ie, err := NewUpdateExpressions(map[string]interface{}{"c": "a", "b": "d"})
	uets.NoError(err)
	eie := []UpdateExpression{
		update{col: ParseIdentifier("b"), val: "d"},
		update{col: ParseIdentifier("c"), val: "a"},
	}
	uets.Equal(eie, ie)
}

func (uets *updateExpressionTestSuite) TestNewUpdateExpressions_withStructs() {
	type testRecord struct {
		C string `db:"c"`
		B string `db:"b"`
	}
	ie, err := NewUpdateExpressions(testRecord{C: "a", B: "d"})
	uets.NoError(err)
	eie := []UpdateExpression{
		update{col: ParseIdentifier("b"), val: "d"},
		update{col: ParseIdentifier("c"), val: "a"},
	}
	uets.Equal(eie, ie)
}

func (uets *updateExpressionTestSuite) TestNewUpdateExpressions_withStructsWithoutTags() {
	type testRecord struct {
		FieldA int64
		FieldB bool
		FieldC string
	}
	ie, err := NewUpdateExpressions(testRecord{FieldA: 1, FieldB: true, FieldC: "a"})
	uets.NoError(err)
	eie := []UpdateExpression{
		update{col: ParseIdentifier("fielda"), val: int64(1)},
		update{col: ParseIdentifier("fieldb"), val: true},
		update{col: ParseIdentifier("fieldc"), val: "a"},
	}
	uets.Equal(eie, ie)
}

func (uets *updateExpressionTestSuite) TestNewUpdateExpressions_withStructsIgnoredDbTag() {
	type testRecord struct {
		FieldA int64 `db:"-"`
		FieldB bool
		FieldC string
	}
	ie, err := NewUpdateExpressions(testRecord{FieldA: 1, FieldB: true, FieldC: "a"})
	uets.NoError(err)
	eie := []UpdateExpression{
		update{col: ParseIdentifier("fieldb"), val: true},
		update{col: ParseIdentifier("fieldc"), val: "a"},
	}
	uets.Equal(eie, ie)
}

func (uets *updateExpressionTestSuite) TestNewUpdateExpressions_withStructsWithGoquSkipUpdate() {
	type testRecord struct {
		FieldA int64
		FieldB bool   `goqu:"skipupdate"`
		FieldC string `goqu:"skipinsert"`
	}
	ie, err := NewUpdateExpressions(testRecord{FieldA: 1, FieldB: true, FieldC: "a"})
	uets.NoError(err)
	eie := []UpdateExpression{
		update{col: ParseIdentifier("fielda"), val: int64(1)},
		update{col: ParseIdentifier("fieldc"), val: "a"},
	}
	uets.Equal(eie, ie)
}

func (uets *updateExpressionTestSuite) TestNewUpdateExpressions_withStructPointers() {
	type testRecord struct {
		C string `db:"c"`
		B string `db:"b"`
	}
	ie, err := NewUpdateExpressions(&testRecord{C: "a", B: "d"})
	uets.NoError(err)
	eie := []UpdateExpression{
		update{col: ParseIdentifier("b"), val: "d"},
		update{col: ParseIdentifier("c"), val: "a"},
	}
	uets.Equal(eie, ie)
}

func (uets *updateExpressionTestSuite) TestNewUpdateExpressions_withStructsWithEmbeddedStructs() {
	type Phone struct {
		Primary string `db:"primary_phone"`
		Home    string `db:"home_phone"`
	}
	type item struct {
		Phone
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	ie, err := NewUpdateExpressions(
		item{Address: "111 Test Addr", Name: "Test1", Phone: Phone{Home: "123123", Primary: "456456"}},
	)
	uets.NoError(err)
	eie := []UpdateExpression{
		update{col: ParseIdentifier("address"), val: "111 Test Addr"},
		update{col: ParseIdentifier("home_phone"), val: "123123"},
		update{col: ParseIdentifier("name"), val: "Test1"},
		update{col: ParseIdentifier("primary_phone"), val: "456456"},
	}
	uets.Equal(eie, ie)
}

func (uets *updateExpressionTestSuite) TestNewUpdateExpressions_withStructsWithEmbeddedStructPointers() {
	type Phone struct {
		Primary string `db:"primary_phone"`
		Home    string `db:"home_phone"`
	}
	type item struct {
		*Phone
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	ie, err := NewUpdateExpressions(
		item{Address: "111 Test Addr", Name: "Test1", Phone: &Phone{Home: "123123", Primary: "456456"}},
	)
	uets.NoError(err)
	eie := []UpdateExpression{
		update{col: ParseIdentifier("address"), val: "111 Test Addr"},
		update{col: ParseIdentifier("home_phone"), val: "123123"},
		update{col: ParseIdentifier("name"), val: "Test1"},
		update{col: ParseIdentifier("primary_phone"), val: "456456"},
	}
	uets.Equal(eie, ie)
}

func (uets *updateExpressionTestSuite) TestNewUpdateExpressions_withNilEmbeddedStructPointers() {
	type Phone struct {
		Primary string `db:"primary_phone"`
		Home    string `db:"home_phone"`
	}
	type item struct {
		*Phone
		Address string `db:"address"`
		Name    string `db:"name"`
	}
	ie, err := NewUpdateExpressions(
		item{Address: "111 Test Addr", Name: "Test1"},
	)
	uets.NoError(err)
	eie := []UpdateExpression{
		update{col: ParseIdentifier("address"), val: "111 Test Addr"},
		update{col: ParseIdentifier("name"), val: "Test1"},
	}
	uets.Equal(eie, ie)
}

func TestUpdateExpressionSuite(t *testing.T) {
	suite.Run(t, new(updateExpressionTestSuite))
}
