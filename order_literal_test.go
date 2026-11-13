package goqu_test

import (
	"testing"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/mysql"
	"github.com/stretchr/testify/assert"
)

func TestOrderLiteral(t *testing.T) {
	sql, args, err := goqu.From("t").
		Select("a").
		Order(goqu.OrderLiteral("a DESC WITH FILL FROM ? TO ?", 1, 5), goqu.C("b").Asc()).
		OrderAppend(goqu.OrderLiteral("c")).
		ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `SELECT "a" FROM "t" ORDER BY a DESC WITH FILL FROM 1 TO 5, "b" ASC, c`, sql)

	sql, args, err = goqu.Dialect("mysql").Update("t").Set(goqu.Record{"a": 1}).Order(goqu.OrderLiteral("b DESC")).Limit(1).ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, "UPDATE `t` SET `a`=1 ORDER BY b DESC LIMIT 1", sql)

	sql, args, err = goqu.Dialect("mysql").Delete("t").Order(goqu.OrderLiteral("b DESC")).Limit(1).ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, "DELETE FROM `t` ORDER BY b DESC LIMIT 1", sql)
}
