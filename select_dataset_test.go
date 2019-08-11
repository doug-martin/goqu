package goqu

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/doug-martin/goqu/v8/exec"
	"github.com/doug-martin/goqu/v8/exp"
	"github.com/doug-martin/goqu/v8/internal/errors"
	"github.com/doug-martin/goqu/v8/internal/sb"
	"github.com/doug-martin/goqu/v8/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type dsTestActionItem struct {
	Address string `db:"address"`
	Name    string `db:"name"`
}

type selectDatasetSuite struct {
	suite.Suite
}

func (sds *selectDatasetSuite) TestClone() {
	t := sds.T()
	ds := From("test")
	assert.Equal(t, ds.Clone(), ds)
}

func (sds *selectDatasetSuite) TestExpression() {
	t := sds.T()
	ds := From("test")
	assert.Equal(t, ds.Expression(), ds)
}

func (sds *selectDatasetSuite) TestDialect() {
	t := sds.T()
	ds := From("test")
	assert.NotNil(t, ds.Dialect())
}

func (sds *selectDatasetSuite) TestWithDialect() {
	t := sds.T()
	ds := From("test")
	md := new(mocks.SQLDialect)
	ds = ds.SetDialect(md)

	dialect := GetDialect("default")
	ds = ds.WithDialect("default")
	assert.Equal(t, ds.Dialect(), dialect)
}

func (sds *selectDatasetSuite) TestPrepared() {
	t := sds.T()
	ds := From("test")
	preparedDs := ds.Prepared(true)
	assert.True(t, preparedDs.IsPrepared())
	assert.False(t, ds.IsPrepared())
	// should apply the prepared to any datasets created from the root
	assert.True(t, preparedDs.Where(Ex{"a": 1}).IsPrepared())
}

func (sds *selectDatasetSuite) TestGetClauses() {
	t := sds.T()
	ds := From("test")
	ce := exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression(I("test")))
	assert.Equal(t, ce, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestWith() {
	t := sds.T()
	from := From("cte")
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.CommonTablesAppend(exp.NewCommonTableExpression(false, "test-cte", from))
	assert.Equal(t, ec, ds.With("test-cte", from).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestWithRecursive() {
	t := sds.T()
	from := From("cte")
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.CommonTablesAppend(exp.NewCommonTableExpression(true, "test-cte", from))
	assert.Equal(t, ec, ds.WithRecursive("test-cte", from).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestSelect() {
	t := sds.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetSelect(exp.NewColumnListExpression(C("a")))
	assert.Equal(t, ec, ds.Select(C("a")).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestSelect_ToSQL() {
	t := sds.T()
	ds1 := From("test")

	selectSQL, _, err := ds1.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test"`)

	selectSQL, _, err = ds1.Select().ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test"`)

	selectSQL, _, err = ds1.Select("id").ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT "id" FROM "test"`)

	selectSQL, _, err = ds1.Select("id", "name").ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT "id", "name" FROM "test"`)

	selectSQL, _, err = ds1.Select(L("COUNT(*)").As("count")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT COUNT(*) AS "count" FROM "test"`)

	selectSQL, _, err = ds1.Select(C("id").As("other_id"), L("COUNT(*)").As("count")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT "id" AS "other_id", COUNT(*) AS "count" FROM "test"`)

	selectSQL, _, err = ds1.From().Select(ds1.From("test_1").Select("id")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT (SELECT "id" FROM "test_1")`)

	selectSQL, _, err = ds1.From().Select(ds1.From("test_1").Select("id").As("test_id")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT (SELECT "id" FROM "test_1") AS "test_id"`)

	selectSQL, _, err = ds1.From().
		Select(
			DISTINCT("a").As("distinct"),
			COUNT("a").As("count"),
			L("CASE WHEN ? THEN ? ELSE ? END", MIN("a").Eq(10), true, false),
			L("CASE WHEN ? THEN ? ELSE ? END", AVG("a").Neq(10), true, false),
			L("CASE WHEN ? THEN ? ELSE ? END", FIRST("a").Gt(10), true, false),
			L("CASE WHEN ? THEN ? ELSE ? END", FIRST("a").Gte(10), true, false),
			L("CASE WHEN ? THEN ? ELSE ? END", LAST("a").Lt(10), true, false),
			L("CASE WHEN ? THEN ? ELSE ? END", LAST("a").Lte(10), true, false),
			SUM("a").As("sum"),
			COALESCE(C("a"), "a").As("colaseced"),
		).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT `+
		`DISTINCT("a") AS "distinct", `+
		`COUNT("a") AS "count", `+
		`CASE WHEN (MIN("a") = 10) THEN TRUE ELSE FALSE END, `+
		`CASE WHEN (AVG("a") != 10) THEN TRUE ELSE FALSE END, `+
		`CASE WHEN (FIRST("a") > 10) THEN TRUE ELSE FALSE END, `+
		`CASE WHEN (FIRST("a") >= 10) THEN TRUE ELSE FALSE END,`+
		` CASE WHEN (LAST("a") < 10) THEN TRUE ELSE FALSE END, `+
		`CASE WHEN (LAST("a") <= 10) THEN TRUE ELSE FALSE END, `+
		`SUM("a") AS "sum", `+
		`COALESCE("a", 'a') AS "colaseced"`)

	type MyStruct struct {
		Name         string
		Address      string `db:"address"`
		EmailAddress string `db:"email_address"`
		FakeCol      string `db:"-"`
	}
	selectSQL, _, err = ds1.Select(&MyStruct{}).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT "address", "email_address", "name" FROM "test"`)

	selectSQL, _, err = ds1.Select(MyStruct{}).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT "address", "email_address", "name" FROM "test"`)

	type myStruct2 struct {
		MyStruct
		Zipcode string `db:"zipcode"`
	}

	selectSQL, _, err = ds1.Select(&myStruct2{}).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT "address", "email_address", "name", "zipcode" FROM "test"`)

	selectSQL, _, err = ds1.Select(myStruct2{}).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT "address", "email_address", "name", "zipcode" FROM "test"`)

	var myStructs []MyStruct
	selectSQL, _, err = ds1.Select(&myStructs).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT "address", "email_address", "name" FROM "test"`)

	selectSQL, _, err = ds1.Select(myStructs).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT "address", "email_address", "name" FROM "test"`)
	// should not change original
	selectSQL, _, err = ds1.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test"`)
}

func (sds *selectDatasetSuite) TestSelectDistinct() {
	t := sds.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetSelect(exp.NewColumnListExpression(C("a"))).SetDistinct(exp.NewColumnListExpression())
	assert.Equal(t, ec, ds.SelectDistinct(C("a")).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestSelectDistinct_ToSQL() {
	t := sds.T()
	ds1 := From("test")

	selectSQL, _, err := ds1.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test"`)

	selectSQL, _, err = ds1.SelectDistinct("id").ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT DISTINCT "id" FROM "test"`)

	selectSQL, _, err = ds1.SelectDistinct("id", "name").ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT DISTINCT "id", "name" FROM "test"`)

	selectSQL, _, err = ds1.SelectDistinct(L("COUNT(*)").As("count")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT DISTINCT COUNT(*) AS "count" FROM "test"`)

	selectSQL, _, err = ds1.SelectDistinct(C("id").As("other_id"), L("COUNT(*)").As("count")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT DISTINCT "id" AS "other_id", COUNT(*) AS "count" FROM "test"`)

	type MyStruct struct {
		Name         string
		Address      string `db:"address"`
		EmailAddress string `db:"email_address"`
		FakeCol      string `db:"-"`
	}
	selectSQL, _, err = ds1.SelectDistinct(&MyStruct{}).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT DISTINCT "address", "email_address", "name" FROM "test"`)

	selectSQL, _, err = ds1.SelectDistinct(MyStruct{}).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT DISTINCT "address", "email_address", "name" FROM "test"`)

	type myStruct2 struct {
		MyStruct
		Zipcode string `db:"zipcode"`
	}

	selectSQL, _, err = ds1.SelectDistinct(&myStruct2{}).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT DISTINCT "address", "email_address", "name", "zipcode" FROM "test"`)

	selectSQL, _, err = ds1.SelectDistinct(myStruct2{}).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT DISTINCT "address", "email_address", "name", "zipcode" FROM "test"`)

	var myStructs []MyStruct
	selectSQL, _, err = ds1.SelectDistinct(&myStructs).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT DISTINCT "address", "email_address", "name" FROM "test"`)

	selectSQL, _, err = ds1.SelectDistinct(myStructs).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT DISTINCT "address", "email_address", "name" FROM "test"`)
	// should not change original
	selectSQL, _, err = ds1.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test"`)
	// should not change original
	selectSQL, _, err = ds1.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test"`)
}

func (sds *selectDatasetSuite) TestDistinct() {
	t := sds.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetDistinct(exp.NewColumnListExpression())
	ecs := dsc.SetSelect(exp.NewColumnListExpression("a", "b")).SetDistinct(exp.NewColumnListExpression())
	ecsd := dsc.SetSelect(exp.NewColumnListExpression("a", "b")).SetDistinct(exp.NewColumnListExpression("c"))
	assert.Equal(t, ec, ds.Distinct().GetClauses())
	assert.Equal(t, ecs, ds.Select("a", "b").Distinct().GetClauses())
	assert.Equal(t, ecsd, ds.Select("a", "b").Distinct("c").GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestDistinct_ToSQL() {
	t := sds.T()
	ds1 := From("test")

	selectSQL, _, err := ds1.Distinct().ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT DISTINCT * FROM "test"`)

	selectSQL, _, err = ds1.Distinct("id").ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT DISTINCT ON ("id") * FROM "test"`)

	selectSQL, _, err = ds1.Distinct("id").Select("name").ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT DISTINCT ON ("id") "name" FROM "test"`)

	selectSQL, _, err = ds1.Select(L("COUNT(*)").As("count")).Distinct(COALESCE(C("b"), "empty")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT DISTINCT ON (COALESCE("b", 'empty')) COUNT(*) AS "count" FROM "test"`)

	// should not change original
	selectSQL, _, err = ds1.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test"`)
	// should not change original
	selectSQL, _, err = ds1.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test"`)
}

func (sds *selectDatasetSuite) TestClearSelect() {
	t := sds.T()
	ds := From("test").Select(C("a"))
	dsc := ds.GetClauses()
	ec := dsc.SetSelect(exp.NewColumnListExpression(Star()))
	assert.Equal(t, ec, ds.ClearSelect().GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestClearSelect_ToSQL() {
	t := sds.T()
	ds1 := From("test")

	selectSQL, _, err := ds1.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test"`)

	b := ds1.Select("a").ClearSelect()
	selectSQL, _, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test"`)
}

func (sds *selectDatasetSuite) TestSelectAppend(selects ...interface{}) {
	t := sds.T()
	ds := From("test").Select(C("a"))
	dsc := ds.GetClauses()
	ec := dsc.SelectAppend(exp.NewColumnListExpression(C("b")))
	assert.Equal(t, ec, ds.SelectAppend(C("b")).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestSelectAppend_ToSQL() {
	t := sds.T()
	ds1 := From("test")

	selectSQL, _, err := ds1.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test"`)

	b := ds1.Select("a").SelectAppend("b").SelectAppend("c")
	selectSQL, _, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT "a", "b", "c" FROM "test"`)
}

func (sds *selectDatasetSuite) TestFrom() {
	t := sds.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetFrom(exp.NewColumnListExpression(T("t")))
	assert.Equal(t, ec, ds.From(T("t")).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestFrom_ToSQL() {
	t := sds.T()
	ds1 := From("test")

	selectSQL, _, err := ds1.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test"`)

	ds2 := ds1.From("test2")
	selectSQL, _, err = ds2.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test2"`)

	ds2 = ds1.From("test2", "test3")
	selectSQL, _, err = ds2.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test2", "test3"`)

	ds2 = ds1.From(T("test2").As("test_2"), "test3")
	selectSQL, _, err = ds2.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test2" AS "test_2", "test3"`)

	ds2 = ds1.From(ds1.From("test2"), "test3")
	selectSQL, _, err = ds2.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM (SELECT * FROM "test2") AS "t1", "test3"`)

	ds2 = ds1.From(ds1.From("test2").As("test_2"), "test3")
	selectSQL, _, err = ds2.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM (SELECT * FROM "test2") AS "test_2", "test3"`)
	// should not change original
	selectSQL, _, err = ds1.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test"`)
}

func (sds *selectDatasetSuite) TestFromSelf() {
	t := sds.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetFrom(exp.NewColumnListExpression(ds.As("t1")))
	assert.Equal(t, ec, ds.FromSelf().GetClauses())

	ec2 := dsc.SetFrom(exp.NewColumnListExpression(ds.As("test")))
	assert.Equal(t, ec2, ds.As("test").FromSelf().GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestCompoundFromSelf() {
	t := sds.T()
	ds := From("test")
	dsc := ds.GetClauses()
	assert.Equal(t, dsc, ds.CompoundFromSelf().GetClauses())

	ds2 := ds.Limit(1)
	dsc2 := exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression(ds2.As("t1")))
	assert.Equal(t, dsc2, ds2.CompoundFromSelf().GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestJoin() {
	t := sds.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.JoinsAppend(
		exp.NewConditionedJoinExpression(exp.InnerJoinType, T("foo"), On(C("a").IsNull())),
	)
	assert.Equal(t, ec, ds.Join(T("foo"), On(C("a").IsNull())).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestJoin_ToSQL() {
	t := sds.T()
	ds1 := From("items")

	b := ds1.Join(T("players").As("p"), On(Ex{"p.id": I("items.playerId")}))
	selectSQL, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" `+
		`INNER JOIN "players" AS "p" ON ("p"."id" = "items"."playerId")`)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" `+
		`INNER JOIN "players" AS "p" ON ("p"."id" = "items"."playerId")`)

	b = ds1.Join(ds1.From("players").As("p"), On(Ex{"p.id": I("items.playerId")}))
	selectSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" `+
		`INNER JOIN (SELECT * FROM "players") AS "p" ON ("p"."id" = "items"."playerId")`)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" `+
		`INNER JOIN (SELECT * FROM "players") AS "p" ON ("p"."id" = "items"."playerId")`)

	b = ds1.Join(S("v1").Table("test"), On(Ex{"v1.test.id": I("items.playerId")}))
	selectSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" `+
		`INNER JOIN "v1"."test" ON ("v1"."test"."id" = "items"."playerId")`)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" `+
		`INNER JOIN "v1"."test" ON ("v1"."test"."id" = "items"."playerId")`)

	b = ds1.Join(T("test"), Using(C("name"), C("common_id")))
	selectSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" INNER JOIN "test" USING ("name", "common_id")`)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" INNER JOIN "test" USING ("name", "common_id")`)

	b = ds1.Join(T("test"), Using("name", "common_id"))
	selectSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" INNER JOIN "test" USING ("name", "common_id")`)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" INNER JOIN "test" USING ("name", "common_id")`)

	b = ds1.Join(
		T("categories"),
		On(
			I("categories.categoryId").Eq(I("items.id")),
			I("categories.categoryId").In(1, 2, 3),
		),
	)

	selectSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" `+
		`INNER JOIN "categories" ON (`+
		`("categories"."categoryId" = "items"."id") AND ("categories"."categoryId" IN (1, 2, 3))`+
		`)`)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1), int64(2), int64(3)})
	assert.Equal(t, selectSQL, `SELECT * FROM "items" `+
		`INNER JOIN "categories" ON (`+
		`("categories"."categoryId" = "items"."id") AND ("categories"."categoryId" IN (?, ?, ?))`+
		`)`)
}

func (sds *selectDatasetSuite) TestInnerJoin() {
	t := sds.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.JoinsAppend(
		exp.NewConditionedJoinExpression(exp.InnerJoinType, T("foo"), On(C("a").IsNull())),
	)
	assert.Equal(t, ec, ds.InnerJoin(T("foo"), On(C("a").IsNull())).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestInnerJoin_ToSQL() {
	t := sds.T()
	ds1 := From("items")
	selectSQL, _, err := ds1.
		InnerJoin(T("b"), On(Ex{"b.itemsId": I("items.id")})).
		LeftOuterJoin(T("c"), On(Ex{"c.b_id": I("b.id")})).
		ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" `+
		`INNER JOIN "b" ON ("b"."itemsId" = "items"."id") `+
		`LEFT OUTER JOIN "c" ON ("c"."b_id" = "b"."id")`)

	selectSQL, _, err = ds1.
		InnerJoin(T("b"), On(Ex{"b.itemsId": I("items.id")})).
		LeftOuterJoin(T("c"), On(Ex{"c.b_id": I("b.id")})).
		ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" `+
		`INNER JOIN "b" ON ("b"."itemsId" = "items"."id") `+
		`LEFT OUTER JOIN "c" ON ("c"."b_id" = "b"."id")`)

	selectSQL, _, err = ds1.InnerJoin(
		T("categories"),
		On(Ex{"categories.categoryId": I("items.id")}),
	).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" `+
		`INNER JOIN "categories" ON ("categories"."categoryId" = "items"."id")`)
}

func (sds *selectDatasetSuite) TestFullOuterJoin() {
	t := sds.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.JoinsAppend(
		exp.NewConditionedJoinExpression(exp.FullOuterJoinType, T("foo"), On(C("a").IsNull())),
	)
	assert.Equal(t, ec, ds.FullOuterJoin(T("foo"), On(C("a").IsNull())).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestFullOuterJoin_ToSQL() {
	t := sds.T()
	ds1 := From("items")
	selectSQL, _, err := ds1.
		FullOuterJoin(T("categories"), On(Ex{"categories.categoryId": I("items.id")})).
		Order(C("stamp").Asc()).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" `+
		`FULL OUTER JOIN "categories" ON ("categories"."categoryId" = "items"."id") ORDER BY "stamp" ASC`)

	selectSQL, _, err = ds1.FullOuterJoin(
		T("categories"),
		On(Ex{"categories.categoryId": I("items.id")}),
	).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" `+
		`FULL OUTER JOIN "categories" ON ("categories"."categoryId" = "items"."id")`)
}

func (sds *selectDatasetSuite) TestRightOuterJoin() {
	t := sds.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.JoinsAppend(
		exp.NewConditionedJoinExpression(exp.RightOuterJoinType, T("foo"), On(C("a").IsNull())),
	)
	assert.Equal(t, ec, ds.RightOuterJoin(T("foo"), On(C("a").IsNull())).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestRightOuterJoin_ToSQL() {
	t := sds.T()
	ds1 := From("items")
	selectSQL, _, err := ds1.RightOuterJoin(
		T("categories"),
		On(Ex{"categories.categoryId": I("items.id")}),
	).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" `+
		`RIGHT OUTER JOIN "categories" ON ("categories"."categoryId" = "items"."id")`)
}

func (sds *selectDatasetSuite) TestLeftOuterJoin() {
	t := sds.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.JoinsAppend(
		exp.NewConditionedJoinExpression(exp.LeftOuterJoinType, T("foo"), On(C("a").IsNull())),
	)
	assert.Equal(t, ec, ds.LeftOuterJoin(T("foo"), On(C("a").IsNull())).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestLeftOuterJoin_ToSQL() {
	t := sds.T()
	ds1 := From("items")

	selectSQL, _, err := ds1.LeftOuterJoin(T("categories"), On(Ex{
		"categories.categoryId": I("items.id"),
	})).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" `+
		`LEFT OUTER JOIN "categories" ON ("categories"."categoryId" = "items"."id")`)

	selectSQL, _, err = ds1.
		LeftOuterJoin(
			T("categories"),
			On(
				I("categories.categoryId").Eq(I("items.id")),
				I("categories.categoryId").In(1, 2, 3)),
		).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, `SELECT * FROM "items" `+
		`LEFT OUTER JOIN "categories" `+
		`ON (("categories"."categoryId" = "items"."id") AND ("categories"."categoryId" IN (1, 2, 3)))`, selectSQL)

}

func (sds *selectDatasetSuite) TestFullJoin() {
	t := sds.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.JoinsAppend(
		exp.NewConditionedJoinExpression(exp.FullJoinType, T("foo"), On(C("a").IsNull())),
	)
	assert.Equal(t, ec, ds.FullJoin(T("foo"), On(C("a").IsNull())).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestFullJoin_ToSQL() {
	t := sds.T()
	ds1 := From("items")
	selectSQL, _, err := ds1.FullJoin(
		T("categories"),
		On(Ex{"categories.categoryId": I("items.id")}),
	).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" `+
		`FULL JOIN "categories" ON ("categories"."categoryId" = "items"."id")`)
}

func (sds *selectDatasetSuite) TestRightJoin() {
	t := sds.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.JoinsAppend(
		exp.NewConditionedJoinExpression(exp.RightJoinType, T("foo"), On(C("a").IsNull())),
	)
	assert.Equal(t, ec, ds.RightJoin(T("foo"), On(C("a").IsNull())).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestRightJoin_ToSQL() {
	t := sds.T()
	ds1 := From("items")
	selectSQL, _, err := ds1.RightJoin(
		T("categories"),
		On(Ex{"categories.categoryId": I("items.id")}),
	).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" `+
		`RIGHT JOIN "categories" ON ("categories"."categoryId" = "items"."id")`)
}

func (sds *selectDatasetSuite) TestLeftJoin() {
	t := sds.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.JoinsAppend(
		exp.NewConditionedJoinExpression(exp.LeftJoinType, T("foo"), On(C("a").IsNull())),
	)
	assert.Equal(t, ec, ds.LeftJoin(T("foo"), On(C("a").IsNull())).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestLeftJoin_ToSQL() {
	t := sds.T()
	ds1 := From("items")
	selectSQL, _, err := ds1.LeftJoin(
		T("categories"),
		On(Ex{"categories.categoryId": I("items.id")}),
	).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" `+
		`LEFT JOIN "categories" ON ("categories"."categoryId" = "items"."id")`)
}

func (sds *selectDatasetSuite) TestNaturalJoin() {
	t := sds.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.JoinsAppend(
		exp.NewUnConditionedJoinExpression(exp.NaturalJoinType, T("foo")),
	)
	assert.Equal(t, ec, ds.NaturalJoin(T("foo")).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestNaturalJoin_ToSQL() {
	t := sds.T()
	ds1 := From("items")
	selectSQL, _, err := ds1.NaturalJoin(T("categories")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" NATURAL JOIN "categories"`)
}

func (sds *selectDatasetSuite) TestNaturalLeftJoin() {
	t := sds.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.JoinsAppend(
		exp.NewUnConditionedJoinExpression(exp.NaturalLeftJoinType, T("foo")),
	)
	assert.Equal(t, ec, ds.NaturalLeftJoin(T("foo")).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestNaturalLeftJoin_ToSQL() {
	t := sds.T()
	ds1 := From("items")
	selectSQL, _, err := ds1.NaturalLeftJoin(T("categories")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" NATURAL LEFT JOIN "categories"`)

}

func (sds *selectDatasetSuite) TestNaturalRightJoin_ToSQL() {
	t := sds.T()
	ds1 := From("items")
	selectSQL, _, err := ds1.NaturalRightJoin(T("categories")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" NATURAL RIGHT JOIN "categories"`)
}

func (sds *selectDatasetSuite) TestNaturalRightJoin() {
	t := sds.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.JoinsAppend(
		exp.NewUnConditionedJoinExpression(exp.NaturalRightJoinType, T("foo")),
	)
	assert.Equal(t, ec, ds.NaturalRightJoin(T("foo")).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}
func (sds *selectDatasetSuite) TestNaturalFullJoin() {
	t := sds.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.JoinsAppend(
		exp.NewUnConditionedJoinExpression(exp.NaturalFullJoinType, T("foo")),
	)
	assert.Equal(t, ec, ds.NaturalFullJoin(T("foo")).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestNaturalFullJoin_ToSQL() {
	t := sds.T()
	ds1 := From("items")
	selectSQL, _, err := ds1.NaturalFullJoin(T("categories")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" NATURAL FULL JOIN "categories"`)
}

func (sds *selectDatasetSuite) TestCrossJoin() {
	t := sds.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.JoinsAppend(
		exp.NewUnConditionedJoinExpression(exp.CrossJoinType, T("foo")),
	)
	assert.Equal(t, ec, ds.CrossJoin(T("foo")).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestCrossJoin_ToSQL() {
	t := sds.T()
	selectSQL, _, err := From("items").CrossJoin(T("categories")).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "items" CROSS JOIN "categories"`)
}

func (sds *selectDatasetSuite) TestWhere() {
	t := sds.T()
	ds := From("test")
	dsc := ds.GetClauses()
	w := Ex{
		"a": 1,
	}
	ec := dsc.WhereAppend(w)
	assert.Equal(t, ec, ds.Where(w).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestWhere_ToSQL() {
	t := sds.T()
	ds1 := From("test")

	b := ds1.Where(
		C("a").Eq(true),
		C("a").Neq(true),
		C("a").Eq(false),
		C("a").Neq(false),
	)
	selectSQL, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" `+
		`WHERE (("a" IS TRUE) AND ("a" IS NOT TRUE) AND ("a" IS FALSE) AND ("a" IS NOT FALSE))`)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" `+
		`WHERE (("a" IS TRUE) AND ("a" IS NOT TRUE) AND ("a" IS FALSE) AND ("a" IS NOT FALSE))`)

	b = ds1.Where(
		C("a").Eq("a"),
		C("b").Neq("b"),
		C("c").Gt("c"),
		C("d").Gte("d"),
		C("e").Lt("e"),
		C("f").Lte("f"),
	)
	selectSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" `+
		`WHERE (("a" = 'a') AND ("b" != 'b') AND ("c" > 'c') AND ("d" >= 'd') AND ("e" < 'e') AND ("f" <= 'f'))`)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{"a", "b", "c", "d", "e", "f"}, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" `+
		`WHERE (("a" = ?) AND ("b" != ?) AND ("c" > ?) AND ("d" >= ?) AND ("e" < ?) AND ("f" <= ?))`)

	b = ds1.Where(
		C("a").Eq(From("test2").Select("id")),
	)
	selectSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" IN (SELECT "id" FROM "test2"))`)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" IN (SELECT "id" FROM "test2"))`)

	b = ds1.Where(Ex{
		"a": "a",
		"b": Op{"neq": "b"},
		"c": Op{"gt": "c"},
		"d": Op{"gte": "d"},
		"e": Op{"lt": "e"},
		"f": Op{"lte": "f"},
	})
	selectSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" `+
		`WHERE (("a" = 'a') AND ("b" != 'b') AND ("c" > 'c') AND ("d" >= 'd') AND ("e" < 'e') AND ("f" <= 'f'))`)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{"a", "b", "c", "d", "e", "f"}, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" `+
		`WHERE (("a" = ?) AND ("b" != ?) AND ("c" > ?) AND ("d" >= ?) AND ("e" < ?) AND ("f" <= ?))`)

	b = ds1.Where(Ex{
		"a": From("test2").Select("id"),
	})
	selectSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" IN (SELECT "id" FROM "test2"))`)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" IN (SELECT "id" FROM "test2"))`)
}

func (sds *selectDatasetSuite) TestWhere_ToSQLEmpty() {
	t := sds.T()
	ds1 := From("test")

	b := ds1.Where()
	selectSQL, _, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test"`)
}

func (sds *selectDatasetSuite) TestWhere_ToSQLWithChain() {
	t := sds.T()
	ds1 := From("test").Where(
		C("x").Eq(0),
		C("y").Eq(1),
	)

	ds2 := ds1.Where(
		C("z").Eq(2),
	)

	a := ds2.Where(
		C("a").Eq("A"),
	)
	b := ds2.Where(
		C("b").Eq("B"),
	)
	selectSQL, _, err := a.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" `+
		`WHERE (("x" = 0) AND ("y" = 1) AND ("z" = 2) AND ("a" = 'A'))`)
	selectSQL, _, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" `+
		`WHERE (("x" = 0) AND ("y" = 1) AND ("z" = 2) AND ("b" = 'B'))`)
}

func (sds *selectDatasetSuite) TestClearWhere() {
	t := sds.T()
	w := Ex{
		"a": 1,
	}
	ds := From("test").Where(w)
	dsc := ds.GetClauses()
	ec := dsc.ClearWhere()
	assert.Equal(t, ec, ds.ClearWhere().GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestClearWhere_ToSQL() {
	t := sds.T()
	ds1 := From("test")

	b := ds1.Where(
		C("a").Eq(1),
	).ClearWhere()
	selectSQL, _, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, selectSQL, `SELECT * FROM "test"`)
}

func (sds *selectDatasetSuite) TestForUpdate() {
	t := sds.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetLock(exp.NewLock(exp.ForUpdate, NoWait))
	assert.Equal(t, ec, ds.ForUpdate(NoWait).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestForUpdate_ToSQL() {
	t := sds.T()
	ds1 := From("test")

	b := ds1.Where(C("a").Gt(1)).ForUpdate(Wait)
	selectSQL, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1) FOR UPDATE `)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(1)}, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > ?) FOR UPDATE `)

	b = ds1.Where(C("a").Gt(1)).ForUpdate(NoWait)
	selectSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1) FOR UPDATE NOWAIT`)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(1)}, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > ?) FOR UPDATE NOWAIT`)

	b = ds1.Where(C("a").Gt(1)).ForUpdate(SkipLocked)
	selectSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1) FOR UPDATE SKIP LOCKED`)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(1)}, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > ?) FOR UPDATE SKIP LOCKED`)
}

func (sds *selectDatasetSuite) TestForNoKeyUpdate() {
	t := sds.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetLock(exp.NewLock(exp.ForNoKeyUpdate, NoWait))
	assert.Equal(t, ec, ds.ForNoKeyUpdate(NoWait).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestForNoKeyUpdate_ToSQL() {
	t := sds.T()
	ds1 := From("test")

	b := ds1.Where(C("a").Gt(1)).ForNoKeyUpdate(Wait)
	selectSQL, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1) FOR NO KEY UPDATE `)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(1)}, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > ?) FOR NO KEY UPDATE `)

	b = ds1.Where(C("a").Gt(1)).ForNoKeyUpdate(NoWait)
	selectSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1) FOR NO KEY UPDATE NOWAIT`)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(1)}, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > ?) FOR NO KEY UPDATE NOWAIT`)

	b = ds1.Where(C("a").Gt(1)).ForNoKeyUpdate(SkipLocked)
	selectSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1) FOR NO KEY UPDATE SKIP LOCKED`)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(1)}, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > ?) FOR NO KEY UPDATE SKIP LOCKED`)
}

func (sds *selectDatasetSuite) TestForKeyShare() {
	t := sds.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetLock(exp.NewLock(exp.ForKeyShare, NoWait))
	assert.Equal(t, ec, ds.ForKeyShare(NoWait).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestForKeyShare_ToSQL() {
	t := sds.T()
	ds1 := From("test")

	b := ds1.Where(C("a").Gt(1)).ForKeyShare(Wait)
	selectSQL, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1) FOR KEY SHARE `)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(1)}, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > ?) FOR KEY SHARE `)

	b = ds1.Where(C("a").Gt(1)).ForKeyShare(NoWait)
	selectSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1) FOR KEY SHARE NOWAIT`)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(1)}, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > ?) FOR KEY SHARE NOWAIT`)

	b = ds1.Where(C("a").Gt(1)).ForKeyShare(SkipLocked)
	selectSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1) FOR KEY SHARE SKIP LOCKED`)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(1)}, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > ?) FOR KEY SHARE SKIP LOCKED`)
}

func (sds *selectDatasetSuite) TestForShare() {
	t := sds.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetLock(exp.NewLock(exp.ForShare, NoWait))
	assert.Equal(t, ec, ds.ForShare(NoWait).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestForShare_ToSQL() {
	t := sds.T()
	ds1 := From("test")

	b := ds1.Where(C("a").Gt(1)).ForShare(Wait)
	selectSQL, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1) FOR SHARE `)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(1)}, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > ?) FOR SHARE `)

	b = ds1.Where(C("a").Gt(1)).ForShare(NoWait)
	selectSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1) FOR SHARE NOWAIT`)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(1)}, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > ?) FOR SHARE NOWAIT`)

	b = ds1.Where(C("a").Gt(1)).ForShare(SkipLocked)
	selectSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1) FOR SHARE SKIP LOCKED`)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(1)}, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > ?) FOR SHARE SKIP LOCKED`)
}

func (sds *selectDatasetSuite) TestGroupBy() {
	t := sds.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetGroupBy(exp.NewColumnListExpression(C("a")))
	assert.Equal(t, ec, ds.GroupBy("a").GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestGroupBy_ToSQL() {
	t := sds.T()
	ds1 := From("test")

	b := ds1.Where(C("a").Gt(1)).GroupBy("created")
	selectSQL, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1) GROUP BY "created"`)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(1)}, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > ?) GROUP BY "created"`)

	b = ds1.Where(C("a").Gt(1)).GroupBy(L("created::DATE"))
	selectSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1) GROUP BY created::DATE`)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(1)}, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > ?) GROUP BY created::DATE`)

	b = ds1.Where(C("a").Gt(1)).GroupBy("name", L("created::DATE"))
	selectSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1) GROUP BY "name", created::DATE`)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(1)}, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > ?) GROUP BY "name", created::DATE`)
}

func (sds *selectDatasetSuite) TestHaving() {
	t := sds.T()
	ds := From("test")
	dsc := ds.GetClauses()
	h := C("a").Gt(1)
	ec := dsc.HavingAppend(h)
	assert.Equal(t, ec, ds.Having(h).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestHaving_ToSQL() {
	t := sds.T()
	ds1 := From("test")

	b := ds1.Having(Ex{"a": Op{"gt": 1}}).GroupBy("created")
	selectSQL, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" GROUP BY "created" HAVING ("a" > 1)`)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(1)}, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" GROUP BY "created" HAVING ("a" > ?)`)

	b = ds1.Where(Ex{"b": true}).Having(Ex{"a": Op{"gt": 1}}).GroupBy("created")
	selectSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("b" IS TRUE) GROUP BY "created" HAVING ("a" > 1)`)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(1)}, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("b" IS TRUE) GROUP BY "created" HAVING ("a" > ?)`)

	b = ds1.Having(Ex{"a": Op{"gt": 1}})
	selectSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" HAVING ("a" > 1)`)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(1)}, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" HAVING ("a" > ?)`)

	b = ds1.Having(Ex{"a": Op{"gt": 1}}).Having(Ex{"b": 2})
	selectSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" HAVING (("a" > 1) AND ("b" = 2))`)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(1), int64(2)}, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" HAVING (("a" > ?) AND ("b" = ?))`)

	b = ds1.GroupBy("name").Having(SUM("amount").Gt(0))
	selectSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" GROUP BY "name" HAVING (SUM("amount") > 0)`)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(0)}, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" GROUP BY "name" HAVING (SUM("amount") > ?)`)
}

func (sds *selectDatasetSuite) TestOrder() {
	t := sds.T()
	ds := From("test")
	dsc := ds.GetClauses()
	o := C("a").Desc()
	ec := dsc.SetOrder(o)
	assert.Equal(t, ec, ds.Order(o).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestOrder_ToSQL() {
	t := sds.T()

	ds1 := From("test")

	b := ds1.Order(C("a").Asc(), L(`("a" + "b" > 2)`).Asc())
	selectSQL, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" ORDER BY "a" ASC, ("a" + "b" > 2) ASC`)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" ORDER BY "a" ASC, ("a" + "b" > 2) ASC`)
}

func (sds *selectDatasetSuite) TestOrderAppend() {
	t := sds.T()
	ds := From("test").Order(C("a").Desc())
	dsc := ds.GetClauses()
	o := C("b").Desc()
	ec := dsc.OrderAppend(o)
	assert.Equal(t, ec, ds.OrderAppend(o).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestOrderAppend_ToSQL() {
	t := sds.T()
	b := From("test").Order(C("a").Asc().NullsFirst()).OrderAppend(C("b").Desc().NullsLast())
	selectSQL, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" ORDER BY "a" ASC NULLS FIRST, "b" DESC NULLS LAST`)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" ORDER BY "a" ASC NULLS FIRST, "b" DESC NULLS LAST`)

	b = From("test").OrderAppend(C("a").Asc().NullsFirst()).OrderAppend(C("b").Desc().NullsLast())
	selectSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" ORDER BY "a" ASC NULLS FIRST, "b" DESC NULLS LAST`)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" ORDER BY "a" ASC NULLS FIRST, "b" DESC NULLS LAST`)

}

func (sds *selectDatasetSuite) TestClearOrder() {
	t := sds.T()
	ds := From("test").Order(C("a").Desc())
	dsc := ds.GetClauses()
	ec := dsc.ClearOrder()
	assert.Equal(t, ec, ds.ClearOrder().GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestClearOrder_ToSQL() {
	t := sds.T()
	b := From("test").Order(C("a").Asc().NullsFirst()).ClearOrder()
	selectSQL, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test"`)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test"`)
}

func (sds *selectDatasetSuite) TestLimit() {
	t := sds.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetLimit(uint(1))
	assert.Equal(t, ec, ds.Limit(1).GetClauses())
	assert.Equal(t, dsc, ds.Limit(0).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestLimit_ToSQL() {
	t := sds.T()
	ds1 := From("test")

	b := ds1.Where(C("a").Gt(1)).Limit(10)
	selectSQL, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1) LIMIT 10`)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(1), int64(10)}, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > ?) LIMIT ?`)

	b = ds1.Where(C("a").Gt(1)).Limit(0)
	selectSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1)`)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(1)}, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > ?)`)
}

func (sds *selectDatasetSuite) TestLimitAll() {
	t := sds.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetLimit(L("ALL"))
	assert.Equal(t, ec, ds.LimitAll().GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestLimitAll_ToSQL() {
	t := sds.T()
	ds1 := From("test")

	b := ds1.Where(C("a").Gt(1)).LimitAll()
	selectSQL, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1) LIMIT ALL`)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(1)}, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > ?) LIMIT ALL`)

	b = ds1.Where(C("a").Gt(1)).Limit(0).LimitAll()
	selectSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1) LIMIT ALL`)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(1)}, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > ?) LIMIT ALL`)
}

func (sds *selectDatasetSuite) TestClearLimit() {
	t := sds.T()
	ds := From("test").Limit(1)
	dsc := ds.GetClauses()
	ec := dsc.ClearLimit()
	assert.Equal(t, ec, ds.ClearLimit().GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestClearLimit_ToSQL() {
	t := sds.T()
	ds1 := From("test")

	b := ds1.Where(C("a").Gt(1)).LimitAll().ClearLimit()
	selectSQL, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1)`)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(1)}, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > ?)`)

	b = ds1.Where(C("a").Gt(1)).Limit(10).ClearLimit()
	selectSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1)`)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(1)}, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > ?)`)
}

func (sds *selectDatasetSuite) TestOffset() {
	t := sds.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetOffset(1)
	assert.Equal(t, ec, ds.Offset(1).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestOffset_ToSQL() {
	t := sds.T()
	ds1 := From("test")

	b := ds1.Where(C("a").Gt(1)).Offset(10)
	selectSQL, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1) OFFSET 10`)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(1), int64(10)}, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > ?) OFFSET ?`)

	b = ds1.Where(C("a").Gt(1)).Offset(0)
	selectSQL, args, err = b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1)`)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(1)}, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > ?)`)
}

func (sds *selectDatasetSuite) TestClearOffset() {
	t := sds.T()
	ds := From("test").Offset(1)
	dsc := ds.GetClauses()
	ec := dsc.ClearOffset()
	assert.Equal(t, ec, ds.ClearOffset().GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestClearOffset_ToSQL() {
	t := sds.T()
	ds1 := From("test")

	b := ds1.Where(C("a").Gt(1)).Offset(10).ClearOffset()
	selectSQL, args, err := b.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > 1)`)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{int64(1)}, args)
	assert.Equal(t, selectSQL, `SELECT * FROM "test" WHERE ("a" > ?)`)
}

func (sds *selectDatasetSuite) TestUnion() {
	t := sds.T()
	uds := From("union_test")
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.CompoundsAppend(exp.NewCompoundExpression(exp.UnionCompoundType, uds))
	assert.Equal(t, ec, ds.Union(uds).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestUnion_ToSQL() {
	t := sds.T()
	a := From("invoice").Select("id", "amount").Where(C("amount").Gt(1000))
	b := From("invoice").Select("id", "amount").Where(C("amount").Lt(10))

	ds := a.Union(b)
	selectSQL, args, err := ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
		`UNION (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	ds = a.Limit(1).Union(b)
	selectSQL, args, err = ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM (`+
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) LIMIT 1) AS "t1" `+
		`UNION (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	ds = a.Order(C("id").Asc()).Union(b)
	selectSQL, args, err = ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM `+
		`(SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) ORDER BY "id" ASC) AS "t1" `+
		`UNION (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	selectSQL, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(10)})
	assert.Equal(t, selectSQL, `SELECT * FROM `+
		`(SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) ORDER BY "id" ASC) AS "t1" `+
		`UNION (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?))`)

	ds = a.Union(b.Limit(1))
	selectSQL, args, err = ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
		`UNION (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) LIMIT 1) AS "t1")`)

	selectSQL, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(10), int64(1)})
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) `+
		`UNION (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?) LIMIT ?) AS "t1")`)

	ds = a.Union(b.Order(C("id").Desc()))
	selectSQL, args, err = ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
		`UNION (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) ORDER BY "id" DESC) AS "t1")`)

	selectSQL, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(10)})
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) `+
		`UNION (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?) ORDER BY "id" DESC) AS "t1")`)

	ds = a.Limit(1).Union(b.Order(C("id").Desc()))
	selectSQL, args, err = ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM (`+
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) LIMIT 1) AS "t1" `+
		`UNION (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) ORDER BY "id" DESC) AS "t1")`)

	selectSQL, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(1), int64(10)})
	assert.Equal(t, selectSQL, `SELECT * FROM (`+
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) LIMIT ?) AS "t1" `+
		`UNION (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?) ORDER BY "id" DESC) AS "t1")`)

	ds = a.Union(b).Union(b.Where(C("id").Lt(50)))
	selectSQL, args, err = ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
		`UNION (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10)) `+
		`UNION (SELECT "id", "amount" FROM "invoice" WHERE (("amount" < 10) AND ("id" < 50)))`)

	selectSQL, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(10), int64(10), int64(50)})
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) `+
		`UNION (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?)) `+
		`UNION (SELECT "id", "amount" FROM "invoice" WHERE (("amount" < ?) AND ("id" < ?)))`)

}

func (sds *selectDatasetSuite) TestUnionAll() {
	t := sds.T()
	uds := From("union_test")
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.CompoundsAppend(exp.NewCompoundExpression(exp.UnionAllCompoundType, uds))
	assert.Equal(t, ec, ds.UnionAll(uds).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestUnionAll_ToSQL() {
	t := sds.T()
	a := From("invoice").Select("id", "amount").Where(C("amount").Gt(1000))
	b := From("invoice").Select("id", "amount").Where(C("amount").Lt(10))

	ds := a.UnionAll(b)
	selectSQL, args, err := ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
		`UNION ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	ds = a.Limit(1).UnionAll(b)
	selectSQL, args, err = ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM (`+
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) LIMIT 1) AS "t1" `+
		`UNION ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	ds = a.Order(C("id").Asc()).UnionAll(b)
	selectSQL, args, err = ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM `+
		`(SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) ORDER BY "id" ASC) AS "t1" `+
		`UNION ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	selectSQL, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(10)})
	assert.Equal(t, selectSQL, `SELECT * FROM `+
		`(SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) ORDER BY "id" ASC) AS "t1" `+
		`UNION ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?))`)

	ds = a.UnionAll(b.Limit(1))
	selectSQL, args, err = ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
		`UNION ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) LIMIT 1) AS "t1")`)

	selectSQL, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(10), int64(1)})
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) `+
		`UNION ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?) LIMIT ?) AS "t1")`)

	ds = a.UnionAll(b.Order(C("id").Desc()))
	selectSQL, args, err = ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
		`UNION ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) ORDER BY "id" DESC) AS "t1")`)

	selectSQL, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(10)})
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) `+
		`UNION ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?) ORDER BY "id" DESC) AS "t1")`)

	ds = a.Limit(1).UnionAll(b.Order(C("id").Desc()))
	selectSQL, args, err = ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM (`+
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) LIMIT 1) AS "t1" `+
		`UNION ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) ORDER BY "id" DESC) AS "t1")`)

	selectSQL, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(1), int64(10)})
	assert.Equal(t, selectSQL, `SELECT * FROM (`+
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) LIMIT ?) AS "t1" `+
		`UNION ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?) ORDER BY "id" DESC) AS "t1")`)

	ds = a.UnionAll(b).UnionAll(b.Where(C("id").Lt(50)))
	selectSQL, args, err = ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
		`UNION ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10)) `+
		`UNION ALL (SELECT "id", "amount" FROM "invoice" WHERE (("amount" < 10) AND ("id" < 50)))`)

	selectSQL, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(10), int64(10), int64(50)})
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) `+
		`UNION ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?)) `+
		`UNION ALL (SELECT "id", "amount" FROM "invoice" WHERE (("amount" < ?) AND ("id" < ?)))`)

}

func (sds *selectDatasetSuite) TestIntersect() {
	t := sds.T()
	uds := From("union_test")
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.CompoundsAppend(exp.NewCompoundExpression(exp.IntersectCompoundType, uds))
	assert.Equal(t, ec, ds.Intersect(uds).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestIntersect_ToSQL() {
	t := sds.T()
	a := From("invoice").Select("id", "amount").Where(C("amount").Gt(1000))
	b := From("invoice").Select("id", "amount").Where(C("amount").Lt(10))

	ds := a.Intersect(b)
	selectSQL, args, err := ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
		`INTERSECT (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	ds = a.Limit(1).Intersect(b)
	selectSQL, args, err = ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM (`+
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) LIMIT 1) AS "t1" `+
		`INTERSECT (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	ds = a.Order(C("id").Asc()).Intersect(b)
	selectSQL, args, err = ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM `+
		`(SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) ORDER BY "id" ASC) AS "t1" `+
		`INTERSECT (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	selectSQL, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(10)})
	assert.Equal(t, selectSQL, `SELECT * FROM `+
		`(SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) ORDER BY "id" ASC) AS "t1" `+
		`INTERSECT (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?))`)

	ds = a.Intersect(b.Limit(1))
	selectSQL, args, err = ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
		`INTERSECT (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) LIMIT 1) AS "t1")`)

	selectSQL, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(10), int64(1)})
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) `+
		`INTERSECT (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?) LIMIT ?) AS "t1")`)

	ds = a.Intersect(b.Order(C("id").Desc()))
	selectSQL, args, err = ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
		`INTERSECT (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) ORDER BY "id" DESC) AS "t1")`)

	selectSQL, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(10)})
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) `+
		`INTERSECT (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?) ORDER BY "id" DESC) AS "t1")`)

	ds = a.Limit(1).Intersect(b.Order(C("id").Desc()))
	selectSQL, args, err = ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM (`+
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) LIMIT 1) AS "t1" `+
		`INTERSECT (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) ORDER BY "id" DESC) AS "t1")`)

	selectSQL, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(1), int64(10)})
	assert.Equal(t, selectSQL, `SELECT * FROM (`+
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) LIMIT ?) AS "t1" `+
		`INTERSECT (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?) ORDER BY "id" DESC) AS "t1")`)

	ds = a.Intersect(b).Intersect(b.Where(C("id").Lt(50)))
	selectSQL, args, err = ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
		`INTERSECT (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10)) `+
		`INTERSECT (SELECT "id", "amount" FROM "invoice" WHERE (("amount" < 10) AND ("id" < 50)))`)

	selectSQL, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(10), int64(10), int64(50)})
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) `+
		`INTERSECT (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?)) `+
		`INTERSECT (SELECT "id", "amount" FROM "invoice" WHERE (("amount" < ?) AND ("id" < ?)))`)
}

func (sds *selectDatasetSuite) TestIntersectAll() {
	t := sds.T()
	uds := From("union_test")
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.CompoundsAppend(exp.NewCompoundExpression(exp.IntersectAllCompoundType, uds))
	assert.Equal(t, ec, ds.IntersectAll(uds).GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestIntersectAll_ToSQL() {
	t := sds.T()
	a := From("invoice").Select("id", "amount").Where(C("amount").Gt(1000))
	b := From("invoice").Select("id", "amount").Where(C("amount").Lt(10))

	ds := a.IntersectAll(b)
	selectSQL, args, err := ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
		`INTERSECT ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	ds = a.Limit(1).IntersectAll(b)
	selectSQL, args, err = ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM (`+
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) LIMIT 1) AS "t1" `+
		`INTERSECT ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	ds = a.Order(C("id").Asc()).IntersectAll(b)
	selectSQL, args, err = ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM `+
		`(SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) ORDER BY "id" ASC) AS "t1" `+
		`INTERSECT ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`)

	selectSQL, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(10)})
	assert.Equal(t, selectSQL, `SELECT * FROM `+
		`(SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) ORDER BY "id" ASC) AS "t1" `+
		`INTERSECT ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?))`)

	ds = a.IntersectAll(b.Limit(1))
	selectSQL, args, err = ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
		`INTERSECT ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) LIMIT 1) AS "t1")`)

	selectSQL, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(10), int64(1)})
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) `+
		`INTERSECT ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?) LIMIT ?) AS "t1")`)

	ds = a.IntersectAll(b.Order(C("id").Desc()))
	selectSQL, args, err = ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
		`INTERSECT ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) ORDER BY "id" DESC) AS "t1")`)

	selectSQL, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(10)})
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) `+
		`INTERSECT ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?) ORDER BY "id" DESC) AS "t1")`)

	ds = a.Limit(1).IntersectAll(b.Order(C("id").Desc()))
	selectSQL, args, err = ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT * FROM (`+
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) LIMIT 1) AS "t1" `+
		`INTERSECT ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) ORDER BY "id" DESC) AS "t1")`)

	selectSQL, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(1), int64(10)})
	assert.Equal(t, selectSQL, `SELECT * FROM (`+
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) LIMIT ?) AS "t1" `+
		`INTERSECT ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?) ORDER BY "id" DESC) AS "t1")`)

	ds = a.IntersectAll(b).IntersectAll(b.Where(C("id").Lt(50)))
	selectSQL, args, err = ds.ToSQL()
	assert.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
		`INTERSECT ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10)) `+
		`INTERSECT ALL (SELECT "id", "amount" FROM "invoice" WHERE (("amount" < 10) AND ("id" < 50)))`)

	selectSQL, args, err = ds.Prepared(true).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(1000), int64(10), int64(10), int64(50)})
	assert.Equal(t, selectSQL, `SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) `+
		`INTERSECT ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?)) `+
		`INTERSECT ALL (SELECT "id", "amount" FROM "invoice" WHERE (("amount" < ?) AND ("id" < ?)))`)
}

func (sds *selectDatasetSuite) TestAs() {
	t := sds.T()
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetAlias(T("a"))
	assert.Equal(t, ec, ds.As("a").GetClauses())
	assert.Equal(t, dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestToSQL() {
	t := sds.T()
	md := new(mocks.SQLDialect)
	ds := From("test").SetDialect(md)
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(false)
	md.On("ToSelectSQL", sqlB, c).Return(nil).Once()
	sql, args, err := ds.ToSQL()
	assert.Empty(t, sql)
	assert.Empty(t, args)
	assert.Nil(t, err)
	md.AssertExpectations(t)
}

func (sds *selectDatasetSuite) TestToSQL_ReturnedError() {
	t := sds.T()
	md := new(mocks.SQLDialect)
	ds := From("test").SetDialect(md)
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(false)
	ee := errors.New("expected error")
	md.On("ToSelectSQL", sqlB, c).Run(func(args mock.Arguments) {
		args.Get(0).(sb.SQLBuilder).SetError(ee)
	}).Once()

	sql, args, err := ds.ToSQL()
	assert.Empty(t, sql)
	assert.Empty(t, args)
	assert.Equal(t, ee, err)
	md.AssertExpectations(t)
}

func (sds *selectDatasetSuite) TestAppendSQL() {
	t := sds.T()
	md := new(mocks.SQLDialect)
	ds := From("test").SetDialect(md)
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(false)
	md.On("ToSelectSQL", sqlB, c).Return(nil).Once()
	ds.AppendSQL(sqlB)
	assert.NoError(t, sqlB.Error())
	md.AssertExpectations(t)
}

func (sds *selectDatasetSuite) TestScanStructs() {
	t := sds.T()
	mDb, sqlMock, err := sqlmock.New()
	assert.NoError(t, err)
	sqlMock.ExpectQuery(`SELECT "address", "name" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	sqlMock.ExpectQuery(`SELECT DISTINCT "name" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	sqlMock.ExpectQuery(`SELECT "test" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"test"}).FromCSVString("test1\ntest2"))

	qf := exec.NewQueryFactory(mDb)
	ds := newDataset("mock", qf)
	var items []dsTestActionItem
	assert.NoError(t, ds.From("items").ScanStructs(&items))
	assert.Equal(t, items, []dsTestActionItem{
		{Address: "111 Test Addr", Name: "Test1"},
		{Address: "211 Test Addr", Name: "Test2"},
	})

	items = items[0:0]
	assert.NoError(t, ds.From("items").Select("name").Distinct().ScanStructs(&items))
	assert.Equal(t, items, []dsTestActionItem{
		{Address: "111 Test Addr", Name: "Test1"},
		{Address: "211 Test Addr", Name: "Test2"},
	})

	items = items[0:0]
	assert.EqualError(t, ds.From("items").ScanStructs(items),
		"goqu: type must be a pointer to a slice when scanning into structs")
	assert.EqualError(t, ds.From("items").ScanStructs(&dsTestActionItem{}),
		"goqu: type must be a pointer to a slice when scanning into structs")
	assert.EqualError(t, ds.From("items").Select("test").ScanStructs(&items),
		`goqu: unable to find corresponding field to column "test" returned by query`)
}

func (sds *selectDatasetSuite) TestScanStructs_WithPreparedStatements() {
	t := sds.T()
	mDb, sqlMock, err := sqlmock.New()
	assert.NoError(t, err)
	sqlMock.ExpectQuery(
		`SELECT "address", "name" FROM "items" WHERE \(\("address" = \?\) AND \("name" IN \(\?, \?, \?\)\)\)`,
	).
		WithArgs("111 Test Addr", "Bob", "Sally", "Billy").
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).
			FromCSVString("111 Test Addr,Test1\n211 Test Addr,Test2"))

	sqlMock.ExpectQuery(
		`SELECT "test" FROM "items" WHERE \(\("address" = \?\) AND \("name" IN \(\?, \?, \?\)\)\)`,
	).
		WithArgs("111 Test Addr", "Bob", "Sally", "Billy").
		WillReturnRows(sqlmock.NewRows([]string{"test"}).FromCSVString("test1\ntest2"))

	qf := exec.NewQueryFactory(mDb)
	ds := newDataset("mock", qf)
	var items []dsTestActionItem
	assert.NoError(t, ds.From("items").Prepared(true).Where(Ex{
		"name":    []string{"Bob", "Sally", "Billy"},
		"address": "111 Test Addr",
	}).ScanStructs(&items))
	assert.Equal(t, items, []dsTestActionItem{
		{Address: "111 Test Addr", Name: "Test1"},
		{Address: "211 Test Addr", Name: "Test2"},
	})

	items = items[0:0]
	assert.EqualError(t, ds.From("items").ScanStructs(items),
		"goqu: type must be a pointer to a slice when scanning into structs")
	assert.EqualError(t, ds.From("items").ScanStructs(&dsTestActionItem{}),
		"goqu: type must be a pointer to a slice when scanning into structs")
	assert.EqualError(t, ds.From("items").
		Prepared(true).
		Select("test").
		Where(Ex{"name": []string{"Bob", "Sally", "Billy"}, "address": "111 Test Addr"}).
		ScanStructs(&items), `goqu: unable to find corresponding field to column "test" returned by query`)
}

func (sds *selectDatasetSuite) TestScanStruct() {
	t := sds.T()
	mDb, sqlMock, err := sqlmock.New()
	assert.NoError(t, err)
	sqlMock.ExpectQuery(`SELECT "address", "name" FROM "items" LIMIT 1`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).FromCSVString("111 Test Addr,Test1"))

	sqlMock.ExpectQuery(`SELECT DISTINCT "name" FROM "items" LIMIT 1`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).FromCSVString("111 Test Addr,Test1"))

	sqlMock.ExpectQuery(`SELECT "test" FROM "items" LIMIT 1`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"test"}).FromCSVString("test1\ntest2"))

	qf := exec.NewQueryFactory(mDb)
	ds := newDataset("mock", qf)
	var item dsTestActionItem
	found, err := ds.From("items").ScanStruct(&item)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, item.Address, "111 Test Addr")
	assert.Equal(t, item.Name, "Test1")

	item = dsTestActionItem{}
	found, err = ds.From("items").Select("name").Distinct().ScanStruct(&item)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, item.Address, "111 Test Addr")
	assert.Equal(t, item.Name, "Test1")

	_, err = ds.From("items").ScanStruct(item)
	assert.EqualError(t, err, "goqu: type must be a pointer to a struct when scanning into a struct")
	_, err = ds.From("items").ScanStruct([]dsTestActionItem{})
	assert.EqualError(t, err, "goqu: type must be a pointer to a struct when scanning into a struct")
	_, err = ds.From("items").Select("test").ScanStruct(&item)
	assert.EqualError(t, err, `goqu: unable to find corresponding field to column "test" returned by query`)
}

func (sds *selectDatasetSuite) TestScanStruct_WithPreparedStatements() {
	t := sds.T()
	mDb, sqlMock, err := sqlmock.New()
	assert.NoError(t, err)
	sqlMock.ExpectQuery(
		`SELECT "address", "name" FROM "items" WHERE \(\("address" = \?\) AND \("name" IN \(\?, \?, \?\)\)\) LIMIT \?`,
	).
		WithArgs("111 Test Addr", "Bob", "Sally", "Billy", 1).
		WillReturnRows(sqlmock.NewRows([]string{"address", "name"}).FromCSVString("111 Test Addr,Test1"))

	sqlMock.ExpectQuery(`SELECT "test" FROM "items" WHERE \(\("address" = \?\) AND \("name" IN \(\?, \?, \?\)\)\) LIMIT \?`).
		WithArgs("111 Test Addr", "Bob", "Sally", "Billy", 1).
		WillReturnRows(sqlmock.NewRows([]string{"test"}).FromCSVString("test1\ntest2"))

	qf := exec.NewQueryFactory(mDb)
	ds := newDataset("mock", qf)
	var item dsTestActionItem
	found, err := ds.From("items").Prepared(true).Where(Ex{
		"name":    []string{"Bob", "Sally", "Billy"},
		"address": "111 Test Addr",
	}).ScanStruct(&item)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, item.Address, "111 Test Addr")
	assert.Equal(t, item.Name, "Test1")

	_, err = ds.From("items").ScanStruct(item)
	assert.EqualError(t, err, "goqu: type must be a pointer to a struct when scanning into a struct")
	_, err = ds.From("items").ScanStruct([]dsTestActionItem{})
	assert.EqualError(t, err, "goqu: type must be a pointer to a struct when scanning into a struct")
	_, err = ds.From("items").
		Prepared(true).
		Select("test").
		Where(Ex{"name": []string{"Bob", "Sally", "Billy"}, "address": "111 Test Addr"}).
		ScanStruct(&item)
	assert.EqualError(t, err, `goqu: unable to find corresponding field to column "test" returned by query`)
}

func (sds *selectDatasetSuite) TestScanVals() {
	t := sds.T()
	mDb, sqlMock, err := sqlmock.New()
	assert.NoError(t, err)
	sqlMock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("1\n2\n3\n4\n5"))

	qf := exec.NewQueryFactory(mDb)
	ds := newDataset("mock", qf)
	var ids []uint32
	assert.NoError(t, ds.From("items").Select("id").ScanVals(&ids))
	assert.Equal(t, ids, []uint32{1, 2, 3, 4, 5})

	assert.EqualError(t, ds.From("items").ScanVals([]uint32{}),
		"goqu: type must be a pointer to a slice when scanning into vals")
	assert.EqualError(t, ds.From("items").ScanVals(dsTestActionItem{}),
		"goqu: type must be a pointer to a slice when scanning into vals")
}

func (sds *selectDatasetSuite) TestScanVals_WithPreparedStatment() {
	t := sds.T()
	mDb, sqlMock, err := sqlmock.New()
	assert.NoError(t, err)
	sqlMock.ExpectQuery(
		`SELECT "id" FROM "items" WHERE \(\("address" = \?\) AND \("name" IN \(\?, \?, \?\)\)\)`,
	).
		WithArgs("111 Test Addr", "Bob", "Sally", "Billy").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("1\n2\n3\n4\n5"))

	qf := exec.NewQueryFactory(mDb)
	ds := newDataset("mock", qf)
	var ids []uint32
	assert.NoError(t, ds.From("items").
		Prepared(true).
		Select("id").
		Where(Ex{"name": []string{"Bob", "Sally", "Billy"}, "address": "111 Test Addr"}).
		ScanVals(&ids))
	assert.Equal(t, ids, []uint32{1, 2, 3, 4, 5})

	assert.EqualError(t, ds.From("items").ScanVals([]uint32{}),
		"goqu: type must be a pointer to a slice when scanning into vals")
	assert.EqualError(t, ds.From("items").ScanVals(dsTestActionItem{}),
		"goqu: type must be a pointer to a slice when scanning into vals")
}

func (sds *selectDatasetSuite) TestScanVal() {
	t := sds.T()
	mDb, sqlMock, err := sqlmock.New()
	assert.NoError(t, err)
	sqlMock.ExpectQuery(`SELECT "id" FROM "items" LIMIT 1`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("10"))

	qf := exec.NewQueryFactory(mDb)
	ds := newDataset("mock", qf)
	var id int64
	found, err := ds.From("items").Select("id").ScanVal(&id)
	assert.NoError(t, err)
	assert.Equal(t, id, int64(10))
	assert.True(t, found)

	found, err = ds.From("items").ScanVal([]int64{})
	assert.False(t, found)
	assert.EqualError(t, err, "goqu: type must be a pointer when scanning into val")
	found, err = ds.From("items").ScanVal(10)
	assert.False(t, found)
	assert.EqualError(t, err, "goqu: type must be a pointer when scanning into val")
}

func (sds *selectDatasetSuite) TestScanVal_WithPreparedStatement() {
	t := sds.T()
	mDb, sqlMock, err := sqlmock.New()
	assert.NoError(t, err)
	sqlMock.ExpectQuery(
		`SELECT "id" FROM "items" WHERE \(\("address" = \?\) AND \("name" IN \(\?, \?, \?\)\)\) LIMIT ?`,
	).
		WithArgs("111 Test Addr", "Bob", "Sally", "Billy", 1).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("10"))

	qf := exec.NewQueryFactory(mDb)
	ds := newDataset("mock", qf)
	var id int64
	found, err := ds.From("items").
		Prepared(true).
		Select("id").
		Where(Ex{"name": []string{"Bob", "Sally", "Billy"}, "address": "111 Test Addr"}).
		ScanVal(&id)
	assert.NoError(t, err)
	assert.Equal(t, id, int64(10))
	assert.True(t, found)

	found, err = ds.From("items").ScanVal([]int64{})
	assert.False(t, found)
	assert.EqualError(t, err, "goqu: type must be a pointer when scanning into val")
	found, err = ds.From("items").ScanVal(10)
	assert.False(t, found)
	assert.EqualError(t, err, "goqu: type must be a pointer when scanning into val")
}

func (sds *selectDatasetSuite) TestCount() {
	t := sds.T()
	mDb, sqlMock, err := sqlmock.New()
	assert.NoError(t, err)
	sqlMock.ExpectQuery(`SELECT COUNT\(\*\) AS "count" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"count"}).FromCSVString("10"))

	qf := exec.NewQueryFactory(mDb)
	ds := newDataset("mock", qf)
	count, err := ds.From("items").Count()
	assert.NoError(t, err)
	assert.Equal(t, count, int64(10))
}

func (sds *selectDatasetSuite) TestCount_WithPreparedStatement() {
	t := sds.T()
	mDb, sqlMock, err := sqlmock.New()
	assert.NoError(t, err)
	sqlMock.ExpectQuery(
		`SELECT COUNT\(\*\) AS "count" FROM "items" WHERE \(\("address" = \?\) AND \("name" IN \(\?, \?, \?\)\)\)`,
	).
		WithArgs("111 Test Addr", "Bob", "Sally", "Billy", 1).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).FromCSVString("10"))

	qf := exec.NewQueryFactory(mDb)
	ds := newDataset("mock", qf)
	count, err := ds.From("items").
		Prepared(true).
		Where(Ex{"name": []string{"Bob", "Sally", "Billy"}, "address": "111 Test Addr"}).
		Count()
	assert.NoError(t, err)
	assert.Equal(t, count, int64(10))
}

func (sds *selectDatasetSuite) TestPluck() {
	t := sds.T()
	mDb, sqlMock, err := sqlmock.New()
	assert.NoError(t, err)
	sqlMock.ExpectQuery(`SELECT "name" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"name"}).FromCSVString("test1\ntest2\ntest3\ntest4\ntest5"))

	qf := exec.NewQueryFactory(mDb)
	ds := newDataset("mock", qf)
	var names []string
	assert.NoError(t, ds.From("items").Pluck(&names, "name"))
	assert.Equal(t, names, []string{"test1", "test2", "test3", "test4", "test5"})
}

func (sds *selectDatasetSuite) TestPluck_WithPreparedStatement() {
	t := sds.T()
	mDb, sqlMock, err := sqlmock.New()
	assert.NoError(t, err)
	sqlMock.ExpectQuery(
		`SELECT "name" FROM "items" WHERE \(\("address" = \?\) AND \("name" IN \(\?, \?, \?\)\)\)`,
	).
		WithArgs("111 Test Addr", "Bob", "Sally", "Billy").
		WillReturnRows(sqlmock.NewRows([]string{"name"}).FromCSVString("Bob\nSally\nBilly"))

	qf := exec.NewQueryFactory(mDb)
	ds := newDataset("mock", qf)
	var names []string
	assert.NoError(t, ds.From("items").
		Prepared(true).
		Where(Ex{"name": []string{"Bob", "Sally", "Billy"}, "address": "111 Test Addr"}).
		Pluck(&names, "name"))
	assert.Equal(t, names, []string{"Bob", "Sally", "Billy"})
}

func TestSelectDataset(t *testing.T) {
	suite.Run(t, new(selectDatasetSuite))
}
