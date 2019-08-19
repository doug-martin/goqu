package goqu

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/doug-martin/goqu/v8/exec"
	"github.com/doug-martin/goqu/v8/exp"
	"github.com/doug-martin/goqu/v8/internal/errors"
	"github.com/doug-martin/goqu/v8/internal/sb"
	"github.com/doug-martin/goqu/v8/mocks"
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
	ds := From("test")
	sds.Equal(ds, ds.Clone())
}

func (sds *selectDatasetSuite) TestExpression() {
	ds := From("test")
	sds.Equal(ds, ds.Expression())
}

func (sds *selectDatasetSuite) TestDialect() {
	ds := From("test")
	sds.NotNil(ds.Dialect())
}

func (sds *selectDatasetSuite) TestWithDialect() {
	ds := From("test")
	md := new(mocks.SQLDialect)
	ds = ds.SetDialect(md)

	dialect := GetDialect("default")
	dialectDs := ds.WithDialect("default")
	sds.Equal(md, ds.Dialect())
	sds.Equal(dialect, dialectDs.Dialect())
}

func (sds *selectDatasetSuite) TestPrepared() {
	ds := From("test")
	preparedDs := ds.Prepared(true)
	sds.True(preparedDs.IsPrepared())
	sds.False(ds.IsPrepared())
	// should apply the prepared to any datasets created from the root
	sds.True(preparedDs.Where(Ex{"a": 1}).IsPrepared())
}

func (sds *selectDatasetSuite) TestGetClauses() {
	ds := From("test")
	ce := exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression(I("test")))
	sds.Equal(ce, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestWith() {
	from := From("cte")
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.CommonTablesAppend(exp.NewCommonTableExpression(false, "test-cte", from))
	sds.Equal(ec, ds.With("test-cte", from).GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestWithRecursive() {
	from := From("cte")
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.CommonTablesAppend(exp.NewCommonTableExpression(true, "test-cte", from))
	sds.Equal(ec, ds.WithRecursive("test-cte", from).GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestSelect() {
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetSelect(exp.NewColumnListExpression(C("a")))
	sds.Equal(ec, ds.Select(C("a")).GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestSelect_ToSQL() {
	ds1 := From("test")

	selectSQL, _, err := ds1.ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT * FROM "test"`, selectSQL)

	selectSQL, _, err = ds1.Select().ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT * FROM "test"`, selectSQL)

	selectSQL, _, err = ds1.Select("id").ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT "id" FROM "test"`, selectSQL)

	selectSQL, _, err = ds1.Select("id", "name").ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT "id", "name" FROM "test"`, selectSQL)

	selectSQL, _, err = ds1.Select(L("COUNT(*)").As("count")).ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT COUNT(*) AS "count" FROM "test"`, selectSQL)

	selectSQL, _, err = ds1.Select(C("id").As("other_id"), L("COUNT(*)").As("count")).ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT "id" AS "other_id", COUNT(*) AS "count" FROM "test"`, selectSQL)

	selectSQL, _, err = ds1.From().Select(ds1.From("test_1").Select("id")).ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT (SELECT "id" FROM "test_1")`, selectSQL)

	selectSQL, _, err = ds1.From().Select(ds1.From("test_1").Select("id").As("test_id")).ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT (SELECT "id" FROM "test_1") AS "test_id"`, selectSQL)

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
	sds.NoError(err)
	sds.Equal(
		`SELECT `+
			`DISTINCT("a") AS "distinct", `+
			`COUNT("a") AS "count", `+
			`CASE WHEN (MIN("a") = 10) THEN TRUE ELSE FALSE END, `+
			`CASE WHEN (AVG("a") != 10) THEN TRUE ELSE FALSE END, `+
			`CASE WHEN (FIRST("a") > 10) THEN TRUE ELSE FALSE END, `+
			`CASE WHEN (FIRST("a") >= 10) THEN TRUE ELSE FALSE END,`+
			` CASE WHEN (LAST("a") < 10) THEN TRUE ELSE FALSE END, `+
			`CASE WHEN (LAST("a") <= 10) THEN TRUE ELSE FALSE END, `+
			`SUM("a") AS "sum", `+
			`COALESCE("a", 'a') AS "colaseced"`,
		selectSQL,
	)

	type MyStruct struct {
		Name         string
		Address      string `db:"address"`
		EmailAddress string `db:"email_address"`
		FakeCol      string `db:"-"`
	}
	selectSQL, _, err = ds1.Select(&MyStruct{}).ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT "address", "email_address", "name" FROM "test"`, selectSQL)

	selectSQL, _, err = ds1.Select(MyStruct{}).ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT "address", "email_address", "name" FROM "test"`, selectSQL)

	type myStruct2 struct {
		MyStruct
		Zipcode string `db:"zipcode"`
	}

	selectSQL, _, err = ds1.Select(&myStruct2{}).ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT "address", "email_address", "name", "zipcode" FROM "test"`, selectSQL)

	selectSQL, _, err = ds1.Select(myStruct2{}).ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT "address", "email_address", "name", "zipcode" FROM "test"`, selectSQL)

	var myStructs []MyStruct
	selectSQL, _, err = ds1.Select(&myStructs).ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT "address", "email_address", "name" FROM "test"`, selectSQL)

	selectSQL, _, err = ds1.Select(myStructs).ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT "address", "email_address", "name" FROM "test"`, selectSQL)
	// should not change original
	selectSQL, _, err = ds1.ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT * FROM "test"`, selectSQL)
}

func (sds *selectDatasetSuite) TestSelectDistinct() {
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetSelect(exp.NewColumnListExpression(C("a"))).SetDistinct(exp.NewColumnListExpression())
	sds.Equal(ec, ds.SelectDistinct(C("a")).GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestSelectDistinct_ToSQL() {
	ds1 := From("test")

	selectSQL, _, err := ds1.ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT * FROM "test"`, selectSQL)

	selectSQL, _, err = ds1.SelectDistinct("id").ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT DISTINCT "id" FROM "test"`, selectSQL)

	selectSQL, _, err = ds1.SelectDistinct("id", "name").ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT DISTINCT "id", "name" FROM "test"`, selectSQL)

	selectSQL, _, err = ds1.SelectDistinct(L("COUNT(*)").As("count")).ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT DISTINCT COUNT(*) AS "count" FROM "test"`, selectSQL)

	selectSQL, _, err = ds1.SelectDistinct(C("id").As("other_id"), L("COUNT(*)").As("count")).ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT DISTINCT "id" AS "other_id", COUNT(*) AS "count" FROM "test"`, selectSQL)

	type MyStruct struct {
		Name         string
		Address      string `db:"address"`
		EmailAddress string `db:"email_address"`
		FakeCol      string `db:"-"`
	}
	selectSQL, _, err = ds1.SelectDistinct(&MyStruct{}).ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT DISTINCT "address", "email_address", "name" FROM "test"`, selectSQL)

	selectSQL, _, err = ds1.SelectDistinct(MyStruct{}).ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT DISTINCT "address", "email_address", "name" FROM "test"`, selectSQL)

	type myStruct2 struct {
		MyStruct
		Zipcode string `db:"zipcode"`
	}

	selectSQL, _, err = ds1.SelectDistinct(&myStruct2{}).ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT DISTINCT "address", "email_address", "name", "zipcode" FROM "test"`, selectSQL)

	selectSQL, _, err = ds1.SelectDistinct(myStruct2{}).ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT DISTINCT "address", "email_address", "name", "zipcode" FROM "test"`, selectSQL)

	var myStructs []MyStruct
	selectSQL, _, err = ds1.SelectDistinct(&myStructs).ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT DISTINCT "address", "email_address", "name" FROM "test"`, selectSQL)

	selectSQL, _, err = ds1.SelectDistinct(myStructs).ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT DISTINCT "address", "email_address", "name" FROM "test"`, selectSQL)
	// should not change original
	selectSQL, _, err = ds1.ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT * FROM "test"`, selectSQL)
	// should not change original
	selectSQL, _, err = ds1.ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT * FROM "test"`, selectSQL)
}

func (sds *selectDatasetSuite) TestDistinct() {
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetDistinct(exp.NewColumnListExpression())
	ecs := dsc.SetSelect(exp.NewColumnListExpression("a", "b")).SetDistinct(exp.NewColumnListExpression())
	ecsd := dsc.SetSelect(exp.NewColumnListExpression("a", "b")).SetDistinct(exp.NewColumnListExpression("c"))
	sds.Equal(ec, ds.Distinct().GetClauses())
	sds.Equal(ecs, ds.Select("a", "b").Distinct().GetClauses())
	sds.Equal(ecsd, ds.Select("a", "b").Distinct("c").GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestDistinct_ToSQL() {
	ds1 := From("test")

	selectSQL, _, err := ds1.Distinct().ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT DISTINCT * FROM "test"`, selectSQL)

	selectSQL, _, err = ds1.Distinct("id").ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT DISTINCT ON ("id") * FROM "test"`, selectSQL)

	selectSQL, _, err = ds1.Distinct("id").Select("name").ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT DISTINCT ON ("id") "name" FROM "test"`, selectSQL)

	selectSQL, _, err = ds1.Select(L("COUNT(*)").As("count")).Distinct(COALESCE(C("b"), "empty")).ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT DISTINCT ON (COALESCE("b", 'empty')) COUNT(*) AS "count" FROM "test"`, selectSQL)

	// should not change original
	selectSQL, _, err = ds1.ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT * FROM "test"`, selectSQL)
	// should not change original
	selectSQL, _, err = ds1.ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT * FROM "test"`, selectSQL)
}

func (sds *selectDatasetSuite) TestClearSelect() {
	ds := From("test").Select(C("a"))
	dsc := ds.GetClauses()
	ec := dsc.SetSelect(exp.NewColumnListExpression(Star()))
	sds.Equal(ec, ds.ClearSelect().GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestClearSelect_ToSQL() {
	ds1 := From("test")

	selectSQL, _, err := ds1.ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT * FROM "test"`, selectSQL)

	b := ds1.Select("a").ClearSelect()
	selectSQL, _, err = b.ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT * FROM "test"`, selectSQL)
}

func (sds *selectDatasetSuite) TestSelectAppend(selects ...interface{}) {
	ds := From("test").Select(C("a"))
	dsc := ds.GetClauses()
	ec := dsc.SelectAppend(exp.NewColumnListExpression(C("b")))
	sds.Equal(ec, ds.SelectAppend(C("b")).GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestSelectAppend_ToSQL() {
	ds1 := From("test")

	selectSQL, _, err := ds1.ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT * FROM "test"`, selectSQL)

	b := ds1.Select("a").SelectAppend("b").SelectAppend("c")
	selectSQL, _, err = b.ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT "a", "b", "c" FROM "test"`, selectSQL)
}

func (sds *selectDatasetSuite) TestFrom() {
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetFrom(exp.NewColumnListExpression(T("t")))
	sds.Equal(ec, ds.From(T("t")).GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestFrom_ToSQL() {
	ds1 := From("test")

	selectSQL, _, err := ds1.ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT * FROM "test"`, selectSQL)

	ds2 := ds1.From("test2")
	selectSQL, _, err = ds2.ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT * FROM "test2"`, selectSQL)

	ds2 = ds1.From("test2", "test3")
	selectSQL, _, err = ds2.ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT * FROM "test2", "test3"`, selectSQL)

	ds2 = ds1.From(T("test2").As("test_2"), "test3")
	selectSQL, _, err = ds2.ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT * FROM "test2" AS "test_2", "test3"`, selectSQL)

	ds2 = ds1.From(ds1.From("test2"), "test3")
	selectSQL, _, err = ds2.ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT * FROM (SELECT * FROM "test2") AS "t1", "test3"`, selectSQL)

	ds2 = ds1.From(ds1.From("test2").As("test_2"), "test3")
	selectSQL, _, err = ds2.ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT * FROM (SELECT * FROM "test2") AS "test_2", "test3"`, selectSQL)
	// should not change original
	selectSQL, _, err = ds1.ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT * FROM "test"`, selectSQL)
}

func (sds *selectDatasetSuite) TestFromSelf() {
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetFrom(exp.NewColumnListExpression(ds.As("t1")))
	sds.Equal(ec, ds.FromSelf().GetClauses())

	ec2 := dsc.SetFrom(exp.NewColumnListExpression(ds.As("test")))
	sds.Equal(ec2, ds.As("test").FromSelf().GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestCompoundFromSelf() {
	ds := From("test")
	dsc := ds.GetClauses()
	sds.Equal(dsc, ds.CompoundFromSelf().GetClauses())

	ds2 := ds.Limit(1)
	dsc2 := exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression(ds2.As("t1")))
	sds.Equal(dsc2, ds2.CompoundFromSelf().GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestJoin() {
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.JoinsAppend(
		exp.NewConditionedJoinExpression(exp.InnerJoinType, T("foo"), On(C("a").IsNull())),
	)
	sds.Equal(ec, ds.Join(T("foo"), On(C("a").IsNull())).GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestJoin_ToSQL() {
	ds1 := From("items")

	b := ds1.Join(T("players").As("p"), On(Ex{"p.id": I("items.playerId")}))
	selectSQL, args, err := b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(
		`SELECT * FROM "items" INNER JOIN "players" AS "p" ON ("p"."id" = "items"."playerId")`,
		selectSQL,
	)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(
		`SELECT * FROM "items" INNER JOIN "players" AS "p" ON ("p"."id" = "items"."playerId")`,
		selectSQL,
	)

	b = ds1.Join(ds1.From("players").As("p"), On(Ex{"p.id": I("items.playerId")}))
	selectSQL, args, err = b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(
		`SELECT * FROM "items" INNER JOIN (SELECT * FROM "players") AS "p" ON ("p"."id" = "items"."playerId")`,
		selectSQL,
	)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(
		`SELECT * FROM "items" INNER JOIN (SELECT * FROM "players") AS "p" ON ("p"."id" = "items"."playerId")`,
		selectSQL,
	)

	b = ds1.Join(S("v1").Table("test"), On(Ex{"v1.test.id": I("items.playerId")}))
	selectSQL, args, err = b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(
		`SELECT * FROM "items" INNER JOIN "v1"."test" ON ("v1"."test"."id" = "items"."playerId")`,
		selectSQL,
	)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(
		`SELECT * FROM "items" INNER JOIN "v1"."test" ON ("v1"."test"."id" = "items"."playerId")`,
		selectSQL,
	)

	b = ds1.Join(T("test"), Using(C("name"), C("common_id")))
	selectSQL, args, err = b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "items" INNER JOIN "test" USING ("name", "common_id")`, selectSQL)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "items" INNER JOIN "test" USING ("name", "common_id")`, selectSQL)

	b = ds1.Join(T("test"), Using("name", "common_id"))
	selectSQL, args, err = b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "items" INNER JOIN "test" USING ("name", "common_id")`, selectSQL)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "items" INNER JOIN "test" USING ("name", "common_id")`, selectSQL)

	b = ds1.Join(
		T("categories"),
		On(
			I("categories.categoryId").Eq(I("items.id")),
			I("categories.categoryId").In(1, 2, 3),
		),
	)

	selectSQL, args, err = b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(
		`SELECT * FROM "items" `+
			`INNER JOIN "categories" ON (`+
			`("categories"."categoryId" = "items"."id") AND ("categories"."categoryId" IN (1, 2, 3))`+
			`)`,
		selectSQL,
	)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1), int64(2), int64(3)}, args)
	sds.Equal(
		`SELECT * FROM "items" `+
			`INNER JOIN "categories" ON (`+
			`("categories"."categoryId" = "items"."id") AND ("categories"."categoryId" IN (?, ?, ?))`+
			`)`,
		selectSQL,
	)
}

func (sds *selectDatasetSuite) TestInnerJoin() {
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.JoinsAppend(
		exp.NewConditionedJoinExpression(exp.InnerJoinType, T("foo"), On(C("a").IsNull())),
	)
	sds.Equal(ec, ds.InnerJoin(T("foo"), On(C("a").IsNull())).GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestInnerJoin_ToSQL() {
	ds1 := From("items")
	selectSQL, _, err := ds1.
		InnerJoin(T("b"), On(Ex{"b.itemsId": I("items.id")})).
		LeftOuterJoin(T("c"), On(Ex{"c.b_id": I("b.id")})).
		ToSQL()
	sds.NoError(err)
	sds.Equal(
		`SELECT * FROM "items" `+
			`INNER JOIN "b" ON ("b"."itemsId" = "items"."id") `+
			`LEFT OUTER JOIN "c" ON ("c"."b_id" = "b"."id")`,
		selectSQL,
	)

	selectSQL, _, err = ds1.
		InnerJoin(T("b"), On(Ex{"b.itemsId": I("items.id")})).
		LeftOuterJoin(T("c"), On(Ex{"c.b_id": I("b.id")})).
		ToSQL()
	sds.NoError(err)
	sds.Equal(
		`SELECT * FROM "items" `+
			`INNER JOIN "b" ON ("b"."itemsId" = "items"."id") `+
			`LEFT OUTER JOIN "c" ON ("c"."b_id" = "b"."id")`,
		selectSQL,
	)

	selectSQL, _, err = ds1.InnerJoin(
		T("categories"),
		On(Ex{"categories.categoryId": I("items.id")}),
	).ToSQL()
	sds.NoError(err)
	sds.Equal(
		`SELECT * FROM "items" INNER JOIN "categories" ON ("categories"."categoryId" = "items"."id")`,
		selectSQL,
	)
}

func (sds *selectDatasetSuite) TestFullOuterJoin() {
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.JoinsAppend(
		exp.NewConditionedJoinExpression(exp.FullOuterJoinType, T("foo"), On(C("a").IsNull())),
	)
	sds.Equal(ec, ds.FullOuterJoin(T("foo"), On(C("a").IsNull())).GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestFullOuterJoin_ToSQL() {
	ds1 := From("items")
	selectSQL, _, err := ds1.
		FullOuterJoin(T("categories"), On(Ex{"categories.categoryId": I("items.id")})).
		Order(C("stamp").Asc()).ToSQL()
	sds.NoError(err)
	sds.Equal(
		`SELECT * FROM "items" `+
			`FULL OUTER JOIN "categories" ON ("categories"."categoryId" = "items"."id") ORDER BY "stamp" ASC`,
		selectSQL,
	)

	selectSQL, _, err = ds1.FullOuterJoin(
		T("categories"),
		On(Ex{"categories.categoryId": I("items.id")}),
	).ToSQL()
	sds.NoError(err)
	sds.Equal(
		`SELECT * FROM "items" FULL OUTER JOIN "categories" ON ("categories"."categoryId" = "items"."id")`,
		selectSQL,
	)
}

func (sds *selectDatasetSuite) TestRightOuterJoin() {
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.JoinsAppend(
		exp.NewConditionedJoinExpression(exp.RightOuterJoinType, T("foo"), On(C("a").IsNull())),
	)
	sds.Equal(ec, ds.RightOuterJoin(T("foo"), On(C("a").IsNull())).GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestRightOuterJoin_ToSQL() {
	ds1 := From("items")
	selectSQL, _, err := ds1.RightOuterJoin(
		T("categories"),
		On(Ex{"categories.categoryId": I("items.id")}),
	).ToSQL()
	sds.NoError(err)
	sds.Equal(
		`SELECT * FROM "items" RIGHT OUTER JOIN "categories" ON ("categories"."categoryId" = "items"."id")`,
		selectSQL,
	)
}

func (sds *selectDatasetSuite) TestLeftOuterJoin() {
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.JoinsAppend(
		exp.NewConditionedJoinExpression(exp.LeftOuterJoinType, T("foo"), On(C("a").IsNull())),
	)
	sds.Equal(ec, ds.LeftOuterJoin(T("foo"), On(C("a").IsNull())).GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestLeftOuterJoin_ToSQL() {
	ds1 := From("items")

	selectSQL, _, err := ds1.LeftOuterJoin(T("categories"), On(Ex{
		"categories.categoryId": I("items.id"),
	})).ToSQL()
	sds.NoError(err)
	sds.Equal(
		`SELECT * FROM "items" LEFT OUTER JOIN "categories" ON ("categories"."categoryId" = "items"."id")`,
		selectSQL,
	)

	selectSQL, _, err = ds1.
		LeftOuterJoin(
			T("categories"),
			On(
				I("categories.categoryId").Eq(I("items.id")),
				I("categories.categoryId").In(1, 2, 3)),
		).ToSQL()
	sds.NoError(err)
	sds.Equal(
		`SELECT * FROM "items" `+
			`LEFT OUTER JOIN "categories" `+
			`ON (("categories"."categoryId" = "items"."id") AND ("categories"."categoryId" IN (1, 2, 3)))`,
		selectSQL,
	)

}

func (sds *selectDatasetSuite) TestFullJoin() {
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.JoinsAppend(
		exp.NewConditionedJoinExpression(exp.FullJoinType, T("foo"), On(C("a").IsNull())),
	)
	sds.Equal(ec, ds.FullJoin(T("foo"), On(C("a").IsNull())).GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestFullJoin_ToSQL() {
	ds1 := From("items")
	selectSQL, _, err := ds1.FullJoin(
		T("categories"),
		On(Ex{"categories.categoryId": I("items.id")}),
	).ToSQL()
	sds.NoError(err)
	sds.Equal(
		`SELECT * FROM "items" FULL JOIN "categories" ON ("categories"."categoryId" = "items"."id")`,
		selectSQL,
	)
}

func (sds *selectDatasetSuite) TestRightJoin() {
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.JoinsAppend(
		exp.NewConditionedJoinExpression(exp.RightJoinType, T("foo"), On(C("a").IsNull())),
	)
	sds.Equal(ec, ds.RightJoin(T("foo"), On(C("a").IsNull())).GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestRightJoin_ToSQL() {
	ds1 := From("items")
	selectSQL, _, err := ds1.RightJoin(
		T("categories"),
		On(Ex{"categories.categoryId": I("items.id")}),
	).ToSQL()
	sds.NoError(err)
	sds.Equal(
		`SELECT * FROM "items" RIGHT JOIN "categories" ON ("categories"."categoryId" = "items"."id")`,
		selectSQL,
	)
}

func (sds *selectDatasetSuite) TestLeftJoin() {
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.JoinsAppend(
		exp.NewConditionedJoinExpression(exp.LeftJoinType, T("foo"), On(C("a").IsNull())),
	)
	sds.Equal(ec, ds.LeftJoin(T("foo"), On(C("a").IsNull())).GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestLeftJoin_ToSQL() {
	ds1 := From("items")
	selectSQL, _, err := ds1.LeftJoin(
		T("categories"),
		On(Ex{"categories.categoryId": I("items.id")}),
	).ToSQL()
	sds.NoError(err)
	sds.Equal(
		`SELECT * FROM "items" LEFT JOIN "categories" ON ("categories"."categoryId" = "items"."id")`,
		selectSQL,
	)
}

func (sds *selectDatasetSuite) TestNaturalJoin() {
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.JoinsAppend(
		exp.NewUnConditionedJoinExpression(exp.NaturalJoinType, T("foo")),
	)
	sds.Equal(ec, ds.NaturalJoin(T("foo")).GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestNaturalJoin_ToSQL() {
	ds1 := From("items")
	selectSQL, _, err := ds1.NaturalJoin(T("categories")).ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT * FROM "items" NATURAL JOIN "categories"`, selectSQL)
}

func (sds *selectDatasetSuite) TestNaturalLeftJoin() {
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.JoinsAppend(
		exp.NewUnConditionedJoinExpression(exp.NaturalLeftJoinType, T("foo")),
	)
	sds.Equal(ec, ds.NaturalLeftJoin(T("foo")).GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestNaturalLeftJoin_ToSQL() {
	ds1 := From("items")
	selectSQL, _, err := ds1.NaturalLeftJoin(T("categories")).ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT * FROM "items" NATURAL LEFT JOIN "categories"`, selectSQL)

}

func (sds *selectDatasetSuite) TestNaturalRightJoin_ToSQL() {
	ds1 := From("items")
	selectSQL, _, err := ds1.NaturalRightJoin(T("categories")).ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT * FROM "items" NATURAL RIGHT JOIN "categories"`, selectSQL)
}

func (sds *selectDatasetSuite) TestNaturalRightJoin() {
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.JoinsAppend(
		exp.NewUnConditionedJoinExpression(exp.NaturalRightJoinType, T("foo")),
	)
	sds.Equal(ec, ds.NaturalRightJoin(T("foo")).GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}
func (sds *selectDatasetSuite) TestNaturalFullJoin() {
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.JoinsAppend(
		exp.NewUnConditionedJoinExpression(exp.NaturalFullJoinType, T("foo")),
	)
	sds.Equal(ec, ds.NaturalFullJoin(T("foo")).GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestNaturalFullJoin_ToSQL() {
	ds1 := From("items")
	selectSQL, _, err := ds1.NaturalFullJoin(T("categories")).ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT * FROM "items" NATURAL FULL JOIN "categories"`, selectSQL)
}

func (sds *selectDatasetSuite) TestCrossJoin() {
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.JoinsAppend(
		exp.NewUnConditionedJoinExpression(exp.CrossJoinType, T("foo")),
	)
	sds.Equal(ec, ds.CrossJoin(T("foo")).GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestCrossJoin_ToSQL() {
	selectSQL, _, err := From("items").CrossJoin(T("categories")).ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT * FROM "items" CROSS JOIN "categories"`, selectSQL)
}

func (sds *selectDatasetSuite) TestWhere() {
	ds := From("test")
	dsc := ds.GetClauses()
	w := Ex{
		"a": 1,
	}
	ec := dsc.WhereAppend(w)
	sds.Equal(ec, ds.Where(w).GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestWhere_ToSQL() {
	ds1 := From("test")

	b := ds1.Where(
		C("a").Eq(true),
		C("a").Neq(true),
		C("a").Eq(false),
		C("a").Neq(false),
	)
	selectSQL, args, err := b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(
		`SELECT * FROM "test" WHERE (("a" IS TRUE) AND ("a" IS NOT TRUE) AND ("a" IS FALSE) AND ("a" IS NOT FALSE))`,
		selectSQL,
	)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(
		`SELECT * FROM "test" WHERE (("a" IS TRUE) AND ("a" IS NOT TRUE) AND ("a" IS FALSE) AND ("a" IS NOT FALSE))`,
		selectSQL,
	)

	b = ds1.Where(
		C("a").Eq("a"),
		C("b").Neq("b"),
		C("c").Gt("c"),
		C("d").Gte("d"),
		C("e").Lt("e"),
		C("f").Lte("f"),
	)
	selectSQL, args, err = b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(
		`SELECT * FROM "test" `+
			`WHERE (("a" = 'a') AND ("b" != 'b') AND ("c" > 'c') AND ("d" >= 'd') AND ("e" < 'e') AND ("f" <= 'f'))`,
		selectSQL,
	)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{"a", "b", "c", "d", "e", "f"}, args)
	sds.Equal(
		`SELECT * FROM "test" `+
			`WHERE (("a" = ?) AND ("b" != ?) AND ("c" > ?) AND ("d" >= ?) AND ("e" < ?) AND ("f" <= ?))`,
		selectSQL,
	)

	b = ds1.Where(
		C("a").Eq(From("test2").Select("id")),
	)
	selectSQL, args, err = b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" IN (SELECT "id" FROM "test2"))`, selectSQL)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" IN (SELECT "id" FROM "test2"))`, selectSQL)

	b = ds1.Where(Ex{
		"a": "a",
		"b": Op{"neq": "b"},
		"c": Op{"gt": "c"},
		"d": Op{"gte": "d"},
		"e": Op{"lt": "e"},
		"f": Op{"lte": "f"},
	})
	selectSQL, args, err = b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(
		`SELECT * FROM "test" `+
			`WHERE (("a" = 'a') AND ("b" != 'b') AND ("c" > 'c') AND ("d" >= 'd') AND ("e" < 'e') AND ("f" <= 'f'))`,
		selectSQL,
	)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{"a", "b", "c", "d", "e", "f"}, args)
	sds.Equal(
		`SELECT * FROM "test" `+
			`WHERE (("a" = ?) AND ("b" != ?) AND ("c" > ?) AND ("d" >= ?) AND ("e" < ?) AND ("f" <= ?))`,
		selectSQL,
	)

	b = ds1.Where(Ex{
		"a": From("test2").Select("id"),
	})
	selectSQL, args, err = b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" IN (SELECT "id" FROM "test2"))`, selectSQL)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" IN (SELECT "id" FROM "test2"))`, selectSQL)
}

func (sds *selectDatasetSuite) TestWhere_ToSQLEmpty() {
	ds1 := From("test")

	b := ds1.Where()
	selectSQL, _, err := b.ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT * FROM "test"`, selectSQL)
}

func (sds *selectDatasetSuite) TestWhere_ToSQLWithChain() {
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
	sds.NoError(err)
	sds.Equal(
		`SELECT * FROM "test" WHERE (("x" = 0) AND ("y" = 1) AND ("z" = 2) AND ("a" = 'A'))`,
		selectSQL,
	)
	selectSQL, _, err = b.ToSQL()
	sds.NoError(err)
	sds.Equal(
		`SELECT * FROM "test" WHERE (("x" = 0) AND ("y" = 1) AND ("z" = 2) AND ("b" = 'B'))`,
		selectSQL,
	)
}

func (sds *selectDatasetSuite) TestClearWhere() {
	w := Ex{
		"a": 1,
	}
	ds := From("test").Where(w)
	dsc := ds.GetClauses()
	ec := dsc.ClearWhere()
	sds.Equal(ec, ds.ClearWhere().GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestClearWhere_ToSQL() {
	ds1 := From("test")

	b := ds1.Where(
		C("a").Eq(1),
	).ClearWhere()
	selectSQL, _, err := b.ToSQL()
	sds.NoError(err)
	sds.Equal(`SELECT * FROM "test"`, selectSQL)
}

func (sds *selectDatasetSuite) TestForUpdate() {
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetLock(exp.NewLock(exp.ForUpdate, NoWait))
	sds.Equal(ec, ds.ForUpdate(NoWait).GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestForUpdate_ToSQL() {
	ds1 := From("test")

	b := ds1.Where(C("a").Gt(1)).ForUpdate(Wait)
	selectSQL, args, err := b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > 1) FOR UPDATE `, selectSQL)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1)}, args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > ?) FOR UPDATE `, selectSQL)

	b = ds1.Where(C("a").Gt(1)).ForUpdate(NoWait)
	selectSQL, args, err = b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > 1) FOR UPDATE NOWAIT`, selectSQL)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1)}, args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > ?) FOR UPDATE NOWAIT`, selectSQL)

	b = ds1.Where(C("a").Gt(1)).ForUpdate(SkipLocked)
	selectSQL, args, err = b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > 1) FOR UPDATE SKIP LOCKED`, selectSQL)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1)}, args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > ?) FOR UPDATE SKIP LOCKED`, selectSQL)
}

func (sds *selectDatasetSuite) TestForNoKeyUpdate() {
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetLock(exp.NewLock(exp.ForNoKeyUpdate, NoWait))
	sds.Equal(ec, ds.ForNoKeyUpdate(NoWait).GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestForNoKeyUpdate_ToSQL() {
	ds1 := From("test")

	b := ds1.Where(C("a").Gt(1)).ForNoKeyUpdate(Wait)
	selectSQL, args, err := b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > 1) FOR NO KEY UPDATE `, selectSQL)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1)}, args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > ?) FOR NO KEY UPDATE `, selectSQL)

	b = ds1.Where(C("a").Gt(1)).ForNoKeyUpdate(NoWait)
	selectSQL, args, err = b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > 1) FOR NO KEY UPDATE NOWAIT`, selectSQL)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1)}, args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > ?) FOR NO KEY UPDATE NOWAIT`, selectSQL)

	b = ds1.Where(C("a").Gt(1)).ForNoKeyUpdate(SkipLocked)
	selectSQL, args, err = b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > 1) FOR NO KEY UPDATE SKIP LOCKED`, selectSQL)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1)}, args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > ?) FOR NO KEY UPDATE SKIP LOCKED`, selectSQL)
}

func (sds *selectDatasetSuite) TestForKeyShare() {
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetLock(exp.NewLock(exp.ForKeyShare, NoWait))
	sds.Equal(ec, ds.ForKeyShare(NoWait).GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestForKeyShare_ToSQL() {
	ds1 := From("test")

	b := ds1.Where(C("a").Gt(1)).ForKeyShare(Wait)
	selectSQL, args, err := b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > 1) FOR KEY SHARE `, selectSQL)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1)}, args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > ?) FOR KEY SHARE `, selectSQL)

	b = ds1.Where(C("a").Gt(1)).ForKeyShare(NoWait)
	selectSQL, args, err = b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > 1) FOR KEY SHARE NOWAIT`, selectSQL)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1)}, args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > ?) FOR KEY SHARE NOWAIT`, selectSQL)

	b = ds1.Where(C("a").Gt(1)).ForKeyShare(SkipLocked)
	selectSQL, args, err = b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > 1) FOR KEY SHARE SKIP LOCKED`, selectSQL)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1)}, args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > ?) FOR KEY SHARE SKIP LOCKED`, selectSQL)
}

func (sds *selectDatasetSuite) TestForShare() {
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetLock(exp.NewLock(exp.ForShare, NoWait))
	sds.Equal(ec, ds.ForShare(NoWait).GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestForShare_ToSQL() {
	ds1 := From("test")

	b := ds1.Where(C("a").Gt(1)).ForShare(Wait)
	selectSQL, args, err := b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > 1) FOR SHARE `, selectSQL)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1)}, args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > ?) FOR SHARE `, selectSQL)

	b = ds1.Where(C("a").Gt(1)).ForShare(NoWait)
	selectSQL, args, err = b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > 1) FOR SHARE NOWAIT`, selectSQL)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1)}, args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > ?) FOR SHARE NOWAIT`, selectSQL)

	b = ds1.Where(C("a").Gt(1)).ForShare(SkipLocked)
	selectSQL, args, err = b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > 1) FOR SHARE SKIP LOCKED`, selectSQL)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1)}, args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > ?) FOR SHARE SKIP LOCKED`, selectSQL)
}

func (sds *selectDatasetSuite) TestGroupBy() {
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetGroupBy(exp.NewColumnListExpression(C("a")))
	sds.Equal(ec, ds.GroupBy("a").GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestGroupBy_ToSQL() {
	ds1 := From("test")

	b := ds1.Where(C("a").Gt(1)).GroupBy("created")
	selectSQL, args, err := b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > 1) GROUP BY "created"`, selectSQL)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1)}, args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > ?) GROUP BY "created"`, selectSQL)

	b = ds1.Where(C("a").Gt(1)).GroupBy(L("created::DATE"))
	selectSQL, args, err = b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > 1) GROUP BY created::DATE`, selectSQL)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1)}, args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > ?) GROUP BY created::DATE`, selectSQL)

	b = ds1.Where(C("a").Gt(1)).GroupBy("name", L("created::DATE"))
	selectSQL, args, err = b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > 1) GROUP BY "name", created::DATE`, selectSQL)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1)}, args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > ?) GROUP BY "name", created::DATE`, selectSQL)
}

func (sds *selectDatasetSuite) TestHaving() {
	ds := From("test")
	dsc := ds.GetClauses()
	h := C("a").Gt(1)
	ec := dsc.HavingAppend(h)
	sds.Equal(ec, ds.Having(h).GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestWindows() {
	ds := From("test")
	dsc := ds.GetClauses()
	w := W("w").PartitionBy("a").OrderBy("b")
	ec := dsc.SetWindows([]exp.WindowExpression{w})
	sds.Equal(ec, ds.Windows(w).GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestHaving_ToSQL() {
	ds1 := From("test")

	b := ds1.Having(Ex{"a": Op{"gt": 1}}).GroupBy("created")
	selectSQL, args, err := b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "test" GROUP BY "created" HAVING ("a" > 1)`, selectSQL)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1)}, args)
	sds.Equal(`SELECT * FROM "test" GROUP BY "created" HAVING ("a" > ?)`, selectSQL)

	b = ds1.Where(Ex{"b": true}).Having(Ex{"a": Op{"gt": 1}}).GroupBy("created")
	selectSQL, args, err = b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "test" WHERE ("b" IS TRUE) GROUP BY "created" HAVING ("a" > 1)`, selectSQL)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1)}, args)
	sds.Equal(`SELECT * FROM "test" WHERE ("b" IS TRUE) GROUP BY "created" HAVING ("a" > ?)`, selectSQL)

	b = ds1.Having(Ex{"a": Op{"gt": 1}})
	selectSQL, args, err = b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "test" HAVING ("a" > 1)`, selectSQL)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1)}, args)
	sds.Equal(`SELECT * FROM "test" HAVING ("a" > ?)`, selectSQL)

	b = ds1.Having(Ex{"a": Op{"gt": 1}}).Having(Ex{"b": 2})
	selectSQL, args, err = b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "test" HAVING (("a" > 1) AND ("b" = 2))`, selectSQL)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1), int64(2)}, args)
	sds.Equal(`SELECT * FROM "test" HAVING (("a" > ?) AND ("b" = ?))`, selectSQL)

	b = ds1.GroupBy("name").Having(SUM("amount").Gt(0))
	selectSQL, args, err = b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "test" GROUP BY "name" HAVING (SUM("amount") > 0)`, selectSQL)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(0)}, args)
	sds.Equal(`SELECT * FROM "test" GROUP BY "name" HAVING (SUM("amount") > ?)`, selectSQL)
}

func (sds *selectDatasetSuite) TestOrder() {
	ds := From("test")
	dsc := ds.GetClauses()
	o := C("a").Desc()
	ec := dsc.SetOrder(o)
	sds.Equal(ec, ds.Order(o).GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestOrder_ToSQL() {

	ds1 := From("test")

	b := ds1.Order(C("a").Asc(), L(`("a" + "b" > 2)`).Asc())
	selectSQL, args, err := b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "test" ORDER BY "a" ASC, ("a" + "b" > 2) ASC`, selectSQL)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "test" ORDER BY "a" ASC, ("a" + "b" > 2) ASC`, selectSQL)
}

func (sds *selectDatasetSuite) TestOrderAppend() {
	ds := From("test").Order(C("a").Desc())
	dsc := ds.GetClauses()
	o := C("b").Desc()
	ec := dsc.OrderAppend(o)
	sds.Equal(ec, ds.OrderAppend(o).GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestOrderAppend_ToSQL() {
	b := From("test").Order(C("a").Asc().NullsFirst()).OrderAppend(C("b").Desc().NullsLast())
	selectSQL, args, err := b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "test" ORDER BY "a" ASC NULLS FIRST, "b" DESC NULLS LAST`, selectSQL)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "test" ORDER BY "a" ASC NULLS FIRST, "b" DESC NULLS LAST`, selectSQL)

	b = From("test").OrderAppend(C("a").Asc().NullsFirst()).OrderAppend(C("b").Desc().NullsLast())
	selectSQL, args, err = b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "test" ORDER BY "a" ASC NULLS FIRST, "b" DESC NULLS LAST`, selectSQL)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "test" ORDER BY "a" ASC NULLS FIRST, "b" DESC NULLS LAST`, selectSQL)

}

func (sds *selectDatasetSuite) TestClearOrder() {
	ds := From("test").Order(C("a").Desc())
	dsc := ds.GetClauses()
	ec := dsc.ClearOrder()
	sds.Equal(ec, ds.ClearOrder().GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestClearOrder_ToSQL() {
	b := From("test").Order(C("a").Asc().NullsFirst()).ClearOrder()
	selectSQL, args, err := b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "test"`, selectSQL)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "test"`, selectSQL)
}

func (sds *selectDatasetSuite) TestLimit() {
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetLimit(uint(1))
	sds.Equal(ec, ds.Limit(1).GetClauses())
	sds.Equal(dsc, ds.Limit(0).GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestLimit_ToSQL() {
	ds1 := From("test")

	b := ds1.Where(C("a").Gt(1)).Limit(10)
	selectSQL, args, err := b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > 1) LIMIT 10`, selectSQL)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1), int64(10)}, args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > ?) LIMIT ?`, selectSQL)

	b = ds1.Where(C("a").Gt(1)).Limit(0)
	selectSQL, args, err = b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > 1)`, selectSQL)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1)}, args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > ?)`, selectSQL)
}

func (sds *selectDatasetSuite) TestLimitAll() {
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetLimit(L("ALL"))
	sds.Equal(ec, ds.LimitAll().GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestLimitAll_ToSQL() {
	ds1 := From("test")

	b := ds1.Where(C("a").Gt(1)).LimitAll()
	selectSQL, args, err := b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > 1) LIMIT ALL`, selectSQL)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1)}, args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > ?) LIMIT ALL`, selectSQL)

	b = ds1.Where(C("a").Gt(1)).Limit(0).LimitAll()
	selectSQL, args, err = b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > 1) LIMIT ALL`, selectSQL)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1)}, args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > ?) LIMIT ALL`, selectSQL)
}

func (sds *selectDatasetSuite) TestClearLimit() {
	ds := From("test").Limit(1)
	dsc := ds.GetClauses()
	ec := dsc.ClearLimit()
	sds.Equal(ec, ds.ClearLimit().GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestClearLimit_ToSQL() {
	ds1 := From("test")

	b := ds1.Where(C("a").Gt(1)).LimitAll().ClearLimit()
	selectSQL, args, err := b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > 1)`, selectSQL)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1)}, args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > ?)`, selectSQL)

	b = ds1.Where(C("a").Gt(1)).Limit(10).ClearLimit()
	selectSQL, args, err = b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > 1)`, selectSQL)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1)}, args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > ?)`, selectSQL)
}

func (sds *selectDatasetSuite) TestOffset() {
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetOffset(1)
	sds.Equal(ec, ds.Offset(1).GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestOffset_ToSQL() {
	ds1 := From("test")

	b := ds1.Where(C("a").Gt(1)).Offset(10)
	selectSQL, args, err := b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > 1) OFFSET 10`, selectSQL)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1), int64(10)}, args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > ?) OFFSET ?`, selectSQL)

	b = ds1.Where(C("a").Gt(1)).Offset(0)
	selectSQL, args, err = b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > 1)`, selectSQL)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1)}, args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > ?)`, selectSQL)
}

func (sds *selectDatasetSuite) TestClearOffset() {
	ds := From("test").Offset(1)
	dsc := ds.GetClauses()
	ec := dsc.ClearOffset()
	sds.Equal(ec, ds.ClearOffset().GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestClearOffset_ToSQL() {
	ds1 := From("test")

	b := ds1.Where(C("a").Gt(1)).Offset(10).ClearOffset()
	selectSQL, args, err := b.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > 1)`, selectSQL)

	selectSQL, args, err = b.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1)}, args)
	sds.Equal(`SELECT * FROM "test" WHERE ("a" > ?)`, selectSQL)
}

func (sds *selectDatasetSuite) TestUnion() {
	uds := From("union_test")
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.CompoundsAppend(exp.NewCompoundExpression(exp.UnionCompoundType, uds))
	sds.Equal(ec, ds.Union(uds).GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestUnion_ToSQL() {
	a := From("invoice").Select("id", "amount").Where(C("amount").Gt(1000))
	b := From("invoice").Select("id", "amount").Where(C("amount").Lt(10))

	ds := a.Union(b)
	selectSQL, args, err := ds.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
			`UNION (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`,
		selectSQL,
	)

	ds = a.Limit(1).Union(b)
	selectSQL, args, err = ds.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(
		`SELECT * FROM (`+
			`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) LIMIT 1) AS "t1" `+
			`UNION (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`,
		selectSQL,
	)

	ds = a.Order(C("id").Asc()).Union(b)
	selectSQL, args, err = ds.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(
		`SELECT * FROM `+
			`(SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) ORDER BY "id" ASC) AS "t1" `+
			`UNION (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`,
		selectSQL,
	)

	selectSQL, args, err = ds.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1000), int64(10)}, args)
	sds.Equal(
		`SELECT * FROM `+
			`(SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) ORDER BY "id" ASC) AS "t1" `+
			`UNION (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?))`,
		selectSQL,
	)

	ds = a.Union(b.Limit(1))
	selectSQL, args, err = ds.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
			`UNION (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) LIMIT 1) AS "t1")`,
		selectSQL,
	)

	selectSQL, args, err = ds.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1000), int64(10), int64(1)}, args)
	sds.Equal(
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) `+
			`UNION (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?) LIMIT ?) AS "t1")`,
		selectSQL,
	)

	ds = a.Union(b.Order(C("id").Desc()))
	selectSQL, args, err = ds.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
			`UNION (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) ORDER BY "id" DESC) AS "t1")`,
		selectSQL,
	)

	selectSQL, args, err = ds.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1000), int64(10)}, args)
	sds.Equal(
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) `+
			`UNION (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?) ORDER BY "id" DESC) AS "t1")`,
		selectSQL,
	)

	ds = a.Limit(1).Union(b.Order(C("id").Desc()))
	selectSQL, args, err = ds.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(
		`SELECT * FROM (`+
			`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) LIMIT 1) AS "t1" `+
			`UNION (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) ORDER BY "id" DESC) AS "t1")`,
		selectSQL,
	)

	selectSQL, args, err = ds.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1000), int64(1), int64(10)}, args)
	sds.Equal(
		`SELECT * FROM (`+
			`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) LIMIT ?) AS "t1" `+
			`UNION (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?) ORDER BY "id" DESC) AS "t1")`,
		selectSQL,
	)

	ds = a.Union(b).Union(b.Where(C("id").Lt(50)))
	selectSQL, args, err = ds.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
			`UNION (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10)) `+
			`UNION (SELECT "id", "amount" FROM "invoice" WHERE (("amount" < 10) AND ("id" < 50)))`,
		selectSQL,
	)

	selectSQL, args, err = ds.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1000), int64(10), int64(10), int64(50)}, args)
	sds.Equal(
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) `+
			`UNION (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?)) `+
			`UNION (SELECT "id", "amount" FROM "invoice" WHERE (("amount" < ?) AND ("id" < ?)))`,
		selectSQL,
	)

}

func (sds *selectDatasetSuite) TestUnionAll() {
	uds := From("union_test")
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.CompoundsAppend(exp.NewCompoundExpression(exp.UnionAllCompoundType, uds))
	sds.Equal(ec, ds.UnionAll(uds).GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestUnionAll_ToSQL() {
	a := From("invoice").Select("id", "amount").Where(C("amount").Gt(1000))
	b := From("invoice").Select("id", "amount").Where(C("amount").Lt(10))

	ds := a.UnionAll(b)
	selectSQL, args, err := ds.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
			`UNION ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`,
		selectSQL,
	)

	ds = a.Limit(1).UnionAll(b)
	selectSQL, args, err = ds.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(
		`SELECT * FROM (`+
			`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) LIMIT 1) AS "t1" `+
			`UNION ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`,
		selectSQL,
	)

	ds = a.Order(C("id").Asc()).UnionAll(b)
	selectSQL, args, err = ds.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(
		`SELECT * FROM `+
			`(SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) ORDER BY "id" ASC) AS "t1" `+
			`UNION ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`,
		selectSQL,
	)

	selectSQL, args, err = ds.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1000), int64(10)}, args)
	sds.Equal(
		`SELECT * FROM `+
			`(SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) ORDER BY "id" ASC) AS "t1" `+
			`UNION ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?))`,
		selectSQL,
	)

	ds = a.UnionAll(b.Limit(1))
	selectSQL, args, err = ds.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
			`UNION ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) LIMIT 1) AS "t1")`,
		selectSQL,
	)

	selectSQL, args, err = ds.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1000), int64(10), int64(1)}, args)
	sds.Equal(
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) `+
			`UNION ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?) LIMIT ?) AS "t1")`,
		selectSQL,
	)

	ds = a.UnionAll(b.Order(C("id").Desc()))
	selectSQL, args, err = ds.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
			`UNION ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) ORDER BY "id" DESC) AS "t1")`,
		selectSQL,
	)

	selectSQL, args, err = ds.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1000), int64(10)}, args)
	sds.Equal(
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) `+
			`UNION ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?) ORDER BY "id" DESC) AS "t1")`,
		selectSQL,
	)

	ds = a.Limit(1).UnionAll(b.Order(C("id").Desc()))
	selectSQL, args, err = ds.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(
		`SELECT * FROM (`+
			`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) LIMIT 1) AS "t1" `+
			`UNION ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) ORDER BY "id" DESC) AS "t1")`,
		selectSQL,
	)

	selectSQL, args, err = ds.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1000), int64(1), int64(10)}, args)
	sds.Equal(
		`SELECT * FROM (`+
			`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) LIMIT ?) AS "t1" `+
			`UNION ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?) ORDER BY "id" DESC) AS "t1")`,
		selectSQL,
	)

	ds = a.UnionAll(b).UnionAll(b.Where(C("id").Lt(50)))
	selectSQL, args, err = ds.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
			`UNION ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10)) `+
			`UNION ALL (SELECT "id", "amount" FROM "invoice" WHERE (("amount" < 10) AND ("id" < 50)))`,
		selectSQL,
	)

	selectSQL, args, err = ds.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1000), int64(10), int64(10), int64(50)}, args)
	sds.Equal(
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) `+
			`UNION ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?)) `+
			`UNION ALL (SELECT "id", "amount" FROM "invoice" WHERE (("amount" < ?) AND ("id" < ?)))`,
		selectSQL,
	)

}

func (sds *selectDatasetSuite) TestIntersect() {
	uds := From("union_test")
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.CompoundsAppend(exp.NewCompoundExpression(exp.IntersectCompoundType, uds))
	sds.Equal(ec, ds.Intersect(uds).GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestIntersect_ToSQL() {
	a := From("invoice").Select("id", "amount").Where(C("amount").Gt(1000))
	b := From("invoice").Select("id", "amount").Where(C("amount").Lt(10))

	ds := a.Intersect(b)
	selectSQL, args, err := ds.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
			`INTERSECT (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`,
		selectSQL,
	)

	ds = a.Limit(1).Intersect(b)
	selectSQL, args, err = ds.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(
		`SELECT * FROM (`+
			`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) LIMIT 1) AS "t1" `+
			`INTERSECT (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`,
		selectSQL,
	)

	ds = a.Order(C("id").Asc()).Intersect(b)
	selectSQL, args, err = ds.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(
		`SELECT * FROM `+
			`(SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) ORDER BY "id" ASC) AS "t1" `+
			`INTERSECT (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`,
		selectSQL,
	)

	selectSQL, args, err = ds.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1000), int64(10)}, args)
	sds.Equal(
		`SELECT * FROM `+
			`(SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) ORDER BY "id" ASC) AS "t1" `+
			`INTERSECT (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?))`,
		selectSQL,
	)

	ds = a.Intersect(b.Limit(1))
	selectSQL, args, err = ds.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
			`INTERSECT (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) LIMIT 1) AS "t1")`,
		selectSQL,
	)

	selectSQL, args, err = ds.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1000), int64(10), int64(1)}, args)
	sds.Equal(
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) `+
			`INTERSECT (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?) LIMIT ?) AS "t1")`,
		selectSQL,
	)

	ds = a.Intersect(b.Order(C("id").Desc()))
	selectSQL, args, err = ds.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
			`INTERSECT (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) ORDER BY "id" DESC) AS "t1")`,
		selectSQL,
	)

	selectSQL, args, err = ds.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1000), int64(10)}, args)
	sds.Equal(
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) `+
			`INTERSECT (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?) ORDER BY "id" DESC) AS "t1")`,
		selectSQL,
	)

	ds = a.Limit(1).Intersect(b.Order(C("id").Desc()))
	selectSQL, args, err = ds.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(
		`SELECT * FROM (`+
			`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) LIMIT 1) AS "t1" `+
			`INTERSECT (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) ORDER BY "id" DESC) AS "t1")`,
		selectSQL,
	)

	selectSQL, args, err = ds.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1000), int64(1), int64(10)}, args)
	sds.Equal(
		`SELECT * FROM (`+
			`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) LIMIT ?) AS "t1" `+
			`INTERSECT (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?) ORDER BY "id" DESC) AS "t1")`,
		selectSQL,
	)

	ds = a.Intersect(b).Intersect(b.Where(C("id").Lt(50)))
	selectSQL, args, err = ds.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
			`INTERSECT (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10)) `+
			`INTERSECT (SELECT "id", "amount" FROM "invoice" WHERE (("amount" < 10) AND ("id" < 50)))`,
		selectSQL,
	)

	selectSQL, args, err = ds.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal(args, []interface{}{int64(1000), int64(10), int64(10), int64(50)})
	sds.Equal(
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) `+
			`INTERSECT (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?)) `+
			`INTERSECT (SELECT "id", "amount" FROM "invoice" WHERE (("amount" < ?) AND ("id" < ?)))`,
		selectSQL,
	)
}

func (sds *selectDatasetSuite) TestIntersectAll() {
	uds := From("union_test")
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.CompoundsAppend(exp.NewCompoundExpression(exp.IntersectAllCompoundType, uds))
	sds.Equal(ec, ds.IntersectAll(uds).GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestIntersectAll_ToSQL() {
	a := From("invoice").Select("id", "amount").Where(C("amount").Gt(1000))
	b := From("invoice").Select("id", "amount").Where(C("amount").Lt(10))

	ds := a.IntersectAll(b)
	selectSQL, args, err := ds.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
			`INTERSECT ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`,
		selectSQL,
	)

	ds = a.Limit(1).IntersectAll(b)
	selectSQL, args, err = ds.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(
		`SELECT * FROM (`+
			`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) LIMIT 1) AS "t1" `+
			`INTERSECT ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`,
		selectSQL,
	)

	ds = a.Order(C("id").Asc()).IntersectAll(b)
	selectSQL, args, err = ds.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(
		`SELECT * FROM `+
			`(SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) ORDER BY "id" ASC) AS "t1" `+
			`INTERSECT ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10))`,
		selectSQL,
	)

	selectSQL, args, err = ds.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1000), int64(10)}, args)
	sds.Equal(
		`SELECT * FROM `+
			`(SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) ORDER BY "id" ASC) AS "t1" `+
			`INTERSECT ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?))`,
		selectSQL,
	)

	ds = a.IntersectAll(b.Limit(1))
	selectSQL, args, err = ds.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
			`INTERSECT ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) LIMIT 1) AS "t1")`,
		selectSQL,
	)

	selectSQL, args, err = ds.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal([]interface{}{int64(1000), int64(10), int64(1)}, args)
	sds.Equal(
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) `+
			`INTERSECT ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?) LIMIT ?) AS "t1")`,
		selectSQL,
	)

	ds = a.IntersectAll(b.Order(C("id").Desc()))
	selectSQL, args, err = ds.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
			`INTERSECT ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) ORDER BY "id" DESC) AS "t1")`,
		selectSQL,
	)

	selectSQL, args, err = ds.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal(args, []interface{}{int64(1000), int64(10)})
	sds.Equal(
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) `+
			`INTERSECT ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?) ORDER BY "id" DESC) AS "t1")`,
		selectSQL,
	)

	ds = a.Limit(1).IntersectAll(b.Order(C("id").Desc()))
	selectSQL, args, err = ds.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(
		`SELECT * FROM (`+
			`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) LIMIT 1) AS "t1" `+
			`INTERSECT ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10) ORDER BY "id" DESC) AS "t1")`,
		selectSQL,
	)

	selectSQL, args, err = ds.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal(args, []interface{}{int64(1000), int64(1), int64(10)})
	sds.Equal(
		`SELECT * FROM (`+
			`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) LIMIT ?) AS "t1" `+
			`INTERSECT ALL (SELECT * FROM (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?) ORDER BY "id" DESC) AS "t1")`,
		selectSQL,
	)

	ds = a.IntersectAll(b).IntersectAll(b.Where(C("id").Lt(50)))
	selectSQL, args, err = ds.ToSQL()
	sds.NoError(err)
	sds.Empty(args)
	sds.Equal(
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > 1000) `+
			`INTERSECT ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < 10)) `+
			`INTERSECT ALL (SELECT "id", "amount" FROM "invoice" WHERE (("amount" < 10) AND ("id" < 50)))`,
		selectSQL,
	)

	selectSQL, args, err = ds.Prepared(true).ToSQL()
	sds.NoError(err)
	sds.Equal(args, []interface{}{int64(1000), int64(10), int64(10), int64(50)})
	sds.Equal(
		`SELECT "id", "amount" FROM "invoice" WHERE ("amount" > ?) `+
			`INTERSECT ALL (SELECT "id", "amount" FROM "invoice" WHERE ("amount" < ?)) `+
			`INTERSECT ALL (SELECT "id", "amount" FROM "invoice" WHERE (("amount" < ?) AND ("id" < ?)))`,
		selectSQL,
	)
}

func (sds *selectDatasetSuite) TestAs() {
	ds := From("test")
	dsc := ds.GetClauses()
	ec := dsc.SetAlias(T("a"))
	sds.Equal(ec, ds.As("a").GetClauses())
	sds.Equal(dsc, ds.GetClauses())
}

func (sds *selectDatasetSuite) TestToSQL() {
	md := new(mocks.SQLDialect)
	ds := From("test").SetDialect(md)
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(false)
	md.On("ToSelectSQL", sqlB, c).Return(nil).Once()
	sql, args, err := ds.ToSQL()
	sds.Empty(sql)
	sds.Empty(args)
	sds.Nil(err)
	md.AssertExpectations(sds.T())
}

func (sds *selectDatasetSuite) TestToSQL_ReturnedError() {
	md := new(mocks.SQLDialect)
	ds := From("test").SetDialect(md)
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(false)
	ee := errors.New("expected error")
	md.On("ToSelectSQL", sqlB, c).Run(func(args mock.Arguments) {
		args.Get(0).(sb.SQLBuilder).SetError(ee)
	}).Once()

	sql, args, err := ds.ToSQL()
	sds.Empty(sql)
	sds.Empty(args)
	sds.Equal(ee, err)
	md.AssertExpectations(sds.T())
}

func (sds *selectDatasetSuite) TestAppendSQL() {
	md := new(mocks.SQLDialect)
	ds := From("test").SetDialect(md)
	c := ds.GetClauses()
	sqlB := sb.NewSQLBuilder(false)
	md.On("ToSelectSQL", sqlB, c).Return(nil).Once()
	ds.AppendSQL(sqlB)
	sds.NoError(sqlB.Error())
	md.AssertExpectations(sds.T())
}

func (sds *selectDatasetSuite) TestScanStructs() {
	mDb, sqlMock, err := sqlmock.New()
	sds.NoError(err)
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
	sds.NoError(ds.From("items").ScanStructs(&items))
	sds.Equal([]dsTestActionItem{
		{Address: "111 Test Addr", Name: "Test1"},
		{Address: "211 Test Addr", Name: "Test2"},
	}, items)

	items = items[0:0]
	sds.NoError(ds.From("items").Select("name").Distinct().ScanStructs(&items))
	sds.Equal([]dsTestActionItem{
		{Address: "111 Test Addr", Name: "Test1"},
		{Address: "211 Test Addr", Name: "Test2"},
	}, items)

	items = items[0:0]
	sds.EqualError(ds.From("items").ScanStructs(items),
		"goqu: type must be a pointer to a slice when scanning into structs")
	sds.EqualError(ds.From("items").ScanStructs(&dsTestActionItem{}),
		"goqu: type must be a pointer to a slice when scanning into structs")
	sds.EqualError(ds.From("items").Select("test").ScanStructs(&items),
		`goqu: unable to find corresponding field to column "test" returned by query`)
}

func (sds *selectDatasetSuite) TestScanStructs_WithPreparedStatements() {
	mDb, sqlMock, err := sqlmock.New()
	sds.NoError(err)
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
	sds.NoError(ds.From("items").Prepared(true).Where(Ex{
		"name":    []string{"Bob", "Sally", "Billy"},
		"address": "111 Test Addr",
	}).ScanStructs(&items))
	sds.Equal(items, []dsTestActionItem{
		{Address: "111 Test Addr", Name: "Test1"},
		{Address: "211 Test Addr", Name: "Test2"},
	})

	items = items[0:0]
	sds.EqualError(ds.From("items").ScanStructs(items),
		"goqu: type must be a pointer to a slice when scanning into structs")
	sds.EqualError(ds.From("items").ScanStructs(&dsTestActionItem{}),
		"goqu: type must be a pointer to a slice when scanning into structs")
	sds.EqualError(ds.From("items").
		Prepared(true).
		Select("test").
		Where(Ex{"name": []string{"Bob", "Sally", "Billy"}, "address": "111 Test Addr"}).
		ScanStructs(&items), `goqu: unable to find corresponding field to column "test" returned by query`)
}

func (sds *selectDatasetSuite) TestScanStruct() {
	mDb, sqlMock, err := sqlmock.New()
	sds.NoError(err)
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
	sds.NoError(err)
	sds.True(found)
	sds.Equal("111 Test Addr", item.Address)
	sds.Equal("Test1", item.Name)

	item = dsTestActionItem{}
	found, err = ds.From("items").Select("name").Distinct().ScanStruct(&item)
	sds.NoError(err)
	sds.True(found)
	sds.Equal("111 Test Addr", item.Address)
	sds.Equal("Test1", item.Name)

	_, err = ds.From("items").ScanStruct(item)
	sds.EqualError(err, "goqu: type must be a pointer to a struct when scanning into a struct")
	_, err = ds.From("items").ScanStruct([]dsTestActionItem{})
	sds.EqualError(err, "goqu: type must be a pointer to a struct when scanning into a struct")
	_, err = ds.From("items").Select("test").ScanStruct(&item)
	sds.EqualError(err, `goqu: unable to find corresponding field to column "test" returned by query`)
}

func (sds *selectDatasetSuite) TestScanStruct_WithPreparedStatements() {
	mDb, sqlMock, err := sqlmock.New()
	sds.NoError(err)
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
	sds.NoError(err)
	sds.True(found)
	sds.Equal("111 Test Addr", item.Address)
	sds.Equal("Test1", item.Name)

	_, err = ds.From("items").ScanStruct(item)
	sds.EqualError(err, "goqu: type must be a pointer to a struct when scanning into a struct")
	_, err = ds.From("items").ScanStruct([]dsTestActionItem{})
	sds.EqualError(err, "goqu: type must be a pointer to a struct when scanning into a struct")
	_, err = ds.From("items").
		Prepared(true).
		Select("test").
		Where(Ex{"name": []string{"Bob", "Sally", "Billy"}, "address": "111 Test Addr"}).
		ScanStruct(&item)
	sds.EqualError(err, `goqu: unable to find corresponding field to column "test" returned by query`)
}

func (sds *selectDatasetSuite) TestScanVals() {
	mDb, sqlMock, err := sqlmock.New()
	sds.NoError(err)
	sqlMock.ExpectQuery(`SELECT "id" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("1\n2\n3\n4\n5"))

	qf := exec.NewQueryFactory(mDb)
	ds := newDataset("mock", qf)
	var ids []uint32
	sds.NoError(ds.From("items").Select("id").ScanVals(&ids))
	sds.Equal(ids, []uint32{1, 2, 3, 4, 5})

	sds.EqualError(ds.From("items").ScanVals([]uint32{}),
		"goqu: type must be a pointer to a slice when scanning into vals")
	sds.EqualError(ds.From("items").ScanVals(dsTestActionItem{}),
		"goqu: type must be a pointer to a slice when scanning into vals")
}

func (sds *selectDatasetSuite) TestScanVals_WithPreparedStatment() {
	mDb, sqlMock, err := sqlmock.New()
	sds.NoError(err)
	sqlMock.ExpectQuery(
		`SELECT "id" FROM "items" WHERE \(\("address" = \?\) AND \("name" IN \(\?, \?, \?\)\)\)`,
	).
		WithArgs("111 Test Addr", "Bob", "Sally", "Billy").
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("1\n2\n3\n4\n5"))

	qf := exec.NewQueryFactory(mDb)
	ds := newDataset("mock", qf)
	var ids []uint32
	sds.NoError(ds.From("items").
		Prepared(true).
		Select("id").
		Where(Ex{"name": []string{"Bob", "Sally", "Billy"}, "address": "111 Test Addr"}).
		ScanVals(&ids))
	sds.Equal([]uint32{1, 2, 3, 4, 5}, ids)

	sds.EqualError(ds.From("items").ScanVals([]uint32{}),
		"goqu: type must be a pointer to a slice when scanning into vals")
	sds.EqualError(ds.From("items").ScanVals(dsTestActionItem{}),
		"goqu: type must be a pointer to a slice when scanning into vals")
}

func (sds *selectDatasetSuite) TestScanVal() {
	mDb, sqlMock, err := sqlmock.New()
	sds.NoError(err)
	sqlMock.ExpectQuery(`SELECT "id" FROM "items" LIMIT 1`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"id"}).FromCSVString("10"))

	qf := exec.NewQueryFactory(mDb)
	ds := newDataset("mock", qf)
	var id int64
	found, err := ds.From("items").Select("id").ScanVal(&id)
	sds.NoError(err)
	sds.Equal(id, int64(10))
	sds.True(found)

	found, err = ds.From("items").ScanVal([]int64{})
	sds.False(found)
	sds.EqualError(err, "goqu: type must be a pointer when scanning into val")
	found, err = ds.From("items").ScanVal(10)
	sds.False(found)
	sds.EqualError(err, "goqu: type must be a pointer when scanning into val")
}

func (sds *selectDatasetSuite) TestScanVal_WithPreparedStatement() {
	mDb, sqlMock, err := sqlmock.New()
	sds.NoError(err)
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
	sds.NoError(err)
	sds.Equal(int64(10), id)
	sds.True(found)

	found, err = ds.From("items").ScanVal([]int64{})
	sds.False(found)
	sds.EqualError(err, "goqu: type must be a pointer when scanning into val")
	found, err = ds.From("items").ScanVal(10)
	sds.False(found)
	sds.EqualError(err, "goqu: type must be a pointer when scanning into val")
}

func (sds *selectDatasetSuite) TestCount() {
	mDb, sqlMock, err := sqlmock.New()
	sds.NoError(err)
	sqlMock.ExpectQuery(`SELECT COUNT\(\*\) AS "count" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"count"}).FromCSVString("10"))

	qf := exec.NewQueryFactory(mDb)
	ds := newDataset("mock", qf)
	count, err := ds.From("items").Count()
	sds.NoError(err)
	sds.Equal(count, int64(10))
}

func (sds *selectDatasetSuite) TestCount_WithPreparedStatement() {
	mDb, sqlMock, err := sqlmock.New()
	sds.NoError(err)
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
	sds.NoError(err)
	sds.Equal(int64(10), count)
}

func (sds *selectDatasetSuite) TestPluck() {
	mDb, sqlMock, err := sqlmock.New()
	sds.NoError(err)
	sqlMock.ExpectQuery(`SELECT "name" FROM "items"`).
		WithArgs().
		WillReturnRows(sqlmock.NewRows([]string{"name"}).FromCSVString("test1\ntest2\ntest3\ntest4\ntest5"))

	qf := exec.NewQueryFactory(mDb)
	ds := newDataset("mock", qf)
	var names []string
	sds.NoError(ds.From("items").Pluck(&names, "name"))
	sds.Equal([]string{"test1", "test2", "test3", "test4", "test5"}, names)
}

func (sds *selectDatasetSuite) TestPluck_WithPreparedStatement() {
	mDb, sqlMock, err := sqlmock.New()
	sds.NoError(err)
	sqlMock.ExpectQuery(
		`SELECT "name" FROM "items" WHERE \(\("address" = \?\) AND \("name" IN \(\?, \?, \?\)\)\)`,
	).
		WithArgs("111 Test Addr", "Bob", "Sally", "Billy").
		WillReturnRows(sqlmock.NewRows([]string{"name"}).FromCSVString("Bob\nSally\nBilly"))

	qf := exec.NewQueryFactory(mDb)
	ds := newDataset("mock", qf)
	var names []string
	sds.NoError(ds.From("items").
		Prepared(true).
		Where(Ex{"name": []string{"Bob", "Sally", "Billy"}, "address": "111 Test Addr"}).
		Pluck(&names, "name"))
	sds.Equal([]string{"Bob", "Sally", "Billy"}, names)
}

func (sds *selectDatasetSuite) TestWindowFunction() {
	for _, tt := range []struct {
		expectQuery string
		returnRows  *sqlmock.Rows
		fn          exp.Expression
		expectValue []int32
	}{
		{
			expectQuery: `SELECT ROW_NUMBER\(\) OVER \(PARTITION BY "class" ORDER BY "score"\) AS "r" FROM "test"`,
			returnRows:  sqlmock.NewRows([]string{"r"}).FromCSVString("1\n2\n1"),
			fn:          ROW_NUMBER().Over(W().PartitionBy("class").OrderBy("score")).As("r"),
			expectValue: []int32{1, 2, 1},
		},
		{
			expectQuery: `SELECT RANK\(\) OVER \(PARTITION BY "class" ORDER BY "score"\) AS "r" FROM "test"`,
			returnRows:  sqlmock.NewRows([]string{"r"}).FromCSVString("1\n2\n1"),
			fn:          RANK().Over(W().PartitionBy("class").OrderBy("score")).As("r"),
			expectValue: []int32{1, 2, 1},
		},
		{
			expectQuery: `SELECT DENSE_RANK\(\) OVER \(PARTITION BY "class" ORDER BY "score"\) AS "r" FROM "test"`,
			returnRows:  sqlmock.NewRows([]string{"r"}).FromCSVString("1\n2\n1"),
			fn:          DENSE_RANK().Over(W().PartitionBy("class").OrderBy("score")).As("r"),
			expectValue: []int32{1, 2, 1},
		},
		{
			expectQuery: `SELECT PERCENT_RANK\(\) OVER \(PARTITION BY "class" ORDER BY "score"\) AS "r" FROM "test"`,
			returnRows:  sqlmock.NewRows([]string{"r"}).FromCSVString("1\n2\n1"),
			fn:          PERCENT_RANK().Over(W().PartitionBy("class").OrderBy("score")).As("r"),
			expectValue: []int32{1, 2, 1},
		},
		{
			expectQuery: `SELECT CUME_DIST\(\) OVER \(PARTITION BY "class" ORDER BY "score"\) AS "r" FROM "test"`,
			returnRows:  sqlmock.NewRows([]string{"r"}).FromCSVString("1\n2\n1"),
			fn:          CUME_DIST().Over(W().PartitionBy("class").OrderBy("score")).As("r"),
			expectValue: []int32{1, 2, 1},
		},
		{
			expectQuery: `SELECT NTILE\(2\) OVER \(PARTITION BY "class" ORDER BY "score"\) AS "r" FROM "test"`,
			returnRows:  sqlmock.NewRows([]string{"r"}).FromCSVString("100\n100\n99"),
			fn:          NTILE(2).Over(W().PartitionBy("class").OrderBy("score")).As("r"),
			expectValue: []int32{100, 100, 99},
		},
		{
			expectQuery: `SELECT FIRST_VALUE\("score"\) OVER \(PARTITION BY "class" ORDER BY "score"\) AS "r" FROM "test"`,
			returnRows:  sqlmock.NewRows([]string{"r"}).FromCSVString("100\n100\n99"),
			fn:          FIRST_VALUE("score").Over(W().PartitionBy("class").OrderBy("score")).As("r"),
			expectValue: []int32{100, 100, 99},
		},
		{
			expectQuery: `SELECT LAST_VALUE\("score"\) OVER \(PARTITION BY "class" ORDER BY "score"\) AS "r" FROM "test"`,
			returnRows:  sqlmock.NewRows([]string{"r"}).FromCSVString("100\n100\n99"),
			fn:          LAST_VALUE("score").Over(W().PartitionBy("class").OrderBy("score")).As("r"),
			expectValue: []int32{100, 100, 99},
		},
		{
			expectQuery: `SELECT NTH_VALUE\("score", 3\) OVER \(PARTITION BY "class" ORDER BY "score"\) AS "r" FROM "test"`,
			returnRows:  sqlmock.NewRows([]string{"r"}).FromCSVString("100\n100\n99"),
			fn:          NTH_VALUE("score", 3).Over(W().PartitionBy("class").OrderBy("score")).As("r"),
			expectValue: []int32{100, 100, 99},
		},
	} {
		mDb, sqlMock, err := sqlmock.New()
		sds.NoError(err)
		qf := exec.NewQueryFactory(mDb)
		ds := newDataset("mock", qf)
		sqlMock.ExpectQuery(tt.expectQuery).
			WillReturnRows(tt.returnRows)
		var actualValue []int32
		sds.NoError(ds.Prepared(false).
			Select(tt.fn).
			From("test").
			ScanVals(&actualValue))
		sds.Equal(tt.expectValue, actualValue)
	}
}

func TestSelectDataset(t *testing.T) {
	suite.Run(t, new(selectDatasetSuite))
}
