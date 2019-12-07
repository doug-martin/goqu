package sqlgen

import (
	"testing"

	"github.com/doug-martin/goqu/v9/exp"
	"github.com/doug-martin/goqu/v9/internal/errors"
	"github.com/doug-martin/goqu/v9/internal/sb"
	"github.com/stretchr/testify/suite"
)

type (
	selectTestCase struct {
		clause     exp.SelectClauses
		sql        string
		isPrepared bool
		args       []interface{}
		err        string
	}
	selectSQLGeneratorSuite struct {
		baseSQLGeneratorSuite
	}
)

func (ssgs *selectSQLGeneratorSuite) assertCases(ssg SelectSQLGenerator, testCases ...selectTestCase) {
	for _, tc := range testCases {
		b := sb.NewSQLBuilder(tc.isPrepared)
		ssg.Generate(b, tc.clause)
		switch {
		case len(tc.err) > 0:
			ssgs.assertErrorSQL(b, tc.err)
		case tc.isPrepared:
			ssgs.assertPreparedSQL(b, tc.sql, tc.args)
		default:
			ssgs.assertNotPreparedSQL(b, tc.sql)
		}
	}
}

func (ssgs *selectSQLGeneratorSuite) TestDialect() {
	opts := DefaultDialectOptions()
	d := NewSelectSQLGenerator("test", opts)
	ssgs.Equal("test", d.Dialect())

	opts2 := DefaultDialectOptions()
	d2 := NewSelectSQLGenerator("test2", opts2)
	ssgs.Equal("test2", d2.Dialect())
}

func (ssgs *selectSQLGeneratorSuite) TestGenerate() {
	opts := DefaultDialectOptions()
	opts.SelectClause = []byte("select")
	opts.StarRune = '#'

	sc := exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test"))
	scWithCols := sc.SetSelect(exp.NewColumnListExpression("a", "b"))

	ssgs.assertCases(
		NewSelectSQLGenerator("test", opts),
		selectTestCase{clause: sc, sql: `select # FROM "test"`},
		selectTestCase{clause: sc, sql: `select # FROM "test"`, isPrepared: true},

		selectTestCase{clause: scWithCols, sql: `select "a", "b" FROM "test"`},
		selectTestCase{clause: scWithCols, sql: `select "a", "b" FROM "test"`, isPrepared: true},
	)
}

func (ssgs *selectSQLGeneratorSuite) TestGenerate_UnsupportedFragment() {
	opts := DefaultDialectOptions()
	opts.SelectSQLOrder = []SQLFragmentType{InsertBeingSQLFragment}

	sc := exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test"))
	expectedErr := "goqu: unsupported SELECT SQL fragment InsertBeingSQLFragment"
	ssgs.assertCases(
		NewSelectSQLGenerator("test", opts),
		selectTestCase{clause: sc, err: expectedErr},
		selectTestCase{clause: sc, err: expectedErr, isPrepared: true},
	)
}

func (ssgs *selectSQLGeneratorSuite) TestGenerate_WithErroredBuilder() {
	opts := DefaultDialectOptions()
	opts.SelectSQLOrder = []SQLFragmentType{InsertBeingSQLFragment}
	d := NewSelectSQLGenerator("test", opts)

	b := sb.NewSQLBuilder(true).SetError(errors.New("test error"))
	c := exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test"))
	d.Generate(b, c)
	ssgs.assertErrorSQL(b, `goqu: test error`)
}

func (ssgs *selectSQLGeneratorSuite) TestGenerate_withSelectedColumns() {
	opts := DefaultDialectOptions()
	// make sure the fragments are used
	opts.SelectClause = []byte("select")
	opts.StarRune = '#'
	opts.SupportsDistinctOn = true

	sc := exp.NewSelectClauses()
	scCols := sc.SetSelect(exp.NewColumnListExpression("a", "b"))
	scFuncs := sc.SetSelect(exp.NewColumnListExpression(
		exp.NewSQLFunctionExpression("COUNT", exp.Star()),
		exp.NewSQLFunctionExpression("RANK"),
	))

	we := exp.NewWindowExpression(
		nil,
		nil,
		exp.NewColumnListExpression("a", "b"),
		exp.NewOrderedColumnList(exp.ParseIdentifier("c").Asc()),
	)
	scFuncsPartition := sc.SetSelect(exp.NewColumnListExpression(
		exp.NewSQLFunctionExpression("COUNT", exp.Star()).Over(we),
		exp.NewSQLFunctionExpression("RANK").Over(we.Inherit("w")),
	))

	ssgs.assertCases(
		NewSelectSQLGenerator("test", opts),
		selectTestCase{clause: sc, sql: `select #`},
		selectTestCase{clause: sc, sql: `select #`, isPrepared: true},

		selectTestCase{clause: scCols, sql: `select "a", "b"`},
		selectTestCase{clause: scCols, sql: `select "a", "b"`, isPrepared: true},

		selectTestCase{clause: scFuncs, sql: `select COUNT(*), RANK()`},
		selectTestCase{clause: scFuncs, sql: `select COUNT(*), RANK()`, isPrepared: true},

		selectTestCase{
			clause: scFuncsPartition,
			sql:    `select COUNT(*) OVER (PARTITION BY "a", "b" ORDER BY "c" ASC), RANK() OVER ("w" PARTITION BY "a", "b" ORDER BY "c" ASC)`,
		},
		selectTestCase{
			clause:     scFuncsPartition,
			sql:        `select COUNT(*) OVER (PARTITION BY "a", "b" ORDER BY "c" ASC), RANK() OVER ("w" PARTITION BY "a", "b" ORDER BY "c" ASC)`,
			isPrepared: true,
		},
	)
}

func (ssgs *selectSQLGeneratorSuite) TestGenerate_withDistinct() {
	opts := DefaultDialectOptions()
	// make sure the fragments are used
	opts.SelectClause = []byte("select")
	opts.StarRune = '#'
	opts.DistinctFragment = []byte("distinct")
	opts.OnFragment = []byte(" on ")
	opts.SupportsDistinctOn = true

	sc := exp.NewSelectClauses().SetDistinct(exp.NewColumnListExpression())
	scDistinctOn := sc.SetDistinct(exp.NewColumnListExpression("a", "b"))

	ssgs.assertCases(
		NewSelectSQLGenerator("test", opts),
		selectTestCase{clause: sc, sql: `select distinct #`},
		selectTestCase{clause: sc, sql: `select distinct #`, isPrepared: true},

		selectTestCase{clause: scDistinctOn, sql: `select distinct on ("a", "b") #`},
		selectTestCase{clause: scDistinctOn, sql: `select distinct on ("a", "b") #`, isPrepared: true},
	)

	opts = DefaultDialectOptions()
	opts.SupportsDistinctOn = false
	expectedErr := "goqu: dialect does not support DISTINCT ON clause [dialect=test]"
	ssgs.assertCases(
		NewSelectSQLGenerator("test", opts),
		selectTestCase{clause: sc, sql: `SELECT DISTINCT *`},
		selectTestCase{clause: sc, sql: `SELECT DISTINCT *`, isPrepared: true},

		selectTestCase{clause: scDistinctOn, err: expectedErr},
		selectTestCase{clause: scDistinctOn, err: expectedErr, isPrepared: true},
	)
}

func (ssgs *selectSQLGeneratorSuite) TestGenerate_withFromSQL() {
	opts := DefaultDialectOptions()
	opts.FromFragment = []byte(" from")

	sc := exp.NewSelectClauses()
	scFrom := sc.SetFrom(exp.NewColumnListExpression("a", "b"))
	ssgs.assertCases(
		NewSelectSQLGenerator("test", opts),
		selectTestCase{clause: sc, sql: `SELECT *`},
		selectTestCase{clause: sc, sql: `SELECT *`, isPrepared: true},

		selectTestCase{clause: scFrom, sql: `SELECT * from "a", "b"`},
		selectTestCase{clause: scFrom, sql: `SELECT * from "a", "b"`, isPrepared: true},
	)
}

func (ssgs *selectSQLGeneratorSuite) TestGenerate_withJoin() {
	opts := DefaultDialectOptions()
	// override fragements to make sure dialect is used
	opts.UsingFragment = []byte(" using ")
	opts.OnFragment = []byte(" on ")
	opts.JoinTypeLookup = map[exp.JoinType][]byte{
		exp.LeftJoinType:    []byte(" left join "),
		exp.NaturalJoinType: []byte(" natural join "),
	}

	sc := exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test"))
	ti := exp.NewIdentifierExpression("", "test2", "")
	uj := exp.NewUnConditionedJoinExpression(exp.NaturalJoinType, ti)
	cjo := exp.NewConditionedJoinExpression(exp.LeftJoinType, ti, exp.NewJoinOnCondition(exp.Ex{"a": "foo"}))
	cju := exp.NewConditionedJoinExpression(exp.LeftJoinType, ti, exp.NewJoinUsingCondition("a"))
	rj := exp.NewConditionedJoinExpression(exp.RightJoinType, ti, exp.NewJoinUsingCondition(exp.NewIdentifierExpression("", "", "a")))
	badJoin := exp.NewConditionedJoinExpression(exp.LeftJoinType, ti, exp.NewJoinUsingCondition())

	expectedRjError := "goqu: dialect does not support RightJoinType"
	expectedJoinCondError := "goqu: join condition required for conditioned join LeftJoinType"
	ssgs.assertCases(
		NewSelectSQLGenerator("test", opts),
		selectTestCase{clause: sc.JoinsAppend(uj), sql: `SELECT * FROM "test" natural join "test2"`},
		selectTestCase{clause: sc.JoinsAppend(uj), sql: `SELECT * FROM "test" natural join "test2"`, isPrepared: true},

		selectTestCase{clause: sc.JoinsAppend(cjo), sql: `SELECT * FROM "test" left join "test2" on ("a" = 'foo')`},
		selectTestCase{
			clause:     sc.JoinsAppend(cjo),
			sql:        `SELECT * FROM "test" left join "test2" on ("a" = ?)`,
			isPrepared: true,
			args:       []interface{}{"foo"},
		},

		selectTestCase{clause: sc.JoinsAppend(cju), sql: `SELECT * FROM "test" left join "test2" using ("a")`},
		selectTestCase{clause: sc.JoinsAppend(cju), sql: `SELECT * FROM "test" left join "test2" using ("a")`, isPrepared: true},

		selectTestCase{
			clause: sc.JoinsAppend(uj).JoinsAppend(cjo).JoinsAppend(cju),
			sql:    `SELECT * FROM "test" natural join "test2" left join "test2" on ("a" = 'foo') left join "test2" using ("a")`,
		},
		selectTestCase{
			clause:     sc.JoinsAppend(uj).JoinsAppend(cjo).JoinsAppend(cju),
			sql:        `SELECT * FROM "test" natural join "test2" left join "test2" on ("a" = ?) left join "test2" using ("a")`,
			isPrepared: true,
			args:       []interface{}{"foo"},
		},

		selectTestCase{clause: sc.JoinsAppend(rj), err: expectedRjError},
		selectTestCase{clause: sc.JoinsAppend(rj), err: expectedRjError, isPrepared: true},

		selectTestCase{clause: sc.JoinsAppend(badJoin), err: expectedJoinCondError},
		selectTestCase{clause: sc.JoinsAppend(badJoin), err: expectedJoinCondError, isPrepared: true},
	)

}

func (ssgs *selectSQLGeneratorSuite) TestGenerate_withWhere() {
	opts := DefaultDialectOptions()
	opts.WhereFragment = []byte(" where ")

	sc := exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test"))
	w := exp.Ex{"a": "b"}
	w2 := exp.Ex{"b": "c"}
	scWhere1 := sc.WhereAppend(w)
	scWhere2 := sc.WhereAppend(w, w2)

	ssgs.assertCases(
		NewSelectSQLGenerator("test", opts),
		selectTestCase{clause: scWhere1, sql: `SELECT * FROM "test" where ("a" = 'b')`},
		selectTestCase{clause: scWhere1, sql: `SELECT * FROM "test" where ("a" = ?)`, isPrepared: true, args: []interface{}{"b"}},

		selectTestCase{clause: scWhere2, sql: `SELECT * FROM "test" where (("a" = 'b') AND ("b" = 'c'))`},
		selectTestCase{
			clause:     scWhere2,
			sql:        `SELECT * FROM "test" where (("a" = ?) AND ("b" = ?))`,
			isPrepared: true,
			args:       []interface{}{"b", "c"},
		},
	)
}

func (ssgs *selectSQLGeneratorSuite) TestGenerate_withGroupBy() {
	opts := DefaultDialectOptions()
	opts.GroupByFragment = []byte(" group by ")

	sc := exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test"))
	scGroup := sc.SetGroupBy(exp.NewColumnListExpression("a"))
	scGroupMulti := sc.SetGroupBy(exp.NewColumnListExpression("a", "b"))

	ssgs.assertCases(
		NewSelectSQLGenerator("test", opts),
		selectTestCase{clause: scGroup, sql: `SELECT * FROM "test" group by "a"`},
		selectTestCase{clause: scGroup, sql: `SELECT * FROM "test" group by "a"`, isPrepared: true},

		selectTestCase{clause: scGroupMulti, sql: `SELECT * FROM "test" group by "a", "b"`},
		selectTestCase{clause: scGroupMulti, sql: `SELECT * FROM "test" group by "a", "b"`, isPrepared: true},
	)
}

func (ssgs *selectSQLGeneratorSuite) TestGenerate_withHaving() {
	opts := DefaultDialectOptions()
	opts.HavingFragment = []byte(" having ")

	sc := exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test"))
	w := exp.Ex{"a": "b"}
	w2 := exp.Ex{"b": "c"}
	scHaving1 := sc.HavingAppend(w)
	scHaving2 := sc.HavingAppend(w, w2)

	ssgs.assertCases(
		NewSelectSQLGenerator("test", opts),
		selectTestCase{clause: scHaving1, sql: `SELECT * FROM "test" having ("a" = 'b')`},
		selectTestCase{clause: scHaving1, sql: `SELECT * FROM "test" having ("a" = ?)`, isPrepared: true, args: []interface{}{"b"}},

		selectTestCase{clause: scHaving2, sql: `SELECT * FROM "test" having (("a" = 'b') AND ("b" = 'c'))`},
		selectTestCase{
			clause:     scHaving2,
			sql:        `SELECT * FROM "test" having (("a" = ?) AND ("b" = ?))`,
			isPrepared: true,
			args:       []interface{}{"b", "c"},
		},
	)
}

func (ssgs *selectSQLGeneratorSuite) TestGenerate_withWindow() {
	opts := DefaultDialectOptions()
	opts.WindowFragment = []byte(" window ")
	opts.WindowPartitionByFragment = []byte("partition by ")
	opts.WindowOrderByFragment = []byte("order by ")

	sc := exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test"))
	we1 := exp.NewWindowExpression(
		exp.NewIdentifierExpression("", "", "w"),
		nil,
		nil,
		nil,
	)
	wePartitionBy := we1.PartitionBy("a", "b")
	weOrderBy := we1.OrderBy("a", "b")

	weOrderAndPartitionBy := we1.PartitionBy("a", "b").OrderBy("a", "b")

	weInherits := exp.NewWindowExpression(
		exp.NewIdentifierExpression("", "", "w2"),
		exp.NewIdentifierExpression("", "", "w"),
		nil,
		nil,
	)
	weInheritsPartitionBy := weInherits.PartitionBy("c", "d")
	weInheritsOrderBy := weInherits.OrderBy("c", "d")

	weInheritsOrderAndPartitionBy := weInherits.PartitionBy("c", "d").OrderBy("c", "d")

	scNoName := sc.WindowsAppend(exp.NewWindowExpression(nil, nil, nil, nil))

	scWindow1 := sc.WindowsAppend(we1)
	scWindow2 := sc.WindowsAppend(wePartitionBy)
	scWindow3 := sc.WindowsAppend(weOrderBy)
	scWindow4 := sc.WindowsAppend(weOrderAndPartitionBy)

	scWindow5 := sc.WindowsAppend(we1, weInherits)
	scWindow6 := sc.WindowsAppend(we1, weInheritsPartitionBy)
	scWindow7 := sc.WindowsAppend(we1, weInheritsOrderBy)
	scWindow8 := sc.WindowsAppend(we1, weInheritsOrderAndPartitionBy)

	ssgs.assertCases(
		NewSelectSQLGenerator("test", opts),

		selectTestCase{clause: scNoName, err: errNoWindowName.Error()},
		selectTestCase{clause: scNoName, err: errNoWindowName.Error(), isPrepared: true},

		selectTestCase{clause: scWindow1, sql: `SELECT * FROM "test" window "w" AS ()`},
		selectTestCase{clause: scWindow1, sql: `SELECT * FROM "test" window "w" AS ()`, isPrepared: true},

		selectTestCase{clause: scWindow2, sql: `SELECT * FROM "test" window "w" AS (partition by "a", "b")`},
		selectTestCase{
			clause:     scWindow2,
			sql:        `SELECT * FROM "test" window "w" AS (partition by "a", "b")`,
			isPrepared: true,
		},

		selectTestCase{clause: scWindow3, sql: `SELECT * FROM "test" window "w" AS (order by "a", "b")`},
		selectTestCase{
			clause:     scWindow3,
			sql:        `SELECT * FROM "test" window "w" AS (order by "a", "b")`,
			isPrepared: true,
		},

		selectTestCase{
			clause: scWindow4,
			sql:    `SELECT * FROM "test" window "w" AS (partition by "a", "b" order by "a", "b")`,
		},
		selectTestCase{
			clause:     scWindow4,
			sql:        `SELECT * FROM "test" window "w" AS (partition by "a", "b" order by "a", "b")`,
			isPrepared: true,
		},

		selectTestCase{
			clause: scWindow5,
			sql:    `SELECT * FROM "test" window "w" AS (), "w2" AS ("w")`,
		},
		selectTestCase{
			clause:     scWindow5,
			sql:        `SELECT * FROM "test" window "w" AS (), "w2" AS ("w")`,
			isPrepared: true,
		},

		selectTestCase{
			clause: scWindow6,
			sql:    `SELECT * FROM "test" window "w" AS (), "w2" AS ("w" partition by "c", "d")`,
		},
		selectTestCase{
			clause:     scWindow6,
			sql:        `SELECT * FROM "test" window "w" AS (), "w2" AS ("w" partition by "c", "d")`,
			isPrepared: true,
		},

		selectTestCase{
			clause: scWindow7,
			sql:    `SELECT * FROM "test" window "w" AS (), "w2" AS ("w" order by "c", "d")`,
		},
		selectTestCase{
			clause:     scWindow7,
			sql:        `SELECT * FROM "test" window "w" AS (), "w2" AS ("w" order by "c", "d")`,
			isPrepared: true,
		},

		selectTestCase{
			clause: scWindow8,
			sql:    `SELECT * FROM "test" window "w" AS (), "w2" AS ("w" partition by "c", "d" order by "c", "d")`,
		},
		selectTestCase{
			clause:     scWindow8,
			sql:        `SELECT * FROM "test" window "w" AS (), "w2" AS ("w" partition by "c", "d" order by "c", "d")`,
			isPrepared: true,
		},
	)

	opts = DefaultDialectOptions()
	opts.SupportsWindowFunction = false
	ssgs.assertCases(
		NewSelectSQLGenerator("test", opts),

		selectTestCase{clause: scWindow1, err: errWindowNotSupported("test").Error()},
		selectTestCase{clause: scWindow1, err: errWindowNotSupported("test").Error(), isPrepared: true},
	)
}

func (ssgs *selectSQLGeneratorSuite) TestGenerate_withOrder() {
	sc := exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")).
		SetOrder(
			exp.NewIdentifierExpression("", "", "a").Asc(),
			exp.NewIdentifierExpression("", "", "b").Desc(),
		)
	ssgs.assertCases(
		NewSelectSQLGenerator("test", DefaultDialectOptions()),
		selectTestCase{clause: sc, sql: `SELECT * FROM "test" ORDER BY "a" ASC, "b" DESC`},
		selectTestCase{clause: sc, sql: `SELECT * FROM "test" ORDER BY "a" ASC, "b" DESC`, isPrepared: true},
	)
}

func (ssgs *selectSQLGeneratorSuite) TestGenerate_withLimit() {
	sc := exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")).
		SetLimit(10)
	ssgs.assertCases(
		NewSelectSQLGenerator("test", DefaultDialectOptions()),
		selectTestCase{clause: sc, sql: `SELECT * FROM "test" LIMIT 10`},
		selectTestCase{clause: sc, sql: `SELECT * FROM "test" LIMIT ?`, isPrepared: true, args: []interface{}{int64(10)}},
	)
}

func (ssgs *selectSQLGeneratorSuite) TestGenerate_withOffset() {
	sc := exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")).
		SetOffset(10)
	ssgs.assertCases(
		NewSelectSQLGenerator("test", DefaultDialectOptions()),
		selectTestCase{clause: sc, sql: `SELECT * FROM "test" OFFSET 10`},
		selectTestCase{clause: sc, sql: `SELECT * FROM "test" OFFSET ?`, isPrepared: true, args: []interface{}{int64(10)}},
	)
}

func (ssgs *selectSQLGeneratorSuite) TestGenerate_withCommonTables() {

	tse := newTestAppendableExpression("select * from foo", emptyArgs, nil, nil)

	sc := exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test_cte"))
	scCte1 := sc.CommonTablesAppend(exp.NewCommonTableExpression(false, "test_cte", tse))
	scCte2 := sc.CommonTablesAppend(exp.NewCommonTableExpression(true, "test_cte", tse))

	ssgs.assertCases(
		NewSelectSQLGenerator("test", DefaultDialectOptions()),
		selectTestCase{clause: scCte1, sql: `WITH test_cte AS (select * from foo) SELECT * FROM "test_cte"`},
		selectTestCase{clause: scCte1, sql: `WITH test_cte AS (select * from foo) SELECT * FROM "test_cte"`, isPrepared: true},

		selectTestCase{clause: scCte2, sql: `WITH RECURSIVE test_cte AS (select * from foo) SELECT * FROM "test_cte"`},
		selectTestCase{clause: scCte2, sql: `WITH RECURSIVE test_cte AS (select * from foo) SELECT * FROM "test_cte"`, isPrepared: true},
	)

}

func (ssgs *selectSQLGeneratorSuite) TestGenerate_withCompounds() {
	tse := newTestAppendableExpression("select * from foo", emptyArgs, nil, nil)
	sc := exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test")).
		CompoundsAppend(exp.NewCompoundExpression(exp.UnionCompoundType, tse)).
		CompoundsAppend(exp.NewCompoundExpression(exp.IntersectCompoundType, tse))

	expectedSQL := `SELECT * FROM "test" UNION (select * from foo) INTERSECT (select * from foo)`
	ssgs.assertCases(
		NewSelectSQLGenerator("test", DefaultDialectOptions()),
		selectTestCase{clause: sc, sql: expectedSQL},
		selectTestCase{clause: sc, sql: expectedSQL, isPrepared: true},
	)
}

func (ssgs *selectSQLGeneratorSuite) TestToSelectSQL_withFor() {
	opts := DefaultDialectOptions()
	opts.ForUpdateFragment = []byte(" for update ")
	opts.ForNoKeyUpdateFragment = []byte(" for no key update ")
	opts.ForShareFragment = []byte(" for share ")
	opts.ForKeyShareFragment = []byte(" for key share ")
	opts.NowaitFragment = []byte("nowait")
	opts.SkipLockedFragment = []byte("skip locked")

	sc := exp.NewSelectClauses().SetFrom(exp.NewColumnListExpression("test"))
	scFnW := sc.SetLock(exp.NewLock(exp.ForNolock, exp.Wait))
	scFnNw := sc.SetLock(exp.NewLock(exp.ForNolock, exp.NoWait))
	scFnSl := sc.SetLock(exp.NewLock(exp.ForNolock, exp.SkipLocked))

	scFsW := sc.SetLock(exp.NewLock(exp.ForShare, exp.Wait))
	scFsNw := sc.SetLock(exp.NewLock(exp.ForShare, exp.NoWait))
	scFsSl := sc.SetLock(exp.NewLock(exp.ForShare, exp.SkipLocked))

	scFksW := sc.SetLock(exp.NewLock(exp.ForKeyShare, exp.Wait))
	scFksNw := sc.SetLock(exp.NewLock(exp.ForKeyShare, exp.NoWait))
	scFksSl := sc.SetLock(exp.NewLock(exp.ForKeyShare, exp.SkipLocked))

	scFuW := sc.SetLock(exp.NewLock(exp.ForUpdate, exp.Wait))
	scFuNw := sc.SetLock(exp.NewLock(exp.ForUpdate, exp.NoWait))
	scFuSl := sc.SetLock(exp.NewLock(exp.ForUpdate, exp.SkipLocked))

	scFkuW := sc.SetLock(exp.NewLock(exp.ForNoKeyUpdate, exp.Wait))
	scFkuNw := sc.SetLock(exp.NewLock(exp.ForNoKeyUpdate, exp.NoWait))
	scFkuSl := sc.SetLock(exp.NewLock(exp.ForNoKeyUpdate, exp.SkipLocked))
	ssgs.assertCases(
		NewSelectSQLGenerator("test", opts),
		selectTestCase{clause: scFnW, sql: `SELECT * FROM "test"`},
		selectTestCase{clause: scFnW, sql: `SELECT * FROM "test"`, isPrepared: true},

		selectTestCase{clause: scFnNw, sql: `SELECT * FROM "test"`},
		selectTestCase{clause: scFnNw, sql: `SELECT * FROM "test"`, isPrepared: true},

		selectTestCase{clause: scFnSl, sql: `SELECT * FROM "test"`},
		selectTestCase{clause: scFnSl, sql: `SELECT * FROM "test"`, isPrepared: true},

		selectTestCase{clause: scFsW, sql: `SELECT * FROM "test" for share `},
		selectTestCase{clause: scFsW, sql: `SELECT * FROM "test" for share `, isPrepared: true},

		selectTestCase{clause: scFsNw, sql: `SELECT * FROM "test" for share nowait`},
		selectTestCase{clause: scFsNw, sql: `SELECT * FROM "test" for share nowait`, isPrepared: true},

		selectTestCase{clause: scFsSl, sql: `SELECT * FROM "test" for share skip locked`},
		selectTestCase{clause: scFsSl, sql: `SELECT * FROM "test" for share skip locked`, isPrepared: true},

		selectTestCase{clause: scFksW, sql: `SELECT * FROM "test" for key share `},
		selectTestCase{clause: scFksW, sql: `SELECT * FROM "test" for key share `, isPrepared: true},

		selectTestCase{clause: scFksNw, sql: `SELECT * FROM "test" for key share nowait`},
		selectTestCase{clause: scFksNw, sql: `SELECT * FROM "test" for key share nowait`, isPrepared: true},

		selectTestCase{clause: scFksSl, sql: `SELECT * FROM "test" for key share skip locked`},
		selectTestCase{clause: scFksSl, sql: `SELECT * FROM "test" for key share skip locked`, isPrepared: true},

		selectTestCase{clause: scFuW, sql: `SELECT * FROM "test" for update `},
		selectTestCase{clause: scFuW, sql: `SELECT * FROM "test" for update `, isPrepared: true},

		selectTestCase{clause: scFuNw, sql: `SELECT * FROM "test" for update nowait`},
		selectTestCase{clause: scFuNw, sql: `SELECT * FROM "test" for update nowait`, isPrepared: true},

		selectTestCase{clause: scFuSl, sql: `SELECT * FROM "test" for update skip locked`},
		selectTestCase{clause: scFuSl, sql: `SELECT * FROM "test" for update skip locked`, isPrepared: true},

		selectTestCase{clause: scFkuW, sql: `SELECT * FROM "test" for no key update `},
		selectTestCase{clause: scFkuW, sql: `SELECT * FROM "test" for no key update `, isPrepared: true},

		selectTestCase{clause: scFkuNw, sql: `SELECT * FROM "test" for no key update nowait`},
		selectTestCase{clause: scFkuNw, sql: `SELECT * FROM "test" for no key update nowait`, isPrepared: true},

		selectTestCase{clause: scFkuSl, sql: `SELECT * FROM "test" for no key update skip locked`},
		selectTestCase{clause: scFkuSl, sql: `SELECT * FROM "test" for no key update skip locked`, isPrepared: true},
	)
}

func TestSelectSQLGenerator(t *testing.T) {
	suite.Run(t, new(selectSQLGeneratorSuite))
}
