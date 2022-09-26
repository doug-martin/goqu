package sqlite3_test

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/dialect/mysql"
	"github.com/doug-martin/goqu/v9/dialect/sqlite3"

	"github.com/stretchr/testify/suite"
)

const (
	dropTable   = "DROP TABLE IF EXISTS `entry`;"
	createTable = "CREATE  TABLE `entry` (" +
		"`id` INTEGER PRIMARY KEY," +
		"`int` INT NOT NULL ," +
		"`float` FLOAT NOT NULL ," +
		"`string` VARCHAR(255) NOT NULL ," +
		"`time` DATETIME NOT NULL ," +
		"`bool` TINYINT NOT NULL ," +
		"`bytes` BLOB NOT NULL" +
		");"
	insertDefaultRecords = "INSERT INTO `entry` (`int`, `float`, `string`, `time`, `bool`, `bytes`) VALUES" +
		"(0, 0.000000, '0.000000', '2015-02-22T18:19:55.000000000-00:00', 1,  '0.000000')," +
		"(1, 0.100000, '0.100000', '2015-02-22T19:19:55.000000000-00:00', 0, '0.100000')," +
		"(2, 0.200000, '0.200000', '2015-02-22T20:19:55.000000000-00:00', 1,  '0.200000')," +
		"(3, 0.300000, '0.300000', '2015-02-22T21:19:55.000000000-00:00', 0, '0.300000')," +
		"(4, 0.400000, '0.400000', '2015-02-22T22:19:55.000000000-00:00', 1,  '0.400000')," +
		"(5, 0.500000, '0.500000', '2015-02-22T23:19:55.000000000-00:00', 0, '0.500000')," +
		"(6, 0.600000, '0.600000', '2015-02-23T00:19:55.000000000-00:00', 1,  '0.600000')," +
		"(7, 0.700000, '0.700000', '2015-02-23T01:19:55.000000000-00:00', 0, '0.700000')," +
		"(8, 0.800000, '0.800000', '2015-02-23T02:19:55.000000000-00:00', 1,  '0.800000')," +
		"(9, 0.900000, '0.900000', '2015-02-23T03:19:55.000000000-00:00', 0, '0.900000');"
)

var dbURI = ":memory:"

type (
	sqlite3Suite struct {
		suite.Suite
		db *goqu.Database
	}
	entry struct {
		ID     uint32    `db:"id" goqu:"skipinsert,skipupdate"`
		Int    int       `db:"int"`
		Float  float64   `db:"float"`
		String string    `db:"string"`
		Time   time.Time `db:"time"`
		Bool   bool      `db:"bool"`
		Bytes  []byte    `db:"bytes"`
	}
	entryTestCase struct {
		ds    *goqu.SelectDataset
		len   int
		check func(entry entry, index int)
		err   string
	}
)

func (st *sqlite3Suite) SetupSuite() {
	db, err := sql.Open("sqlite3", dbURI)
	if err != nil {
		panic(err.Error())
	}
	st.db = goqu.New("sqlite3", db)
}

func (st *sqlite3Suite) assertSQL(cases ...sqlTestCase) {
	for i, c := range cases {
		actualSQL, actualArgs, err := c.ds.ToSQL()
		if c.err == "" {
			st.NoError(err, "test case %d failed", i)
		} else {
			st.EqualError(err, c.err, "test case %d failed", i)
		}
		st.Equal(c.sql, actualSQL, "test case %d failed", i)
		if c.isPrepared && c.args != nil || len(c.args) > 0 {
			st.Equal(c.args, actualArgs, "test case %d failed", i)
		} else {
			st.Empty(actualArgs, "test case %d failed", i)
		}
	}
}

func (st *sqlite3Suite) assertEntries(cases ...entryTestCase) {
	for i, c := range cases {
		var entries []entry
		err := c.ds.ScanStructs(&entries)
		if c.err == "" {
			st.NoError(err, "test case %d failed", i)
		} else {
			st.EqualError(err, c.err, "test case %d failed", i)
		}
		st.Len(entries, c.len)
		for index, entry := range entries {
			c.check(entry, index)
		}
	}
}

func (st *sqlite3Suite) SetupTest() {
	if _, err := st.db.Exec(dropTable); err != nil {
		panic(err)
	}
	if _, err := st.db.Exec(createTable); err != nil {
		panic(err)
	}
	if _, err := st.db.Exec(insertDefaultRecords); err != nil {
		panic(err)
	}
}

func (st *sqlite3Suite) TestSelectSQL() {
	ds := st.db.From("entry")
	st.assertSQL(
		sqlTestCase{ds: ds.Select("id", "float", "string", "time", "bool"), sql: "SELECT `id`, `float`, `string`, `time`, `bool` FROM `entry`"},
		sqlTestCase{ds: ds.Where(goqu.C("int").Eq(10)), sql: "SELECT * FROM `entry` WHERE (`int` = 10)"},
		sqlTestCase{
			ds:   ds.Prepared(true).Where(goqu.L("? = ?", goqu.C("int"), 10)),
			sql:  "SELECT * FROM `entry` WHERE `int` = ?",
			args: []interface{}{int64(10)},
		},
	)
}

func (st *sqlite3Suite) TestCompoundQueries() {
	ds1 := st.db.From("entry").Select("int").Where(goqu.C("int").Gt(0))
	ds2 := st.db.From("entry").Select("int").Where(goqu.C("int").Gt(5))

	var ids []int64
	err := ds1.Union(ds2).ScanVals(&ids)
	st.NoError(err)
	st.Equal([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9}, ids)

	ids = ids[0:0]
	err = ds1.UnionAll(ds2).ScanVals(&ids)
	st.NoError(err)
	st.Equal([]int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 6, 7, 8, 9}, ids)

	ids = ids[0:0]
	err = ds1.Intersect(ds2).ScanVals(&ids)
	st.NoError(err)
	st.Equal([]int64{6, 7, 8, 9}, ids)
}

func (st *sqlite3Suite) TestQuery() {
	ds := st.db.From("entry")
	floatVal := float64(0)
	baseDate, err := time.Parse(sqlite3.DialectOptions().TimeFormat, "2015-02-22T18:19:55.000000000-00:00")
	st.NoError(err)
	st.assertEntries(
		entryTestCase{ds: ds.Order(goqu.C("id").Asc()), len: 10, check: func(entry entry, index int) {
			f := fmt.Sprintf("%f", floatVal)
			st.Equal(uint32(index+1), entry.ID)
			st.Equal(index, entry.Int)
			st.Equal(f, fmt.Sprintf("%f", entry.Float))
			st.Equal(f, entry.String)
			st.Equal([]byte(f), entry.Bytes)
			st.Equal(index%2 == 0, entry.Bool)
			st.Equal(baseDate.Add(time.Duration(index)*time.Hour).Unix(), entry.Time.Unix())
			floatVal += float64(0.1)
		}},
		entryTestCase{ds: ds.Where(goqu.C("bool").IsTrue()).Order(goqu.C("id").Asc()), len: 5, check: func(entry entry, _ int) {
			st.True(entry.Bool)
		}},
		entryTestCase{ds: ds.Where(goqu.C("int").Gt(4)).Order(goqu.C("id").Asc()), len: 5, check: func(entry entry, _ int) {
			st.True(entry.Int > 4)
		}},
		entryTestCase{ds: ds.Where(goqu.C("int").Gte(5)).Order(goqu.C("id").Asc()), len: 5, check: func(entry entry, _ int) {
			st.True(entry.Int >= 5)
		}},
		entryTestCase{ds: ds.Where(goqu.C("int").Lt(5)).Order(goqu.C("id").Asc()), len: 5, check: func(entry entry, _ int) {
			st.True(entry.Int < 5)
		}},
		entryTestCase{ds: ds.Where(goqu.C("int").Lte(4)).Order(goqu.C("id").Asc()), len: 5, check: func(entry entry, _ int) {
			st.True(entry.Int <= 4)
		}},
		entryTestCase{ds: ds.Where(goqu.C("int").Between(goqu.Range(3, 6))).Order(goqu.C("id").Asc()), len: 4, check: func(entry entry, _ int) {
			st.True(entry.Int >= 3)
			st.True(entry.Int <= 6)
		}},
		entryTestCase{ds: ds.Where(goqu.C("string").Eq("0.100000")).Order(goqu.C("id").Asc()), len: 1, check: func(entry entry, _ int) {
			st.Equal(entry.String, "0.100000")
		}},
		entryTestCase{ds: ds.Where(goqu.C("string").Like("0.1%")).Order(goqu.C("id").Asc()), len: 1, check: func(entry entry, _ int) {
			st.Equal(entry.String, "0.100000")
		}},
		entryTestCase{ds: ds.Where(goqu.C("string").NotLike("0.1%")).Order(goqu.C("id").Asc()), len: 9, check: func(entry entry, _ int) {
			st.NotEqual(entry.String, "0.100000")
		}},
		entryTestCase{ds: ds.Where(goqu.C("string").IsNull()).Order(goqu.C("id").Asc()), len: 0, check: func(entry entry, _ int) {
			st.Fail("Should not have returned any records")
		}},
	)
}

func (st *sqlite3Suite) TestQuery_Prepared() {
	ds := st.db.From("entry").Prepared(true)
	floatVal := float64(0)
	baseDate, err := time.Parse(sqlite3.DialectOptions().TimeFormat, "2015-02-22T18:19:55.000000000-00:00")
	st.NoError(err)
	st.assertEntries(
		entryTestCase{ds: ds.Order(goqu.C("id").Asc()), len: 10, check: func(entry entry, index int) {
			f := fmt.Sprintf("%f", floatVal)
			st.Equal(uint32(index+1), entry.ID)
			st.Equal(index, entry.Int)
			st.Equal(f, fmt.Sprintf("%f", entry.Float))
			st.Equal(f, entry.String)
			st.Equal([]byte(f), entry.Bytes)
			st.Equal(index%2 == 0, entry.Bool)
			st.Equal(baseDate.Add(time.Duration(index)*time.Hour).Unix(), entry.Time.Unix())
			floatVal += float64(0.1)
		}},
		entryTestCase{ds: ds.Where(goqu.C("bool").IsTrue()).Order(goqu.C("id").Asc()), len: 5, check: func(entry entry, _ int) {
			st.True(entry.Bool)
		}},
		entryTestCase{ds: ds.Where(goqu.C("int").Gt(4)).Order(goqu.C("id").Asc()), len: 5, check: func(entry entry, _ int) {
			st.True(entry.Int > 4)
		}},
		entryTestCase{ds: ds.Where(goqu.C("int").Gte(5)).Order(goqu.C("id").Asc()), len: 5, check: func(entry entry, _ int) {
			st.True(entry.Int >= 5)
		}},
		entryTestCase{ds: ds.Where(goqu.C("int").Lt(5)).Order(goqu.C("id").Asc()), len: 5, check: func(entry entry, _ int) {
			st.True(entry.Int < 5)
		}},
		entryTestCase{ds: ds.Where(goqu.C("int").Lte(4)).Order(goqu.C("id").Asc()), len: 5, check: func(entry entry, _ int) {
			st.True(entry.Int <= 4)
		}},
		entryTestCase{ds: ds.Where(goqu.C("int").Between(goqu.Range(3, 6))).Order(goqu.C("id").Asc()), len: 4, check: func(entry entry, _ int) {
			st.True(entry.Int >= 3)
			st.True(entry.Int <= 6)
		}},
		entryTestCase{ds: ds.Where(goqu.C("string").Eq("0.100000")).Order(goqu.C("id").Asc()), len: 1, check: func(entry entry, _ int) {
			st.Equal(entry.String, "0.100000")
		}},
		entryTestCase{ds: ds.Where(goqu.C("string").Like("0.1%")).Order(goqu.C("id").Asc()), len: 1, check: func(entry entry, _ int) {
			st.Equal(entry.String, "0.100000")
		}},
		entryTestCase{ds: ds.Where(goqu.C("string").NotLike("0.1%")).Order(goqu.C("id").Asc()), len: 9, check: func(entry entry, _ int) {
			st.NotEqual(entry.String, "0.100000")
		}},
		entryTestCase{ds: ds.Where(goqu.C("string").IsNull()).Order(goqu.C("id").Asc()), len: 0, check: func(entry entry, _ int) {
			st.Fail("Should not have returned any records")
		}},
	)
}

func (st *sqlite3Suite) TestQuery_ValueExpressions() {
	type wrappedEntry struct {
		entry
		BoolValue bool `db:"bool_value"`
	}
	expectedDate, err := time.Parse("2006-01-02T15:04:05.000000000-00:00", "2015-02-22T19:19:55.000000000-00:00")
	st.NoError(err)
	ds := st.db.From("entry").Select(goqu.Star(), goqu.V(true).As("bool_value")).Where(goqu.Ex{"int": 1})
	var we wrappedEntry
	found, err := ds.ScanStruct(&we)
	st.NoError(err)
	st.True(found)
	st.Equal(we, wrappedEntry{
		entry{2, 1, 0.100000, "0.100000", expectedDate, false, []byte("0.100000")},
		true,
	})
}

func (st *sqlite3Suite) TestCount() {
	ds := st.db.From("entry")
	count, err := ds.Count()
	st.NoError(err)
	st.Equal(int64(10), count)
	count, err = ds.Where(goqu.C("int").Gt(4)).Count()
	st.NoError(err)
	st.Equal(int64(5), count)
	count, err = ds.Where(goqu.C("int").Gte(4)).Count()
	st.NoError(err)
	st.Equal(int64(6), count)
	count, err = ds.Where(goqu.C("string").Like("0.1%")).Count()
	st.NoError(err)
	st.Equal(int64(1), count)
	count, err = ds.Where(goqu.C("string").IsNull()).Count()
	st.NoError(err)
	st.Equal(int64(0), count)
}

func (st *sqlite3Suite) TestInsert() {
	ds := st.db.From("entry")
	now := time.Now()
	e := entry{Int: 10, Float: 1.000000, String: "1.000000", Time: now, Bool: true, Bytes: []byte("1.000000")}
	_, err := ds.Insert().Rows(e).Executor().Exec()
	st.NoError(err)

	var insertedEntry entry
	found, err := ds.Where(goqu.C("int").Eq(10)).ScanStruct(&insertedEntry)
	st.NoError(err)
	st.True(found)
	st.True(insertedEntry.ID > 0)

	entries := []entry{
		{Int: 11, Float: 1.100000, String: "1.100000", Time: now, Bool: false, Bytes: []byte("1.100000")},
		{Int: 12, Float: 1.200000, String: "1.200000", Time: now, Bool: true, Bytes: []byte("1.200000")},
		{Int: 13, Float: 1.300000, String: "1.300000", Time: now, Bool: false, Bytes: []byte("1.300000")},
		{Int: 14, Float: 1.400000, String: "1.400000", Time: now, Bool: true, Bytes: []byte("1.400000")},
		{Int: 14, Float: 1.400000, String: `abc'd"e"f\\gh\n\ri\x00`, Time: now, Bool: true, Bytes: []byte("1.400000")},
	}
	_, err = ds.Insert().Rows(entries).Executor().Exec()
	st.NoError(err)

	var newEntries []entry
	st.NoError(ds.Where(goqu.C("int").In([]uint32{11, 12, 13, 14})).ScanStructs(&newEntries))
	for i, e := range newEntries {
		st.Equal(entries[i].Int, e.Int)
		st.Equal(entries[i].Float, e.Float)
		st.Equal(entries[i].String, e.String)
		st.Equal(entries[i].Time.UTC().Format(mysql.DialectOptions().TimeFormat), e.Time.Format(mysql.DialectOptions().TimeFormat))
		st.Equal(entries[i].Bool, e.Bool)
		st.Equal(entries[i].Bytes, e.Bytes)
	}

	_, err = ds.Insert().Rows(
		entry{Int: 15, Float: 1.500000, String: "1.500000", Time: now, Bool: false, Bytes: []byte("1.500000")},
		entry{Int: 16, Float: 1.600000, String: "1.600000", Time: now, Bool: true, Bytes: []byte("1.600000")},
		entry{Int: 17, Float: 1.700000, String: "1.700000", Time: now, Bool: false, Bytes: []byte("1.700000")},
		entry{Int: 18, Float: 1.800000, String: "1.800000", Time: now, Bool: true, Bytes: []byte("1.800000")},
		entry{Int: 18, Float: 1.800000, String: `abc'd"e"f\\gh\n\ri\x00`, Time: now, Bool: true, Bytes: []byte("1.800000")},
	).Executor().Exec()
	st.NoError(err)

	newEntries = newEntries[0:0]
	st.NoError(ds.Where(goqu.C("int").In([]uint32{15, 16, 17, 18})).ScanStructs(&newEntries))
	st.Len(newEntries, 5)
}

func (st *sqlite3Suite) TestInsert_returning() {
	ds := st.db.From("entry")
	now := time.Now()
	e := entry{Int: 10, Float: 1.000000, String: "1.000000", Time: now, Bool: true, Bytes: []byte("1.000000")}
	_, err := ds.Insert().Rows(e).Returning(goqu.Star()).Executor().ScanStruct(&e)
	st.Error(err)
}

func (st *sqlite3Suite) TestUpdate() {
	ds := st.db.From("entry")
	var e entry
	found, err := ds.Where(goqu.C("int").Eq(9)).Select("id").ScanStruct(&e)
	st.NoError(err)
	st.True(found)
	e.Int = 11
	_, err = ds.Where(goqu.C("id").Eq(e.ID)).Update().Set(e).Executor().Exec()
	st.NoError(err)

	count, err := ds.Where(goqu.C("int").Eq(11)).Count()
	st.NoError(err)
	st.Equal(int64(1), count)
}

func (st *sqlite3Suite) TestUpdateReturning() {
	ds := st.db.From("entry")
	var id uint32
	_, err := ds.
		Where(goqu.C("id").Eq(1)).
		Update().
		Set(goqu.Record{"int": 11}).
		Returning("id").
		Executor().ScanVal(&id)
	st.NoError(err)
	st.GreaterOrEqual(id, uint32(0))
}

func (st *sqlite3Suite) TestDelete() {
	ds := st.db.From("entry")
	var e entry
	found, err := ds.Where(goqu.C("int").Eq(9)).Select("id").ScanStruct(&e)
	st.NoError(err)
	st.True(found)
	_, err = ds.Where(goqu.C("id").Eq(e.ID)).Delete().Executor().Exec()
	st.NoError(err)

	count, err := ds.Count()
	st.NoError(err)
	st.Equal(int64(9), count)

	var id uint32
	found, err = ds.Where(goqu.C("id").Eq(e.ID)).ScanVal(&id)
	st.NoError(err)
	st.False(found)

	e = entry{}
	found, err = ds.Where(goqu.C("int").Eq(8)).Select("id").ScanStruct(&e)
	st.NoError(err)
	st.True(found)
	st.NotEqual(int64(0), e.ID)

	id = 0
	_, err = ds.Where(goqu.C("id").Eq(e.ID)).Delete().Returning("id").Executor().ScanVal(&id)
	st.NoError(err)
	st.GreaterOrEqual(id, uint32(0))
}

func (st *sqlite3Suite) TestInsert_OnConflict() {
	ds := st.db.From("entry")
	now := time.Now()

	// insert new record with ID = 11
	e := entry{Int: 11, Float: 1.100000, String: "1.100000", Time: now, Bool: false, Bytes: []byte("1.100000")}
	_, err := ds.Insert().Rows(e).OnConflict(goqu.DoNothing()).Executor().Exec()
	st.NoError(err)

	var entryActual entry
	_, err = ds.Where(goqu.C("id").Eq(11)).ScanStruct(&entryActual)
	st.NoError(err)
	st.Equal("1.100000", entryActual.String)

	// duplicate with ON CONFLICT DO NOTHING should not be actually inserted
	_, err = ds.Insert().Rows(
		goqu.Record{
			"id":     11,
			"int":    99999999,
			"float":  "0.99999999",
			"string": "99999999",
			"time":   now,
			"bool":   true,
			"bytes":  []byte("0.99999999"),
		},
	).OnConflict(goqu.DoNothing()).Executor().Exec()
	st.NoError(err)

	_, err = ds.Where(goqu.C("id").Eq(11)).ScanStruct(&entryActual)
	st.NoError(err)
	st.Equal("1.100000", entryActual.String)

	// UPSERT record with ID primary key value conflict
	_, err = ds.Insert().Rows(
		goqu.Record{
			"id":     11,
			"int":    11,
			"float":  "1.100000",
			"string": "1.100000",
			"time":   now,
			"bool":   true,
			"bytes":  []byte("1.100000"),
		},
	).OnConflict(goqu.DoUpdate("id", goqu.Record{"string": "upsert"})).Executor().Exec()
	st.NoError(err)

	_, err = ds.Where(goqu.C("id").Eq(11)).ScanStruct(&entryActual)
	st.NoError(err)
	st.Equal("upsert", entryActual.String)

	// UPDATE ... ON CONFLICT (...) WHERE ... SET ... should result in error for now
	entries := []entry{
		{Int: 8, Float: 6.100000, String: "6.100000", Time: now, Bytes: []byte("6.100000")},
		{Int: 9, Float: 7.200000, String: "7.200000", Time: now, Bytes: []byte("7.200000")},
	}
	_, err = ds.Insert().
		Rows(entries).
		OnConflict(goqu.DoUpdate("id", goqu.Record{"string": "upsert"}).Where(goqu.C("id").Eq(9))).
		Executor().Exec()
	st.EqualError(err, "goqu: dialect does not support upsert with where clause [dialect=sqlite3]")
}

func TestSqlite3Suite(t *testing.T) {
	suite.Run(t, new(sqlite3Suite))
}
