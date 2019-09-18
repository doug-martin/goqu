package sqlite3

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/mattn/go-sqlite3"

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
	insertDefaultReords = "INSERT INTO `entry` (`int`, `float`, `string`, `time`, `bool`, `bytes`) VALUES" +
		"(0, 0.000000, '0.000000', '2015-02-22 18:19:55', 1,  '0.000000')," +
		"(1, 0.100000, '0.100000', '2015-02-22 19:19:55', 0, '0.100000')," +
		"(2, 0.200000, '0.200000', '2015-02-22 20:19:55', 1,  '0.200000')," +
		"(3, 0.300000, '0.300000', '2015-02-22 21:19:55', 0, '0.300000')," +
		"(4, 0.400000, '0.400000', '2015-02-22 22:19:55', 1,  '0.400000')," +
		"(5, 0.500000, '0.500000', '2015-02-22 23:19:55', 0, '0.500000')," +
		"(6, 0.600000, '0.600000', '2015-02-23 00:19:55', 1,  '0.600000')," +
		"(7, 0.700000, '0.700000', '2015-02-23 01:19:55', 0, '0.700000')," +
		"(8, 0.800000, '0.800000', '2015-02-23 02:19:55', 1,  '0.800000')," +
		"(9, 0.900000, '0.900000', '2015-02-23 03:19:55', 0, '0.900000');"
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
)

func (st *sqlite3Suite) SetupSuite() {
	fmt.Println(dbURI)
	db, err := sql.Open("sqlite3", dbURI)
	if err != nil {
		panic(err.Error())
	}
	st.db = goqu.New("sqlite3", db)
}

func (st *sqlite3Suite) SetupTest() {
	if _, err := st.db.Exec(dropTable); err != nil {
		panic(err)
	}
	if _, err := st.db.Exec(createTable); err != nil {
		panic(err)
	}
	if _, err := st.db.Exec(insertDefaultReords); err != nil {
		panic(err)
	}
}

func (st *sqlite3Suite) TestSelectSQL() {
	ds := st.db.From("entry")
	s, _, err := ds.Select("id", "float", "string", "time", "bool").ToSQL()
	st.NoError(err)
	st.Equal("SELECT `id`, `float`, `string`, `time`, `bool` FROM `entry`", s)

	s, _, err = ds.Where(goqu.C("int").Eq(10)).ToSQL()
	st.NoError(err)
	st.Equal("SELECT * FROM `entry` WHERE (`int` = 10)", s)

	s, args, err := ds.Prepared(true).Where(goqu.L("? = ?", goqu.C("int"), 10)).ToSQL()
	st.NoError(err)
	st.Equal([]interface{}{int64(10)}, args)
	st.Equal("SELECT * FROM `entry` WHERE `int` = ?", s)
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
	var entries []entry
	ds := st.db.From("entry")
	st.NoError(ds.Order(goqu.C("id").Asc()).ScanStructs(&entries))
	st.Len(entries, 10)
	floatVal := float64(0)
	baseDate, err := time.Parse(DialectOptions().TimeFormat, "2015-02-22 18:19:55")
	st.NoError(err)
	for i, entry := range entries {
		f := fmt.Sprintf("%f", floatVal)
		st.Equal(uint32(i+1), entry.ID)
		st.Equal(i, entry.Int)
		st.Equal(f, fmt.Sprintf("%f", entry.Float))
		st.Equal(f, entry.String)
		st.Equal([]byte(f), entry.Bytes)
		st.Equal(i%2 == 0, entry.Bool)
		st.Equal(baseDate.Add(time.Duration(i)*time.Hour), entry.Time)
		floatVal += float64(0.1)
	}
	entries = entries[0:0]
	st.NoError(ds.Where(goqu.C("bool").IsTrue()).Order(goqu.C("id").Asc()).ScanStructs(&entries))
	st.Len(entries, 5)
	st.NoError(err)
	for _, entry := range entries {
		st.True(entry.Bool)
	}

	entries = entries[0:0]
	st.NoError(ds.Where(goqu.C("int").Gt(4)).Order(goqu.C("id").Asc()).ScanStructs(&entries))
	st.Len(entries, 5)
	st.NoError(err)
	for _, entry := range entries {
		st.True(entry.Int > 4)
	}

	entries = entries[0:0]
	st.NoError(ds.Where(goqu.C("int").Gte(5)).Order(goqu.C("id").Asc()).ScanStructs(&entries))
	st.Len(entries, 5)
	st.NoError(err)
	for _, entry := range entries {
		st.True(entry.Int >= 5)
	}

	entries = entries[0:0]
	st.NoError(ds.Where(goqu.C("int").Lt(5)).Order(goqu.C("id").Asc()).ScanStructs(&entries))
	st.Len(entries, 5)
	st.NoError(err)
	for _, entry := range entries {
		st.True(entry.Int < 5)
	}

	entries = entries[0:0]
	st.NoError(ds.Where(goqu.C("int").Lte(4)).Order(goqu.C("id").Asc()).ScanStructs(&entries))
	st.Len(entries, 5)
	st.NoError(err)
	for _, entry := range entries {
		st.True(entry.Int <= 4)
	}

	entries = entries[0:0]
	st.NoError(ds.Where(goqu.C("int").Between(goqu.Range(3, 6))).Order(goqu.C("id").Asc()).ScanStructs(&entries))
	st.Len(entries, 4)
	st.NoError(err)
	for _, entry := range entries {
		st.True(entry.Int >= 3)
		st.True(entry.Int <= 6)
	}

	entries = entries[0:0]
	st.NoError(ds.Where(goqu.C("string").Eq("0.100000")).Order(goqu.C("id").Asc()).ScanStructs(&entries))
	st.Len(entries, 1)
	st.NoError(err)
	for _, entry := range entries {
		st.Equal(entry.String, "0.100000")
	}

	entries = entries[0:0]
	st.NoError(ds.Where(goqu.C("string").Like("0.1%")).Order(goqu.C("id").Asc()).ScanStructs(&entries))
	st.Len(entries, 1)
	st.NoError(err)
	for _, entry := range entries {
		st.Equal("0.100000", entry.String)
	}

	entries = entries[0:0]
	st.NoError(ds.Where(goqu.C("string").NotLike("0.1%")).Order(goqu.C("id").Asc()).ScanStructs(&entries))
	st.Len(entries, 9)
	st.NoError(err)
	for _, entry := range entries {
		st.NotEqual("0.100000", entry.String)
	}

	entries = entries[0:0]
	st.NoError(ds.Where(goqu.C("string").IsNull()).Order(goqu.C("id").Asc()).ScanStructs(&entries))
	st.Empty(entries)
}

func (st *sqlite3Suite) TestQuery_ValueExpressions() {
	type wrappedEntry struct {
		entry
		BoolValue bool `db:"bool_value"`
	}
	expectedDate, err := time.Parse("2006-01-02 15:04:05", "2015-02-22 19:19:55")
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
	}
	_, err = ds.Insert().Rows(entries).Executor().Exec()
	st.NoError(err)

	var newEntries []entry
	st.NoError(ds.Where(goqu.C("int").In([]uint32{11, 12, 13, 14})).ScanStructs(&newEntries))
	st.Len(newEntries, 4)

	_, err = ds.Insert().Rows(
		entry{Int: 15, Float: 1.500000, String: "1.500000", Time: now, Bool: false, Bytes: []byte("1.500000")},
		entry{Int: 16, Float: 1.600000, String: "1.600000", Time: now, Bool: true, Bytes: []byte("1.600000")},
		entry{Int: 17, Float: 1.700000, String: "1.700000", Time: now, Bool: false, Bytes: []byte("1.700000")},
		entry{Int: 18, Float: 1.800000, String: "1.800000", Time: now, Bool: true, Bytes: []byte("1.800000")},
	).Executor().Exec()
	st.NoError(err)

	newEntries = newEntries[0:0]
	st.NoError(ds.Where(goqu.C("int").In([]uint32{15, 16, 17, 18})).ScanStructs(&newEntries))
	st.Len(newEntries, 4)
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
		Where(goqu.C("int").Eq(11)).
		Update().
		Set(map[string]interface{}{"int": 9}).
		Returning("id").
		Executor().ScanVal(&id)
	st.Error(err)
	st.EqualError(err, "goqu: dialect does not support RETURNING clause [dialect=sqlite3]")
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
	st.EqualError(err, "goqu: dialect does not support RETURNING clause [dialect=sqlite3]")
}

func TestSqlite3Suite(t *testing.T) {
	suite.Run(t, new(sqlite3Suite))
}
