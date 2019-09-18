package mysql_test

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/mysql"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/suite"
)

const (
	dropTable   = "DROP TABLE IF EXISTS `entry`;"
	createTable = "CREATE  TABLE `entry` (" +
		"`id` INT NOT NULL AUTO_INCREMENT ," +
		"`int` INT NOT NULL UNIQUE," +
		"`float` FLOAT NOT NULL ," +
		"`string` VARCHAR(255) NOT NULL ," +
		"`time` DATETIME NOT NULL ," +
		"`bool` TINYINT NOT NULL ," +
		"`bytes` BLOB NOT NULL ," +
		"PRIMARY KEY (`id`) );"
	insertDefaultReords = "INSERT INTO `entry` (`int`, `float`, `string`, `time`, `bool`, `bytes`) VALUES" +
		"(0, 0.000000, '0.000000', '2015-02-22 18:19:55', TRUE,  '0.000000')," +
		"(1, 0.100000, '0.100000', '2015-02-22 19:19:55', FALSE, '0.100000')," +
		"(2, 0.200000, '0.200000', '2015-02-22 20:19:55', TRUE,  '0.200000')," +
		"(3, 0.300000, '0.300000', '2015-02-22 21:19:55', FALSE, '0.300000')," +
		"(4, 0.400000, '0.400000', '2015-02-22 22:19:55', TRUE,  '0.400000')," +
		"(5, 0.500000, '0.500000', '2015-02-22 23:19:55', FALSE, '0.500000')," +
		"(6, 0.600000, '0.600000', '2015-02-23 00:19:55', TRUE,  '0.600000')," +
		"(7, 0.700000, '0.700000', '2015-02-23 01:19:55', FALSE, '0.700000')," +
		"(8, 0.800000, '0.800000', '2015-02-23 02:19:55', TRUE,  '0.800000')," +
		"(9, 0.900000, '0.900000', '2015-02-23 03:19:55', FALSE, '0.900000');"
)

const defaultDbURI = "root@/goqumysql?parseTime=true"

type (
	mysqlTest struct {
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

func (mt *mysqlTest) SetupSuite() {
	dbURI := os.Getenv("MYSQL_URI")
	if dbURI == "" {
		dbURI = defaultDbURI
	}
	db, err := sql.Open("mysql", dbURI)
	if err != nil {
		panic(err.Error())
	}
	mt.db = goqu.New("mysql", db)
}

func (mt *mysqlTest) SetupTest() {
	if _, err := mt.db.Exec(dropTable); err != nil {
		panic(err)
	}
	if _, err := mt.db.Exec(createTable); err != nil {
		panic(err)
	}
	if _, err := mt.db.Exec(insertDefaultReords); err != nil {
		panic(err)
	}
}

func (mt *mysqlTest) TestToSQL() {
	ds := mt.db.From("entry")
	s, _, err := ds.Select("id", "float", "string", "time", "bool").ToSQL()
	mt.NoError(err)
	mt.Equal("SELECT `id`, `float`, `string`, `time`, `bool` FROM `entry`", s)

	s, _, err = ds.Where(goqu.C("int").Eq(10)).ToSQL()
	mt.NoError(err)
	mt.Equal("SELECT * FROM `entry` WHERE (`int` = 10)", s)

	s, args, err := ds.Prepared(true).Where(goqu.L("? = ?", goqu.C("int"), 10)).ToSQL()
	mt.NoError(err)
	mt.Equal([]interface{}{int64(10)}, args)
	mt.Equal("SELECT * FROM `entry` WHERE `int` = ?", s)
}

func (mt *mysqlTest) TestQuery() {
	var entries []entry
	ds := mt.db.From("entry")
	mt.NoError(ds.Order(goqu.C("id").Asc()).ScanStructs(&entries))
	mt.Len(entries, 10)
	floatVal := float64(0)
	baseDate, err := time.Parse(
		"2006-01-02 15:04:05",
		"2015-02-22 18:19:55",
	)
	mt.NoError(err)
	for i, entry := range entries {
		f := fmt.Sprintf("%f", floatVal)
		mt.Equal(uint32(i+1), entry.ID)
		mt.Equal(i, entry.Int)
		mt.Equal(f, fmt.Sprintf("%f", entry.Float))
		mt.Equal(f, entry.String)
		mt.Equal([]byte(f), entry.Bytes)
		mt.Equal(i%2 == 0, entry.Bool)
		mt.Equal(baseDate.Add(time.Duration(i)*time.Hour), entry.Time)
		floatVal += float64(0.1)
	}
	entries = entries[0:0]
	mt.NoError(ds.Where(goqu.C("bool").IsTrue()).Order(goqu.C("id").Asc()).ScanStructs(&entries))
	mt.Len(entries, 5)
	mt.NoError(err)
	for _, entry := range entries {
		mt.True(entry.Bool)
	}

	entries = entries[0:0]
	mt.NoError(ds.Where(goqu.C("int").Gt(4)).Order(goqu.C("id").Asc()).ScanStructs(&entries))
	mt.Len(entries, 5)
	mt.NoError(err)
	for _, entry := range entries {
		mt.True(entry.Int > 4)
	}

	entries = entries[0:0]
	mt.NoError(ds.Where(goqu.C("int").Gte(5)).Order(goqu.C("id").Asc()).ScanStructs(&entries))
	mt.Len(entries, 5)
	mt.NoError(err)
	for _, entry := range entries {
		mt.True(entry.Int >= 5)
	}

	entries = entries[0:0]
	mt.NoError(ds.Where(goqu.C("int").Lt(5)).Order(goqu.C("id").Asc()).ScanStructs(&entries))
	mt.Len(entries, 5)
	mt.NoError(err)
	for _, entry := range entries {
		mt.True(entry.Int < 5)
	}

	entries = entries[0:0]
	mt.NoError(ds.Where(goqu.C("int").Lte(4)).Order(goqu.C("id").Asc()).ScanStructs(&entries))
	mt.Len(entries, 5)
	mt.NoError(err)
	for _, entry := range entries {
		mt.True(entry.Int <= 4)
	}

	entries = entries[0:0]
	mt.NoError(ds.Where(goqu.C("int").Between(goqu.Range(3, 6))).Order(goqu.C("id").Asc()).ScanStructs(&entries))
	mt.Len(entries, 4)
	mt.NoError(err)
	for _, entry := range entries {
		mt.True(entry.Int >= 3)
		mt.True(entry.Int <= 6)
	}

	entries = entries[0:0]
	mt.NoError(ds.Where(goqu.C("string").Eq("0.100000")).Order(goqu.C("id").Asc()).ScanStructs(&entries))
	mt.Len(entries, 1)
	mt.NoError(err)
	for _, entry := range entries {
		mt.Equal("0.100000", entry.String)
	}

	entries = entries[0:0]
	mt.NoError(ds.Where(goqu.C("string").Like("0.1%")).Order(goqu.C("id").Asc()).ScanStructs(&entries))
	mt.Len(entries, 1)
	mt.NoError(err)
	for _, entry := range entries {
		mt.Equal("0.100000", entry.String)
	}

	entries = entries[0:0]
	mt.NoError(ds.Where(goqu.C("string").NotLike("0.1%")).Order(goqu.C("id").Asc()).ScanStructs(&entries))
	mt.Len(entries, 9)
	mt.NoError(err)
	for _, entry := range entries {
		mt.NotEqual("0.100000", entry.String)
	}

	entries = entries[0:0]
	mt.NoError(ds.Where(goqu.C("string").IsNull()).Order(goqu.C("id").Asc()).ScanStructs(&entries))
	mt.Len(entries, 0)
}

func (mt *mysqlTest) TestQuery_ValueExpressions() {
	type wrappedEntry struct {
		entry
		BoolValue bool `db:"bool_value"`
	}
	expectedDate, err := time.Parse("2006-01-02 15:04:05", "2015-02-22 19:19:55")
	mt.NoError(err)
	ds := mt.db.From("entry").Select(goqu.Star(), goqu.V(true).As("bool_value")).Where(goqu.Ex{"int": 1})
	var we wrappedEntry
	found, err := ds.ScanStruct(&we)
	mt.NoError(err)
	mt.True(found)
	mt.Equal(wrappedEntry{
		entry{2, 1, 0.100000, "0.100000", expectedDate, false, []byte("0.100000")},
		true,
	}, we)
}

func (mt *mysqlTest) TestCount() {
	ds := mt.db.From("entry")
	count, err := ds.Count()
	mt.NoError(err)
	mt.Equal(int64(10), count)
	count, err = ds.Where(goqu.C("int").Gt(4)).Count()
	mt.NoError(err)
	mt.Equal(int64(5), count)
	count, err = ds.Where(goqu.C("int").Gte(4)).Count()
	mt.NoError(err)
	mt.Equal(int64(6), count)
	count, err = ds.Where(goqu.C("string").Like("0.1%")).Count()
	mt.NoError(err)
	mt.Equal(int64(1), count)
	count, err = ds.Where(goqu.C("string").IsNull()).Count()
	mt.NoError(err)
	mt.Equal(int64(0), count)
}

func (mt *mysqlTest) TestInsert() {
	ds := mt.db.From("entry")
	now := time.Now()
	e := entry{Int: 10, Float: 1.000000, String: "1.000000", Time: now, Bool: true, Bytes: []byte("1.000000")}
	_, err := ds.Insert().Rows(e).Executor().Exec()
	mt.NoError(err)

	var insertedEntry entry
	found, err := ds.Where(goqu.C("int").Eq(10)).ScanStruct(&insertedEntry)
	mt.NoError(err)
	mt.True(found)
	mt.True(insertedEntry.ID > 0)

	entries := []entry{
		{Int: 11, Float: 1.100000, String: "1.100000", Time: now, Bool: false, Bytes: []byte("1.100000")},
		{Int: 12, Float: 1.200000, String: "1.200000", Time: now, Bool: true, Bytes: []byte("1.200000")},
		{Int: 13, Float: 1.300000, String: "1.300000", Time: now, Bool: false, Bytes: []byte("1.300000")},
		{Int: 14, Float: 1.400000, String: "1.400000", Time: now, Bool: true, Bytes: []byte("1.400000")},
	}
	_, err = ds.Insert().Rows(entries).Executor().Exec()
	mt.NoError(err)

	var newEntries []entry
	mt.NoError(ds.Where(goqu.C("int").In([]uint32{11, 12, 13, 14})).ScanStructs(&newEntries))
	mt.Len(newEntries, 4)

	_, err = ds.Insert().Rows(
		entry{Int: 15, Float: 1.500000, String: "1.500000", Time: now, Bool: false, Bytes: []byte("1.500000")},
		entry{Int: 16, Float: 1.600000, String: "1.600000", Time: now, Bool: true, Bytes: []byte("1.600000")},
		entry{Int: 17, Float: 1.700000, String: "1.700000", Time: now, Bool: false, Bytes: []byte("1.700000")},
		entry{Int: 18, Float: 1.800000, String: "1.800000", Time: now, Bool: true, Bytes: []byte("1.800000")},
	).Executor().Exec()
	mt.NoError(err)

	newEntries = newEntries[0:0]
	mt.NoError(ds.Where(goqu.C("int").In([]uint32{15, 16, 17, 18})).ScanStructs(&newEntries))
	mt.Len(newEntries, 4)
}

func (mt *mysqlTest) TestInsertReturning() {
	ds := mt.db.From("entry")
	now := time.Now()
	e := entry{Int: 10, Float: 1.000000, String: "1.000000", Time: now, Bool: true, Bytes: []byte("1.000000")}
	_, err := ds.Insert().Rows(e).Returning(goqu.Star()).Executor().ScanStruct(&e)
	mt.Error(err)

}

func (mt *mysqlTest) TestUpdate() {
	ds := mt.db.From("entry")
	var e entry
	found, err := ds.Where(goqu.C("int").Eq(9)).Select("id").ScanStruct(&e)
	mt.NoError(err)
	mt.True(found)
	e.Int = 11
	_, err = ds.Where(goqu.C("id").Eq(e.ID)).Update().Set(e).Executor().Exec()
	mt.NoError(err)

	count, err := ds.Where(goqu.C("int").Eq(11)).Count()
	mt.NoError(err)
	mt.Equal(int64(1), count)
}

func (mt *mysqlTest) TestUpdateReturning() {
	ds := mt.db.From("entry")
	var id uint32
	_, err := ds.Where(goqu.C("int").Eq(11)).
		Update().
		Set(goqu.Record{"int": 9}).
		Returning("id").
		Executor().ScanVal(&id)
	mt.Error(err)
	mt.EqualError(err, "goqu: dialect does not support RETURNING clause [dialect=mysql]")
}

func (mt *mysqlTest) TestDelete() {
	ds := mt.db.From("entry")
	var e entry
	found, err := ds.Where(goqu.C("int").Eq(9)).Select("id").ScanStruct(&e)
	mt.NoError(err)
	mt.True(found)
	_, err = ds.Where(goqu.C("id").Eq(e.ID)).Delete().Executor().Exec()
	mt.NoError(err)

	count, err := ds.Count()
	mt.NoError(err)
	mt.Equal(int64(9), count)

	var id uint32
	found, err = ds.Where(goqu.C("id").Eq(e.ID)).ScanVal(&id)
	mt.NoError(err)
	mt.False(found)

	e = entry{}
	found, err = ds.Where(goqu.C("int").Eq(8)).Select("id").ScanStruct(&e)
	mt.NoError(err)
	mt.True(found)
	mt.NotEqual(0, e.ID)

	id = 0
	_, err = ds.Where(goqu.C("id").Eq(e.ID)).Delete().Returning("id").Executor().ScanVal(&id)
	mt.EqualError(err, "goqu: dialect does not support RETURNING clause [dialect=mysql]")
}

func (mt *mysqlTest) TestInsertIgnore() {
	ds := mt.db.From("entry")
	now := time.Now()

	// insert one
	entries := []entry{
		{Int: 8, Float: 6.100000, String: "6.100000", Time: now, Bytes: []byte("6.100000")},
		{Int: 9, Float: 7.200000, String: "7.200000", Time: now, Bytes: []byte("7.200000")},
		{Int: 10, Float: 7.200000, String: "7.200000", Time: now, Bytes: []byte("7.200000")},
	}
	_, err := ds.Insert().Rows(entries).OnConflict(goqu.DoNothing()).Executor().Exec()
	mt.NoError(err)

	count, err := ds.Count()
	mt.NoError(err)
	mt.Equal(count, int64(11))
}

func (mt *mysqlTest) TestInsert_OnConflict() {
	ds := mt.db.From("entry")
	now := time.Now()

	// insert
	e := entry{Int: 10, Float: 1.100000, String: "1.100000", Time: now, Bool: false, Bytes: []byte("1.100000")}
	_, err := ds.Insert().Rows(e).OnConflict(goqu.DoNothing()).Executor().Exec()
	mt.NoError(err)

	// duplicate
	e = entry{Int: 10, Float: 2.100000, String: "2.100000", Time: now.Add(time.Hour * 100), Bool: false, Bytes: []byte("2.100000")}
	_, err = ds.Insert().Rows(e).OnConflict(goqu.DoNothing()).Executor().Exec()
	mt.NoError(err)

	// update
	var entryActual entry
	e2 := entry{Int: 10, String: "2.000000"}
	_, err = ds.Insert().
		Rows(e2).
		OnConflict(goqu.DoUpdate("int", goqu.Record{"string": "upsert"})).
		Executor().Exec()
	mt.NoError(err)
	_, err = ds.Where(goqu.C("int").Eq(10)).ScanStruct(&entryActual)
	mt.NoError(err)
	mt.Equal("upsert", entryActual.String)

	// update where should error
	entries := []entry{
		{Int: 8, Float: 6.100000, String: "6.100000", Time: now, Bytes: []byte("6.100000")},
		{Int: 9, Float: 7.200000, String: "7.200000", Time: now, Bytes: []byte("7.200000")},
	}
	_, err = ds.Insert().
		Rows(entries).
		OnConflict(goqu.DoUpdate("int", goqu.Record{"string": "upsert"}).Where(goqu.C("int").Eq(9))).
		Executor().Exec()
	mt.EqualError(err, "goqu: dialect does not support upsert with where clause [dialect=mysql]")
}

func (mt *mysqlTest) TestWindowFunction() {
	var version string
	ok, err := mt.db.Select(goqu.Func("version")).ScanVal(&version)
	mt.NoError(err)
	mt.True(ok)

	fields := strings.Split(version, ".")
	mt.True(len(fields) > 0)
	major, err := strconv.Atoi(fields[0])
	mt.NoError(err)
	if major < 8 {
		fmt.Printf("SKIPPING MYSQL WINDOW FUNCTION TEST BECAUSE VERSION IS < 8 [mysql_version:=%d]\n", major)
		return
	}

	ds := mt.db.From("entry").
		Select("int", goqu.ROW_NUMBER().OverName(goqu.I("w")).As("id")).
		Window(goqu.W("w").OrderBy(goqu.I("int").Desc()))

	var entries []entry
	mt.NoError(ds.WithDialect("mysql8").ScanStructs(&entries))

	mt.Equal([]entry{
		{Int: 9, ID: 1},
		{Int: 8, ID: 2},
		{Int: 7, ID: 3},
		{Int: 6, ID: 4},
		{Int: 5, ID: 5},
		{Int: 4, ID: 6},
		{Int: 3, ID: 7},
		{Int: 2, ID: 8},
		{Int: 1, ID: 9},
		{Int: 0, ID: 10},
	}, entries)

	mt.Error(ds.WithDialect("mysql").ScanStructs(&entries), "goqu: adapter does not support window function clause")
}

func TestMysqlSuite(t *testing.T) {
	suite.Run(t, new(mysqlTest))
}
