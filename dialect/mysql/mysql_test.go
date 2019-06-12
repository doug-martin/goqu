package mysql_test

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/doug-martin/goqu/v7"
	_ "github.com/doug-martin/goqu/v7/dialect/mysql"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
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
	t := mt.T()
	ds := mt.db.From("entry")
	s, _, err := ds.Select("id", "float", "string", "time", "bool").ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, s, "SELECT `id`, `float`, `string`, `time`, `bool` FROM `entry`")

	s, _, err = ds.Where(goqu.C("int").Eq(10)).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, s, "SELECT * FROM `entry` WHERE (`int` = 10)")

	s, args, err := ds.Prepared(true).Where(goqu.L("? = ?", goqu.C("int"), 10)).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(10)})
	assert.Equal(t, s, "SELECT * FROM `entry` WHERE `int` = ?")
}

func (mt *mysqlTest) TestQuery() {
	t := mt.T()
	var entries []entry
	ds := mt.db.From("entry")
	assert.NoError(t, ds.Order(goqu.C("id").Asc()).ScanStructs(&entries))
	assert.Len(t, entries, 10)
	floatVal := float64(0)
	baseDate, err := time.Parse(
		"2006-01-02 15:04:05",
		"2015-02-22 18:19:55",
	)
	assert.NoError(t, err)
	for i, entry := range entries {
		f := fmt.Sprintf("%f", floatVal)
		assert.Equal(t, entry.ID, uint32(i+1))
		assert.Equal(t, entry.Int, i)
		assert.Equal(t, fmt.Sprintf("%f", entry.Float), f)
		assert.Equal(t, entry.String, f)
		assert.Equal(t, entry.Bytes, []byte(f))
		assert.Equal(t, entry.Bool, i%2 == 0)
		assert.Equal(t, entry.Time, baseDate.Add(time.Duration(i)*time.Hour))
		floatVal += float64(0.1)
	}
	entries = entries[0:0]
	assert.NoError(t, ds.Where(goqu.C("bool").IsTrue()).Order(goqu.C("id").Asc()).ScanStructs(&entries))
	assert.Len(t, entries, 5)
	assert.NoError(t, err)
	for _, entry := range entries {
		assert.True(t, entry.Bool)
	}

	entries = entries[0:0]
	assert.NoError(t, ds.Where(goqu.C("int").Gt(4)).Order(goqu.C("id").Asc()).ScanStructs(&entries))
	assert.Len(t, entries, 5)
	assert.NoError(t, err)
	for _, entry := range entries {
		assert.True(t, entry.Int > 4)
	}

	entries = entries[0:0]
	assert.NoError(t, ds.Where(goqu.C("int").Gte(5)).Order(goqu.C("id").Asc()).ScanStructs(&entries))
	assert.Len(t, entries, 5)
	assert.NoError(t, err)
	for _, entry := range entries {
		assert.True(t, entry.Int >= 5)
	}

	entries = entries[0:0]
	assert.NoError(t, ds.Where(goqu.C("int").Lt(5)).Order(goqu.C("id").Asc()).ScanStructs(&entries))
	assert.Len(t, entries, 5)
	assert.NoError(t, err)
	for _, entry := range entries {
		assert.True(t, entry.Int < 5)
	}

	entries = entries[0:0]
	assert.NoError(t, ds.Where(goqu.C("int").Lte(4)).Order(goqu.C("id").Asc()).ScanStructs(&entries))
	assert.Len(t, entries, 5)
	assert.NoError(t, err)
	for _, entry := range entries {
		assert.True(t, entry.Int <= 4)
	}

	entries = entries[0:0]
	assert.NoError(t, ds.Where(goqu.C("int").Between(goqu.Range(3, 6))).Order(goqu.C("id").Asc()).ScanStructs(&entries))
	assert.Len(t, entries, 4)
	assert.NoError(t, err)
	for _, entry := range entries {
		assert.True(t, entry.Int >= 3)
		assert.True(t, entry.Int <= 6)
	}

	entries = entries[0:0]
	assert.NoError(t, ds.Where(goqu.C("string").Eq("0.100000")).Order(goqu.C("id").Asc()).ScanStructs(&entries))
	assert.Len(t, entries, 1)
	assert.NoError(t, err)
	for _, entry := range entries {
		assert.Equal(t, entry.String, "0.100000")
	}

	entries = entries[0:0]
	assert.NoError(t, ds.Where(goqu.C("string").Like("0.1%")).Order(goqu.C("id").Asc()).ScanStructs(&entries))
	assert.Len(t, entries, 1)
	assert.NoError(t, err)
	for _, entry := range entries {
		assert.Equal(t, entry.String, "0.100000")
	}

	entries = entries[0:0]
	assert.NoError(t, ds.Where(goqu.C("string").NotLike("0.1%")).Order(goqu.C("id").Asc()).ScanStructs(&entries))
	assert.Len(t, entries, 9)
	assert.NoError(t, err)
	for _, entry := range entries {
		assert.NotEqual(t, entry.String, "0.100000")
	}

	entries = entries[0:0]
	assert.NoError(t, ds.Where(goqu.C("string").IsNull()).Order(goqu.C("id").Asc()).ScanStructs(&entries))
	assert.Len(t, entries, 0)
}

func (mt *mysqlTest) TestCount() {
	t := mt.T()
	ds := mt.db.From("entry")
	count, err := ds.Count()
	assert.NoError(t, err)
	assert.Equal(t, count, int64(10))
	count, err = ds.Where(goqu.C("int").Gt(4)).Count()
	assert.NoError(t, err)
	assert.Equal(t, count, int64(5))
	count, err = ds.Where(goqu.C("int").Gte(4)).Count()
	assert.NoError(t, err)
	assert.Equal(t, count, int64(6))
	count, err = ds.Where(goqu.C("string").Like("0.1%")).Count()
	assert.NoError(t, err)
	assert.Equal(t, count, int64(1))
	count, err = ds.Where(goqu.C("string").IsNull()).Count()
	assert.NoError(t, err)
	assert.Equal(t, count, int64(0))
}

func (mt *mysqlTest) TestInsert() {
	t := mt.T()
	ds := mt.db.From("entry")
	now := time.Now()
	e := entry{Int: 10, Float: 1.000000, String: "1.000000", Time: now, Bool: true, Bytes: []byte("1.000000")}
	_, err := ds.Insert(e).Exec()
	assert.NoError(t, err)

	var insertedEntry entry
	found, err := ds.Where(goqu.C("int").Eq(10)).ScanStruct(&insertedEntry)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.True(t, insertedEntry.ID > 0)

	entries := []entry{
		{Int: 11, Float: 1.100000, String: "1.100000", Time: now, Bool: false, Bytes: []byte("1.100000")},
		{Int: 12, Float: 1.200000, String: "1.200000", Time: now, Bool: true, Bytes: []byte("1.200000")},
		{Int: 13, Float: 1.300000, String: "1.300000", Time: now, Bool: false, Bytes: []byte("1.300000")},
		{Int: 14, Float: 1.400000, String: "1.400000", Time: now, Bool: true, Bytes: []byte("1.400000")},
	}
	_, err = ds.Insert(entries).Exec()
	assert.NoError(t, err)

	var newEntries []entry
	assert.NoError(t, ds.Where(goqu.C("int").In([]uint32{11, 12, 13, 14})).ScanStructs(&newEntries))
	assert.Len(t, newEntries, 4)

	_, err = ds.Insert(
		entry{Int: 15, Float: 1.500000, String: "1.500000", Time: now, Bool: false, Bytes: []byte("1.500000")},
		entry{Int: 16, Float: 1.600000, String: "1.600000", Time: now, Bool: true, Bytes: []byte("1.600000")},
		entry{Int: 17, Float: 1.700000, String: "1.700000", Time: now, Bool: false, Bytes: []byte("1.700000")},
		entry{Int: 18, Float: 1.800000, String: "1.800000", Time: now, Bool: true, Bytes: []byte("1.800000")},
	).Exec()
	assert.NoError(t, err)

	newEntries = newEntries[0:0]
	assert.NoError(t, ds.Where(goqu.C("int").In([]uint32{15, 16, 17, 18})).ScanStructs(&newEntries))
	assert.Len(t, newEntries, 4)
}

func (mt *mysqlTest) TestInsertReturning() {
	t := mt.T()
	ds := mt.db.From("entry")
	now := time.Now()
	e := entry{Int: 10, Float: 1.000000, String: "1.000000", Time: now, Bool: true, Bytes: []byte("1.000000")}
	_, err := ds.Returning(goqu.Star()).Insert(e).ScanStruct(&e)
	assert.Error(t, err)

}

func (mt *mysqlTest) TestUpdate() {
	t := mt.T()
	ds := mt.db.From("entry")
	var e entry
	found, err := ds.Where(goqu.C("int").Eq(9)).Select("id").ScanStruct(&e)
	assert.NoError(t, err)
	assert.True(t, found)
	e.Int = 11
	_, err = ds.Where(goqu.C("id").Eq(e.ID)).Update(e).Exec()
	assert.NoError(t, err)

	count, err := ds.Where(goqu.C("int").Eq(11)).Count()
	assert.NoError(t, err)
	assert.Equal(t, count, int64(1))
}

func (mt *mysqlTest) TestUpdateReturning() {
	t := mt.T()
	ds := mt.db.From("entry")
	var id uint32
	_, err := ds.Where(goqu.C("int").Eq(11)).Returning("id").Update(goqu.Record{"int": 9}).ScanVal(&id)
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "goqu: adapter does not support RETURNING clause")
}

func (mt *mysqlTest) TestDelete() {
	t := mt.T()
	ds := mt.db.From("entry")
	var e entry
	found, err := ds.Where(goqu.C("int").Eq(9)).Select("id").ScanStruct(&e)
	assert.NoError(t, err)
	assert.True(t, found)
	_, err = ds.Where(goqu.C("id").Eq(e.ID)).Delete().Exec()
	assert.NoError(t, err)

	count, err := ds.Count()
	assert.NoError(t, err)
	assert.Equal(t, count, int64(9))

	var id uint32
	found, err = ds.Where(goqu.C("id").Eq(e.ID)).ScanVal(&id)
	assert.NoError(t, err)
	assert.False(t, found)

	e = entry{}
	found, err = ds.Where(goqu.C("int").Eq(8)).Select("id").ScanStruct(&e)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.NotEqual(t, e.ID, 0)

	id = 0
	_, err = ds.Where(goqu.C("id").Eq(e.ID)).Returning("id").Delete().ScanVal(&id)
	assert.Equal(t, err.Error(), "goqu: adapter does not support RETURNING clause")
}

func (mt *mysqlTest) TestInsertIgnore() {
	t := mt.T()
	ds := mt.db.From("entry")
	now := time.Now()

	// insert one
	entries := []entry{
		{Int: 8, Float: 6.100000, String: "6.100000", Time: now, Bytes: []byte("6.100000")},
		{Int: 9, Float: 7.200000, String: "7.200000", Time: now, Bytes: []byte("7.200000")},
		{Int: 10, Float: 7.200000, String: "7.200000", Time: now, Bytes: []byte("7.200000")},
	}
	_, err := ds.InsertIgnore(entries).Exec()
	assert.NoError(t, err)

	count, err := ds.Count()
	assert.NoError(t, err)
	assert.Equal(t, count, int64(11))
}

func (mt *mysqlTest) TestInsertConflict() {
	t := mt.T()
	ds := mt.db.From("entry")
	now := time.Now()

	// insert
	e := entry{Int: 10, Float: 1.100000, String: "1.100000", Time: now, Bool: false, Bytes: []byte("1.100000")}
	_, err := ds.InsertConflict(goqu.DoNothing(), e).Exec()
	assert.NoError(t, err)

	// duplicate
	e = entry{Int: 10, Float: 2.100000, String: "2.100000", Time: now.Add(time.Hour * 100), Bool: false, Bytes: []byte("2.100000")}
	_, err = ds.InsertConflict(goqu.DoNothing(), e).Exec()
	assert.NoError(t, err)

	// update
	var entryActual entry
	e2 := entry{Int: 10, String: "2.000000"}
	_, err = ds.InsertConflict(goqu.DoUpdate("int", goqu.Record{"string": "upsert"}), e2).Exec()
	assert.NoError(t, err)
	_, err = ds.Where(goqu.C("int").Eq(10)).ScanStruct(&entryActual)
	assert.NoError(t, err)
	assert.Equal(t, "upsert", entryActual.String)

	// update where should error
	entries := []entry{
		{Int: 8, Float: 6.100000, String: "6.100000", Time: now, Bytes: []byte("6.100000")},
		{Int: 9, Float: 7.200000, String: "7.200000", Time: now, Bytes: []byte("7.200000")},
	}
	_, err = ds.InsertConflict(goqu.DoUpdate("int", goqu.Record{"string": "upsert"}).Where(goqu.C("int").Eq(9)), entries).Exec()
	assert.Equal(t, err.Error(), "goqu: adapter does not support upsert with where clause")
}

func TestMysqlSuite(t *testing.T) {
	suite.Run(t, new(mysqlTest))
}
