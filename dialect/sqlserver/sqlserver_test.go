package sqlserver_test

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/doug-martin/goqu/v9/dialect/mysql"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/sqlserver"
	"github.com/stretchr/testify/suite"
)

const (
	dropTable   = "DROP TABLE IF EXISTS \"entry\";"
	createTable = "CREATE  TABLE \"entry\" (" +
		"\"id\" INT NOT NULL IDENTITY(1,1)," +
		"\"int\" INT NOT NULL UNIQUE," +
		"\"float\" FLOAT NOT NULL ," +
		"\"string\" VARCHAR(255) NOT NULL ," +
		"\"time\" DATETIME NOT NULL ," +
		"\"bool\" BIT NOT NULL ," +
		"\"bytes\" VARBINARY(100) NOT NULL ," +
		"PRIMARY KEY (\"id\") );"
	insertDefaultRecords = "INSERT INTO [entry] ([int], [float], [string], [time], [bool], [bytes]) VALUES" +
		"(0, 0.000000, '0.000000', '2015-02-22 18:19:55', 1, CONVERT(BINARY(8), '0.000000'))," +
		"(1, 0.100000, '0.100000', '2015-02-22 19:19:55', 0, CONVERT(BINARY(8), '0.100000'))," +
		"(2, 0.200000, '0.200000', '2015-02-22 20:19:55', 1, CONVERT(BINARY(8), '0.200000'))," +
		"(3, 0.300000, '0.300000', '2015-02-22 21:19:55', 0, CONVERT(BINARY(8), '0.300000'))," +
		"(4, 0.400000, '0.400000', '2015-02-22 22:19:55', 1, CONVERT(BINARY(8), '0.400000'))," +
		"(5, 0.500000, '0.500000', '2015-02-22 23:19:55', 0, CONVERT(BINARY(8), '0.500000'))," +
		"(6, 0.600000, '0.600000', '2015-02-23 00:19:55', 1, CONVERT(BINARY(8), '0.600000'))," +
		"(7, 0.700000, '0.700000', '2015-02-23 01:19:55', 0, CONVERT(BINARY(8), '0.700000'))," +
		"(8, 0.800000, '0.800000', '2015-02-23 02:19:55', 1, CONVERT(BINARY(8), '0.800000'))," +
		"(9, 0.900000, '0.900000', '2015-02-23 03:19:55', 0, CONVERT(BINARY(8), '0.900000'));"
)

const defaultDbURI = "sqlserver://sa:qwe123QWE@127.0.0.1:1433?database=master&connection+timeout=30"

type (
	sqlserverTest struct {
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

func (mt *sqlserverTest) SetupSuite() {
	dbURI := os.Getenv("SQLSERVER_URI")
	if dbURI == "" {
		dbURI = defaultDbURI
	}
	db, err := sql.Open("sqlserver", dbURI)
	if err != nil {
		panic(err.Error())
	}
	mt.db = goqu.New("sqlserver", db)
}

func (mt *sqlserverTest) SetupTest() {
	if _, err := mt.db.Exec(dropTable); err != nil {
		panic(err)
	}
	if _, err := mt.db.Exec(createTable); err != nil {
		panic(err)
	}
	if _, err := mt.db.Exec(insertDefaultRecords); err != nil {
		panic(err)
	}
}

func (mt *sqlserverTest) TestToSQL() {
	ds := mt.db.From("entry")
	s, _, err := ds.Select("id", "float", "string", "time", "bool").ToSQL()
	mt.NoError(err)
	mt.Equal("SELECT \"id\", \"float\", \"string\", \"time\", \"bool\" FROM \"entry\"", s)

	s, _, err = ds.Where(goqu.C("int").Eq(10)).ToSQL()
	mt.NoError(err)
	mt.Equal("SELECT * FROM \"entry\" WHERE (\"int\" = 10)", s)

	s, args, err := ds.Prepared(true).Where(goqu.L("? = ?", goqu.C("int"), 10)).ToSQL()
	mt.NoError(err)
	mt.Equal([]interface{}{int64(10)}, args)
	mt.Equal("SELECT * FROM \"entry\" WHERE \"int\" = @p1", s)
}

func (mt *sqlserverTest) TestQuery() {
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

func (mt *sqlserverTest) TestQuery_Prepared() {
	var entries []entry
	ds := mt.db.From("entry").Prepared(true)

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

func (mt *sqlserverTest) TestQuery_ValueExpressions() {
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

func (mt *sqlserverTest) TestCount() {
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

func (mt *sqlserverTest) TestLimitOffset() {
	ds := mt.db.From("entry").Where(goqu.C("id").Gte(1)).Limit(1)
	var e entry
	found, err := ds.ScanStruct(&e)
	mt.NoError(err)
	mt.True(found)
	mt.Equal(uint32(1), e.ID)

	ds = mt.db.From("entry").Where(goqu.C("id").Gte(1)).Order(goqu.C("id").Desc()).Limit(1)
	found, err = ds.ScanStruct(&e)
	mt.NoError(err)
	mt.True(found)
	mt.Equal(uint32(10), e.ID)

	ds = mt.db.From("entry").Where(goqu.C("id").Gte(1)).Order(goqu.C("id").Asc()).Offset(1).Limit(1)
	found, err = ds.ScanStruct(&e)
	mt.NoError(err)
	mt.True(found)
	mt.Equal(uint32(2), e.ID)
}

func (mt *sqlserverTest) TestLimitOffsetParameterized() {
	ds := mt.db.From("entry").Prepared(true).Where(goqu.C("id").Gte(1)).Limit(1)
	var e entry
	found, err := ds.ScanStruct(&e)
	mt.NoError(err)
	mt.True(found)
	mt.Equal(uint32(1), e.ID)

	ds = mt.db.From("entry").Prepared(true).Where(goqu.C("id").Gte(1)).Order(goqu.C("id").Desc()).Limit(1)
	found, err = ds.ScanStruct(&e)
	mt.NoError(err)
	mt.True(found)
	mt.Equal(uint32(10), e.ID)

	ds = mt.db.From("entry").Prepared(true).Where(goqu.C("id").Gte(1)).Order(goqu.C("id").Asc()).Offset(1).Limit(1)
	found, err = ds.ScanStruct(&e)
	mt.NoError(err)
	mt.True(found)
	mt.Equal(uint32(2), e.ID)
}

func (mt *sqlserverTest) TestInsert() {
	ds := mt.db.From("entry")
	now := time.Now()
	_, err := ds.Insert().Rows(goqu.Record{
		"Int":    10,
		"Float":  1.00000,
		"String": "1.000000",
		"Time":   now,
		"Bool":   true,
		"Bytes":  goqu.Cast(goqu.V([]byte("1.000000")), "BINARY(8)"),
	}).Executor().Exec()
	mt.NoError(err)

	var insertedEntry entry
	found, err := ds.Where(goqu.C("int").Eq(10)).ScanStruct(&insertedEntry)
	mt.NoError(err)
	mt.True(found)
	mt.True(insertedEntry.ID > 0)

	entries := []goqu.Record{
		{
			"Int": 11, "Float": 1.100000, "String": "1.100000", "Time": now,
			"Bool": false, "Bytes": goqu.Cast(goqu.V([]byte("1.100000")), "BINARY(8)"),
		},
		{
			"Int": 12, "Float": 1.200000, "String": "1.200000", "Time": now,
			"Bool": true, "Bytes": goqu.Cast(goqu.V([]byte("1.200000")), "BINARY(8)"),
		},
		{
			"Int": 13, "Float": 1.300000, "String": "1.300000", "Time": now,
			"Bool": false, "Bytes": goqu.Cast(goqu.V([]byte("1.300000")), "BINARY(8)"),
		},
		{
			"Int": 14, "Float": 1.400000, "String": "1.400000", "Time": now,
			"Bool": true, "Bytes": goqu.Cast(goqu.V([]byte("1.400000")), "BINARY(8)"),
		},
	}
	_, err = ds.Insert().Rows(entries).Executor().Exec()
	mt.NoError(err)

	var newEntries []entry
	mt.NoError(ds.Where(goqu.C("int").In([]uint32{11, 12, 13, 14})).ScanStructs(&newEntries))
	mt.Len(newEntries, 4)
	for i, e := range newEntries {
		mt.Equal(entries[i]["Int"], e.Int)
		mt.Equal(entries[i]["Float"], e.Float)
		mt.Equal(entries[i]["String"], e.String)
		mt.Equal(
			entries[i]["Time"].(time.Time).UTC().Format(mysql.DialectOptions().TimeFormat),
			e.Time.Format(mysql.DialectOptions().TimeFormat),
		)
		mt.Equal(entries[i]["Bool"], e.Bool)
		mt.Equal([]byte(entries[i]["String"].(string)), e.Bytes)
	}

	_, err = ds.Insert().Rows(
		goqu.Record{
			"Int": 15, "Float": 1.500000, "String": "1.500000", "Time": now,
			"Bool": false, "Bytes": goqu.Cast(goqu.V([]byte("1.500000")), "BINARY(8)"),
		},
		goqu.Record{
			"Int": 16, "Float": 1.600000, "String": "1.600000", "Time": now,
			"Bool": true, "Bytes": goqu.Cast(goqu.V([]byte("1.600000")), "BINARY(8)"),
		},
		goqu.Record{
			"Int": 17, "Float": 1.700000, "String": "1.700000", "Time": now,
			"Bool": false, "Bytes": goqu.Cast(goqu.V([]byte("1.700000")), "BINARY(8)"),
		},
		goqu.Record{
			"Int": 18, "Float": 1.800000, "String": "1.800000", "Time": now,
			"Bool": true, "Bytes": goqu.Cast(goqu.V([]byte("1.800000")), "BINARY(8)"),
		},
	).Executor().Exec()
	mt.NoError(err)

	newEntries = newEntries[0:0]
	mt.NoError(ds.Where(goqu.C("int").In([]uint32{15, 16, 17, 18})).ScanStructs(&newEntries))
	mt.Len(newEntries, 4)
}

func (mt *sqlserverTest) TestInsertReturningProducesError() {
	ds := mt.db.From("entry")
	now := time.Now()
	e := entry{Int: 10, Float: 1.000000, String: "1.000000", Time: now, Bool: true, Bytes: []byte("1.000000")}
	_, err := ds.Insert().Rows(e).Returning(goqu.Star()).Executor().ScanStruct(&e)
	mt.Error(err)
}

func (mt *sqlserverTest) TestUpdate() {
	ds := mt.db.From("entry")
	var e entry
	found, err := ds.Where(goqu.C("int").Eq(9)).Select("id").ScanStruct(&e)
	mt.NoError(err)
	mt.True(found)
	e.Int = 11
	_, err = ds.Where(goqu.C("id").Eq(e.ID)).Update().Set(goqu.Record{"Int": e.Int}).Executor().Exec()
	mt.NoError(err)

	count, err := ds.Where(goqu.C("int").Eq(11)).Count()
	mt.NoError(err)
	mt.Equal(int64(1), count)
}

func (mt *sqlserverTest) TestUpdateReturning() {
	ds := mt.db.From("entry")
	var id uint32
	_, err := ds.Where(goqu.C("int").Eq(11)).
		Update().
		Set(goqu.Record{"int": 9}).
		Returning("id").
		Executor().ScanVal(&id)
	mt.Error(err)
	mt.EqualError(err, "goqu: dialect does not support RETURNING clause [dialect=sqlserver]")
}

func (mt *sqlserverTest) TestDelete() {
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
	mt.EqualError(err, "goqu: dialect does not support RETURNING clause [dialect=sqlserver]")
}

func (mt *sqlserverTest) TestInsertIgnoreNotSupported() {
	ds := mt.db.From("entry")
	now := time.Now()

	// insert one
	entries := []goqu.Record{
		{
			"Int": 8, "Float": 6.100000, "String": "6.100000", "Bool": false, "Time": now,
			"Bytes": goqu.Cast(goqu.V([]byte("6.100000")), "BINARY(8)"),
		},
		{
			"Int": 9, "Float": 7.200000, "String": "7.200000", "Bool": false, "Time": now,
			"Bytes": goqu.Cast(goqu.V([]byte("7.200000")), "BINARY(8)"),
		},
		{
			"Int": 10, "Float": 7.200000, "String": "7.200000", "Bool": false, "Time": now,
			"Bytes": goqu.Cast(goqu.V([]byte("7.200000")), "BINARY(8)"),
		},
	}
	_, err := ds.Insert().Rows(entries).OnConflict(goqu.DoNothing()).Executor().Exec()
	mt.Error(err)
	mt.Contains(err.Error(), "Cannot insert duplicate key in object 'dbo.entry'. The duplicate key value is (8)")

	count, err := ds.Count()
	mt.NoError(err)
	mt.Equal(count, int64(10))
}

func TestSqlServerSuite(t *testing.T) {
	suite.Run(t, new(sqlserverTest))
}
