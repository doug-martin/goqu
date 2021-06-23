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

const defaultDBURI = "sqlserver://sa:qwe123QWE@127.0.0.1:1433?database=master&connection+timeout=30"

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
	entryTestCase struct {
		ds    *goqu.SelectDataset
		len   int
		check func(entry entry, index int)
		err   string
	}
)

func (sst *sqlserverTest) assertEntries(cases ...entryTestCase) {
	for i, c := range cases {
		var entries []entry
		err := c.ds.ScanStructs(&entries)
		if c.err == "" {
			sst.NoError(err, "test case %d failed", i)
		} else {
			sst.EqualError(err, c.err, "test case %d failed", i)
		}
		sst.Len(entries, c.len)
		for index, entry := range entries {
			c.check(entry, index)
		}
	}
}

func (sst *sqlserverTest) SetupSuite() {
	dbURI := os.Getenv("SQLSERVER_URI")
	if dbURI == "" {
		dbURI = defaultDBURI
	}
	db, err := sql.Open("sqlserver", dbURI)
	if err != nil {
		panic(err.Error())
	}
	sst.db = goqu.New("sqlserver", db)
}

func (sst *sqlserverTest) SetupTest() {
	if _, err := sst.db.Exec(dropTable); err != nil {
		panic(err)
	}
	if _, err := sst.db.Exec(createTable); err != nil {
		panic(err)
	}
	if _, err := sst.db.Exec(insertDefaultRecords); err != nil {
		panic(err)
	}
}

func (sst *sqlserverTest) TestToSQL() {
	ds := sst.db.From("entry")
	s, _, err := ds.Select("id", "float", "string", "time", "bool").ToSQL()
	sst.NoError(err)
	sst.Equal("SELECT \"id\", \"float\", \"string\", \"time\", \"bool\" FROM \"entry\"", s)

	s, _, err = ds.Where(goqu.C("int").Eq(10)).ToSQL()
	sst.NoError(err)
	sst.Equal("SELECT * FROM \"entry\" WHERE (\"int\" = 10)", s)

	s, args, err := ds.Prepared(true).Where(goqu.L("? = ?", goqu.C("int"), 10)).ToSQL()
	sst.NoError(err)
	sst.Equal([]interface{}{int64(10)}, args)
	sst.Equal("SELECT * FROM \"entry\" WHERE \"int\" = @p1", s)
}

func (sst *sqlserverTest) TestQuery() {
	ds := sst.db.From("entry")
	floatVal := float64(0)
	baseDate, err := time.Parse(
		"2006-01-02 15:04:05",
		"2015-02-22 18:19:55",
	)
	sst.NoError(err)
	sst.assertEntries(
		entryTestCase{ds: ds.Order(goqu.C("id").Asc()), len: 10, check: func(entry entry, index int) {
			f := fmt.Sprintf("%f", floatVal)
			sst.Equal(uint32(index+1), entry.ID)
			sst.Equal(index, entry.Int)
			sst.Equal(f, fmt.Sprintf("%f", entry.Float))
			sst.Equal(f, entry.String)
			sst.Equal([]byte(f), entry.Bytes)
			sst.Equal(index%2 == 0, entry.Bool)
			sst.Equal(baseDate.Add(time.Duration(index)*time.Hour).Unix(), entry.Time.Unix())
			floatVal += float64(0.1)
		}},
		entryTestCase{
			ds:  ds.Where(goqu.C("bool").IsTrue()).Order(goqu.C("id").Asc()),
			err: "goqu: boolean data type is not supported by dialect \"sqlserver\"",
		},
		entryTestCase{ds: ds.Where(goqu.C("int").Gt(4)).Order(goqu.C("id").Asc()), len: 5, check: func(entry entry, _ int) {
			sst.True(entry.Int > 4)
		}},
		entryTestCase{ds: ds.Where(goqu.C("int").Gte(5)).Order(goqu.C("id").Asc()), len: 5, check: func(entry entry, _ int) {
			sst.True(entry.Int >= 5)
		}},
		entryTestCase{ds: ds.Where(goqu.C("int").Lt(5)).Order(goqu.C("id").Asc()), len: 5, check: func(entry entry, _ int) {
			sst.True(entry.Int < 5)
		}},
		entryTestCase{ds: ds.Where(goqu.C("int").Lte(4)).Order(goqu.C("id").Asc()), len: 5, check: func(entry entry, _ int) {
			sst.True(entry.Int <= 4)
		}},
		entryTestCase{ds: ds.Where(goqu.C("int").Between(goqu.Range(3, 6))).Order(goqu.C("id").Asc()), len: 4, check: func(entry entry, _ int) {
			sst.True(entry.Int >= 3)
			sst.True(entry.Int <= 6)
		}},
		entryTestCase{ds: ds.Where(goqu.C("string").Eq("0.100000")).Order(goqu.C("id").Asc()), len: 1, check: func(entry entry, _ int) {
			sst.Equal(entry.String, "0.100000")
		}},
		entryTestCase{ds: ds.Where(goqu.C("string").Like("0.1%")).Order(goqu.C("id").Asc()), len: 1, check: func(entry entry, _ int) {
			sst.Equal(entry.String, "0.100000")
		}},
		entryTestCase{ds: ds.Where(goqu.C("string").NotLike("0.1%")).Order(goqu.C("id").Asc()), len: 9, check: func(entry entry, _ int) {
			sst.NotEqual(entry.String, "0.100000")
		}},
		entryTestCase{ds: ds.Where(goqu.C("string").IsNull()).Order(goqu.C("id").Asc()), len: 0, check: func(entry entry, _ int) {
			sst.Fail("Should not have returned any records")
		}},
	)
}

func (sst *sqlserverTest) TestQuery_Prepared() {
	ds := sst.db.From("entry").Prepared(true)
	floatVal := float64(0)
	baseDate, err := time.Parse(
		"2006-01-02 15:04:05",
		"2015-02-22 18:19:55",
	)
	sst.NoError(err)
	sst.assertEntries(
		entryTestCase{ds: ds.Order(goqu.C("id").Asc()), len: 10, check: func(entry entry, index int) {
			f := fmt.Sprintf("%f", floatVal)
			sst.Equal(uint32(index+1), entry.ID)
			sst.Equal(index, entry.Int)
			sst.Equal(f, fmt.Sprintf("%f", entry.Float))
			sst.Equal(f, entry.String)
			sst.Equal([]byte(f), entry.Bytes)
			sst.Equal(index%2 == 0, entry.Bool)
			sst.Equal(baseDate.Add(time.Duration(index)*time.Hour).Unix(), entry.Time.Unix())
			floatVal += float64(0.1)
		}},
		entryTestCase{
			ds:  ds.Where(goqu.C("bool").IsTrue()).Order(goqu.C("id").Asc()),
			err: "goqu: boolean data type is not supported by dialect \"sqlserver\"",
		},
		entryTestCase{ds: ds.Where(goqu.C("int").Gt(4)).Order(goqu.C("id").Asc()), len: 5, check: func(entry entry, _ int) {
			sst.True(entry.Int > 4)
		}},
		entryTestCase{ds: ds.Where(goqu.C("int").Gte(5)).Order(goqu.C("id").Asc()), len: 5, check: func(entry entry, _ int) {
			sst.True(entry.Int >= 5)
		}},
		entryTestCase{ds: ds.Where(goqu.C("int").Lt(5)).Order(goqu.C("id").Asc()), len: 5, check: func(entry entry, _ int) {
			sst.True(entry.Int < 5)
		}},
		entryTestCase{ds: ds.Where(goqu.C("int").Lte(4)).Order(goqu.C("id").Asc()), len: 5, check: func(entry entry, _ int) {
			sst.True(entry.Int <= 4)
		}},
		entryTestCase{ds: ds.Where(goqu.C("int").Between(goqu.Range(3, 6))).Order(goqu.C("id").Asc()), len: 4, check: func(entry entry, _ int) {
			sst.True(entry.Int >= 3)
			sst.True(entry.Int <= 6)
		}},
		entryTestCase{ds: ds.Where(goqu.C("string").Eq("0.100000")).Order(goqu.C("id").Asc()), len: 1, check: func(entry entry, _ int) {
			sst.Equal(entry.String, "0.100000")
		}},
		entryTestCase{ds: ds.Where(goqu.C("string").Like("0.1%")).Order(goqu.C("id").Asc()), len: 1, check: func(entry entry, _ int) {
			sst.Equal(entry.String, "0.100000")
		}},
		entryTestCase{ds: ds.Where(goqu.C("string").NotLike("0.1%")).Order(goqu.C("id").Asc()), len: 9, check: func(entry entry, _ int) {
			sst.NotEqual(entry.String, "0.100000")
		}},
		entryTestCase{ds: ds.Where(goqu.C("string").IsNull()).Order(goqu.C("id").Asc()), len: 0, check: func(entry entry, _ int) {
			sst.Fail("Should not have returned any records")
		}},
	)
}

func (sst *sqlserverTest) TestQuery_ValueExpressions() {
	type wrappedEntry struct {
		entry
		BoolValue bool `db:"bool_value"`
	}
	expectedDate, err := time.Parse("2006-01-02 15:04:05", "2015-02-22 19:19:55")
	sst.NoError(err)
	ds := sst.db.From("entry").Select(goqu.Star(), goqu.V(true).As("bool_value")).Where(goqu.Ex{"int": 1})
	var we wrappedEntry
	found, err := ds.ScanStruct(&we)
	sst.NoError(err)
	sst.True(found)
	sst.Equal(wrappedEntry{
		entry{2, 1, 0.100000, "0.100000", expectedDate, false, []byte("0.100000")},
		true,
	}, we)
}

func (sst *sqlserverTest) TestCount() {
	ds := sst.db.From("entry")
	count, err := ds.Count()
	sst.NoError(err)
	sst.Equal(int64(10), count)
	count, err = ds.Where(goqu.C("int").Gt(4)).Count()
	sst.NoError(err)
	sst.Equal(int64(5), count)
	count, err = ds.Where(goqu.C("int").Gte(4)).Count()
	sst.NoError(err)
	sst.Equal(int64(6), count)
	count, err = ds.Where(goqu.C("string").Like("0.1%")).Count()
	sst.NoError(err)
	sst.Equal(int64(1), count)
	count, err = ds.Where(goqu.C("string").IsNull()).Count()
	sst.NoError(err)
	sst.Equal(int64(0), count)
}

func (sst *sqlserverTest) TestLimitOffset() {
	ds := sst.db.From("entry").Where(goqu.C("id").Gte(1)).Limit(1)
	var e entry
	found, err := ds.ScanStruct(&e)
	sst.NoError(err)
	sst.True(found)
	sst.Equal(uint32(1), e.ID)

	ds = sst.db.From("entry").Where(goqu.C("id").Gte(1)).Order(goqu.C("id").Desc()).Limit(1)
	found, err = ds.ScanStruct(&e)
	sst.NoError(err)
	sst.True(found)
	sst.Equal(uint32(10), e.ID)

	ds = sst.db.From("entry").Where(goqu.C("id").Gte(1)).Order(goqu.C("id").Asc()).Offset(1).Limit(1)
	found, err = ds.ScanStruct(&e)
	sst.NoError(err)
	sst.True(found)
	sst.Equal(uint32(2), e.ID)
}

func (sst *sqlserverTest) TestLimitOffsetParameterized() {
	ds := sst.db.From("entry").Prepared(true).Where(goqu.C("id").Gte(1)).Limit(1)
	var e entry
	found, err := ds.ScanStruct(&e)
	sst.NoError(err)
	sst.True(found)
	sst.Equal(uint32(1), e.ID)

	ds = sst.db.From("entry").Prepared(true).Where(goqu.C("id").Gte(1)).Order(goqu.C("id").Desc()).Limit(1)
	found, err = ds.ScanStruct(&e)
	sst.NoError(err)
	sst.True(found)
	sst.Equal(uint32(10), e.ID)

	ds = sst.db.From("entry").Prepared(true).Where(goqu.C("id").Gte(1)).Order(goqu.C("id").Asc()).Offset(1).Limit(1)
	found, err = ds.ScanStruct(&e)
	sst.NoError(err)
	sst.True(found)
	sst.Equal(uint32(2), e.ID)
}

func (sst *sqlserverTest) TestInsert() {
	ds := sst.db.From("entry")
	now := time.Now()
	_, err := ds.Insert().Rows(goqu.Record{
		"Int":    10,
		"Float":  1.00000,
		"String": "1.000000",
		"Time":   now,
		"Bool":   true,
		"Bytes":  goqu.Cast(goqu.V([]byte("1.000000")), "BINARY(8)"),
	}).Executor().Exec()
	sst.NoError(err)

	var insertedEntry entry
	found, err := ds.Where(goqu.C("int").Eq(10)).ScanStruct(&insertedEntry)
	sst.NoError(err)
	sst.True(found)
	sst.True(insertedEntry.ID > 0)

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
	sst.NoError(err)

	var newEntries []entry
	sst.NoError(ds.Where(goqu.C("int").In([]uint32{11, 12, 13, 14})).ScanStructs(&newEntries))
	sst.Len(newEntries, 4)
	for i, e := range newEntries {
		sst.Equal(entries[i]["Int"], e.Int)
		sst.Equal(entries[i]["Float"], e.Float)
		sst.Equal(entries[i]["String"], e.String)
		sst.Equal(
			entries[i]["Time"].(time.Time).UTC().Format(mysql.DialectOptions().TimeFormat),
			e.Time.Format(mysql.DialectOptions().TimeFormat),
		)
		sst.Equal(entries[i]["Bool"], e.Bool)
		sst.Equal([]byte(entries[i]["String"].(string)), e.Bytes)
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
	sst.NoError(err)

	newEntries = newEntries[0:0]
	sst.NoError(ds.Where(goqu.C("int").In([]uint32{15, 16, 17, 18})).ScanStructs(&newEntries))
	sst.Len(newEntries, 4)
}

func (sst *sqlserverTest) TestInsertReturningProducesError() {
	ds := sst.db.From("entry")
	now := time.Now()
	e := entry{Int: 10, Float: 1.000000, String: "1.000000", Time: now, Bool: true, Bytes: []byte("1.000000")}
	_, err := ds.Insert().Rows(e).Returning(goqu.Star()).Executor().ScanStruct(&e)
	sst.Error(err)
}

func (sst *sqlserverTest) TestUpdate() {
	ds := sst.db.From("entry")
	var e entry
	found, err := ds.Where(goqu.C("int").Eq(9)).Select("id").ScanStruct(&e)
	sst.NoError(err)
	sst.True(found)
	e.Int = 11
	_, err = ds.Where(goqu.C("id").Eq(e.ID)).Update().Set(goqu.Record{"Int": e.Int}).Executor().Exec()
	sst.NoError(err)

	count, err := ds.Where(goqu.C("int").Eq(11)).Count()
	sst.NoError(err)
	sst.Equal(int64(1), count)
}

func (sst *sqlserverTest) TestUpdateReturning() {
	ds := sst.db.From("entry")
	var id uint32
	_, err := ds.Where(goqu.C("int").Eq(11)).
		Update().
		Set(goqu.Record{"int": 9}).
		Returning("id").
		Executor().ScanVal(&id)
	sst.Error(err)
	sst.EqualError(err, "goqu: dialect does not support RETURNING clause [dialect=sqlserver]")
}

func (sst *sqlserverTest) TestDelete() {
	ds := sst.db.From("entry")
	var e entry
	found, err := ds.Where(goqu.C("int").Eq(9)).Select("id").ScanStruct(&e)
	sst.NoError(err)
	sst.True(found)
	_, err = ds.Where(goqu.C("id").Eq(e.ID)).Delete().Executor().Exec()
	sst.NoError(err)

	count, err := ds.Count()
	sst.NoError(err)
	sst.Equal(int64(9), count)

	var id uint32
	found, err = ds.Where(goqu.C("id").Eq(e.ID)).ScanVal(&id)
	sst.NoError(err)
	sst.False(found)

	e = entry{}
	found, err = ds.Where(goqu.C("int").Eq(8)).Select("id").ScanStruct(&e)
	sst.NoError(err)
	sst.True(found)
	sst.NotEqual(0, e.ID)

	id = 0
	_, err = ds.Where(goqu.C("id").Eq(e.ID)).Delete().Returning("id").Executor().ScanVal(&id)
	sst.EqualError(err, "goqu: dialect does not support RETURNING clause [dialect=sqlserver]")
}

func (sst *sqlserverTest) TestInsertIgnoreNotSupported() {
	ds := sst.db.From("entry")
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
	sst.Error(err)
	sst.Contains(err.Error(), "Cannot insert duplicate key in object 'dbo.entry'. The duplicate key value is (8)")

	count, err := ds.Count()
	sst.NoError(err)
	sst.Equal(count, int64(10))
}

func TestSqlServerSuite(t *testing.T) {
	suite.Run(t, new(sqlserverTest))
}
