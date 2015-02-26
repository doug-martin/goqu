package postgres

import (
	"database/sql"
	"fmt"
	"github.com/doug-martin/gql"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
)

const schema = `
        DROP TABLE IF EXISTS "entry";
        CREATE  TABLE "entry" (
            "id" SERIAL PRIMARY KEY NOT NULL,
            "int" INT NOT NULL ,
            "float" NUMERIC NOT NULL ,
            "string" VARCHAR(45) NOT NULL ,
            "time" TIMESTAMP NOT NULL ,
            "bool" BOOL NOT NULL ,
            "bytes" VARCHAR(45) NOT NULL);
        INSERT INTO "entry" ("int", "float", "string", "time", "bool", "bytes") VALUES
            (0, 0.000000, '0.000000', '2015-02-22T18:19:55.000000000-06:00', TRUE,  '0.000000'),
            (1, 0.100000, '0.100000', '2015-02-22T19:19:55.000000000-06:00', FALSE, '0.100000'),
            (2, 0.200000, '0.200000', '2015-02-22T20:19:55.000000000-06:00', TRUE,  '0.200000'),
            (3, 0.300000, '0.300000', '2015-02-22T21:19:55.000000000-06:00', FALSE, '0.300000'),
            (4, 0.400000, '0.400000', '2015-02-22T22:19:55.000000000-06:00', TRUE,  '0.400000'),
            (5, 0.500000, '0.500000', '2015-02-22T23:19:55.000000000-06:00', FALSE, '0.500000'),
            (6, 0.600000, '0.600000', '2015-02-23T00:19:55.000000000-06:00', TRUE,  '0.600000'),
            (7, 0.700000, '0.700000', '2015-02-23T01:19:55.000000000-06:00', FALSE, '0.700000'),
            (8, 0.800000, '0.800000', '2015-02-23T02:19:55.000000000-06:00', TRUE,  '0.800000'),
            (9, 0.900000, '0.900000', '2015-02-23T03:19:55.000000000-06:00', FALSE, '0.900000');
    `

type (
	logger       struct{}
	postgresTest struct {
		suite.Suite
		db gql.Database
	}
	entry struct {
		Id     uint32    `db:"id" gql:"skipinsert,skipupdate"`
		Int    int       `db:"int"`
		Float  float64   `db:"float"`
		String string    `db:"string"`
		Time   time.Time `db:"time"`
		Bool   bool      `db:"bool"`
		Bytes  []byte    `db:"bytes"`
	}
)

func (me logger) Printf(sql string, args ...interface{}) {
	fmt.Printf("\n"+sql, args)
}

func (me *postgresTest) SetupSuite() {
	db, err := sql.Open("postgres", "user=postgres dbname=gqlpostgres sslmode=disable ")
	if err != nil {
		panic(err)
	}
	me.db = gql.New("postgres", db)
	//	me.db.Logger(logger{})
}

func (me *postgresTest) SetupTest() {
	if _, err := me.db.Exec(schema); err != nil {
		panic(err)
	}
}

func (me *postgresTest) TestSelectSql() {
	t := me.T()
	ds := me.db.From("entry")
	sql, err := ds.Select("id", "float", "string", "time", "bool").Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT "id", "float", "string", "time", "bool" FROM "entry"`)

	sql, err = ds.Where(gql.I("int").Eq(10)).Sql()
	assert.NoError(t, err)
	assert.Equal(t, sql, `SELECT * FROM "entry" WHERE ("int" = 10)`)

	sql, args, err := ds.Where(gql.L("? = ?", gql.I("int"), 10)).ToSql(true)
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{10})
	assert.Equal(t, sql, `SELECT * FROM "entry" WHERE "int" = $1`)
}

func (me *postgresTest) TestQuery() {
	t := me.T()
	var entries []entry
	ds := me.db.From("entry")
	assert.NoError(t, ds.Order(gql.I("id").Asc()).ScanStructs(&entries))
	assert.Len(t, entries, 10)
	floatVal := float64(0)
	baseDate, err := time.Parse(time.RFC3339Nano, "2015-02-22T18:19:55.000000000-00:00")
	assert.NoError(t, err)
	for i, entry := range entries {
		f := fmt.Sprintf("%f", floatVal)
		assert.Equal(t, entry.Id, uint32(i+1))
		assert.Equal(t, entry.Int, i)
		assert.Equal(t, fmt.Sprintf("%f", entry.Float), f)
		assert.Equal(t, entry.String, f)
		assert.Equal(t, entry.Bytes, []byte(f))
		assert.Equal(t, entry.Bool, i%2 == 0)
		assert.Equal(t, entry.Time, baseDate.Add(time.Duration(i)*time.Hour))
		floatVal += float64(0.1)
	}
	entries = entries[0:0]
	assert.NoError(t, ds.Where(gql.I("bool").IsTrue()).Order(gql.I("id").Asc()).ScanStructs(&entries))
	assert.Len(t, entries, 5)
	assert.NoError(t, err)
	for _, entry := range entries {
		assert.True(t, entry.Bool)
	}

	entries = entries[0:0]
	assert.NoError(t, ds.Where(gql.I("int").Gt(4)).Order(gql.I("id").Asc()).ScanStructs(&entries))
	assert.Len(t, entries, 5)
	assert.NoError(t, err)
	for _, entry := range entries {
		assert.True(t, entry.Int > 4)
	}

	entries = entries[0:0]
	assert.NoError(t, ds.Where(gql.I("int").Gte(5)).Order(gql.I("id").Asc()).ScanStructs(&entries))
	assert.Len(t, entries, 5)
	assert.NoError(t, err)
	for _, entry := range entries {
		assert.True(t, entry.Int >= 5)
	}

	entries = entries[0:0]
	assert.NoError(t, ds.Where(gql.I("int").Lt(5)).Order(gql.I("id").Asc()).ScanStructs(&entries))
	assert.Len(t, entries, 5)
	assert.NoError(t, err)
	for _, entry := range entries {
		assert.True(t, entry.Int < 5)
	}

	entries = entries[0:0]
	assert.NoError(t, ds.Where(gql.I("int").Lte(4)).Order(gql.I("id").Asc()).ScanStructs(&entries))
	assert.Len(t, entries, 5)
	assert.NoError(t, err)
	for _, entry := range entries {
		assert.True(t, entry.Int <= 4)
	}

	entries = entries[0:0]
	assert.NoError(t, ds.Where(gql.I("string").Eq("0.100000")).Order(gql.I("id").Asc()).ScanStructs(&entries))
	assert.Len(t, entries, 1)
	assert.NoError(t, err)
	for _, entry := range entries {
		assert.Equal(t, entry.String, "0.100000")
	}

	entries = entries[0:0]
	assert.NoError(t, ds.Where(gql.I("string").Like("0.1%")).Order(gql.I("id").Asc()).ScanStructs(&entries))
	assert.Len(t, entries, 1)
	assert.NoError(t, err)
	for _, entry := range entries {
		assert.Equal(t, entry.String, "0.100000")
	}

	entries = entries[0:0]
	assert.NoError(t, ds.Where(gql.I("string").NotLike("0.1%")).Order(gql.I("id").Asc()).ScanStructs(&entries))
	assert.Len(t, entries, 9)
	assert.NoError(t, err)
	for _, entry := range entries {
		assert.NotEqual(t, entry.String, "0.100000")
	}

	entries = entries[0:0]
	assert.NoError(t, ds.Where(gql.I("string").IsNull()).Order(gql.I("id").Asc()).ScanStructs(&entries))
	assert.Len(t, entries, 0)
}

func (me *postgresTest) TestCount() {
	t := me.T()
	ds := me.db.From("entry")
	count, err := ds.Count()
	assert.NoError(t, err)
	assert.Equal(t, count, 10)
	count, err = ds.Where(gql.I("int").Gt(4)).Count()
	assert.NoError(t, err)
	assert.Equal(t, count, 5)
	count, err = ds.Where(gql.I("int").Gte(4)).Count()
	assert.NoError(t, err)
	assert.Equal(t, count, 6)
	count, err = ds.Where(gql.I("string").Like("0.1%")).Count()
	assert.NoError(t, err)
	assert.Equal(t, count, 1)
	count, err = ds.Where(gql.I("string").IsNull()).Count()
	assert.NoError(t, err)
	assert.Equal(t, count, 0)
}

func (me *postgresTest) TestInsert() {
	t := me.T()
	ds := me.db.From("entry")
	now := time.Now()
	e := entry{Int: 10, Float: 1.000000, String: "1.000000", Time: now, Bool: true, Bytes: []byte("1.000000")}
	_, err := ds.Insert(e).Exec()
	assert.NoError(t, err)

	var insertedEntry entry
	found, err := ds.Where(gql.I("int").Eq(10)).ScanStruct(&insertedEntry)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.True(t, insertedEntry.Id > 0)

	entries := []entry{
		{Int: 11, Float: 1.100000, String: "1.100000", Time: now, Bool: false, Bytes: []byte("1.100000")},
		{Int: 12, Float: 1.200000, String: "1.200000", Time: now, Bool: true, Bytes: []byte("1.200000")},
		{Int: 13, Float: 1.300000, String: "1.300000", Time: now, Bool: false, Bytes: []byte("1.300000")},
		{Int: 14, Float: 1.400000, String: "1.400000", Time: now, Bool: true, Bytes: []byte("1.400000")},
	}
	_, err = ds.Insert(entries).Exec()
	assert.NoError(t, err)

	var newEntries []entry
	assert.NoError(t, ds.Where(gql.I("int").In([]uint32{11, 12, 13, 14})).ScanStructs(&newEntries))
	assert.Len(t, newEntries, 4)

	_, err = ds.Insert(
		entry{Int: 15, Float: 1.500000, String: "1.500000", Time: now, Bool: false, Bytes: []byte("1.500000")},
		entry{Int: 16, Float: 1.600000, String: "1.600000", Time: now, Bool: true, Bytes: []byte("1.600000")},
		entry{Int: 17, Float: 1.700000, String: "1.700000", Time: now, Bool: false, Bytes: []byte("1.700000")},
		entry{Int: 18, Float: 1.800000, String: "1.800000", Time: now, Bool: true, Bytes: []byte("1.800000")},
	).Exec()
	assert.NoError(t, err)

	newEntries = newEntries[0:0]
	assert.NoError(t, ds.Where(gql.I("int").In([]uint32{15, 16, 17, 18})).ScanStructs(&newEntries))
	assert.Len(t, newEntries, 4)
}

func (me *postgresTest) TestInsertReturning() {
	t := me.T()
	ds := me.db.From("entry")
	now := time.Now()
	e := entry{Int: 10, Float: 1.000000, String: "1.000000", Time: now, Bool: true, Bytes: []byte("1.000000")}
	found, err := ds.Returning(gql.Star()).Insert(e).ScanStruct(&e)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.True(t, e.Id > 0)

	var ids []uint32
	assert.NoError(t, ds.Returning("id").Insert([]entry{
		{Int: 11, Float: 1.100000, String: "1.100000", Time: now, Bool: false, Bytes: []byte("1.100000")},
		{Int: 12, Float: 1.200000, String: "1.200000", Time: now, Bool: true, Bytes: []byte("1.200000")},
		{Int: 13, Float: 1.300000, String: "1.300000", Time: now, Bool: false, Bytes: []byte("1.300000")},
		{Int: 14, Float: 1.400000, String: "1.400000", Time: now, Bool: true, Bytes: []byte("1.400000")},
	}).ScanVals(&ids))
	assert.Len(t, ids, 4)
	for _, id := range ids {
		assert.True(t, id > 0)
	}

	var ints []int64
	assert.NoError(t, ds.Returning("int").Insert(
		entry{Int: 15, Float: 1.500000, String: "1.500000", Time: now, Bool: false, Bytes: []byte("1.500000")},
		entry{Int: 16, Float: 1.600000, String: "1.600000", Time: now, Bool: true, Bytes: []byte("1.600000")},
		entry{Int: 17, Float: 1.700000, String: "1.700000", Time: now, Bool: false, Bytes: []byte("1.700000")},
		entry{Int: 18, Float: 1.800000, String: "1.800000", Time: now, Bool: true, Bytes: []byte("1.800000")},
	).ScanVals(&ints))
	assert.True(t, found)
	assert.Equal(t, ints, []int64{15, 16, 17, 18})
}

func (me *postgresTest) TestUpdate() {
	t := me.T()
	ds := me.db.From("entry")
	var e entry
	found, err := ds.Where(gql.I("int").Eq(9)).Select("id").ScanStruct(&e)
	assert.NoError(t, err)
	assert.True(t, found)
	e.Int = 11
	_, err = ds.Where(gql.I("id").Eq(e.Id)).Update(e).Exec()
	assert.NoError(t, err)

	count, err := ds.Where(gql.I("int").Eq(11)).Count()
	assert.NoError(t, err)
	assert.Equal(t, count, 1)

	var id uint32
	found, err = ds.Where(gql.I("int").Eq(11)).Returning("id").Update(map[string]interface{}{"int": 9}).ScanVal(&id)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, id, e.Id)
}

func (me *postgresTest) TestDelete() {
	t := me.T()
	ds := me.db.From("entry")
	var e entry
	found, err := ds.Where(gql.I("int").Eq(9)).Select("id").ScanStruct(&e)
	assert.NoError(t, err)
	assert.True(t, found)
	_, err = ds.Where(gql.I("id").Eq(e.Id)).Delete().Exec()
	assert.NoError(t, err)

	count, err := ds.Count()
	assert.NoError(t, err)
	assert.Equal(t, count, 9)

	var id uint32
	found, err = ds.Where(gql.I("id").Eq(e.Id)).ScanVal(&id)
	assert.NoError(t, err)
	assert.False(t, found)

	e = entry{}
	found, err = ds.Where(gql.I("int").Eq(8)).Select("id").ScanStruct(&e)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.NotEqual(t, e.Id, 0)

	id = 0
	_, err = ds.Where(gql.I("id").Eq(e.Id)).Returning("id").Delete().ScanVal(&id)
	assert.NoError(t, err)
	assert.Equal(t, id, e.Id)
}

func TestPostgresSuite(t *testing.T) {
	suite.Run(t, new(postgresTest))
}
