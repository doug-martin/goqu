package postgres

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/doug-martin/goqu/v8"

	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

const schema = `
        DROP TABLE IF EXISTS "entry";
        CREATE  TABLE "entry" (
            "id" SERIAL PRIMARY KEY NOT NULL,
            "int" INT NOT NULL UNIQUE,
            "float" NUMERIC NOT NULL ,
            "string" VARCHAR(45) NOT NULL ,
            "time" TIMESTAMP NOT NULL ,
            "bool" BOOL NOT NULL ,
            "bytes" VARCHAR(45) NOT NULL);
        INSERT INTO "entry" ("int", "float", "string", "time", "bool", "bytes") VALUES
            (0, 0.000000, '0.000000', '2015-02-22T18:19:55.000000000-00:00', TRUE,  '0.000000'),
            (1, 0.100000, '0.100000', '2015-02-22T19:19:55.000000000-00:00', FALSE, '0.100000'),
            (2, 0.200000, '0.200000', '2015-02-22T20:19:55.000000000-00:00', TRUE,  '0.200000'),
            (3, 0.300000, '0.300000', '2015-02-22T21:19:55.000000000-00:00', FALSE, '0.300000'),
            (4, 0.400000, '0.400000', '2015-02-22T22:19:55.000000000-00:00', TRUE,  '0.400000'),
            (5, 0.500000, '0.500000', '2015-02-22T23:19:55.000000000-00:00', FALSE, '0.500000'),
            (6, 0.600000, '0.600000', '2015-02-23T00:19:55.000000000-00:00', TRUE,  '0.600000'),
            (7, 0.700000, '0.700000', '2015-02-23T01:19:55.000000000-00:00', FALSE, '0.700000'),
            (8, 0.800000, '0.800000', '2015-02-23T02:19:55.000000000-00:00', TRUE,  '0.800000'),
            (9, 0.900000, '0.900000', '2015-02-23T03:19:55.000000000-00:00', FALSE, '0.900000');
    `

const defaultDbURI = "postgres://postgres:@localhost:5435/goqupostgres?sslmode=disable"

type (
	postgresTest struct {
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

func (pt *postgresTest) SetupSuite() {
	dbURI := os.Getenv("PG_URI")
	if dbURI == "" {
		dbURI = defaultDbURI
	}
	uri, err := pq.ParseURL(dbURI)
	if err != nil {
		panic(err)
	}
	db, err := sql.Open("postgres", uri)
	if err != nil {
		panic(err)
	}
	pt.db = goqu.New("postgres", db)
}

func (pt *postgresTest) SetupTest() {
	if _, err := pt.db.Exec(schema); err != nil {
		panic(err)
	}
}

func (pt *postgresTest) TestToSQL() {
	t := pt.T()
	ds := pt.db.From("entry")
	s, _, err := ds.Select("id", "float", "string", "time", "bool").ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, s, `SELECT "id", "float", "string", "time", "bool" FROM "entry"`)

	s, _, err = ds.Where(goqu.C("int").Eq(10)).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, s, `SELECT * FROM "entry" WHERE ("int" = 10)`)

	s, args, err := ds.Prepared(true).Where(goqu.L("? = ?", goqu.C("int"), 10)).ToSQL()
	assert.NoError(t, err)
	assert.Equal(t, args, []interface{}{int64(10)})
	assert.Equal(t, s, `SELECT * FROM "entry" WHERE "int" = $1`)
}

func (pt *postgresTest) TestQuery() {
	t := pt.T()
	var entries []entry
	ds := pt.db.From("entry")
	assert.NoError(t, ds.Order(goqu.C("id").Asc()).ScanStructs(&entries))
	assert.Len(t, entries, 10)
	floatVal := float64(0)
	baseDate, err := time.Parse(time.RFC3339Nano, "2015-02-22T18:19:55.000000000-00:00")
	assert.NoError(t, err)
	baseDate = baseDate.UTC()
	for i, entry := range entries {
		f := fmt.Sprintf("%f", floatVal)
		assert.Equal(t, entry.ID, uint32(i+1))
		assert.Equal(t, entry.Int, i)
		assert.Equal(t, fmt.Sprintf("%f", entry.Float), f)
		assert.Equal(t, entry.String, f)
		assert.Equal(t, entry.Bytes, []byte(f))
		assert.Equal(t, entry.Bool, i%2 == 0)
		assert.Equal(t, entry.Time.Unix(), baseDate.Add(time.Duration(i)*time.Hour).Unix())
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

func (pt *postgresTest) TestCount() {
	t := pt.T()
	ds := pt.db.From("entry")
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

func (pt *postgresTest) TestInsert() {
	t := pt.T()
	ds := pt.db.From("entry")
	now := time.Now()
	e := entry{Int: 10, Float: 1.000000, String: "1.000000", Time: now, Bool: true, Bytes: []byte("1.000000")}
	_, err := ds.Insert().Rows(e).Executor().Exec()
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
	_, err = ds.Insert().Rows(entries).Executor().Exec()
	assert.NoError(t, err)

	var newEntries []entry
	assert.NoError(t, ds.Where(goqu.C("int").In([]uint32{11, 12, 13, 14})).ScanStructs(&newEntries))
	assert.Len(t, newEntries, 4)

	_, err = ds.Insert().Rows(
		entry{Int: 15, Float: 1.500000, String: "1.500000", Time: now, Bool: false, Bytes: []byte("1.500000")},
		entry{Int: 16, Float: 1.600000, String: "1.600000", Time: now, Bool: true, Bytes: []byte("1.600000")},
		entry{Int: 17, Float: 1.700000, String: "1.700000", Time: now, Bool: false, Bytes: []byte("1.700000")},
		entry{Int: 18, Float: 1.800000, String: "1.800000", Time: now, Bool: true, Bytes: []byte("1.800000")},
	).Executor().Exec()
	assert.NoError(t, err)

	newEntries = newEntries[0:0]
	assert.NoError(t, ds.Where(goqu.C("int").In([]uint32{15, 16, 17, 18})).ScanStructs(&newEntries))
	assert.Len(t, newEntries, 4)
}

func (pt *postgresTest) TestInsertReturning() {
	t := pt.T()
	ds := pt.db.From("entry")
	now := time.Now()
	e := entry{Int: 10, Float: 1.000000, String: "1.000000", Time: now, Bool: true, Bytes: []byte("1.000000")}
	found, err := ds.Insert().Rows(e).Returning(goqu.Star()).Executor().ScanStruct(&e)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.True(t, e.ID > 0)

	var ids []uint32
	assert.NoError(t, ds.Insert().Rows([]entry{
		{Int: 11, Float: 1.100000, String: "1.100000", Time: now, Bool: false, Bytes: []byte("1.100000")},
		{Int: 12, Float: 1.200000, String: "1.200000", Time: now, Bool: true, Bytes: []byte("1.200000")},
		{Int: 13, Float: 1.300000, String: "1.300000", Time: now, Bool: false, Bytes: []byte("1.300000")},
		{Int: 14, Float: 1.400000, String: "1.400000", Time: now, Bool: true, Bytes: []byte("1.400000")},
	}).Returning("id").Executor().ScanVals(&ids))
	assert.Len(t, ids, 4)
	for _, id := range ids {
		assert.True(t, id > 0)
	}

	var ints []int64
	assert.NoError(t, ds.Insert().Rows(
		entry{Int: 15, Float: 1.500000, String: "1.500000", Time: now, Bool: false, Bytes: []byte("1.500000")},
		entry{Int: 16, Float: 1.600000, String: "1.600000", Time: now, Bool: true, Bytes: []byte("1.600000")},
		entry{Int: 17, Float: 1.700000, String: "1.700000", Time: now, Bool: false, Bytes: []byte("1.700000")},
		entry{Int: 18, Float: 1.800000, String: "1.800000", Time: now, Bool: true, Bytes: []byte("1.800000")},
	).Returning("int").Executor().ScanVals(&ints))
	assert.True(t, found)
	assert.Equal(t, ints, []int64{15, 16, 17, 18})
}

func (pt *postgresTest) TestUpdate() {
	t := pt.T()
	ds := pt.db.From("entry")
	var e entry
	found, err := ds.Where(goqu.C("int").Eq(9)).Select("id").ScanStruct(&e)
	assert.NoError(t, err)
	assert.True(t, found)
	e.Int = 11
	_, err = ds.Where(goqu.C("id").Eq(e.ID)).Update().Set(e).Executor().Exec()
	assert.NoError(t, err)

	count, err := ds.Where(goqu.C("int").Eq(11)).Count()
	assert.NoError(t, err)
	assert.Equal(t, count, int64(1))

	var id uint32
	found, err = ds.Where(goqu.C("int").Eq(11)).
		Update().
		Set(goqu.Record{"int": 9}).
		Returning("id").Executor().ScanVal(&id)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, id, e.ID)
}

func (pt *postgresTest) TestUpdateSQL_multipleTables() {
	ds := pt.db.Update("test")
	updateSQL, _, err := ds.
		Set(goqu.Record{"foo": "bar"}).
		From("test_2").
		Where(goqu.I("test.id").Eq(goqu.I("test_2.test_id"))).
		ToSQL()
	pt.NoError(err)
	pt.Equal(`UPDATE "test" SET "foo"='bar' FROM "test_2" WHERE ("test"."id" = "test_2"."test_id")`, updateSQL)
}

func (pt *postgresTest) TestDelete() {
	t := pt.T()
	ds := pt.db.From("entry")
	var e entry
	found, err := ds.Where(goqu.C("int").Eq(9)).Select("id").ScanStruct(&e)
	assert.NoError(t, err)
	assert.True(t, found)
	_, err = ds.Where(goqu.C("id").Eq(e.ID)).Delete().Executor().Exec()
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
	assert.NotEqual(t, e.ID, int64(0))

	id = 0
	_, err = ds.Where(goqu.C("id").Eq(e.ID)).Delete().Returning("id").Executor().ScanVal(&id)
	assert.NoError(t, err)
	assert.Equal(t, id, e.ID)
}

func (pt *postgresTest) TestInsert_OnConflict() {
	t := pt.T()
	ds := pt.db.From("entry")
	now := time.Now()

	// DO NOTHING insert
	e := entry{Int: 10, Float: 1.100000, String: "1.100000", Time: now, Bool: false, Bytes: []byte("1.100000")}
	_, err := ds.Insert().Rows(e).OnConflict(goqu.DoNothing()).Executor().Exec()
	assert.NoError(t, err)

	// DO NOTHING duplicate
	e = entry{Int: 10, Float: 2.100000, String: "2.100000", Time: now.Add(time.Hour * 100), Bool: false, Bytes: []byte("2.100000")}
	_, err = ds.Insert().Rows(e).OnConflict(goqu.DoNothing()).Executor().Exec()
	assert.NoError(t, err)

	// DO NOTHING update
	var entryActual entry
	e2 := entry{Int: 0, String: "2.000000"}
	_, err = ds.Insert().
		Rows(e2).
		OnConflict(goqu.DoUpdate("int", goqu.Record{"string": "upsert"})).
		Executor().Exec()
	assert.NoError(t, err)
	_, err = ds.Where(goqu.C("int").Eq(0)).ScanStruct(&entryActual)
	assert.NoError(t, err)
	assert.Equal(t, "upsert", entryActual.String)

	// DO NOTHING update where
	entries := []entry{
		{Int: 1, Float: 6.100000, String: "6.100000", Time: now, Bytes: []byte("6.100000")},
		{Int: 2, Float: 7.200000, String: "7.200000", Time: now, Bytes: []byte("7.200000")},
	}
	_, err = ds.Insert().
		Rows(entries).
		OnConflict(goqu.DoUpdate("int", goqu.Record{"string": "upsert"}).Where(goqu.I("excluded.int").Eq(2))).
		Executor().
		Exec()
	assert.NoError(t, err)

	var entry8, entry9 entry
	_, err = ds.Where(goqu.Ex{"int": 1}).ScanStruct(&entry8)
	assert.NoError(t, err)
	assert.Equal(t, "0.100000", entry8.String)

	_, err = ds.Where(goqu.Ex{"int": 2}).ScanStruct(&entry9)
	assert.NoError(t, err)
	assert.Equal(t, "upsert", entry9.String)
}

func TestPostgresSuite(t *testing.T) {
	suite.Run(t, new(postgresTest))
}
