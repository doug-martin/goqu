package postgres

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/doug-martin/goqu/v9"

	"github.com/lib/pq"
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
	ds := pt.db.From("entry")
	s, _, err := ds.Select("id", "float", "string", "time", "bool").ToSQL()
	pt.NoError(err)
	pt.Equal(`SELECT "id", "float", "string", "time", "bool" FROM "entry"`, s)

	s, _, err = ds.Where(goqu.C("int").Eq(10)).ToSQL()
	pt.NoError(err)
	pt.Equal(`SELECT * FROM "entry" WHERE ("int" = 10)`, s)

	s, args, err := ds.Prepared(true).Where(goqu.L("? = ?", goqu.C("int"), 10)).ToSQL()
	pt.NoError(err)
	pt.Equal([]interface{}{int64(10)}, args)
	pt.Equal(`SELECT * FROM "entry" WHERE "int" = $1`, s)
}

func (pt *postgresTest) TestQuery() {
	var entries []entry
	ds := pt.db.From("entry")
	pt.NoError(ds.Order(goqu.C("id").Asc()).ScanStructs(&entries))
	pt.Len(entries, 10)
	floatVal := float64(0)
	baseDate, err := time.Parse(time.RFC3339Nano, "2015-02-22T18:19:55.000000000-00:00")
	pt.NoError(err)
	baseDate = baseDate.UTC()
	for i, entry := range entries {
		f := fmt.Sprintf("%f", floatVal)
		pt.Equal(uint32(i+1), entry.ID)
		pt.Equal(i, entry.Int)
		pt.Equal(f, fmt.Sprintf("%f", entry.Float))
		pt.Equal(f, entry.String)
		pt.Equal([]byte(f), entry.Bytes)
		pt.Equal(i%2 == 0, entry.Bool)
		pt.Equal(baseDate.Add(time.Duration(i)*time.Hour).Unix(), entry.Time.Unix())
		floatVal += float64(0.1)
	}
	entries = entries[0:0]
	pt.NoError(ds.Where(goqu.C("bool").IsTrue()).Order(goqu.C("id").Asc()).ScanStructs(&entries))
	pt.Len(entries, 5)
	pt.NoError(err)
	for _, entry := range entries {
		pt.True(entry.Bool)
	}

	entries = entries[0:0]
	pt.NoError(ds.Where(goqu.C("int").Gt(4)).Order(goqu.C("id").Asc()).ScanStructs(&entries))
	pt.Len(entries, 5)
	pt.NoError(err)
	for _, entry := range entries {
		pt.True(entry.Int > 4)
	}

	entries = entries[0:0]
	pt.NoError(ds.Where(goqu.C("int").Gte(5)).Order(goqu.C("id").Asc()).ScanStructs(&entries))
	pt.Len(entries, 5)
	pt.NoError(err)
	for _, entry := range entries {
		pt.True(entry.Int >= 5)
	}

	entries = entries[0:0]
	pt.NoError(ds.Where(goqu.C("int").Lt(5)).Order(goqu.C("id").Asc()).ScanStructs(&entries))
	pt.Len(entries, 5)
	pt.NoError(err)
	for _, entry := range entries {
		pt.True(entry.Int < 5)
	}

	entries = entries[0:0]
	pt.NoError(ds.Where(goqu.C("int").Lte(4)).Order(goqu.C("id").Asc()).ScanStructs(&entries))
	pt.Len(entries, 5)
	pt.NoError(err)
	for _, entry := range entries {
		pt.True(entry.Int <= 4)
	}

	entries = entries[0:0]
	pt.NoError(ds.Where(goqu.C("int").Between(goqu.Range(3, 6))).Order(goqu.C("id").Asc()).ScanStructs(&entries))
	pt.Len(entries, 4)
	pt.NoError(err)
	for _, entry := range entries {
		pt.True(entry.Int >= 3)
		pt.True(entry.Int <= 6)
	}

	entries = entries[0:0]
	pt.NoError(ds.Where(goqu.C("string").Eq("0.100000")).Order(goqu.C("id").Asc()).ScanStructs(&entries))
	pt.Len(entries, 1)
	pt.NoError(err)
	for _, entry := range entries {
		pt.Equal(entry.String, "0.100000")
	}

	entries = entries[0:0]
	pt.NoError(ds.Where(goqu.C("string").Like("0.1%")).Order(goqu.C("id").Asc()).ScanStructs(&entries))
	pt.Len(entries, 1)
	pt.NoError(err)
	for _, entry := range entries {
		pt.Equal("0.100000", entry.String)
	}

	entries = entries[0:0]
	pt.NoError(ds.Where(goqu.C("string").NotLike("0.1%")).Order(goqu.C("id").Asc()).ScanStructs(&entries))
	pt.Len(entries, 9)
	pt.NoError(err)
	for _, entry := range entries {
		pt.NotEqual("0.100000", entry.String)
	}

	entries = entries[0:0]
	pt.NoError(ds.Where(goqu.C("string").IsNull()).Order(goqu.C("id").Asc()).ScanStructs(&entries))
	pt.Len(entries, 0)
}

func (pt *postgresTest) TestQuery_ValueExpressions() {
	type wrappedEntry struct {
		entry
		BoolValue bool `db:"bool_value"`
	}
	expectedDate, err := time.Parse(time.RFC3339Nano, "2015-02-22T19:19:55.000000000-00:00")
	pt.NoError(err)
	ds := pt.db.From("entry").Select(goqu.Star(), goqu.V(true).As("bool_value")).Where(goqu.Ex{"int": 1})
	var we wrappedEntry
	found, err := ds.ScanStruct(&we)
	pt.NoError(err)
	pt.True(found)
	pt.Equal(1, we.Int)
	pt.Equal(0.100000, we.Float)
	pt.Equal("0.100000", we.String)
	pt.Equal(expectedDate.Unix(), we.Time.Unix())
	pt.Equal(false, we.Bool)
	pt.Equal([]byte("0.100000"), we.Bytes)
	pt.True(we.BoolValue)
}

func (pt *postgresTest) TestCount() {
	ds := pt.db.From("entry")
	count, err := ds.Count()
	pt.NoError(err)
	pt.Equal(int64(10), count)
	count, err = ds.Where(goqu.C("int").Gt(4)).Count()
	pt.NoError(err)
	pt.Equal(int64(5), count)
	count, err = ds.Where(goqu.C("int").Gte(4)).Count()
	pt.NoError(err)
	pt.Equal(int64(6), count)
	count, err = ds.Where(goqu.C("string").Like("0.1%")).Count()
	pt.NoError(err)
	pt.Equal(int64(1), count)
	count, err = ds.Where(goqu.C("string").IsNull()).Count()
	pt.NoError(err)
	pt.Equal(int64(0), count)
}

func (pt *postgresTest) TestInsert() {
	ds := pt.db.From("entry")
	now := time.Now()
	e := entry{Int: 10, Float: 1.000000, String: "1.000000", Time: now, Bool: true, Bytes: []byte("1.000000")}
	_, err := ds.Insert().Rows(e).Executor().Exec()
	pt.NoError(err)

	var insertedEntry entry
	found, err := ds.Where(goqu.C("int").Eq(10)).ScanStruct(&insertedEntry)
	pt.NoError(err)
	pt.True(found)
	pt.True(insertedEntry.ID > 0)

	entries := []entry{
		{Int: 11, Float: 1.100000, String: "1.100000", Time: now, Bool: false, Bytes: []byte("1.100000")},
		{Int: 12, Float: 1.200000, String: "1.200000", Time: now, Bool: true, Bytes: []byte("1.200000")},
		{Int: 13, Float: 1.300000, String: "1.300000", Time: now, Bool: false, Bytes: []byte("1.300000")},
		{Int: 14, Float: 1.400000, String: "1.400000", Time: now, Bool: true, Bytes: []byte("1.400000")},
	}
	_, err = ds.Insert().Rows(entries).Executor().Exec()
	pt.NoError(err)

	var newEntries []entry
	pt.NoError(ds.Where(goqu.C("int").In([]uint32{11, 12, 13, 14})).ScanStructs(&newEntries))
	pt.Len(newEntries, 4)

	_, err = ds.Insert().Rows(
		entry{Int: 15, Float: 1.500000, String: "1.500000", Time: now, Bool: false, Bytes: []byte("1.500000")},
		entry{Int: 16, Float: 1.600000, String: "1.600000", Time: now, Bool: true, Bytes: []byte("1.600000")},
		entry{Int: 17, Float: 1.700000, String: "1.700000", Time: now, Bool: false, Bytes: []byte("1.700000")},
		entry{Int: 18, Float: 1.800000, String: "1.800000", Time: now, Bool: true, Bytes: []byte("1.800000")},
	).Executor().Exec()
	pt.NoError(err)

	newEntries = newEntries[0:0]
	pt.NoError(ds.Where(goqu.C("int").In([]uint32{15, 16, 17, 18})).ScanStructs(&newEntries))
	pt.Len(newEntries, 4)
}

func (pt *postgresTest) TestInsertReturning() {
	ds := pt.db.From("entry")
	now := time.Now()
	e := entry{Int: 10, Float: 1.000000, String: "1.000000", Time: now, Bool: true, Bytes: []byte("1.000000")}
	found, err := ds.Insert().Rows(e).Returning(goqu.Star()).Executor().ScanStruct(&e)
	pt.NoError(err)
	pt.True(found)
	pt.True(e.ID > 0)

	var ids []uint32
	pt.NoError(ds.Insert().Rows([]entry{
		{Int: 11, Float: 1.100000, String: "1.100000", Time: now, Bool: false, Bytes: []byte("1.100000")},
		{Int: 12, Float: 1.200000, String: "1.200000", Time: now, Bool: true, Bytes: []byte("1.200000")},
		{Int: 13, Float: 1.300000, String: "1.300000", Time: now, Bool: false, Bytes: []byte("1.300000")},
		{Int: 14, Float: 1.400000, String: "1.400000", Time: now, Bool: true, Bytes: []byte("1.400000")},
	}).Returning("id").Executor().ScanVals(&ids))
	pt.Len(ids, 4)
	for _, id := range ids {
		pt.True(id > 0)
	}

	var ints []int64
	pt.NoError(ds.Insert().Rows(
		entry{Int: 15, Float: 1.500000, String: "1.500000", Time: now, Bool: false, Bytes: []byte("1.500000")},
		entry{Int: 16, Float: 1.600000, String: "1.600000", Time: now, Bool: true, Bytes: []byte("1.600000")},
		entry{Int: 17, Float: 1.700000, String: "1.700000", Time: now, Bool: false, Bytes: []byte("1.700000")},
		entry{Int: 18, Float: 1.800000, String: "1.800000", Time: now, Bool: true, Bytes: []byte("1.800000")},
	).Returning("int").Executor().ScanVals(&ints))
	pt.True(found)
	pt.Equal(ints, []int64{15, 16, 17, 18})
}

func (pt *postgresTest) TestUpdate() {
	ds := pt.db.From("entry")
	var e entry
	found, err := ds.Where(goqu.C("int").Eq(9)).Select("id").ScanStruct(&e)
	pt.NoError(err)
	pt.True(found)
	e.Int = 11
	_, err = ds.Where(goqu.C("id").Eq(e.ID)).Update().Set(e).Executor().Exec()
	pt.NoError(err)

	count, err := ds.Where(goqu.C("int").Eq(11)).Count()
	pt.NoError(err)
	pt.Equal(int64(1), count)

	var id uint32
	found, err = ds.Where(goqu.C("int").Eq(11)).
		Update().
		Set(goqu.Record{"int": 9}).
		Returning("id").Executor().ScanVal(&id)
	pt.NoError(err)
	pt.True(found)
	pt.Equal(id, e.ID)
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
	ds := pt.db.From("entry")
	var e entry
	found, err := ds.Where(goqu.C("int").Eq(9)).Select("id").ScanStruct(&e)
	pt.NoError(err)
	pt.True(found)
	_, err = ds.Where(goqu.C("id").Eq(e.ID)).Delete().Executor().Exec()
	pt.NoError(err)

	count, err := ds.Count()
	pt.NoError(err)
	pt.Equal(int64(9), count)

	var id uint32
	found, err = ds.Where(goqu.C("id").Eq(e.ID)).ScanVal(&id)
	pt.NoError(err)
	pt.False(found)

	e = entry{}
	found, err = ds.Where(goqu.C("int").Eq(8)).Select("id").ScanStruct(&e)
	pt.NoError(err)
	pt.True(found)
	pt.NotEqual(e.ID, int64(0))

	id = 0
	_, err = ds.Where(goqu.C("id").Eq(e.ID)).Delete().Returning("id").Executor().ScanVal(&id)
	pt.NoError(err)
	pt.Equal(id, e.ID)
}

func (pt *postgresTest) TestInsert_OnConflict() {
	ds := pt.db.From("entry")
	now := time.Now()

	// DO NOTHING insert
	e := entry{Int: 10, Float: 1.100000, String: "1.100000", Time: now, Bool: false, Bytes: []byte("1.100000")}
	_, err := ds.Insert().Rows(e).OnConflict(goqu.DoNothing()).Executor().Exec()
	pt.NoError(err)

	// DO NOTHING duplicate
	e = entry{Int: 10, Float: 2.100000, String: "2.100000", Time: now.Add(time.Hour * 100), Bool: false, Bytes: []byte("2.100000")}
	_, err = ds.Insert().Rows(e).OnConflict(goqu.DoNothing()).Executor().Exec()
	pt.NoError(err)

	// DO NOTHING update
	var entryActual entry
	e2 := entry{Int: 0, String: "2.000000"}
	_, err = ds.Insert().
		Rows(e2).
		OnConflict(goqu.DoUpdate("int", goqu.Record{"string": "upsert"})).
		Executor().Exec()
	pt.NoError(err)
	_, err = ds.Where(goqu.C("int").Eq(0)).ScanStruct(&entryActual)
	pt.NoError(err)
	pt.Equal("upsert", entryActual.String)

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
	pt.NoError(err)

	var entry8, entry9 entry
	_, err = ds.Where(goqu.Ex{"int": 1}).ScanStruct(&entry8)
	pt.NoError(err)
	pt.Equal("0.100000", entry8.String)

	_, err = ds.Where(goqu.Ex{"int": 2}).ScanStruct(&entry9)
	pt.NoError(err)
	pt.Equal("upsert", entry9.String)
}

func (pt *postgresTest) TestWindowFunction() {
	ds := pt.db.From("entry").
		Select("int", goqu.ROW_NUMBER().OverName(goqu.I("w")).As("id")).
		Window(goqu.W("w").OrderBy(goqu.I("int").Desc()))

	var entries []entry
	pt.NoError(ds.ScanStructs(&entries))

	pt.Equal([]entry{
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
}

func TestPostgresSuite(t *testing.T) {
	suite.Run(t, new(postgresTest))
}
