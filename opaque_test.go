package goqu_test

import (
	dbsql "database/sql"
	"database/sql/driver"
	"io"
	"reflect"
	"sync"
	"testing"

	"github.com/doug-martin/goqu/v9"
	"github.com/doug-martin/goqu/v9/internal/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// opaqueNative looks like a driver specific type (e.g. pgx): no driver.Valuer, no sql.Scanner.
type opaqueNative struct {
	Text  string
	Valid bool
}

// opaqueValuer is encoded through its driver.Valuer when interpolated.
type opaqueValuer struct{ A, B int }

func (opaqueValuer) Value() (driver.Value, error) { return "valuer", nil }

type opaqueTagged struct {
	ID     int
	Native opaqueNative `goqu:"opaque"`
	Plain  opaqueNative
}

// OpaqueBase is exported because goqu ignores embedded fields of unexported types.
type OpaqueBase struct {
	Text  string
	Valid bool
}

type opaqueEmbedded struct {
	OpaqueBase `goqu:"opaque"`
}

type opaqueRegistered struct {
	ID int
	N  opaqueRegisteredType
	P  *opaqueRegisteredType
}

type opaqueRegisteredType struct{ X, Y int }

func TestOpaqueTag(t *testing.T) {
	ds := goqu.Insert("t").Rows(opaqueTagged{ID: 1, Native: opaqueNative{Text: "a", Valid: true}})

	// the tagged field is one column, the untagged one is still flattened
	sql, args, err := ds.Prepared(true).ToSQL()
	require.NoError(t, err)
	assert.Equal(t, `INSERT INTO "t" ("id", "native", "plain"."text", "plain"."valid") VALUES (?, ?, ?, ?)`, sql)
	assert.Equal(t, []any{int64(1), opaqueNative{Text: "a", Valid: true}, "", false}, args)

	// interpolating a value without a driver.Valuer is a clear error
	_, _, err = ds.ToSQL()
	assert.Error(t, err)

	sql, _, err = goqu.Update("t").Set(opaqueTagged{ID: 1}).Prepared(true).ToSQL()
	require.NoError(t, err)
	assert.Contains(t, sql, `"native"=?`)
	assert.NotContains(t, sql, `"native"."text"`)

	sql, _, err = goqu.From("t").Select(opaqueTagged{}).ToSQL()
	require.NoError(t, err)
	assert.Equal(t, `SELECT "id", "native", "plain"."text" AS "plain.text", "plain"."valid" AS "plain.valid" FROM "t"`, sql)
}

func TestOpaqueEmbeddedTag(t *testing.T) {
	sql, args, err := goqu.Insert("t").Rows(opaqueEmbedded{}).Prepared(true).ToSQL()
	require.NoError(t, err)
	assert.Equal(t, `INSERT INTO "t" ("opaquebase") VALUES (?)`, sql)
	assert.Equal(t, []any{OpaqueBase{}}, args)
}

func TestRegisterOpaqueType(t *testing.T) {
	t.Cleanup(util.ResetOpaqueTypes)
	row := opaqueRegistered{ID: 1, N: opaqueRegisteredType{1, 2}, P: &opaqueRegisteredType{3, 4}}

	// before registration the type is flattened
	sql, _, err := goqu.Insert("t").Rows(row).Prepared(true).ToSQL()
	require.NoError(t, err)
	assert.Contains(t, sql, `"n"."x"`)

	// registering after the struct was first used resets the cached column map; pointers are covered too
	goqu.RegisterOpaque[opaqueRegisteredType]()
	sql, args, err := goqu.Insert("t").Rows(row).Prepared(true).ToSQL()
	require.NoError(t, err)
	assert.Equal(t, `INSERT INTO "t" ("id", "n", "p") VALUES (?, ?, ?)`, sql)
	assert.Equal(t, []any{int64(1), opaqueRegisteredType{1, 2}, &opaqueRegisteredType{3, 4}}, args)

	goqu.RegisterOpaqueType(reflect.TypeOf(&opaqueRegisteredType{})) // idempotent, pointer normalized
	goqu.RegisterOpaqueType(nil)                                     // ignored
}

func TestOpaqueValuerInterpolated(t *testing.T) {
	type row struct {
		V opaqueValuer `goqu:"opaque"`
	}
	sql, args, err := goqu.Insert("t").Rows(row{}).ToSQL()
	require.NoError(t, err)
	assert.Empty(t, args)
	assert.Equal(t, `INSERT INTO "t" ("v") VALUES ('valuer')`, sql)

	// without the tag a Valuer-only struct is still flattened (unchanged behavior)
	type flat struct{ V opaqueValuer }
	sql, _, err = goqu.Insert("t").Rows(flat{}).ToSQL()
	require.NoError(t, err)
	assert.Equal(t, `INSERT INTO "t" ("v"."a", "v"."b") VALUES (0, 0)`, sql)
}

// nativeDriver returns a struct value for the "native" column, like a driver that decodes its own types (pgx).
type (
	nativeDriver struct{ val opaqueNative }
	nativeConn   struct{ val opaqueNative }
	nativeStmt   struct{ val opaqueNative }
	nativeRows   struct {
		val  opaqueNative
		done bool
	}
)

func (d nativeDriver) Open(string) (driver.Conn, error)  { return nativeConn(d), nil }
func (c nativeConn) Prepare(string) (driver.Stmt, error) { return nativeStmt(c), nil }
func (nativeConn) Close() error                          { return nil }
func (nativeConn) Begin() (driver.Tx, error)             { return nil, driver.ErrSkip }
func (nativeStmt) Close() error                          { return nil }
func (nativeStmt) NumInput() int                         { return -1 }
func (nativeStmt) Exec([]driver.Value) (driver.Result, error) {
	return nil, driver.ErrSkip
}
func (s nativeStmt) Query([]driver.Value) (driver.Rows, error) { return &nativeRows{val: s.val}, nil }
func (*nativeRows) Columns() []string                          { return []string{"id", "native"} }
func (*nativeRows) Close() error                               { return nil }
func (r *nativeRows) Next(dest []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	dest[0], dest[1] = int64(1), r.val
	return nil
}

var registerNativeDriver sync.Once

func TestOpaqueScan(t *testing.T) {
	type row struct {
		ID     int
		Native opaqueNative `goqu:"opaque"`
	}
	want := opaqueNative{Text: "a", Valid: true}
	// registering the same driver name twice panics, e.g. with go test -count=2
	registerNativeDriver.Do(func() { dbsql.Register("goqu_opaque_native", nativeDriver{val: want}) })
	db, err := dbsql.Open("goqu_opaque_native", "")
	require.NoError(t, err)
	defer db.Close()

	var rows []row
	require.NoError(t, goqu.New("default", db).From("t").ScanStructs(&rows))
	assert.Equal(t, []row{{ID: 1, Native: want}}, rows)

	var one row
	found, err := goqu.New("default", db).From("t").ScanStruct(&one)
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, want, one.Native)
}

func TestRegisterOpaqueTypeConcurrent(t *testing.T) {
	t.Cleanup(util.ResetOpaqueTypes)
	type concurrentType struct{ V int }
	type row struct {
		ID int
		C  concurrentType
	}
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if i%2 == 0 {
				goqu.RegisterOpaque[concurrentType]()
			}
			_, _, err := goqu.Insert("t").Rows(row{}).Prepared(true).ToSQL()
			assert.NoError(t, err)
		}()
	}
	wg.Wait()
}

type opaqueUnexported struct{ Text string }

func TestOpaqueTagOnUnexportedEmbeddedType(t *testing.T) {
	type row struct {
		ID               int
		opaqueUnexported `goqu:"opaque"`
	}
	// the value of an unexported embedded field cannot be read, so the field is left out instead of flattened
	query, _, err := goqu.Insert("t").Rows(row{ID: 1}).ToSQL()
	require.NoError(t, err)
	assert.Equal(t, `INSERT INTO "t" ("id") VALUES (1)`, query)
}
