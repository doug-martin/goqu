package goqu

import (
	"strings"
	"sync"

	"github.com/doug-martin/goqu/v9/exp"
	"github.com/doug-martin/goqu/v9/internal/sb"
	"github.com/doug-martin/goqu/v9/sqlgen"
)

type (
	SQLDialectOptions = sqlgen.SQLDialectOptions
	// An adapter interface to be used by a Dataset to generate SQL for a specific dialect.
	// See DefaultAdapter for a concrete implementation and examples.
	SQLDialect interface {
		Dialect() string
		ToSelectSQL(b sb.SQLBuilder, clauses exp.SelectClauses)
		ToUpdateSQL(b sb.SQLBuilder, clauses exp.UpdateClauses)
		ToInsertSQL(b sb.SQLBuilder, clauses exp.InsertClauses)
		ToDeleteSQL(b sb.SQLBuilder, clauses exp.DeleteClauses)
		ToTruncateSQL(b sb.SQLBuilder, clauses exp.TruncateClauses)
	}
	// The default adapter. This class should be used when building a new adapter. When creating a new adapter you can
	// either override methods, or more typically update default values.
	// See (github.com/doug-martin/goqu/dialect/postgres)
	sqlDialect struct {
		dialect        string
		dialectOptions *SQLDialectOptions
		selectGen      sqlgen.SelectSQLGenerator
		updateGen      sqlgen.UpdateSQLGenerator
		insertGen      sqlgen.InsertSQLGenerator
		deleteGen      sqlgen.DeleteSQLGenerator
		truncateGen    sqlgen.TruncateSQLGenerator
	}
)

var (
	dialects              = make(map[string]SQLDialect)
	DefaultDialectOptions = sqlgen.DefaultDialectOptions
	dialectsMu            sync.RWMutex
)

func init() {
	RegisterDialect("default", DefaultDialectOptions())
}

func RegisterDialect(name string, do *SQLDialectOptions) {
	dialectsMu.Lock()
	defer dialectsMu.Unlock()
	lowerName := strings.ToLower(name)
	dialects[lowerName] = newDialect(lowerName, do)
}

func DeregisterDialect(name string) {
	dialectsMu.Lock()
	defer dialectsMu.Unlock()
	delete(dialects, strings.ToLower(name))
	resetDefaultDialectIfDeregistered(name)
}

func GetDialect(name string) SQLDialect {
	name = strings.ToLower(name)
	dialectsMu.RLock()
	d, ok := dialects[name]
	dialectsMu.RUnlock()
	if ok {
		return d
	}
	return newDialect("default", DefaultDialectOptions())
}

func newDialect(dialect string, do *SQLDialectOptions) SQLDialect {
	return &sqlDialect{
		dialect:        dialect,
		dialectOptions: do,
		selectGen:      sqlgen.NewSelectSQLGenerator(dialect, do),
		updateGen:      sqlgen.NewUpdateSQLGenerator(dialect, do),
		insertGen:      sqlgen.NewInsertSQLGenerator(dialect, do),
		deleteGen:      sqlgen.NewDeleteSQLGenerator(dialect, do),
		truncateGen:    sqlgen.NewTruncateSQLGenerator(dialect, do),
	}
}

func (d *sqlDialect) Dialect() string {
	return d.dialect
}

func (d *sqlDialect) ToSelectSQL(b sb.SQLBuilder, clauses exp.SelectClauses) {
	d.selectGen.Generate(b, clauses)
}

func (d *sqlDialect) ToUpdateSQL(b sb.SQLBuilder, clauses exp.UpdateClauses) {
	d.updateGen.Generate(b, clauses)
}

func (d *sqlDialect) ToInsertSQL(b sb.SQLBuilder, clauses exp.InsertClauses) {
	d.insertGen.Generate(b, clauses)
}

func (d *sqlDialect) ToDeleteSQL(b sb.SQLBuilder, clauses exp.DeleteClauses) {
	d.deleteGen.Generate(b, clauses)
}

func (d *sqlDialect) ToTruncateSQL(b sb.SQLBuilder, clauses exp.TruncateClauses) {
	d.truncateGen.Generate(b, clauses)
}

// nestedDialect returns the dialect a nested dataset (subquery, CTE, compound, insert source) must render with so the
// whole statement uses a single dialect.
//   - A custom SQLDialect implementation is left alone, since goqu cannot know how to re-target it.
//   - When own already uses the enclosing statement's options it is returned as is (the common case, no lookup).
//   - Otherwise the registered dialect with the enclosing name is used when it shares the options, falling back to a
//     dialect built from the options.
func nestedDialect(own SQLDialect, name string, do *SQLDialectOptions) SQLDialect {
	osd, ok := own.(*sqlDialect)
	if !ok || osd.dialectOptions == do {
		return own
	}
	dialectsMu.RLock()
	d, found := dialects[strings.ToLower(name)]
	dialectsMu.RUnlock()
	if found {
		if sd, isSQLDialect := d.(*sqlDialect); isSQLDialect && sd.dialectOptions == do {
			return d
		}
	}
	return newDialect(name, do)
}
