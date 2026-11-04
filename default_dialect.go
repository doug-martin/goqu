package goqu

import (
	"fmt"
	"strings"
	"sync"
)

var (
	defaultDialect   = "default"
	defaultDialectMu sync.RWMutex
)

func defaultDialectName() string {
	defaultDialectMu.RLock()
	defer defaultDialectMu.RUnlock()
	return defaultDialect
}

// resetDefaultDialectIfDeregistered switches the default dialect back to "default" when its dialect is deregistered,
// so the package level functions never silently depend on a missing dialect.
func resetDefaultDialectIfDeregistered(name string) {
	defaultDialectMu.Lock()
	defer defaultDialectMu.Unlock()
	if strings.EqualFold(defaultDialect, name) {
		defaultDialect = "default"
	}
}

func dialectRegistered(name string) bool {
	dialectsMu.RLock()
	defer dialectsMu.RUnlock()
	_, ok := dialects[strings.ToLower(name)]
	return ok
}

// SetDefaultDialect sets the dialect used by the package level goqu.From, goqu.Select, goqu.Insert, goqu.Update,
// goqu.Delete and goqu.Truncate functions. It is "default" unless changed. Datasets that choose a dialect explicitly
// (goqu.Dialect, WithDialect, goqu.New) are not affected.
//
// SetDefaultDialect panics if the dialect is not registered, which usually means the dialect package was not imported.
// It is intended to be called once at startup, after the dialect packages have been imported and before any dataset is
// created. A dataset keeps the dialect it was created with, so package level variables such as
// `var q = goqu.From("t")` are initialized before an init() function runs and will not see the new default.
// If the default dialect is later removed with DeregisterDialect the default is reset to "default".
//
// An INSERT ... FromQuery(goqu.From(...)) accepts a select created with the package level functions in any dialect
// (it adopts the insert's dialect), as it did before this function existed.
//
//	import _ "github.com/doug-martin/goqu/v9/dialect/postgres"
//
//	goqu.SetDefaultDialect("postgres")
func SetDefaultDialect(dialect string) {
	// Hold the registry read lock while validating and assigning so a concurrent DeregisterDialect cannot slip in
	// between. Lock order is registry then default dialect, the same as DeregisterDialect.
	dialectsMu.RLock()
	defer dialectsMu.RUnlock()
	name := strings.ToLower(dialect)
	if _, ok := dialects[name]; !ok {
		panic(fmt.Errorf("goqu: dialect %q is not registered, did you import its package?", dialect))
	}
	defaultDialectMu.Lock()
	defer defaultDialectMu.Unlock()
	defaultDialect = name
}

// RequireDialect returns a DialectWrapper for the dialect, or an error if the dialect is not registered
// (for example because its package was not imported). goqu.Dialect silently falls back to the "default" dialect
// for unregistered names.
func RequireDialect(dialect string) (DialectWrapper, error) {
	if !dialectRegistered(dialect) {
		return DialectWrapper{}, fmt.Errorf("goqu: dialect %q is not registered, did you import its package?", dialect)
	}
	return Dialect(dialect), nil
}

// MustDialect is like RequireDialect but panics if the dialect is not registered.
func MustDialect(dialect string) DialectWrapper {
	dw, err := RequireDialect(dialect)
	if err != nil {
		panic(err)
	}
	return dw
}
