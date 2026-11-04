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
// It is intended to be called once at startup, after the dialect packages have been imported.
//
//	import _ "github.com/doug-martin/goqu/v9/dialect/postgres"
//
//	goqu.SetDefaultDialect("postgres")
func SetDefaultDialect(dialect string) {
	if !dialectRegistered(dialect) {
		panic(fmt.Errorf("goqu: dialect %q is not registered, did you import its package?", dialect))
	}
	defaultDialectMu.Lock()
	defer defaultDialectMu.Unlock()
	defaultDialect = strings.ToLower(dialect)
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
