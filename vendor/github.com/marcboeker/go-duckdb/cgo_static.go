//go:build !duckdb_use_lib && (darwin || (linux && (amd64 || arm64)) || (freebsd && amd64))

package duckdb

/*
#cgo LDFLAGS: -lduckdb
#cgo darwin,amd64 LDFLAGS: -lc++ -L${SRCDIR}/deps/darwin_amd64
#cgo darwin,arm64 LDFLAGS: -lc++ -L${SRCDIR}/deps/darwin_arm64
#cgo linux,amd64 LDFLAGS: -lstdc++ -lm -ldl -L${SRCDIR}/deps/linux_amd64
#cgo linux,arm64 LDFLAGS: -lstdc++ -lm -ldl -L${SRCDIR}/deps/linux_arm64
#cgo freebsd,amd64 LDFLAGS: -lstdc++ -lm -ldl -L${SRCDIR}/deps/freebsd_amd64
#include <duckdb.h>
*/
import "C"
