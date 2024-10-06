package duckdb

/*
#include <duckdb.h>
*/
import "C"

import (
	"database/sql"
	"unsafe"
)

// ProfilingInfo is a recursive type containing metrics for each node in DuckDB's query plan.
// There are two types of nodes: the QUERY_ROOT and OPERATOR nodes.
// The QUERY_ROOT refers exclusively to the top-level node; its metrics are measured over the entire query.
// The OPERATOR nodes refer to the individual operators in the query plan.
type ProfilingInfo struct {
	// Metrics contains all key-value pairs of the current node.
	// The key represents the name and corresponds to the measured value.
	Metrics map[string]string
	// Children contains all children of the node and their respective metrics.
	Children []ProfilingInfo
}

// GetProfilingInfo obtains all available metrics set by the current connection.
func GetProfilingInfo(c *sql.Conn) (ProfilingInfo, error) {
	info := ProfilingInfo{}
	err := c.Raw(func(driverConn any) error {
		con := driverConn.(*conn)
		duckdbInfo := C.duckdb_get_profiling_info(con.duckdbCon)
		if duckdbInfo == nil {
			return getError(errProfilingInfoEmpty, nil)
		}

		// Recursive tree traversal.
		info.getMetrics(duckdbInfo)
		return nil
	})
	return info, err
}

func (info *ProfilingInfo) getMetrics(duckdbInfo C.duckdb_profiling_info) {
	m := C.duckdb_profiling_info_get_metrics(duckdbInfo)
	count := C.duckdb_get_map_size(m)
	info.Metrics = make(map[string]string, count)

	for i := C.idx_t(0); i < count; i++ {
		key := C.duckdb_get_map_key(m, i)
		value := C.duckdb_get_map_value(m, i)

		cKey := C.duckdb_get_varchar(key)
		cValue := C.duckdb_get_varchar(value)
		keyStr := C.GoString(cKey)
		valueStr := C.GoString(cValue)

		info.Metrics[keyStr] = valueStr

		C.duckdb_destroy_value(&key)
		C.duckdb_destroy_value(&value)
		C.duckdb_free(unsafe.Pointer(cKey))
		C.duckdb_free(unsafe.Pointer(cValue))
	}
	C.duckdb_destroy_value(&m)

	childCount := C.duckdb_profiling_info_get_child_count(duckdbInfo)
	for i := C.idx_t(0); i < childCount; i++ {
		duckdbChildInfo := C.duckdb_profiling_info_get_child(duckdbInfo, i)
		childInfo := ProfilingInfo{}
		childInfo.getMetrics(duckdbChildInfo)
		info.Children = append(info.Children, childInfo)
	}
}
