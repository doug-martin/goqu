package mysql

import (
	"github.com/doug-martin/gql"
)

func init() {
	gql.RegisterDbAdapter("mysql", func(db gql.Db) gql.DbAdapter {
		return newDbAdapter(db)
	})
	gql.RegisterDatasetAdapter("mysql", func(ds *gql.Dataset) gql.Adapter {
		return newDatasetAdapter(ds)
	})
}
