package postgres

import (
	"github.com/doug-martin/gql"
)

func init() {
	gql.RegisterDbAdapter("postgres", func(db gql.Db) gql.DbAdapter {
		return newDbAdapter(db)
	})
	gql.RegisterDatasetAdapter("postgres", func(ds *gql.Dataset) gql.Adapter {
		return newDatasetAdapter(ds)
	})
}
