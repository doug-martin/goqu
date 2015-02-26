package mysql

import (
	"database/sql"
	"github.com/doug-martin/gql"
)

type (
	DbAdapter struct {
		*gql.DefaultDbAdapter
	}
)

func newDbAdapter(db gql.Db) gql.DbAdapter {
	return &DbAdapter{gql.NewDefaultDbAdapter(db, newTxDbAdapter)}
}

func (me DbAdapter) QueryAdapter(dataset *gql.Dataset) gql.Adapter {
	return newDatasetAdapter(dataset)
}

type TxDbAdapter struct {
	*gql.DefaultTxDbAdapter
}

func newTxDbAdapter(db *sql.Tx) gql.TxDbAdapter {
	return &TxDbAdapter{gql.NewDefaultTxDbAdapter(db)}
}

func (me TxDbAdapter) QueryAdapter(dataset *gql.Dataset) gql.Adapter {
	return newDatasetAdapter(dataset)
}
