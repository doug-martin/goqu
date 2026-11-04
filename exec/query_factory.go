package exec

import (
	"context"
	"database/sql"

	"github.com/doug-martin/goqu/v9/internal/sb"
)

type (
	DbExecutor interface {
		ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
		QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	}
	QueryFactory interface {
		FromSQL(sql string, args ...interface{}) QueryExecutor
		FromSQLBuilder(b sb.SQLBuilder) QueryExecutor
	}
	querySupport struct {
		de      DbExecutor
		tagName string
	}
)

func NewQueryFactory(tagName string, de DbExecutor) QueryFactory {
	return &querySupport{tagName: tagName, de: de}
}

func (qs *querySupport) FromSQL(query string, args ...interface{}) QueryExecutor {
	return newQueryExecutor(qs.tagName, qs.de, nil, query, args...)
}

func (qs *querySupport) FromSQLBuilder(b sb.SQLBuilder) QueryExecutor {
	query, args, err := b.ToSQL()
	return newQueryExecutor(qs.tagName, qs.de, err, query, args...)
}
