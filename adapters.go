package gql

import (
	"database/sql"
	"reflect"
	"strings"
	"time"
)

type (
	Adapter interface {
		PlaceHolderSql(buf *SqlBuilder) error
		UpdateBeginSql(buf *SqlBuilder) error
		InsertBeginSql(buf *SqlBuilder) error
		DeleteBeginSql(buf *SqlBuilder) error
		TruncateSql(buf *SqlBuilder, cols ColumnList, opts TruncateOptions) error
		DefaultValuesSql(buf *SqlBuilder) error
		UpdateExpressionsSql(buf *SqlBuilder, updates ...UpdateExpression) error
		SelectSql(buf *SqlBuilder, cols ColumnList) error
		SelectDistinctSql(buf *SqlBuilder, cols ColumnList) error
		ReturningSql(buf *SqlBuilder, cols ColumnList) error
		FromSql(buf *SqlBuilder, from ColumnList) error
		SourcesSql(buf *SqlBuilder, from ColumnList) error
		JoinSql(buf *SqlBuilder, joins JoiningClauses) error
		WhereSql(buf *SqlBuilder, where ExpressionList) error
		GroupBySql(buf *SqlBuilder, groupBy ColumnList) error
		HavingSql(buf *SqlBuilder, having ExpressionList) error
		CompoundsSql(buf *SqlBuilder, compounds []CompoundExpression) error
		OrderSql(buf *SqlBuilder, order ColumnList) error
		LimitSql(buf *SqlBuilder, limit interface{}) error
		OffsetSql(buf *SqlBuilder, offset uint) error
		DatasetSql(buf *SqlBuilder, builder Dataset) error
		QuoteIdentifier(buf *SqlBuilder, ident IdentifierExpression) error
		LiteralNil(buf *SqlBuilder) error
		LiteralBool(buf *SqlBuilder, b bool) error
		LiteralTime(buf *SqlBuilder, t time.Time) error
		LiteralFloat(buf *SqlBuilder, f float64) error
		LiteralInt(buf *SqlBuilder, i int64) error
		LiteralString(buf *SqlBuilder, s string) error
		SliceValueSql(buf *SqlBuilder, slice reflect.Value) error
		AliasedExpressionSql(buf *SqlBuilder, aliased AliasedExpression) error
		BooleanExpressionSql(buf *SqlBuilder, operator BooleanExpression) error
		OrderedExpressionSql(buf *SqlBuilder, order OrderedExpression) error
		ExpressionListSql(buf *SqlBuilder, expressionList ExpressionList) error
		SqlFunctionExpressionSql(buf *SqlBuilder, sqlFunc SqlFunctionExpression) error
		CastExpressionSql(buf *SqlBuilder, casted CastExpression) error
		CompoundExpressionSql(buf *SqlBuilder, compound CompoundExpression) error
		ColumnListSql(buf *SqlBuilder, columnList ColumnList) error
		UpdateExpressionSql(buf *SqlBuilder, update UpdateExpression) error
		Literal(buf *SqlBuilder, i interface{}) error
		LiteralExpressionSql(buf *SqlBuilder, literal LiteralExpression) error
		InsertColumnsSql(buf *SqlBuilder, cols ColumnList) error
		InsertValuesSql(buf *SqlBuilder, values [][]interface{}) error
	}
	Db interface {
		Exec(query string, args ...interface{}) (sql.Result, error)
		Prepare(query string) (*sql.Stmt, error)
		Query(query string, args ...interface{}) (*sql.Rows, error)
		QueryRow(query string, args ...interface{}) *sql.Row
		Begin() (*sql.Tx, error)
	}
	baseDbAdapter interface {
		Logger(logger Logger)
		Trace(message string, args ...interface{})
		QueryAdapter(dataset *Dataset) Adapter
		Exec(query string, args ...interface{}) (sql.Result, error)
		Prepare(query string) (*sql.Stmt, error)
		Query(query string, args ...interface{}) (*sql.Rows, error)
		QueryRow(query string, args ...interface{}) *sql.Row
		Select(columnMap ColumnMap, query string, args ...interface{}) ([]Result, error)
		Insert(query string, args ...interface{}) ([]Result, error)
		Update(query string, args ...interface{}) (int64, error)
		Delete(query string, args ...interface{}) (int64, error)
	}
	DbAdapter interface {
		baseDbAdapter
		Begin() (TxDbAdapter, error)
	}
	TxDbAdapter interface {
		baseDbAdapter
		Commit() error
		Rollback() error
	}
	Result        map[string]interface{}
	SelectResults []Result
)

var (
	db_adapters = make(map[string]func(db Db) DbAdapter)
	ds_adapters = make(map[string]func(db *Dataset) Adapter)
)

func RegisterDbAdapter(t string, gen func(db Db) DbAdapter) {
	db_adapters[strings.ToLower(t)] = gen
}

func RegisterDatasetAdapter(t string, gen func(db *Dataset) Adapter) {
	ds_adapters[strings.ToLower(t)] = gen
}

func newDbAdapter(t string, db Db) DbAdapter {
	return db_adapters[strings.ToLower(t)](db)
}

func newDsAdapter(t string, ds *Dataset) Adapter {
	return ds_adapters[strings.ToLower(t)](ds)
}
