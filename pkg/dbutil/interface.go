package dbutil

import (
	"context"
	"database/sql"
)

// check compatibility
var (
	_ DBExecutor = &sql.DB{}
	_ DBExecutor = &sql.Conn{}
)

type QueryExecutor interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

type DBExecutor interface {
	QueryExecutor
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}
