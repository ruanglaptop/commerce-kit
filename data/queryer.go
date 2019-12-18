package data

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
)

type key int

const (
	txKey key = 0
)

// Queryer represents the database commands interface
type Queryer interface {
	PrepareNamed(query string) (*sqlx.NamedStmt, error)
	Rebind(query string) string
	MustExec(query string, args ...interface{}) sql.Result
	Select(dest interface{}, query string, args ...interface{}) error
	Get(dest interface{}, query string, args ...interface{}) error
}

// NewContext creates a new data context
func NewContext(ctx context.Context, q Queryer) context.Context {
	ctx = context.WithValue(ctx, txKey, q)
	return ctx
}

// TxFromContext returns the trasanction object from the context
func TxFromContext(ctx context.Context) (Queryer, bool) {
	q, ok := ctx.Value(txKey).(Queryer)
	return q, ok
}
