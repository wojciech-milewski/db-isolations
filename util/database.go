package util

import (
	"context"
	"database/sql"
)

func TruncateCounters(db *sql.DB) {
	_, err := db.Exec("TRUNCATE counters")
	PanicIfNotNil(err)
}

func BeginTx(db *sql.DB, isolationLevel sql.IsolationLevel) *sql.Tx {
	transaction, err := db.BeginTx(
		context.Background(),
		&sql.TxOptions{Isolation: isolationLevel},
	)
	PanicIfNotNil(err)
	return transaction
}

func ScanToInt(row *sql.Row) int {
	var result int
	err := row.Scan(&result)
	PanicIfNotNil(err)

	return result
}
