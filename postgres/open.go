package postgres

import "database/sql"

const (
	postgresDSN        = "postgres://root:root@localhost:5432/anomaly_test?sslmode=disable"
	postgresDriverName = "postgres"
)

func Open() (*sql.DB, error) {
	return sql.Open(postgresDriverName, postgresDSN)
}
