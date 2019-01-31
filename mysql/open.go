package mysql

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

const (
	mysqlDSN        = "root:root@tcp(localhost:3306)/anomaly_test?multiStatements=true"
	mysqlDriverName = "mysql"
)

func Open() (*sql.DB, error) {
	return sql.Open(mysqlDriverName, mysqlDSN)
}
