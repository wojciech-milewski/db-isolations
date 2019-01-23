package dirty_write

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

const (
	ResetCountersQueryMysql    = "INSERT INTO counters (name, counter) VALUES ('first', 0), ('second', 0) ON DUPLICATE KEY UPDATE counter=0;"
	ResetCountersQueryPostgres = "INSERT INTO counters (name, counter) VALUES ('first', 0), ('second', 0) ON CONFLICT (name) DO UPDATE SET counter=0;"
)

func TestShouldFindDirtyWriteOnMySQL(t *testing.T) {
	testShouldFindDirtyWrite(t, OpenMysql, ResetCountersQueryMysql)
}

func TestShouldFindDirtyWriteOnPostgres(t *testing.T) {
	testShouldFindDirtyWrite(t, OpenPostgres, ResetCountersQueryPostgres)
}

func testShouldFindDirtyWrite(t *testing.T, sqlOpenFunc func() (*sql.DB, error), resetQuery string) {
	db, err := sqlOpenFunc()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer func() {
		closeErr := db.Close()
		if closeErr != nil {
			panic(closeErr)
		}
	}()

	_, err = db.Exec("TRUNCATE counters")
	if err != nil {
		t.Fatalf("Failed to truncate: %v", err)
	}

	for i := 1; i <= 1000; i++ {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			_, err = db.Exec(resetQuery)
			if err != nil {
				t.Fatalf("Failed to insert counter: %v", err)
			}

			firstWriteDone := make(chan bool, 1)
			secondWriteDone := make(chan bool, 1)

			go runAsync(func() { setValues(db, 1) }, firstWriteDone)

			go runAsync(func() { setValues(db, 2) }, secondWriteDone)

			<-firstWriteDone
			<-secondWriteDone

			assertConsistentCounters(t, db)
		})
	}
}

func setValues(db *sql.DB, value int) {
	_, err := db.Exec(fmt.Sprintf(`
			SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
			BEGIN;
			UPDATE counters SET counter=%d; 
			COMMIT;
			`, value))

	if err != nil {
		panic(err)
	}
}

func assertConsistentCounters(t *testing.T, db *sql.DB) {
	var firstCounter int
	err := db.QueryRow(`SELECT counter FROM counters WHERE name='first';`).Scan(&firstCounter)
	if err != nil {
		panic(err)
	}

	var secondCounter int
	err = db.QueryRow(`SELECT counter FROM counters WHERE name='second';`).Scan(&secondCounter)
	if err != nil {
		panic(err)
	}

	assert.True(
		t,
		firstCounter == secondCounter,
		fmt.Sprintf("Counters not equal. First: %d, Second: %d", firstCounter, secondCounter),
	)
}

const (
	mysqlDSN    = "root:root@tcp(localhost:3306)/anomaly_test?multiStatements=true&interpolateParams=true"
	postgresDSN = "postgres://root:root@localhost:5432/anomaly_test?sslmode=disable"

	mysqlDriverName    = "mysql"
	postgresDriverName = "postgres"
)

func OpenMysql() (*sql.DB, error) {
	return sql.Open(mysqlDriverName, mysqlDSN)
}

func OpenPostgres() (*sql.DB, error) {
	return sql.Open(postgresDriverName, postgresDSN)
}

func runAsync(f func(), done chan bool) {
	f()
	done <- true
}
