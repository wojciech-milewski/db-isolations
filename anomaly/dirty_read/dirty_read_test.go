package dirty_read

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

const (
	mysqlDSN    = "root:root@tcp(localhost:3306)/anomaly_test?multiStatements=true"
	postgresDSN = "postgres://root:root@localhost:5432/anomaly_test?sslmode=disable"

	mysqlDriverName    = "mysql"
	postgresDriverName = "postgres"
)

func TestShouldFindDirtyReadOnMySQL(t *testing.T) {
	testShouldFindDirtyRead(t, OpenMysql)
}

func TestShouldFindDirtyReadOnPostgres(t *testing.T) {
	testShouldFindDirtyRead(t, OpenPostgres)
}

func testShouldFindDirtyRead(t *testing.T, sqlOpenFunc func() (*sql.DB, error)) {
	db, err := sqlOpenFunc()
	if err != nil {
		t.Fatalf("Failed to open db: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("TRUNCATE counters")
	if err != nil {
		t.Fatalf("Failed to truncate: %v", err)
	}

	_, err = db.Exec("INSERT INTO counters (name, counter) VALUES ('x', 10);")
	if err != nil {
		t.Fatalf("Failed to insert counter: %v", err)
	}

	for i := 1; i <= 1000; i++ {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			writeDone := make(chan bool, 1)
			readDone := make(chan bool, 1)

			go runAsync(func() { incrementAndRollback(db) }, writeDone)

			go runAsync(func() { readAndAssert(db, t) }, readDone)

			<-writeDone
			<-readDone
		})
	}
}

func OpenMysql() (*sql.DB, error) {
	return sql.Open(mysqlDriverName, mysqlDSN)
}

func OpenPostgres() (*sql.DB, error) {
	return sql.Open(postgresDriverName, postgresDSN)
}

func incrementAndRollback(db *sql.DB) {
	_, err := db.Exec(`
		BEGIN;
		UPDATE counters SET counter = counter + 1 WHERE name='x';
		ROLLBACK;
		`)

	if err != nil {
		panic(err)
	}
}

func readAndAssert(db *sql.DB, t *testing.T) {
	actualCounter, err := read(db)
	assert.NoError(t, err)
	assert.Equal(t, 10, actualCounter)
}

func runAsync(f func(), done chan bool) {
	f()
	done <- true
}

func read(db *sql.DB) (int, error) {
	row := db.QueryRow(`
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
	BEGIN;
	SELECT counter FROM counters WHERE name='x';
	COMMIT;
`)

	var counter int
	err := row.Scan(&counter)

	return counter, err
}
