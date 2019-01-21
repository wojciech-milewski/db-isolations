package anomaly

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

const (
	mysqlDSN = "root:root@tcp(localhost:3306)/anomaly_test?multiStatements=true"
)

func TestShouldFindDirtyRead(t *testing.T) {
	db, err := sql.Open("mysql", mysqlDSN)
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

			go updateAndSignal(db, t, writeDone)
			go readAndSignal(db, t, readDone)

			<-writeDone
			<-readDone
		})
	}

}

func updateAndRollback(db *sql.DB) error {
	_, err := db.Exec(`
		BEGIN;
		UPDATE counters SET counter = counter + 1 WHERE name='x';
		ROLLBACK;
		`)

	return err
}

func updateAndSignal(db *sql.DB, t *testing.T, done chan bool) {
	err := updateAndRollback(db)
	if err != nil {
		t.Fatalf("Failed to update counter and rollback: %v", err)
	}
	done <- true
}

func readAndSignal(db *sql.DB, t *testing.T, done chan bool) {
	actualCounter, err := read(db)

	assert.NoError(t, err)
	assert.Equal(t, 10, actualCounter)
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
