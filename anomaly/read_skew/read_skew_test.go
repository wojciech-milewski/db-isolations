package dirty_write

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

//noinspection SqlNoDataSourceInspection,SqlResolve
const (
	ResetCountersQueryPostgres      = "INSERT INTO counters (name, counter) VALUES ('first', 0), ('second', 0) ON CONFLICT (name) DO UPDATE SET counter=0;"
	ResetSingleCounterQueryPostgres = "INSERT INTO counters (name, counter) VALUES ('first', 0) ON CONFLICT (name) DO UPDATE SET counter=0;"
)

func TestShouldFindReadSkewOnPostgres_MultiObject(t *testing.T) {
	db, err := OpenPostgres()
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

	for i := 1; i <= 5000; i++ {
		_, err = db.Exec(ResetCountersQueryPostgres)
		if err != nil {
			t.Fatalf("Failed to insert counter: %v", err)
		}

		t.Run(strconv.Itoa(i), func(t *testing.T) {
			writeDone := make(chan bool, 1)
			readDone := make(chan bool, 1)

			go runAsync(func() { setValues(db, 1) }, writeDone)

			go runAsync(func() { assertConsistentCounters(t, db) }, readDone)
			<-writeDone

			<-readDone
		})
	}
}

func TestShouldFindReadSkewOnPostgres_SingleObject(t *testing.T) {
	db, err := OpenPostgres()
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

	for i := 1; i <= 5000; i++ {
		_, err = db.Exec(ResetSingleCounterQueryPostgres)
		if err != nil {
			t.Fatalf("Failed to insert counter: %v", err)
		}

		t.Run(strconv.Itoa(i), func(t *testing.T) {
			writeDone := make(chan bool, 1)
			readDone := make(chan bool, 1)

			go runAsync(func() { setCounter(db, 1) }, writeDone)

			go runAsync(func() { assertConsistentValues(t, db) }, readDone)
			<-writeDone

			<-readDone
		})
	}
}

func setValues(db *sql.DB, value int) {
	_, err := db.Exec(fmt.Sprintf(`
			SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
			BEGIN;
			UPDATE counters SET counter=%d WHERE name='first'; 
			UPDATE counters SET counter=%d WHERE name='second'; 
			COMMIT;
			`, value, value))

	if err != nil {
		panic(err)
	}
}

func setCounter(db *sql.DB, value int) {
	_, err := db.Exec(fmt.Sprintf(`
			SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
			BEGIN;
			UPDATE counters SET counter=%d WHERE name='first'; 
			COMMIT;
			`, value))

	if err != nil {
		panic(err)
	}
}

func assertConsistentCounters(t *testing.T, db *sql.DB) {
	firstCounter, secondCounter := readCounters(db)

	assert.True(
		t,
		firstCounter == secondCounter,
		fmt.Sprintf("Counters not equal. First: %d, Second: %d", firstCounter, secondCounter),
	)
}

//noinspection SqlNoDataSourceInspection,SqlResolve
func readCounters(db *sql.DB) (int, int) {
	rows, err := db.Query(`
			SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
			BEGIN;
			SELECT counter FROM counters WHERE name='first';
			SELECT counter FROM counters WHERE name='second';
			COMMIT;
`)

	defer func() {
		closeErr := rows.Close()
		if closeErr != nil {
			panic(closeErr)
		}
	}()

	if err != nil {
		panic(err)
	}

	firstCounter := scanCounter(rows)

	if !rows.NextResultSet() {
		panic("expected next result set")
	}

	secondCounter := scanCounter(rows)
	return firstCounter, secondCounter
}

func assertConsistentValues(t *testing.T, db *sql.DB) {
	firstValue, secondValue := readCounter(db)

	assert.True(
		t,
		firstValue == secondValue,
		fmt.Sprintf("Counters not equal. First: %d, Second: %d", firstValue, secondValue),
	)
}

//noinspection SqlNoDataSourceInspection,SqlResolve
func readCounter(db *sql.DB) (int, int) {
	rows, err := db.Query(`
			SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
BEGIN;
			SELECT counter FROM counters WHERE name='first';
			SELECT counter FROM counters WHERE name='first';
COMMIT;
`)

	defer func() {
		closeErr := rows.Close()
		if closeErr != nil {
			panic(closeErr)
		}
	}()

	if err != nil {
		panic(err)
	}

	firstCounter := scanCounter(rows)

	if !rows.NextResultSet() {
		panic("expected next result set")
	}

	secondCounter := scanCounter(rows)
	return firstCounter, secondCounter
}

func scanCounter(rows *sql.Rows) int {
	if !rows.Next() {
		panic(errors.New("expected row"))
	}

	var counter int
	err := rows.Scan(&counter)
	if err != nil {
		panic(err)
	}

	if rows.Next() {
		panic(errors.New("expected only one row"))
	}

	return counter
}

const (
	postgresDSN        = "postgres://root:root@localhost:5432/anomaly_test?sslmode=disable"
	postgresDriverName = "postgres"
)

func OpenPostgres() (*sql.DB, error) {
	return sql.Open(postgresDriverName, postgresDSN)
}

func runAsync(f func(), done chan bool) {
	f()
	done <- true
}
