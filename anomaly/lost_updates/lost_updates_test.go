package dirty_write

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

const (
	ResetCountersQueryPostgres = "INSERT INTO counters (name, counter) VALUES ('first', 0), ('second', 0) ON CONFLICT (name) DO UPDATE SET counter=0;"
)

func TestShouldFindLostUpdatesOnPostgres(t *testing.T) {
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

	for i := 1; i <= 100; i++ {
		_, err = db.Exec(ResetCountersQueryPostgres)
		if err != nil {
			t.Fatalf("Failed to insert counter: %v", err)
		}

		t.Run(strconv.Itoa(i), func(t *testing.T) {
			firstIncrementDone := make(chan bool, 1)
			secondIncrementDone := make(chan bool, 1)

			go runAsync(func() { incrementCounterByOne(db) }, firstIncrementDone)

			go runAsync(func() { incrementCounterByOne(db) }, secondIncrementDone)

			<-firstIncrementDone
			<-secondIncrementDone
		})
	}
}

func incrementCounterByOne(db *sql.DB) {
	transaction, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		panic(err)
	}
	var counter int
	err = transaction.QueryRow("SELECT counter FROM counters WHERE name='first';").Scan(&counter)

	if err != nil {
		panic(err)
	}

	counter = counter + 1

	_, err = transaction.Exec("UPDATE counters SET counter=$1 WHERE name='first';", counter)
	if err != nil {
		panic(err)
	}

	err = transaction.Commit()
	if err != nil {
		panic(err)
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

//noinspection SqlNoDataSourceInspection,SqlResolve

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
