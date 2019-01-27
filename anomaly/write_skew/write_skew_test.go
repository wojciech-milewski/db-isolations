package write_skew

import (
	"context"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

const (
	ResetCountersQueryPostgres = "INSERT INTO counters (name, counter) VALUES ('first', 0), ('second', 0) ON CONFLICT (name) DO UPDATE SET counter=0;"
)

func TestShouldFindWriteSkew(t *testing.T) {
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

	for i := 1; i <= 1000; i++ {
		_, err = db.Exec("TRUNCATE counters")
		if err != nil {
			t.Fatalf("Failed to truncate: %v", err)
		}

		t.Run(strconv.Itoa(i), func(t *testing.T) {
			firstInsertDone := make(chan bool, 1)
			secondInsertDone := make(chan bool, 1)

			go runAsync(func() { insertCounterIfNoCounters(db, "first") }, firstInsertDone)

			go runAsync(func() { insertCounterIfNoCounters(db, "second") }, secondInsertDone)

			<-firstInsertDone
			<-secondInsertDone

			assertOneCounter(t, db)
		})
	}
}

//noinspection SqlNoDataSourceInspection,SqlResolve,SqlDialectInspection
func insertCounterIfNoCounters(db *sql.DB, counterName string) {
	transaction, err := db.BeginTx(
		context.Background(),
		&sql.TxOptions{Isolation: sql.LevelRepeatableRead},
	)
	if err != nil {
		panic(err)
	}

	var numberOfCounters int
	err = transaction.QueryRow("SELECT count(name) FROM counters;").Scan(&numberOfCounters)

	if err != nil {
		panic(err)
	}

	if numberOfCounters == 0 {
		_, err := transaction.Exec("INSERT INTO counters (name, counter) VALUES ($1, 0);", counterName)
		if err != nil {
			panic(err)
		}
	}

	err = transaction.Commit()
	if err != nil {
		panic(err)
	}
}

func assertOneCounter(t *testing.T, db *sql.DB) {
	numberOfCounters := readNumberOfCounters(db)

	assert.Equal(t, 1, numberOfCounters)
}

//noinspection SqlNoDataSourceInspection,SqlResolve,SqlDialectInspection
func readNumberOfCounters(db *sql.DB) int {
	var numberOfCounters int

	err := db.QueryRow(`
			SELECT count(name) FROM counters;
`).Scan(&numberOfCounters)

	if err != nil {
		panic(err)
	}

	return numberOfCounters
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
