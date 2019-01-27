package lost_updates

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

	for i := 1; i <= 1000; i++ {
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

			assertIncrementedByTwo(t, db)
		})
	}
}

func TestShouldPreventLostUpdatesOnPostgresWithAtomicUpdate(t *testing.T) {
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

	for i := 1; i <= 1000; i++ {
		_, err = db.Exec(ResetCountersQueryPostgres)
		if err != nil {
			t.Fatalf("Failed to insert counter: %v", err)
		}

		t.Run(strconv.Itoa(i), func(t *testing.T) {
			firstIncrementDone := make(chan bool, 1)
			secondIncrementDone := make(chan bool, 1)

			go runAsync(func() { incrementCounterByOneWithAtomicUpdate(db) }, firstIncrementDone)

			go runAsync(func() { incrementCounterByOneWithAtomicUpdate(db) }, secondIncrementDone)

			<-firstIncrementDone
			<-secondIncrementDone

			assertIncrementedByTwo(t, db)
		})
	}
}

func TestShouldPreventLostUpdatesOnPostgresWithSelectForUpdate(t *testing.T) {
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

	for i := 1; i <= 1000; i++ {
		_, err = db.Exec(ResetCountersQueryPostgres)
		if err != nil {
			t.Fatalf("Failed to insert counter: %v", err)
		}

		t.Run(strconv.Itoa(i), func(t *testing.T) {
			firstIncrementDone := make(chan bool, 1)
			secondIncrementDone := make(chan bool, 1)

			go runAsync(func() { incrementCounterByOneWithSelectForUpdate(db) }, firstIncrementDone)

			go runAsync(func() { incrementCounterByOneWithSelectForUpdate(db) }, secondIncrementDone)

			<-firstIncrementDone
			<-secondIncrementDone

			assertIncrementedByTwo(t, db)
		})
	}
}

func TestShouldPreventLostUpdatesOnPostgresWithCompareAndSet(t *testing.T) {
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

	for i := 1; i <= 1000; i++ {
		_, err = db.Exec(ResetCountersQueryPostgres)
		if err != nil {
			t.Fatalf("Failed to insert counter: %v", err)
		}

		t.Run(strconv.Itoa(i), func(t *testing.T) {
			firstIncrementDone := make(chan bool, 1)
			secondIncrementDone := make(chan bool, 1)

			go runAsync(func() { incrementCounterByOneWithCompareAndSet(db) }, firstIncrementDone)

			go runAsync(func() { incrementCounterByOneWithCompareAndSet(db) }, secondIncrementDone)

			<-firstIncrementDone
			<-secondIncrementDone

			assertIncrementedByTwo(t, db)
		})
	}
}

func incrementCounterByOne(db *sql.DB) {
	transaction, err := db.BeginTx(
		context.Background(),
		&sql.TxOptions{Isolation: sql.LevelReadCommitted},
	)
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

//noinspection SqlNoDataSourceInspection,SqlResolve,SqlDialectInspection
func incrementCounterByOneWithAtomicUpdate(db *sql.DB) {
	transaction, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		panic(err)
	}

	_, err = transaction.Exec("UPDATE counters SET counter = counter + 1 WHERE name='first';")
	if err != nil {
		panic(err)
	}

	err = transaction.Commit()
	if err != nil {
		panic(err)
	}
}

func incrementCounterByOneWithSelectForUpdate(db *sql.DB) {
	transaction, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		panic(err)
	}
	var counter int
	err = transaction.QueryRow("SELECT counter FROM counters WHERE name='first' FOR UPDATE;").Scan(&counter)

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

//noinspection SqlNoDataSourceInspection,SqlResolve,SqlDialectInspection
func incrementCounterByOneWithCompareAndSet(db *sql.DB) {
	transaction, err := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		panic(err)
	}
	var counter int
	err = transaction.QueryRow("SELECT counter FROM counters WHERE name='first';").Scan(&counter)

	if err != nil {
		panic(err)
	}

	newCounter := counter + 1

	updateResult, err := transaction.Exec("UPDATE counters SET counter=$1 WHERE name='first' AND counter=$2;", newCounter, counter)
	if err != nil {
		panic(err)
	}
	rowsAffected, err := updateResult.RowsAffected()
	if err != nil {
		panic(err)
	}

	if rowsAffected == 0 {
		err := transaction.Rollback()
		if err != nil {
			panic(err)
		}
		incrementCounterByOneWithCompareAndSet(db)
	} else {
		err = transaction.Commit()
		if err != nil {
			panic(err)
		}
	}
}

func assertIncrementedByTwo(t *testing.T, db *sql.DB) {
	counter := readCounter(db)

	assert.Equal(t, 2, counter)
}

//noinspection SqlNoDataSourceInspection,SqlResolve,SqlDialectInspection
func readCounter(db *sql.DB) int {
	var counter int

	err := db.QueryRow(`
			SELECT counter FROM counters WHERE name='first';
`).Scan(&counter)

	if err != nil {
		panic(err)
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
