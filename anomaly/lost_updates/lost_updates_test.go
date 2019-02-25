package lost_updates

import (
	"database/sql"
	"db-isolations/postgres"
	"db-isolations/util"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestShouldFindLostUpdatesOnPostgres(t *testing.T) {
	db, err := postgres.Open()
	util.PanicIfNotNil(err)

	defer util.CloseOrPanic(db)

	t.Run("Should FAIL on dummy increment", testLostUpdates(db, incrementCounterByOne))
	t.Run("Should PASS on increment with atomic update", testLostUpdates(db, incrementCounterByOneWithAtomicUpdate))
	t.Run("Should PASS on select for update", testLostUpdates(db, incrementCounterByOneWithSelectForUpdate))
	t.Run("Should PASS on compare and set", testLostUpdates(db, incrementCounterByOneWithCompareAndSet))
}

func testLostUpdates(db *sql.DB, incrementCounterFunc func(db *sql.DB)) func(t *testing.T) {
	return util.RepeatTest(func(t *testing.T) {
		util.ResetCounters(db)

		firstIncrementDone := make(chan bool, 1)
		secondIncrementDone := make(chan bool, 1)

		go runAsync(func() { incrementCounterFunc(db) }, firstIncrementDone)

		go runAsync(func() { incrementCounterFunc(db) }, secondIncrementDone)

		<-firstIncrementDone
		<-secondIncrementDone

		assertIncrementedByTwo(t, db)
	})
}

func incrementCounterByOne(db *sql.DB) {
	transaction := util.BeginTx(db, sql.LevelReadCommitted)

	counter := util.ScanToInt(transaction.QueryRow("SELECT counter FROM counters WHERE name='first';"))

	counter = counter + 1

	_, err := transaction.Exec("UPDATE counters SET counter=$1 WHERE name='first';", counter)
	util.PanicIfNotNil(err)

	err = transaction.Commit()
	util.PanicIfNotNil(err)
}

//noinspection SqlNoDataSourceInspection,SqlResolve,SqlDialectInspection
func incrementCounterByOneWithAtomicUpdate(db *sql.DB) {
	transaction := util.BeginTx(db, sql.LevelReadCommitted)

	_, err := transaction.Exec("UPDATE counters SET counter = counter + 1 WHERE name='first';")
	util.PanicIfNotNil(err)

	err = transaction.Commit()
	util.PanicIfNotNil(err)
}

func incrementCounterByOneWithSelectForUpdate(db *sql.DB) {
	transaction := util.BeginTx(db, sql.LevelReadCommitted)

	counter := util.ScanToInt(transaction.QueryRow("SELECT counter FROM counters WHERE name='first' FOR UPDATE;"))

	counter = counter + 1

	_, err := transaction.Exec("UPDATE counters SET counter=$1 WHERE name='first';", counter)
	util.PanicIfNotNil(err)

	err = transaction.Commit()
	util.PanicIfNotNil(err)
}

//noinspection SqlNoDataSourceInspection,SqlResolve,SqlDialectInspection
func incrementCounterByOneWithCompareAndSet(db *sql.DB) {
	transaction := util.BeginTx(db, sql.LevelReadCommitted)

	counter := util.ScanToInt(transaction.QueryRow("SELECT counter FROM counters WHERE name='first';"))

	newCounter := counter + 1

	updateResult, err := transaction.Exec("UPDATE counters SET counter=$1 WHERE name='first' AND counter=$2;", newCounter, counter)
	util.PanicIfNotNil(err)

	rowsAffected, err := updateResult.RowsAffected()
	util.PanicIfNotNil(err)

	if rowsAffected == 0 {
		err := transaction.Rollback()
		util.PanicIfNotNil(err)

		incrementCounterByOneWithCompareAndSet(db)
	} else {
		err = transaction.Commit()
		util.PanicIfNotNil(err)
	}
}

func assertIncrementedByTwo(t *testing.T, db *sql.DB) {
	counter := readCounter(db)

	assert.Equal(t, 2, counter)
}

//noinspection SqlNoDataSourceInspection,SqlResolve,SqlDialectInspection
func readCounter(db *sql.DB) int {
	return util.ScanToInt(db.QueryRow(`
				SELECT counter FROM counters WHERE name='first';
	`))
}

func runAsync(f func(), done chan bool) {
	f()
	done <- true
}
