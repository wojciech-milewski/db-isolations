package write_skew

import (
	"database/sql"
	"db-isolations/postgres"
	"db-isolations/util"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestWriteSkew(t *testing.T) {
	db, err := postgres.Open()
	util.PanicIfNotNil(err)

	defer util.CloseOrPanic(db)

	t.Run("Should FAIL on read committed", testWriteSkewWithNaiveQuery(db, sql.LevelReadCommitted))
	t.Run("Should PASS on read committed with locking", testWriteSkewWithLocks(db, sql.LevelReadCommitted))
	t.Run("Should FAIL on repeatable read", testWriteSkewWithNaiveQuery(db, sql.LevelRepeatableRead))
	t.Run("Should FAIL on repeatable read with locking", testWriteSkewWithLocks(db, sql.LevelRepeatableRead))
	t.Run("Should PANIC on serializable", testWriteSkewWithNaiveQuery(db, sql.LevelSerializable))
	t.Run("Should PANIC on serializable with locking", testWriteSkewWithLocks(db, sql.LevelSerializable))
}

func testWriteSkewWithNaiveQuery(db *sql.DB, isolationLevel sql.IsolationLevel) func(t *testing.T) {
	return util.RepeatTest(func(t *testing.T) {
		util.TruncateCounters(db)

		firstInsertDone := make(chan bool, 1)
		secondInsertDone := make(chan bool, 1)

		go runAsync(func() { insertCounterIfNoCounters(db, "first", isolationLevel) }, firstInsertDone)

		go runAsync(func() { insertCounterIfNoCounters(db, "second", isolationLevel) }, secondInsertDone)

		<-firstInsertDone
		<-secondInsertDone

		assertOneCounter(t, db)
	})
}

func testWriteSkewWithLocks(db *sql.DB, isolationLevel sql.IsolationLevel) func(t *testing.T) {
	return util.RepeatTest(func(t *testing.T) {
		err := addMaterializedLock(db)
		util.PanicIfNotNil(err)

		util.TruncateCounters(db)

		firstInsertDone := make(chan bool, 1)
		secondInsertDone := make(chan bool, 1)

		go runAsync(func() { insertCounterIfNoCountersWithMaterializedLock(db, "first", isolationLevel) }, firstInsertDone)

		go runAsync(func() { insertCounterIfNoCountersWithMaterializedLock(db, "second", isolationLevel) }, secondInsertDone)

		<-firstInsertDone
		<-secondInsertDone

		assertOneCounter(t, db)
	})
}

func addMaterializedLock(db *sql.DB) error {
	_, err := db.Exec("INSERT INTO materialized_locks (name) VALUES ('counters') ON CONFLICT(name) DO NOTHING")
	return err
}

func insertCounterIfNoCounters(db *sql.DB, counterName string, isolationLevel sql.IsolationLevel) {
	transaction := util.BeginTx(db, isolationLevel)

	numberOfCounters := util.ScanToInt(transaction.QueryRow("SELECT count(name) FROM counters;"))

	if numberOfCounters == 0 {
		insertSQL := "INSERT INTO counters (name, counter) VALUES ($1, 0);"
		_, err := transaction.Exec(insertSQL, counterName)
		util.PanicIfNotNil(err)
	}

	err := transaction.Commit()
	util.PanicIfNotNil(err)
}

func insertCounterIfNoCountersWithMaterializedLock(db *sql.DB, counterName string, isolationLevel sql.IsolationLevel) {
	transaction := util.BeginTx(db, isolationLevel)

	lockSQL := "SELECT * FROM materialized_locks WHERE name='counters' FOR UPDATE"
	result, err := transaction.Query(lockSQL)
	util.PanicIfNotNil(err)

	for result.Next() {
	}

	selectCountSQL := "SELECT count(name) FROM counters;"
	numberOfCounters := util.ScanToInt(transaction.QueryRow(selectCountSQL))

	if numberOfCounters == 0 {
		insertSQL := "INSERT INTO counters (name, counter) VALUES ($1, 0);"
		_, err := transaction.Exec(insertSQL, counterName)
		util.PanicIfNotNil(err)
	}

	err = transaction.Commit()
	util.PanicIfNotNil(err)
}

func assertOneCounter(t *testing.T, db *sql.DB) {
	numberOfCounters := readNumberOfCounters(db)

	assert.Equal(t, 1, numberOfCounters)
}

func readNumberOfCounters(db *sql.DB) int {
	selectSQL := `SELECT count(name) FROM counters;`
	row := db.QueryRow(selectSQL)
	return util.ScanToInt(row)
}

func runAsync(f func(), done chan<- bool) {
	f()
	done <- true
}
